package com.meipian.queues.redis;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.hamcrest.core.IsEqual;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.meipian.queues.core.DelayQueue;
import com.meipian.queues.core.Message;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.sortedset.ZAddParams;

public class RedisDelayQueue implements DelayQueue {
	private transient final ReentrantLock lock = new ReentrantLock();

	private final Condition available = lock.newCondition();

	private JedisCluster jedisCluster;

	private long MAX_TIMEOUT = 525600000; // 最大超时时间不能超过一年

	private ObjectMapper om;

	private int unackTime = 60 * 1000;

	private String queueName;

	private String redisKeyPrefix;

	private String messageStoreKey;

	private String realQueueName;

	private DelayQueueProcessListener delayQueueProcessListener;

	private volatile boolean isEmpty = true;

	public RedisDelayQueue(String redisKeyPrefix, String queueName, JedisCluster jedisCluster, int unackTime,
			DelayQueueProcessListener delayQueueProcessListener) {
		om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(Include.NON_NULL);
		om.setSerializationInclusion(Include.NON_EMPTY);
		om.disable(SerializationFeature.INDENT_OUTPUT);
		this.redisKeyPrefix = redisKeyPrefix;
		this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
		this.unackTime = unackTime;
		this.jedisCluster = jedisCluster;
		realQueueName = redisKeyPrefix + ".QUEUE." + queueName;
		this.delayQueueProcessListener = delayQueueProcessListener;
	}

	public String getQueueName() {
		return queueName;
	}

	@Override
	public boolean push(Message message) {
		if (message.getTimeout() > MAX_TIMEOUT) {
			throw new RuntimeException("Maximum delay time should not be exceed one year");
		}
		try {
			String json = om.writeValueAsString(message);
			jedisCluster.hset(messageStoreKey, message.getId(), json);
			double priority = message.getPriority() / 100;
			double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
			jedisCluster.zadd(realQueueName, score, message.getId());
			delayQueueProcessListener.pushCallback(message);
			isEmpty = false;
			return true;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return false;

	}

	public void listen() {
		while (true) {
			String id = peekId();
			if (id == null) {
				continue;
			}
			String json = jedisCluster.hget(messageStoreKey, id);
			try {
				Message message = om.readValue(json, Message.class);
				if (message == null) {
					continue;
				}
				long delay = message.getCreateTime() + message.getTimeout() - System.currentTimeMillis();
				System.out.println(delay);
				if (delay <= 0) {
					delayQueueProcessListener.peekCallback(message);
				} else {
					LockSupport.parkNanos(this, TimeUnit.NANOSECONDS.convert(delay, TimeUnit.MILLISECONDS));
					delayQueueProcessListener.peekCallback(message);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	@Override
	public boolean ack(String messageId) {
		String unackQueueName = getUnackQueueName(queueName);
		jedisCluster.zrem(unackQueueName, messageId);
		Long removed = jedisCluster.zrem(realQueueName, messageId);
		Long msgRemoved = jedisCluster.hdel(messageStoreKey, messageId);
		if (removed > 0 && msgRemoved > 0) {
			return true;
		}
		return false;

	}

	@Override
	public boolean setUnackTimeout(String messageId, long timeout) {
		double unackScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue();
		String unackQueueName = getUnackQueueName(queueName);
		Double score = jedisCluster.zscore(unackQueueName, messageId);
		if (score != null) {
			jedisCluster.zadd(unackQueueName, unackScore, messageId);
			return true;
		}
		return false;

	}

	@Override
	public boolean setTimeout(String messageId, long timeout) {
		try {
			String json = jedisCluster.hget(messageStoreKey, messageId);
			if (json == null) {
				return false;
			}
			Message message = om.readValue(json, Message.class);
			message.setTimeout(timeout);
			Double score = jedisCluster.zscore(realQueueName, messageId);
			if (score != null) {
				double priorityd = message.getPriority() / 100;
				double newScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue() + priorityd;
				ZAddParams params = ZAddParams.zAddParams().xx();
				long added = jedisCluster.zadd(realQueueName, newScore, messageId, params);
				if (added == 1) {
					json = om.writeValueAsString(message);
					jedisCluster.hset(messageStoreKey, message.getId(), json);
					return true;
				}
				return false;
			}
			return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Message get(String messageId) {
		String json = jedisCluster.hget(messageStoreKey, messageId);
		if (json == null) {
			return null;
		}
		Message msg;
		try {
			msg = om.readValue(json, Message.class);
			return msg;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}

	@Override
	public long size() {
		return jedisCluster.zcard(realQueueName);
	}

	@Override
	public void clear() {
		String unackShard = getUnackQueueName(queueName);
		jedisCluster.del(realQueueName);
		jedisCluster.del(unackShard);
		jedisCluster.del(messageStoreKey);

	}

	private String peekId() {
		try {
			if (!isEmpty) {
				System.out.println("xxxxx");
				lock.lockInterruptibly();
				double max = Long.valueOf(System.currentTimeMillis() + MAX_TIMEOUT).doubleValue();
				Set<String> scanned = jedisCluster.zrangeByScore(realQueueName, 0, max, 0, 1);
				if (scanned.size() > 0) {
					String messageId = scanned.toArray()[0].toString();
					jedisCluster.zrem(realQueueName, messageId);
					setUnackTimeout(messageId, unackTime);
					if (size() == 0) {
						isEmpty = true;
					}
					available.signal();
					lock.unlock();
					return messageId;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			available.signal();
			lock.unlock();
		}
		return null;
	}

	public void processUnacks() {
		long queueDepth = size();
		int batchSize = 1_000;
		String unackQueueName = getUnackQueueName(queueName);
		double now = Long.valueOf(System.currentTimeMillis()).doubleValue();
		Set<Tuple> unacks = jedisCluster.zrangeByScoreWithScores(unackQueueName, 0, now, 0, batchSize);
		for (Tuple unack : unacks) {
			double score = unack.getScore();
			String member = unack.getElement();
			String payload = jedisCluster.hget(messageStoreKey, member);
			if (payload == null) {
				jedisCluster.zrem(unackQueueName, member);
				continue;
			}
			jedisCluster.zadd(realQueueName, score, member);
			jedisCluster.zrem(unackQueueName, member);
		}
	}

	private String getUnackQueueName(String queueName) {
		return redisKeyPrefix + ".UNACK." + queueName;
	}

	@Override
	public String getName() {
		return this.realQueueName;
	}

	@Override
	public int getUnackTime() {

		return this.unackTime;
	}

}
