package com.meipian.queues.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
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

	private LinkedBlockingQueue<String> prefetchedIds;

	private JedisCluster jedisCluster;

	private long MAX_TIMEOUT = 525600000; // 最大超时时间不能超过一年

	private ObjectMapper om;

	private int unackTime = 60;

	private String queueName;

	private String redisKeyPrefix;

	private String messageStoreKey;

	private ExecutorService executorService;

	private int retryCount = 2;

	private String realQueueName;

	private long defaultTimeout;

	public RedisDelayQueue(String redisKeyPrefix, String queueName, JedisCluster jedisCluster) {
		ObjectMapper om = new ObjectMapper();
		om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
		om.configure(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, false);
		om.setSerializationInclusion(Include.NON_NULL);
		om.setSerializationInclusion(Include.NON_EMPTY);
		om.disable(SerializationFeature.INDENT_OUTPUT);
		this.redisKeyPrefix = redisKeyPrefix;
		this.messageStoreKey = redisKeyPrefix + ".MESSAGE." + queueName;
		this.jedisCluster = jedisCluster;
		this.prefetchedIds = new LinkedBlockingQueue<>();
		realQueueName = getQueueName(queueName);
	}

	public String getQueueName() {
		return queueName;
	}

	@Override
	public List<String> push(final List<Message> messages) {
		List<String> messageIds = new ArrayList<String>();
		for (Message message : messages) {
			try {
				String json = om.writeValueAsString(message);
				jedisCluster.hset(messageStoreKey, message.getId(), json);
				double priority = message.getPriority() / 100;
				double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
				String realQueueName = getQueueName(queueName);
				jedisCluster.zadd(realQueueName, score, message.getId());
				messageIds.add(message.getId());
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}

		}
		return messageIds;

	}
	@Override
	public String push(Message message) {
			try {
				String json = om.writeValueAsString(message);
				jedisCluster.hset(messageStoreKey, message.getId(), json);
				double priority = message.getPriority() / 100;
				double score = Long.valueOf(System.currentTimeMillis() + message.getTimeout()).doubleValue() + priority;
				String realQueueName = getQueueName(queueName);
				jedisCluster.zadd(realQueueName, score, message.getId());
				return  message.getId();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			return null;

		}

	@Override
	public List<Message> peek(final int messageCount) {
		Set<String> ids = peekIds(0, messageCount);
		if (ids == null) {
			return Collections.emptyList();
		}
		List<Message> messages = new LinkedList<Message>();
		for (String id : ids) {
			String json = jedisCluster.hget(messageStoreKey, id);
			try {
				Message message = om.readValue(json, Message.class);
				messages.add(message);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return messages;
	}

	public Message peek() throws InterruptedException {
		lock.lockInterruptibly();
		try {
			String id = peekId();
			if (id == null) {
				available.await();
			} else {
				String json = jedisCluster.hget(messageStoreKey, id);
				Message message = om.readValue(json, Message.class);
				if (message == null) {
					return null;
				}
				long delay = System.currentTimeMillis() - message.getCreateTime() + message.getTimeout();
				if (delay <= 0)
					return message;
				else {
					available.await(delay, TimeUnit.MILLISECONDS);
				}
				return message;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} finally {
			available.signal();
			lock.unlock();
		}
		return null;

	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Message> pop(int messageCount, int wait, TimeUnit unit) throws Exception {
		if (messageCount < 1) {
			return Collections.emptyList();
		}
		Set<String> ids = new HashSet<>();
		if (prefetchedIds.size() < messageCount) {
			prefetch.addAndGet(messageCount - prefetchedIds.size());
			String id = prefetchedIds.poll(wait, unit);
			if (id != null) {
				ids.add(id);
			}
		}
		prefetchedIds.drainTo(ids, messageCount);
		if (ids.isEmpty()) {
			return Collections.emptyList();
		}
		return _pop(ids, messageCount);

	}

	AtomicInteger prefetch = new AtomicInteger(0);

	private void prefetchIds() {

		if (prefetch.get() < 1) {
			return;
		}

		int prefetchCount = prefetch.get();
		try {

			Set<String> ids = peekIds(0, prefetchCount);
			prefetchedIds.addAll(ids);
			prefetch.addAndGet((-1 * ids.size()));
			if (prefetch.get() < 0 || ids.isEmpty()) {
				prefetch.set(0);
			}
		} finally {
		}

	}

	private List<Message> _pop(Set<String> ids, int messageCount) throws Exception {

		double unackScore = Long.valueOf(System.currentTimeMillis() + unackTime).doubleValue();
		String unackQueueName = getUnackQueueName(queueName);
		List<Message> popped = new LinkedList<>();
		ZAddParams zParams = ZAddParams.zAddParams().nx();
		for (String msgId : ids) {

			long added = jedisCluster.zadd(unackQueueName, unackScore, msgId, zParams);
			if (added == 0) {
				continue;
			}

			long removed = jedisCluster.zrem(realQueueName, msgId);
			if (removed == 0) {
				continue;
			}

			String json = jedisCluster.hget(messageStoreKey, msgId);
			if (json == null) {

				continue;
			}

			Message msg = om.readValue(json, Message.class);
			popped.add(msg);

			if (popped.size() == messageCount) {
				return popped;
			}

		}

		return popped;

	}

	@Override
	public boolean ack(String messageId) {
		String unackShardKey = getUnackQueueName(queueName);
		Long removed = jedisCluster.zrem(unackShardKey, messageId);
		if (removed > 0) {
			jedisCluster.hdel(messageStoreKey, messageId);
			return true;
		}
		return false;

	}

	@Override
	public boolean setUnackTimeout(String messageId, long timeout) {
		double unackScore = Long.valueOf(System.currentTimeMillis() + timeout).doubleValue();
		String unackShardKey = getUnackQueueName(queueName);
		Double score = jedisCluster.zscore(unackShardKey, messageId);
		if (score != null) {
			jedisCluster.zadd(unackShardKey, unackScore, messageId);
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
	public boolean remove(String messageId) {
		String unackShardKey = getUnackQueueName(queueName);
		jedisCluster.zrem(unackShardKey, messageId);
		String realQueueName = getQueueName(queueName);
		Long removed = jedisCluster.zrem(realQueueName, messageId);
		Long msgRemoved = jedisCluster.hdel(messageStoreKey, messageId);
		if (removed > 0 && msgRemoved > 0) {
			return true;
		}
		return false;

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
		return jedisCluster.zcard(getQueueName(queueName));
	}

	@Override
	public void clear() {
		String realQueueName = getQueueName(queueName);
		String unackShard = getUnackQueueName(queueName);
		jedisCluster.del(realQueueName);
		jedisCluster.del(unackShard);
		jedisCluster.del(messageStoreKey);

	}

	private Set<String> peekIds(int offset, int count) {

		double now = Long.valueOf(System.currentTimeMillis() + 1).doubleValue();
		Set<String> scanned = jedisCluster.zrangeByScore(realQueueName, 0, now, offset, count);
		return scanned;

	}

	private String peekId() {
		double max = Long.valueOf(System.currentTimeMillis() + MAX_TIMEOUT).doubleValue();
		Set<String> scanned = jedisCluster.zrangeByScore(realQueueName, 0, max, 0, 1);
		if (scanned.size() > 0) {
			String messageId = scanned.toArray()[0].toString();
			setUnackTimeout(messageId, defaultTimeout);
			return messageId;
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


	private String getQueueName(String queueName) {
		return redisKeyPrefix + ".QUEUE." + queueName;
	}

	private String getUnackQueueName(String queueName) {
		return redisKeyPrefix + ".UNACK." + queueName;
	}

	private <R> R execute(Callable<R> r) {
		return executeWithRetry(executorService, r, 0);
	}

	private <R> R executeWithRetry(ExecutorService es, Callable<R> r, int retryCount) {

		try {

			return es.submit(r).get(1, TimeUnit.MINUTES);

		} catch (ExecutionException e) {

			if (retryCount < this.retryCount) {
				return executeWithRetry(es, r, ++retryCount);
			}
			throw new RuntimeException(e.getCause());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public int getUnackTime() {

		return 0;
	}

}
