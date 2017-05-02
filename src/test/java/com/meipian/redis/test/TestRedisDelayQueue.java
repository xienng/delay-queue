package com.meipian.redis.test;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.meipian.queues.core.Message;
import com.meipian.queues.redis.RedisDelayQueue;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

public class TestRedisDelayQueue {
	JedisCluster jedisCluster = null;

	@Before
	public void init() {
		String ip = "192.168.2.160";
		Set<HostAndPort> nodes = new HashSet<>();
		nodes.add(new HostAndPort(ip, 7701));
		nodes.add(new HostAndPort(ip, 7702));
		nodes.add(new HostAndPort(ip, 7703));
		nodes.add(new HostAndPort(ip, 7704));
		nodes.add(new HostAndPort(ip, 7705));
		nodes.add(new HostAndPort(ip, 7706));
		JedisPoolConfig pool = new JedisPoolConfig();
		pool.setMaxTotal(100);
		pool.setFairness(false);
		pool.setNumTestsPerEvictionRun(100);
		pool.setMaxWaitMillis(5000);
		pool.setTestOnBorrow(true);
		jedisCluster = new JedisCluster(nodes, 1000, 1000, 100,null, pool); // maxAttempt必须调大
		jedisCluster.set("test", "test");
		System.out.println(jedisCluster.get("test"));
		assertEquals("test", jedisCluster.get("test"));
	}

	@Test
	public void testCreate() {
		RedisDelayQueue queue = new RedisDelayQueue("com.meipian", "delayqueue", jedisCluster, 60 * 1000);
		Message message = new Message();
		message.setId("1234");
		message.setPayload("test");
		message.setPriority(0);
		message.setTimeout(10000);
		queue.push(message);
	}

}
