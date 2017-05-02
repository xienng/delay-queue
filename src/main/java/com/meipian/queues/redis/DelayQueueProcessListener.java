package com.meipian.queues.redis;

import com.meipian.queues.core.Message;

public interface DelayQueueProcessListener {

	public void ackCallback(Message message);

	public void peekCallback(Message message);

	public void pushCallback(Message message);
}
