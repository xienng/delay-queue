package com.meipian.queues.core;

public interface DelayQueue {

	/**
	 * 
	 * @return 消息队列名
	 */
	public String getName();
	
	/**
	 * 
	 * @return 当消息已经被取出，等待确认的时间。等待确认时间过去后将被自动删除
	 * @see #ack(String)
	 */
	public int getUnackTime();
	
	
	
	/**
	 * Provides an acknowledgement for the message.  Once ack'ed the message is removed from the queue forever.
	 * @param messageId ID of the message to be acknowledged  
	 * @return true if the message was found pending acknowledgement and is now ack'ed.  false if the message id is invalid or message is no longer present in the queue.
	 */
	public boolean ack(String messageId);
	
	/**
	 * Sets the unack timeout on the message (changes the default timeout to the new value).  Useful when extended lease is required for a message by consumer before sending ack.
	 * @param messageId ID of the message to be acknowledged
	 * @param timeout time in milliseconds for which the message will remain in un-ack state.  If no ack is received after the timeout period has expired, the message is put back into the queue
	 * @return true if the message id was found and updated with new timeout.  false otherwise.
	 */
	public boolean setUnackTimeout(String messageId, long timeout);
	
	
	/**
	 * Updates the timeout for the message.  
	 * @param messageId ID of the message to be acknowledged
	 * @param timeout time in milliseconds for which the message will remain invisible and not popped out of the queue.
	 * @return true if the message id was found and updated with new timeout.  false otherwise.
	 */
	public boolean setTimeout(String messageId, long timeout);
	

	
	
	/**
	 * 
	 * @param messageId message to be retrieved.
	 * @return Retrieves the message stored in the queue by the messageId.  Null if not found.
	 */
	public Message get(String messageId);
	
	/**
	 * 
	 * @return Size of the queue.
	 * @see #shardSizes()
	 */
	public long size();

	
	/**
	 *   清除整个消息队列
	 */
	public void clear();

	boolean push(Message message);
    /**
     * 开启消息队列监听
     */
	public void listen();
}
