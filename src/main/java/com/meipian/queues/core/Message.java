package com.meipian.queues.core;

import java.util.concurrent.TimeUnit;

public class Message {

	private String id;
	private String payload;
	private long timeout;
	private int priority;
	private long createTime=System.currentTimeMillis();

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public int getPriority() {
		return priority;
	}

	public void setTimeout(long time, TimeUnit unit) {
		this.timeout = TimeUnit.MILLISECONDS.convert(time, unit);
	}

	public void setPriority(int priority) {
		if (priority < 0 || priority > 99) {
			throw new IllegalArgumentException("prioirty MUST be between 0 and 99 (inclusive)");
		}
		this.priority = priority;
	}

	public long getCreateTime() {
		return createTime;
	}


	/**
	 * equals 与hashCode由IDE自动生成
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "Message [id=" + id + ", payload=" + payload + ", timeout=" + timeout + ", priority=" + priority + "]";
	}

}
