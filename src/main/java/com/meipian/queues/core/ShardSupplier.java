package com.meipian.queues.core;

import java.util.Set;

public interface ShardSupplier {
	/**
	 * 
	 * @return Provides the set of all the available queue shards.  The elements are evenly distributed amongst these shards
	 */
	public Set<String> getQueueShards();
	
	/**
	 * 
	 * @return Name of the current shard.  Used when popping elements out of the queue
	 */
	public String getCurrentShard();
}
