/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.decider;

import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.core.index.shard.ShardId;

public class InPlaceShardSplitAllocationDecider extends AllocationDecider {

    public static final String NAME = "in_place_shard_split";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        ShardId parentShardId = shardRouting.getSplittingShardId();

        if (parentShardId == null) {
            return super.canAllocate(shardRouting, node, allocation);
        }

        if (node != null && node.getByShardId(parentShardId) != null) {
            return allocation.decision(Decision.YES, NAME,
                "Found routing node [" + node.getByShardId(parentShardId) + "] for parent shard [" +
                    parentShardId + "] matching routing node [" + node + "]");
        }

        return allocation.decision(Decision.NO, NAME,
            "Parent Shard [" + parentShardId + "] is not assigned to the node [" + node + "]");
    }
}
