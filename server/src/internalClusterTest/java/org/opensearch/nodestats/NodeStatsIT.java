/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nodestats;

import org.hamcrest.MatcherAssert;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Requests;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class NodeStatsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test_index";
    private static final Map<Integer, AtomicLong> expectedDocStatusCounter;

    static {
        expectedDocStatusCounter = new HashMap<>();
        expectedDocStatusCounter.put(200, new AtomicLong(0));
        expectedDocStatusCounter.put(201, new AtomicLong(0));
        expectedDocStatusCounter.put(404, new AtomicLong(0));
    }

    public void testNodeIndicesStatsBulk() {
        int sizeOfIndexRequests = scaledRandomIntBetween(10, 20);
        int sizeOfDeleteRequests = scaledRandomIntBetween(5, sizeOfIndexRequests);
        int sizeOfNotFountRequests = scaledRandomIntBetween(5, sizeOfIndexRequests);

        BulkRequest bulkRequest = new BulkRequest();

        for (int i = 0; i < sizeOfIndexRequests; ++i) {
            bulkRequest.add(new IndexRequest(INDEX).id(String.valueOf(i)).source(Requests.INDEX_CONTENT_TYPE, "field", "value"));
        }

        BulkResponse response = client().bulk(bulkRequest).actionGet();

        MatcherAssert.assertThat(response.hasFailures(), equalTo(false));
        MatcherAssert.assertThat(response.getItems().length, equalTo(sizeOfIndexRequests));

        for (BulkItemResponse itemResponse : response.getItems()) {
            expectedDocStatusCounter.get(itemResponse.getResponse().status().getStatus()).incrementAndGet();
        }

        refresh(INDEX);
        bulkRequest.requests().clear();

        for (int i = 0; i < sizeOfDeleteRequests; ++i) {
            bulkRequest.add(new DeleteRequest(INDEX, String.valueOf(i)));
        }
        for (int i = 0; i < sizeOfNotFountRequests; ++i) {
            bulkRequest.add(new DeleteRequest(INDEX, String.valueOf(25 + i)));
        }

        response = client().bulk(bulkRequest).actionGet();

        MatcherAssert.assertThat(response.hasFailures(), equalTo(false));
        MatcherAssert.assertThat(response.getItems().length, equalTo(sizeOfDeleteRequests + sizeOfNotFountRequests));

        for (BulkItemResponse itemResponse : response.getItems()) {
            expectedDocStatusCounter.get(itemResponse.getResponse().status().getStatus()).incrementAndGet();
        }

        refresh(INDEX);

        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().execute().actionGet();
        IndexingStats.Stats stats = nodesStatsResponse.getNodes().get(0).getIndices().getIndexing().getTotal();

        MatcherAssert.assertThat(stats.getIndexCount(), greaterThan(0L));
        MatcherAssert.assertThat(stats.getIndexTime().duration(), greaterThan(0L));
        MatcherAssert.assertThat(stats.getIndexCurrent(), notNullValue());
        MatcherAssert.assertThat(stats.getIndexFailedCount(), notNullValue());
        MatcherAssert.assertThat(stats.getDeleteCount(), greaterThan(0L));
        MatcherAssert.assertThat(stats.getDeleteTime().duration(), greaterThan(0L));
        MatcherAssert.assertThat(stats.getDeleteCurrent(), notNullValue());
        MatcherAssert.assertThat(stats.getNoopUpdateCount(), notNullValue());
        MatcherAssert.assertThat(stats.isThrottled(), notNullValue());
        MatcherAssert.assertThat(stats.getThrottleTime(), notNullValue());

        Map<Integer, AtomicLong> docStatusCounter = stats.getDocStatusStats().getDocStatusCounter();

        for (Integer key : docStatusCounter.keySet()) {
            MatcherAssert.assertThat(docStatusCounter.get(key).longValue(), equalTo(expectedDocStatusCounter.get(key).longValue()));
        }
    }

}
