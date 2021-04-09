/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.opensearch.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class QueryShardExceptionTests extends OpenSearchTestCase {

    public void testCreateFromQueryShardContext() {
        String indexUuid = randomAlphaOfLengthBetween(5, 10);
        String clusterAlias = randomAlphaOfLengthBetween(5, 10);
        QueryShardContext queryShardContext = QueryShardContextTests.createQueryShardContext(indexUuid, clusterAlias);
        {
            QueryShardException queryShardException = new QueryShardException(queryShardContext, "error");
            assertThat(queryShardException.getIndex().getName(), equalTo(clusterAlias + ":index"));
            assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
        }
        {
            QueryShardException queryShardException = new QueryShardException(queryShardContext, "error", new IllegalArgumentException());
            assertThat(queryShardException.getIndex().getName(), equalTo(clusterAlias + ":index"));
            assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
        }
    }

    public void testCreateFromIndex() {
        String indexUuid = randomAlphaOfLengthBetween(5, 10);
        String indexName = randomAlphaOfLengthBetween(5, 10);
        Index index = new Index(indexName, indexUuid);
        QueryShardException queryShardException = new QueryShardException(index, "error", new IllegalArgumentException());
        assertThat(queryShardException.getIndex().getName(), equalTo(indexName));
        assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
    }
}
