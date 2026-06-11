/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.utils;

import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_TABLE_ID;
import static org.apache.fluss.record.TestData.DATA1_TABLE_INFO;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ServerRpcMessageUtils}. */
class ServerRpcMessageUtilsTest {

    @Test
    void testUpdateMetadataRequestPreservesBucketIsr() {
        BucketMetadata tableBucketMetadata =
                new BucketMetadata(0, 1, 3, Arrays.asList(1, 2, 3), Arrays.asList(1, 3));
        TableMetadata tableMetadata =
                new TableMetadata(DATA1_TABLE_INFO, Collections.singletonList(tableBucketMetadata));

        BucketMetadata partitionBucketMetadata =
                new BucketMetadata(1, 2, 4, Arrays.asList(2, 3), Collections.singletonList(2));
        PartitionMetadata partitionMetadata =
                new PartitionMetadata(
                        DATA1_TABLE_ID,
                        "p0",
                        100L,
                        Collections.singletonList(partitionBucketMetadata));

        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(tableMetadata),
                        Collections.singletonList(partitionMetadata));

        ClusterMetadata decodedMetadata = getUpdateMetadataRequestData(request);

        assertThat(decodedMetadata.getTableMetadataList()).containsExactly(tableMetadata);
        assertThat(decodedMetadata.getPartitionMetadataList()).hasSize(1);
        PartitionMetadata decodedPartitionMetadata =
                decodedMetadata.getPartitionMetadataList().get(0);
        assertThat(decodedPartitionMetadata.getTableId()).isEqualTo(DATA1_TABLE_ID);
        assertThat(decodedPartitionMetadata.getPartitionName()).isEqualTo("p0");
        assertThat(decodedPartitionMetadata.getPartitionId()).isEqualTo(100L);
        assertThat(decodedPartitionMetadata.getBucketMetadataList())
                .containsExactly(partitionBucketMetadata);
    }

    @Test
    void testUpdateMetadataRequestPreservesBucketWithoutLeaderAndIsr() {
        BucketMetadata bucketMetadata = new BucketMetadata(0, null, null, Arrays.asList(1, 2, 3));
        TableMetadata tableMetadata =
                new TableMetadata(DATA1_TABLE_INFO, Collections.singletonList(bucketMetadata));

        UpdateMetadataRequest request =
                makeUpdateMetadataRequest(
                        null,
                        null,
                        Collections.emptySet(),
                        Collections.singletonList(tableMetadata),
                        Collections.emptyList());

        ClusterMetadata decodedMetadata = getUpdateMetadataRequestData(request);

        assertThat(decodedMetadata.getTableMetadataList()).hasSize(1);
        BucketMetadata decodedBucketMetadata =
                decodedMetadata.getTableMetadataList().get(0).getBucketMetadataList().get(0);
        assertThat(decodedBucketMetadata.getBucketId()).isEqualTo(0);
        assertThat(decodedBucketMetadata.getLeaderId().isPresent()).isFalse();
        assertThat(decodedBucketMetadata.getLeaderEpoch().isPresent()).isFalse();
        assertThat(decodedBucketMetadata.getReplicas()).containsExactly(1, 2, 3);
        assertThat(decodedBucketMetadata.getIsr()).isEmpty();
    }
}
