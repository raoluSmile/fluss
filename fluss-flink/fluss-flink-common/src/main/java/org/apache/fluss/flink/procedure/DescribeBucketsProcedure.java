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

package org.apache.fluss.flink.procedure;

import org.apache.fluss.metadata.BucketInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.ProcedureHint;
import org.apache.flink.table.procedure.ProcedureContext;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Procedure to describe bucket metadata for a table.
 *
 * <p>Usage examples:
 *
 * <pre>
 * CALL sys.describe_buckets('db.table');
 * CALL sys.describe_buckets('db.table', 'partition_key=partition_value');
 * </pre>
 */
public class DescribeBucketsProcedure extends ProcedureBase {

    private static final String OUTPUT_TYPE =
            "ROW<table_path STRING, table_id BIGINT, partition_id BIGINT, "
                    + "partition_name STRING, bucket_id INT, leader_id INT, leader_epoch INT, "
                    + "replicas ARRAY<INT>, isr ARRAY<INT>>";

    @ProcedureHint(
            argument = {@ArgumentHint(name = "table_path", type = @DataTypeHint("STRING"))},
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String tablePath) throws Exception {
        return describeBuckets(tablePath, null);
    }

    @ProcedureHint(
            argument = {
                @ArgumentHint(name = "table_path", type = @DataTypeHint("STRING")),
                @ArgumentHint(name = "partition_spec", type = @DataTypeHint("STRING"))
            },
            output = @DataTypeHint(OUTPUT_TYPE))
    public Row[] call(ProcedureContext context, String tablePath, String partitionSpec)
            throws Exception {
        return describeBuckets(tablePath, partitionSpec);
    }

    private Row[] describeBuckets(String tablePath, @Nullable String partitionSpec)
            throws Exception {
        TablePath parsedTablePath = parseTablePath(tablePath);
        List<BucketInfo> bucketInfos =
                partitionSpec == null || partitionSpec.trim().isEmpty()
                        ? admin.describeBuckets(parsedTablePath).get()
                        : admin.describeBuckets(parsedTablePath, parsePartitionSpec(partitionSpec))
                                .get();
        return bucketInfos.stream().map(DescribeBucketsProcedure::toRow).toArray(Row[]::new);
    }

    private static Row toRow(BucketInfo bucketInfo) {
        return Row.of(
                bucketInfo.getTablePath().toString(),
                bucketInfo.getTableId(),
                optionalLong(bucketInfo.getPartitionId()),
                bucketInfo.getPartitionName(),
                bucketInfo.getBucketId(),
                optionalInt(bucketInfo.getLeaderId()),
                optionalInt(bucketInfo.getLeaderEpoch()),
                bucketInfo.getReplicas().toArray(new Integer[0]),
                bucketInfo.getIsr().toArray(new Integer[0]));
    }

    private static Long optionalLong(OptionalLong value) {
        return value.isPresent() ? value.getAsLong() : null;
    }

    private static Integer optionalInt(OptionalInt value) {
        return value.isPresent() ? value.getAsInt() : null;
    }

    private static TablePath parseTablePath(String tablePath) {
        if (tablePath == null || tablePath.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "table_path cannot be null or empty. Expected format is 'database.table'.");
        }

        String[] parts = tablePath.trim().split("\\.", -1);
        if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid table_path '" + tablePath + "'. Expected format is 'database.table'.");
        }
        return TablePath.of(parts[0], parts[1]);
    }

    private static PartitionSpec parsePartitionSpec(String partitionSpec) {
        ResolvedPartitionSpec resolvedPartitionSpec =
                ResolvedPartitionSpec.fromPartitionQualifiedName(partitionSpec.trim());
        Map<String, String> spec = new LinkedHashMap<>();
        for (int i = 0; i < resolvedPartitionSpec.getPartitionKeys().size(); i++) {
            spec.put(
                    resolvedPartitionSpec.getPartitionKeys().get(i),
                    resolvedPartitionSpec.getPartitionValues().get(i));
        }
        return new PartitionSpec(spec);
    }
}
