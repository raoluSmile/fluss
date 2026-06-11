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

package org.apache.fluss.metadata;

import org.apache.fluss.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Information of a physical table bucket, including replica assignment and leader/ISR state.
 *
 * @since 0.9
 */
@PublicEvolving
public class BucketInfo {
    private final TablePath tablePath;
    private final long tableId;
    private final @Nullable Long partitionId;
    private final @Nullable String partitionName;
    private final int bucketId;
    private final @Nullable Integer leaderId;
    private final @Nullable Integer leaderEpoch;
    private final List<Integer> replicas;
    private final List<Integer> isr;

    public BucketInfo(
            TablePath tablePath,
            long tableId,
            @Nullable Long partitionId,
            @Nullable String partitionName,
            int bucketId,
            @Nullable Integer leaderId,
            @Nullable Integer leaderEpoch,
            List<Integer> replicas,
            List<Integer> isr) {
        this.tablePath = checkNotNull(tablePath, "tablePath should not be null.");
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.partitionName = partitionName;
        this.bucketId = bucketId;
        this.leaderId = leaderId;
        this.leaderEpoch = leaderEpoch;
        this.replicas =
                Collections.unmodifiableList(
                        new ArrayList<>(checkNotNull(replicas, "replicas should not be null.")));
        this.isr =
                Collections.unmodifiableList(
                        new ArrayList<>(checkNotNull(isr, "isr should not be null.")));
    }

    public TablePath getTablePath() {
        return tablePath;
    }

    public long getTableId() {
        return tableId;
    }

    public OptionalLong getPartitionId() {
        return partitionId == null ? OptionalLong.empty() : OptionalLong.of(partitionId);
    }

    @Nullable
    public String getPartitionName() {
        return partitionName;
    }

    public int getBucketId() {
        return bucketId;
    }

    public OptionalInt getLeaderId() {
        return leaderId == null ? OptionalInt.empty() : OptionalInt.of(leaderId);
    }

    public OptionalInt getLeaderEpoch() {
        return leaderEpoch == null ? OptionalInt.empty() : OptionalInt.of(leaderEpoch);
    }

    public List<Integer> getReplicas() {
        return replicas;
    }

    public List<Integer> getIsr() {
        return isr;
    }

    @Override
    public String toString() {
        return "BucketInfo{"
                + "tablePath="
                + tablePath
                + ", tableId="
                + tableId
                + ", partitionId="
                + partitionId
                + ", partitionName='"
                + partitionName
                + '\''
                + ", bucketId="
                + bucketId
                + ", leaderId="
                + leaderId
                + ", leaderEpoch="
                + leaderEpoch
                + ", replicas="
                + replicas
                + ", isr="
                + isr
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BucketInfo that = (BucketInfo) o;
        return tableId == that.tableId
                && bucketId == that.bucketId
                && Objects.equals(tablePath, that.tablePath)
                && Objects.equals(partitionId, that.partitionId)
                && Objects.equals(partitionName, that.partitionName)
                && Objects.equals(leaderId, that.leaderId)
                && Objects.equals(leaderEpoch, that.leaderEpoch)
                && replicas.equals(that.replicas)
                && isr.equals(that.isr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tablePath,
                tableId,
                partitionId,
                partitionName,
                bucketId,
                leaderId,
                leaderEpoch,
                replicas,
                isr);
    }
}
