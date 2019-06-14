// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.tsd;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Simple partitioner class. It assumes that our key is a positive integer
 * based on the hash code. If you pass in a string that can't parse to a Long,
 * then the partitioner will throw an exception.
 */
public class KafkaSimplePartitioner implements Partitioner {

    @Override
    public int partition(String s, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int num_partitions = cluster.partitionCountForTopic(s);
        return (int) (Long.parseLong((String) key) % num_partitions);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
