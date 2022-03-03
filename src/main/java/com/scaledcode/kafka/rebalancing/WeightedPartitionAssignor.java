package com.scaledcode.kafka.rebalancing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.CircularIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class WeightedPartitionAssignor extends AbstractPartitionAssignor {
    private static final int BITS_IN_BYTE = 8;
    private static final int BYTES_IN_INTEGER = Integer.SIZE / BITS_IN_BYTE;
    @Setter
    private static int weight = 100;
    private List<TopicPartition> memberAssignedPartitions;
    private Map<String, MemberAssignmentInfo> memberAssignmentInfo;

    @Override
    public void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
        memberAssignedPartitions = assignment.partitions();
        super.onAssignment(assignment, metadata);
    }

    @Override
    public ByteBuffer subscriptionUserData(Set<String> topics) {
        Map<String, List<Integer>> topicPartitions =  memberAssignedPartitions == null ? Collections.emptyMap()
                                                                                        : groupPartitionsByTopic(memberAssignedPartitions);
        byte[] assignmentBytes = serializeAssignment(topicPartitions);
        var size = assignmentBytes.length + (Integer.SIZE/BITS_IN_BYTE);
        ByteBuffer totalBuffer = ByteBuffer.allocate(size);
        totalBuffer.putInt(weight);
        totalBuffer.put(assignmentBytes);
        totalBuffer.flip();
        return totalBuffer;
    }

    private static Map<String, List<Integer>> groupPartitionsByTopic(List<TopicPartition> partitions) {
        Map<String, List<Integer>> topicPartitions = new HashMap<>();
        partitions.forEach(partition -> topicPartitions.computeIfAbsent(partition.topic(), k -> new ArrayList<>()).add(partition.partition()));
        return topicPartitions;
    }

    private static byte[] serializeAssignment(Map<String, List<Integer>> topicPartitions) {
        List<byte[]> assignmentBytes = new ArrayList<>();
        AtomicInteger size = new AtomicInteger(0);

        topicPartitions.forEach((topic, partitions) -> {
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            ByteBuffer topicBuffer = ByteBuffer.allocate(BYTES_IN_INTEGER + topicBytes.length);
            topicBuffer.putInt(topicBytes.length);
            topicBuffer.put(topicBytes);
            ByteBuffer partitionsBuffer = ByteBuffer.allocate(BYTES_IN_INTEGER + partitions.size() * BYTES_IN_INTEGER);
            partitionsBuffer.putInt(partitions.size());
            partitions.forEach(partitionsBuffer::putInt);

            byte[] partitionBytes = partitionsBuffer.array();
            byte[] topicBufferBytes = topicBuffer.array();
            assignmentBytes.add(topicBufferBytes);
            assignmentBytes.add(partitionBytes);

            size.addAndGet(topicBufferBytes.length + partitionBytes.length);
        });

        byte[] totalAssignmentBytes = new byte[size.get()];
        int offset = 0;
        for(var segment : assignmentBytes) {
            System.arraycopy(segment, 0, totalAssignmentBytes, offset, segment.length);
            offset += segment.length;
        }
        return totalAssignmentBytes;
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        memberAssignmentInfo = new HashMap<>();
        subscriptions.forEach((memberId, subscription) -> memberAssignmentInfo.put(memberId, getMemberAssignmentInfo(subscription.userData())));

        var topicMemberPortions = generateMemberTopicPortions(subscriptions, partitionsPerTopic);

        var partitionsToAssign = allPartitionsSorted(partitionsPerTopic, subscriptions);

        var assignments = subscriptions.keySet().stream()
                                       .collect(Collectors.toMap(item -> item,
                                                                 item -> (List<TopicPartition>) new ArrayList<TopicPartition>()));

        // Assign owned up to quota
        subscriptions.forEach((key, value) ->
                                      memberAssignmentInfo.get(key).getAssignments().forEach(topicPartition -> {
                                          if (topicMemberPortions.get(topicPartition.topic()).get(key) > 0 && partitionsToAssign.contains(topicPartition)) {
                                              assignments.get(key).add(topicPartition);
                                              partitionsToAssign.remove(topicPartition);
                                              topicMemberPortions.get(topicPartition.topic())
                                                                 .put(key, topicMemberPortions.get(topicPartition.topic()).get(key) - 1);
                                          }
                                      }));

        // Assign unassigned up to quota
        topicMemberPortions.forEach((topic, topicMemberPortion) ->
                                            topicMemberPortion.forEach((memberId, remainingToFill) -> {
                                                for (int i = 0; i < remainingToFill; i++) {
                                                    if (!partitionsToAssign.isEmpty()) {
                                                        assignments.get(memberId).add(partitionsToAssign.remove(0));
                                                    }
                                                }
                                            }));

        // Assign stragglers evenly
        if (!partitionsToAssign.isEmpty()) {
            CircularIterator<String> memberIterator = new CircularIterator<>(subscriptions.keySet());
            partitionsToAssign.forEach(topicPartition -> {
                assignments.get(memberIterator.next()).add(topicPartition);
            });
        }

        return assignments;
    }

    private static List<TopicPartition> allPartitionsSorted(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        SortedSet<String> topics = new TreeSet<>();

        subscriptions.forEach((memberId, subscription) -> topics.addAll(subscription.topics()));

        List<TopicPartition> allPartitions = new ArrayList<>();
        topics.forEach(topic -> allPartitions.addAll(AbstractPartitionAssignor.partitions(topic, partitionsPerTopic.get(topic))));

        return allPartitions;
    }

    private Map<String, Map<String, Integer>> generateMemberTopicPortions(Map<String, Subscription> subscriptions,
                                                                          Map<String, Integer> partitionsPerTopic) {
        Map<String, PortionBuilder> topicWeights = new HashMap<>();
        subscriptions.forEach((member, subscription) -> {
            int subscriptionWeight = memberAssignmentInfo.get(member).getWeight();
            subscription.topics()
                        .forEach(topic -> topicWeights.computeIfAbsent(topic, t -> new PortionBuilder())
                                                      .addTarget(member, subscriptionWeight));
        });

        return topicWeights.entrySet()
                           .stream()
                           .map(entry -> Map.entry(entry.getKey(), entry.getValue().build(partitionsPerTopic.get(entry.getKey()))))
                           .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static class PortionBuilder {
        private int totalWeight;
        private final List<MemberWeight> memberWeights = new ArrayList<>();

        public void addTarget(String memberId, int weight) {
            totalWeight += weight;
            memberWeights.add(new MemberWeight(memberId, weight));
        }

        public Map<String, Integer> build(int partitionCount) {
            Map<String, Integer> memberPortions = new HashMap<>();
            memberWeights.forEach(memberWeight -> {
                var portion = Math.floor(partitionCount * (memberWeight.weight / (totalWeight * 1.0)));
                memberPortions.put(memberWeight.memberId, (int) portion);
            });

            return memberPortions;
        }
    }

    @Data
    @AllArgsConstructor
    private static class MemberWeight {
        private String memberId;
        private long weight;
    }

    @Data
    @RequiredArgsConstructor
    private static class MemberAssignmentInfo {
        private final int weight;
        private final List<TopicPartition> assignments;
    }

    private static MemberAssignmentInfo getMemberAssignmentInfo(ByteBuffer userData) {
        //read weight
        int weight = userData.getInt();
        List<TopicPartition> assignedPartitions = new ArrayList<>();

        //read assigned partitions
        while(userData.hasRemaining()) {
            // Get topic name
            int topicNameSize = userData.getInt();
            byte[] topicNameBytes = new byte[topicNameSize];
            userData.get(topicNameBytes);
            String topicName = new String(topicNameBytes, StandardCharsets.UTF_8);
            // Get number of partitions
            int partitionCount = userData.getInt();
            // Read partition numbers
            for (int i=0; i<partitionCount; i++) {
                assignedPartitions.add(new TopicPartition(topicName, userData.getInt()));
            }
        }

        return new MemberAssignmentInfo(weight, assignedPartitions);
    }

    @Override
    public String name() {
        return "weighted-partition-assignor";
    }
}
