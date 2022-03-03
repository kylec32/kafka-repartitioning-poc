package com.scaledcode.kafka.rebalancing;

public class Runner {
    public static void main(String[] args) {
        if (args[0].equals("producer")) {
            new Producer(args[1], Integer.parseInt(args[2])).run();
        } else {
            if (args.length == 5) {
                WeightedPartitionAssignor.setWeight(Integer.parseInt(args[4]));
            }
            new Consumer(args[1], args[2], Integer.parseInt(args[3])).run();
        }
//        Admin admin = Admin.create(ConfigurationHolder.producerConfiguration());
//        var result = admin.createTopics(Collections.singletonList(new NewTopic("test-topic", 5, (short) 1))).values().get("test-topic");
//        System.out.println(result);
    }
}
