#include "helpers.h"

using namespace NKafkaRdkafkaTests;

Y_UNIT_TEST_SUITE(KafkaLibrdkafkaTransactions) {
    Y_UNIT_TEST(CommitIsVisible) {
        const TString topic = UniqueName("rdk-txn-commit");
        CreateYdbTopic(topic, 1);

        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "committed-value", "committed-key");
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit_transaction");
        UNIT_ASSERT_VALUES_EQUAL(producer->Dr.Fail.load(), 0);
        UNIT_ASSERT_GT(producer->Dr.Ok.load(), 0);

        auto consumer = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto payloads = ConsumePayloads(*consumer->Handle, 1);
        UNIT_ASSERT_VALUES_EQUAL(payloads[0], "committed-value");
    }

    Y_UNIT_TEST(AbortIsNotVisible) {
        const TString topic = UniqueName("rdk-txn-abort");
        CreateYdbTopic(topic, 1);

        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "aborted-value", "aborted-key");
        AssertTxnOk(producer->Handle->abort_transaction(30000), "abort_transaction");

        auto consumer = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(
            std::string(topic),
            0,
            RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(consumer->Handle->assign(partitions), "assign");
        RdKafka::TopicPartition::destroy(partitions);
        ConsumeUntilEmpty(*consumer->Handle, TDuration::Seconds(3));
    }
}
