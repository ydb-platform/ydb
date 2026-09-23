#include <ydb/tests/functional/kafka/test_common/helpers.h>
#include <ydb/tests/functional/federation_test/common_functions.h>

using namespace NKafkaRdkafkaTests;
using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NFederationTests;

Y_UNIT_TEST_SUITE(LibrdkafkaFederationCompatibilityTests) {
    Y_UNIT_TEST(CheckWriteReadKafkaOnFederation) {
        TClusterEndpoints env;
        TString prodCMDatabasePath = "/logbroker-federation/prod";
        const TString topic = UniqueName("rdk-topic");
        const TString topicFullPath = "/Root" + prodCMDatabasePath + "/" + topic;
        CreateYdbTopic(topic, 1, env.EndpointCM, prodCMDatabasePath);

        const THashMap<TString, TString> kafkaConf = {
            {"security.protocol", "SASL_PLAINTEXT"},
            {"sasl.mechanisms", "PLAIN"},
            {"sasl.username", "root@/Root" + prodCMDatabasePath},
            {"sasl.password", "1234"},
        };
        auto producer = MakeProducer(kafkaConf);
        WaitTopicPartitions(*producer->Handle, topic, 1, TDuration::Seconds(60));
        ProduceAndFlush(*producer, topic, {"message1", "message2", "message3"}, {"key1", "key2", "key3"});

        auto consumer = MakeConsumer(UniqueName("group"), kafkaConf);
        // AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe full");
        std::vector<RdKafka::TopicPartition*> tps;
        tps.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(consumer->Handle->assign(tps), "assign");
        RdKafka::TopicPartition::destroy(tps);
        const auto first = ConsumeMessages(*consumer->Handle, 3);
        for (size_t i = 1; i < 4; i++) {
            UNIT_ASSERT_VALUES_EQUAL(first[i - 1].Payload, "message" + std::to_string(i));
            UNIT_ASSERT_VALUES_EQUAL(first[i - 1].Key, "key" + std::to_string(i));
        }
    }

    Y_UNIT_TEST(TransactionCommitIsVisibleOnFederation) {
        TClusterEndpoints env;
        const TString prodCMDatabasePath = "/logbroker-federation/prod";
        const TString topic = UniqueName("rdk-txn-commit");
        CreateYdbTopic(topic, 1, env.EndpointCM, prodCMDatabasePath);

        const THashMap<TString, TString> kafkaConf = {
            {"security.protocol", "SASL_PLAINTEXT"},
            {"sasl.mechanisms", "PLAIN"},
            {"sasl.username", "root@/Root" + prodCMDatabasePath},
            {"sasl.password", "1234"},
        };

        THashMap<TString, TString> producerConf = kafkaConf;
        producerConf["transactional.id"] = UniqueName("txn");
        producerConf["enable.idempotence"] = "true";
        auto producer = MakeProducer(producerConf);
        Cerr << TInstant::Now() << " WaitTopicPartitions" << Endl;
        WaitTopicPartitions(*producer->Handle, topic, 1, TDuration::Seconds(60));
        Cerr << TInstant::Now() << " Starting init_transactions" << Endl;
        AssertTxnOk(producer->Handle->init_transactions(6000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "committed-value-1", "committed-key-1");
        Produce(*producer->Handle, topic, "committed-value-2", "committed-key-2");
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit_transaction");

        THashMap<TString, TString> consumerConf = kafkaConf;
        consumerConf["isolation.level"] = "read_committed";
        auto consumer = MakeConsumer(UniqueName("group"), consumerConf);
        std::vector<RdKafka::TopicPartition*> tps;
        tps.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(consumer->Handle->assign(tps), "assign");
        RdKafka::TopicPartition::destroy(tps);
        // AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        const auto messages = ConsumeMessages(*consumer->Handle, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Payload, "committed-value-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[0].Key, "committed-key-1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].Payload, "committed-value-2");
        UNIT_ASSERT_VALUES_EQUAL(messages[1].Key, "committed-key-2");
    }

    Y_UNIT_TEST(TransactionAbortIsNotVisibleOnFederation) {
        TClusterEndpoints env;
        const TString prodCMDatabasePath = "/logbroker-federation/prod";
        const TString topic = UniqueName("rdk-txn-abort");
        CreateYdbTopic(topic, 1, env.EndpointCM, prodCMDatabasePath);

        const THashMap<TString, TString> kafkaConf = {
            {"security.protocol", "SASL_PLAINTEXT"},
            {"sasl.mechanisms", "PLAIN"},
            {"sasl.username", "root@/Root" + prodCMDatabasePath},
            {"sasl.password", "1234"},
        };

        THashMap<TString, TString> producerConf = kafkaConf;
        producerConf["transactional.id"] = UniqueName("txn");
        producerConf["enable.idempotence"] = "true";
        auto producer = MakeProducer(producerConf);
        WaitTopicPartitions(*producer->Handle, topic, 1, TDuration::Seconds(60));
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "aborted-value", "aborted-key");
        AssertTxnOk(producer->Handle->abort_transaction(30000), "abort_transaction");

        THashMap<TString, TString> consumerConf = kafkaConf;
        consumerConf["isolation.level"] = "read_committed";
        auto consumer = MakeConsumer(UniqueName("group"), consumerConf);
        ConsumeUntilEmpty(*consumer->Handle, TDuration::Seconds(3));
    }
}
