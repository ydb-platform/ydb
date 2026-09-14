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

    Y_UNIT_TEST(InFlightNotVisibleToReadCommitted) {
        const TString topic = UniqueName("rdk-txn-inflight");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "inflight");
        producer->Handle->poll(1000);

        auto committed = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        std::vector<RdKafka::TopicPartition*> partitions;
        partitions.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(committed->Handle->assign(partitions), "assign committed");
        RdKafka::TopicPartition::destroy(partitions);
        ConsumeUntilEmpty(*committed->Handle, TDuration::Seconds(2));

        auto uncommitted = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_uncommitted"}});
        std::vector<RdKafka::TopicPartition*> uncommittedParts;
        uncommittedParts.push_back(RdKafka::TopicPartition::create(std::string(topic), 0, RdKafka::Topic::OFFSET_BEGINNING));
        AssertRdKafkaOk(uncommitted->Handle->assign(uncommittedParts), "assign uncommitted");
        RdKafka::TopicPartition::destroy(uncommittedParts);
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(3);
        while (TInstant::Now() < deadline) {
            std::unique_ptr<RdKafka::Message> message(uncommitted->Handle->consume(500));
            UNIT_ASSERT(message);
            if (message->err() == RdKafka::ERR_NO_ERROR) {
                UNIT_ASSERT_VALUES_EQUAL(MessagePayload(*message), "inflight");
                break;
            }
            if (message->err() != RdKafka::ERR__TIMED_OUT) {
                UNIT_FAIL("read_uncommitted consume failed: " << RdKafka::err2str(message->err()));
            }
        }

        AssertTxnOk(producer->Handle->abort_transaction(30000), "abort_transaction");
    }

    Y_UNIT_TEST(SeveralPartitionsInOneTxn) {
        const TString topic = UniqueName("rdk-txn-parts");
        constexpr ui32 partitions = 3;
        CreateYdbTopic(topic, partitions);
        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*producer->Handle, topic, partitions);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        for (ui32 partition = 0; partition < partitions; ++partition) {
            Produce(
                *producer->Handle,
                topic,
                TStringBuilder() << "txn-" << partition,
                TStringBuilder() << "k-" << partition,
                static_cast<int32_t>(partition));
        }
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit_transaction");

        auto consumer = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        auto messages = ConsumeMessages(*consumer->Handle, partitions);
        THashSet<TString> payloads;
        for (const auto& message : messages) {
            payloads.insert(message.Payload);
        }
        UNIT_ASSERT(payloads.contains("txn-0"));
        UNIT_ASSERT(payloads.contains("txn-1"));
        UNIT_ASSERT(payloads.contains("txn-2"));
    }

    Y_UNIT_TEST(EmptyTxn) {
        const TString topic = UniqueName("rdk-txn-empty");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit empty");

        AssertTxnOk(producer->Handle->begin_transaction(), "begin after empty");
        Produce(*producer->Handle, topic, "after-empty");
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit after empty");

        auto consumer = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "after-empty");
    }

    Y_UNIT_TEST(SendOffsetsToTransaction) {
        const TString input = UniqueName("rdk-txn-off-in");
        const TString output = UniqueName("rdk-txn-off-out");
        CreateYdbTopic(input, 1);
        CreateYdbTopic(output, 1);

        auto source = MakeProducer();
        WaitTopicPartitions(*source->Handle, input, 1);
        WaitTopicPartitions(*source->Handle, output, 1);
        ProduceAndFlush(*source, input, {"in-0", "in-1"});

        const TString group = UniqueName("txn-off-group");
        auto consumer = MakeConsumer(group);
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(input)}), "subscribe input");
        ConsumeMessages(*consumer->Handle, 1);

        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
        });
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, output, "out-0");
        const TInstant delivered = TInstant::Now() + TDuration::Seconds(10);
        while (producer->Dr.Ok.load() == 0 && TInstant::Now() < delivered) {
            producer->Handle->poll(200);
        }

        std::unique_ptr<RdKafka::ConsumerGroupMetadata> meta(consumer->Handle->groupMetadata());
        UNIT_ASSERT(meta);
        std::vector<RdKafka::TopicPartition*> offsets;
        offsets.push_back(RdKafka::TopicPartition::create(std::string(input), 0));
        AssertRdKafkaOk(consumer->Handle->position(offsets), "position");
        AssertTxnOk(
            producer->Handle->send_offsets_to_transaction(offsets, meta.get(), 30000),
            "send_offsets_to_transaction");
        RdKafka::TopicPartition::destroy(offsets);
        AssertTxnOk(producer->Handle->commit_transaction(30000), "commit_transaction");
        consumer.reset();

        auto outputConsumer = MakeConsumer(UniqueName("out-group"), {{"isolation.level", "read_committed"}});
        AssertRdKafkaOk(outputConsumer->Handle->subscribe({std::string(output)}), "subscribe output");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*outputConsumer->Handle, 1)[0], "out-0");

        auto next = MakeConsumer(group);
        AssertRdKafkaOk(next->Handle->subscribe({std::string(input)}), "subscribe next");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*next->Handle, 1)[0], "in-1");
    }

    Y_UNIT_TEST(Fencing) {
        const TString topic = UniqueName("rdk-txn-fence");
        CreateYdbTopic(topic, 1);
        const TString transactionalId = UniqueName("txn-fence");

        auto first = MakeProducer({
            {"transactional.id", transactionalId},
            {"enable.idempotence", "true"},
        });
        WaitTopicPartitions(*first->Handle, topic, 1);
        AssertTxnOk(first->Handle->init_transactions(60000), "init first");
        AssertTxnOk(first->Handle->begin_transaction(), "begin first");
        Produce(*first->Handle, topic, "from-first");
        first->Handle->poll(500);

        auto second = MakeProducer({
            {"transactional.id", transactionalId},
            {"enable.idempotence", "true"},
        });
        AssertTxnOk(second->Handle->init_transactions(60000), "init second");
        AssertTxnOk(second->Handle->begin_transaction(), "begin second");
        Produce(*second->Handle, topic, "from-second");
        AssertTxnOk(second->Handle->commit_transaction(30000), "commit second");

        RdKafka::Error* commitErr = first->Handle->commit_transaction(15000);
        UNIT_ASSERT_C(commitErr, "first producer must be fenced");
        const auto code = commitErr->code();
        const TString name(commitErr->name().c_str());
        delete commitErr;
        UNIT_ASSERT_C(
            code == RdKafka::ERR_PRODUCER_FENCED
                || code == RdKafka::ERR_INVALID_PRODUCER_EPOCH
                || name.Contains("FENCE") || name.Contains("EPOCH"),
            name);

        auto consumer = MakeConsumer(UniqueName("group"), {{"isolation.level", "read_committed"}});
        AssertRdKafkaOk(consumer->Handle->subscribe({std::string(topic)}), "subscribe");
        UNIT_ASSERT_VALUES_EQUAL(ConsumePayloads(*consumer->Handle, 1)[0], "from-second");
    }

    Y_UNIT_TEST(TransactionTimeoutFencesProducer) {
        const TString topic = UniqueName("rdk-txn-ttl");
        CreateYdbTopic(topic, 1);
        auto producer = MakeProducer({
            {"transactional.id", UniqueName("txn")},
            {"enable.idempotence", "true"},
            {"transaction.timeout.ms", "8000"},
            {"socket.timeout.ms", "4000"},
        });
        WaitTopicPartitions(*producer->Handle, topic, 1);
        AssertTxnOk(producer->Handle->init_transactions(60000), "init_transactions");
        AssertTxnOk(producer->Handle->begin_transaction(), "begin_transaction");
        Produce(*producer->Handle, topic, "will-timeout");
        Sleep(TDuration::Seconds(12));
        RdKafka::Error* err = producer->Handle->commit_transaction(15000);
        UNIT_ASSERT_C(err, "commit after transaction timeout must fail");
        const auto code = err->code();
        const TString name(err->name().c_str());
        delete err;
        UNIT_ASSERT_C(
            code == RdKafka::ERR_PRODUCER_FENCED
                || code == RdKafka::ERR_INVALID_PRODUCER_EPOCH
                || code == RdKafka::ERR_INVALID_TXN_STATE
                || name.Contains("FENCE") || name.Contains("TIMEOUT") || name.Contains("EPOCH"),
            name);
    }
}
