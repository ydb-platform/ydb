#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

static inline IOutputStream& operator<<(IOutputStream& o, const std::set<size_t> t) {
    o << "[" << JoinRange(", ", t.begin(), t.end()) << "]";

    return o;
}

/*
static inline IOutputStream& operator<<(IOutputStream& o, const std::optional<std::set<size_t>> t) {
    if (t) {
        o << t.value();
    } else {
        o << "[empty]";
    }

    return o;
}
*/

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TopicSplitMerge) {

    Y_UNIT_TEST(Simple) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic();

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");

        TTestReadSession ReadSession(client, 2);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionSplit) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        TTestReadSession ReadSession(client, 2, false);

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.2", 3)));

        Sleep(TDuration::Seconds(1)); // Wait read session events

        UNIT_ASSERT_EQUAL_C(1, ReadSession.Partitions.size(), "We are reading only one partitions because offset is not commited");
        ReadSession.Commit();

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT(1 == info.PartitionId || 2 == info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionSplit_PreferedPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");
        auto writeSession3 = CreateWriteSession(client, "producer-3", 0);

        TTestReadSession ReadSession(client, 6);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 1)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        writeSession1->Write(Msg("message_1.2_2", 2)); // Will be ignored because duplicated SeqNo
        writeSession3->Write(Msg("message_3.2", 11)); // Will be fail because partition is not writable after split
        writeSession1->Write(Msg("message_1.2", 5));
        writeSession2->Write(Msg("message_2.2", 7));

        auto writeSettings4 = TWriteSessionSettings()
                        .Path(TEST_TOPIC)
                        .DeduplicationEnabled(false)
                        .PartitionId(1);
        auto writeSession4 = client.CreateSimpleBlockingWriteSession(writeSettings4);
        writeSession4->Write(TWriteMessage("message_4.1"));

        ReadSession.WaitAllMessages();

        Cerr << ">>>>> All messages received" << Endl;

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_EQUAL(5, info.SeqNo);
            } else if (info.Data == "message_2.2") {
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_EQUAL(7, info.SeqNo);
            } else if (info.Data == "message_3.1") {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(1, info.SeqNo);
            } else if (info.Data == "message_4.1") {
                UNIT_ASSERT_C(1, info.PartitionId);
                UNIT_ASSERT_C(1, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1", 0);
        auto writeSession2 = CreateWriteSession(client, "producer-2", 1);

        TTestReadSession ReadSession(client, 3);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        ui64 txId = 1012;
        MergePartition(setup, ++txId, 0, 1);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.2", 5))); // Will be fail because partition is not writable after merge
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.2", 7))); // Will be fail because partition is not writable after merge

        auto writeSession3 = CreateWriteSession(client, "producer-2", 2);

        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 2)));  // Will be ignored because duplicated SeqNo
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.2", 11)));

        ReadSession.WaitAllMessages();

        for(const auto& info : ReadSession.ReceivedMessages) {
            if (info.Data == TString("message_1.1")) {
                UNIT_ASSERT_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_EQUAL(2, info.SeqNo);
            } else if (info.Data == TString("message_2.1")) {
                UNIT_ASSERT_EQUAL(1, info.PartitionId);
                UNIT_ASSERT_EQUAL(3, info.SeqNo);
            } else if (info.Data == TString("message_3.2")) {
                UNIT_ASSERT_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_EQUAL(11, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
    }

    using TOffsets = std::unordered_map<ui32, ui64>;

    struct TTestPartitionReadSession {
        TOffsets Offsets;
        TString Name;

        std::shared_ptr<IReadSession> Session;


        std::set<size_t> Partitions;

        std::optional<std::set<size_t>> ExpectedPartitions;
        NThreading::TPromise<std::set<size_t>> Promise;

        TMutex Lock;
        TSemaphore Semaphore;

        static constexpr size_t SemCount = 1;

        void Acquire() {
            Cerr << ">>>>> " << Name << " Acquire()" << Endl;
            Semaphore.Acquire();
        }

        void Release() {
            Cerr << ">>>>> " << Name << " Release()" << Endl;
            Semaphore.Release();
        }

        TTestPartitionReadSession(const TString& name, TTopicClient& client, bool commitMessages = false, std::optional<ui32> partition = std::nullopt)
            : Name(name)
            , Semaphore(name.c_str(), SemCount) {

            Y_UNUSED(commitMessages);

            Acquire();

            auto readSettings = TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC);
            if (partition) {
                readSettings.Topics_[0].AppendPartitionIds(partition.value());
            }

            readSettings.EventHandlers_.StartPartitionSessionHandler(
                    [&]
                    (TReadSessionEvent::TStartPartitionSessionEvent& ev) mutable {
                        Cerr << ">>>>> " << Name << " Received TStartPartitionSessionEvent message " << ev.DebugString() << Endl;
                        auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                        Modify([&](std::set<size_t>& s) { s.insert(partitionId); });
                        if (Offsets.contains(partitionId)) {
                            Cerr << ">>>>> " << Name << " Start reading partition " << partitionId << " from offset " << Offsets[partitionId] << Endl;
                            ev.Confirm(Offsets[partitionId], TMaybe<ui64>());
                        } else {
                            Cerr << ">>>>> " << Name << " Start reading partition " << partitionId << " without offset" << Endl;
                            ev.Confirm();
                        }
            });

            readSettings.EventHandlers_.StopPartitionSessionHandler(
                    [&]
                    (TReadSessionEvent::TStopPartitionSessionEvent& ev) mutable {
                        Cerr << ">>>>> " << Name << " Received TStopPartitionSessionEvent message " << ev.DebugString() << Endl;
                        auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                        Modify([&](std::set<size_t>& s) { s.erase(partitionId); });
                        Cerr << ">>>>> " << Name << " Stop reading partition " << partitionId << Endl;
                        ev.Confirm();
            });

            readSettings.EventHandlers_.PartitionSessionClosedHandler(
                    [&]
                    (TReadSessionEvent::TPartitionSessionClosedEvent& ev) mutable {
                        Cerr << ">>>>> " << Name << " Received TPartitionSessionClosedEvent message " << ev.DebugString() << Endl;
                        auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                        Modify([&](std::set<size_t>& s) { s.erase(partitionId); });
                        Cerr << ">>>>> " << Name << " Stop (closed) reading partition " << partitionId << Endl;
            });

            readSettings.EventHandlers_.SessionClosedHandler(
                    [&]
                    (const TSessionClosedEvent& ev) mutable {
                        Cerr << ">>>>> " << Name << " Received TSessionClosedEvent message " << ev.DebugString() << Endl;
                        //auto partitionId = ev.GetPartitionSession()->GetPartitionId();
                        //Modify([&](std::set<size_t>& s) { s.erase(partitionId); });
                        //Cerr << ">>>>> " << Name << " Stop reading partition " << partitionId << Endl;
            });


            readSettings.EventHandlers_.DataReceivedHandler(
                    [=]
                    (TReadSessionEvent::TDataReceivedEvent& ev) mutable {
                    auto& messages = ev.GetMessages();
                    for (size_t i = 0u; i < messages.size(); ++i) {
                        auto& message = messages[i];

                        Cerr << ">>>>> " << name << " Received TDataReceivedEvent message partitionId=" << message.GetPartitionSession()->GetPartitionId()
                                << ", message=" << message.GetData()
                                << ", seqNo=" << message.GetSeqNo()
                                << ", offset=" << message.GetOffset()
                                << Endl;

                        if (commitMessages) {
                            message.Commit();
                        }
                    }
            });

            Session = client.CreateReadSession(readSettings);
        }

        NThreading::TFuture<std::set<size_t>> Wait(std::set<size_t> partitions, const TString& message) {
            Cerr << ">>>>> " << Name << " Wait partitions " << partitions << " " << message << Endl;

            with_lock (Lock) {
                ExpectedPartitions = partitions;
                Promise = NThreading::NewPromise<std::set<size_t>>();

                if (Partitions == ExpectedPartitions.value()) {
                    Promise.SetValue(ExpectedPartitions.value());
                }
            }

            return Promise.GetFuture();
        }

        void Assert(const std::set<size_t>& expected, NThreading::TFuture<std::set<size_t>> f, const TString& message) {
            Cerr << ">>>>> " << Name << " Partitions " << Partitions << " received #2" << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(expected, f.HasValue() ? f.GetValueSync() : Partitions, message);
            Release();
        }

        void WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message) {
            auto f = Wait(partitions, message);
            f.Wait(TDuration::Seconds(5));
            Assert(partitions, f, message);
        }

        void Run() {
            ExpectedPartitions = std::nullopt;
            Semaphore.TryAcquire();
            Release();
        }

        void Close() {
            Run();
            Session->Close();
        }

    private:
        void Modify(std::function<void (std::set<size_t>&)> modifier) {
            bool found = false;

            with_lock (Lock) {
                modifier(Partitions);

                if (ExpectedPartitions && Partitions == ExpectedPartitions.value()) {
                    ExpectedPartitions = std::nullopt;
                    Promise.SetValue(Partitions);
                    found = true;
                }
            }

            if (found) {
                Acquire();
            }
        }
    };

    Y_UNIT_TEST(PartitionSplit_ReadEmptyPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();
        TTestPartitionReadSession readSession("session-0", client);

        readSession.WaitAndAssertPartitions({0}, "Must read all exists partitions");

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        readSession.WaitAndAssertPartitions({0, 1, 2}, "After split must read all partitions because parent partition is empty");

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ReadNotEmptyPartitions) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();
        TTestPartitionReadSession readSession("Session-0", client);

        auto writeSession = CreateWriteSession(client, "producer-1", 0);

        readSession.WaitAndAssertPartitions({0}, "Must read all exists partitions");

        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        readSession.WaitAndAssertPartitions({0}, "After split must read only 0 partition because had been read not from the end of partition");
        readSession.WaitAndAssertPartitions({}, "Partition must be released for secondary read after 1 second");
        readSession.WaitAndAssertPartitions({0}, "Must secondary read for check read from end");
        readSession.WaitAndAssertPartitions({}, "Partition must be released for secondary read because start not from the end of partition after 2 seconds");

        readSession.Offsets[0] = 1;
        readSession.WaitAndAssertPartitions({0, 1, 2}, "Must read from all partitions because had been read from the end of partition");

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        TTestPartitionReadSession readSession1("Session-0", client);
        readSession1.Offsets[0] = 1;
        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because read the partition 0 from offset 1");

        TTestPartitionReadSession readSession2("Session-1", client, false, 0);
        readSession2.Offsets[0] = 0;

        auto p1 = readSession1.Wait({}, "Must release all partitions becase readSession2 read not from EndOffset");
        auto p2 = readSession2.Wait({0}, "Must read partition 0 because it defined in the readSession");

        p1.Wait(TDuration::Seconds(5));
        readSession1.Assert({}, p1, "");
        readSession1.Run();

        p2.Wait(TDuration::Seconds(5));
        readSession2.Assert({0}, p2, "");

        readSession2.WaitAndAssertPartitions({}, "Partition must be released because reding finished");
        readSession1.WaitAndAssertPartitions({}, "Partitions must be read only from Session-1");

        readSession1.Close();
        readSession2.Close();
    }
}

} // namespace NKikimr
