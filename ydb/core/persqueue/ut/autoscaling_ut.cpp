#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

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

Y_UNIT_TEST_SUITE(TopicAutoscaling) {

    void SimpleTest(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic();

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");

        TTestReadSession readSession("Session-0", client, 2, !autoscaleAwareSDK, {}, autoscaleAwareSDK);
        readSession.Run();

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        readSession.WaitAllMessages();

        for(const auto& info : readSession.ReceivedMessages) {
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
        readSession.Close();
    }

    Y_UNIT_TEST(Simple_BeforeAutoscaleAwareSDK) {
        SimpleTest(false);
    }

    Y_UNIT_TEST(Simple_NewSDK) {
        SimpleTest(true);
    }

    Y_UNIT_TEST(PartitionSplit_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        TTestReadSession readSession("Session-0", client, 2, false, {}, false);
        readSession.Run();

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.2", 3)));

        Sleep(TDuration::Seconds(1)); // Wait read session events

        readSession.WaitAndAssertPartitions({0}, "We are reading only one partition because offset is not commited");
        readSession.Run();
        readSession.AutoCommit = true;
        readSession.Commit();
        readSession.WaitAndAssertPartitions({0, 1, 2}, "We are reading all partitions because offset is commited");
        readSession.Run();

        readSession.WaitAllMessages();

        for(const auto& info : readSession.ReceivedMessages) {
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

        UNIT_ASSERT_C(readSession.EndedPartitionEvents.empty(), "Old SDK is not support EndPartitionEvent");

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_NewSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        TTestReadSession readSession("Session-0", client, 2, false, {}, true);
        readSession.Run();

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.2", 3)));

        readSession.WaitAndAssertPartitions({0, 1, 2}, "We are reading all partitions because new SDK is not wait commit");
        readSession.Run();

        readSession.WaitAllMessages();

        for(const auto& info : readSession.ReceivedMessages) {
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

        UNIT_ASSERT_VALUES_EQUAL_C(1, readSession.EndedPartitionEvents.size(), "Only one partition was ended");
        auto& ev = readSession.EndedPartitionEvents.front();
        UNIT_ASSERT_VALUES_EQUAL_C(std::vector<ui32>{}, ev.GetAdjacentPartitionIds(), "There isn`t adjacent partitions after split");
        std::vector<ui32> children = {1, 2};
        UNIT_ASSERT_VALUES_EQUAL_C(children, ev.GetChildPartitionIds(), "");

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    void PartitionSplit_PreferedPartition(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");
        auto writeSession3 = CreateWriteSession(client, "producer-3", 0);

        TTestReadSession readSession("Session-0", client, 6, !autoscaleAwareSDK, {}, autoscaleAwareSDK);
        readSession.Run();

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

        readSession.WaitAllMessages();

        Cerr << ">>>>> All messages received" << Endl;

        for(const auto& info : readSession.ReceivedMessages) {
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
        writeSession4->Close(TDuration::Seconds(1));

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_PreferedPartition_BeforeAutoscaleAwareSDK) {
        PartitionSplit_PreferedPartition(false);
    }

    Y_UNIT_TEST(PartitionSplit_PreferedPartition_NewSDK) {
        PartitionSplit_PreferedPartition(true);
    }


    void PartitionMerge_PreferedPartition(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1", 0);
        auto writeSession2 = CreateWriteSession(client, "producer-2", 1);

        TTestReadSession readSession("Session-0", client, 3, !autoscaleAwareSDK, {}, autoscaleAwareSDK);
        readSession.Run();

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        ui64 txId = 1012;
        MergePartition(setup, ++txId, 0, 1);

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.2", 5))); // Will be fail because partition is not writable after merge
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.2", 7))); // Will be fail because partition is not writable after merge

        auto writeSession3 = CreateWriteSession(client, "producer-2", 2);

        UNIT_ASSERT(writeSession3->Write(Msg("message_3.1", 2)));  // Will be ignored because duplicated SeqNo
        UNIT_ASSERT(writeSession3->Write(Msg("message_3.2", 11)));

        readSession.WaitAllMessages();

        for(const auto& info : readSession.ReceivedMessages) {
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

        if (autoscaleAwareSDK) {
            UNIT_ASSERT_VALUES_EQUAL_C(2, readSession.EndedPartitionEvents.size(), "Two partition was ended which was merged");
            for (auto& ev : readSession.EndedPartitionEvents) {
                UNIT_ASSERT(ev.GetAdjacentPartitionIds() == std::vector<ui32>{0} || ev.GetAdjacentPartitionIds() == std::vector<ui32>{1});
                UNIT_ASSERT_VALUES_EQUAL_C(std::vector<ui32>{2}, ev.GetChildPartitionIds(), "");
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(0, readSession.EndedPartitionEvents.size(), "OLD SDK");
        }


        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition_BeforeAutoscaleAwareSDK) {
        PartitionMerge_PreferedPartition(false);
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition_NewSDK) {
        PartitionMerge_PreferedPartition(true);
    }

    void PartitionSplit_ReadEmptyPartitions(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();
        TTestReadSession readSession("session-0", client, Max<size_t>(), false, {}, autoscaleAwareSDK);

        readSession.WaitAndAssertPartitions({0}, "Must read all exists partitions");

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        readSession.WaitAndAssertPartitions({0, 1, 2}, "After split must read all partitions because parent partition is empty");

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ReadEmptyPartitions_BeforeAutoscaleAwareSDK) {
        PartitionSplit_ReadEmptyPartitions(false);
    }

    Y_UNIT_TEST(PartitionSplit_ReadEmptyPartitions_NewSDK) {
        PartitionSplit_ReadEmptyPartitions(true);
    }

    Y_UNIT_TEST(PartitionSplit_ReadNotEmptyPartitions_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();
        TTestReadSession readSession("Session-0", client, Max<size_t>(), false, {}, false);

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

    Y_UNIT_TEST(PartitionSplit_ReadNotEmptyPartitions_NewSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();
        TTestReadSession readSession("Session-0", client, Max<size_t>(), false, {}, true);

        auto writeSession = CreateWriteSession(client, "producer-1", 0);

        readSession.WaitAndAssertPartitions({0}, "Must read all exists partitions");

        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        readSession.WaitAndAssertPartitions({0, 1, 2}, "Must read from all partitions because used new SDK");

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        TTestReadSession readSession1("Session-0", client, Max<size_t>(), false, {0, 1, 2}, false);
        readSession1.Offsets[0] = 1;
        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because read the partition 0 from offset 1");
        readSession1.Offsets[0] = 0;

        TTestReadSession readSession2("Session-1", client, Max<size_t>(), false, {0}, false);
        readSession2.Offsets[0] = 0;

        readSession2.WaitAndAssertPartitions({0}, "Must read partition 0 because it defined in the readSession");
        readSession2.Run();

        readSession1.WaitAndAssertPartitions({}, "Must release all partitions becase readSession2 read not from EndOffset");
        readSession1.Run();

        readSession1.WaitAndAssertPartitions({0}, "Partition 0 must rebalance to other sessions (Session-0)");

        readSession1.Close();
        readSession2.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession_NewSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        Sleep(TDuration::Seconds(1));

        TTestReadSession readSession1("Session-0", client, Max<size_t>(), false, {0, 1, 2}, true);
        TTestReadSession readSession2("Session-1", client, Max<size_t>(), false, {0}, true);

        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because used new SDK");
        readSession1.Commit();
        readSession2.Run();

        readSession2.WaitAndAssertPartitions({0}, "Must read partition 0 because it defined in the readSession");
        readSession2.Run();

        readSession1.WaitAndAssertPartitions({1, 2}, "Partition 0 must rebalance to other sessions (Session-0)");

        readSession1.Close();
        readSession2.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession_existed_NewSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        TTestReadSession readSession1("Session-0", client, Max<size_t>(), false, {}, true);
        TTestReadSession readSession2("Session-1", client, Max<size_t>(), false, {0}, true);

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because used new SDK");
        readSession1.Commit();
        readSession2.Run();

        readSession2.WaitAndAssertPartitions({0}, "Must read partition 0 because it defined in the readSession");
        readSession2.Run();

        readSession1.WaitAndAssertPartitions({1, 2}, "Partition 0 must rebalance to other sessions (Session-0)");

        readSession1.Close();
        readSession2.Close();
    }

    Y_UNIT_TEST(CommitTopPast_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));
        UNIT_ASSERT(writeSession->Write(Msg("message_2", 3)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        auto status = client.CommitOffset(TEST_TOPIC, 0, TEST_CONSUMER, 0).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "The consumer has just started reading the inactive partition and he can commit");

        status = client.CommitOffset(TEST_TOPIC, 0, TEST_CONSUMER, 1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "A consumer who has not read to the end can commit messages forward.");

        status = client.CommitOffset(TEST_TOPIC, 0, TEST_CONSUMER, 0).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "A consumer who has not read to the end can commit messages back.");

        status = client.CommitOffset(TEST_TOPIC, 0, TEST_CONSUMER, 2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "The consumer can commit at the end of the inactive partition.");

        status = client.CommitOffset(TEST_TOPIC, 0, TEST_CONSUMER, 0).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::BAD_REQUEST, status.GetStatus(), "The consumer cannot commit an offset for inactive, read-to-the-end partitions.");
    }
}

} // namespace NKikimr
