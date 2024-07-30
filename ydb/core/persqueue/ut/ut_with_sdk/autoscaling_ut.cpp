#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

static inline IOutputStream& operator<<(IOutputStream& o, std::set<size_t> t) {
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

Y_UNIT_TEST_SUITE(TopicAutoscaling) {

    void SimpleTest(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale();

        TTopicClient client = setup.MakeClient();

        auto writeSession1 = CreateWriteSession(client, "producer-1");
        auto writeSession2 = CreateWriteSession(client, "producer-2");

        TTestReadSession readSession("Session-0", client, 2, !autoscaleAwareSDK, {}, autoscaleAwareSDK);
        readSession.Run();

        UNIT_ASSERT(writeSession1->Write(Msg("message_1.1", 2)));
        UNIT_ASSERT(writeSession2->Write(Msg("message_2.1", 3)));

        readSession.WaitAllMessages();

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
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

    Y_UNIT_TEST(Simple_AutoscaleAwareSDK) {
        SimpleTest(true);
    }

    void ReadingAfterSplitTest(bool autoscaleAwareSDK, bool autoCommit) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale();

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_2.1", 3)));

        SplitPartition(setup, ++txId, 2, "d");

        UNIT_ASSERT(writeSession->Write(Msg("message_3.1", 5)));

        TTestReadSession readSession("Session-0", client, 3, autoCommit, {}, autoscaleAwareSDK);
        readSession.Run();
        readSession.WaitAllMessages();

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
            } else if (info.Data == "message_3.1") {
                UNIT_ASSERT_VALUES_EQUAL(4, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(5, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(ReadingAfterSplitTest_BeforeAutoscaleAwareSDK) {
        ReadingAfterSplitTest(false, true);
    }

    Y_UNIT_TEST(ReadingAfterSplitTest_AutoscaleAwareSDK) {
        ReadingAfterSplitTest(true, false);
    }

    Y_UNIT_TEST(ReadingAfterSplitTest_AutoscaleAwareSDK_AutoCommit) {
        ReadingAfterSplitTest(true, false);
    }

    void ReadingAfterSplitTest_PreferedPartition(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale();

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1");

        UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

        ui64 txId = 1006;
        SplitPartition(setup, ++txId, 0, "a");

        UNIT_ASSERT(writeSession->Write(Msg("message_2.1", 3)));

        SplitPartition(setup, ++txId, 2, "d");

        UNIT_ASSERT(writeSession->Write(Msg("message_3.1", 5)));

        TTestReadSession readSession("Session-0", client, 1, !autoscaleAwareSDK, {2}, autoscaleAwareSDK);
        readSession.Run();
        readSession.WaitAllMessages();

        Sleep(TDuration::Seconds(1));

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == "message_2.1") {
                UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(ReadingAfterSplitTest_PreferedPartition_BeforeAutoscaleAwareSDK) {
        ReadingAfterSplitTest_PreferedPartition(false);
    }

    Y_UNIT_TEST(ReadingAfterSplitTest_PreferedPartition_AutoscaleAwareSDK) {
        ReadingAfterSplitTest_PreferedPartition(true);
    }

    Y_UNIT_TEST(PartitionSplit_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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
        readSession.Impl->AutoCommit = true;
        readSession.Commit();
        readSession.WaitAndAssertPartitions({0, 1, 2}, "We are reading all partitions because offset is commited");
        readSession.Run();

        readSession.WaitAllMessages();

        for(const auto& info : readSession.Impl->ReceivedMessages) {
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

        UNIT_ASSERT_C(readSession.Impl->EndedPartitionEvents.empty(), "Old SDK is not support EndPartitionEvent");

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_AutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT(1 == info.PartitionId || 2 == info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL_C(1, readSession.Impl->EndedPartitionEvents.size(), "Only one partition was ended");
        auto& ev = readSession.Impl->EndedPartitionEvents.front();
        UNIT_ASSERT_VALUES_EQUAL_C(std::vector<ui32>{}, ev.GetAdjacentPartitionIds(), "There isn`t adjacent partitions after split");
        std::vector<ui32> children = {1, 2};
        UNIT_ASSERT_VALUES_EQUAL_C(children, ev.GetChildPartitionIds(), "");

        writeSession->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    void PartitionSplit_PreferedPartition(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == "message_1.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(2, info.SeqNo);
            } else if (info.Data == "message_2.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
            } else if (info.Data == "message_1.2") {
                UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(5, info.SeqNo);
            } else if (info.Data == "message_2.2") {
                UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(7, info.SeqNo);
            } else if (info.Data == "message_3.1") {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(1, info.SeqNo);
            } else if (info.Data == "message_4.1") {
                UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(1, info.SeqNo);
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

    Y_UNIT_TEST(PartitionSplit_PreferedPartition_AutoscaleAwareSDK) {
        PartitionSplit_PreferedPartition(true);
    }


    void PartitionMerge_PreferedPartition(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 2, 100);

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

        for(const auto& info : readSession.Impl->ReceivedMessages) {
            if (info.Data == TString("message_1.1")) {
                UNIT_ASSERT_VALUES_EQUAL(0, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(2, info.SeqNo);
            } else if (info.Data == TString("message_2.1")) {
                UNIT_ASSERT_VALUES_EQUAL(1, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(3, info.SeqNo);
            } else if (info.Data == TString("message_3.2")) {
                UNIT_ASSERT_VALUES_EQUAL(2, info.PartitionId);
                UNIT_ASSERT_VALUES_EQUAL(11, info.SeqNo);
            } else {
                UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
            }
        }

        if (autoscaleAwareSDK) {
            UNIT_ASSERT_VALUES_EQUAL_C(2, readSession.Impl->EndedPartitionEvents.size(), "Two partition was ended which was merged");
            for (auto& ev : readSession.Impl->EndedPartitionEvents) {
                UNIT_ASSERT(ev.GetAdjacentPartitionIds() == std::vector<ui32>{0} || ev.GetAdjacentPartitionIds() == std::vector<ui32>{1});
                UNIT_ASSERT_VALUES_EQUAL_C(std::vector<ui32>{2}, ev.GetChildPartitionIds(), "");
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(0, readSession.Impl->EndedPartitionEvents.size(), "OLD SDK");
        }


        writeSession1->Close(TDuration::Seconds(1));
        writeSession2->Close(TDuration::Seconds(1));
        writeSession3->Close(TDuration::Seconds(1));
        readSession.Close();
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition_BeforeAutoscaleAwareSDK) {
        PartitionMerge_PreferedPartition(false);
    }

    Y_UNIT_TEST(PartitionMerge_PreferedPartition_AutoscaleAwareSDK) {
        PartitionMerge_PreferedPartition(true);
    }

    void PartitionSplit_ReadEmptyPartitions(bool autoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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

    Y_UNIT_TEST(PartitionSplit_ReadEmptyPartitions_AutoscaleAwareSDK) {
        PartitionSplit_ReadEmptyPartitions(true);
    }

    Y_UNIT_TEST(PartitionSplit_ReadNotEmptyPartitions_BeforeAutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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

        readSession.SetOffset(0, 1);
        readSession.WaitAndAssertPartitions({0, 1, 2}, "Must read from all partitions because had been read from the end of partition");

        readSession.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ReadNotEmptyPartitions_AutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        TTestReadSession readSession1("Session-0", client, Max<size_t>(), false, {0, 1, 2}, false);
        readSession1.SetOffset(0, 1);
        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because read the partition 0 from offset 1");
        readSession1.SetOffset(0, 0);

        TTestReadSession readSession2("Session-1", client, Max<size_t>(), false, {0}, false);
        readSession2.SetOffset(0, 0);

        readSession2.WaitAndAssertPartitions({0}, "Must read partition 0 because it defined in the readSession");
        readSession2.Run();

        readSession1.WaitAndAssertPartitions({}, "Must release all partitions becase readSession2 read not from EndOffset");
        readSession1.Run();

        readSession1.WaitAndAssertPartitions({0}, "Partition 0 must rebalance to other sessions (Session-0)");

        readSession1.Close();
        readSession2.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession_AutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

        TTopicClient client = setup.MakeClient();

        auto writeSession = CreateWriteSession(client, "producer-1", 0);
        UNIT_ASSERT(writeSession->Write(Msg("message_1", 2)));

        ui64 txId = 1023;
        SplitPartition(setup, ++txId, 0, "a");

        Sleep(TDuration::Seconds(1));

        TTestReadSession readSession1("Session-0", client, Max<size_t>(), false, {}, true);

        readSession1.WaitAndAssertPartitions({0, 1, 2}, "Must read all exists partitions because used new SDK");
        readSession1.Commit();
        readSession1.Run();

        TTestReadSession readSession2("Session-1", client, Max<size_t>(), false, {}, true);
        readSession2.Run();

        Sleep(TDuration::Seconds(1));

        auto p1 = readSession1.GetPartitions();
        auto p2 = readSession2.GetPartitions();

        std::set<size_t> partitions;
        partitions.insert(p1.begin(), p1.end());
        partitions.insert(p2.begin(), p2.end());

        std::set<size_t> expected{0, 1, 2};

        UNIT_ASSERT_VALUES_EQUAL(expected, partitions);

        readSession1.Close();

        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(expected, readSession2.GetPartitions());

        readSession2.Close();
    }

    Y_UNIT_TEST(PartitionSplit_ManySession_existed_AutoscaleAwareSDK) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);

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

    Y_UNIT_TEST(ControlPlane_CreateAlterDescribe) {
        auto autoscalingTestTopic = "autoscalit-topic";
        TTopicSdkTestSetup setup = CreateSetup();
        TTopicClient client = setup.MakeClient();

        auto minParts = 5;
        auto maxParts = 10;
        auto scaleUpPercent = 80;
        auto scaleDownPercent = 20;
        auto threshold = 500;
        auto strategy = EAutoPartitioningStrategy::ScaleUp;

        TCreateTopicSettings createSettings;
        createSettings
            .BeginConfigurePartitioningSettings()
            .MinActivePartitions(minParts)
            .MaxActivePartitions(maxParts)
                .BeginConfigureAutoPartitioningSettings()
                .UpUtilizationPercent(scaleUpPercent)
                .DownUtilizationPercent(scaleDownPercent)
                .StabilizationWindow(TDuration::Seconds(threshold))
                .Strategy(strategy)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings();
        client.CreateTopic(autoscalingTestTopic, createSettings).Wait();

        TDescribeTopicSettings descSettings;

        auto describe = client.DescribeTopic(autoscalingTestTopic, descSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NYdb::EStatus::SUCCESS, describe.GetIssues().ToString());


        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), minParts);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetMaxActivePartitions(), maxParts);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), strategy);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetDownUtilizationPercent(), scaleDownPercent);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetUpUtilizationPercent(), scaleUpPercent);
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStabilizationWindow().Seconds(), threshold);

        auto alterMinParts = 10;
        auto alterMaxParts = 20;
        auto alterScaleUpPercent = 90;
        auto alterScaleDownPercent = 10;
        auto alterThreshold = 700;
        auto alterStrategy = EAutoPartitioningStrategy::ScaleUpAndDown;

        TAlterTopicSettings alterSettings;
        alterSettings
            .BeginAlterPartitioningSettings()
            .MinActivePartitions(alterMinParts)
            .MaxActivePartitions(alterMaxParts)
                .BeginAlterAutoPartitioningSettings()
                .DownUtilizationPercent(alterScaleDownPercent)
                .UpUtilizationPercent(alterScaleUpPercent)
                .StabilizationWindow(TDuration::Seconds(alterThreshold))
                .Strategy(alterStrategy)
                .EndAlterAutoPartitioningSettings()
            .EndAlterTopicPartitioningSettings();

        client.AlterTopic(autoscalingTestTopic, alterSettings).Wait();

        auto describeAfterAlter = client.DescribeTopic(autoscalingTestTopic).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), alterMinParts);
        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetMaxActivePartitions(), alterMaxParts);
        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), alterStrategy);
        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetUpUtilizationPercent(), alterScaleUpPercent);
        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetDownUtilizationPercent(), alterScaleDownPercent);
        UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStabilizationWindow().Seconds(), alterThreshold);
    }

    Y_UNIT_TEST(ControlPlane_DisableAutoPartitioning) {
        auto topicName = "autoscalit-topic";

        TTopicSdkTestSetup setup = CreateSetup();
        TTopicClient client = setup.MakeClient();

        {
            TCreateTopicSettings createSettings;
            createSettings
                .BeginConfigurePartitioningSettings()
                    .MinActivePartitions(1)
                    .MaxActivePartitions(100)
                    .BeginConfigureAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::ScaleUp)
                    .EndConfigureAutoPartitioningSettings()
                .EndConfigurePartitioningSettings();
            client.CreateTopic(topicName, createSettings).Wait();
        }

        {
            TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .BeginAlterAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::Disabled)
                    .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings();
            auto f = client.AlterTopic(topicName, alterSettings);
            f.Wait();

            auto v = f.GetValueSync();
            UNIT_ASSERT_C(!v.IsSuccess(), "Must receve error becuse max-partition is not 0");
        }

        {
            TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MaxActivePartitions(0)
                    .BeginAlterAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::Disabled)
                    .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings();
            auto f = client.AlterTopic(topicName, alterSettings);
            f.Wait();

            auto v = f.GetValueSync();
            UNIT_ASSERT_C(v.IsSuccess(),  "Error: " << v);
        }

        {
            auto describeAfterAlter = client.DescribeTopic(topicName).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), EAutoPartitioningStrategy::Disabled);
        }
    }

    Y_UNIT_TEST(ControlPlane_PauseAutoPartitioning) {
        auto topicName = "autoscalit-topic";

        TTopicSdkTestSetup setup = CreateSetup();
        TTopicClient client = setup.MakeClient();

        {
            TCreateTopicSettings createSettings;
            createSettings
                .BeginConfigurePartitioningSettings()
                    .MinActivePartitions(1)
                    .MaxActivePartitions(100)
                    .BeginConfigureAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::ScaleUp)
                    .EndConfigureAutoPartitioningSettings()
                .EndConfigurePartitioningSettings();
            client.CreateTopic(topicName, createSettings).Wait();
        }

        {
            TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(3)
                    .MaxActivePartitions(107)
                    .BeginAlterAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::Paused)
                    .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings();
            auto f = client.AlterTopic(topicName, alterSettings);
            f.Wait();

            auto v = f.GetValueSync();
            UNIT_ASSERT_C(v.IsSuccess(),  "Error: " << v);
        }

        {
            auto describeAfterAlter = client.DescribeTopic(topicName).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), 3);
            UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetMaxActivePartitions(), 107);
            UNIT_ASSERT_VALUES_EQUAL(describeAfterAlter.GetTopicDescription().GetPartitioningSettings().GetAutoPartitioningSettings().GetStrategy(), EAutoPartitioningStrategy::Paused);
        }
    }

    Y_UNIT_TEST(ControlPlane_AutoscalingWithStorageSizeRetention) {
        auto autoscalingTestTopic = "autoscalit-topic";
        TTopicSdkTestSetup setup = CreateSetup();
        TTopicClient client = setup.MakeClient();

        TCreateTopicSettings createSettings;
        createSettings
            .RetentionStorageMb(1024)
            .BeginConfigurePartitioningSettings()
                .BeginConfigureAutoPartitioningSettings()
                .Strategy(EAutoPartitioningStrategy::ScaleUp)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings();
        auto result = client.CreateTopic(autoscalingTestTopic, createSettings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::BAD_REQUEST);

        createSettings.RetentionStorageMb(0);
        result = client.CreateTopic(autoscalingTestTopic, createSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

        TAlterTopicSettings alterSettings;
        alterSettings
            .SetRetentionStorageMb(1024);

        result = client.AlterTopic(autoscalingTestTopic, alterSettings).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(PartitionSplit_AutosplitByLoad) {
        TTopicSdkTestSetup setup = CreateSetup();
        TTopicClient client = setup.MakeClient();

        TCreateTopicSettings createSettings;
        createSettings
            .BeginConfigurePartitioningSettings()
            .MinActivePartitions(1)
            .MaxActivePartitions(100)
                .BeginConfigureAutoPartitioningSettings()
                .UpUtilizationPercent(2)
                .DownUtilizationPercent(1)
                .StabilizationWindow(TDuration::Seconds(1))
                .Strategy(EAutoPartitioningStrategy::ScaleUp)
                .EndConfigureAutoPartitioningSettings()
            .EndConfigurePartitioningSettings();
        client.CreateTopic(TEST_TOPIC, createSettings).Wait();

        auto msg = TString(1_MB, 'a');

        auto writeSession = CreateWriteSession(client, "producer-1", 0, TEST_TOPIC, false);
        UNIT_ASSERT(writeSession->Write(Msg(msg, 1)));
        UNIT_ASSERT(writeSession->Write(Msg(msg, 2)));
        Sleep(TDuration::Seconds(5));
        auto describe = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe.GetTopicDescription().GetPartitions().size(), 3);

        auto writeSession2 = CreateWriteSession(client, "producer-1", 1, TEST_TOPIC, false);
        UNIT_ASSERT(writeSession2->Write(Msg(msg, 3)));
        UNIT_ASSERT(writeSession2->Write(Msg(msg, 4)));
        Sleep(TDuration::Seconds(5));
        auto describe2 = client.DescribeTopic(TEST_TOPIC).GetValueSync();
        UNIT_ASSERT_EQUAL(describe2.GetTopicDescription().GetPartitions().size(), 5);
    }

    Y_UNIT_TEST(MidOfRange) {
        auto AsString = [](std::vector<ui16> vs) {
            TStringBuilder a;
            for (auto v : vs) {
                a << static_cast<unsigned char>(v);
            }
            return a;
        };

        auto ToHex = [](const TString& value) {
            return TStringBuilder() << HexText(TBasicStringBuf(value));
        };

        {
            auto res = NKikimr::NPQ::MiddleOf("", "");
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "7F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf("", AsString({0x7F}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "3F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x7F}), "");
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "BF");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x7F}), AsString({0xBF}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "9F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x01}), AsString({0x02}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "01 7F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x01, 0x7F}), AsString({0x02}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "01 BF");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x02}), AsString({0x03, 0x7F}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "02 3F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x01, 0xFF}), AsString({0x02, 0x00}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "01 FF 7F");
        }

        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x03, 0xFF}), AsString({0x04, 0x20}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "04 0F");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x03, 0x40}), AsString({0x04, 0x40}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "03 C0");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x03, 0x20}), AsString({0x04, 0x10}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "03 98");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x04, 0xFF, 0xFF}), AsString({0x05, 0x20}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "05 0F FF");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x03, 0x40, 0x7F}), AsString({0x04, 0x40}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "03 C0 BF");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x03, 0x40, 0x30}), AsString({0x04, 0x40, 0x20}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "03 C0 A8");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x01, 0xFF, 0xFF, 0xFF}), AsString({0x02, 0x00, 0x00, 0x10}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "02 00 00 07");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0xFF}), AsString({0xFF, 0xFF}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "FF 7F");
        }
        {
            auto res = NKikimr::NPQ::MiddleOf(AsString({0x99, 0xFF}), AsString({0x9A}));
            UNIT_ASSERT_VALUES_EQUAL(ToHex(res), "99 FF 7F");
        }
    }
}

} // namespace NKikimr
