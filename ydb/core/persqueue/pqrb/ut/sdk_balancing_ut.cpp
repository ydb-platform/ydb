#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>

#include <memory>
#include <set>
#include <vector>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

Y_UNIT_TEST_SUITE(Balancing) {

    void Simple(SdkVersion sdk) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        auto readSession0 = CreateTestReadSession({ .Name="Session-0", .Setup=setup, .Sdk = sdk });
        {
            readSession0->WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0->Run();
        }

        auto readSession1 = CreateTestReadSession({ .Name="Session-1", .Setup=setup, .Sdk = sdk });
        {
            readSession1->Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        auto readSession2 = CreateTestReadSession({ .Name="Session-2", .Setup=setup, .Sdk = sdk, .Partitions = {0, 1} });
        {
            readSession2->WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2->Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();
            auto pa = p0;
            pa.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(4, p0.size(), "There should be an even distribution of partitions " << p0);
            UNIT_ASSERT_VALUES_EQUAL_C(4, p1.size(), "There should be an even distribution of partitions " << p1);
            UNIT_ASSERT_VALUES_EQUAL_C(8, pa.size(), "Must read all partitions but " << pa);
        }

        auto readSession3 = CreateTestReadSession({ .Name="Session-3", .Setup=setup, .Sdk = sdk, .Partitions = {0} });
        {
            readSession3->WaitAndAssertPartitions({0}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2->WaitAndAssertPartitions({1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(8, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession3->Run();
            readSession3->Close();

            readSession2->WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read. (after release Session-3)");
            readSession2->Run();
        }

        {
            readSession2->Run();
            readSession2->Close();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession1->Run();
            readSession1->Close();

            readSession0->WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0->Run();
        }


        readSession0->Close();
    }

    Y_UNIT_TEST(Balancing_OneTopic_TopicApi) {
        Simple(SdkVersion::Topic);
    }

    Y_UNIT_TEST(Balancing_OneTopic_PQv1) {
        Simple(SdkVersion::PQv1);
    }


    void ManyTopics(SdkVersion sdk) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);
        setup.CreateTopic("other-test-topic", TEST_CONSUMER, 10);

        TTopicClient client = setup.MakeClient();

        auto readSession0 = CreateTestReadSession({ .Name="Session-0", .Setup=setup, .Sdk = sdk, .Topics = {TEST_TOPIC, "other-test-topic"} });
        Sleep(TDuration::Seconds(1));

        {
            auto p = readSession0->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(10, p[TString{TEST_TOPIC}].size());
            UNIT_ASSERT_VALUES_EQUAL(10, p["other-test-topic"].size());
        }

        auto readSession1 = CreateTestReadSession({ .Name="Session-1", .Setup=setup, .Sdk = sdk, .Topics = {TEST_TOPIC, "other-test-topic"} });
        Sleep(TDuration::Seconds(1));

        {
            auto p = readSession0->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(5, p[TString{TEST_TOPIC}].size());
            UNIT_ASSERT_VALUES_EQUAL(5, p["other-test-topic"].size());
        }
        {
            auto p = readSession1->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(5, p[TString{TEST_TOPIC}].size());
            UNIT_ASSERT_VALUES_EQUAL(5, p["other-test-topic"].size());
        }

        readSession0->Close();
        readSession1->Close();
    }

    Y_UNIT_TEST(Balancing_ManyTopics_TopicApi) {
        ManyTopics(SdkVersion::Topic);
    }

    Y_UNIT_TEST(Balancing_ManyTopics_PQv1) {
        ManyTopics(SdkVersion::PQv1);
    }

 }

void WaitEnded(const std::shared_ptr<ITestReadSession>& session, size_t count) {
    for (size_t i = 0; i < 15; ++i) {
        if (session->GetEndedPartitionEvents().size() >= count) {
            return;
        }
        Sleep(TDuration::Seconds(1));
    }
    UNIT_ASSERT_VALUES_EQUAL_C(count, session->GetEndedPartitionEvents().size(),
        "timed out waiting for ended partition events");
}

void WaitFamilyOnOneSession(
    const std::shared_ptr<ITestReadSession>& first,
    const std::shared_ptr<ITestReadSession>& second,
    const std::set<size_t>& family,
    const TString& message)
{
    std::set<size_t> p0;
    std::set<size_t> p1;
    for (size_t i = 0; i < 20; ++i) {
        p0 = first->GetPartitions();
        p1 = second->GetPartitions();
        bool firstHasAll = true;
        bool secondHasAll = true;
        bool firstHasAny = false;
        bool secondHasAny = false;
        for (auto id : family) {
            const bool inFirst = p0.contains(id);
            const bool inSecond = p1.contains(id);
            firstHasAll = firstHasAll && inFirst;
            secondHasAll = secondHasAll && inSecond;
            firstHasAny = firstHasAny || inFirst;
            secondHasAny = secondHasAny || inSecond;
        }
        if ((firstHasAll && !secondHasAny) || (secondHasAll && !firstHasAny)) {
            return;
        }
        Sleep(TDuration::Seconds(1));
    }
    UNIT_ASSERT_C(false, message << ", p0=" << p0 << " p1=" << p1);
}

Y_UNIT_TEST_SUITE(MergeBalancing) {

    Y_UNIT_TEST(OneSession_ChildAfterBothParents) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 2, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        auto write1 = CreateWriteSession(client, "producer-1", 1);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));
        UNIT_ASSERT(write1->Write(Msg("p1", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 2,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session->WaitAndAssertPartitions({0, 1}, "Must read both parent partitions");
        session->WaitAllMessages();

        ui64 txId = 2001;
        MergePartition(setup, ++txId, 0, 1);

        WaitEnded(session, 2);
        session->WaitAndAssertPartitions({0, 1, 2}, "ScaleAware session must get the merge child after both parents ended");
        session->Close();
        write0->Close(TDuration::Seconds(1));
        write1->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ThirdSessionDoesNotSplitUncommittedFamily) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 2, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        auto write1 = CreateWriteSession(client, "producer-1", 1);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));
        UNIT_ASSERT(write1->Write(Msg("p1", 1)));

        auto session0 = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 2,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session0->WaitAndAssertPartitions({0, 1}, "Must read parents");
        session0->WaitAllMessages();

        ui64 txId = 2004;
        MergePartition(setup, ++txId, 0, 1);
        WaitEnded(session0, 2);
        session0->WaitAndAssertPartitions({0, 1, 2}, "Family must include merge child");
        session0->Run();

        auto session1 = CreateTestReadSession({
            .Name = "Session-1",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        WaitFamilyOnOneSession(session0, session1, {0, 1, 2},
            "Uncommitted merge family must stay on one session");

        session0->Close();
        session1->Close();
        write0->Close(TDuration::Seconds(1));
        write1->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(UnaffectedPartitionStaysReadable) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 3, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        auto write1 = CreateWriteSession(client, "producer-1", 1);
        auto write2 = CreateWriteSession(client, "producer-2", 2);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));
        UNIT_ASSERT(write1->Write(Msg("p1", 1)));
        UNIT_ASSERT(write2->Write(Msg("p2", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 3,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session->WaitAndAssertPartitions({0, 1, 2}, "All original partitions");
        session->WaitAllMessages();

        ui64 txId = 2006;
        MergePartition(setup, ++txId, 0, 1);
        UNIT_ASSERT_C(session->GetPartitions().contains(2), "Partition 2 must stay assigned during merge of 0 and 1");
        WaitEnded(session, 2);
        session->WaitAndAssertPartitions({0, 1, 2, 3}, "Unaffected partition 2 stays with the merge child");

        session->Close();
        write0->Close(TDuration::Seconds(1));
        write1->Close(TDuration::Seconds(1));
        write2->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(SplitThenMerge_ChildAfterBothSplitChildren) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 1,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session->WaitAndAssertPartitions({0}, "root");
        session->WaitAllMessages();

        ui64 txId = 2007;
        SplitPartition(setup, ++txId, 0, "a");
        WaitEnded(session, 1);
        session->Commit();
        session->WaitAndAssertPartitions({0, 1, 2}, "split children");
        session->Run();

        MergePartition(setup, ++txId, 1, 2);
        WaitEnded(session, 3);
        session->WaitAndAssertPartitions({0, 1, 2, 3}, "merge child after both split children ended");

        session->Close();
        write0->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(PQv1_ChildAfterParentsFinished) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 2, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        auto write1 = CreateWriteSession(client, "producer-1", 1);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));
        UNIT_ASSERT(write1->Write(Msg("p1", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::PQv1,
            .ExpectedMessagesCount = 2,
            .AutoCommit = true,
            .AutoPartitioningSupport = false,
        });
        session->WaitAndAssertPartitions({0, 1}, "PQv1 reads parents");
        session->WaitAllMessages();

        ui64 txId = 2008;
        MergePartition(setup, ++txId, 0, 1);
        Sleep(TDuration::Seconds(2));
        auto partitions = session->GetPartitions();
        UNIT_ASSERT_C(partitions.contains(2) || partitions.contains(0),
            "PQv1 session must keep reading after merge, partitions=" << partitions);

        session->Close();
        write0->Close(TDuration::Seconds(1));
        write1->Close(TDuration::Seconds(1));
    }

 }

Y_UNIT_TEST_SUITE(SplitBalancing) {

    Y_UNIT_TEST(OneSession_ChildrenAfterParent) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 1,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session->WaitAndAssertPartitions({0}, "root");
        session->WaitAllMessages();

        ui64 txId = 3001;
        SplitPartition(setup, ++txId, 0, "a");
        WaitEnded(session, 1);
        session->WaitAndAssertPartitions({0, 1, 2}, "ScaleAware session must get both split children after the parent ended");

        session->Close();
        write0->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ThirdSessionDoesNotSplitUncommittedFamily) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));

        auto session0 = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 1,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session0->WaitAndAssertPartitions({0}, "root");
        session0->WaitAllMessages();

        ui64 txId = 3002;
        SplitPartition(setup, ++txId, 0, "a");
        WaitEnded(session0, 1);
        session0->WaitAndAssertPartitions({0, 1, 2}, "family includes split children");
        session0->Run();

        auto session1 = CreateTestReadSession({
            .Name = "Session-1",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        WaitFamilyOnOneSession(session0, session1, {0, 1, 2},
            "Uncommitted split family must stay on one session");

        session0->Close();
        session1->Close();
        write0->Close(TDuration::Seconds(1));
    }

    Y_UNIT_TEST(UnaffectedPartitionStaysReadable) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 2, 100);
        TTopicClient client = setup.MakeClient();

        auto write0 = CreateWriteSession(client, "producer-0", 0);
        auto write1 = CreateWriteSession(client, "producer-1", 1);
        UNIT_ASSERT(write0->Write(Msg("p0", 1)));
        UNIT_ASSERT(write1->Write(Msg("p1", 1)));

        auto session = CreateTestReadSession({
            .Name = "Session-0",
            .Setup = setup,
            .Sdk = SdkVersion::Topic,
            .ExpectedMessagesCount = 2,
            .AutoCommit = false,
            .AutoPartitioningSupport = true,
        });
        session->WaitAndAssertPartitions({0, 1}, "both original partitions");
        session->WaitAllMessages();

        ui64 txId = 3003;
        SplitPartition(setup, ++txId, 0, "a");
        UNIT_ASSERT_C(session->GetPartitions().contains(1), "Partition 1 must stay assigned during split of 0");
        WaitEnded(session, 1);
        session->WaitAndAssertPartitions({0, 1, 2, 3}, "Unaffected partition 1 stays with the split children");

        session->Close();
        write0->Close(TDuration::Seconds(1));
        write1->Close(TDuration::Seconds(1));
    }

 }

} // namespace NKikimr
