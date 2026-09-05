#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NKikimr::NPQ::NTest;

namespace {

ui64 GetCommittedOffset(TTopicSdkTestSetup& setup, const TString& topic, const TString& consumer, ui32 partitionId = 0) {
    auto describe = setup.DescribeConsumer(topic, consumer);
    UNIT_ASSERT_LT(partitionId, describe.GetPartitions().size());
    const auto& stats = describe.GetPartitions()[partitionId].GetPartitionConsumerStats();
    UNIT_ASSERT(stats);
    return stats->GetCommittedOffset();
}

} // namespace

Y_UNIT_TEST_SUITE(TSetOffsetsSdkTests) {

Y_UNIT_TEST(EarliestLatestTimestamp) {
    TTopicSdkTestSetup setup("SetOffsetsSdk", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    setup.Write("topic1", "m1", 0);
    setup.Write("topic1", "m2", 0);

    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath("topic1");

    {
        auto status = client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 2);
    }
    {
        auto status = client.SetOffsets(path, "consumer", TSetOffsetsSettings().Earliest()).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 0);
    }
    {
        auto status = client.SetOffsets(path, "consumer", TSetOffsetsSettings().FromWrittenAt(TInstant::Now() + TDuration::Hours(1))).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 2);
    }
}

Y_UNIT_TEST(MissingTopicAndConsumer) {
    TTopicSdkTestSetup setup("SetOffsetsSdkErrors", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    TTopicClient client(setup.MakeDriver());

    {
        auto status = client.SetOffsets("/Root/missing", "consumer", TSetOffsetsSettings().Earliest()).GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::SCHEME_ERROR);
    }
    {
        auto status = client.SetOffsets(setup.GetFullTopicPath("topic1"), "no-such-consumer", TSetOffsetsSettings().Earliest()).GetValueSync();
        UNIT_ASSERT(!status.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::SCHEME_ERROR);
    }
}

Y_UNIT_TEST(UnspecifiedPositionRejected) {
    TTopicSdkTestSetup setup("SetOffsetsSdkNoPosition", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    TTopicClient client(setup.MakeDriver());
    auto status = client.SetOffsets(setup.GetFullTopicPath("topic1"), "consumer", TSetOffsetsSettings()).GetValueSync();
    UNIT_ASSERT(!status.IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(status.GetStatus(), EStatus::BAD_REQUEST);
}

Y_UNIT_TEST(IdempotentLatest) {
    TTopicSdkTestSetup setup("SetOffsetsSdkIdempotent", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    setup.Write("topic1", "m1", 0);
    setup.Write("topic1", "m2", 0);

    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath("topic1");
    UNIT_ASSERT_C(client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync().IsSuccess(),
        "first latest");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 2);
    UNIT_ASSERT_C(client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync().IsSuccess(),
        "second latest");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 2);
}

Y_UNIT_TEST(OtherConsumerUnaffected) {
    TTopicSdkTestSetup setup("SetOffsetsSdkTwoConsumers", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer-a");
    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath("topic1");
    auto alter = client.AlterTopic(path, TAlterTopicSettings()
        .BeginAddConsumer("consumer-b")
        .EndAddConsumer()).GetValueSync();
    UNIT_ASSERT_C(alter.IsSuccess(), alter.GetIssues().ToString());

    setup.Write("topic1", "m1", 0);
    setup.Write("topic1", "m2", 0);

    UNIT_ASSERT_C(client.SetOffsets(path, "consumer-a", TSetOffsetsSettings().Latest()).GetValueSync().IsSuccess(),
        "reset a");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer-a"), 2);
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer-b"), 0);
}

Y_UNIT_TEST(ResetLatestAllPartitionsCommitted) {
    constexpr ui32 partitionCount = 1024;
    TTopicSdkTestSetup setup("SetOffsetsSdkAllPartitions", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer", partitionCount);

    auto client = setup.MakeClient();
    const auto path = setup.GetFullTopicPath("topic1");
    for (ui32 partitionId = 0; partitionId < partitionCount; ++partitionId) {
        auto session = client.CreateSimpleBlockingWriteSession(
            TWriteSessionSettings()
                .Path(path)
                .PartitionId(partitionId)
                .DeduplicationEnabled(false)
                .Codec(ECodec::RAW));
        UNIT_ASSERT_C(session->Write("m"), TStringBuilder() << "write partition " << partitionId);
        UNIT_ASSERT_C(session->Close(), TStringBuilder() << "close partition " << partitionId);
    }

    auto status = client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    auto descr = setup.DescribeConsumer("topic1", "consumer");
    UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitions().size(), partitionCount);
    for (const auto& part : descr.GetPartitions()) {
        const auto& stats = part.GetPartitionConsumerStats();
        UNIT_ASSERT_C(stats, TStringBuilder() << "partition " << part.GetPartitionId());
        UNIT_ASSERT_VALUES_EQUAL_C(stats->GetCommittedOffset(), 1, TStringBuilder() << "partition " << part.GetPartitionId());
    }
}

Y_UNIT_TEST(LatestSurvivesTabletReboot) {
    TTopicSdkTestSetup setup("SetOffsetsSdkRebootLatest", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    setup.Write("topic1", "m1", 0);

    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath("topic1");
    auto status = client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    auto assertCommittedAtEnd = [&] {
        auto descr = setup.DescribeConsumer("topic1", "consumer");
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitions().size(), 1);
        const auto& part = descr.GetPartitions()[0];
        UNIT_ASSERT(part.GetPartitionStats());
        UNIT_ASSERT(part.GetPartitionConsumerStats());
        UNIT_ASSERT_VALUES_EQUAL(part.GetPartitionStats()->GetEndOffset(), 1);
        UNIT_ASSERT_VALUES_EQUAL(part.GetPartitionConsumerStats()->GetCommittedOffset(),
                                 part.GetPartitionStats()->GetEndOffset());
    };

    assertCommittedAtEnd();
    setup.GetServer().KillTopicPqTablets(TString{path});
    assertCommittedAtEnd();
}

Y_UNIT_TEST(RewindSurvivesTabletReboot) {
    TTopicSdkTestSetup setup("SetOffsetsSdkRebootRewind", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopic("topic1", "consumer");
    setup.Write("topic1", "m1", 0);
    setup.Write("topic1", "m2", 0);

    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath("topic1");
    UNIT_ASSERT_C(client.SetOffsets(path, "consumer", TSetOffsetsSettings().Latest()).GetValueSync().IsSuccess(),
        "latest");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 2);
    UNIT_ASSERT_C(client.SetOffsets(path, "consumer", TSetOffsetsSettings().Earliest()).GetValueSync().IsSuccess(),
        "earliest");
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 0);

    setup.GetServer().KillTopicPqTablets(TString{path});
    UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, "topic1", "consumer"), 0);
}

Y_UNIT_TEST(RewindInactiveAfterSplit) {
    TTopicSdkTestSetup setup("SetOffsetsSdkInactive", TTopicSdkTestSetup::MakeServerSettings(), false);
    setup.CreateTopicWithAutoscale(TEST_TOPIC, TEST_CONSUMER, 1, 100);
    setup.Write(TEST_TOPIC, "before-split", 0);

    TTopicClient client(setup.MakeDriver());
    const auto path = setup.GetFullTopicPath(TEST_TOPIC);
    auto committed = client.SetOffsets(path, TEST_CONSUMER, TSetOffsetsSettings().Latest()).GetValueSync();
    UNIT_ASSERT_C(committed.IsSuccess(), committed.GetIssues().ToString());

    ui64 txId = 1000;
    SplitPartition(setup, txId, 0, "\x80");

    for (int i = 0; i < 50; ++i) {
        auto descr = setup.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER);
        bool hasInactive = false;
        for (const auto& part : descr.GetPartitions()) {
            if (!part.GetActive()) {
                hasInactive = true;
                break;
            }
        }
        if (hasInactive) {
            break;
        }
        Sleep(TDuration::MilliSeconds(200));
    }

    auto status = client.SetOffsets(path, TEST_CONSUMER, TSetOffsetsSettings().Earliest()).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

    auto descr = setup.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER);
    bool sawInactive = false;
    for (const auto& part : descr.GetPartitions()) {
        if (!part.GetActive()) {
            sawInactive = true;
            const auto& stats = part.GetPartitionConsumerStats();
            UNIT_ASSERT(stats);
            UNIT_ASSERT_VALUES_EQUAL(stats->GetCommittedOffset(), 0);
        }
    }
    UNIT_ASSERT(sawInactive);
}

} // TSetOffsetsSdkTests
