#include "test_topic.h"
#include "test_env.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>

namespace NKikimr::NReplication::NTestHelpers {

void TTestTopicDescription::SerializeTo(NKikimrSchemeOp::TPersQueueGroupDescription& proto) const {
    proto.SetName(Name);
    proto.SetTotalGroupCount(1);

    auto& pq = *proto.MutablePQTabletConfig();
    auto& p = *pq.MutablePartitionConfig();
    p.SetLifetimeSeconds(3600);
}

THolder<NKikimrSchemeOp::TPersQueueGroupDescription> MakeTopicDescription(const TTestTopicDescription& desc) {
    auto result = MakeHolder<NKikimrSchemeOp::TPersQueueGroupDescription>();
    desc.SerializeTo(*result);
    return result;
}

TFeatureFlags TopicReplicationFlags() {
    TFeatureFlags flags;
    flags.FeatureFlags.SetEnableTopicReplication(true);
    flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
    return flags;
}

void WaitForReplication(TEnv<true>& env, const std::function<bool()>& ready, const TString& description) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(60);
    while (!ready()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for " << description);
        if (env.GetRuntime().IsRealThreads()) {
            Sleep(TDuration::MilliSeconds(10));
        } else {
            env.GetRuntime().SimulateSleep(TDuration::MilliSeconds(10));
        }
    }
}

std::optional<TTopicOffsets> GetTopicOffsets(TEnv<true>& env, const TString& topic, const TString& consumer) {
    NYdb::NTopic::TTopicClient client(env.GetDriver());
    const auto result = env.RunCall([&] {
        return client.DescribeConsumer(topic, consumer,
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
    });
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    const auto& partitions = result.GetConsumerDescription().GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
    const auto& partition = partitions.front();
    UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);
    const auto& stats = partition.GetPartitionStats();
    const auto& consumerStats = partition.GetPartitionConsumerStats();
    if (!stats || !consumerStats) {
        return std::nullopt;
    }
    return TTopicOffsets{stats->GetStartOffset(), stats->GetEndOffset(), consumerStats->GetCommittedOffset()};
}

TVector<TTopicRecord> ReadTopic(TEnv<true>& env, const TString& topic, size_t count, ui64 firstOffset) {
    using namespace NYdb::NTopic;
    TTopicClient client(env.GetDriver());
    auto reader = client.CreateReadSession(TReadSessionSettings().WithoutConsumer()
        .AppendTopics(TTopicReadSettings(topic).AppendPartitionIds(0)));
    TVector<TTopicRecord> records;
    WaitForReplication(env, [&] {
        auto event = reader->GetEvent();
        if (!event) {
            return false;
        }
        if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            start->Confirm(firstOffset + records.size());
        } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            for (const auto& message : data->GetMessages()) {
                records.push_back({TString(message.GetData()), message.GetOffset(),
                    TString(message.GetProducerId()), message.GetSeqNo()});
            }
            UNIT_ASSERT_LE(records.size(), count);
        } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
            UNIT_FAIL(closed->DebugString());
        }
        return records.size() == count;
    }, topic);
    UNIT_ASSERT(env.RunCall([&] { return reader->Close(TDuration::Seconds(10)); }));
    return records;
}

}
