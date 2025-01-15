#include "topic_description.h"

#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/persqueue/utils.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/services/lib/actors/pq_schema_actor.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/builder.h>

namespace NKikimr {

bool FillConsumer(Ydb::Topic::Consumer& rr, const NKikimrPQ::TPQTabletConfig_TConsumer& consumer,
    Ydb::StatusIds_StatusCode& status, TString& error)
{
    const NKikimrPQ::TPQConfig pqConfig = AppData()->PQConfig;
    auto consumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName(), pqConfig);
    rr.set_name(consumerName);
    rr.mutable_read_from()->set_seconds(consumer.GetReadFromTimestampsMs() / 1000);
    auto version = consumer.GetVersion();
    if (version != 0)
        (*rr.mutable_attributes())["_version"] = TStringBuilder() << version;
    for (const auto &codec : consumer.GetCodec().GetIds()) {
        rr.mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec) (codec + 1));
    }

    rr.set_important(consumer.GetImportant());
    TString serviceType = "";
    if (consumer.HasServiceType()) {
        serviceType = consumer.GetServiceType();
    } else {
        if (pqConfig.GetDisallowDefaultClientServiceType()) {
            error = "service type must be set for all read rules";
            status = Ydb::StatusIds::INTERNAL_ERROR;
            return false;
        }
        serviceType = pqConfig.GetDefaultClientServiceType().GetName();
    }
    (*rr.mutable_attributes())["_service_type"] = serviceType;
    return true;
}

void FillTopicDescription(Ydb::Topic::DescribeTopicResult& out, const NKikimrSchemeOp::TPersQueueGroupDescription& in,
    const NKikimrSchemeOp::TDirEntry &fromDirEntry, const TMaybe<TString>& cdcName) {
    
    const NKikimrPQ::TPQConfig pqConfig = AppData()->PQConfig;

    Ydb::Scheme::Entry *selfEntry = out.mutable_self();
    ConvertDirectoryEntry(fromDirEntry, selfEntry, true);
    if (cdcName) {
        selfEntry->set_name(*cdcName);
    }

    for (auto& sourcePart: in.GetPartitions()) {
        auto destPart = out.add_partitions();
        destPart->set_partition_id(sourcePart.GetPartitionId());
        destPart->set_active(sourcePart.GetStatus() == ::NKikimrPQ::ETopicPartitionStatus::Active);
        if (sourcePart.HasKeyRange()) {
            if (sourcePart.GetKeyRange().HasFromBound()) {
                destPart->mutable_key_range()->set_from_bound(sourcePart.GetKeyRange().GetFromBound());
            }
            if (sourcePart.GetKeyRange().HasToBound()) {
                destPart->mutable_key_range()->set_to_bound(sourcePart.GetKeyRange().GetToBound());
            }
        }

        for (size_t i = 0; i < sourcePart.ChildPartitionIdsSize(); ++i) {
            destPart->add_child_partition_ids(static_cast<int64_t>(sourcePart.GetChildPartitionIds(i)));
        }

        for (size_t i = 0; i < sourcePart.ParentPartitionIdsSize(); ++i) {
            destPart->add_parent_partition_ids(static_cast<int64_t>(sourcePart.GetParentPartitionIds(i)));
        }
    }

    const auto &config = in.GetPQTabletConfig();
    if (AppData()->FeatureFlags.GetEnableTopicSplitMerge() && NPQ::SplitMergeEnabled(config)) {
        out.mutable_partitioning_settings()->set_min_active_partitions(config.GetPartitionStrategy().GetMinPartitionCount());
    } else {
        out.mutable_partitioning_settings()->set_min_active_partitions(in.GetTotalGroupCount());
    }

    out.mutable_partitioning_settings()->set_max_active_partitions(config.GetPartitionStrategy().GetMaxPartitionCount());
    switch(config.GetPartitionStrategy().GetPartitionStrategyType()) {
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT:
            out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
            break;
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE:
            out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
            break;
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED:
            out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED);
            break;
        default:
            out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);
            break;
    }
    out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(config.GetPartitionStrategy().GetScaleThresholdSeconds());
    out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_down_utilization_percent(config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent());
    out.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_up_utilization_percent(config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent());

    if (!config.GetRequireAuthWrite()) {
        (*out.mutable_attributes())["_allow_unauthenticated_write"] = "true";
    }

    if (!config.GetRequireAuthRead()) {
        (*out.mutable_attributes())["_allow_unauthenticated_read"] = "true";
    }

    if (in.GetPartitionPerTablet() != 2) {
        (*out.mutable_attributes())["_partitions_per_tablet"] =
            TStringBuilder() << in.GetPartitionPerTablet();
    }
    if (config.HasAbcId()) {
        (*out.mutable_attributes())["_abc_id"] = TStringBuilder() << config.GetAbcId();
    }
    if (config.HasAbcSlug()) {
        (*out.mutable_attributes())["_abc_slug"] = config.GetAbcSlug();
    }
    if (config.HasFederationAccount()) {
        (*out.mutable_attributes())["_federation_account"] = config.GetFederationAccount();
    }
    bool local = config.GetLocalDC();
    const auto &partConfig = config.GetPartitionConfig();
    i64 msip = partConfig.GetMaxSizeInPartition();
    if (partConfig.HasMaxSizeInPartition() && msip != Max<i64>()) {
        (*out.mutable_attributes())["_max_partition_storage_size"] = TStringBuilder() << msip;
    }
    out.mutable_retention_period()->set_seconds(partConfig.GetLifetimeSeconds());
    out.set_retention_storage_mb(partConfig.GetStorageLimitBytes() / 1024 / 1024);
    (*out.mutable_attributes())["_message_group_seqno_retention_period_ms"] = TStringBuilder() << (partConfig.GetSourceIdLifetimeSeconds() * 1000);
    (*out.mutable_attributes())["__max_partition_message_groups_seqno_stored"] = TStringBuilder() << partConfig.GetSourceIdMaxCounts();

    if (local || pqConfig.GetTopicsAreFirstClassCitizen()) {
        out.set_partition_write_speed_bytes_per_second(partConfig.GetWriteSpeedInBytesPerSecond());
        out.set_partition_write_burst_bytes(partConfig.GetBurstSize());
    }

    if (pqConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()) {
        auto readSpeedPerConsumer = partConfig.GetWriteSpeedInBytesPerSecond() * 2;
        out.set_partition_total_read_speed_bytes_per_second(readSpeedPerConsumer * pqConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition());
        out.set_partition_consumer_read_speed_bytes_per_second(readSpeedPerConsumer);
    }

    for (const auto &codec : config.GetCodecs().GetIds()) {
        out.mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec)(codec + 1));
    }

    if (pqConfig.GetBillingMeteringConfig().GetEnabled()) {
        switch (config.GetMeteringMode()) {
            case NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY:
                out.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
                break;
            case NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS:
                out.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
                break;
            default:
                break;
        }
    }
}

} // namespace NKikimr
