#include "export_common.h"

#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/services/persqueue_v1/actors/schema_actors.h>
#include <ydb/library/persqueue/obfuscate/obfuscate.h>

#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NDataShard {

static void ResortColumns(
        google::protobuf::RepeatedPtrField<Ydb::Table::ColumnMeta>& columns,
        const TMap<ui32, TUserTable::TUserColumn>& order)
{
    THashMap<TString, ui32> nameToTag;
    for (const auto& [tag, column] : order) {
        Y_ABORT_UNLESS(nameToTag.emplace(column.Name, tag).second);
    }

    SortBy(columns, [&nameToTag](const auto& column) {
        auto it = nameToTag.find(column.name());
        Y_ABORT_UNLESS(it != nameToTag.end());
        return it->second;
    });
}

TMaybe<Ydb::Table::CreateTableRequest> GenYdbScheme(
        const TMap<ui32, TUserTable::TUserColumn>& columns,
        const NKikimrSchemeOp::TPathDescription& pathDesc)
{
    if (!pathDesc.HasTable()) {
        return Nothing();
    }

    Ydb::Table::CreateTableRequest scheme;

    const auto& tableDesc = pathDesc.GetTable();
    NKikimrMiniKQL::TType mkqlKeyType;

    try {
        FillColumnDescription(scheme, mkqlKeyType, tableDesc);
    } catch (const yexception&) {
        return Nothing();
    }

    ResortColumns(*scheme.mutable_columns(), columns);

    scheme.mutable_primary_key()->CopyFrom(tableDesc.GetKeyColumnNames());

    try {
        FillTableBoundary(scheme, tableDesc, mkqlKeyType);
        FillIndexDescription(scheme, tableDesc);
    } catch (const yexception&) {
        return Nothing();
    }

    FillStorageSettings(scheme, tableDesc);
    FillColumnFamilies(scheme, tableDesc);
    FillAttributes(scheme, pathDesc);
    FillPartitioningSettings(scheme, tableDesc);
    FillKeyBloomFilter(scheme, tableDesc);
    FillReadReplicasSettings(scheme, tableDesc);

    TString error;
    Ydb::StatusIds::StatusCode status;
    if (!FillSequenceDescription(scheme, tableDesc, status, error)) {
        return Nothing();
    }

    return scheme;
}

TMaybe<Ydb::Scheme::ModifyPermissionsRequest> GenYdbPermissions(const NKikimrSchemeOp::TPathDescription& pathDesc) {
    if (!pathDesc.HasSelf()) {
        return Nothing();
    }

    Ydb::Scheme::ModifyPermissionsRequest permissions;

    const auto& selfDesc = pathDesc.GetSelf();
    permissions.mutable_actions()->Add()->set_change_owner(selfDesc.GetOwner());

    NProtoBuf::RepeatedPtrField<Ydb::Scheme::Permissions> toGrant;
    ConvertAclToYdb(selfDesc.GetOwner(), selfDesc.GetACL(), false, &toGrant);
    for (const auto& permission : toGrant) {
        *permissions.mutable_actions()->Add()->mutable_grant() = permission;
    }

    return permissions;
}

bool FillConsumerProto(Ydb::Topic::Consumer *rr, const NKikimrPQ::TPQTabletConfig::TConsumer& consumer, Ydb::StatusIds::StatusCode& status, TString& error)
{
    // const auto& pqConfig = AppData(ctx)->PQConfig;

    auto consumerName = NPersQueue::ConvertOldConsumerName(consumer.GetName());
    rr->set_name(consumerName);
    rr->mutable_read_from()->set_seconds(consumer.GetReadFromTimestampsMs() / 1000);
    auto version = consumer.GetVersion();
    if (version != 0)
        (*rr->mutable_attributes())["_version"] = TStringBuilder() << version;
    for (const auto &codec : consumer.GetCodec().GetIds()) {
        rr->mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec) (codec + 1));
    }

    rr->set_important(consumer.GetImportant());
    TString serviceType = "";
    if (consumer.HasServiceType()) {
        serviceType = consumer.GetServiceType();
    } else {
        // if (pqConfig.GetDisallowDefaultClientServiceType()) {
        //     error = "service type must be set for all read rules";
        //     status = Ydb::StatusIds::INTERNAL_ERROR;
        //     return false;
        // }
        // serviceType = pqConfig.GetDefaultClientServiceType().GetName();
    }
    (*rr->mutable_attributes())["_service_type"] = serviceType;
    return true;
}

Ydb::Topic::DescribeTopicResult GenYdbDescribeTopicResult(const NKikimrSchemeOp::TPersQueueGroupDescription& persQueue) {

    Ydb::Topic::DescribeTopicResult topic;

    // Y_ABORT_UNLESS(ev->Get()->Request.Get()->ResultSet.size() == 1); // describe for only one topic
    // if (ReplyIfNotTopic(ev)) {
    //     return;
    // }

    // const auto& response = ev->Get()->Request.Get()->ResultSet.front();

    // const TString path = JoinSeq("/", response.Path);

    // Ydb::Scheme::Entry *selfEntry = Result.mutable_self();
    // ConvertDirectoryEntry(response.Self->Info, selfEntry, true);
    // if (const auto& name = GetCdcStreamName()) {
    //     selfEntry->set_name(*name);
    // }

    const auto& pqDescr = persQueue;
    for (auto& sourcePart: pqDescr.GetPartitions()) {
        auto destPart = topic.add_partitions();
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

    const auto &config = pqDescr.GetPQTabletConfig();

    topic.mutable_partitioning_settings()->set_max_active_partitions(config.GetPartitionStrategy().GetMaxPartitionCount());
    switch(config.GetPartitionStrategy().GetPartitionStrategyType()) {
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT:
            topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP);
            break;
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE:
            topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_SCALE_UP_AND_DOWN);
            break;
        case ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_PAUSED:
            topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_PAUSED);
            break;
        default:
            topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->set_strategy(Ydb::Topic::AutoPartitioningStrategy::AUTO_PARTITIONING_STRATEGY_DISABLED);
            break;
    }
    topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->mutable_stabilization_window()->set_seconds(config.GetPartitionStrategy().GetScaleThresholdSeconds());
    topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_down_utilization_percent(config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent());
    topic.mutable_partitioning_settings()->mutable_auto_partitioning_settings()->mutable_partition_write_speed()->set_up_utilization_percent(config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent());

    if (!config.GetRequireAuthWrite()) {
        (*topic.mutable_attributes())["_allow_unauthenticated_write"] = "true";
    }

    if (!config.GetRequireAuthRead()) {
        (*topic.mutable_attributes())["_allow_unauthenticated_read"] = "true";
    }

    if (pqDescr.GetPartitionPerTablet() != 2) {
        (*topic.mutable_attributes())["_partitions_per_tablet"] =
            TStringBuilder() << pqDescr.GetPartitionPerTablet();
    }
    if (config.HasAbcId()) {
        (*topic.mutable_attributes())["_abc_id"] = TStringBuilder() << config.GetAbcId();
    }
    if (config.HasAbcSlug()) {
        (*topic.mutable_attributes())["_abc_slug"] = config.GetAbcSlug();
    }
    if (config.HasFederationAccount()) {
        (*topic.mutable_attributes())["_federation_account"] = config.GetFederationAccount();
    }

    const auto &partConfig = config.GetPartitionConfig();
    i64 msip = partConfig.GetMaxSizeInPartition();
    if (partConfig.HasMaxSizeInPartition() && msip != Max<i64>()) {
        (*topic.mutable_attributes())["_max_partition_storage_size"] = TStringBuilder() << msip;
    }
    topic.mutable_retention_period()->set_seconds(partConfig.GetLifetimeSeconds());
    topic.set_retention_storage_mb(partConfig.GetStorageLimitBytes() / 1024 / 1024);
    (*topic.mutable_attributes())["_message_group_seqno_retention_period_ms"] = TStringBuilder() << (partConfig.GetSourceIdLifetimeSeconds() * 1000);
    (*topic.mutable_attributes())["__max_partition_message_groups_seqno_stored"] = TStringBuilder() << partConfig.GetSourceIdMaxCounts();

    // const auto& pqConfig = AppData(ActorContext())->PQConfig;

    // if (local || pqConfig.GetTopicsAreFirstClassCitizen()) {
    //     topic.set_partition_write_speed_bytes_per_second(partConfig.GetWriteSpeedInBytesPerSecond());
    //     topic.set_partition_write_burst_bytes(partConfig.GetBurstSize());
    // }

    // if (pqConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()) {
    //     auto readSpeedPerConsumer = partConfig.GetWriteSpeedInBytesPerSecond() * 2;
    //     topic.set_partition_total_read_speed_bytes_per_second(readSpeedPerConsumer * pqConfig.GetQuotingConfig().GetMaxParallelConsumersPerPartition());
    //     topic.set_partition_consumer_read_speed_bytes_per_second(readSpeedPerConsumer);
    // }

    for (const auto &codec : config.GetCodecs().GetIds()) {
        topic.mutable_supported_codecs()->add_codecs((Ydb::Topic::Codec)(codec + 1));
    }

    // if (pqConfig.GetBillingMeteringConfig().GetEnabled()) {
    //     switch (config.GetMeteringMode()) {
    //         case NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY:
    //             topic.set_metering_mode(Ydb::Topic::METERING_MODE_RESERVED_CAPACITY);
    //             break;
    //         case NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS:
    //             topic.set_metering_mode(Ydb::Topic::METERING_MODE_REQUEST_UNITS);
    //             break;
    //         default:
    //             break;
    //     }
    // }
    for (const auto& consumer : config.GetConsumers()) {
        auto rr = topic.add_consumers();
        Ydb::StatusIds::StatusCode status;
        TString error;
        FillConsumerProto(rr, consumer, status, error);
    }
}

} // NDataShard
} // NKikimr
