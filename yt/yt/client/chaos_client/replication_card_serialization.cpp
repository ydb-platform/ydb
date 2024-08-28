#include "replication_card_serialization.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NChaosClient {

using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

struct TSerializableSegment
    : public TYsonStruct
{
    TUnversionedOwningRow LowerKey;
    TTimestamp Timestamp;

    REGISTER_YSON_STRUCT(TSerializableSegment);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("lower_key", &TThis::LowerKey)
            .Default();
        registrar.Parameter("timestamp", &TThis::Timestamp)
            .Default();
    }
};

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicationProgress)

struct TSerializableReplicationProgress
    : public TYsonStruct
{
    std::vector<TIntrusivePtr<TSerializableSegment>> Segments;
    TUnversionedOwningRow UpperKey;

    REGISTER_YSON_STRUCT(TSerializableReplicationProgress);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("segments", &TThis::Segments)
            .Default();
        registrar.Parameter("upper_key", &TThis::UpperKey)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicationProgress)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicaInfo)

struct TSerializableReplicaInfo
    : public TYsonStruct
{
    TString ClusterName;
    NYPath::TYPath ReplicaPath;
    NTabletClient::ETableReplicaContentType ContentType;
    NTabletClient::ETableReplicaMode Mode;
    NTabletClient::ETableReplicaState State;
    TReplicationProgress ReplicationProgress;
    bool EnableReplicatedTableTracker;

    REGISTER_YSON_STRUCT(TSerializableReplicaInfo);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("cluster_name", &TThis::ClusterName)
            .NonEmpty();
        registrar.Parameter("replica_path", &TThis::ReplicaPath)
            .NonEmpty();
        registrar.Parameter("content_type", &TThis::ContentType);
        registrar.Parameter("mode", &TThis::Mode)
            .Default(ETableReplicaMode::Async);
        registrar.Parameter("state", &TThis::State)
            .Default(ETableReplicaState::Disabled);
        registrar.Parameter("replication_progress", &TThis::ReplicationProgress)
            .Default();
        registrar.Parameter("enable_replicated_table_tracker", &TThis::EnableReplicatedTableTracker)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicaInfo)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSerializableReplicationCard)

struct TSerializableReplicationCard
    : public NYTree::TYsonStruct
{
    THashMap<TString, TReplicaInfo> Replicas;
    std::vector<NObjectClient::TCellId> CoordinatorCellIds;
    TReplicationEra Era;
    TTableId TableId;
    TYPath TablePath;
    TString TableClusterName;
    NTransactionClient::TTimestamp CurrentTimestamp;
    NTabletClient::TReplicatedTableOptionsPtr ReplicatedTableOptions;
    TReplicationCardCollocationId ReplicationCardCollocationId;

    REGISTER_YSON_STRUCT(TSerializableReplicationCard);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("replicas", &TThis::Replicas);
        registrar.Parameter("coordinator_cell_ids", &TThis::CoordinatorCellIds)
            .Default();
        registrar.Parameter("era", &TThis::Era)
            .Default(0);
        registrar.Parameter("table_id", &TThis::TableId)
            .Default();
        registrar.Parameter("table_path", &TThis::TablePath)
            .Default();
        registrar.Parameter("table_cluster_name", &TThis::TableClusterName)
            .Default();
        registrar.Parameter("current_timestamp", &TThis::CurrentTimestamp)
            .Default();
        registrar.Parameter("replicated_table_options", &TThis::ReplicatedTableOptions)
            .Default();
        registrar.Parameter("replication_card_collocation_id", &TThis::ReplicationCardCollocationId)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializableReplicationCard)

////////////////////////////////////////////////////////////////////////////////

void DeserializeImpl(TReplicationProgress& replicationProgress, TSerializableReplicationProgressPtr serializable)
{
    replicationProgress.UpperKey = std::move(serializable->UpperKey);
    replicationProgress.Segments.reserve(serializable->Segments.size());

    for (auto& segment : serializable->Segments) {
        replicationProgress.Segments.push_back({
            .LowerKey = std::move(segment->LowerKey),
            .Timestamp = segment->Timestamp
        });
    }
}

void DeserializeImpl(TReplicaInfo& replicaInfo, TSerializableReplicaInfoPtr serializable)
{
    replicaInfo.ClusterName = serializable->ClusterName;
    replicaInfo.ReplicaPath = serializable->ReplicaPath;
    replicaInfo.ContentType = serializable->ContentType;
    replicaInfo.Mode = serializable->Mode;
    replicaInfo.State = serializable->State;
    replicaInfo.ReplicationProgress = std::move(serializable->ReplicationProgress);
}

void DeserializeImpl(TReplicationCard& replicationCard, TSerializableReplicationCardPtr serializable)
{
    replicationCard.Replicas.clear();
    for (const auto& [replicaId, replicaInfo] : serializable->Replicas) {
        EmplaceOrCrash(replicationCard.Replicas, TReplicaId::FromString(replicaId), replicaInfo);
    }
    replicationCard.CoordinatorCellIds = std::move(serializable->CoordinatorCellIds);
    replicationCard.Era = serializable->Era;
    replicationCard.TableId = serializable->TableId;
    replicationCard.TablePath = serializable->TablePath;
    replicationCard.TableClusterName = serializable->TableClusterName;
    replicationCard.CurrentTimestamp = serializable->CurrentTimestamp;
    replicationCard.ReplicatedTableOptions = serializable->ReplicatedTableOptions;
    replicationCard.ReplicationCardCollocationId = serializable->ReplicationCardCollocationId;
}

void Deserialize(TReplicationProgress& replicationProgress, INodePtr node)
{
    DeserializeImpl(replicationProgress, ConvertTo<TSerializableReplicationProgressPtr>(node));
}

void Deserialize(TReplicaInfo& replicaInfo, INodePtr node)
{
    DeserializeImpl(replicaInfo, ConvertTo<TSerializableReplicaInfoPtr>(node));
}

void Deserialize(TReplicationCard& replicationCard, INodePtr node)
{
    DeserializeImpl(replicationCard, ConvertTo<TSerializableReplicationCardPtr>(node));
}

void Deserialize(TReplicationProgress& replicationProgress, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicationProgress, ExtractTo<TSerializableReplicationProgressPtr>(cursor));
}

void Deserialize(TReplicaInfo& replicaInfo, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicaInfo, ExtractTo<TSerializableReplicaInfoPtr>(cursor));
}

void Deserialize(TReplicationCard& replicationCard, TYsonPullParserCursor* cursor)
{
    DeserializeImpl(replicationCard, ExtractTo<TSerializableReplicationCardPtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReplicationProgress& replicationProgress, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("segments").DoListFor(replicationProgress.Segments, [] (auto fluent, const auto& segment) {
                fluent
                    .Item().BeginMap()
                        .Item("lower_key").Value(segment.LowerKey ? segment.LowerKey : EmptyKey())
                        .Item("timestamp").Value(segment.Timestamp)
                    .EndMap();
            })
            .Item("upper_key").Value(replicationProgress.UpperKey ? replicationProgress.UpperKey : EmptyKey())
        .EndMap();
}

void Serialize(const TReplicaHistoryItem& replicaHistoryItem, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("era").Value(replicaHistoryItem.Era)
            .Item("timestamp").Value(replicaHistoryItem.Timestamp)
            .Item("mode").Value(replicaHistoryItem.Mode)
            .Item("state").Value(replicaHistoryItem.State)
        .EndMap();
}

void Serialize(
    const TReplicaInfo& replicaInfo,
    TFluentMap fluent,
    const TReplicationCardFetchOptions& options)
{
    fluent
        .Item("cluster_name").Value(replicaInfo.ClusterName)
        .Item("replica_path").Value(replicaInfo.ReplicaPath)
        .Item("content_type").Value(replicaInfo.ContentType)
        .Item("mode").Value(replicaInfo.Mode)
        .Item("state").Value(replicaInfo.State)
        .Item("enable_replicated_table_tracker").Value(replicaInfo.EnableReplicatedTableTracker)
        .DoIf(options.IncludeProgress, [&] (auto fluent) {
            fluent
                .Item("replication_progress").Value(replicaInfo.ReplicationProgress);
        })
        .DoIf(options.IncludeHistory, [&] (auto fluent) {
            fluent
                .Item("history").Value(replicaInfo.History);
        });
}

void Serialize(
    const TReplicaInfo& replicaInfo,
    IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                Serialize(replicaInfo, fluent, options);
            })
        .EndMap();
}

void Serialize(
    const TReplicationCard& replicationCard,
    IYsonConsumer* consumer,
    const TReplicationCardFetchOptions& options)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Do([&] (auto fluent) {
                Serialize(replicationCard, fluent, options);
            })
        .EndMap();
}

void Serialize(
    const TReplicationCard& replicationCard,
    TFluentMap fluent,
    const TReplicationCardFetchOptions& options)
{
    fluent
        .Item("replicas").DoMapFor(replicationCard.Replicas, [&] (auto fluent, const auto& pair) {
            fluent
                .Item(ToString(pair.first)).Do([&] (auto fluent) {
                    Serialize(pair.second, fluent.GetConsumer(), options);
                });
        })
        .DoIf(options.IncludeCoordinators, [&] (auto fluent) {
            fluent
                .Item("coordinator_cell_ids").Value(replicationCard.CoordinatorCellIds);
        })
        .DoIf(options.IncludeReplicatedTableOptions && replicationCard.ReplicatedTableOptions, [&] (auto fluent) {
            fluent
                .Item("replicated_table_options").Value(replicationCard.ReplicatedTableOptions);
        })
        .Item("era").Value(replicationCard.Era)
        .Item("table_id").Value(replicationCard.TableId)
        .Item("table_path").Value(replicationCard.TablePath)
        .Item("table_cluster_name").Value(replicationCard.TableClusterName)
        .Item("current_timestamp").Value(replicationCard.CurrentTimestamp)
        .Item("replication_card_collocation_id").Value(replicationCard.ReplicationCardCollocationId);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NChaosClient::NProto::TReplicationProgress::TSegment* protoSegment, const TReplicationProgress::TSegment& segment)
{
    ToProto(protoSegment->mutable_lower_key(), segment.LowerKey);
    protoSegment->set_timestamp(segment.Timestamp);
}

void FromProto(TReplicationProgress::TSegment* segment, const NChaosClient::NProto::TReplicationProgress::TSegment& protoSegment)
{
    segment->LowerKey = FromProto<TUnversionedOwningRow>(protoSegment.lower_key());
    segment->Timestamp = protoSegment.timestamp();
}

void ToProto(NChaosClient::NProto::TReplicationProgress* protoReplicationProgress, const TReplicationProgress& replicationProgress)
{
    ToProto(protoReplicationProgress->mutable_segments(), replicationProgress.Segments);
    ToProto(protoReplicationProgress->mutable_upper_key(), replicationProgress.UpperKey);
}

void FromProto(TReplicationProgress* replicationProgress, const NChaosClient::NProto::TReplicationProgress& protoReplicationProgress)
{
    FromProto(&replicationProgress->Segments, protoReplicationProgress.segments());
    FromProto(&replicationProgress->UpperKey, protoReplicationProgress.upper_key());
}

void ToProto(NChaosClient::NProto::TReplicaHistoryItem* protoHistoryItem, const TReplicaHistoryItem& historyItem)
{
    protoHistoryItem->set_era(historyItem.Era);
    protoHistoryItem->set_timestamp(ToProto<ui64>(historyItem.Timestamp));
    protoHistoryItem->set_mode(ToProto<int>(historyItem.Mode));
    protoHistoryItem->set_state(ToProto<int>(historyItem.State));
}

void FromProto(TReplicaHistoryItem* historyItem, const NChaosClient::NProto::TReplicaHistoryItem& protoHistoryItem)
{
    historyItem->Era = protoHistoryItem.era();
    historyItem->Timestamp = FromProto<TTimestamp>(protoHistoryItem.timestamp());
    historyItem->Mode = FromProto<ETableReplicaMode>(protoHistoryItem.mode());
    historyItem->State = FromProto<ETableReplicaState>(protoHistoryItem.state());
}

void ToProto(
    NChaosClient::NProto::TReplicaInfo* protoReplicaInfo,
    const TReplicaInfo& replicaInfo,
    const TReplicationCardFetchOptions& options)
{
    protoReplicaInfo->set_cluster_name(ToProto<TProtobufString>(replicaInfo.ClusterName));
    protoReplicaInfo->set_replica_path(replicaInfo.ReplicaPath);
    protoReplicaInfo->set_content_type(ToProto<int>(replicaInfo.ContentType));
    protoReplicaInfo->set_mode(ToProto<int>(replicaInfo.Mode));
    protoReplicaInfo->set_state(ToProto<int>(replicaInfo.State));
    protoReplicaInfo->set_enable_replicated_table_tracker(replicaInfo.EnableReplicatedTableTracker);
    if (options.IncludeProgress) {
        ToProto(protoReplicaInfo->mutable_progress(), replicaInfo.ReplicationProgress);
    }
    if (options.IncludeHistory) {
        ToProto(protoReplicaInfo->mutable_history(), replicaInfo.History);
    }
}

void FromProto(TReplicaInfo* replicaInfo, const NChaosClient::NProto::TReplicaInfo& protoReplicaInfo)
{
    replicaInfo->ClusterName = protoReplicaInfo.cluster_name();
    replicaInfo->ReplicaPath = protoReplicaInfo.replica_path();
    replicaInfo->ContentType = FromProto<ETableReplicaContentType>(protoReplicaInfo.content_type());
    replicaInfo->Mode = FromProto<ETableReplicaMode>(protoReplicaInfo.mode());
    replicaInfo->State = FromProto<ETableReplicaState>(protoReplicaInfo.state());
    if (protoReplicaInfo.has_progress()) {
        FromProto(&replicaInfo->ReplicationProgress, protoReplicaInfo.progress());
    }
    FromProto(&replicaInfo->History, protoReplicaInfo.history());
    if (protoReplicaInfo.has_enable_replicated_table_tracker()) {
        replicaInfo->EnableReplicatedTableTracker = protoReplicaInfo.enable_replicated_table_tracker();
    }
}

void ToProto(
    NChaosClient::NProto::TReplicationCard* protoReplicationCard,
    const TReplicationCard& replicationCard,
    const TReplicationCardFetchOptions& options)
{
    for (const auto& [replicaId, replicaInfo] : SortHashMapByKeys(replicationCard.Replicas)) {
        auto* protoReplicaEntry = protoReplicationCard->add_replicas();
        ToProto(protoReplicaEntry->mutable_id(), replicaId);
        ToProto(protoReplicaEntry->mutable_info(), replicaInfo, options);
    }
    if (options.IncludeCoordinators) {
        ToProto(protoReplicationCard->mutable_coordinator_cell_ids(), replicationCard.CoordinatorCellIds);
    }
    if (options.IncludeReplicatedTableOptions && replicationCard.ReplicatedTableOptions) {
        protoReplicationCard->set_replicated_table_options(ConvertToYsonString(replicationCard.ReplicatedTableOptions).ToString());
    }
    protoReplicationCard->set_era(replicationCard.Era);
    ToProto(protoReplicationCard->mutable_table_id(), replicationCard.TableId);
    protoReplicationCard->set_table_path(replicationCard.TablePath);
    protoReplicationCard->set_table_cluster_name(ToProto<TProtobufString>(replicationCard.TableClusterName));
    protoReplicationCard->set_current_timestamp(replicationCard.CurrentTimestamp);
    ToProto(protoReplicationCard->mutable_replication_card_collocation_id(), replicationCard.ReplicationCardCollocationId);
}

void FromProto(TReplicationCard* replicationCard, const NChaosClient::NProto::TReplicationCard& protoReplicationCard)
{
    for (const auto& protoEntry : protoReplicationCard.replicas()) {
        auto replicaId = FromProto<TReplicaId>(protoEntry.id());
        auto& replicaInfo = EmplaceOrCrash(replicationCard->Replicas, replicaId, TReplicaInfo())->second;
        FromProto(&replicaInfo, protoEntry.info());
    }
    FromProto(&replicationCard->CoordinatorCellIds, protoReplicationCard.coordinator_cell_ids());
    replicationCard->Era = protoReplicationCard.era();
    replicationCard->TableId = FromProto<TTableId>(protoReplicationCard.table_id());
    replicationCard->TablePath = protoReplicationCard.table_path();
    replicationCard->TableClusterName = protoReplicationCard.table_cluster_name();
    replicationCard->CurrentTimestamp = protoReplicationCard.current_timestamp();
    if (protoReplicationCard.has_replicated_table_options()) {
        replicationCard->ReplicatedTableOptions = ConvertTo<TReplicatedTableOptionsPtr>(TYsonString(protoReplicationCard.replicated_table_options()));
    }
    if (protoReplicationCard.has_replication_card_collocation_id()) {
        FromProto(&replicationCard->ReplicationCardCollocationId, protoReplicationCard.replication_card_collocation_id());
    }
}

void ToProto(
    NChaosClient::NProto::TReplicationCardFetchOptions* protoOptions,
    const TReplicationCardFetchOptions& options)
{
    protoOptions->set_include_coordinators(options.IncludeCoordinators);
    protoOptions->set_include_progress(options.IncludeProgress);
    protoOptions->set_include_history(options.IncludeHistory);
    protoOptions->set_include_replicated_table_options(options.IncludeReplicatedTableOptions);
}

void FromProto(
    TReplicationCardFetchOptions* options,
    const NChaosClient::NProto::TReplicationCardFetchOptions& protoOptions)
{
    options->IncludeCoordinators = protoOptions.include_coordinators();
    options->IncludeProgress = protoOptions.include_progress();
    options->IncludeHistory = protoOptions.include_history();
    options->IncludeReplicatedTableOptions = protoOptions.include_replicated_table_options();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
