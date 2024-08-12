#pragma once

#include "schemeshard.h"
#include "schemeshard_types.h"
#include "schemeshard_info_types.h"

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/table_stats.pb.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NKikimr {
namespace NSchemeShard {

inline constexpr TStringBuf SYSTEM_COLUMN_PREFIX = "__ydb_";

inline bool IsAllowedKeyType(NScheme::TTypeInfo typeInfo) {
    switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::Float:
        case NScheme::NTypeIds::Double:
        case NScheme::NTypeIds::JsonDocument:
            return false;
        case NScheme::NTypeIds::Pg:
            return NPg::TypeDescIsComparable(typeInfo.GetTypeDesc());
        default:
            return true;
    }
}

inline bool IsValidSystemColumnName(const TString& name) {
    for (auto c: name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

inline bool IsValidColumnName(const TString& name) {
    return IsValidSystemColumnName(name) && !name.StartsWith(SYSTEM_COLUMN_PREFIX);
}

inline NKikimrSchemeOp::TModifyScheme TransactionTemplate(const TString& workingDir, NKikimrSchemeOp::EOperationType type) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetWorkingDir(workingDir);
    tx.SetOperationType(type);

    return tx;
}

TSerializedCellVec ChooseSplitKeyByHistogram(const NKikimrTableStats::THistogram& histogram, ui64 total,
                                  const TConstArrayRef<NScheme::TTypeInfo>& keyColumnTypes);

class TShardDeleter {
    struct TPerHiveDeletions {
        TActorId PipeToHive;
        THashSet<TShardIdx> ShardsToDelete;
    };

    ui64 MyTabletID;
    // Hive TabletID -> non-acked deletions
    THashMap<TTabletId, TPerHiveDeletions> PerHiveDeletions;
    // Tablet -> Hive TabletID
    THashMap<TShardIdx, TTabletId> ShardHive;
    NTabletPipe::TClientRetryPolicy HivePipeRetryPolicy;

public:
    explicit TShardDeleter(ui64 myTabletId)
        : MyTabletID(myTabletId)
        , HivePipeRetryPolicy({})
    {}

    TShardDeleter(const TShardDeleter&) = delete;
    TShardDeleter& operator=(const TShardDeleter&) = delete;

    void Shutdown(const TActorContext& ctx);
    void SendDeleteRequests(TTabletId hiveTabletId, const THashSet<TShardIdx>& shardsToDelete,
                            const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ResendDeleteRequests(TTabletId hiveTabletId,
                              const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ResendDeleteRequest(TTabletId hiveTabletId,
                             const THashMap<TShardIdx, TShardInfo>& shardsInfos, TShardIdx shardIdx, const TActorContext& ctx);
    void RedirectDeleteRequest(TTabletId hiveFromTabletId, TTabletId hiveToTabletId, TShardIdx shardIdx,
                               const THashMap<TShardIdx, TShardInfo>& shardsInfos, const TActorContext& ctx);
    void ShardDeleted(TShardIdx shardIdx, const TActorContext& ctx);
    bool Has(TTabletId hiveTabletId, TActorId pipeClientActorId) const;
    bool Has(TShardIdx shardIdx) const;
    bool Empty() const;
};

// Self ping stuff
class TSelfPinger {
private:
    static constexpr TDuration SELF_PING_INTERVAL = TDuration::MilliSeconds(1000);

public:
    TSelfPinger(TTabletId id, TTabletCountersBase* counters)
        : TabletId(id)
        , TabletCounters(counters)
        , SelfPingInFlight(false)
        , SelfPingWakeupScheduled(false)
    {}

    void Handle(TEvSchemeShard::TEvMeasureSelfResponseTime::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSchemeShard::TEvWakeupToMeasureSelfResponseTime::TPtr &ev, const TActorContext &ctx);
    void OnAnyEvent(const TActorContext &ctx);
    void DoSelfPing(const TActorContext &ctx);
    void ScheduleSelfPingWakeup(const TActorContext &ctx);

private:
    const TTabletId TabletId;
    TTabletCountersBase * const TabletCounters;

    TDuration LastResponseTime;
    TInstant SelfPingSentTime;
    bool SelfPingInFlight;
    TInstant SelfPingWakeupScheduledTime;
    bool SelfPingWakeupScheduled;
};

class PQGroupReserve {
public:
    PQGroupReserve(const ::NKikimrPQ::TPQTabletConfig& tabletConfig, ui64 partitions);

    ui64 Storage;
    ui64 Throughput;
};

} // NSchemeShard

namespace NTableIndex {

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDesc,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreeLevelImplTableDesc(
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix = {});

NKikimrSchemeOp::TTableDescription CalcVectorKmeansTreePostingImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDescr,
    const NKikimrSchemeOp::TPartitionConfig& baseTablePartitionConfig,
    const TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc,
    std::string_view suffix = {});

TTableColumns ExtractInfo(const NSchemeShard::TTableInfo::TPtr& tableInfo);
TTableColumns ExtractInfo(const NKikimrSchemeOp::TTableDescription& tableDesc);
TIndexColumns ExtractInfo(const NKikimrSchemeOp::TIndexCreationConfig& indexDesc);

using TColumnTypes = THashMap<TString, NScheme::TTypeInfo>;

bool ExtractTypes(const NSchemeShard::TTableInfo::TPtr& baseTableInfo, TColumnTypes& columnsTypes, TString& explain);
bool ExtractTypes(const NKikimrSchemeOp::TTableDescription& baseTableDesc, TColumnTypes& columnsTypes, TString& explain);

bool IsCompatibleKeyTypes(
    const TColumnTypes& baseTableColumnsTypes,
    const TTableColumns& implTableColumns,
    bool uniformTable,
    TString& explain);

template <typename TTableDesc>
bool CommonCheck(const TTableDesc& tableDesc, const NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
        const NSchemeShard::TSchemeLimits& schemeLimits, bool uniformTable,
        TTableColumns& implTableColumns, NKikimrScheme::EStatus& status, TString& error)
{
    const TTableColumns baseTableColumns = ExtractInfo(tableDesc);
    const TIndexColumns indexKeys = ExtractInfo(indexDesc);

    if (indexKeys.KeyColumns.empty()) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        error = "No key columns in index creation config";
        return false;
    }

    if (!indexKeys.DataColumns.empty() && !AppData()->FeatureFlags.GetEnableDataColumnForIndexTable()) {
        status = NKikimrScheme::EStatus::StatusPreconditionFailed;
        error = "It is not allowed to create index with data column";
        return false;
    }

    if (!IsCompatibleIndex(indexDesc.GetType(), baseTableColumns, indexKeys, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    TColumnTypes baseColumnTypes;
    if (!ExtractTypes(tableDesc, baseColumnTypes, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    implTableColumns = CalcTableImplDescription(indexDesc.GetType(), baseTableColumns, indexKeys);

    if (indexDesc.GetType() == NKikimrSchemeOp::EIndexType::EIndexTypeGlobalVectorKmeansTree) {
        //We have already checked this in IsCompatibleIndex
        Y_ABORT_UNLESS(indexKeys.KeyColumns.size() == 1);

        const TString& indexColumnName = indexKeys.KeyColumns[0];
        Y_ABORT_UNLESS(baseColumnTypes.contains(indexColumnName));
        auto typeInfo = baseColumnTypes.at(indexColumnName);

        if (typeInfo.GetTypeId() != NScheme::NTypeIds::String) {
            status = NKikimrScheme::EStatus::StatusInvalidParameter;
            error = TStringBuilder() << "Index column '" << indexColumnName << "' expected type 'String' but got " << NScheme::TypeName(typeInfo); 
            return false;
        }
    } else if (!IsCompatibleKeyTypes(baseColumnTypes, implTableColumns, uniformTable, error)) {
        status = NKikimrScheme::EStatus::StatusInvalidParameter;
        return false;
    }

    if (implTableColumns.Keys.size() > schemeLimits.MaxTableKeyColumns) {
        status = NKikimrScheme::EStatus::StatusSchemeError;
        error = TStringBuilder()
            << "Too many keys indexed, index table reaches the limit of the maximum key columns count"
            << ": indexing columns: " << indexKeys.KeyColumns.size()
            << ", requested keys columns for index table: " << implTableColumns.Keys.size()
            << ", limit: " << schemeLimits.MaxTableKeyColumns;
        return false;
    }

    return true;
}

template <typename TTableDesc>
bool CommonCheck(const TTableDesc& tableDesc, const NKikimrSchemeOp::TIndexCreationConfig& indexDesc,
        const NSchemeShard::TSchemeLimits& schemeLimits, TString& error)
{
    TTableColumns implTableColumns;
    NKikimrScheme::EStatus status;
    return CommonCheck(tableDesc, indexDesc, schemeLimits, false, implTableColumns, status, error);
}

} // NTableIndex
} // NKikimr
