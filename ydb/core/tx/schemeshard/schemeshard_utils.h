#pragma once

#include "schemeshard.h"
#include "schemeshard_types.h"
#include "schemeshard_info_types.h"

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/table_index.h>

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <ydb/library/yql/minikql/mkql_type_ops.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/intrlist.h>

namespace NKikimr {
namespace NSchemeShard {

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

inline bool IsValidColumnName(const TString& name) {
    for (auto c: name) {
        if (!std::isalnum(c) && c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

inline NKikimrSchemeOp::TModifyScheme TransactionTemplate(const TString& workingDir, NKikimrSchemeOp::EOperationType type) {
    NKikimrSchemeOp::TModifyScheme tx;
    tx.SetWorkingDir(workingDir);
    tx.SetOperationType(type);

    return tx;
}

TSerializedCellVec ChooseSplitKeyByHistogram(const NKikimrTableStats::THistogram& histogram,
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
    void SheduleSelfPingWakeup(const TActorContext &ctx);

private:
    const TTabletId TabletId;
    TTabletCountersBase * const TabletCounters;

    TDuration LastResponseTime;
    TInstant SelfPingSentTime;
    bool SelfPingInFlight;
    TInstant SelfPingWakeupScheduledTime;
    bool SelfPingWakeupScheduled;
};

}

namespace NTableIndex {

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NTableIndex::TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TTableDescription CalcImplTableDesc(
    const NKikimrSchemeOp::TTableDescription& baseTableDesrc,
    const NTableIndex::TTableColumns& implTableColumns,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TPartitionConfig PartitionConfigForIndexes(
    const NSchemeShard::TTableInfo::TPtr& baseTableInfo,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

NKikimrSchemeOp::TPartitionConfig PartitionConfigForIndexes(
    const NKikimrSchemeOp::TTableDescription& baseTableDesrc,
    const NKikimrSchemeOp::TTableDescription& indexTableDesc);

TTableColumns ExtractInfo(const NKikimrSchemeOp::TTableDescription& tableDesrc);
TIndexColumns ExtractInfo(const NKikimrSchemeOp::TIndexCreationConfig& indexDesc);
TTableColumns ExtractInfo(const NSchemeShard::TTableInfo::TPtr& tableInfo);

using TColumnTypes = THashMap<TString, NScheme::TTypeInfo>;

bool ExtractTypes(const NKikimrSchemeOp::TTableDescription& baseTableDesrc, TColumnTypes& columsTypes, TString& explain);
TColumnTypes ExtractTypes(const NSchemeShard::TTableInfo::TPtr& baseTableInfo);

bool IsCompatibleKeyTypes(
    const TColumnTypes& baseTableColumsTypes,
    const TTableColumns& implTableColumns,
    bool uniformTable,
    TString& explain);
}

}
