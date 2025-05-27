#include "scan.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>

#include <ydb/core/sys_view/auth/group_members.h>
#include <ydb/core/sys_view/auth/groups.h>
#include <ydb/core/sys_view/auth/owners.h>
#include <ydb/core/sys_view/auth/permissions.h>
#include <ydb/core/sys_view/auth/users.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/sys_view/nodes/nodes.h>
#include <ydb/core/sys_view/partition_stats/partition_stats.h>
#include <ydb/core/sys_view/partition_stats/top_partitions.h>
#include <ydb/core/sys_view/pg_tables/pg_tables.h>
#include <ydb/core/sys_view/query_stats/query_metrics.h>
#include <ydb/core/sys_view/query_stats/query_stats.h>
#include <ydb/core/sys_view/resource_pool_classifiers/resource_pool_classifiers.h>
#include <ydb/core/sys_view/resource_pools/resource_pools.h>
#include <ydb/core/sys_view/sessions/sessions.h>
#include <ydb/core/sys_view/show_create/show_create.h>
#include <ydb/core/sys_view/storage/groups.h>
#include <ydb/core/sys_view/storage/pdisks.h>
#include <ydb/core/sys_view/storage/storage_pools.h>
#include <ydb/core/sys_view/storage/storage_stats.h>
#include <ydb/core/sys_view/storage/vslots.h>
#include <ydb/core/sys_view/tablets/tablets.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NSysView {

namespace {
    using NKikimrSysView::ESysViewType;

    const THashMap<TStringBuf, ESysViewType> SYS_VIEW_TYPES_MAP = {
        {PartitionStatsName, ESysViewType::EPartitionStats},
        {NodesName, ESysViewType::ENodes},

        {TopQueriesByDuration1MinuteName, ESysViewType::ETopQueriesByDurationOneMinute},
        {TopQueriesByDuration1HourName, ESysViewType::ETopQueriesByDurationOneHour},
        {TopQueriesByReadBytes1MinuteName, ESysViewType::ETopQueriesByReadBytesOneMinute},
        {TopQueriesByReadBytes1HourName, ESysViewType::ETopQueriesByReadBytesOneHour},
        {TopQueriesByCpuTime1MinuteName, ESysViewType::ETopQueriesByCpuTimeOneMinute},
        {TopQueriesByCpuTime1HourName, ESysViewType::ETopQueriesByCpuTimeOneHour},
        {TopQueriesByRequestUnits1MinuteName, ESysViewType::ETopQueriesByRequestUnitsOneMinute},
        {TopQueriesByRequestUnits1HourName, ESysViewType::ETopQueriesByRequestUnitsOneHour},

        {QuerySessions, ESysViewType::EQuerySessions},

        {PDisksName, ESysViewType::EPDisks},
        {VSlotsName, ESysViewType::EVSlots},
        {GroupsName, ESysViewType::EGroups},
        {StoragePoolsName, ESysViewType::EStoragePools},
        {StorageStatsName, ESysViewType::EStorageStats},

        {TabletsName, ESysViewType::ETablets},

        {QueryMetricsName, ESysViewType::EQueryMetricsOneMinute},

        {TopPartitionsByCpu1MinuteName, ESysViewType::ETopPartitionsByCpuOneMinute},
        {TopPartitionsByCpu1HourName, ESysViewType::ETopPartitionsByCpuOneHour},
        {TopPartitionsByTli1MinuteName, ESysViewType::ETopPartitionsByTliOneMinute},
        {TopPartitionsByTli1HourName, ESysViewType::ETopPartitionsByTliOneHour},

        {PgTablesName, ESysViewType::EPgTables},
        {InformationSchemaTablesName, ESysViewType::EInformationSchemaTables},
        {PgClassName, ESysViewType::EPgClass},

        {ResourcePoolClassifiersName, ESysViewType::EResourcePoolClassifiers},
        {ResourcePoolsName, ESysViewType::EResourcePools},

        {NAuth::UsersName, ESysViewType::EAuthUsers},
        {NAuth::GroupsName, ESysViewType::EAuthGroups},
        {NAuth::GroupMembersName, ESysViewType::EAuthGroupMembers},
        {NAuth::OwnersName, ESysViewType::EAuthOwners},
        {NAuth::PermissionsName, ESysViewType::EAuthPermissions},
        {NAuth::EffectivePermissionsName, ESysViewType::EAuthEffectivePermissions},

        {ShowCreateName, ESysViewType::EShowCreate}
    };
}

class TSysViewRangesReader : public TActor<TSysViewRangesReader> {
public:
    using TBase = TActor<TSysViewRangesReader>;

    TSysViewRangesReader(
        const NActors::TActorId& ownerId,
        ui32 scanId,
        const TTableId& tableId,
        const TString& tablePath,
        const TMaybe<ESysViewType>& sysViewType,
        TVector<TSerializedTableRange> ranges,
        const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        const TString& database,
        bool reverse)
        : TBase(&TSysViewRangesReader::ScanState)
        , OwnerId(ownerId)
        , ScanId(scanId)
        , TableId(tableId)
        , TablePath(tablePath)
        , SysViewType(sysViewType)
        , Ranges(std::move(ranges))
        , Columns(columns.begin(), columns.end())
        , UserToken(std::move(userToken))
        , Database(database)
        , Reverse(reverse)
    {
    }

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    STFUNC(ScanState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(NKikimr::NKqp::TEvKqpCompute::TEvScanData, Handle);
            hFunc(NKikimr::NKqp::TEvKqpCompute::TEvScanError, ResendToOwnerAndDie);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, ResendToOwnerAndDie);
            hFunc(TEvents::TEvUndelivered, ResendToOwnerAndDie);
            hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, Handle);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& msg) {
        ActorIdToProto(SelfId(), msg->Get()->Record.MutableScanActorId());
        Send(OwnerId, msg->Release().Release());
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr& ack) {
        Y_DEBUG_ABORT_UNLESS(ack->Sender == OwnerId);
        if (!ScanActorId) {
            if (CurrentRange < Ranges.size()) {
                auto actor = CreateSystemViewScan(
                    SelfId(), ScanId, TableId, TablePath, SysViewType, Ranges[CurrentRange].ToTableRange(),
                    Columns, UserToken, Database, Reverse);
                ScanActorId = Register(actor.Release());
                CurrentRange += 1;
            } else {
                PassAway();
                return;
            }
        }

        Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(ack->Get()->FreeSpace, ack->Get()->Generation));
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanData::TPtr& data) {
        bool& finished = (*data->Get()).Finished;
        if (finished) {
            if (CurrentRange != Ranges.size()) {
                finished = false;
                ScanActorId.Clear();
            } else {
                TBase::Send(OwnerId, THolder(data->Release().Release()));
                PassAway();
                return;
            }
        }

        TBase::Send(OwnerId, THolder(data->Release().Release()));
    }

    void HandleAbortExecution(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::SYSTEM_VIEWS,
            "Got abort execution event, actor: " << TBase::SelfId()
                << ", owner: " << OwnerId
                << ", scan id: " << ScanId
                << ", table id: " << TableId
                << ", code: " << NYql::NDqProto::StatusIds::StatusCode_Name(ev->Get()->Record.GetStatusCode())
                << ", error: " << ev->Get()->GetIssues().ToOneLineString());

        if (ScanActorId) {
            Send(*ScanActorId, THolder(ev->Release().Release()));
        }

        PassAway();
    }

    void ResendToOwnerAndDie(auto& err) {
        TBase::Send(OwnerId, err->Release().Release());
        PassAway();
    }

    void PassAway() {
        if (ScanActorId) {
            Send(*ScanActorId, new TEvents::TEvPoison());
        }
        TBase::PassAway();
    }

private:
    TActorId OwnerId;
    ui32 ScanId;
    TTableId TableId;
    TString TablePath;
    const TMaybe<ESysViewType> SysViewType;
    TVector<TSerializedTableRange> Ranges;
    TVector<NMiniKQL::TKqpComputeContextBase::TColumn> Columns;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
    const bool Reverse;

    ui64 CurrentRange = 0;
    TMaybe<TActorId> ScanActorId;
};

THolder<NActors::IActor> CreateSystemViewScan(
    const NActors::TActorId& ownerId,
    ui32 scanId,
    const TTableId& tableId,
    const TString& tablePath,
    const TMaybe<ESysViewType>& sysViewType,
    TVector<TSerializedTableRange> ranges,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    bool reverse
) {
    if (ranges.size() == 1) {
        return CreateSystemViewScan(ownerId, scanId, tableId, tablePath, sysViewType, ranges[0].ToTableRange(),
                                    columns, std::move(userToken), database, reverse);
    } else {
        return MakeHolder<TSysViewRangesReader>(ownerId, scanId, tableId, tablePath, sysViewType, ranges,
                                                columns, std::move(userToken), database, reverse);
    }
}

THolder<NActors::IActor> CreateSystemViewScan(
    const NActors::TActorId& ownerId,
    ui32 scanId,
    const TTableId& tableId,
    const TString& tablePath,
    const TMaybe<ESysViewType>& sysViewType,
    const TTableRange& tableRange,
    const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    bool reverse
) {
    ESysViewType systemViewType;
    if (sysViewType) {
        systemViewType = *sysViewType;
    } else {
        auto typesIt = SYS_VIEW_TYPES_MAP.find(tableId.SysViewInfo);
        Y_ABORT_UNLESS(typesIt != SYS_VIEW_TYPES_MAP.end());
        systemViewType = typesIt->second;
    }

    switch (systemViewType) {
    case ESysViewType::EPartitionStats:
        return CreatePartitionStatsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::ENodes:
        return CreateNodesScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EQuerySessions:
        return CreateSessionsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::ETopQueriesByDurationOneMinute:
    case ESysViewType::ETopQueriesByDurationOneHour:
    case ESysViewType::ETopQueriesByReadBytesOneMinute:
    case ESysViewType::ETopQueriesByReadBytesOneHour:
    case ESysViewType::ETopQueriesByCpuTimeOneMinute:
    case ESysViewType::ETopQueriesByCpuTimeOneHour:
    case ESysViewType::ETopQueriesByRequestUnitsOneMinute:
    case ESysViewType::ETopQueriesByRequestUnitsOneHour:
        return CreateQueryStatsScan(ownerId, scanId, tableId, systemViewType, tableRange, columns);
    case ESysViewType::EPDisks:
        return CreatePDisksScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EVSlots:
        return CreateVSlotsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EGroups:
        return CreateGroupsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EStoragePools:
        return CreateStoragePoolsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EStorageStats:
        return CreateStorageStatsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::ETablets:
         return CreateTabletsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::EQueryMetricsOneMinute:
        return CreateQueryMetricsScan(ownerId, scanId, tableId, tableRange, columns);
    case ESysViewType::ETopPartitionsByCpuOneMinute:
    case ESysViewType::ETopPartitionsByCpuOneHour:
        return CreateTopPartitionsByCpuScan(ownerId, scanId, tableId, systemViewType, tableRange, columns);
    case ESysViewType::ETopPartitionsByTliOneMinute:
    case ESysViewType::ETopPartitionsByTliOneHour:
        return CreateTopPartitionsByTliScan(ownerId, scanId, tableId, systemViewType, tableRange, columns);
    case ESysViewType::EPgTables:
        return CreatePgTablesScan(ownerId, scanId, tableId, tablePath, tableRange, columns);
    case ESysViewType::EInformationSchemaTables:
        return CreateInformationSchemaTablesScan(ownerId, scanId, tableId, tablePath, tableRange, columns);
    case ESysViewType::EPgClass:
        return CreatePgClassScan(ownerId, scanId, tableId, tablePath, tableRange, columns);
    case ESysViewType::EResourcePoolClassifiers:
        return CreateResourcePoolClassifiersScan(ownerId, scanId, tableId, tableRange, columns,
                                                 std::move(userToken), database, reverse);
    case ESysViewType::EResourcePools:
        return CreateResourcePoolsScan(ownerId, scanId, tableId, tableRange, columns, std::move(userToken), database, reverse);
    case ESysViewType::EAuthUsers:
        return NAuth::CreateUsersScan(ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
    case ESysViewType::EAuthGroups:
        return NAuth::CreateGroupsScan(ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
    case ESysViewType::EAuthGroupMembers:
        return NAuth::CreateGroupMembersScan(ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
    case ESysViewType::EAuthOwners:
        return NAuth::CreateOwnersScan(ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
    case ESysViewType::EAuthPermissions:
    case ESysViewType::EAuthEffectivePermissions:
        return NAuth::CreatePermissionsScan(systemViewType == ESysViewType::EAuthEffectivePermissions,
                                            ownerId, scanId, tableId, tableRange, columns, std::move(userToken));
    case ESysViewType::EShowCreate:
        return CreateShowCreate(ownerId, scanId, tableId, tableRange, columns, database, std::move(userToken));
    default:
        return {};
    }
}

} // NSysView
} // NKikimr
