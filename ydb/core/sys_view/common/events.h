#pragma once

#include "utils.h"
#include "db_counters.h"

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/protos/tablet.pb.h>

namespace NKikimr {
namespace NSysView {

using TShardIdx = std::pair<ui64, ui64>;

class IDbCounters : public virtual TThrRefBase {
public:
    virtual void ToProto(NKikimr::NSysView::TDbServiceCounters& counters) = 0;
    virtual void FromProto(NKikimr::NSysView::TDbServiceCounters& counters) = 0;
};

struct TEvSysView {
    enum EEv {
        EvSendPartitionStats = EventSpaceBegin(TKikimrEvents::ES_SYSTEM_VIEW),
        EvSetPartitioning,
        EvRemoveTable,
        EvGetPartitionStats,
        EvGetPartitionStatsResult,

        EvCollectQueryStats,
        EvGetQueryStats,
        EvGetQueryStatsResult,

        EvGetScanLimiter,
        EvGetScanLimiterResult,

        EvGetPDisksRequest,
        EvGetPDisksResponse,
        EvGetVSlotsRequest,
        EvGetVSlotsResponse,
        EvGetGroupsRequest,
        EvGetGroupsResponse,
        EvGetStoragePoolsRequest,
        EvGetStoragePoolsResponse,

        EvGetTabletIdsRequest,
        EvGetTabletIdsResponse,
        EvGetTabletsRequest,
        EvGetTabletsResponse,

        EvConfigureProcessor,
        EvIntervalQuerySummary,
        EvGetIntervalMetricsRequest,
        EvGetIntervalMetricsResponse,
        EvGetQueryMetricsRequest,
        EvGetQueryMetricsResponse,
        EvGetQueryStatsResponse,

        EvRegisterDbCounters,
        EvSendDbCountersRequest,
        EvSendDbCountersResponse,
        EvSendDbLabeledCountersRequest,
        EvSendDbLabeledCountersResponse,
        EvWatchDatabase,

        EvUpdateTtlStats,

        EvGetStorageStatsRequest,
        EvGetStorageStatsResponse,

        EvSendTopPartitions,
        EvGetTopPartitionsRequest,
        EvGetTopPartitionsResponse,

        EvInitPartitionStatsCollector,

        EvCalculateStorageStatsRequest,
        EvCalculateStorageStatsResponse,

        EvEnd,
    };

    struct TEvSendPartitionStats : public TEventLocal<
        TEvSendPartitionStats,
        EvSendPartitionStats>
    {
        TPathId DomainKey;
        TPathId PathId;
        TShardIdx ShardIdx;

        NKikimrSysView::TPartitionStats Stats;

        TEvSendPartitionStats(TPathId domainKey, TPathId pathId, TShardIdx shardIdx)
            : DomainKey(domainKey)
            , PathId(pathId)
            , ShardIdx(shardIdx)
        {}
    };

    struct TEvSetPartitioning : public TEventLocal<
        TEvSetPartitioning,
        EvSetPartitioning>
    {
        TPathId DomainKey;
        TPathId PathId;
        TString Path;
        TVector<TShardIdx> ShardIndices;

        TEvSetPartitioning(TPathId domainKey, TPathId pathId, const TStringBuf path)
            : DomainKey(domainKey)
            , PathId(pathId)
            , Path(path)
        {}
    };

    struct TEvRemoveTable : public TEventLocal<
        TEvRemoveTable,
        EvRemoveTable>
    {
        TPathId DomainKey;
        TPathId PathId;

        TEvRemoveTable(TPathId domainKey, TPathId pathId)
            : DomainKey(domainKey)
            , PathId(pathId)
        {}
    };

    struct TEvGetPartitionStats : public TEventPB<
        TEvGetPartitionStats,
        NKikimrSysView::TEvGetPartitionStats,
        EvGetPartitionStats>
    {};

    struct TEvGetPartitionStatsResult : public TEventPB<
        TEvGetPartitionStatsResult,
        NKikimrSysView::TEvGetPartitionStatsResult,
        EvGetPartitionStatsResult>
    {};

    // KQP -> SysViewService
    struct TEvCollectQueryStats : public TEventLocal<
        TEvCollectQueryStats,
        EvCollectQueryStats>
    {
        NKikimrSysView::TQueryStats QueryStats;
        TString Database;
    };

    // top queries scan -> SysViewService
    struct TEvGetQueryStats : public TEventPB<
        TEvGetQueryStats,
        NKikimrSysView::TEvGetQueryStats,
        EvGetQueryStats>
    {};

    // SysViewService -> top queries scan
    struct TEvGetQueryStatsResult : public TEventPB<
        TEvGetQueryStatsResult,
        NKikimrSysView::TEvGetQueryStatsResult,
        EvGetQueryStatsResult>
    {};

    struct TEvGetScanLimiter : public TEventLocal<
        TEvGetScanLimiter,
        EvGetScanLimiter>
    {};

    struct TEvGetScanLimiterResult : public TEventLocal<
        TEvGetScanLimiterResult,
        EvGetScanLimiterResult>
    {
        TIntrusivePtr<TScanLimiter> ScanLimiter;
    };

    struct TEvGetPDisksRequest : public TEventPB<
        TEvGetPDisksRequest,
        NKikimrSysView::TEvGetPDisksRequest,
        EvGetPDisksRequest>
    {};

    struct TEvGetPDisksResponse : public TEventPB<
        TEvGetPDisksResponse,
        NKikimrSysView::TEvGetPDisksResponse,
        EvGetPDisksResponse>
    {};

    struct TEvGetVSlotsRequest : public TEventPB<
        TEvGetVSlotsRequest,
        NKikimrSysView::TEvGetVSlotsRequest,
        EvGetVSlotsRequest>
    {};

    struct TEvGetVSlotsResponse : public TEventPB<
        TEvGetVSlotsResponse,
        NKikimrSysView::TEvGetVSlotsResponse,
        EvGetVSlotsResponse>
    {};

    struct TEvGetGroupsRequest : public TEventPB<
        TEvGetGroupsRequest,
        NKikimrSysView::TEvGetGroupsRequest,
        EvGetGroupsRequest>
    {};

    struct TEvGetGroupsResponse : public TEventPB<
        TEvGetGroupsResponse,
        NKikimrSysView::TEvGetGroupsResponse,
        EvGetGroupsResponse>
    {};

    struct TEvGetStoragePoolsRequest : public TEventPB<
        TEvGetStoragePoolsRequest,
        NKikimrSysView::TEvGetStoragePoolsRequest,
        EvGetStoragePoolsRequest>
    {};

    struct TEvGetStoragePoolsResponse : public TEventPB<
        TEvGetStoragePoolsResponse,
        NKikimrSysView::TEvGetStoragePoolsResponse,
        EvGetStoragePoolsResponse>
    {};

    struct TEvGetTabletIdsRequest : public TEventPB<
        TEvGetTabletIdsRequest,
        NKikimrSysView::TEvGetTabletIdsRequest,
        EvGetTabletIdsRequest>
    {};

    struct TEvGetTabletIdsResponse : public TEventPB<
        TEvGetTabletIdsResponse,
        NKikimrSysView::TEvGetTabletIdsResponse,
        EvGetTabletIdsResponse>
    {};

    struct TEvGetTabletsRequest : public TEventPB<
        TEvGetTabletsRequest,
        NKikimrSysView::TEvGetTabletsRequest,
        EvGetTabletsRequest>
    {};

    struct TEvGetTabletsResponse : public TEventPB<
        TEvGetTabletsResponse,
        NKikimrSysView::TEvGetTabletsResponse,
        EvGetTabletsResponse>
    {};

    struct TEvConfigureProcessor : public TEventPB<
        TEvConfigureProcessor,
        NKikimrSysView::TEvConfigureProcessor,
        EvConfigureProcessor>
    {
        TEvConfigureProcessor() = default;
        explicit TEvConfigureProcessor(const TString& database) {
            Record.SetDatabase(database);
        }
    };

    struct TEvIntervalQuerySummary : public TEventPB<
        TEvIntervalQuerySummary,
        NKikimrSysView::TEvIntervalQuerySummary,
        EvIntervalQuerySummary>
    {};

    struct TEvGetIntervalMetricsRequest : public TEventPB<
        TEvGetIntervalMetricsRequest,
        NKikimrSysView::TEvGetIntervalMetricsRequest,
        EvGetIntervalMetricsRequest>
    {};

    struct TEvGetIntervalMetricsResponse : public TEventPB<
        TEvGetIntervalMetricsResponse,
        NKikimrSysView::TEvGetIntervalMetricsResponse,
        EvGetIntervalMetricsResponse>
    {};

    struct TEvGetQueryMetricsRequest : public TEventPB<
        TEvGetQueryMetricsRequest,
        NKikimrSysView::TEvGetQueryMetricsRequest,
        EvGetQueryMetricsRequest>
    {};

    struct TEvGetQueryMetricsResponse : public TEventPB<
        TEvGetQueryMetricsResponse,
        NKikimrSysView::TEvGetQueryMetricsResponse,
        EvGetQueryMetricsResponse>
    {};

    struct TEvGetQueryStatsResponse : public TEventPB<
        TEvGetQueryStatsResponse,
        NKikimrSysView::TEvGetQueryStatsResponse,
        EvGetQueryStatsResponse>
    {};

    struct TEvRegisterDbCounters : public TEventLocal<
        TEvRegisterDbCounters,
        EvRegisterDbCounters>
    {
        NKikimrSysView::EDbCountersService Service;
        TString Database;
        TPathId PathId; // database path id, for tablet counters
        TIntrusivePtr<IDbCounters> Counters;

        TEvRegisterDbCounters(
            NKikimrSysView::EDbCountersService service,
            const TString& database,
            TIntrusivePtr<IDbCounters> counters)
            : Service(service)
            , Database(database)
            , Counters(counters)
        {}

        TEvRegisterDbCounters(
            NKikimrSysView::EDbCountersService service,
            TPathId pathId,
            TIntrusivePtr<IDbCounters> counters)
            : Service(service)
            , PathId(pathId)
            , Counters(counters)
        {}
    };

    struct TEvSendDbCountersRequest : public TEventPB<
        TEvSendDbCountersRequest,
        NKikimrSysView::TEvSendDbCountersRequest,
        EvSendDbCountersRequest>
    {};

    struct TEvSendDbCountersResponse : public TEventPB<
        TEvSendDbCountersResponse,
        NKikimrSysView::TEvSendDbCountersResponse,
        EvSendDbCountersResponse>
    {};

    struct TEvSendDbLabeledCountersRequest : public TEventPB<
        TEvSendDbLabeledCountersRequest,
        NKikimrSysView::TEvSendDbLabeledCountersRequest,
        EvSendDbLabeledCountersRequest>
    {};

    struct TEvSendDbLabeledCountersResponse : public TEventPB<
        TEvSendDbLabeledCountersResponse,
        NKikimrSysView::TEvSendDbLabeledCountersResponse,
        EvSendDbLabeledCountersResponse>
    {};

    struct TEvWatchDatabase : public TEventLocal<
        TEvWatchDatabase,
        EvWatchDatabase>
    {
        TString Database;
        TPathId PathId;

        explicit TEvWatchDatabase(const TString& database)
            : Database(database)
        {}

        explicit TEvWatchDatabase(TPathId pathId)
            : PathId(pathId)
        {}
    };

    struct TEvUpdateTtlStats : public TEventLocal<
        TEvUpdateTtlStats,
        EvUpdateTtlStats>
    {
        TPathId DomainKey;
        TPathId PathId;
        TShardIdx ShardIdx;

        NKikimrSysView::TTtlStats Stats;

        TEvUpdateTtlStats(TPathId domainKey, TPathId pathId, TShardIdx shardIdx)
            : DomainKey(domainKey)
            , PathId(pathId)
            , ShardIdx(shardIdx)
        {}
    };

    struct TEvGetStorageStatsRequest
        : TEventPB<TEvGetStorageStatsRequest, NKikimrSysView::TEvGetStorageStatsRequest, EvGetStorageStatsRequest>
    {};

    struct TEvGetStorageStatsResponse
        : TEventPB<TEvGetStorageStatsResponse, NKikimrSysView::TEvGetStorageStatsResponse, EvGetStorageStatsResponse>
    {};

    struct TEvSendTopPartitions : public TEventPB<
        TEvSendTopPartitions,
        NKikimrSysView::TEvSendTopPartitions,
        EvSendTopPartitions>
    {};

    struct TEvGetTopPartitionsRequest : public TEventPB<
        TEvGetTopPartitionsRequest,
        NKikimrSysView::TEvGetTopPartitionsRequest,
        EvGetTopPartitionsRequest>
    {};

    struct TEvGetTopPartitionsResponse : public TEventPB<
        TEvGetTopPartitionsResponse,
        NKikimrSysView::TEvGetTopPartitionsResponse,
        EvGetTopPartitionsResponse>
    {};


    struct TEvInitPartitionStatsCollector : public TEventLocal<
        TEvInitPartitionStatsCollector,
        EvInitPartitionStatsCollector>
    {
        TPathId DomainKey;
        ui64 SysViewProcessorId = 0;

        TEvInitPartitionStatsCollector(TPathId domainKey, ui64 sysViewProcessorId)
            : DomainKey(domainKey)
            , SysViewProcessorId(sysViewProcessorId)
        {}
    };
};

} // NSysView
} // NKikimr
