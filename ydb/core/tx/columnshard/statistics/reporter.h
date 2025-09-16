#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/counters/aggregation/table_stats.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/ctor_logger.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/datashard/datashard.h>

using namespace NActors;
using namespace NKikimr;

namespace NKikimr::NOlap {

class TColumnShardStatisticsReporter : public NActors::TActorBootstrapped<TColumnShardStatisticsReporter> {
private:
    TActorId StatsReportPipe;
    ui64 SSId = 0;
    ui64 SSLocalId = 0;
    ui64 TabletId = 0;
    ui32 ReportStatisticsPeriodMs;
    NKikimr::NColumnShard::TCountersManager& CountersManager;
    ui64 StatsReportRound = 0;
    std::unique_ptr<TEvDataShard::TEvPeriodicTableStats> latestCSExecutorStats;

    void BuildSSPipe();
    void ReportBaseStatistics();
    void ReportExecutorStatistics();
    void SendPeriodicStats();

    void FillWhateverCan(std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev);

    // class TEvCSExecutorStatistics: public NActors::TEventLocal<TEvCSExecutorStatistics, NColumnShard::TEvPrivate::EEv::EvCSExecutorStatistics> {};

    STFUNC(StateFunc) {
        // TLogContextGuard gLogging(
        //     NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", ParentActorId));
        switch (ev->GetTypeRewrite()) {
            // cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
            sFunc(TEvTabletPipe::TEvClientConnected, SendPeriodicStats)
            sFunc(NColumnShard::TEvPrivate::TEvReportStatistics, SendPeriodicStats);
            hFunc(TEvDataShard::TEvPeriodicTableStats, Handle);
            hFunc(TEvSetSSId, Handle);
            default:
                AFL_VERIFY(false)("ev", (ev->ToString()));
        }
    }

public:

    class TEvSetSSId: public NActors::TEventLocal<TEvSetSSId, NColumnShard::TEvPrivate::EEv::EvSetSSId> {
    public:
        ui64 SSId;
        ui64 SSLocalId;
        explicit TEvSetSSId(ui64 sSId, ui64 sSLocalId) : SSId(sSId), SSLocalId(sSLocalId) {}
    };

    TColumnShardStatisticsReporter (
        ui64 tabletId,
        ui32 reportStatisticsPeriodMs,
        NColumnShard::TCountersManager& countersManager)
        :
        TabletId(tabletId),
        ReportStatisticsPeriodMs(reportStatisticsPeriodMs),
        CountersManager(countersManager) {}
    void Bootstrap(const NActors::TActorContext&);
    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev);
    // void Handle(TEvTabletPipe::TEvClientConnected::TPtr&, const TActorContext&);
    // void Handle(TEvReportStatistics::TPtr&, const NActors::TActorContext&);
    void Handle(TEvDataShard::TEvPeriodicTableStats::TPtr&);
    void Handle(TEvSetSSId::TPtr&);

};

}