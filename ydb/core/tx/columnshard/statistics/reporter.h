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
    NColumnShard::TColumnShard& Owner;
    ui32 ReportBaseStatisticsPeriodMs;
    ui32 ReportExecutorStatisticsPeriodMs;
    NKikimr::NColumnShard::TCountersManager& CountersManager;
    ui64 StatsReportRound = 0;

    void BuildSSPipe(const TActorContext& ctx);
    void ReportBaseStatistics();
    void ReportExecutorStatistics();
    void SendPeriodicStats();

    class TEvReportBaseStatistics: public NActors::TEventLocal<TEvReportBaseStatistics, NColumnShard::TEvPrivate::EEv::EvReportBaseStatistics> {};
    class TEvReportExecutorStatistics: public NActors::TEventLocal<TEvReportExecutorStatistics, NColumnShard::TEvPrivate::EEv::EvReportExecutorStatistics> {};

    STFUNC(StateFunc) {
        // TLogContextGuard gLogging(
        //     NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", ParentActorId));
        switch (ev->GetTypeRewrite()) {
            // cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
            HFunc(TEvTabletPipe::TEvClientConnected, Handle)
            HFunc(TEvReportBaseStatistics, Handle);
            HFunc(TEvReportExecutorStatistics, Handle);
            HFunc(TEvSetSSId, Handle);
            default:
                AFL_VERIFY(false)("ev", (ev->ToString()));
        }
    }

public:

    class TEvSetSSId: public NActors::TEventLocal<TEvSetSSId, NColumnShard::TEvPrivate::EEv::EvSetSSId> {
    public:
        ui64 SSId;
        explicit TEvSetSSId(ui64 sSId) : SSId(sSId){}
    };

    TColumnShardStatisticsReporter (
        NColumnShard::TColumnShard& owner,
        ui32 reportBaseStatisticsPeriodMs,
        ui32 reportExecutorStatisticsPeriodMs,
        NColumnShard::TCountersManager& countersManager)
        :
        Owner(owner),
        ReportBaseStatisticsPeriodMs(reportBaseStatisticsPeriodMs),
        ReportExecutorStatisticsPeriodMs(reportExecutorStatisticsPeriodMs),
        CountersManager(countersManager) {}
    void Bootstrap(const NActors::TActorContext&);
    void SetSSId(ui64 sSId, const TActorContext&);
    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext&);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr&, const TActorContext&);
    void Handle(TEvReportBaseStatistics::TPtr&, const NActors::TActorContext&);
    void Handle(TEvReportExecutorStatistics::TPtr&, const NActors::TActorContext&);
    void Handle(TEvSetSSId::TPtr&, const NActors::TActorContext&);

};

}