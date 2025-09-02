#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
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

    void BuildSSPipe(const TActorContext& ctx);
    void UpdateSSId();
    // void FillOlapStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev);
    // void FillColumnTableStats(const TActorContext& ctx, std::unique_ptr<TEvDataShard::TEvPeriodicTableStats>& ev);

class TEvReportBaseStatistics: public NActors::TEventLocal<TEvReportBaseStatistics, NColumnShard::TEvPrivate::EEv::EvReportBaseStatistics> {};
class TEvReportExecutorStatistics: public NActors::TEventLocal<TEvReportExecutorStatistics, NColumnShard::TEvPrivate::EEv::EvReportExecutorStatistics> {};


    STFUNC(StateFunc) {
        // TLogContextGuard gLogging(
        //     NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId)("parent", ParentActorId));
        switch (ev->GetTypeRewrite()) {
            // cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
            HFunc(TEvReportBaseStatistics, Handle);
            HFunc(TEvReportExecutorStatistics, Handle);
            default:
                AFL_VERIFY(false);
        }
    }

public:
    TColumnShardStatisticsReporter (NColumnShard::TColumnShard& owner, ui32 reportBaseStatisticsPeriodMs, ui32 reportExecutorStatisticsPeriodMs):
        Owner(owner), ReportBaseStatisticsPeriodMs(reportBaseStatisticsPeriodMs), ReportExecutorStatisticsPeriodMs(reportExecutorStatisticsPeriodMs) {}
    void Bootstrap(const NActors::TActorContext& /*ctx*/);
    void SendPeriodicStats();
    void SetSSId(ui64 sSId, const TActorContext& ctx);
    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext&);
    void Handle(TEvReportBaseStatistics::TPtr& ev, const NActors::TActorContext&);
    void Handle(TEvReportExecutorStatistics::TPtr& ev, const NActors::TActorContext&);

};

}