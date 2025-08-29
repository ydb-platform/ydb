#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/core/tx/ctor_logger.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/accessor/accessor.h>

using namespace NActors;
using namespace NKikimr;

class TColumnShardStatisticsReporter : public NActors::TActorBootstrapped<TColumnShardStatisticsReporter> {
private:
    TActorId StatsReportPipe;
    ui64 SSId;

    void BuildSSPipe(const TActorContext& ctx);
    void UpdateSSId();

public:

    TColumnShardStatisticsReporter ();
    void Bootstrap(const NActors::TActorContext& /*ctx*/);
    void SendPeriodicStats();
    void SetSSId(ui64 sSId, const TActorContext& ctx);
    void Handle(NKikimr::TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext&);

};