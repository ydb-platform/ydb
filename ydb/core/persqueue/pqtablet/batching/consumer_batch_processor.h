#pragma once

#include "batch_cutter.h"
#include "batch_processor.h"

namespace NKikimr::NPQ::NBatching {

class TConsumerBatchProcessor : public TBaseTabletActor<TConsumerBatchProcessor> {
public:
    TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user);

    void Bootstrap(const NActors::TActorContext& ctx);

    void Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);

    const TString& GetLogPrefix() const override;

private:
    STFUNC(StateWork);

private:
    const TString User;
    TString LogPrefix;

    // codec -> batch cutter
    THashMap<int, THolder<IBatchCutter>> BatchCutters;
    ui64 CPUUsageMetric = 0;
};

NActors::IActor* CreateConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user);

} // namespace NKikimr::NPQ::NBatching
