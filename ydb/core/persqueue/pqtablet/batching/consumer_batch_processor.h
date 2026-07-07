#pragma once

#include "batch_cutter.h"
#include "batch_processor.h"

namespace NKikimr::NPQ::NBatching {

class TConsumerBatchProcessor : public TBaseTabletActor<TConsumerBatchProcessor> {
public:
    TConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user);

    void Bootstrap(const NActors::TActorContext& ctx);

    void Handle(TEvProcessBatch::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvProcessBatchKeys::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NActors::TEvents::TEvPoisonPill::TPtr& ev, const NActors::TActorContext& ctx);

    const TString& GetLogPrefix() const override;

private:
    STFUNC(StateWork);
    void FlushCPUUsageMetrics(const NActors::TActorContext& ctx, bool scheduleNext);

private:
    const TString User;
    TString LogPrefix;

    // codec -> batch cutter
    THashMap<int, THolder<IBatchCutter>> BatchCutters;
    THashMap<ui32, ui64> CPUUsageMetricByPartition;
    ui64 CurrentCPUUsageMetric = 0;
    ui32 CurrentCPUUsagePartitionId = 0;
    bool HasCurrentCPUUsagePartitionId = false;
};

NActors::IActor* CreateConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user);

} // namespace NKikimr::NPQ::NBatching
