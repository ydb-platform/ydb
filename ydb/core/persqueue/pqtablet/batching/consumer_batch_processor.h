#pragma once

#include "batch_processor.h"
#include "batch_cutter.h"

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

    const IBatchCutter& GetBatchCutter(NKikimrClient::EMessageFormat messageFormat) const;

private:
    const TString User;
    TString LogPrefix;
    THashMap<NKikimrClient::EMessageFormat, THolder<IBatchCutter>> BatchCutters;
};

NActors::IActor* CreateConsumerBatchProcessor(ui64 tabletId, const NActors::TActorId& tabletActorId, TString user);

} // namespace NKikimr::NPQ::NBatching
