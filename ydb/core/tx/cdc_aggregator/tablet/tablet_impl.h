#pragma once

#include <ydb/core/protos/counters_cdc_aggregator.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NCdcAggregator {

class TCdcAggregator
    : public TActor<TCdcAggregator>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    class TTxInitSchema;
    class TTxInit;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info);

private:
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);

    STFUNC(StateInit);
    STFUNC(StateWork);
};

} // namespace NKikimr::NCdcAggregator
