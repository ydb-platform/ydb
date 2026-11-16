#pragma once

#include "schema.h"

#include <ydb/core/protos/counters_cdc_aggregator.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NCdcAggregator {

class TCdcAggregator
    : public TActor<TCdcAggregator>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    using Schema = TCdcAggregatorSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TCdcAggregator>;

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

    void Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx);

    STFUNC(StateInit);
    STFUNC(StateWork);
};

} // namespace NKikimr::NCdcAggregator
