#pragma once

#include "cdc_aggregator_stream_repository.h"
#include "schema.h"

#include <ydb/core/protos/counters_cdc_aggregator.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <optional>

namespace NKikimr::NCdcAggregator {

class TCdcAggregator
    : public NTabletFlatExecutor::TTabletActor<TActor<TCdcAggregator>>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    using Schema = TCdcAggregatorSchema;

    class TTxBase : public NTabletFlatExecutor::TTransactionBase<TCdcAggregator> {
    public:
        TTxBase(const TString& name, TCdcAggregator* self)
            : TTransactionBase(self)
            , TxLogPrefix(YDB_LOG_CREATE_MESSAGE({"tabletId", self->TabletID()}, {"txName", name}))
        {
        }

    protected:
        const NActors::NStructuredLog::TStructuredMessage TxLogPrefix;
    };

    class TTxInitSchema;
    class TTxInit;
    class TTxConfigureTracking;
    class TTxPartitioningChanged;
    class TTxWriteHeartbeats;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CDC_AGGREGATOR_TABLET;
    }

    TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info);

private:
    void Die(const TActorContext& ctx) override;
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;

    void RunTxInitSchema(const TActorContext& ctx);
    void RunTxInit(const TActorContext& ctx);
    void SwitchToWork(const TActorContext& ctx);
    void BroadcastHeartbeat(const NPQ::THeartbeat& heartbeat, const TActorContext& ctx);

    void Handle(TEvCdcAggregator::TEvConfigureTracking::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvPartitioningChanged::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx);

    STFUNC(StateInit);
    STFUNC(StateWork);

private:
    std::optional<TCdcAggregatorStream> CdcAggregatorStream;
    TCdcAggregatorStreamProtoConverter CdcAggregatorStreamProtoConverter;
    TCdcAggregatorStreamRepository CdcAggregatorStreamRepository;
    TActorId CdcAggregatorHeartbeatBroadcaster;
};

} // namespace NKikimr::NCdcAggregator
