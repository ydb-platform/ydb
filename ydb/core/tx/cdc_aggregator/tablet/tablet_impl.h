#pragma once

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/counters_cdc_aggregator.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/cdc_aggregator/public/events.h>
#include <ydb/core/tx/cdc_aggregator/stream/stream.h>

namespace NKikimr::NCdcAggregator {

// CdcAggregator tablet: transport of the stream use cases.
//
// Every request runs as a transaction: Execute applies a use case to Stream and
// writes the changes, Complete replies. Even a request that changes nothing
// goes through a transaction, because the executor completes a read-only
// transaction only after the preceding commits (TLogicRedo::CommitROTransaction).
//
// Stream already contains changes of transactions that are not committed yet.
// So replies and the publishable barrier are captured in Execute of their own
// transaction and leave the tablet only from its Complete.
class TCdcAggregator
    : public TActor<TCdcAggregator>
    , public NTabletFlatExecutor::TTabletExecutedFlat
{
    class TTxInitSchema;
    class TTxInit;
    class TTxConfigure;
    class TTxActivate;
    class TTxReportDeliveredBatch;
    class TTxReplaceSource;
    class TTxDescribeStream;

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

    STFUNC(StateInit);
    STFUNC(StateWork);

    void SwitchToWork(const TActorContext& ctx);

    void Handle(TEvCdcAggregator::TEvConfigure::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvActivate::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvReportDeliveredBatch::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvReplaceSource::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvCdcAggregator::TEvDescribeStream::TPtr& ev, const TActorContext& ctx);

    // Called from Complete in commit order.
    void OnReadyBarrierCommitted(const TBarrier& barrier);
    void OnActivationCommitted();
    void UpdateStreamCounters();

    // The only place where the barrier leaves the tablet: after it is
    // committed and the stream is active. Publication to the partitions
    // (CmdPublishCdcBarrier, retries per partition) is added here by stage 4.
    void PublishCommittedBarrier();

private:
    THolder<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;

    // Includes changes of transactions that are not committed yet; read and
    // changed only from Execute.
    TStream Stream;

    // Committed state that may leave the tablet.
    std::optional<TBarrier> CommittedReadyBarrier;
    bool CommittedActive = false;
};

} // namespace NKikimr::NCdcAggregator
