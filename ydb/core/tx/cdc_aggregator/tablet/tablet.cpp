#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

TCdcAggregator::TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , TabletCountersPtr(new TProtobufTabletCounters<
            ESimpleCounters_descriptor,
            ECumulativeCounters_descriptor,
            EPercentileCounters_descriptor,
            ETxTypes_descriptor
        >())
    , TabletCounters(TabletCountersPtr.Get())
{
}

void TCdcAggregator::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnActivateExecutor(const TActorContext& ctx) {
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr.Release());
    RunTxInitSchema(ctx);
}

void TCdcAggregator::DefaultSignalTabletActive(const TActorContext&) {
    // The tablet becomes active after TTxInit has restored the stream.
}

STFUNC(TCdcAggregator::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TCdcAggregator::StateWork) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvCdcAggregator::TEvConfigure, Handle);
        HFunc(TEvCdcAggregator::TEvActivate, Handle);
        HFunc(TEvCdcAggregator::TEvReportDeliveredBatch, Handle);
        HFunc(TEvCdcAggregator::TEvReplaceSource, Handle);
        HFunc(TEvCdcAggregator::TEvDescribeStream, Handle);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("Unexpected event",
                {"tabletId", TabletID()},
                {"eventType", ev->GetTypeRewrite()});
        }
    }
}

void TCdcAggregator::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TCdcAggregator::OnReadyBarrierCommitted(const TBarrier& barrier) {
    Y_ENSURE(!CommittedReadyBarrier || CommittedReadyBarrier->Version < barrier.Version,
        "Committed ready barrier goes back from " << CommittedReadyBarrier->Version << " to " << barrier.Version);
    CommittedReadyBarrier = barrier;
    TabletCounters->Cumulative()[COUNTER_READY_BARRIER_ADVANCES].Increment(1);

    YDB_LOG_DEBUG("Ready barrier committed",
        {"tabletId", TabletID()},
        {"version", barrier.Version});
    PublishCommittedBarrier();
}

void TCdcAggregator::OnActivationCommitted() {
    CommittedActive = true;
    YDB_LOG_NOTICE("Stream activated",
        {"tabletId", TabletID()});
    PublishCommittedBarrier();
}

void TCdcAggregator::UpdateStreamCounters() {
    auto& simple = TabletCounters->Simple();
    simple[COUNTER_SOURCES].Set(Stream.GetSources().size());
    simple[COUNTER_SOURCES_WITHOUT_PROGRESS].Set(Stream.CountSourcesWithoutProgress());
    simple[COUNTER_RETIRED_SOURCES].Set(Stream.GetReplacements().size());
    simple[COUNTER_ACTIVE].Set(CommittedActive ? 1 : 0);
}

void TCdcAggregator::PublishCommittedBarrier() {
    if (!CommittedActive || !CommittedReadyBarrier) {
        return;
    }
    YDB_LOG_DEBUG("Ready barrier may be published",
        {"tabletId", TabletID()},
        {"version", CommittedReadyBarrier->Version});
}

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
