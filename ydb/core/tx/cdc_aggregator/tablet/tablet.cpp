#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <ydb/library/actors/core/log.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CDC_AGGREGATOR

namespace NKikimr::NCdcAggregator {

TCdcAggregator::TCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
}

void TCdcAggregator::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void TCdcAggregator::OnActivateExecutor(const TActorContext& ctx) {
    RunTxInitSchema(ctx);
}

void TCdcAggregator::DefaultSignalTabletActive(const TActorContext&) {
}

STFUNC(TCdcAggregator::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TCdcAggregator::StateWork) {
    switch (ev->GetTypeRewrite()) {
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

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
