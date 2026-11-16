#include "tablet.h"
#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>

#include <ydb/library/actors/core/log.h>

#include <algorithm>

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
    YDB_LOG_CREATE_CONTEXT({"tabletId", TabletID()});
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvCdcAggregator::TEvConfigureTracking, Handle);
        HFunc(TEvCdcAggregator::TEvWriteHeartbeats, Handle);
        HFunc(TEvCdcAggregator::TEvPartitioningChanged, Handle);
        HFunc(TEvCdcAggregator::TEvDescribeTracking, Handle);
        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("Unexpected event",
                {"eventType", ev->GetTypeRewrite()});
        }
    }
}

void TCdcAggregator::SwitchToWork(const TActorContext& ctx) {
    SignalTabletActive(ctx);
    Become(&TThis::StateWork);
}

void TCdcAggregator::PersistCdcAggregatorStream(NIceDb::TNiceDb& db) {
    db.Table<Schema::State>().Key(Schema::State::Id::Value).Update(
        NIceDb::TUpdate<Schema::State::TrackingConfig>(SerializeCdcAggregatorStreamTopic(CdcAggregatorStream->GetTopic())));

    for (const auto dataShardId : CdcAggregatorStream->GetSourceDataShardIds()) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }
}

void TCdcAggregator::PersistCdcAggregatorStreamSourcesDiff(NIceDb::TNiceDb& db, const TCdcAggregatorStreamSourcesDiff& diff) {
    for (const auto dataShardId : diff.Deleted) {
        db.Table<Schema::Sources>().Key(dataShardId).Delete();
    }

    for (const auto dataShardId : diff.Inserted) {
        db.Table<Schema::Sources>().Key(dataShardId).Update();
    }
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvWriteHeartbeats::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    auto result = MakeHolder<TEvCdcAggregator::TEvWriteHeartbeatsResult>();
    result->Record.SetStatus(NKikimrCdcAggregator::TEvWriteHeartbeatsResult::NOT_READY);
    result->Record.SetOrigin(TabletID());
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

void TCdcAggregator::Handle(TEvCdcAggregator::TEvDescribeTracking::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_TRACE("Handle",
        {"ev", ev->Get()->ToString()});

    auto result = MakeHolder<TEvCdcAggregator::TEvDescribeTrackingResult>();
    if (CdcAggregatorStream) {
        TVector<ui64> sources(CdcAggregatorStream->GetSourceDataShardIds().begin(), CdcAggregatorStream->GetSourceDataShardIds().end());
        std::sort(sources.begin(), sources.end());
        for (const auto dataShardId : sources) {
            result->Record.AddSources()->SetDataShardId(dataShardId);
        }
    }
    ctx.Send(ev->Sender, result.Release(), 0, ev->Cookie);
}

IActor* CreateCdcAggregator(const TActorId& tablet, TTabletStorageInfo* info) {
    return new TCdcAggregator(tablet, info);
}

} // namespace NKikimr::NCdcAggregator
