#include "pqtablet_mock.h"
#include <ydb/core/testlib/tablet_helpers.h>

namespace NKikimr::NPQ::NHelpers {

TPQTabletMock::TPQTabletMock(const TActorId& tablet, TTabletStorageInfo* info) :
    TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, nullptr)
    , PipeClientCacheConfig(new NTabletPipe::TBoundedClientCacheConfig())
    , PipeClientCache(NTabletPipe::CreateBoundedClientCache(PipeClientCacheConfig, GetPipeClientConfig()))
{
}

NTabletPipe::TClientConfig TPQTabletMock::GetPipeClientConfig()
{
    NTabletPipe::TClientConfig config;
    config.CheckAliveness = true;
    config.RetryPolicy = {
        .RetryLimitCount = 30,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(500),
        .BackoffMultiplier = 2,
    };
    return config;
}

void TPQTabletMock::SendReadSet(NActors::TTestActorRuntime& runtime, const TSendReadSetParams& params)
{
    auto event = std::make_unique<TEvPQTablet::TEvSendReadSet>();
    event->Params = params;

    ForwardToTablet(runtime, TabletID(), TActorId(), event.release());
}

void TPQTabletMock::SendReadSetAck(NActors::TTestActorRuntime& runtime, const TSendReadSetAckParams& params)
{
    auto event = std::make_unique<TEvPQTablet::TEvSendReadSetAck>();
    event->Params = params;

    ForwardToTablet(runtime, TabletID(), TActorId(), event.release());
}

void TPQTabletMock::OnDetach(const TActorContext &ctx)
{
    Die(ctx);
}

void TPQTabletMock::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ev);

    Die(ctx);
}

void TPQTabletMock::DefaultSignalTabletActive(const TActorContext &)
{
    // must be empty
}

void TPQTabletMock::OnActivateExecutor(const TActorContext &ctx)
{
    Become(&TThis::StateWork);
    SignalTabletActive(ctx);
}

void TPQTabletMock::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(ev->Get()->Leader, "Unexpectedly connected to follower of tablet %" PRIu64, ev->Get()->TabletId);

    if (PipeClientCache->OnConnect(ev)) {
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "Connected to tablet " << ev->Get()->TabletId << " from tablet " << TabletID());
    } else {
        if (ev->Get()->Dead) {
            //AckRSToDeletedTablet(ev->Get()->TabletId, ctx);
        } else {
            LOG_NOTICE_S(ctx, NKikimrServices::PERSQUEUE,
                         "Failed to connect to tablet " << ev->Get()->TabletId << " from tablet " << TabletID());
            //RestartPipeRS(ev->Get()->TabletId, ctx);
        }
    }
}

void TPQTabletMock::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Client pipe to tablet " << ev->Get()->TabletId << " from " << TabletID() << " is reset");

    PipeClientCache->OnDisconnect(ev);
    //RestartPipeRS(ev->Get()->TabletId, ctx);
}

void TPQTabletMock::Handle(TEvTxProcessing::TEvReadSet::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto& record = ev->Get()->Record;

    ReadSet = record;
    ReadSets[std::make_pair(record.GetStep(), record.GetTxId())].push_back(record);
}

void TPQTabletMock::Handle(TEvTxProcessing::TEvReadSetAck::TPtr& ev, const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    ReadSetAck = ev->Get()->Record;
}

void TPQTabletMock::Handle(TEvPQTablet::TEvSendReadSet::TPtr& ev, const TActorContext& ctx)
{
    TSendReadSetParams& params = ev->Get()->Params;

    NKikimrTx::TReadSetData payload;
    payload.SetDecision(params.Decision);

    TString body;
    Y_ABORT_UNLESS(payload.SerializeToString(&body));

    auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(params.Step,
                                                               params.TxId,
                                                               TabletID(),
                                                               params.Target,
                                                               TabletID(),
                                                               body,
                                                               0);

    PipeClientCache->Send(ctx, params.Target, event.release());
}

void TPQTabletMock::Handle(TEvPQTablet::TEvSendReadSetAck::TPtr& ev, const TActorContext& ctx)
{
    TSendReadSetAckParams& params = ev->Get()->Params;

    auto event = std::make_unique<TEvTxProcessing::TEvReadSetAck>(params.Step,
                                                                  params.TxId,
                                                                  params.Source,
                                                                  TabletID(),
                                                                  TabletID(),
                                                                  0);

    PipeClientCache->Send(ctx, params.Source, event.release());
}

TPQTabletMock* CreatePQTabletMock(const TActorId& tablet, TTabletStorageInfo *info)
{
    return new TPQTabletMock(tablet, info);
}

}
