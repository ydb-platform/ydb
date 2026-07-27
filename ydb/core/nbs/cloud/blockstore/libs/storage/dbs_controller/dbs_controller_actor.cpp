#include "dbs_controller_actor.h"

#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

using namespace NKikimr;
using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TDbsControllerActor::TDbsControllerActor(
    const TActorId& tablet,
    NKikimr::TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletBase<TDbsControllerActor>(
          tablet,
          NKikimr::TTabletStorageInfoPtr(info),
          nullptr)
{
    LOG_INFO(
        TActivationContext::AsActorContext(),
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] DbsController initialization started",
        TabletID());
}

TDbsControllerActor::~TDbsControllerActor() = default;

void TDbsControllerActor::OnDetach(const TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] OnDetach",
        TabletID());
    Die(ctx);
}

void TDbsControllerActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] OnTabletDead",
        TabletID());
    Die(ctx);
}

void TDbsControllerActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] Started DbsController: actor id %s",
        TabletID(),
        SelfId().ToString().data());

    if (!Executor()->GetStats().IsFollower()) {
        ExecuteTx(ctx, CreateTx<TInitSchema>());
    }

    // allow pipes to connect
    SignalTabletActive(ctx);
}

void TDbsControllerActor::DefaultSignalTabletActive(const TActorContext& ctx)
{
    Y_UNUSED(ctx);
}

void TDbsControllerActor::HandleServerConnected(
    const TEvTabletPipe::TEvServerConnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] Pipe client %s server %s connected",
        TabletID(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TDbsControllerActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] Pipe client %s server %s disconnected",
        TabletID(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TDbsControllerActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_INFO(
        ctx,
        NKikimrServices::DBS_CONTROLLER,
        "[%lu] Pipe client %s server %s got destroyed",
        TabletID(),
        ToString(msg->ClientId).c_str(),
        ToString(msg->ServerId).c_str());
}

void TDbsControllerActor::ReportTabletState(const TActorContext& ctx)
{
    auto service =
        NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId());

    auto request = std::make_unique<
        NNodeWhiteboard::TEvWhiteboard::TEvWhiteboard::TEvTabletStateUpdate>(
        TabletID(),
        STATE_WORK);

    NYdb::NBS::Send(ctx, service, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TDbsControllerActor::StateInit(TAutoPtr<NActors::IEventHandle>& ev)
{
    StateInitImpl(ev, SelfId());
}

STFUNC(TDbsControllerActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        cFunc(TEvents::TEvPoison::EventType, PassAway);

        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_ERROR(
                    TActivationContext::AsActorContext(),
                    NKikimrServices::DBS_CONTROLLER,
                    "[%lu] Unhandled event type: %u event %s",
                    TabletID(),
                    ev->GetTypeRewrite(),
                    ev->ToString().c_str());
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
