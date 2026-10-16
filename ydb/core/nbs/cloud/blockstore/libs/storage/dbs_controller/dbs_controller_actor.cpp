#include "dbs_controller_actor.h"

#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::DBS_CONTROLLER

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
    YDB_LOG_INFO_CTX(TActivationContext::AsActorContext(), "DbsController initialization started",
        {"tabletID", TabletID()});
}

TDbsControllerActor::~TDbsControllerActor() = default;

void TDbsControllerActor::OnDetach(const TActorContext& ctx)
{
    YDB_LOG_INFO_CTX(ctx, "OnDetach",
        {"tabletID", TabletID()});
    Die(ctx);
}

void TDbsControllerActor::OnTabletDead(
    TEvTablet::TEvTabletDead::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    YDB_LOG_INFO_CTX(ctx, "OnTabletDead",
        {"tabletID", TabletID()});
    Die(ctx);
}

void TDbsControllerActor::OnActivateExecutor(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    YDB_LOG_INFO_CTX(ctx, "Started DbsController: actor id",
        {"tabletID", TabletID()},
        {"#_SelfId", SelfId()});

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

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server connected",
        {"tabletID", TabletID()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TDbsControllerActor::HandleServerDisconnected(
    const TEvTabletPipe::TEvServerDisconnected::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_DEBUG_CTX(ctx, "Pipe client server disconnected",
        {"tabletID", TabletID()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
}

void TDbsControllerActor::HandleServerDestroyed(
    const TEvTabletPipe::TEvServerDestroyed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    YDB_LOG_INFO_CTX(ctx, "Pipe client server got destroyed",
        {"tabletID", TabletID()},
        {"#_ToString(msg->ClientId).c_str", ToString(msg->ClientId)},
        {"#_ToString(msg->ServerId).c_str", ToString(msg->ServerId)});
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
        HFunc(TEvTabletPipe::TEvServerConnected, HandleServerConnected);
        HFunc(TEvTabletPipe::TEvServerDisconnected, HandleServerDisconnected);
        HFunc(TEvTabletPipe::TEvServerDestroyed, HandleServerDestroyed);
        HFunc(
            TEvDbsControllerPrivate::TEvUpdateDDiskMapRequest,
            HandleUpdateDDiskMapRequest);
        HFunc(
            TEvDbsControllerPrivate::TEvRemoveTabletDDiskMapRequest,
            HandleRemoveTabletDDiskMapRequest);
        HFunc(
            TEvDbsControllerPrivate::TEvGetPartitionsForNodeRequest,
            HandleGetPartitionsForNodeRequest);
        HFunc(
            TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest,
            HandleNodeMaintenancePermissionRequest);
        HFunc(
            TEvDbsControllerPrivate::TEvDiskMaintenancePermissionRequest,
            HandleDiskMaintenancePermissionRequest);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                YDB_LOG_ERROR_CTX(TActivationContext::AsActorContext(), "Unhandled event event",
                    {"tabletID", TabletID()},
                    {"type", ev->GetTypeRewrite()},
                    {"#_ev->ToString().c_str", ev->ToString()});
            }
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
