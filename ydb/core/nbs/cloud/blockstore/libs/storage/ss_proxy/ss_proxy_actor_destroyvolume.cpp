#include "ss_proxy_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/volume_label.h>

#include <ydb/core/nbs/cloud/storage/core/libs/actors/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyVolumeActor final: public TActorBootstrapped<TDestroyVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TString SchemeShardDir;
    const TString DiskId;

    TString VolumeDir;
    TString VolumeName;

public:
    TDestroyVolumeActor(
        TRequestInfoPtr requestInfo,
        TString schemeShardDir,
        TString diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DestroyVolume(const TActorContext& ctx);

    void HandleDestroyVolumeResponse(
        const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDestroyVolumeResponse> response);

    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TDestroyVolumeActor::TDestroyVolumeActor(
    TRequestInfoPtr requestInfo,
    TString schemeShardDir,
    TString diskId)
    : RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , DiskId(std::move(diskId))
{
    if (DiskId) {
        std::tie(VolumeDir, VolumeName) =
            DiskIdToVolumeDirAndName(SchemeShardDir, DiskId);
    }
}

void TDestroyVolumeActor::Bootstrap(const TActorContext& ctx)
{
    if (!DiskId || !VolumeName) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDestroyVolumeResponse>(MakeError(
                E_ARGUMENT,
                "DestroyVolume requires a non-empty DiskId")));
        return;
    }

    DestroyVolume(ctx);
    Become(&TThis::StateWork);
}

void TDestroyVolumeActor::DestroyVolume(const TActorContext& ctx)
{
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::EOperationType::ESchemeOpDropBlockStoreVolume);
    modifyScheme.SetWorkingDir(VolumeDir);
    modifyScheme.MutableDrop()->SetName(VolumeName);

    auto request = std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    LOG_DEBUG(
        ctx,
        NKikimrServices::NBS_SS_PROXY,
        "Sending drop request for %s in directory %s (diskId %s)",
        VolumeName.Quote().data(),
        VolumeDir.Quote().data(),
        DiskId.Quote().data());

    NYdb::NBS::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDestroyVolumeActor::HandleDestroyVolumeResponse(
    const TEvSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            NKikimrServices::NBS_SS_PROXY,
            "Volume %s: drop failed: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDestroyVolumeResponse>(error));
        return;
    }

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_SS_PROXY,
        "Volume %s dropped successfully",
        DiskId.Quote().data());

    ReplyAndDie(ctx, std::make_unique<TEvSSProxy::TEvDestroyVolumeResponse>());
}

void TDestroyVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDestroyVolumeResponse> response)
{
    NYdb::NBS::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TDestroyVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvModifySchemeResponse, HandleDestroyVolumeResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                NKikimrServices::NBS_SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDestroyVolume(
    const TEvSSProxy::TEvDestroyVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NYdb::NBS::Register<TDestroyVolumeActor>(
        ctx,
        std::move(requestInfo),
        NbsStorageConfig.GetSchemeShardDir(),
        msg->DiskId);
}

}   // namespace NYdb::NBS::NStorage
