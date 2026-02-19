#include "ss_proxy_actor.h"

// #include <ydb/core/nbs/cloud/blockstore/libs/storage/core/config.h>
// #include <ydb/core/nbs/cloud/blockstore/libs/storage/core/probes.h>
// #include
// <ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/ss_proxy_events_private.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/helpers.h>
#include <ydb/core/nbs/cloud/storage/core/libs/kikimr/helpers.h>

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

// LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER)

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeSchemeActor final
    : public TActorBootstrapped<TDescribeSchemeActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString SchemeShardDir;
    const TString Path;
    TActorId PathDescriptionBackup;

public:
    TDescribeSchemeActor(TRequestInfoPtr requestInfo, TString schemeShardDir,
                         TString path, TActorId pathDescriptionBackup);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeScheme(const TActorContext& ctx);

    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response);

private:
    STFUNC(StateWork);

    void HandleDescribeSchemeResult(
        const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeSchemeActor::TDescribeSchemeActor(TRequestInfoPtr requestInfo,
                                           TString schemeShardDir, TString path,
                                           TActorId pathDescriptionBackup)
    : RequestInfo(std::move(requestInfo))
    , SchemeShardDir(std::move(schemeShardDir))
    , Path(std::move(path))
    , PathDescriptionBackup(std::move(pathDescriptionBackup))
{}

void TDescribeSchemeActor::Bootstrap(const TActorContext& ctx)
{
    DescribeScheme(ctx);
    Become(&TThis::StateWork);
}

void TDescribeSchemeActor::DescribeScheme(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(Path);
    request->Record.SetDatabaseName(SchemeShardDir);

    // LWTRACK(
    //     RequestSent_Proxy,
    //     RequestInfo->CallContext->LWOrbit,
    //     "Navigate",
    //     RequestInfo->CallContext->RequestId);

    NYdb::NBS::Send(ctx, MakeTxProxyID(), std::move(request));
}

bool TDescribeSchemeActor::HandleError(const TActorContext& ctx,
                                       const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        ReplyAndDie(
            ctx,
            std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(error));
        return true;
    }
    return false;
}

void TDescribeSchemeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeSchemeResponse> response)
{
    NYdb::NBS::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TDescribeSchemeActor::HandleDescribeSchemeResult(
    const TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->GetRecord();

    auto error = MakeSchemeShardError(record.GetStatus(), record.GetReason());

    // LWTRACK(
    //     ResponseReceived_Proxy,
    //     RequestInfo->CallContext->LWOrbit,
    //     "DescribeSchemeResult",
    //     RequestInfo->CallContext->RequestId);

    if (HasError(error)) {
        auto status = static_cast<NKikimrScheme::EStatus>(
            STATUS_FROM_CODE(error.GetCode()));

        if (status == NKikimrScheme::StatusNotAvailable) {
            error.SetCode(E_REJECTED);
        }

        // TODO: return E_NOT_FOUND instead of StatusPathDoesNotExist
        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            SetErrorProtoFlag(error, NYdb::NBS::NProto::EF_SILENT);
        }
    }

    if (HandleError(ctx, error)) {
        return;
    }

    // if (PathDescriptionBackup) {
    //     auto updateRequest =
    //         std::make_unique<TEvSSProxyPrivate::TEvUpdatePathDescriptionBackupRequest>(
    //             record.GetPath(),
    //             record.GetPathDescription()
    //         );
    //     NYdb::NBS::Send(ctx, PathDescriptionBackup,
    //     std::move(updateRequest));
    // }

    auto response = std::make_unique<TEvSSProxy::TEvDescribeSchemeResponse>(
        record.GetPath(), record.GetPathDescription());

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TDescribeSchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSchemeShard::TEvDescribeSchemeResult,
              HandleDescribeSchemeResult);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeScheme(
    const TEvSSProxy::TEvDescribeSchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NYdb::NBS::Register<TDescribeSchemeActor>(
        ctx, std::move(requestInfo), NbsStorageConfig.GetSchemeShardDir(),
        msg->Path, PathDescriptionBackup);
}

}   // namespace NYdb::NBS::NStorage
