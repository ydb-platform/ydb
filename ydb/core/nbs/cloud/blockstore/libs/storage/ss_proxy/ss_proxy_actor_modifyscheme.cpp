#include "ss_proxy_actor.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NSchemeShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TModifySchemeActor final: public TActorBootstrapped<TModifySchemeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TActorId Owner;
    const NKikimrSchemeOp::TModifyScheme ModifyScheme;

    ui64 TxId = 0;
    ui64 SchemeShardTabletId = 0;
    NKikimrScheme::EStatus SchemeShardStatus = NKikimrScheme::StatusSuccess;
    TString SchemeShardReason;

public:
    TModifySchemeActor(TRequestInfoPtr requestInfo, const TActorId& owner,
                       NKikimrSchemeOp::TModifyScheme modifyScheme);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleStatus(
        const TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
        const TActorContext& ctx);

    void HandleTxDone(const TEvSSProxy::TEvWaitSchemeTxResponse::TPtr& ev,
                      const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx,
                     NProto::TError error = NProto::TError());
};

////////////////////////////////////////////////////////////////////////////////

TModifySchemeActor::TModifySchemeActor(
    TRequestInfoPtr requestInfo, const TActorId& owner,
    NKikimrSchemeOp::TModifyScheme modifyScheme)
    : RequestInfo(std::move(requestInfo))
    , Owner(owner)
    , ModifyScheme(std::move(modifyScheme))
{}

void TModifySchemeActor::Bootstrap(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();

    auto* tx = request->Record.MutableTransaction();
    tx->MutableModifyScheme()->CopyFrom(ModifyScheme);

    NYdb::NBS::Send(ctx, MakeTxProxyID(), std::move(request));

    Become(&TThis::StateWork);
}

void TModifySchemeActor::HandleStatus(
    const TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& record = ev->Get()->Record;

    TxId = record.GetTxId();
    SchemeShardTabletId = record.GetSchemeShardTabletId();
    SchemeShardStatus = (NKikimrScheme::EStatus)record.GetSchemeShardStatus();
    SchemeShardReason = record.GetSchemeShardReason();

    auto status = (TEvTxUserProxy::TEvProposeTransactionStatus::EStatus)
                      record.GetStatus();
    switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
            LOG_DEBUG(
                ctx,
                NKikimrServices::NBS_SS_PROXY,
                "Request %s with TxId# %lu completed immediately",
                NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data(), TxId);

            ReplyAndDie(ctx);
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::
            ExecInProgress:
            LOG_DEBUG(
                ctx,
                NKikimrServices::NBS_SS_PROXY,
                "Request %s with TxId# %lu in progress, waiting for completion",
                NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data(), TxId);

            NYdb::NBS::Send<TEvSSProxy::TEvWaitSchemeTxRequest>(
                ctx, Owner, 0, SchemeShardTabletId, TxId);
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError: {
            LOG_DEBUG(
                ctx,
                NKikimrServices::NBS_SS_PROXY,
                "Request %s with TxId# %lu failed with status %s",
                NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data(), TxId,
                NKikimrScheme::EStatus_Name(SchemeShardStatus).data());

            if ((SchemeShardStatus ==
                 NKikimrScheme::StatusMultipleModifications) &&
                (record.GetPathCreateTxId() != 0 ||
                 record.GetPathDropTxId() != 0))
            {
                ui64 txId = record.GetPathCreateTxId() != 0
                                ? record.GetPathCreateTxId()
                                : record.GetPathDropTxId();
                LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
                          "Waiting for a different TxId# %lu", txId);

                NYdb::NBS::Send<TEvSSProxy::TEvWaitSchemeTxRequest>(
                    ctx, Owner, 0, SchemeShardTabletId, txId);
                break;
            }

            ui32 errorCode = MAKE_SCHEMESHARD_ERROR(SchemeShardStatus);

            if (SchemeShardStatus ==
                    NKikimrScheme::StatusMultipleModifications ||
                SchemeShardStatus == NKikimrScheme::StatusNotAvailable)
            {
                errorCode = E_REJECTED;
            }

            ReplyAndDie(
                ctx,
                MakeError(
                    errorCode,
                    (TStringBuilder()
                     << NKikimrSchemeOp::EOperationType_Name(
                            ModifyScheme.GetOperationType())
                            .data()
                     << " failed with reason: "
                     << (SchemeShardReason.empty()
                             ? NKikimrScheme::EStatus_Name(SchemeShardStatus)
                                   .data()
                             : SchemeShardReason))));

            break;
        }

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
            if (SchemeShardStatus == NKikimrScheme::StatusPathDoesNotExist) {
                LOG_DEBUG(
                    ctx,
                    NKikimrServices::NBS_SS_PROXY,
                    "Request %s failed to resolve parent path",
                    NKikimrSchemeOp::EOperationType_Name(
                        ModifyScheme.GetOperationType())
                        .data());

                // TODO: return E_NOT_FOUND instead of StatusPathDoesNotExist
                ReplyAndDie(
                    ctx,
                    MakeError(
                        MAKE_SCHEMESHARD_ERROR(SchemeShardStatus),
                        (TStringBuilder()
                         << NKikimrSchemeOp::EOperationType_Name(
                                ModifyScheme.GetOperationType())
                                .data()
                         << " failed with reason: "
                         << (SchemeShardReason.empty()
                                 ? NKikimrScheme::EStatus_Name(
                                       SchemeShardStatus)
                                       .data()
                                 : SchemeShardReason))));
                break;
            }

            /* fall through */

        default:
            LOG_DEBUG(
                ctx,
                NKikimrServices::NBS_SS_PROXY,
                "Request %s to tx_proxy failed with code %u",
                NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data(), status);

            ReplyAndDie(
                ctx,
                TranslateTxProxyError(MakeError(
                    MAKE_TXPROXY_ERROR(status),
                    TStringBuilder() << "TxProxy failed: " << status)));
            break;
    }
}

void TModifySchemeActor::HandleTxDone(
    const TEvSSProxy::TEvWaitSchemeTxResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, NKikimrServices::NBS_SS_PROXY,
              "TModifySchemeActor received TEvWaitSchemeTxResponse");

    ReplyAndDie(ctx, msg->GetError());
}

void TModifySchemeActor::ReplyAndDie(const TActorContext& ctx,
                                     NProto::TError error)
{
    if (SchemeShardStatus == NKikimrScheme::StatusPreconditionFailed) {
        error = GetErrorFromPreconditionFailed(error);
    }

    auto response = std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(
        error, SchemeShardTabletId, SchemeShardStatus, SchemeShardReason);

    NYdb::NBS::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TModifySchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleStatus);
        HFunc(TEvSSProxy::TEvWaitSchemeTxResponse, HandleTxDone);

        default:
            HandleUnexpectedEvent(ev, NKikimrServices::NBS_SS_PROXY,
                                  __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleModifyScheme(
    const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NYdb::NBS::Register<TModifySchemeActor>(ctx, std::move(requestInfo),
                                            ctx.SelfID, msg->ModifyScheme);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvSSProxy::TEvModifySchemeRequest>
CreateModifySchemeRequestForAlterVolume(
    TString path, ui64 pathId, ui64 version,
    const NKikimrBlockStore::TVolumeConfig& volumeConfig)
{
    TString volumeDir;
    TString volumeName;

    {
        TStringBuf dir;
        TStringBuf name;
        TStringBuf(path).RSplit('/', dir, name);
        volumeDir = TString{dir};
        volumeName = TString{name};
    }

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir(volumeDir);
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume);

    auto* op = modifyScheme.MutableAlterBlockStoreVolume();
    op->SetName(volumeName);

    op->MutableVolumeConfig()->CopyFrom(volumeConfig);
    auto* applyIf = modifyScheme.MutableApplyIf()->Add();
    applyIf->SetPathId(pathId);
    applyIf->SetPathVersion(version);

    return std::make_unique<TEvSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));
}

}   // namespace NYdb::NBS::NStorage
