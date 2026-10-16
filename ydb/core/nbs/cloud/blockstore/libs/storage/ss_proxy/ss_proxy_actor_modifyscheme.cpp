#include "ss_proxy_actor.h"

#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_SS_PROXY

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
    TModifySchemeActor(
        TRequestInfoPtr requestInfo,
        const TActorId& owner,
        NKikimrSchemeOp::TModifyScheme modifyScheme);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleStatus(
        const TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev,
        const TActorContext& ctx);

    void HandleTxDone(
        const TEvSSProxy::TEvWaitSchemeTxResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TError error = NProto::TError());
};

////////////////////////////////////////////////////////////////////////////////

TModifySchemeActor::TModifySchemeActor(
    TRequestInfoPtr requestInfo,
    const TActorId& owner,
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
            YDB_LOG_DEBUG_CTX(ctx, "Request with completed immediately",
                {"#_NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data", NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data()},
                {"txId", TxId});

            ReplyAndDie(ctx);
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::
            ExecInProgress:
            YDB_LOG_DEBUG_CTX(ctx, "Request with in progress, waiting for completion",
                {"#_NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data", NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data()},
                {"txId", TxId});

            NYdb::NBS::Send<TEvSSProxy::TEvWaitSchemeTxRequest>(
                ctx,
                Owner,
                0,
                SchemeShardTabletId,
                TxId);
            break;

        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError: {
            YDB_LOG_DEBUG_CTX(ctx, "Request with failed with status",
                {"#_NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data", NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data()},
                {"txId", TxId},
                {"#_NKikimrScheme::EStatus_Name(SchemeShardStatus).data", NKikimrScheme::EStatus_Name(SchemeShardStatus).data()});

            if ((SchemeShardStatus ==
                 NKikimrScheme::StatusMultipleModifications) &&
                (record.GetPathCreateTxId() != 0 ||
                 record.GetPathDropTxId() != 0))
            {
                ui64 txId = record.GetPathCreateTxId() != 0
                                ? record.GetPathCreateTxId()
                                : record.GetPathDropTxId();
                YDB_LOG_DEBUG_CTX(ctx, "Waiting for a different",
                    {"txId", txId});

                NYdb::NBS::Send<TEvSSProxy::TEvWaitSchemeTxRequest>(
                    ctx,
                    Owner,
                    0,
                    SchemeShardTabletId,
                    txId);
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
                YDB_LOG_DEBUG_CTX(ctx, "Request failed to resolve parent path",
                    {"#_NKikimrSchemeOp::EOperationType_Name(
                        ModifyScheme.GetOperationType())
                        .data", NKikimrSchemeOp::EOperationType_Name(
                        ModifyScheme.GetOperationType())
                        .data()});

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
            YDB_LOG_DEBUG_CTX(ctx, "Request to tx_proxy failed with code",
                {"#_NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data", NKikimrSchemeOp::EOperationType_Name(
                    ModifyScheme.GetOperationType())
                    .data()},
                {"status", status});

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

    YDB_LOG_DEBUG_CTX(ctx, "TModifySchemeActor received TEvWaitSchemeTxResponse");

    ReplyAndDie(ctx, msg->GetError());
}

void TModifySchemeActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    if (SchemeShardStatus == NKikimrScheme::StatusPreconditionFailed) {
        error = GetErrorFromPreconditionFailed(error);
    }

    auto response = std::make_unique<TEvSSProxy::TEvModifySchemeResponse>(
        error,
        SchemeShardTabletId,
        SchemeShardStatus,
        SchemeShardReason);

    NYdb::NBS::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TModifySchemeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, HandleStatus);
        HFunc(TEvSSProxy::TEvWaitSchemeTxResponse, HandleTxDone);

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

void TSSProxyActor::HandleModifyScheme(
    const TEvSSProxy::TEvModifySchemeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    NYdb::NBS::Register<TModifySchemeActor>(
        ctx,
        std::move(requestInfo),
        ctx.SelfID,
        msg->ModifyScheme);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvSSProxy::TEvModifySchemeRequest>
CreateModifySchemeRequestForAlterVolume(
    TString path,
    ui64 pathId,
    ui64 version,
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
