#pragma once 

#include "kqp_ic_gateway_actors.h"
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NKqp {

class TSchemeOpRequestHandler: public TRequestHandlerBase<
    TSchemeOpRequestHandler,
    TEvTxUserProxy::TEvProposeTransaction,
    TEvTxUserProxy::TEvProposeTransactionStatus,
    NYql::IKikimrGateway::TGenericResult>
{
public:
    using TBase = typename TSchemeOpRequestHandler::TBase;
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;
    using TResponse = TEvTxUserProxy::TEvProposeTransactionStatus;
    using TResult = NYql::IKikimrGateway::TGenericResult;

    TSchemeOpRequestHandler(TRequest* request, NThreading::TPromise<TResult> promise, bool failedOnAlreadyExists)
        : TBase(request, promise, {})
        , FailedOnAlreadyExists(failedOnAlreadyExists)
        {}

    TSchemeOpRequestHandler(TRequest* request, NThreading::TPromise<TResult> promise, bool failedOnAlreadyExists, bool successOnNotExist)
        : TBase(request, promise, {})
        , FailedOnAlreadyExists(failedOnAlreadyExists)
        , SuccessOnNotExist(successOnNotExist)
        {}


    void Bootstrap(const TActorContext& ctx) {
        TActorId txproxy = MakeTxProxyID();
        ctx.Send(txproxy, this->Request.Release());

        this->Become(&TSchemeOpRequestHandler::AwaitState);
    }

    using TBase::Handle;

    void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& response = ev->Get()->Record;
        auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(response.GetStatus());

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Received TEvProposeTransactionStatus for scheme request"
            << ", TxId: " << response.GetTxId()
            << ", status: " << status
            << ", scheme shard status: " << response.GetSchemeShardStatus());

        switch (status) {
            case TEvTxUserProxy::TResultStatus::ExecInProgress: {
                ui64 schemeShardTabletId = response.GetSchemeShardTabletId();
                IActor* pipeActor = NTabletPipe::CreateClient(ctx.SelfID, schemeShardTabletId);
                Y_ABORT_UNLESS(pipeActor);
                ShemePipeActorId = ctx.ExecutorThread.RegisterActor(pipeActor);

                auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
                request->Record.SetTxId(response.GetTxId());
                NTabletPipe::SendData(ctx, ShemePipeActorId, request.Release());

                LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Sent TEvNotifyTxCompletion request"
                    << ", TxId: " << response.GetTxId());

                return;
            }

            case TEvTxUserProxy::TResultStatus::AccessDenied: {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Access denied for scheme request"
                    << ", TxId: " << response.GetTxId());

                NYql::TIssue issue(NYql::TPosition(), "Access denied.");
                Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
                    "Access denied for scheme request", {issue}));
                this->Die(ctx);
                return;
            }

            case TEvTxUserProxy::TResultStatus::ExecComplete: {
                if (response.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess ||
                    (!FailedOnAlreadyExists && response.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists))
                {
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Successful completion of scheme request"
                        << ", TxId: " << response.GetTxId());

                    if (!response.GetIssues().empty()) {
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(response.GetIssues(), issues);
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::SUCCESS, "", issues));
                    } else {
                        TResult result;
                        result.SetSuccess();
                        Promise.SetValue(std::move(result));
                    }

                    this->Die(ctx);
                    return;
                }
                break;
            }

            case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable: {
                Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    "Schemeshard not available", {}));
                this->Die(ctx);
                return;
            }

            case TEvTxUserProxy::TResultStatus::ResolveError: {
                if (response.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusPathDoesNotExist
                    && SuccessOnNotExist) {
                    LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Successful completion of scheme request: path does not exist,"
                        << "SuccessOnNotExist: true, TxId: " << response.GetTxId());
                    TResult result;
                    result.SetSuccess();
                    Promise.SetValue(std::move(result));
                    this->Die(ctx);
                } else {
                    Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        response.GetSchemeShardReason(), {}));
                    this->Die(ctx);
                }
                return;
            }

            case TEvTxUserProxy::TResultStatus::ExecError:
                switch (response.GetSchemeShardStatus()) {
                    case NKikimrScheme::EStatus::StatusMultipleModifications: {
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_MULTIPLE_SCHEME_MODIFICATIONS,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }
                    case NKikimrScheme::EStatus::StatusPathDoesNotExist: {
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }

                    case NKikimrScheme::EStatus::StatusSchemeError: {
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }

                    case NKikimrScheme::EStatus::StatusPreconditionFailed: {
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }

                    case NKikimrScheme::EStatus::StatusInvalidParameter: {
                        Promise.SetValue(NYql::NCommon::ResultFromIssues<TResult>(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }

                    default:
                        break;
                }
                break;

            default:
                break;
        }

        LOG_ERROR_S(ctx, NKikimrServices::KQP_GATEWAY, "Unexpected error on scheme request"
            << ", TxId: " << response.GetTxId()
            << ", ProxyStatus: " << status
            << ", SchemeShardReason: " << response.GetSchemeShardReason());

        TStringBuilder message;
        message << "Scheme operation failed, status: " << status;
        if (!response.GetSchemeShardReason().empty()) {
            message << ", reason: " << response.GetSchemeShardReason();
        }

        Promise.SetValue(NYql::NCommon::ResultFromError<TResult>(NYql::TIssue({}, message)));
        this->Die(ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Received TEvNotifyTxCompletionResult for scheme request"
            << ", TxId: " << response.GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Successful completion of scheme request"
            << ", TxId: " << response.GetTxId());

        TResult result;
        result.SetSuccess();

        Promise.SetValue(std::move(result));
        NTabletPipe::CloseClient(ctx, ShemePipeActorId);
        this->Die(ctx);
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {}

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        default:
            TBase::HandleUnexpectedEvent("TSchemeOpRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ShemePipeActorId;
    bool FailedOnAlreadyExists = false;
    bool SuccessOnNotExist = false;
};

} // namespace NKqp
} // namespace NKikimr
