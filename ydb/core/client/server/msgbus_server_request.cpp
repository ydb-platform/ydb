#include "msgbus_server_request.h"
#include "msgbus_securereq.h"

#include <ydb/core/actorlib_impl/async_destroyer.h>
#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/protos/query_stats.pb.h>

#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NKikimr {
namespace NMsgBusProxy {

class TMessageBusServerRequest : public TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerRequest>> {
    using TBase = TMessageBusSecureRequest<TMessageBusServerRequestBase<TMessageBusServerRequest>>;
    THolder<TBusRequest> Request;
    TVector<TString*> WriteResolvedKeysTo;
    TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal;
    TAutoPtr<NKikimrTxUserProxy::TEvProposeTransactionStatus> ProposalStatus;

    size_t InFlyRequests;
    THashMap<TString, ui64> CompileResolveCookies;
    TString TextProgramForCompilation;
    bool CompilationRetried;

    void ReplyWithResult(EResponseStatus status, NKikimrTxUserProxy::TEvProposeTransactionStatus &result,
                         const TActorContext &ctx);
    bool RetryResolve(const TActorContext &ctx);
    void FinishReply(const TActorContext &ctx);
    void TryToAllocateQuota(const TActorContext &ctx);

    void Handle(TMiniKQLCompileServiceEvents::TEvCompileStatus::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev, const TActorContext &ctx);

    bool AllRequestsCompleted(const TActorContext& ctx);
    bool AllRequestsCompletedMKQL(const TActorContext& ctx);
    bool AllRequestsCompletedReadTable(const TActorContext& ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_MKQL_REQUEST;
    }

    TMessageBusServerRequest(TEvBusProxy::TEvRequest* msg)
        : TBase(msg->MsgContext)
        , Request(static_cast<TBusRequest*>(msg->MsgContext.ReleaseMessage()))
        , InFlyRequests(0)
        , CompilationRetried(false)
    {
        TBase::SetSecurityToken(Request->Record.GetSecurityToken());
        TBase::SetRequireAdminAccess(true); // MiniKQL and ReadTable execution required administative access
        TBase::SetPeerName(msg->MsgContext.GetPeerName());
    }

    //STFUNC(StateWork)
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TMiniKQLCompileServiceEvents::TEvCompileStatus, Handle);
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            HFunc(TEvDataShard::TEvGetReadTableStreamStateRequest, Handle);
        }
    }

    void Bootstrap(const TActorContext &ctx) {
        TBase::Become(&TMessageBusServerRequest::StateWork);

        ProposalStatus.Reset(new NKikimrTxUserProxy::TEvProposeTransactionStatus());
        Proposal.Reset(new TEvTxUserProxy::TEvProposeTransaction());
        NKikimrTxUserProxy::TEvProposeTransaction &record = Proposal->Record;
        record.SetPeerName(GetPeerName());

        // Transaction protobuf structure might be very heavy (if it has a batch of parameters)
        // so we don't want to copy it, just move its contents
        record.MutableTransaction()->Swap(Request->Record.MutableTransaction());

        if (Request->Record.HasProxyFlags())
            record.SetProxyFlags(Request->Record.GetProxyFlags());

        if (Request->Record.HasExecTimeoutPeriod())
            record.SetExecTimeoutPeriod(Request->Record.GetExecTimeoutPeriod());
        else {
            ui64 msgBusTimeout = GetTotalTimeout() * 3 / 4;
            if (msgBusTimeout > 3600000) // when we see something weird - rewrite with somewhat meaningful
                msgBusTimeout = 1800000;
            record.SetExecTimeoutPeriod(msgBusTimeout);
        }

        record.SetStreamResponse(false);

        auto* transaction = record.MutableTransaction();
        if (transaction->HasMiniKQLTransaction()) {
            auto& mkqlTx = *transaction->MutableMiniKQLTransaction();
            if (!mkqlTx.GetFlatMKQL()) {
                return HandleError(MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::EStatus::NotImplemented, ctx);
            }

            if (mkqlTx.HasProgram() && mkqlTx.GetProgram().HasText()) {
                TextProgramForCompilation = mkqlTx.GetProgram().GetText();
                const bool forceRefresh = (mkqlTx.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE);
                auto* compEv = new TMiniKQLCompileServiceEvents::TEvCompile(TextProgramForCompilation);
                compEv->ForceRefresh = forceRefresh;

                ctx.Send(GetMiniKQLCompileServiceID(), compEv);
                ++InFlyRequests;
            }
            if (mkqlTx.HasParams()) {
                if (mkqlTx.GetParams().HasText()) {
                    auto* compEv = new TMiniKQLCompileServiceEvents::TEvCompile(mkqlTx.GetParams().GetText());
                    ctx.Send(GetMiniKQLCompileServiceID(), compEv); // todo: handle undelivery (with warns atleast)
                    ++InFlyRequests;
                } else
                if (mkqlTx.GetParams().HasProto()) {
                    try {
                        TAlignedPagePoolCounters counters(AppData(ctx)->Counters, "params");
                        NMiniKQL::TScopedAlloc alloc(__LOCATION__, counters,
                            AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
                        NMiniKQL::TTypeEnvironment env(alloc);
                        NMiniKQL::TRuntimeNode node = NMiniKQL::ImportValueFromProto(mkqlTx.GetParams().GetProto(), env);
                        TString bin = NMiniKQL::SerializeRuntimeNode(node, env);
                        mkqlTx.MutableParams()->ClearProto();
                        mkqlTx.MutableParams()->SetBin(bin);
                    }
                    catch(const yexception& e) {
                        NKikimrTxUserProxy::TEvProposeTransactionStatus status;
                        *status.MutableMiniKQLErrors() = e.what();
                        return ReplyWithResult(EResponseStatus::MSTATUS_ERROR, status, ctx);
                    }
                }
            }
            if (InFlyRequests == 0) {
                AllRequestsCompleted(ctx);
            }
            return;
        } else if (transaction->HasReadTableTransaction()) {
            NKikimrTxUserProxy::TEvProposeTransactionStatus status;
            return ReplyWithResult(EResponseStatus::MSTATUS_ERROR, status, ctx);
        }
        ctx.Send(MakeTxProxyID(), Proposal.Release());
    }
};

bool TMessageBusServerRequest::RetryResolve(const TActorContext &ctx) {
    if (CompilationRetried || !TextProgramForCompilation)
        return false;

    auto *compEv = new TMiniKQLCompileServiceEvents::TEvCompile(TextProgramForCompilation);
    compEv->ForceRefresh = false;
    compEv->CompileResolveCookies = std::move(CompileResolveCookies);
    ctx.Send(GetMiniKQLCompileServiceID(), compEv);
    ++InFlyRequests;

    CompilationRetried = true;
    return true;
}

void TMessageBusServerRequest::ReplyWithResult(EResponseStatus status,
                                               NKikimrTxUserProxy::TEvProposeTransactionStatus &result,
                                               const TActorContext &ctx)
{
    TAutoPtr<TBusResponse> response(ProposeTransactionStatusToResponse(status, result));

    if (result.HasExecutionEngineEvaluatedResponse()) {
        response->Record.MutableExecutionEngineEvaluatedResponse()->Swap(result.MutableExecutionEngineEvaluatedResponse());
    }
    if (result.HasSerializedReadTableResponse()) {
        response->Record.SetSerializedReadTableResponse(result.GetSerializedReadTableResponse());
    }
    if (result.HasStatus()) {
        response->Record.SetProxyErrorCode(result.GetStatus());
    }

    if (result.HasTxStats()) {
        response->Record.MutableTxStats()->Swap(result.MutableTxStats());
    }

    SendReplyAutoPtr(response);

    FinishReply(ctx);
}

void TMessageBusServerRequest::FinishReply(const TActorContext &ctx)
{
    if (Proposal)
        AsyncDestroy(Proposal, ctx, AppData(ctx)->UserPoolId);

    Die(ctx);
}

void TMessageBusServerRequest::Handle(TMiniKQLCompileServiceEvents::TEvCompileStatus::TPtr &ev, const TActorContext &ctx) {
    auto* mkqlTx = Proposal->Record.MutableTransaction()->MutableMiniKQLTransaction();

    const auto& result = ev->Get()->Result;

    const bool need2CompileProgram = (bool)TextProgramForCompilation;
    const bool need2CompileParams = mkqlTx->HasParams() && mkqlTx->GetParams().HasText();
    const TString& pgm = ev->Get()->Program;
    Y_ABORT_UNLESS((need2CompileProgram && TextProgramForCompilation == pgm) // TODO: do not check texts, trust cookies
        || (need2CompileParams && mkqlTx->GetParams().GetText() == pgm));

    if (need2CompileProgram && TextProgramForCompilation == pgm) {
        auto* compileResults = ProposalStatus->MutableMiniKQLCompileResults();
        auto* pgm = mkqlTx->MutableProgram();
        if (result.Errors.Empty()) {
            pgm->ClearText();
            pgm->SetBin(result.CompiledProgram);
            compileResults->SetCompiledProgram(result.CompiledProgram);
        } else {
            NYql::IssuesToMessage(result.Errors, compileResults->MutableProgramCompileErrors());
        }
        --InFlyRequests;
        CompileResolveCookies = std::move(ev->Get()->CompileResolveCookies);
    }

    if (need2CompileParams && mkqlTx->GetParams().GetText() == pgm) {
        auto* compileResults = ProposalStatus->MutableMiniKQLCompileResults();
        auto* params = mkqlTx->MutableParams();
        if (result.Errors.Empty()) {
            params->ClearText();
            params->SetBin(result.CompiledProgram);
            compileResults->SetCompiledParams(result.CompiledProgram);
        } else {
            NYql::IssuesToMessage(result.Errors, compileResults->MutableParamsCompileErrors());
        }
        --InFlyRequests;
    }

    if (InFlyRequests == 0) {
        AllRequestsCompleted(ctx);
    }
}

bool TMessageBusServerRequest::AllRequestsCompleted(const TActorContext& ctx) {
    auto &transaction = Proposal->Record.GetTransaction();
    if (transaction.HasMiniKQLTransaction())
        return AllRequestsCompletedMKQL(ctx);
    else
        Y_ABORT("Unexpected transaction type");
}

bool TMessageBusServerRequest::AllRequestsCompletedMKQL(const TActorContext& ctx) {
    auto* mkqlTx = Proposal->Record.MutableTransaction()->MutableMiniKQLTransaction();
    const bool need2CompileProgram = mkqlTx->HasProgram() && mkqlTx->GetProgram().HasText();
    const bool need2CompileParams = mkqlTx->HasParams() && mkqlTx->GetParams().HasText();
    const bool programCompiled = need2CompileProgram
        ? (ProposalStatus->MutableMiniKQLCompileResults()->ProgramCompileErrorsSize() == 0)
        : true;
    const bool paramsCompiled = need2CompileParams
        ? (ProposalStatus->MutableMiniKQLCompileResults()->ParamsCompileErrorsSize() == 0)
        : true;

    const bool allCompiledOk = programCompiled && paramsCompiled;
    if (!allCompiledOk) {
        ReplyWithResult(MSTATUS_ERROR, *ProposalStatus.Get(), ctx);
        return false;
    }

    NKikimrTxUserProxy::TEvProposeTransaction &record = Proposal->Record;
    record.SetUserToken(TBase::GetSerializedToken());

    switch (mkqlTx->GetMode()) {
        case NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE_AND_EXEC: {
            TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> ev = new TEvTxUserProxy::TEvProposeTransaction();
            ev->Record.CopyFrom(Proposal->Record);
            ctx.Send(MakeTxProxyID(), ev.Release());
            return true;
        }
        case NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE: {
            ReplyWithResult(MSTATUS_OK, *ProposalStatus.Get(), ctx);
            return true;
        }
        default:
            Y_ABORT("Unknown mkqlTxMode.");
    }
}

void TMessageBusServerRequest::Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr &ev, const TActorContext &ctx) {
    TEvTxUserProxy::TEvProposeTransactionStatus *msg = ev->Get();

    const TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
    switch (status) {
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyAccepted:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned:
    // transitional statuses
        return;
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready:
    // completion
        return ReplyWithResult(MSTATUS_OK, msg->Record, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout:
        return ReplyWithResult(MSTATUS_INPROGRESS, msg->Record, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady:
        return HandleError(MSTATUS_NOTREADY, status, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted:
        return HandleError(MSTATUS_ABORTED, status, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::EmptyAffectedSet:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest:
        return ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
        if (!RetryResolve(ctx))
            ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
        return;
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable:
        if (!RetryResolve(ctx)) // TODO: retry if partitioning changed due to split/merge
            ReplyWithResult(MSTATUS_REJECTED, msg->Record, ctx);
        return;
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown:
        return ReplyWithResult(MSTATUS_TIMEOUT, msg->Record, ctx);
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorAborted:
    case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorOutdated:
        return ReplyWithResult(MSTATUS_REJECTED, msg->Record, ctx);
    default:
        return ReplyWithResult(MSTATUS_INTERNALERROR, msg->Record, ctx);
    }
}

void TMessageBusServerRequest::Handle(TEvDataShard::TEvGetReadTableStreamStateRequest::TPtr &ev, const TActorContext &ctx) {
    auto *response = new TEvDataShard::TEvGetReadTableStreamStateResponse;

    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::GENERIC_ERROR);
    auto *issue = response->Record.MutableStatus()->AddIssues();
    issue->set_severity(NYql::TSeverityIds::S_ERROR);
    issue->set_message("request proxy is not streaming");
    ctx.Send(ev->Sender, response);
}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.Register(new TMessageBusServerRequest(ev->Get()));
}

}
}
