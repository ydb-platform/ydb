#include "kqp_executer.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/session_actor/kqp_worker_common.h>

namespace NKikimr::NKqp {

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << ". " << stream)

using namespace NThreading;

namespace {

class TKqpSchemeExecuter : public TActorBootstrapped<TKqpSchemeExecuter> {
    struct TEvPrivate {
        enum EEv {
            EvResult = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvResult : public TEventLocal<TEvResult, EEv::EvResult> {
            IKqpGateway::TGenericResult Result;
        };
    };
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpSchemeExecuter(TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TString& database,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc)
        : PhyTx(phyTx)
        , Target(target)
        , Database(database)
        , UserToken(userToken)
    {
        YQL_ENSURE(PhyTx);
        YQL_ENSURE(PhyTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_SCHEME);

        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(txAlloc);
    }

    void Bootstrap() {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(Database);
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        const auto& schemeOp = PhyTx->GetSchemeOperation();
        switch (schemeOp.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateTable:
                ev->Record.MutableTransaction()->MutableModifyScheme()->CopyFrom(schemeOp.GetCreateTable());
                break;

            default:
                InternalError(TStringBuilder() << "Unexpected scheme operation: "
                    << (ui32) schemeOp.GetOperationCase());
                return;
        }

        auto promise = NewPromise<IKqpGateway::TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(ev.Release(), promise, true);
        RegisterWithSameMailbox(requestHandler);

        auto actorSystem = TlsActivationContext->AsActorContext().ExecutorThread.ActorSystem;
        auto selfId = SelfId();
        promise.GetFuture().Subscribe([actorSystem, selfId](const TFuture<IKqpGateway::TGenericResult>& future) {
            auto ev = MakeHolder<TEvPrivate::TEvResult>();
            ev->Result = future.GetValue();

            actorSystem->Send(selfId, ev.Release());
        });

        Become(&TKqpSchemeExecuter::ExecuteState);
    }

public:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvResult, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
    }

    void HandleExecute(TEvPrivate::TEvResult::TPtr& ev) {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(GetYdbStatus(ev->Get()->Result));
        IssuesToMessage(ev->Get()->Result.Issues(), response.MutableIssues());

        Send(Target, ResponseEv.release());
        PassAway();
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        LOG_D("Got EvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());

        ReplyErrorAndDie(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
    }

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpSchemeExecuter, unexpected event: " << eventType
            << ", at state:" << state << ", selfID: " << SelfId());
        InternalError(TStringBuilder() << "Unexpected event at TKqpSchemeExecuter, state: " << state
            << ", event: " << eventType);
    }

    void InternalError(const NYql::TIssues& issues) {
        LOG_E(issues.ToOneLineString());
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED,
            "Internal error while executing scheme operation.");

        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void InternalError(const TString& message) {
        InternalError(NYql::TIssues({NYql::TIssue(message)}));
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    TKqpPhyTxHolder::TConstPtr PhyTx;
    const TActorId Target;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
};

} // namespace

IActor* CreateKqpSchemeExecuter(TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc)
{
    return new TKqpSchemeExecuter(phyTx, target, database, userToken, txAlloc);
}

} // namespace NKikimr::NKqp
