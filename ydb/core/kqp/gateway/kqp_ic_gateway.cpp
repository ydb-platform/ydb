#include "kqp_gateway.h"
#include "kqp_ic_gateway_actors.h"
#include "kqp_metadata_loader.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/grpc_services/table_settings.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/table_profiles.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/persqueue_v1/rpc_calls.h>

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/string/split.h>
#include <util/string/vector.h>

namespace NKikimr {
namespace NKqp {

using NYql::TIssue;
using TIssuesIds = NYql::TIssuesIds;
using namespace NThreading;
using namespace NYql::NCommon;
using namespace NSchemeShard;
using namespace NKikimrSchemeOp;

constexpr const IKqpGateway::TKqpSnapshot IKqpGateway::TKqpSnapshot::InvalidSnapshot = TKqpSnapshot();

#define STATIC_ASSERT_STATE_EQUAL(name) \
    static_assert(static_cast<ui32>(NYql::TIndexDescription::EIndexState::name) \
        == NKikimrSchemeOp::EIndexState::EIndexState##name, \
        "index state missmatch, flag: ## name");

STATIC_ASSERT_STATE_EQUAL(Invalid)
STATIC_ASSERT_STATE_EQUAL(Ready)
STATIC_ASSERT_STATE_EQUAL(NotReady)
STATIC_ASSERT_STATE_EQUAL(WriteOnly)

#undef STATIC_ASSERT_STATE_EQUAL

namespace {

template <class TResult>
static NThreading::TFuture<TResult> NotImplemented() {
    TResult result;
    result.AddIssue(TIssue({}, "Not implemented in interconnect gateway."));
    return NThreading::MakeFuture(result);
}

struct TAppConfigResult : public IKqpGateway::TGenericResult {
    std::shared_ptr<const NKikimrConfig::TAppConfig> Config;
};


template<typename TRequest, typename TResponse, typename TResult>
class TProxyRequestHandler: public TRequestHandlerBase<
    TProxyRequestHandler<TRequest, TResponse, TResult>,
    TRequest,
    TResponse,
    TResult>
{
public:
    using TBase = typename TProxyRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;

    TProxyRequestHandler(TRequest* request, TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback) {}

    void Bootstrap(const TActorContext& ctx) {
        TActorId txproxy = MakeTxProxyID();
        ctx.Send(txproxy, this->Request.Release());

        this->Become(&TProxyRequestHandler::AwaitState);
    }

    using TBase::Handle;
    using TBase::HandleResponse;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

        default:
            TBase::HandleUnexpectedEvent("TProxyRequestHandler", ev->GetTypeRewrite());
        }
    }
};

template<typename TRequest, typename TResponse, typename TResult>
class TKqpRequestHandler: public TRequestHandlerBase<
    TKqpRequestHandler<TRequest, TResponse, TResult>,
    TRequest,
    TResponse,
    TResult>
{
public:
    using TBase = typename TKqpRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;

    TKqpRequestHandler(TRequest* request, TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback) {}

    void Bootstrap(const TActorContext& ctx) {
        TActorId kqpProxy = MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, this->Request.Release());

        this->Become(&TKqpRequestHandler::AwaitState);
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& kqpResponse = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, ctx.SelfID
            << "Received process error for kqp query: " << kqpResponse.GetError());

        TBase::HandleError(kqpResponse.GetError(), ctx);
    }

    using TBase::Handle;
    using TBase::HandleResponse;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpRequestHandler", ev->GetTypeRewrite());
        }
    }
};

class TKqpScanQueryRequestHandler : public TRequestHandlerBase<
    TKqpScanQueryRequestHandler,
    NKqp::TEvKqp::TEvQueryRequest,
    NKqp::TEvKqp::TEvQueryResponse,
    IKqpGateway::TQueryResult>
{
public:
    const ui32 ResultSetBytesLimit = 48 * 1024 * 1024; // 48 MB

    using TRequest = NKqp::TEvKqp::TEvQueryRequest;
    using TResponse = NKqp::TEvKqp::TEvQueryResponse;
    using TResult = IKqpGateway::TQueryResult;

    using TBase = TKqpScanQueryRequestHandler::TBase;

    TKqpScanQueryRequestHandler(TRequest* request, ui64 rowsLimit, TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback)
        , RowsLimit(rowsLimit) {}

    void Bootstrap(const TActorContext& ctx) {
        ActorIdToProto(SelfId(), this->Request->Record.MutableRequestActorId());

        TActorId kqpProxy = MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, this->Request.Release());

        this->Become(&TKqpScanQueryRequestHandler::AwaitState);
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev, const TActorContext& ctx) {
        ExecuterActorId = ev->Sender;
        auto& record = ev->Get()->Record;

        if (!HasMeta) {
            for (auto& column : record.GetResultSet().columns()) {
                ResultSet.add_columns()->CopyFrom(column);
            }

            HasMeta = true;
        }

        bool truncated = false;
        for (auto& row : record.GetResultSet().rows()) {
            truncated = truncated || (RowsLimit && (ui64)ResultSet.rows_size() >= RowsLimit);
            truncated = truncated || (ResultSet.ByteSizeLong() >= ResultSetBytesLimit);
            if (truncated) {
                break;
            }

            ResultSet.add_rows()->CopyFrom(row);
        }

        if (truncated) {
            ResultSet.set_truncated(true);
        }

        auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        resp->Record.SetEnough(truncated);
        resp->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        resp->Record.SetFreeSpace(ResultSetBytesLimit);
        ctx.Send(ev->Sender, resp.Release());
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamProfile::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Executions.push_back(std::move(*ev->Get()->Record.MutableProfile()));
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& kqpResponse = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received process error for scan query: " << kqpResponse.GetError());

        TBase::HandleError(kqpResponse.GetError(), ctx);
    }

    void Handle(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        const TString msg = ev->Get()->GetIssues().ToOneLineString();
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received abort execution event for scan query: " << msg);

        TBase::HandleError(msg, ctx);
    }

    using TBase::HandleResponse;

    void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& response = *ev->Get()->Record.GetRef().MutableResponse();

        NKikimr::ConvertYdbResultToKqpResult(ResultSet,*response.AddResults());
        for (auto& execStats : Executions) {
            response.MutableQueryStats()->AddExecutions()->Swap(&execStats);
        }
        Executions.clear();

        TBase::HandleResponse(ev, ctx);
    }

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvAbortExecution, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpScanQueryRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    ui64 RowsLimit = 0;
    TActorId ExecuterActorId;
    bool HasMeta = false;
    Ydb::ResultSet ResultSet;
    TVector<NYql::NDqProto::TDqExecutionStats> Executions;
};

// Handles data query request for StreamExecuteYqlScript
template<typename TRequest, typename TResponse, typename TResult>
class TKqpStreamRequestHandler : public TRequestHandlerBase<
    TKqpStreamRequestHandler<TRequest, TResponse, TResult>,
    TRequest,
    TResponse,
    TResult>
{
public:
    using TBase = typename TKqpStreamRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;

    TKqpStreamRequestHandler(TRequest* request, const TActorId& target, TPromise<TResult> promise,
            TCallbackFunc callback)
        : TBase(request, promise, callback)
        , TargetActorId(target) {}

    void Bootstrap(const TActorContext& ctx) {
        TActorId kqpProxy = MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, this->Request.Release());

        this->Become(&TKqpStreamRequestHandler::AwaitState);
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& kqpResponse = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, ctx.SelfID
            << "Received process error for kqp data query: " << kqpResponse.GetError());

        TBase::HandleError(kqpResponse.GetError(), ctx);
    }

    using TBase::Promise;
    using TBase::Callback;

    virtual void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& record = ev->Get()->Record.GetRef();
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            if (record.MutableResponse()->GetResults().size()) {
                // Send result sets to RPC actor TStreamExecuteYqlScriptRPC
                auto evStreamPart = MakeHolder<NKqp::TEvKqp::TEvDataQueryStreamPart>();
                ActorIdToProto(this->SelfId(), evStreamPart->Record.MutableGatewayActorId());

                for (int i = 0; i < record.MutableResponse()->MutableResults()->size(); ++i) {
                    // Workaround to avoid errors on Pull execution stage which would expect some results
                    Ydb::ResultSet resultSet;
                    NKikimr::ConvertYdbResultToKqpResult(resultSet, *evStreamPart->Record.AddResults());
                }

                evStreamPart->Record.MutableResults()->Swap(record.MutableResponse()->MutableResults());
                this->Send(TargetActorId, evStreamPart.Release());

                // Save response without data to send it later
                ResponseHandle = ev.Release();
            } else {
                // Response has no result sets. Forward to main pipeline
                Callback(Promise, std::move(*ev->Get()));
                this->Die(ctx);
            }
        } else {
            // Forward error to main pipeline
            Callback(Promise, std::move(*ev->Get()));
            this->Die(ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvDataQueryStreamPartAck::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Callback(Promise, std::move(*ResponseHandle->Get()));
        this->Die(ctx);
    }

    void Handle(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        const TString msg = ev->Get()->GetIssues().ToOneLineString();
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, this->SelfId()
            << "Received abort execution event for data query: " << msg);

        TBase::HandleError(msg, ctx);
    }

    using TBase::Handle;
    using TBase::HandleResponse;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(TResponse, HandleResponse);
            HFunc(NKqp::TEvKqp::TEvDataQueryStreamPartAck, Handle);
            HFunc(NKqp::TEvKqp::TEvAbortExecution, Handle);

        default:
            TBase::HandleUnexpectedEvent("TKqpStreamRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId TargetActorId;
    typename TResponse::TPtr ResponseHandle;
};

// Handles scan query request for StreamExecuteYqlScript
class TKqpScanQueryStreamRequestHandler : public TRequestHandlerBase<
    TKqpScanQueryStreamRequestHandler,
    NKqp::TEvKqp::TEvQueryRequest,
    NKqp::TEvKqp::TEvQueryResponse,
    IKqpGateway::TQueryResult>
{
public:
    using TRequest = NKqp::TEvKqp::TEvQueryRequest;
    using TResponse = NKqp::TEvKqp::TEvQueryResponse;
    using TResult = IKqpGateway::TQueryResult;

    using TBase = TKqpScanQueryStreamRequestHandler::TBase;

    TKqpScanQueryStreamRequestHandler(TRequest* request, const TActorId& target, TPromise<TResult> promise,
            TCallbackFunc callback)
        : TBase(request, promise, callback)
        , TargetActorId(target) {}

    void Bootstrap(const TActorContext& ctx) {
        ActorIdToProto(SelfId(), this->Request->Record.MutableRequestActorId());

        TActorId kqpProxy = MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, this->Request.Release());

        this->Become(&TKqpScanQueryStreamRequestHandler::AwaitState);
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        ExecuterActorId = ev->Sender;
        TlsActivationContext->Send(ev->Forward(TargetActorId));
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamDataAck::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        TlsActivationContext->Send(ev->Forward(ExecuterActorId));
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamProfile::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Executions.push_back(std::move(*ev->Get()->Record.MutableProfile()));
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& kqpResponse = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received process error for scan query: " << kqpResponse.GetError());

        TBase::HandleError(kqpResponse.GetError(), ctx);
    }

    void Handle(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        const TString msg = ev->Get()->GetIssues().ToOneLineString();
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received abort execution event for scan query: " << msg);

        TBase::HandleError(msg, ctx);
    }

    using TBase::HandleResponse;

    void HandleResponse(typename TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& response = *ev->Get()->Record.GetRef().MutableResponse();

        Ydb::ResultSet resultSet;
        NKikimr::ConvertYdbResultToKqpResult(resultSet, *response.AddResults());
        for (auto& execStats : Executions) {
            response.MutableQueryStats()->AddExecutions()->Swap(&execStats);
        }
        Executions.clear();

        TBase::HandleResponse(ev, ctx);
    }

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvProcessResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvAbortExecution, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpScanQueryStreamRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ExecuterActorId;
    TActorId TargetActorId;
    TVector<NYql::NDqProto::TDqExecutionStats> Executions;
};


class TKqpExecLiteralRequestHandler: public TActorBootstrapped<TKqpExecLiteralRequestHandler> {
public:
    using TResult = IKqpGateway::TExecPhysicalResult;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXEC_PHYSICAL_REQUEST_HANDLER;
    }

    TKqpExecLiteralRequestHandler(IKqpGateway::TExecPhysicalRequest&& request,
        TKqpRequestCounters::TPtr counters, TPromise<TResult> promise, TQueryData::TPtr params, ui32 txIndex)
        : Request(std::move(request))
        , TxIndex(txIndex)
        , Parameters(params)
        , Counters(counters)
        , Promise(promise)
    {}

    void Bootstrap() {
        auto result = ::NKikimr::NKqp::ExecuteLiteral(std::move(Request), Counters, SelfId());
        ProcessPureExecution(result);
        Become(&TThis::DieState);
        Send(SelfId(), new TEvents::TEvPoisonPill());
    }

private:

    STATEFN(DieState) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    void ProcessPureExecution(std::unique_ptr<TEvKqpExecuter::TEvTxResponse>& ev) {
        auto* response = ev->Record.MutableResponse();

        TResult result;
        if (response->GetStatus() == Ydb::StatusIds::SUCCESS) {
            result.SetSuccess();
        }
        for (auto& issue : response->GetIssues()) {
            result.AddIssue(NYql::IssueFromMessage(issue));
        }

        result.ExecuterResult.Swap(response->MutableResult());
        {
            auto g = Parameters->TypeEnv().BindAllocator();
            auto& txResults = ev->GetTxResults();
            result.Results.reserve(txResults.size());
            for(auto& tx : txResults) {
                result.Results.emplace_back(tx.GetMkql());
            }
            Parameters->AddTxHolders(std::move(ev->GetTxHolders()));

            if (!txResults.empty()) {
                Parameters->AddTxResults(TxIndex, std::move(txResults));
            }
        }
        Promise.SetValue(std::move(result));
        this->PassAway();
    }

private:
    IKqpGateway::TExecPhysicalRequest Request;
    const ui32 TxIndex;
    TQueryData::TPtr Parameters;
    TKqpRequestCounters::TPtr Counters;
    TPromise<TResult> Promise;
};


class TSchemeOpRequestHandler: public TRequestHandlerBase<
    TSchemeOpRequestHandler,
    TEvTxUserProxy::TEvProposeTransaction,
    TEvTxUserProxy::TEvProposeTransactionStatus,
    IKqpGateway::TGenericResult>
{
public:
    using TBase = typename TSchemeOpRequestHandler::TBase;
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;
    using TResponse = TEvTxUserProxy::TEvProposeTransactionStatus;
    using TResult = IKqpGateway::TGenericResult;

    TSchemeOpRequestHandler(TRequest* request, TPromise<TResult> promise, bool failedOnAlreadyExists)
        : TBase(request, promise, {})
        , FailedOnAlreadyExists(failedOnAlreadyExists)
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
                Y_VERIFY(pipeActor);
                ShemePipeActorId = ctx.ExecutorThread.RegisterActor(pipeActor);

                auto request = MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>();
                request->Record.SetTxId(response.GetTxId());
                NTabletPipe::SendData(ctx, ShemePipeActorId, request.Release());

                LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Sent TEvNotifyTxCompletion request"
                    << ", TxId: " << response.GetTxId());

                return;
            }

            case TEvTxUserProxy::TResultStatus::AccessDenied: {
                LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Access denied for scheme request"
                    << ", TxId: " << response.GetTxId());

                TIssue issue(NYql::TPosition(), "Access denied.");
                Promise.SetValue(ResultFromIssues<TResult>(TIssuesIds::KIKIMR_ACCESS_DENIED,
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

                    IKqpGateway::TGenericResult result;
                    result.SetSuccess();
                    Promise.SetValue(std::move(result));
                    this->Die(ctx);
                    return;
                }
                break;
            }

            case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable: {
                Promise.SetValue(ResultFromIssues<TResult>(TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                    "Schemeshard not available", {}));
                this->Die(ctx);
                return;
            }

            case TEvTxUserProxy::TResultStatus::ResolveError: {
                Promise.SetValue(ResultFromIssues<TResult>(TIssuesIds::KIKIMR_SCHEME_ERROR,
                    response.GetSchemeShardReason(), {}));
                this->Die(ctx);
                return;
            }

            case TEvTxUserProxy::TResultStatus::ExecError:
                switch (response.GetSchemeShardStatus()) {
                    case NKikimrScheme::EStatus::StatusMultipleModifications: {
                        Promise.SetValue(ResultFromIssues<TResult>(TIssuesIds::KIKIMR_MULTIPLE_SCHEME_MODIFICATIONS,
                            response.GetSchemeShardReason(), {}));
                        this->Die(ctx);
                        return;
                    }
                    case NKikimrScheme::EStatus::StatusPathDoesNotExist: {
                        Promise.SetValue(ResultFromIssues<TResult>(TIssuesIds::KIKIMR_SCHEME_ERROR,
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

        Promise.SetValue(ResultFromError<TResult>(TIssue({}, message)));
        this->Die(ctx);
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx) {
        auto& response = ev->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Received TEvNotifyTxCompletionResult for scheme request"
            << ", TxId: " << response.GetTxId());

        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, "Successful completion of scheme request"
            << ", TxId: " << response.GetTxId());

        IKqpGateway::TGenericResult result;
        result.SetSuccess();

        Promise.SetValue(std::move(result));
        NTabletPipe::CloseClient(ctx, ShemePipeActorId);
        this->Die(ctx);
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {}

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
        default:
            TBase::HandleUnexpectedEvent("TSchemeOpRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ShemePipeActorId;
    bool FailedOnAlreadyExists = false;
};

template<typename TResult>
TFuture<TResult> InvalidCluster(const TString& cluster) {
    return MakeFuture(ResultFromError<TResult>("Invalid cluster:" + cluster));
}

void KqpResponseToQueryResult(const NKikimrKqp::TEvQueryResponse& response, IKqpGateway::TQueryResult& queryResult) {
    auto& queryResponse = response.GetResponse();

    if (response.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
        queryResult.SetSuccess();
    }

    for (auto& issue : queryResponse.GetQueryIssues()) {
        queryResult.AddIssue(NYql::IssueFromMessage(issue));
    }

    for (auto& result : queryResponse.GetResults()) {
        auto arenaResult = google::protobuf::Arena::CreateMessage<NKikimrMiniKQL::TResult>(
            queryResult.ProtobufArenaPtr.get());

        arenaResult->CopyFrom(result);
        queryResult.Results.push_back(arenaResult);
    }

    queryResult.QueryAst = queryResponse.GetQueryAst();
    queryResult.QueryPlan = queryResponse.GetQueryPlan();
    queryResult.QueryStats = queryResponse.GetQueryStats();
}

namespace {
    struct TSendRoleWrapper : public TThrRefBase {
        using TMethod = std::function<void(TString&&, NYql::TAlterGroupSettings::EAction, std::vector<TString>&&)>;
        TMethod SendNextRole;
    };
}

class TKikimrIcGateway : public IKqpGateway {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    TKikimrIcGateway(const TString& cluster, const TString& database, std::shared_ptr<IKqpTableMetadataLoader>&& metadataLoader,
        TActorSystem* actorSystem, ui32 nodeId, TKqpRequestCounters::TPtr counters)
        : Cluster(cluster)
        , Database(database)
        , ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Counters(counters)
        , MetadataLoader(std::move(metadataLoader)) {}

    bool HasCluster(const TString& cluster) override {
        return cluster == Cluster;
    }

    TVector<TString> GetClusters() override {
        return {Cluster};
    }

    TString GetDefaultCluster() override {
        return Cluster;
    }

    TMaybe<TString> GetSetting(const TString& cluster, const TString& name) override {
        Y_UNUSED(cluster);
        Y_UNUSED(name);
        return {};
    }

    void SetToken(const TString& cluster, const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        YQL_ENSURE(cluster == Cluster);
        UserToken = token;
    }

    TVector<TString> GetCollectedSchemeData() override {
        return MetadataLoader->GetCollectedSchemeData();
    }

    TString GetTokenCompat() const {
        return UserToken ? UserToken->GetSerializedToken() : TString();
    }

    TFuture<TListPathResult> ListPath(const TString& cluster, const TString &path) override {
        using TRequest = TEvTxUserProxy::TEvNavigate;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TListPathResult>(cluster);
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& describePath = *ev->Record.MutableDescribePath();
            describePath.SetPath(CanonizePath(path));

            return SendProxyRequest<TRequest, TDescribeSchemeResponse, TListPathResult>(ev.Release(),
                [path] (TPromise<TListPathResult> promise, TDescribeSchemeResponse&& response) {
                    try {
                        promise.SetValue(GetListPathResult(
                            response.GetRecord().GetPathDescription(), path));
                    }
                    catch (yexception& e) {
                        promise.SetValue(ResultFromException<TListPathResult>(e));
                    }
                });
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TListPathResult>(e));
        }
    }

    static ui64 GetExpectedVersion(const std::pair<TIndexId, TString>& pathId) {
        return pathId.first.SchemaVersion;
    }

    static ui64 GetExpectedVersion(const TString&) {
        return 0;
    }

    TFuture<TTableMetadataResult> LoadTableMetadata(const TString& cluster, const TString& table,
        TLoadTableMetadataSettings settings) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TTableMetadataResult>(cluster);
            }

            return MetadataLoader->LoadTableMetadata(cluster, table, settings, Database, UserToken);

        } catch (yexception& e) {
            return MakeFuture(ResultFromException<TTableMetadataResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTable(NYql::TKikimrTableMetadataPtr metadata, bool createDir) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(metadata->Cluster)) {
                return InvalidCluster<TGenericResult>(metadata->Cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(metadata->Name, pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            using TConfigRequest = NConsole::TEvConfigsDispatcher::TEvGetConfigRequest;
            using TConfigResponse = NConsole::TEvConfigsDispatcher::TEvGetConfigResponse;

            ui32 configKind = (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem;
            auto ev = MakeHolder<TConfigRequest>(configKind);

            auto configsDispatcherId = NConsole::MakeConfigsDispatcherID(NodeId);
            auto configFuture = SendActorRequest<TConfigRequest, TConfigResponse, TAppConfigResult>(
                configsDispatcherId,
                ev.Release(),
                [](TPromise<TAppConfigResult> promise, TConfigResponse&& response) mutable {
                    TAppConfigResult result;
                    result.SetSuccess();
                    result.Config = response.Config;
                    promise.SetValue(result);
                });

            auto tablePromise = NewPromise<TGenericResult>();

            configFuture.Subscribe([this, metadata, tablePromise, pathPair]
                (const TFuture<TAppConfigResult>& future) mutable {
                    auto configResult = future.GetValue();
                    if (!configResult.Success()) {
                        tablePromise.SetValue(configResult);
                        return;
                    }

                    TTableProfiles profiles;
                    profiles.Load(configResult.Config->GetTableProfilesConfig());

                    auto ev = MakeHolder<TRequest>();
                    ev->Record.SetDatabaseName(Database);
                    if (UserToken) {
                        ev->Record.SetUserToken(UserToken->GetSerializedToken());
                    }
                    auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
                    schemeTx.SetWorkingDir(pathPair.first);
                    NKikimrSchemeOp::TTableDescription* tableDesc = nullptr;
                    if (!metadata->Indexes.empty()) {
                        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexedTable);
                        tableDesc = schemeTx.MutableCreateIndexedTable()->MutableTableDescription();
                        for (const auto& index : metadata->Indexes) {
                            auto indexDesc = schemeTx.MutableCreateIndexedTable()->AddIndexDescription();
                            indexDesc->SetName(index.Name);
                            switch (index.Type) {
                                case NYql::TIndexDescription::EType::GlobalSync:
                                    indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobal);
                                    break;
                                case NYql::TIndexDescription::EType::GlobalAsync:
                                    indexDesc->SetType(NKikimrSchemeOp::EIndexType::EIndexTypeGlobalAsync);
                                    break;
                            }

                            indexDesc->SetState(static_cast<::NKikimrSchemeOp::EIndexState>(index.State));
                            for (const auto& col : index.KeyColumns) {
                                indexDesc->AddKeyColumnNames(col);
                            }
                            for (const auto& col : index.DataColumns) {
                                indexDesc->AddDataColumnNames(col);
                            }
                        }
                        FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                    } else {
                        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateTable);
                        tableDesc = schemeTx.MutableCreateTable();
                        FillCreateTableColumnDesc(*tableDesc, pathPair.second, metadata);
                    }

                    Ydb::StatusIds::StatusCode code;
                    TString error;
                    TList<TString> warnings;

                    if (!FillCreateTableDesc(metadata, *tableDesc, profiles, code, error, warnings)) {
                        IKqpGateway::TGenericResult errResult;
                        errResult.AddIssue(NYql::TIssue(error));
                        errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                        tablePromise.SetValue(errResult);
                        return;
                    }

                    SendSchemeRequest(ev.Release()).Apply(
                        [tablePromise, warnings{std::move(warnings)}](const TFuture<TGenericResult>& future) mutable {
                            if (warnings.size()) {
                                auto result = future.GetValue();
                                for (const auto& warning : warnings) {
                                    result.AddIssue(
                                        NYql::TIssue(warning)
                                        .SetCode(NKikimrIssues::TIssuesIds::WARNING, NYql::TSeverityIds::S_WARNING)
                                    );
                                    tablePromise.SetValue(result);
                                }
                            } else {
                                tablePromise.SetValue(future.GetValue());
                            }
                        });
                });

            return tablePromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateColumnTable(NYql::TKikimrTableMetadataPtr metadata, bool createDir) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(metadata->Cluster)) {
                return InvalidCluster<TGenericResult>(metadata->Cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(metadata->Name, pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);

            Ydb::StatusIds::StatusCode code;
            TString error;

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnTable);
            NKikimrSchemeOp::TColumnTableDescription* tableDesc = schemeTx.MutableCreateColumnTable();

            tableDesc->SetName(pathPair.second);
            FillColumnTableSchema(*tableDesc->MutableSchema(), *metadata);

            if (!FillCreateColumnTableDesc(metadata, *tableDesc, code, error)) {
                IKqpGateway::TGenericResult errResult;
                errResult.AddIssue(NYql::TIssue(error));
                errResult.SetStatus(NYql::YqlStatusFromYdbStatus(code));
                return MakeFuture(std::move(errResult));
            }

            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTable(Ydb::Table::AlterTableRequest&& req, const TString& cluster) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            // FIXME: should be defined in grpc_services/rpc_calls.h, but cause cyclic dependency
            using namespace NGRpcService;
            using TEvAlterTableRequest = TGrpcRequestOperationCall<Ydb::Table::AlterTableRequest,
                Ydb::Table::AlterTableResponse>;

            return SendLocalRpcRequestNoResult<TEvAlterTableRequest>(std::move(req), Database, GetTokenCompat());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> RenameTable(const TString& src, const TString& dst, const TString& cluster) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpMoveTable);
            auto& op = *schemeTx.MutableMoveTable();
            op.SetSrcPath(src);
            op.SetDstPath(dst);

            auto movePromise = NewPromise<TGenericResult>();

            SendSchemeRequest(ev.Release()).Apply(
                [movePromise](const TFuture<TGenericResult>& future) mutable {
                        movePromise.SetValue(future.GetValue());
                });

            return movePromise.GetFuture();

        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTable(const TString& cluster, const TString& table) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            Ydb::Table::DropTableRequest dropTable;
            dropTable.set_path(table);

            // FIXME: should be defined in grpc_services/rpc_calls.h, but cause cyclic dependency
            using namespace NGRpcService;
            using TEvDropTableRequest = TGrpcRequestOperationCall<Ydb::Table::DropTableRequest,
                Ydb::Table::DropTableResponse>;

            return SendLocalRpcRequestNoResult<TEvDropTableRequest>(std::move(dropTable), Database, GetTokenCompat());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTopic(const TString& cluster, Ydb::Topic::CreateTopicRequest&& request) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            using namespace NGRpcService;
            return SendLocalRpcRequestNoResult<TEvRpcCreateTopicRequest>(std::move(request), Database, GetTokenCompat());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTopic(const TString& cluster, Ydb::Topic::AlterTopicRequest&& request) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            using namespace NGRpcService;
            return SendLocalRpcRequestNoResult<TEvRpcAlterTopicRequest>(std::move(request), Database, GetTokenCompat());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTopic(const TString& cluster, const TString& topic) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            Ydb::Topic::DropTopicRequest dropTopic;
            dropTopic.set_path(topic);

            using namespace NGRpcService;
            return SendLocalRpcRequestNoResult<TEvRpcDropTopicRequest>(std::move(dropTopic), Database, GetTokenCompat());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterColumnTable(const TString& cluster,
                                             const NYql::TAlterColumnTableSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.Table, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
            NKikimrSchemeOp::TAlterColumnTable* alter = schemeTx.MutableAlterColumnTable();
            alter->SetName(settings.Table);

            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateTableStore(const TString& cluster,
                                             const NYql::TCreateTableStoreSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.TableStore, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnStore);
            NKikimrSchemeOp::TColumnStoreDescription* storeDesc = schemeTx.MutableCreateColumnStore();
            storeDesc->SetName(pathPair.second);
            storeDesc->SetColumnShardCount(settings.ShardsCount);

            NKikimrSchemeOp::TColumnTableSchemaPreset* schemaPreset = storeDesc->AddSchemaPresets();
            schemaPreset->SetName("default");
            FillColumnTableSchema(*schemaPreset->MutableSchema(), settings);

            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterTableStore(const TString& cluster,
                                            const NYql::TAlterTableStoreSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.TableStore, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnStore);
            NKikimrSchemeOp::TAlterColumnStore* alter = schemeTx.MutableAlterColumnStore();
            alter->SetName(pathPair.second);

            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropTableStore(const TString& cluster,
                                           const NYql::TDropTableStoreSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.TableStore, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);

            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropColumnStore);
            NKikimrSchemeOp::TDrop* drop = schemeTx.MutableDrop();
            drop->SetName(pathPair.second);
            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateExternalTable(const TString& cluster,
                                                const NYql::TCreateExternalTableSettings& settings,
                                                bool createDir) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.ExternalTable, pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalTable);

            NKikimrSchemeOp::TExternalTableDescription& externalTableDesc = *schemeTx.MutableCreateExternalTable();
            FillCreateExternalTableColumnDesc(externalTableDesc, pathPair.second, settings);
            return SendSchemeRequest(ev.Release(), true);
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterExternalTable(const TString& cluster,
                                               const NYql::TAlterExternalTableSettings& settings) override {
        Y_UNUSED(cluster, settings);
        return MakeErrorFuture<TGenericResult>(std::make_exception_ptr(yexception() << "The alter is not supported for the external table"));
    }

    TFuture<TGenericResult> DropExternalTable(const TString& cluster,
                                              const NYql::TDropExternalTableSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.ExternalTable, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }

            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalTable);

            NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
            drop.SetName(pathPair.second);
            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateUser(const TString& cluster, const NYql::TCreateUserSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            auto createUserPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createUser = *schemeTx.MutableAlterLogin()->MutableCreateUser();

            createUser.SetUser(settings.UserName);
            if (settings.Password) {
                createUser.SetPassword(settings.Password);
            }

            SendSchemeRequest(ev.Release()).Apply(
                [createUserPromise](const TFuture<TGenericResult>& future) mutable {
                    createUserPromise.SetValue(future.GetValue());
                }
            );

            return createUserPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> CreateExternalDataSource(const TString& cluster,
                                                     const NYql::TCreateExternalDataSourceSettings& settings,
                                                     bool createDir) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.ExternalDataSource, pathPair, error, createDir)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);

            NKikimrSchemeOp::TExternalDataSourceDescription& dataSourceDesc = *schemeTx.MutableCreateExternalDataSource();
            FillCreateExternalDataSourceDesc(dataSourceDesc, pathPair.second, settings);
            return SendSchemeRequest(ev.Release(), true);
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterExternalDataSource(const TString& cluster,
                                                    const NYql::TAlterExternalDataSourceSettings& settings) override {
        Y_UNUSED(cluster, settings);
        return MakeErrorFuture<TGenericResult>(std::make_exception_ptr(yexception() << "The alter is not supported for the external data source"));
    }

    TFuture<TGenericResult> DropExternalDataSource(const TString& cluster,
                                                   const NYql::TDropExternalDataSourceSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            std::pair<TString, TString> pathPair;
            {
                TString error;
                if (!GetPathPair(settings.ExternalDataSource, pathPair, error, false)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }

            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(pathPair.first);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalDataSource);

            NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
            drop.SetName(pathPair.second);
            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterUser(const TString& cluster, const NYql::TAlterUserSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            auto alterUserPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& alterUser = *schemeTx.MutableAlterLogin()->MutableModifyUser();

            alterUser.SetUser(settings.UserName);
            if (settings.Password) {
                alterUser.SetPassword(settings.Password);
            }

            SendSchemeRequest(ev.Release()).Apply(
                [alterUserPromise](const TFuture<TGenericResult>& future) mutable {
                alterUserPromise.SetValue(future.GetValue());
            }
            );

            return alterUserPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropUser(const TString& cluster, const NYql::TDropUserSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            auto dropUserPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropUser = *schemeTx.MutableAlterLogin()->MutableRemoveUser();

            dropUser.SetUser(settings.UserName);

            SendSchemeRequest(ev.Release()).Apply(
                [dropUserPromise, &settings](const TFuture<TGenericResult>& future) mutable {
                    const auto& realResult = future.GetValue();
                    if (!realResult.Success() && realResult.Status() == TIssuesIds::DEFAULT_ERROR && settings.Force) {
                        IKqpGateway::TGenericResult fakeResult;
                        fakeResult.SetSuccess();
                        dropUserPromise.SetValue(std::move(fakeResult));
                    } else {
                        dropUserPromise.SetValue(realResult);
                    }
                }
            );

            return dropUserPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    template <class TSettings>
    class IObjectModifier {
    private:
        TKikimrIcGateway& Owner;
    protected:
        virtual TFuture<NMetadata::NModifications::TObjectOperatorResult> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const TSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) = 0;
        ui32 GetNodeId() const {
            return Owner.NodeId;
        }
        TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken() const {
            return Owner.UserToken;
        }
    public:
        IObjectModifier(TKikimrIcGateway& owner)
            : Owner(owner)
        {

        }
        TFuture<TGenericResult> Execute(const TString& cluster, const TSettings& settings) {
            try {
                if (!Owner.CheckCluster(cluster)) {
                    return InvalidCluster<TGenericResult>(cluster);
                }
                TString database;
                if (!Owner.GetDatabaseForLoginOperation(database)) {
                    return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
                }
                NMetadata::IClassBehaviour::TPtr cBehaviour(NMetadata::IClassBehaviour::TFactory::Construct(settings.GetTypeId()));
                if (!cBehaviour) {
                    return MakeFuture(ResultFromError<TGenericResult>("incorrect object type"));
                }
                if (!cBehaviour->GetOperationsManager()) {
                    return MakeFuture(ResultFromError<TGenericResult>("type has not manager for operations"));
                }
                NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
                if (GetUserToken()) {
                    context.SetUserToken(*GetUserToken());
                }
                return DoExecute(cBehaviour, settings, context).Apply([](const NThreading::TFuture<NMetadata::NModifications::TObjectOperatorResult>& f) {
                    if (f.HasValue() && !f.HasException() && f.GetValue().IsSuccess()) {
                        TGenericResult result;
                        result.SetSuccess();
                        return NThreading::MakeFuture<TGenericResult>(result);
                    } else {
                        TGenericResult result;
                        result.AddIssue(NYql::TIssue(f.GetValue().GetErrorMessage()));
                        return NThreading::MakeFuture<TGenericResult>(result);
                    }
                    });
            } catch (yexception& e) {
                return MakeFuture(ResultFromException<TGenericResult>(e));
            }
        }
    };

    class TObjectCreate: public IObjectModifier<NYql::TCreateObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TCreateObjectSettings>;
    protected:
        virtual TFuture<NMetadata::NModifications::TObjectOperatorResult> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TCreateObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override
        {
            return manager->GetOperationsManager()->CreateObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    class TObjectAlter: public IObjectModifier<NYql::TAlterObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TAlterObjectSettings>;
    protected:
        virtual TFuture<NMetadata::NModifications::TObjectOperatorResult> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TAlterObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override {
            return manager->GetOperationsManager()->AlterObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    class TObjectDrop: public IObjectModifier<NYql::TDropObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TDropObjectSettings>;
    protected:
        virtual TFuture<NMetadata::NModifications::TObjectOperatorResult> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TDropObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override {
            return manager->GetOperationsManager()->DropObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    TFuture<TGenericResult> CreateObject(const TString& cluster, const NYql::TCreateObjectSettings& settings) override {
        return TObjectCreate(*this).Execute(cluster, settings);
    }

    TFuture<TGenericResult> AlterObject(const TString& cluster, const NYql::TAlterObjectSettings& settings) override {
        return TObjectAlter(*this).Execute(cluster, settings);
    }

    TFuture<TGenericResult> DropObject(const TString& cluster, const NYql::TDropObjectSettings& settings) override {
        return TObjectDrop(*this).Execute(cluster, settings);
    }

    TFuture<TGenericResult> CreateGroup(const TString& cluster, const NYql::TCreateGroupSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            auto createGroupPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& createGroup = *schemeTx.MutableAlterLogin()->MutableCreateGroup();

            createGroup.SetGroup(settings.GroupName);

            SendSchemeRequest(ev.Release()).Apply(
                [createGroupPromise](const TFuture<TGenericResult>& future) mutable {
                    createGroupPromise.SetValue(future.GetValue());
                }
            );

            return createGroupPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> AlterGroup(const TString& cluster, NYql::TAlterGroupSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            if (!settings.Roles.size()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles given for AlterGroup request"));
            }

            TPromise<TGenericResult> alterGroupPromise = NewPromise<TGenericResult>();

            auto sendRoleWrapper = MakeIntrusive<TSendRoleWrapper>();

            sendRoleWrapper->SendNextRole = [alterGroupPromise, sendRoleWrapper, this, database = std::move(database)]
                (TString&& groupName, NYql::TAlterGroupSettings::EAction action, std::vector<TString>&& rolesToSend)
                mutable
            {
                auto ev = MakeHolder<TRequest>();
                ev->Record.SetDatabaseName(database);
                if (UserToken) {
                    ev->Record.SetUserToken(UserToken->GetSerializedToken());
                }
                auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
                schemeTx.SetWorkingDir(database);
                schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
                switch (action) {
                case NYql::TAlterGroupSettings::EAction::AddRoles:
                {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableAddGroupMembership();
                    alterGroup.SetGroup(groupName);
                    alterGroup.SetMember(*rolesToSend.begin());
                    break;
                }
                case NYql::TAlterGroupSettings::EAction::RemoveRoles:
                {
                    auto& alterGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroupMembership();
                    alterGroup.SetGroup(groupName);
                    alterGroup.SetMember(*rolesToSend.begin());
                    break;
                }
                default:
                    break;
                }

                std::vector<TString> restOfRoles(
                    std::make_move_iterator(rolesToSend.begin() + 1),
                    std::make_move_iterator(rolesToSend.end())
                );

                SendSchemeRequest(ev.Release()).Apply(
                    [alterGroupPromise, &sendRoleWrapper, groupName = std::move(groupName), action, restOfRoles = std::move(restOfRoles)]
                        (const TFuture<TGenericResult>& future) mutable
                    {
                        auto result = future.GetValue();
                        if (!result.Success()) {
                            alterGroupPromise.SetValue(result);
                            sendRoleWrapper.Reset();
                            return;
                        }
                        if (restOfRoles.size()) {
                            try {
                                sendRoleWrapper->SendNextRole(std::move(groupName), action, std::move(restOfRoles));
                            }
                            catch (yexception& e) {
                                sendRoleWrapper.Reset();
                                return alterGroupPromise.SetValue(ResultFromException<TGenericResult>(e));
                            }
                        } else {
                            sendRoleWrapper.Reset();
                            alterGroupPromise.SetValue(result);
                        }
                    }
                );
            };

            sendRoleWrapper->SendNextRole(std::move(settings.GroupName), settings.Action, std::move(settings.Roles));

            return alterGroupPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> DropGroup(const TString& cluster, const NYql::TDropGroupSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            auto dropGroupPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& dropGroup = *schemeTx.MutableAlterLogin()->MutableRemoveGroup();

            dropGroup.SetGroup(settings.GroupName);

            SendSchemeRequest(ev.Release()).Apply(
                [dropGroupPromise, &settings](const TFuture<TGenericResult>& future) mutable {
                    const auto& realResult = future.GetValue();
                    if (!realResult.Success() && realResult.Status() == TIssuesIds::DEFAULT_ERROR && settings.Force) {
                        IKqpGateway::TGenericResult fakeResult;
                        fakeResult.SetSuccess();
                        dropGroupPromise.SetValue(std::move(fakeResult));
                    } else {
                        dropGroupPromise.SetValue(realResult);
                    }
                }
            );

            return dropGroupPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TExecPhysicalResult> ExecuteLiteral(TExecPhysicalRequest&& request, TQueryData::TPtr params, ui32 txIndex) override {
        YQL_ENSURE(!request.Transactions.empty());
        YQL_ENSURE(request.DataShardLocks.empty());
        YQL_ENSURE(!request.NeedTxId);

        auto containOnlyLiteralStages = [](const auto& request) {
            for (const auto& tx : request.Transactions) {
                if (tx.Body->GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) {
                    return false;
                }

                for (const auto& stage : tx.Body->GetStages()) {
                    if (stage.InputsSize() != 0) {
                        return false;
                    }
                }
            }

            return true;
        };

        YQL_ENSURE(containOnlyLiteralStages(request));
        auto promise = NewPromise<TExecPhysicalResult>();
        IActor* requestHandler = new TKqpExecLiteralRequestHandler(std::move(request), Counters, promise, params, txIndex);
        RegisterActor(requestHandler);
        return promise.GetFuture();
    }

    TFuture<TQueryResult> ExecScanQueryAst(const TString& cluster, const TString& query,
        TQueryData::TPtr params, const TAstQuerySettings& settings, ui64 rowsLimit) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_SCAN);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetCollectStats(settings.CollectStats);

        FillParameters(params, *ev->Record.MutableRequest()->MutableParameters());

        return SendKqpScanQueryRequest(ev.Release(), rowsLimit,
            [] (TPromise<TQueryResult> promise, TResponse&& responseEv) {
                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
                KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
                promise.SetValue(std::move(queryResult));
            });
    }

    TFuture<TQueryResult> StreamExecDataQueryAst(const TString& cluster, const TString& query,
        TQueryData::TPtr params, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings, const NActors::TActorId& target) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_DML);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetCollectStats(settings.CollectStats);

        FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());

        auto& txControl = *ev->Record.MutableRequest()->MutableTxControl();
        txControl.mutable_begin_tx()->CopyFrom(txSettings);
        txControl.set_commit_tx(true);

        return SendKqpStreamRequest<TRequest, TResponse, TQueryResult>(ev.Release(), target,
            [](TPromise<TQueryResult> promise, TResponse&& responseEv) {
            TQueryResult queryResult;
            queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
            KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
            promise.SetValue(std::move(queryResult));
        });
    }

    TFuture<TQueryResult> StreamExecScanQueryAst(const TString& cluster, const TString& query,
        TQueryData::TPtr params, const TAstQuerySettings& settings, const NActors::TActorId& target,
        std::shared_ptr<NGRpcService::IRequestCtxMtSafe> ctx) override
    {
        YQL_ENSURE(cluster == Cluster);
        YQL_ENSURE(ctx);

        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto q = query;
        auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>(
            NKikimrKqp::QUERY_ACTION_EXECUTE,
            NKikimrKqp::QUERY_TYPE_AST_SCAN,
            target,
            ctx,
            TString(), //sessionId
            std::move(q),
            TString(), //queryId
            nullptr, //tx_control
            nullptr,
            settings.CollectStats,
            nullptr, // query_cache_policy
            nullptr
        );

        // TODO: Rewrite CollectParameters at kqp_host
        FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());

        return SendKqpScanQueryStreamRequest(ev.Release(), target,
            [](TPromise<TQueryResult> promise, TResponse&& responseEv) {
            TQueryResult queryResult;
            queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
            KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
            promise.SetValue(std::move(queryResult));
        });
    }

    TFuture<TQueryResult> ExplainScanQueryAst(const TString& cluster, const TString& query) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_SCAN);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);

        return SendKqpScanQueryRequest(ev.Release(), 100,
            [] (TPromise<TQueryResult> promise, TResponse&& responseEv) {
                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
                KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
                promise.SetValue(std::move(queryResult));
            });
    }

    TFuture<TQueryResult> ExecDataQueryAst(const TString& cluster, const TString& query, TQueryData::TPtr params,
        const TAstQuerySettings& settings, const Ydb::Table::TransactionSettings& txSettings) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_DML);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetCollectStats(settings.CollectStats);

        FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());

        auto& txControl = *ev->Record.MutableRequest()->MutableTxControl();
        txControl.mutable_begin_tx()->CopyFrom(txSettings);
        txControl.set_commit_tx(true);

        return SendKqpRequest<TRequest, TResponse, TQueryResult>(ev.Release(),
            [] (TPromise<TQueryResult> promise, TResponse&& responseEv) {
                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
                KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
                promise.SetValue(std::move(queryResult));
            });
    }

    TFuture<TQueryResult> ExplainDataQueryAst(const TString& cluster, const TString& query) override {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_DML);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);

        return SendKqpRequest<TRequest, TResponse, TQueryResult>(ev.Release(),
            [] (TPromise<TQueryResult> promise, TResponse&& responseEv) {
                auto& response = responseEv.Record.GetRef();
                auto& queryResponse = response.GetResponse();

                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());

                if (response.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                    queryResult.SetSuccess();
                }

                for (auto& issue : queryResponse.GetQueryIssues()) {
                    queryResult.AddIssue(NYql::IssueFromMessage(issue));
                }

                queryResult.QueryAst = queryResponse.GetQueryAst();
                queryResult.QueryPlan = queryResponse.GetQueryPlan();

                promise.SetValue(std::move(queryResult));
            });
    }

private:
    using TDescribeSchemeResponse = TEvSchemeShard::TEvDescribeSchemeResult;
    using TTransactionResponse = TEvTxUserProxy::TEvProposeTransactionStatus;

private:
    TActorId RegisterActor(IActor* actor) {
        return ActorSystem->Register(actor, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    }

    template<typename TRequest, typename TResponse, typename TResult>
    TFuture<TResult> SendProxyRequest(TRequest* request,
        typename TProxyRequestHandler<TRequest, TResponse, TResult>::TCallbackFunc callback)
    {
        auto promise = NewPromise<TResult>();
        IActor* requestHandler = new TProxyRequestHandler<TRequest, TResponse, TResult>(request,
            promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    template<typename TRequest, typename TResponse, typename TResult>
    TFuture<TResult> SendKqpRequest(TRequest* request,
        typename TKqpRequestHandler<TRequest, TResponse, TResult>::TCallbackFunc callback)
    {
        auto promise = NewPromise<TResult>();
        IActor* requestHandler = new TKqpRequestHandler<TRequest, TResponse, TResult>(request,
            promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    TFuture<TQueryResult> SendKqpScanQueryRequest(NKqp::TEvKqp::TEvQueryRequest* request, ui64 rowsLimit,
        TKqpScanQueryRequestHandler::TCallbackFunc callback)
    {
        auto promise = NewPromise<TQueryResult>();
        IActor* requestHandler = new TKqpScanQueryRequestHandler(request, rowsLimit, promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    template<typename TRequest, typename TResponse, typename TResult>
    TFuture<TResult> SendKqpStreamRequest(TRequest* request, const NActors::TActorId& target,
        typename TKqpStreamRequestHandler<TRequest, TResponse, TResult>::TCallbackFunc callback)
    {
        auto promise = NewPromise<TResult>();
        IActor* requestHandler = new TKqpStreamRequestHandler<TRequest, TResponse, TResult>(request,
            target, promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    TFuture<TQueryResult> SendKqpScanQueryStreamRequest(NKqp::TEvKqp::TEvQueryRequest* request,
        const NActors::TActorId& target, TKqpScanQueryStreamRequestHandler::TCallbackFunc callback)
    {
        auto promise = NewPromise<TQueryResult>();
        IActor* requestHandler = new TKqpScanQueryStreamRequestHandler(request, target, promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    template<typename TRequest, typename TResponse, typename TResult>
    TFuture<TResult> SendActorRequest(const TActorId& actorId, TRequest* request,
        typename TActorRequestHandler<TRequest, TResponse, TResult>::TCallbackFunc callback)
    {
        auto promise = NewPromise<TResult>();
        IActor* requestHandler = new TActorRequestHandler<TRequest, TResponse, TResult>(actorId, request,
            promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    TFuture<TGenericResult> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request, bool failedOnAlreadyExists = false)
    {
        auto promise = NewPromise<TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(request, promise, failedOnAlreadyExists);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    template<typename TRpc>
    TFuture<TGenericResult> SendLocalRpcRequestNoResult(typename TRpc::TRequest&& proto, const TString& databse, const TString& token) {
        return NRpcService::DoLocalRpc<TRpc>(std::move(proto), databse, token, ActorSystem).Apply([](NThreading::TFuture<typename TRpc::TResponse> future) {
            auto r = future.ExtractValue();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(r.operation().issues(), issues);

            if (r.operation().ready() != true) {
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, TStringBuilder()
                    << "Unexpected operation for \"sync\" mode"));

                const auto& yqlStatus = TIssuesIds::DEFAULT_ERROR;

                auto result = ResultFromIssues<TGenericResult>(yqlStatus, issues);
                return NThreading::MakeFuture(result);
            }

            const auto& yqlStatus = NYql::YqlStatusFromYdbStatus(r.operation().status());

            auto result = ResultFromIssues<TGenericResult>(yqlStatus, issues);
            return NThreading::MakeFuture(result);
        });
    }

    bool CheckCluster(const TString& cluster) {
        return cluster == Cluster;
    }

    bool GetDatabaseForLoginOperation(TString& database) {
        TAppData* appData = AppData(ActorSystem);
        if (appData && appData->AuthConfig.GetDomainLoginOnly()) {
            if (appData->DomainsInfo && !appData->DomainsInfo->Domains.empty()) {
                database = "/" + appData->DomainsInfo->Domains.begin()->second->Name;
                return true;
            }
        } else {
            database = Database;
            return true;
        }
        return false;
    }

    bool GetPathPair(const TString& tableName, std::pair<TString, TString>& pathPair, TString& error,
        bool createDir) {
        if (createDir) {
            return TrySplitPathByDb(tableName, Database, pathPair, error);
        } else {
            return TrySplitTablePath(tableName, pathPair, error);
        }
    }

private:
    static TListPathResult GetListPathResult(const TPathDescription& pathDesc, const TString& path) {
        if (pathDesc.GetSelf().GetPathType() != EPathTypeDir) {
            return ResultFromError<TListPathResult>(TString("Directory not found: ") + path);
        }

        TListPathResult result;
        result.SetSuccess();

        result.Path = path;
        for (auto entry : pathDesc.GetChildren()) {
            result.Items.push_back(NYql::TKikimrListPathItem(
                entry.GetName(),
                entry.GetPathType() == EPathTypeDir));
        }

        return result;
    }

    static void FillCreateTableColumnDesc(NKikimrSchemeOp::TTableDescription& tableDesc,
                                          const TString& name, NYql::TKikimrTableMetadataPtr metadata)
    {
        tableDesc.SetName(name);

        Y_ENSURE(metadata->ColumnOrder.size() == metadata->Columns.size());
        for (const auto& name : metadata->ColumnOrder) {
            auto columnIt = metadata->Columns.find(name);
            Y_ENSURE(columnIt != metadata->Columns.end());

            TColumnDescription& columnDesc = *tableDesc.AddColumns();
            columnDesc.SetName(columnIt->second.Name);
            columnDesc.SetType(columnIt->second.Type);
            columnDesc.SetNotNull(columnIt->second.NotNull);
            if (columnIt->second.Families) {
                columnDesc.SetFamilyName(*columnIt->second.Families.begin());
            }
        }

        for (TString& keyColumn : metadata->KeyColumnNames) {
            tableDesc.AddKeyColumnNames(keyColumn);
        }
    }

    template <typename T>
    static void FillColumnTableSchema(NKikimrSchemeOp::TColumnTableSchema& schema, const T& metadata)
    {
        Y_ENSURE(metadata.ColumnOrder.size() == metadata.Columns.size());
        for (const auto& name : metadata.ColumnOrder) {
            auto columnIt = metadata.Columns.find(name);
            Y_ENSURE(columnIt != metadata.Columns.end());

            TOlapColumnDescription& columnDesc = *schema.AddColumns();
            columnDesc.SetName(columnIt->second.Name);
            columnDesc.SetType(columnIt->second.Type);
            columnDesc.SetNotNull(columnIt->second.NotNull);
        }

        for (const auto& keyColumn : metadata.KeyColumnNames) {
            schema.AddKeyColumnNames(keyColumn);
        }

        schema.SetEngine(NKikimrSchemeOp::EColumnTableEngine::COLUMN_ENGINE_REPLACING_TIMESERIES);
    }

    static void FillCreateExternalTableColumnDesc(NKikimrSchemeOp::TExternalTableDescription& externalTableDesc,
                                                  const TString& name,
                                                  const NYql::TCreateExternalTableSettings& settings)
    {
        externalTableDesc.SetName(name);
        externalTableDesc.SetDataSourcePath(settings.DataSourcePath);
        externalTableDesc.SetLocation(settings.Location);
        externalTableDesc.SetSourceType("General");

        Y_ENSURE(settings.ColumnOrder.size() == settings.Columns.size());
        for (const auto& name : settings.ColumnOrder) {
            auto columnIt = settings.Columns.find(name);
            Y_ENSURE(columnIt != settings.Columns.end());

            TColumnDescription& columnDesc = *externalTableDesc.AddColumns();
            columnDesc.SetName(columnIt->second.Name);
            columnDesc.SetType(columnIt->second.Type);
            columnDesc.SetNotNull(columnIt->second.NotNull);
        }
        NKikimrExternalSources::TGeneral general;
        auto& attributes = *general.mutable_attributes();
        for (const auto& [key, value]: settings.SourceTypeParameters) {
            attributes.insert({key, value});
        }
        externalTableDesc.SetContent(general.SerializeAsString());
    }

    static void FillCreateExternalDataSourceDesc(NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc,
                                                 const TString& name,
                                                 const NYql::TCreateExternalDataSourceSettings& settings)
    {
        externaDataSourceDesc.SetName(name);
        externaDataSourceDesc.SetSourceType(settings.SourceType);
        externaDataSourceDesc.SetLocation(settings.Location);
        externaDataSourceDesc.SetInstallation(settings.Installation);

        if (settings.AuthMethod == "NONE") {
            externaDataSourceDesc.MutableAuth()->MutableNone();
        }
    }

    static void FillParameters(TQueryData::TPtr params, NKikimrMiniKQL::TParams& output) {
        if (!params) {
            return;
        }

        if (params->GetParams().empty()) {
            return;
        }

        output.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
        auto type = output.MutableType()->MutableStruct();
        auto value = output.MutableValue();
        for (auto& pair : params->GetParams()) {
            auto typeMember = type->AddMember();
            typeMember->SetName(pair.first);

            typeMember->MutableType()->CopyFrom(pair.second.GetType());
            value->AddStruct()->CopyFrom(pair.second.GetValue());
        }
    }

    static bool ConvertDataSlotToYdbTypedValue(NYql::EDataSlot fromType, const TString& fromValue, Ydb::Type* toType,
            Ydb::Value* toValue)
    {
        switch (fromType) {
        case NYql::EDataSlot::Bool:
            toType->set_type_id(Ydb::Type::BOOL);
            toValue->set_bool_value(FromString<bool>(fromValue));
            break;
        case NYql::EDataSlot::Int8:
            toType->set_type_id(Ydb::Type::INT8);
            toValue->set_int32_value(FromString<i32>(fromValue));
            break;
        case NYql::EDataSlot::Uint8:
            toType->set_type_id(Ydb::Type::UINT8);
            toValue->set_uint32_value(FromString<ui32>(fromValue));
            break;
        case NYql::EDataSlot::Int16:
            toType->set_type_id(Ydb::Type::INT16);
            toValue->set_int32_value(FromString<i32>(fromValue));
            break;
        case NYql::EDataSlot::Uint16:
            toType->set_type_id(Ydb::Type::UINT16);
            toValue->set_uint32_value(FromString<ui32>(fromValue));
            break;
        case NYql::EDataSlot::Int32:
            toType->set_type_id(Ydb::Type::INT32);
            toValue->set_int32_value(FromString<i32>(fromValue));
            break;
        case NYql::EDataSlot::Uint32:
            toType->set_type_id(Ydb::Type::UINT32);
            toValue->set_uint32_value(FromString<ui32>(fromValue));
            break;
        case NYql::EDataSlot::Int64:
            toType->set_type_id(Ydb::Type::INT64);
            toValue->set_int64_value(FromString<i64>(fromValue));
            break;
        case NYql::EDataSlot::Uint64:
            toType->set_type_id(Ydb::Type::UINT64);
            toValue->set_uint64_value(FromString<ui64>(fromValue));
            break;
        case NYql::EDataSlot::Float:
            toType->set_type_id(Ydb::Type::FLOAT);
            toValue->set_float_value(FromString<float>(fromValue));
            break;
        case NYql::EDataSlot::Double:
            toType->set_type_id(Ydb::Type::DOUBLE);
            toValue->set_double_value(FromString<double>(fromValue));
            break;
        case NYql::EDataSlot::Json:
            toType->set_type_id(Ydb::Type::JSON);
            toValue->set_text_value(fromValue);
            break;
        case NYql::EDataSlot::String:
            toType->set_type_id(Ydb::Type::STRING);
            toValue->set_bytes_value(fromValue);
            break;
        case NYql::EDataSlot::Utf8:
            toType->set_type_id(Ydb::Type::UTF8);
            toValue->set_text_value(fromValue);
            break;
        case NYql::EDataSlot::Date:
            toType->set_type_id(Ydb::Type::DATE);
            toValue->set_uint32_value(FromString<ui32>(fromValue));
            break;
        case NYql::EDataSlot::Datetime:
            toType->set_type_id(Ydb::Type::DATETIME);
            toValue->set_uint32_value(FromString<ui32>(fromValue));
            break;
        case NYql::EDataSlot::Timestamp:
            toType->set_type_id(Ydb::Type::TIMESTAMP);
            toValue->set_uint64_value(FromString<ui64>(fromValue));
            break;
        case NYql::EDataSlot::Interval:
            toType->set_type_id(Ydb::Type::INTERVAL);
            toValue->set_int64_value(FromString<i64>(fromValue));
            break;
        default:
            return false;
        }
        return true;
    }

    // Convert TKikimrTableMetadata struct to public API proto
    static bool ConvertCreateTableSettingsToProto(NYql::TKikimrTableMetadataPtr metadata,
            Ydb::Table::CreateTableRequest& proto, Ydb::StatusIds::StatusCode& code, TString& error)
    {
        for (const auto& family : metadata->ColumnFamilies) {
            auto* familyProto = proto.add_column_families();
            familyProto->set_name(family.Name);
            if (family.Data) {
                familyProto->mutable_data()->set_media(family.Data.GetRef());
            }
            if (family.Compression) {
                if (to_lower(family.Compression.GetRef()) == "off") {
                    familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_NONE);
                } else if (to_lower(family.Compression.GetRef()) == "lz4") {
                    familyProto->set_compression(Ydb::Table::ColumnFamily::COMPRESSION_LZ4);
                } else {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = TStringBuilder() << "Unknown compression '" << family.Compression.GetRef() << "' for a column family";
                    return false;
                }
            }
        }

        if (metadata->TableSettings.CompactionPolicy) {
            proto.set_compaction_policy(metadata->TableSettings.CompactionPolicy.GetRef());
        }

        if (metadata->TableSettings.PartitionBy) {
            if (metadata->TableSettings.PartitionBy.size() > metadata->KeyColumnNames.size()) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "\"Partition by\" contains more columns than primary key does";
                return false;
            } else if (metadata->TableSettings.PartitionBy.size() == metadata->KeyColumnNames.size()) {
                for (size_t i = 0; i < metadata->TableSettings.PartitionBy.size(); ++i) {
                    if (metadata->TableSettings.PartitionBy[i] != metadata->KeyColumnNames[i]) {
                        code = Ydb::StatusIds::BAD_REQUEST;
                        error = "\"Partition by\" doesn't match primary key";
                        return false;
                    }
                }
            } else {
                code = Ydb::StatusIds::UNSUPPORTED;
                error = "\"Partition by\" is not supported yet";
                return false;
            }
        }

        if (metadata->TableSettings.AutoPartitioningBySize) {
            auto& partitioningSettings = *proto.mutable_partitioning_settings();
            TString value = to_lower(metadata->TableSettings.AutoPartitioningBySize.GetRef());
            if (value == "enabled") {
                partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::ENABLED);
            } else if (value == "disabled") {
                partitioningSettings.set_partitioning_by_size(Ydb::FeatureFlag::DISABLED);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown feature flag '"
                    << metadata->TableSettings.AutoPartitioningBySize.GetRef()
                    << "' for auto partitioning by size";
                return false;
            }
        }

        if (metadata->TableSettings.PartitionSizeMb) {
            auto& partitioningSettings = *proto.mutable_partitioning_settings();
            partitioningSettings.set_partition_size_mb(metadata->TableSettings.PartitionSizeMb.GetRef());
        }

        if (metadata->TableSettings.AutoPartitioningByLoad) {
            auto& partitioningSettings = *proto.mutable_partitioning_settings();
            TString value = to_lower(metadata->TableSettings.AutoPartitioningByLoad.GetRef());
            if (value == "enabled") {
                partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::ENABLED);
            } else if (value == "disabled") {
                partitioningSettings.set_partitioning_by_load(Ydb::FeatureFlag::DISABLED);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown feature flag '"
                    << metadata->TableSettings.AutoPartitioningByLoad.GetRef()
                    << "' for auto partitioning by load";
                return false;
            }
        }

        if (metadata->TableSettings.MinPartitions) {
            auto& partitioningSettings = *proto.mutable_partitioning_settings();
            partitioningSettings.set_min_partitions_count(metadata->TableSettings.MinPartitions.GetRef());
        }

        if (metadata->TableSettings.MaxPartitions) {
            auto& partitioningSettings = *proto.mutable_partitioning_settings();
            partitioningSettings.set_max_partitions_count(metadata->TableSettings.MaxPartitions.GetRef());
        }

        if (metadata->TableSettings.UniformPartitions) {
            if (metadata->TableSettings.PartitionAtKeys) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Uniform partitions and partitions at keys settings are mutually exclusive."
                    << " Use either one of them.";
                return false;
            }
            proto.set_uniform_partitions(metadata->TableSettings.UniformPartitions.GetRef());
        }

        if (metadata->TableSettings.PartitionAtKeys) {
            auto* borders = proto.mutable_partition_at_keys();
            for (const auto& splitPoint : metadata->TableSettings.PartitionAtKeys) {
                auto* border = borders->Addsplit_points();
                auto &keyType = *border->mutable_type()->mutable_tuple_type();
                for (const auto& key : splitPoint) {
                    auto* type = keyType.add_elements()->mutable_optional_type()->mutable_item();
                    auto* value = border->mutable_value()->add_items();
                    if (!ConvertDataSlotToYdbTypedValue(key.first, key.second, type, value)) {
                        code = Ydb::StatusIds::BAD_REQUEST;
                        error = TStringBuilder() << "Unsupported type for PartitionAtKeys: '"
                            << key.first << "'";
                        return false;
                    }
                }
            }
        }

        if (metadata->TableSettings.KeyBloomFilter) {
            TString value = to_lower(metadata->TableSettings.KeyBloomFilter.GetRef());
            if (value == "enabled") {
                proto.set_key_bloom_filter(Ydb::FeatureFlag::ENABLED);
            } else if (value == "disabled") {
                proto.set_key_bloom_filter(Ydb::FeatureFlag::DISABLED);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown feature flag '"
                    << metadata->TableSettings.KeyBloomFilter.GetRef()
                    << "' for key bloom filter";
                return false;
            }
        }

        if (metadata->TableSettings.ReadReplicasSettings) {
            if (!NYql::ConvertReadReplicasSettingsToProto(metadata->TableSettings.ReadReplicasSettings.GetRef(),
                    *proto.mutable_read_replicas_settings(), code, error)) {
                return false;
            }
        }

        if (const auto& ttl = metadata->TableSettings.TtlSettings) {
            if (ttl.IsSet()) {
                ConvertTtlSettingsToProto(ttl.GetValueSet(), *proto.mutable_ttl_settings());
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "Can't reset TTL settings";
                return false;
            }
        }

        if (const auto& tiering = metadata->TableSettings.Tiering) {
            if (tiering.IsSet()) {
                proto.set_tiering(tiering.GetValueSet());
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "Can't reset TIERING";
                return false;
            }
        }

        return true;
    }

    static bool FillCreateTableDesc(NYql::TKikimrTableMetadataPtr metadata,
        NKikimrSchemeOp::TTableDescription& tableDesc, const TTableProfiles& profiles,
        Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings)
    {
        Ydb::Table::CreateTableRequest createTableProto;
        if (!profiles.ApplyTableProfile(*createTableProto.mutable_profile(), tableDesc, code, error)
            || !ConvertCreateTableSettingsToProto(metadata, createTableProto, code, error)) {
            return false;
        }

        TColumnFamilyManager families(tableDesc.MutablePartitionConfig());

        for (const auto& familySettings : createTableProto.column_families()) {
            if (!families.ApplyFamilySettings(familySettings, &code, &error)) {
                return false;
            }
        }

        if (families.Modified && !families.ValidateColumnFamilies(&code, &error)) {
            return false;
        }

        if (!NGRpcService::FillCreateTableSettingsDesc(tableDesc, createTableProto, profiles, code, error, warnings)) {
            return false;
        }
        return true;
    }

    static bool FillCreateColumnTableDesc(NYql::TKikimrTableMetadataPtr metadata,
        NKikimrSchemeOp::TColumnTableDescription& tableDesc, Ydb::StatusIds::StatusCode& code, TString& error)
    {
        if (metadata->Columns.empty()) {
            tableDesc.SetSchemaPresetName("default");
        }

        auto& hashSharding = *tableDesc.MutableSharding()->MutableHashSharding();

        for (const TString& column : metadata->TableSettings.PartitionBy) {
            if (!metadata->Columns.count(column)) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown column '" << column << "' in partition by key";
                return false;
            }

            hashSharding.AddColumns(column);
        }

        if (metadata->TableSettings.PartitionByHashFunction) {
            if (to_lower(metadata->TableSettings.PartitionByHashFunction.GetRef()) == "cloud_logs") {
                hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
            } else {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown hash function '"
                    << metadata->TableSettings.PartitionByHashFunction.GetRef() << "' to partition by";
                return false;
            }
        } else {
            hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_MODULO_N);
        }

        if (metadata->TableSettings.MinPartitions) {
            tableDesc.SetColumnShardCount(*metadata->TableSettings.MinPartitions);
        }

        return true;
    }

private:
    TString Cluster;
    TString Database;
    TActorSystem* ActorSystem;
    ui32 NodeId;
    TKqpRequestCounters::TPtr Counters;
    TAlignedPagePoolCounters AllocCounters;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::shared_ptr<IKqpTableMetadataLoader> MetadataLoader;
};

} // namespace

TIntrusivePtr<IKqpGateway> CreateKikimrIcGateway(const TString& cluster, const TString& database,
    std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader>&& metadataLoader, TActorSystem* actorSystem,
    ui32 nodeId, TKqpRequestCounters::TPtr counters)
{
    return MakeIntrusive<TKikimrIcGateway>(cluster, database, std::move(metadataLoader), actorSystem, nodeId,
        counters);
}

} // namespace NKqp
} // namespace NKikimr
