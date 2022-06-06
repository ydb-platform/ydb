#include "kqp_ic_gateway_actors.h"
#include "kqp_impl.h"
#include "kqp_metadata_loader.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/executer/kqp_executer.h>
#include <ydb/core/kqp/rm/kqp_snapshot_manager.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/grpc_services/table_profiles.h>
#include <ydb/core/grpc_services/table_settings.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/yql/minikql/mkql_node_serialization.h>
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

using NKikimrTxUserProxy::TMiniKQLTransaction;

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
            TBase::HandleUnexpectedEvent("TProxyRequestHandler", ev->GetTypeRewrite(), ctx);
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
            TBase::HandleUnexpectedEvent("TKqpRequestHandler", ev->GetTypeRewrite(), ctx);
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

    void Handle(NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev, const TActorContext& ctx) {
        ExecuterActorId = ActorIdFromProto(ev->Get()->Record.GetExecuterActorId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received executer progress for scan query, id: " << ExecuterActorId);
    }

    void Handle(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& kqpResponse = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received process error for scan query: " << kqpResponse.GetError());

        TBase::HandleError(kqpResponse.GetError(), ctx);
    }

    void Handle(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received abort execution event for scan query: " << record.GetMessage());

        TBase::HandleError(record.GetMessage(), ctx);
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
            HFunc(NKqp::TEvKqpExecuter::TEvExecuterProgress, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpScanQueryRequestHandler", ev->GetTypeRewrite(), ctx);
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
        auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, this->SelfId()
            << "Received abort execution event for data query: " << record.GetMessage());

        TBase::HandleError(record.GetMessage(), ctx);
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
            TBase::HandleUnexpectedEvent("TKqpStreamRequestHandler", ev->GetTypeRewrite(), ctx);
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
        TlsActivationContext->Send(ev->Forward(TargetActorId));
    }

    void Handle(NKqp::TEvKqpExecuter::TEvStreamDataAck::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ctx);
        TlsActivationContext->Send(ev->Forward(ExecuterActorId));
    }

    void Handle(NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev, const TActorContext& ctx) {
        ExecuterActorId = ActorIdFromProto(ev->Get()->Record.GetExecuterActorId());
        ActorIdToProto(SelfId(), ev->Get()->Record.MutableExecuterActorId());
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received executer progress for scan query, id: " << ExecuterActorId);
        TlsActivationContext->Send(ev->Forward(TargetActorId));
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
        auto& record = ev->Get()->Record;
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received abort execution event for scan query: " << record.GetMessage());

        TBase::HandleError(record.GetMessage(), ctx);
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
            HFunc(NKqp::TEvKqpExecuter::TEvExecuterProgress, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpScanQueryStreamRequestHandler", ev->GetTypeRewrite(), ctx);
        }
    }

private:
    TActorId ExecuterActorId;
    TActorId TargetActorId;
    TVector<NYql::NDqProto::TDqExecutionStats> Executions;
};

class TMkqlRequestHandler : public TRequestHandlerBase<
    TMkqlRequestHandler,
    TEvTxUserProxy::TEvProposeTransaction,
    TEvTxUserProxy::TEvProposeTransactionStatus,
    IKqpGateway::TMkqlResult>
{
public:
    using TBase = typename TMkqlRequestHandler::TBase;
    using TCallbackFunc = typename TBase::TCallbackFunc;
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;
    using TResponse = TEvTxUserProxy::TEvProposeTransactionStatus;
    using TResult = IKqpGateway::TMkqlResult;

    TMkqlRequestHandler(const TAlignedPagePoolCounters& allocCounters, TRequest* request,
        TKqpParamsMap&& paramsMap, TPromise<TResult> promise, TCallbackFunc callback,
        const TActorId& miniKqlComplileServiceActorId)
        : TBase(request, promise, callback)
        , ParamsMap(std::move(paramsMap))
        , CompilationPending(false)
        , CompilationRetried(false)
        , AllocCounters(allocCounters)
        , MiniKqlComplileServiceActorId(miniKqlComplileServiceActorId)
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto& mkqlTx = *Request->Record.MutableTransaction()->MutableMiniKQLTransaction();

        if (mkqlTx.HasProgram() && mkqlTx.GetProgram().HasText()) {
            MkqlProgramText = mkqlTx.GetProgram().GetText();
            CompileProgram(ctx, mkqlTx.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE);
        }

        mkqlTx.SetCollectStats(true);
        YQL_ENSURE(!mkqlTx.HasParams());

        if (!ParamsMap.Values.empty()) {
            try {
                mkqlTx.MutableParams()->SetBin(BuildParams(ParamsMap, AllocCounters, ctx));
            }
            catch(const yexception& e) {
                Promise.SetValue(ResultFromError<TResult>(e.what()));
                this->Die(ctx);
                return;
            }
        }

        ProceedWithExecution(ctx);

        this->Become(&TMkqlRequestHandler::AwaitState);
    }

    void Handle(TMiniKQLCompileServiceEvents::TEvCompileStatus::TPtr &ev, const TActorContext &ctx) {
        const auto& result = ev->Get()->Result;
        auto& mkqlTx = *Request->Record.MutableTransaction()->MutableMiniKQLTransaction();

        if (!result.Errors.Empty()) {
            Promise.SetValue(ResultFromIssues<TResult>(GetMkqlCompileStatus(result.Errors),
                "Query compilation failed", result.Errors));

            this->Die(ctx);
            return;
        }

        if (mkqlTx.GetMode() == NKikimrTxUserProxy::TMiniKQLTransaction::COMPILE) {
            TResult reply;
            reply.SetSuccess();
            reply.CompiledProgram = result.CompiledProgram;
            Promise.SetValue(std::move(reply));
            this->Die(ctx);
            return;
        }

        auto& pgm = *mkqlTx.MutableProgram();
        pgm.ClearText();
        pgm.SetBin(result.CompiledProgram);

        CompileResolveCookies = std::move(ev->Get()->CompileResolveCookies);

        CompilationPending = false;
        ProceedWithExecution(ctx);
    }

    void HandleResponse(TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& response = *ev->Get();
        ui32 status = response.Record.GetStatus();

        bool resolveError = status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError ||
            status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable;

        if (resolveError && !CompilationRetried && !MkqlProgramText.empty()) {
            CompilationRetried = true;
            CompileProgram(ctx, false);
            ProceedWithExecution(ctx);
            return;
        }

        Callback(Promise, std::move(*ev->Get()));
        this->Die(ctx);
    }

    using TBase::Handle;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TResponse, HandleResponse);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TMiniKQLCompileServiceEvents::TEvCompileStatus, Handle);
        default:
            TBase::HandleUnexpectedEvent("TMkqlRequestHandler", ev->GetTypeRewrite(), ctx);
        }
    }

private:
    static TString BuildParams(const TKqpParamsMap& paramsMap, const TAlignedPagePoolCounters& counters,
        const TActorContext &ctx)
    {
        NMiniKQL::TScopedAlloc alloc(counters, AppData(ctx)->FunctionRegistry->SupportsSizedAllocators());
        NMiniKQL::TTypeEnvironment env(alloc);

        NMiniKQL::TStructLiteralBuilder structBuilder(env);
        structBuilder.Reserve(paramsMap.Values.size());
        for (auto& pair : paramsMap.Values) {
            const TString& name = pair.first;
            const NYql::NDq::TMkqlValueRef& param = pair.second;

            auto valueNode = NMiniKQL::ImportValueFromProto(param.GetType(), param.GetValue(), env);
            structBuilder.Add(name, valueNode);
        }

        auto node = NMiniKQL::TRuntimeNode(structBuilder.Build(), true);
        return NMiniKQL::SerializeRuntimeNode(node, env);
    }

    NYql::EYqlIssueCode GetMkqlCompileStatus(const NYql::TIssues& issues) {
        for (auto& issue : issues) {
            switch (issue.GetCode()) {
                case NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR:
                    return TIssuesIds::KIKIMR_SCHEME_MISMATCH;

                case NKikimrIssues::TIssuesIds::RESOLVE_LOOKUP_ERROR:
                    return TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
            }
        }

        return TIssuesIds::DEFAULT_ERROR;
    }

    void CompileProgram(const TActorContext &ctx, bool forceRefresh) {
        auto compileEv = MakeHolder<TMiniKQLCompileServiceEvents::TEvCompile>(MkqlProgramText);
        compileEv->ForceRefresh = forceRefresh;
        if (!CompileResolveCookies.empty()) {
            compileEv->CompileResolveCookies = std::move(CompileResolveCookies);
        }
        ctx.Send(MiniKqlComplileServiceActorId, compileEv.Release());
        CompilationPending = true;
    }

    void ProceedWithExecution(const TActorContext& ctx) {
        if (!CompilationPending) {
            TAutoPtr<TRequest> ev = new TRequest();
            ev->Record.CopyFrom(this->Request->Record);

            TActorId txproxy = MakeTxProxyID();
            ctx.Send(txproxy, ev.Release());
        }
    }

private:
    TKqpParamsMap ParamsMap;
    bool CompilationPending;
    bool CompilationRetried;
    TString MkqlProgramText;
    THashMap<TString, ui64> CompileResolveCookies;
    TAlignedPagePoolCounters AllocCounters;
    TActorId MiniKqlComplileServiceActorId;
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

    TSchemeOpRequestHandler(TRequest* request, TPromise<TResult> promise)
        : TBase(request, promise, {}) {}

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
                    response.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists)
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

            case TEvTxUserProxy::TResultStatus::ExecError: {
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
            }

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
            TBase::HandleUnexpectedEvent("TSchemeOpRequestHandler", ev->GetTypeRewrite(), ctx);
        }
    }

private:
    TActorId ShemePipeActorId;
};

class TKqpExecPhysicalRequestHandler: public TActorBootstrapped<TKqpExecPhysicalRequestHandler> {
public:
    using TRequest = TEvTxUserProxy::TEvProposeKqpTransaction;
    using TResult = IKqpGateway::TExecPhysicalResult;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXEC_PHYSICAL_REQUEST_HANDLER;
    }

    TKqpExecPhysicalRequestHandler(TRequest* request, bool streaming, const TActorId& target, TPromise<TResult> promise, bool needTxId)
        : Request(request)
        , Streaming(streaming)
        , Executer(request->ExecuterId)
        , Target(target)
        , Promise(promise)
        , NeedTxId(needTxId) {}

    void Bootstrap(const TActorContext& ctx) {
        if (NeedTxId) {
            ctx.Send(MakeTxProxyID(), this->Request.Release());
        } else {
            auto executerEv = MakeHolder<NKqp::TEvKqpExecuter::TEvTxRequest>();
            ActorIdToProto(ctx.SelfID, executerEv->Record.MutableTarget());
            executerEv->Record.MutableRequest()->SetTxId(0);
            ctx.Send(Request->ExecuterId, executerEv.Release());
        }

        Become(&TKqpExecPhysicalRequestHandler::ProcessState);
    }

private:
    void Handle(TEvKqpExecuter::TEvStreamData::TPtr &ev, const TActorContext &ctx) {
        if (Streaming) {
            TlsActivationContext->Send(ev->Forward(Target));
        } else {
            HandleUnexpectedEvent(ev->GetTypeRewrite(), ctx);
        }
    }

    void Handle(TEvKqpExecuter::TEvStreamDataAck::TPtr &ev, const TActorContext &ctx) {
        if (Streaming) {
            TlsActivationContext->Send(ev->Forward(Executer));
        } else {
            HandleUnexpectedEvent(ev->GetTypeRewrite(), ctx);
        }
    }

    void Handle(TEvKqpExecuter::TEvTxResponse::TPtr &ev, const TActorContext &) {
        auto* response = ev->Get()->Record.MutableResponse();

        TResult result;
        if (response->GetStatus() == Ydb::StatusIds::SUCCESS) {
            result.SetSuccess();
        }
        for (auto& issue : response->GetIssues()) {
            result.AddIssue(NYql::IssueFromMessage(issue));
        }

        result.ExecuterResult.Swap(response->MutableResult());

        if (Target && result.ExecuterResult.HasStats()) {
            auto statsEv = MakeHolder<TEvKqpExecuter::TEvStreamProfile>();
            auto& record = statsEv->Record;

            record.MutableProfile()->Swap(result.ExecuterResult.MutableStats());
            this->Send(Target, statsEv.Release());
        }

        Promise.SetValue(std::move(result));
        this->PassAway();
    }

    void Handle(TEvKqpExecuter::TEvExecuterProgress::TPtr& ev, const TActorContext&) {
        this->Send(Target, ev->Release().Release());
    }

    void Handle(TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        auto& msg = ev->Get()->Record;

        LOG_ERROR_S(ctx, NKikimrServices::KQP_GATEWAY,
            "TKqpExecPhysicalRequestHandler, got EvAbortExecution event."
             << " Code: " << Ydb::StatusIds_StatusCode_Name(msg.GetStatusCode())
             << ", reason: " << msg.GetMessage());

        auto issueCode = NYql::YqlStatusFromYdbStatus(msg.GetStatusCode());
        Promise.SetValue(ResultFromError<TResult>(YqlIssue({}, issueCode, msg.GetMessage())));

        this->PassAway();
    }

    void HandleUnexpectedEvent(ui32 eventType, const TActorContext &ctx) {
        LOG_CRIT_S(ctx, NKikimrServices::KQP_GATEWAY,
            "TKqpExecPhysicalRequestHandler, unexpected event, type: " << eventType);

        Promise.SetValue(ResultFromError<TResult>(YqlIssue({}, TIssuesIds::UNEXPECTED, TStringBuilder()
            << "TKqpExecPhysicalRequestHandler, unexpected event, type: " << eventType)));

        this->PassAway();
    }

    STFUNC(ProcessState) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvKqpExecuter::TEvStreamData, Handle);
            HFunc(TEvKqpExecuter::TEvStreamDataAck, Handle);
            HFunc(TEvKqpExecuter::TEvTxResponse, Handle);
            HFunc(TEvKqpExecuter::TEvExecuterProgress, Handle);
            HFunc(TEvKqp::TEvAbortExecution, Handle);
        default:
            HandleUnexpectedEvent(ev->GetTypeRewrite(), ctx);
        }
    }

private:
    THolder<TRequest> Request;
    bool Streaming;
    TActorId Executer;
    TActorId Target;
    TPromise<TResult> Promise;
    bool NeedTxId;
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
    struct TUserTokenData {
        TString Serialized;
        NACLib::TUserToken Data;

        TUserTokenData(const TString& serialized)
            : Serialized(serialized)
            , Data(serialized) {}
    };
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    TKikimrIcGateway(const TString& cluster, const TString& database, std::shared_ptr<IKqpTableMetadataLoader>&& metadataLoader,
        TActorSystem* actorSystem, ui32 nodeId, TKqpRequestCounters::TPtr counters, const TActorId& mkqlComplileService)
        : Cluster(cluster)
        , Database(database)
        , ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Counters(counters)
        , MetadataLoader(std::move(metadataLoader))
        , MkqlComplileService(mkqlComplileService)
    {}

    bool HasCluster(const TString& cluster) override {
        return cluster == Cluster;
    }

    TVector<TString> GetClusters() override {
        return {Cluster};
    }

    TString GetDefaultCluster() override {
        return Cluster;
    }

    TMaybe<NYql::TKikimrClusterConfig> GetClusterConfig(const TString& cluster) override {
        Y_UNUSED(cluster);
        return {};
    }

    TMaybe<TString> GetSetting(const TString& cluster, const TString& name) override {
        Y_UNUSED(cluster);
        Y_UNUSED(name);
        return {};
    }

    void SetToken(const TString& cluster, const TString& token) override {
        YQL_ENSURE(cluster == Cluster);
        if (!token.empty()) {
            UserToken = TUserTokenData(token);
        }
    }

    TVector<TString> GetCollectedSchemeData() override {
        return MetadataLoader->GetCollectedSchemeData();
    }

    TString GetTokenCompat() const {
        return UserToken ? UserToken->Serialized : TString();
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
                ev->Record.SetUserToken(UserToken->Serialized);
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
            if (UserToken) {
                return MetadataLoader->LoadTableMetadata(cluster, table, settings, Database, UserToken->Data);
            } else {
                return MetadataLoader->LoadTableMetadata(cluster, table, settings, Database, Nothing());
            }

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
            if (createDir) {
                TString error;
                if (!TrySplitPathByDb(metadata->Name, Database, pathPair, error)) {
                    return MakeFuture(ResultFromError<TGenericResult>(error));
                }
            } else {
                TString error;
                if (!TrySplitTablePath(metadata->Name, pathPair, error)) {
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

                    NGRpcService::TTableProfiles profiles;
                    profiles.Load(configResult.Config->GetTableProfilesConfig());

                    auto ev = MakeHolder<TRequest>();
                    ev->Record.SetDatabaseName(Database);
                    if (UserToken) {
                        ev->Record.SetUserToken(UserToken->Serialized);
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

    TFuture<TGenericResult> AlterTable(Ydb::Table::AlterTableRequest&& req, const TString& cluster) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            // FIXME: should be defined in grpc_services/rpc_calls.h, but cause cyclic dependency
            using namespace NGRpcService;
            using TEvAlterTableRequest = TGRpcRequestValidationWrapper<TRpcServices::EvAlterTable, Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse, true, TRateLimiterMode::Rps>;

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
                ev->Record.SetUserToken(UserToken->Serialized);
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
            using TEvDropTableRequest = TGRpcRequestWrapper<TRpcServices::EvDropTable, Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse, true, TRateLimiterMode::Rps>;

            return SendLocalRpcRequestNoResult<TEvDropTableRequest>(std::move(dropTable), Database, GetTokenCompat());
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
                ev->Record.SetUserToken(UserToken->Serialized);
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
                ev->Record.SetUserToken(UserToken->Serialized);
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
                ev->Record.SetUserToken(UserToken->Serialized);
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
                ev->Record.SetUserToken(UserToken->Serialized);
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
                    ev->Record.SetUserToken(UserToken->Serialized);
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
                ev->Record.SetUserToken(UserToken->Serialized);
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

    TFuture<TMkqlResult> ExecuteMkql(const TString& cluster, const TString& program,
        TKqpParamsMap&& params, const TMkqlSettings& settings, const TKqpSnapshot& snapshot) override
    {
        return RunInternal(cluster, program, std::move(params), false, false, settings, snapshot);
    }

    TFuture<TMkqlResult> ExecuteMkqlPrepared(const TString& cluster, const TString& program,
        TKqpParamsMap&& params, const TMkqlSettings& settings, const TKqpSnapshot& snapshot) override
    {
        return RunInternal(cluster, program, std::move(params), false, true, settings, snapshot);
    }

    TFuture<TMkqlResult> PrepareMkql(const TString& cluster, const TString& program) override {
        return RunInternal(cluster, program, TKqpParamsMap(), true, false, TMkqlSettings());
    }

    TFuture<TExecPhysicalResult> ExecutePhysical(TExecPhysicalRequest&& request, const NActors::TActorId& target) override {
        return ExecutePhysicalQueryInternal(std::move(request), target, false);
    }

    TFuture<TExecPhysicalResult> ExecutePure(TExecPhysicalRequest&& request, const NActors::TActorId& target) override {
        YQL_ENSURE(!request.Transactions.empty());
        YQL_ENSURE(request.Locks.empty());

        auto containOnlyPureStages = [](const auto& request) {
            for (const auto& tx : request.Transactions) {
                if (tx.Body.GetType() != NKqpProto::TKqpPhyTx::TYPE_COMPUTE) {
                    return false;
                }

                for (const auto& stage : tx.Body.GetStages()) {
                    if (stage.InputsSize() != 0) {
                        return false;
                    }
                }
            }

            return true;
        };

        YQL_ENSURE(containOnlyPureStages(request));
        return ExecutePhysicalQueryInternal(std::move(request), target, false);
    }

    TFuture<TQueryResult> ExecScanQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings, ui64 rowsLimit) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->Serialized);
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_SCAN);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetStatsMode(settings.StatsMode);

        if (!params.Values.empty()) {
            FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());
        }

        return SendKqpScanQueryRequest(ev.Release(), rowsLimit,
            [] (TPromise<TQueryResult> promise, TResponse&& responseEv) {
                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
                KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
                promise.SetValue(std::move(queryResult));
            });
    }

    TFuture<TQueryResult> StreamExecDataQueryAst(const TString& cluster, const TString& query,
        TKqpParamsMap&& params, const TAstQuerySettings& settings,
        const Ydb::Table::TransactionSettings& txSettings, const NActors::TActorId& target) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->Serialized);
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_DML);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetStatsMode(settings.StatsMode);

        if (!params.Values.empty()) {
            FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());
        }

        //auto& querySettings = *ev->Record.MutableRequest()->MutableQuerySettings();
        //querySettings.set_use_new_engine(NYql::GetFlagValue(settings.UseNewEngine));

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
        TKqpParamsMap&& params, const TAstQuerySettings& settings, const NActors::TActorId& target) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->Serialized);
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_SCAN);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetStatsMode(settings.StatsMode);

        if (!params.Values.empty()) {
            FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());
        }

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
            ev->Record.SetUserToken(UserToken->Serialized);
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

    TFuture<TQueryResult> ExecDataQueryAst(const TString& cluster, const TString& query, TKqpParamsMap&& params,
        const TAstQuerySettings& settings, const Ydb::Table::TransactionSettings& txSettings) override
    {
        YQL_ENSURE(cluster == Cluster);

        using TRequest = NKqp::TEvKqp::TEvQueryRequest;
        using TResponse = NKqp::TEvKqp::TEvQueryResponse;

        auto ev = MakeHolder<TRequest>();
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->Serialized);
        }

        ev->Record.MutableRequest()->SetDatabase(Database);
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_AST_DML);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetStatsMode(settings.StatsMode);

        if (!params.Values.empty()) {
            FillParameters(std::move(params), *ev->Record.MutableRequest()->MutableParameters());
        }

        //auto& querySettings = *ev->Record.MutableRequest()->MutableQuerySettings();
        //querySettings.set_use_new_engine(NYql::GetFlagValue(settings.UseNewEngine));

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
            ev->Record.SetUserToken(UserToken->Serialized);
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

    TFuture<TExecPhysicalResult> ExecuteScanQuery(TExecPhysicalRequest&& request, const TActorId& target) override {
        return ExecutePhysicalQueryInternal(std::move(request), target, true);
    }

    TFuture<TKqpSnapshotHandle> CreatePersistentSnapshot(const TVector<TString>& tablePaths, TDuration queryTimeout) override {
        auto* snapMgr = CreateKqpSnapshotManager(Database, queryTimeout);
        auto snapMgrActorId = RegisterActor(snapMgr);

        auto ev = MakeHolder<TEvKqpSnapshot::TEvCreateSnapshotRequest>(tablePaths);

        return SendActorRequest<
            TEvKqpSnapshot::TEvCreateSnapshotRequest,
            TEvKqpSnapshot::TEvCreateSnapshotResponse,
            IKqpGateway::TKqpSnapshotHandle>
        (
            snapMgrActorId,
            ev.Release(),
            [snapMgrActorId](TPromise<IKqpGateway::TKqpSnapshotHandle> promise,
                             TEvKqpSnapshot::TEvCreateSnapshotResponse&& response) mutable
            {
                IKqpGateway::TKqpSnapshotHandle handle;
                handle.Snapshot = response.Snapshot;
                handle.ManagingActor = snapMgrActorId;
                handle.Status = response.Status;
                handle.AddIssues(response.Issues);
                promise.SetValue(handle);
            }
        );
    }

    NThreading::TFuture<TKqpSnapshotHandle> AcquireMvccSnapshot(TDuration queryTimeout) override {
        auto* snapMgr = CreateKqpSnapshotManager(Database, queryTimeout);
        auto snapMgrActorId = RegisterActor(snapMgr);

        auto ev = MakeHolder<TEvKqpSnapshot::TEvCreateSnapshotRequest>();

        return SendActorRequest<
            TEvKqpSnapshot::TEvCreateSnapshotRequest,
            TEvKqpSnapshot::TEvCreateSnapshotResponse,
            IKqpGateway::TKqpSnapshotHandle>
            (
                snapMgrActorId,
                ev.Release(),
                [](TPromise<IKqpGateway::TKqpSnapshotHandle> promise,
                                 TEvKqpSnapshot::TEvCreateSnapshotResponse&& response) mutable
                {
                    IKqpGateway::TKqpSnapshotHandle handle;
                    handle.Snapshot = response.Snapshot;
                    handle.Status = response.Status;
                    handle.AddIssues(response.Issues);
                    promise.SetValue(handle);
                }
            );
    }

    void DiscardPersistentSnapshot(const TKqpSnapshotHandle& handle) override {
        if (handle.ManagingActor)
            ActorSystem->Send(handle.ManagingActor, new TEvKqpSnapshot::TEvDiscardSnapshot(handle.Snapshot));
    }

    TInstant GetCurrentTime() const override {
        return TAppData::TimeProvider->Now();
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

    TFuture<TMkqlResult> SendMkqlRequest(TEvTxUserProxy::TEvProposeTransaction* request,
        TKqpParamsMap&& paramsMap, TMkqlRequestHandler::TCallbackFunc callback)
    {
        auto promise = NewPromise<TMkqlResult>();
        IActor* requestHandler = new TMkqlRequestHandler(Counters->Counters->AllocCounters, request,
            std::move(paramsMap), promise, callback, MkqlComplileService);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    TFuture<TGenericResult> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request)
    {
        auto promise = NewPromise<TGenericResult>();
        IActor* requestHandler = new TSchemeOpRequestHandler(request, promise);
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

    TFuture<TMkqlResult> RunInternal(const TString& cluster, const TString& program, TKqpParamsMap&& params,
        bool compileOnly, bool prepared, const TMkqlSettings& settings, const TKqpSnapshot& snapshot = TKqpSnapshot::InvalidSnapshot)
    {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TMkqlResult>(cluster);
            }

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(Database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->Serialized);
            }
            auto& mkqlTx = *ev->Record.MutableTransaction()->MutableMiniKQLTransaction();
            mkqlTx.SetFlatMKQL(true);
            mkqlTx.SetMode(compileOnly ? TMiniKQLTransaction::COMPILE : TMiniKQLTransaction::COMPILE_AND_EXEC);

            if (prepared) {
                mkqlTx.MutableProgram()->SetBin(program);
            } else {
                mkqlTx.MutableProgram()->SetText(program);
            }

            if (!compileOnly) {
                if (settings.LlvmRuntime) {
                    mkqlTx.SetLlvmRuntime(true);
                }

                if (settings.PerShardKeysSizeLimitBytes) {
                    mkqlTx.SetPerShardKeysSizeLimitBytes(*settings.PerShardKeysSizeLimitBytes);
                }

                if (settings.CancelAfterMs) {
                    ev->Record.SetCancelAfterMs(settings.CancelAfterMs);
                }

                if (settings.TimeoutMs) {
                    ev->Record.SetExecTimeoutPeriod(settings.TimeoutMs);
                }

                const auto& limits = settings.Limits;

                if (limits.AffectedShardsLimit) {
                    mkqlTx.MutableLimits()->SetAffectedShardsLimit(limits.AffectedShardsLimit);
                }

                if (limits.ReadsetCountLimit) {
                    mkqlTx.MutableLimits()->SetReadsetCountLimit(limits.ReadsetCountLimit);
                }

                if (limits.ComputeNodeMemoryLimitBytes) {
                    mkqlTx.MutableLimits()->SetComputeNodeMemoryLimitBytes(limits.ComputeNodeMemoryLimitBytes);
                }

                if (limits.TotalReadSizeLimitBytes) {
                    mkqlTx.MutableLimits()->SetTotalReadSizeLimitBytes(limits.TotalReadSizeLimitBytes);
                }

                if (snapshot.IsValid()) {
                    mkqlTx.SetSnapshotStep(snapshot.Step);
                    mkqlTx.SetSnapshotTxId(snapshot.TxId);
                }
            }

            if (settings.CollectStats) {
                mkqlTx.SetCollectStats(true);
            }

            return SendMkqlRequest(ev.Release(), std::move(params),
                [compileOnly] (TPromise<TMkqlResult> promise, TTransactionResponse&& response) {
                    try {
                        promise.SetValue(GetMkqlResult(GetRunResponse(std::move(response.Record)), compileOnly));
                    }
                    catch (yexception& e) {
                        promise.SetValue(ResultFromException<TMkqlResult>(e));
                    }
                });
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TMkqlResult>(e));
        }
    }

    TFuture<TExecPhysicalResult> ExecutePhysicalQueryInternal(TExecPhysicalRequest&& request, const TActorId& target,
        bool streaming)
    {
        const bool needTxId = request.NeedTxId;
        auto executerActor = CreateKqpExecuter(std::move(request), Database,
            UserToken ? TMaybe<TString>(UserToken->Serialized) : Nothing(), Counters);
        auto executerId = RegisterActor(executerActor);

        LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_GATEWAY, "Created new KQP executer: " << executerId);

        auto promise = NewPromise<TExecPhysicalResult>();

        auto ev = MakeHolder<TEvTxUserProxy::TEvProposeKqpTransaction>(executerId);
        IActor* requestHandler = new TKqpExecPhysicalRequestHandler(ev.Release(), streaming, target, promise, needTxId);
        RegisterActor(requestHandler);

        return promise.GetFuture();
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

private:
    static TRunResponse GetRunResponse(NKikimrTxUserProxy::TEvProposeTransactionStatus&& ev) {
        IKqpGateway::TRunResponse response;

        response.HasProxyError = ev.GetStatus() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
            && ev.GetStatus() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready;
        response.ProxyStatus = ev.GetStatus();
        if (response.HasProxyError) {
            NMsgBusProxy::ExplainProposeTransactionStatus(ev.GetStatus(), response.ProxyStatusName,
                response.ProxyStatusDesc);
        }

        auto executionResponseStatus = static_cast<NMiniKQL::IEngineFlat::EStatus>(
            ev.GetExecutionEngineResponseStatus());
        auto executionEngineStatus = static_cast<NMiniKQL::IEngineFlat::EResult>(
            ev.GetExecutionEngineStatus());
        response.HasExecutionEngineError = executionResponseStatus == NMiniKQL::IEngineFlat::EStatus::Error &&
            executionEngineStatus != NMiniKQL::IEngineFlat::EResult::Ok;
        if (response.HasExecutionEngineError) {
            NMsgBusProxy::ExplainExecutionEngineStatus(ev.GetExecutionEngineStatus(),
                response.ExecutionEngineStatusName, response.ExecutionEngineStatusDesc);
        }

        NYql::IssuesFromMessage(ev.GetIssues(), response.Issues);

        response.MiniKQLErrors = ev.GetMiniKQLErrors();
        response.DataShardErrors = ev.GetDataShardErrors();
        response.MiniKQLCompileResults = ev.GetMiniKQLCompileResults();

        response.ExecutionEngineEvaluatedResponse.Swap(ev.MutableExecutionEngineEvaluatedResponse());
        response.TxStats = ev.GetTxStats();

        return response;
    }

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

    static bool CheckLoadTableMetadataStatus(ui32 status, const TString& reason,
        IKikimrGateway::TTableMetadataResult& error)
    {
        using TResult = IKikimrGateway::TTableMetadataResult;

        switch (status) {
            case NKikimrScheme::EStatus::StatusSuccess:
            case NKikimrScheme::EStatus::StatusPathDoesNotExist:
                return true;
            case NKikimrScheme::EStatus::StatusSchemeError:
                error = ResultFromError<TResult>(YqlIssue({}, TIssuesIds::KIKIMR_SCHEME_ERROR, reason));
                return false;
            case NKikimrScheme::EStatus::StatusAccessDenied:
                error = ResultFromError<TResult>(YqlIssue({}, TIssuesIds::KIKIMR_ACCESS_DENIED, reason));
                return false;
            case NKikimrScheme::EStatus::StatusNotAvailable:
                error = ResultFromError<TResult>(YqlIssue({}, TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, reason));
                return false;
            default:
                error = ResultFromError<TResult>(TStringBuilder() << status << ": " <<  reason);
                return false;
        }
    }

    static TMkqlResult GetMkqlResult(TRunResponse&& response, bool compileOnly) {
        auto& txRes = response.MiniKQLCompileResults;

        if (txRes.ProgramCompileErrorsSize() > 0) {
            NYql::TIssues errors;
            for (size_t i = 0, end = txRes.ProgramCompileErrorsSize(); i < end; ++i) {
                const auto& err = txRes.GetProgramCompileErrors(i);
                errors.AddIssue(NYql::IssueFromMessage(err));
            }
            return ResultFromIssues<TMkqlResult>(TIssuesIds::KIKIMR_COMPILE_ERROR, "MiniKQL compilation error",
                errors);
        }

        YQL_ENSURE(txRes.ParamsCompileErrorsSize() == 0);

        if (!compileOnly) {
            NYql::TIssues internalIssues;
            if (response.HasExecutionEngineError) {
                internalIssues.AddIssue(TIssue(NYql::TPosition(), TString("Execution engine failure (")
                    + response.ExecutionEngineStatusName + "): " + response.ExecutionEngineStatusDesc));
            }

            if (!response.DataShardErrors.empty()) {
                internalIssues.AddIssue(TIssue(NYql::TPosition(), TString("Data shard errors: ")
                    + response.DataShardErrors));
            }

            if (!response.MiniKQLErrors.empty()) {
                internalIssues.AddIssue(TIssue(NYql::TPosition(), TString("Execution engine errors: ")
                    + response.MiniKQLErrors));
            }

            if (response.HasProxyError) {
                auto message = TString("Error executing transaction (") + response.ProxyStatusName + "): "
                    + response.ProxyStatusDesc;

                NYql::TIssue proxyIssue(NYql::TPosition(), message);
                for (auto& issue : internalIssues) {
                    proxyIssue.AddSubIssue(MakeIntrusive<TIssue>(issue));
                }
                for (auto& issue : response.Issues) {
                    proxyIssue.AddSubIssue(MakeIntrusive<TIssue>(issue));
                }

                return ResultFromIssues<TMkqlResult>(KikimrProxyErrorStatus(response.ProxyStatus), {proxyIssue});
            }

            if (!internalIssues.Empty()) {
                return ResultFromErrors<TMkqlResult>(internalIssues);
            }
        }

        TMkqlResult result;
        result.SetSuccess();
        result.CompiledProgram = txRes.GetCompiledProgram();
        result.Result.Swap(&response.ExecutionEngineEvaluatedResponse);
        result.TxStats.Swap(&response.TxStats);
        return result;
    }

    static NYql::EYqlIssueCode KikimrProxyErrorStatus(ui32 proxyStatus) {
        NYql::EYqlIssueCode status = TIssuesIds::DEFAULT_ERROR;

        switch (proxyStatus) {
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
                status = TIssuesIds::KIKIMR_SCHEME_MISMATCH;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorDeclined:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorOutdated:
                status = TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded:
                status = TIssuesIds::KIKIMR_OVERLOADED;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecResultUnavailable:
                status = TIssuesIds::KIKIMR_RESULT_UNAVAILABLE;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied:
                status = TIssuesIds::KIKIMR_ACCESS_DENIED;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout:
                status = TIssuesIds::KIKIMR_TIMEOUT;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecCancelled:
                status = TIssuesIds::KIKIMR_OPERATION_CANCELLED;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest:
                status = TIssuesIds::KIKIMR_BAD_REQUEST;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown:
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorUnknown:
                status = TIssuesIds::KIKIMR_OPERATION_STATE_UNKNOWN;
                break;
            case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted:
                status = TIssuesIds::KIKIMR_OPERATION_ABORTED;
                break;
            default:
                break;
        }

        return status;
    }

    static void FillParameters(TKqpParamsMap&& params, NKikimrMiniKQL::TParams& output) {
        output.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
        auto type = output.MutableType()->MutableStruct();
        auto value = output.MutableValue();
        for (auto& pair : params.Values) {
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
                    error = TStringBuilder() << "Unknown compression '" << family.Compression << "' for a column family";
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

        return true;
    }

    static bool FillCreateTableDesc(NYql::TKikimrTableMetadataPtr metadata,
        NKikimrSchemeOp::TTableDescription& tableDesc, const NGRpcService::TTableProfiles& profiles,
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

private:
    TString Cluster;
    TString Database;
    TActorSystem* ActorSystem;
    ui32 NodeId;
    TKqpRequestCounters::TPtr Counters;
    TAlignedPagePoolCounters AllocCounters;
    TMaybe<TUserTokenData> UserToken;
    std::shared_ptr<IKqpTableMetadataLoader> MetadataLoader;
    TActorId MkqlComplileService;
};

} // namespace

TIntrusivePtr<IKqpGateway> CreateKikimrIcGateway(const TString& cluster, const TString& database,
    std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader>&& metadataLoader, TActorSystem* actorSystem, ui32 nodeId, TKqpRequestCounters::TPtr counters,
    const TActorId& mkqlComplileService)
{
    return MakeIntrusive<TKikimrIcGateway>(cluster, database, std::move(metadataLoader), actorSystem, nodeId, counters, mkqlComplileService);
}

} // namespace NKqp
} // namespace NKikimr
