#include "kqp_gateway.h"
#include "actors/kqp_ic_gateway_actors.h"
#include "actors/analyze_actor.h"
#include "actors/scheme.h"
#include "kqp_metadata_loader.h"
#include "local_rpc/helper.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/engine/mkql_proto.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/rm_service/kqp_snapshot_manager.h>
#include <ydb/core/protos/external_sources.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/ydb_convert/column_families.h>
#include <ydb/core/ydb_convert/table_profiles.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/public/lib/base/msgbus_status.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/persqueue_v1/rpc_calls.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/string/split.h>
#include <util/string/vector.h>

#include <ydb/core/protos/auth.pb.h>

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

    using TBase::Handle;
    using TBase::HandleResponse;

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
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

// Handles scan query and generic query request for StreamExecuteYqlScript
class TKqpForwardStreamRequestHandler : public TRequestHandlerBase<
    TKqpForwardStreamRequestHandler,
    NKqp::TEvKqp::TEvQueryRequest,
    NKqp::TEvKqp::TEvQueryResponse,
    IKqpGateway::TQueryResult>
{
public:
    using TRequest = NKqp::TEvKqp::TEvQueryRequest;
    using TResponse = NKqp::TEvKqp::TEvQueryResponse;
    using TResult = IKqpGateway::TQueryResult;

    using TBase = TKqpForwardStreamRequestHandler::TBase;

    TKqpForwardStreamRequestHandler(TRequest* request, const TActorId& target, TPromise<TResult> promise,
            TCallbackFunc callback)
        : TBase(request, promise, callback)
        , TargetActorId(target) {}

    void Bootstrap(const TActorContext& ctx) {
        ActorIdToProto(SelfId(), this->Request->Record.MutableRequestActorId());

        TActorId kqpProxy = MakeKqpProxyID(ctx.SelfID.NodeId());
        ctx.Send(kqpProxy, this->Request.Release());

        this->Become(&TKqpForwardStreamRequestHandler::AwaitState);
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

    void Handle(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev, const TActorContext& ctx) {
        const TString msg = ev->Get()->GetIssues().ToOneLineString();
        LOG_DEBUG_S(ctx, NKikimrServices::KQP_GATEWAY, SelfId()
            << "Received abort execution event for query: " << msg);

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
            HFunc(NKqp::TEvKqp::TEvAbortExecution, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamData, Handle);
            HFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, Handle);
            HFunc(TResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpForwardStreamRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    TActorId ExecuterActorId;
    TActorId TargetActorId;
    TVector<NYql::NDqProto::TDqExecutionStats> Executions;
};

class TKqpGenericQueryRequestHandler: public TRequestHandlerBase<
    TKqpGenericQueryRequestHandler,
    TEvKqp::TEvQueryRequest,
    TEvKqp::TEvQueryResponse,
    IKqpGateway::TQueryResult>
{
    struct TResultSetDescription {
        Ydb::ResultSet ResultSet;
        ui64 RowCount = 0;
        ui64 ByteCount = 0;
        bool Initialized = false;
    };

public:
    using TRequest = TEvKqp::TEvQueryRequest;
    using TResponse = TEvKqp::TEvQueryResponse;
    using TResult = IKqpGateway::TQueryResult;

    using TBase = TKqpGenericQueryRequestHandler::TBase;
    using TCallbackFunc = TBase::TCallbackFunc;

    TKqpGenericQueryRequestHandler(TRequest* request, ui64 rowsLimit, ui64 sizeLimit, TPromise<TResult> promise, TCallbackFunc callback)
        : TBase(request, promise, callback)
        , RowsLimit(rowsLimit)
        , SizeLimit(sizeLimit)
    {}

    void Bootstrap() {
        ActorIdToProto(SelfId(), Request->Record.MutableRequestActorId());
        Send(MakeKqpProxyID(SelfId().NodeId()), Request.Release());
        Become(&TKqpGenericQueryRequestHandler::AwaitState);
    }

    void Handle(TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        auto& record = ev->Get()->Record;

        const ui32 resultSetId = record.GetQueryResultIndex();
        if (resultSetId >= ResultSets.size()) {
            ResultSets.resize(resultSetId + 1);
        }

        if (!ResultSets[resultSetId].Initialized) {
            ResultSets[resultSetId].Initialized = true;
            for (auto& column : *record.MutableResultSet()->mutable_columns()) {
                ResultSets[resultSetId].ResultSet.add_columns()->CopyFrom(std::move(column));
            }
        }

        if (!ResultSets[resultSetId].ResultSet.truncated()) {
            for (auto& row : *record.MutableResultSet()->mutable_rows()) {
                if (RowsLimit && ResultSets[resultSetId].RowCount + 1 > RowsLimit) {
                    ResultSets[resultSetId].ResultSet.set_truncated(true);
                    break;
                }

                auto serializedSize = row.ByteSizeLong();
                if (SizeLimit && ResultSets[resultSetId].ByteCount + serializedSize > SizeLimit) {
                    ResultSets[resultSetId].ResultSet.set_truncated(true);
                    break;
                }

                ResultSets[resultSetId].RowCount++;
                ResultSets[resultSetId].ByteCount += serializedSize;
                *ResultSets[resultSetId].ResultSet.add_rows() = std::move(row);
            }
        }

        auto response = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        response->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        response->Record.SetFreeSpace(SizeLimit && SizeLimit < std::numeric_limits<i64>::max() ? SizeLimit : std::numeric_limits<i64>::max());
        Send(ev->Sender, response.Release());
    }

    using TBase::HandleResponse;

    void HandleResponse(TResponse::TPtr &ev, const TActorContext &ctx) {
        auto& response = *ev->Get()->Record.GetRef().MutableResponse();

        for (auto& resultSet : ResultSets) {
            ConvertYdbResultToKqpResult(std::move(resultSet.ResultSet), *response.AddResults());
        }

        TBase::HandleResponse(ev, ctx);
    }

    STFUNC(AwaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvStreamData, Handle);
            HFunc(TEvKqp::TEvQueryResponse, HandleResponse);

        default:
            TBase::HandleUnexpectedEvent("TKqpGenericQueryRequestHandler", ev->GetTypeRewrite());
        }
    }

private:
    std::vector<TResultSetDescription> ResultSets;
    const ui64 RowsLimit;
    const ui64 SizeLimit;
};

class TKqpSchemeExecuterRequestHandler: public TActorBootstrapped<TKqpSchemeExecuterRequestHandler> {
public:
    using TResult = IKqpGateway::TGenericResult;

    TKqpSchemeExecuterRequestHandler(TKqpPhyTxHolder::TConstPtr phyTx, NKikimrKqp::EQueryType queryType, const TMaybe<TString>& requestType, const TString& database,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, TPromise<TResult> promise)
        : PhyTx(std::move(phyTx))
        , QueryType(queryType)
        , Database(database)
        , UserToken(std::move(userToken))
        , Promise(promise)
        , RequestType(requestType)
    {}

    void Bootstrap() {
        auto ctx = MakeIntrusive<TUserRequestContext>();
        IActor* actor = CreateKqpSchemeExecuter(PhyTx, QueryType, SelfId(), RequestType, Database, UserToken, false /* temporary */, TString() /* sessionId */, ctx);
        Register(actor);
        Become(&TThis::WaitState);
    }

    STATEFN(WaitState) {
        switch(ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvTxResponse, Handle);
        }
    }

    void Handle(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();

        TResult result;
        if (response->GetStatus() == Ydb::StatusIds::SUCCESS) {
            result.SetSuccess();
        } else {
            for (auto& issue : response->GetIssues()) {
                result.AddIssue(NYql::IssueFromMessage(issue));
            }
        }

        Promise.SetValue(result);
        this->PassAway();
    }

private:
    TKqpPhyTxHolder::TConstPtr PhyTx;
    const NKikimrKqp::EQueryType QueryType;
    const TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TPromise<TResult> Promise;
    const TMaybe<TString> RequestType;
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
        auto result = ::NKikimr::NKqp::ExecuteLiteral(std::move(Request), Counters, SelfId(), MakeIntrusive<TUserRequestContext>());
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
        } else {
            for (auto& issue : response->GetIssues()) {
                result.AddIssue(NYql::IssueFromMessage(issue));
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

template<typename TResult>
TFuture<TResult> InvalidCluster(const TString& cluster) {
    return MakeFuture(ResultFromError<TResult>("Invalid cluster: " + cluster));
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

    struct TModifyPermissionsWrapper : public TThrRefBase {
        using TMethod = std::function<void(NYql::TModifyPermissionsSettings::EAction action, THashSet<TString>&& permissions, THashSet<TString>&& roles, TVector<TString>&& paths)>;
        TMethod ModifyPermissionsForPaths;
    };
}

class TKikimrIcGateway : public IKqpGateway {
private:
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    TKikimrIcGateway(const TString& cluster, NKikimrKqp::EQueryType queryType, const TString& database, std::shared_ptr<IKqpTableMetadataLoader>&& metadataLoader,
        TActorSystem* actorSystem, ui32 nodeId, TKqpRequestCounters::TPtr counters, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig)
        : Cluster(cluster)
        , QueryType(queryType)
        , Database(database)
        , ActorSystem(actorSystem)
        , NodeId(nodeId)
        , Counters(counters)
        , MetadataLoader(std::move(metadataLoader))
        , QueryServiceConfig(queryServiceConfig) {}

    bool HasCluster(const TString& cluster) override {
        return cluster == Cluster;
    }

    TVector<TString> GetClusters() override {
        return {Cluster};
    }

    TString GetDefaultCluster() override {
        return Cluster;
    }

    TString GetDatabase() override {
        return Database;
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

    bool GetDomainLoginOnly() override {
        TAppData* appData = AppData(ActorSystem);
        return appData && appData->AuthConfig.GetDomainLoginOnly();
    }

    TMaybe<TString> GetDomainName() override {
        TAppData* appData = AppData(ActorSystem);
        if (GetDomainLoginOnly()) {
            if (appData->DomainsInfo && appData->DomainsInfo->Domain) {
                return appData->DomainsInfo->GetDomain()->Name;
            }
        }
        return {};
    }

    TVector<NKikimrKqp::TKqpTableMetadataProto> GetCollectedSchemeData() override {
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
            if (!settings.WithExternalDatasources_ && !CheckCluster(cluster)) {
                return InvalidCluster<TTableMetadataResult>(cluster);
            }

            return MetadataLoader->LoadTableMetadata(cluster, table, settings, Database, UserToken);
        } catch (yexception& e) {
            return MakeFuture(ResultFromException<TTableMetadataResult>(e));
        }
    }

    TFuture<TKqpTableProfilesResult> GetTableProfiles() override {
        using TConfigRequest = NConsole::TEvConfigsDispatcher::TEvGetConfigRequest;
        using TConfigResponse = NConsole::TEvConfigsDispatcher::TEvGetConfigResponse;

        ui32 configKind = (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem;
        auto ev = MakeHolder<TConfigRequest>(configKind);

        auto profilesPromise = NewPromise<TKqpTableProfilesResult>();

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

        configFuture.Subscribe([profilesPromise](const TFuture<TAppConfigResult>& future) mutable {
            auto configResult = future.GetValue();
            if (!configResult.Success()) {
                profilesPromise.SetValue(ResultFromIssues<TKqpTableProfilesResult>(configResult.Status(),
                    configResult.Issues()));
                return;
            }

            TKqpTableProfilesResult result;
            result.SetSuccess();
            result.Profiles.Load(configResult.Config->GetTableProfilesConfig());

            profilesPromise.SetValue(std::move(result));
        });

        return profilesPromise.GetFuture();
    }

    TFuture<TGenericResult> CreateTable(NYql::TKikimrTableMetadataPtr metadata, bool createDir, bool existingOk, bool replaceIfExists) override {
        Y_UNUSED(metadata);
        Y_UNUSED(createDir);
        Y_UNUSED(existingOk);
        Y_UNUSED(replaceIfExists);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> ModifyScheme(NKikimrSchemeOp::TModifyScheme&& modifyScheme) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(Database);
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }
        ev->Record.MutableTransaction()->MutableModifyScheme()->Swap(&modifyScheme);

        auto tablePromise = NewPromise<TGenericResult>();
        SendSchemeRequest(ev.Release()).Apply(
            [tablePromise](const TFuture<TGenericResult>& future) mutable {
                tablePromise.SetValue(future.GetValue());
            });

        return tablePromise.GetFuture();
    }

    TFuture<TGenericResult> CreateColumnTable(NYql::TKikimrTableMetadataPtr metadata,
            bool createDir, bool existingOk) override {
        Y_UNUSED(metadata);
        Y_UNUSED(createDir);
        Y_UNUSED(existingOk);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> AlterTable(const TString&, Ydb::Table::AlterTableRequest&&, const TMaybe<TString>&, ui64, NKikimrIndexBuilder::TIndexBuildSettings&&) override
    {
        try {
            YQL_ENSURE(false, "gateway doesn't implement alter");
        } catch (yexception& e) {
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

    TFuture<TGenericResult> DropTable(const TString& cluster, const NYql::TDropTableSettings& settings) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            Ydb::Table::DropTableRequest dropTable;
            dropTable.set_path(settings.Table);

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

    TFuture<TGenericResult> CreateSequence(const TString& cluster,
            const NYql::TCreateSequenceSettings& settings, bool existingOk) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(existingOk);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> DropSequence(const TString& cluster,
            const NYql::TDropSequenceSettings& settings, bool missingOk) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(missingOk);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> AlterSequence(const TString& cluster,
            const NYql::TAlterSequenceSettings& settings, bool missingOk) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(missingOk);
        return NotImplemented<TGenericResult>();
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

    TFuture<TGenericResult> CreateReplication(const TString&, const NYql::TCreateReplicationSettings&) override {
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> AlterReplication(const TString&, const NYql::TAlterReplicationSettings&) override {
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> DropReplication(const TString&, const NYql::TDropReplicationSettings&) override {
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> AlterColumnTable(const TString& cluster,
                                             const NYql::TAlterColumnTableSettings& settings) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> CreateTableStore(const TString& cluster,
                                             const NYql::TCreateTableStoreSettings& settings,
                                             bool existingOk) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(existingOk);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> AlterTableStore(const TString& cluster,
                                            const NYql::TAlterTableStoreSettings& settings) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> DropTableStore(const TString& cluster,
                                           const NYql::TDropTableStoreSettings& settings, bool missingOk) override {
        Y_UNUSED(cluster);
        Y_UNUSED(settings);
        Y_UNUSED(missingOk);
        return NotImplemented<TGenericResult>();
    }

    TFuture<TGenericResult> CreateExternalTable(const TString& cluster,
                                                const NYql::TCreateExternalTableSettings& settings,
                                                bool createDir, bool existingOk, bool replaceIfExists) override {
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
            schemeTx.SetFailedOnAlreadyExists(!existingOk);

            NKikimrSchemeOp::TExternalTableDescription& externalTableDesc = *schemeTx.MutableCreateExternalTable();
            NSchemeHelpers::FillCreateExternalTableColumnDesc(externalTableDesc, pathPair.second, replaceIfExists, settings);
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
                                              const NYql::TDropExternalTableSettings& settings,
                                              bool missingOk) override {
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
            schemeTx.SetSuccessOnNotExist(missingOk);

            NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
            drop.SetName(pathPair.second);
            return SendSchemeRequest(ev.Release());
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> ModifyPermissions(const TString& cluster, const NYql::TModifyPermissionsSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            if (settings.Permissions.empty() && !settings.IsPermissionsClear) {
                return MakeFuture(ResultFromError<TGenericResult>("No permissions names for modify permissions"));
            }

            if (settings.Paths.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No paths for modify permissions"));
            }

            if (settings.Roles.empty()) {
                return MakeFuture(ResultFromError<TGenericResult>("No roles for modify permissions"));
            }

            TVector<TPromise<TGenericResult>> promises;
            promises.reserve(settings.Paths.size());
            TVector<TFuture<TGenericResult>> futures;
            futures.reserve(settings.Paths.size());

            NACLib::TDiffACL acl;
            switch (settings.Action) {
                case NYql::TModifyPermissionsSettings::EAction::Grant: {
                    for (const auto& sid : settings.Roles) {
                        for (const auto& permission : settings.Permissions) {
                            TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                            acl.AddAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                        }
                    }
                }
                break;
                case NYql::TModifyPermissionsSettings::EAction::Revoke: {
                    if (settings.IsPermissionsClear) {
                        for (const auto& sid : settings.Roles) {
                            acl.ClearAccessForSid(sid);
                        }
                    } else {
                        for (const auto& sid : settings.Roles) {
                            for (const auto& permission : settings.Permissions) {
                                TACLAttrs aclAttrs = ConvertYdbPermissionNameToACLAttrs(permission);
                                acl.RemoveAccess(NACLib::EAccessType::Allow, aclAttrs.AccessMask, sid, aclAttrs.InheritanceType);
                            }
                        }
                    }
                }
                break;
                default: {
                    return MakeFuture(ResultFromError<TGenericResult>("Unknown permission action"));
                }
            }

            const auto serializedDiffAcl = acl.SerializeAsString();

            TVector<std::pair<const TString*, std::pair<TString, TString>>> pathPairs;
            pathPairs.reserve(settings.Paths.size());
            for (const auto& path : settings.Paths) {
                pathPairs.push_back(std::make_pair(&path, NSchemeHelpers::SplitPathByDirAndBaseNames(path)));
            }

            for (const auto& path : pathPairs) {
                promises.push_back(NewPromise<TGenericResult>());
                futures.push_back(promises.back().GetFuture());

                auto ev = MakeHolder<TRequest>();
                auto& record = ev->Record;
                record.SetDatabaseName(Database);
                if (UserToken) {
                    record.SetUserToken(UserToken->GetSerializedToken());
                }

                const auto& [dirname, basename] = path.second;
                NKikimrSchemeOp::TModifyScheme* modifyScheme = record.MutableTransaction()->MutableModifyScheme();
                modifyScheme->SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);
                modifyScheme->SetWorkingDir(dirname);
                modifyScheme->MutableModifyACL()->SetName(basename);

                modifyScheme->MutableModifyACL()->SetDiffACL(serializedDiffAcl);
                SendSchemeRequest(ev.Release()).Apply([promise = promises.back(), path = *path.first](const TFuture<TGenericResult>& future) mutable{
                    auto result = future.GetValue();
                    if (!result.Success()) {
                        result.AddIssue(NYql::TIssue("Error for the path: " + path));
                    }
                    promise.SetValue(result);
                });
            }

            return WaitAll(futures).Apply([futures](const TFuture<void>& f){
                Y_UNUSED(f);
                TGenericResult result;
                result.SetSuccess();
                bool isSuccess = true;
                for (const auto& future : futures) {
                    TGenericResult receivedResult = future.GetValue();
                    if (!receivedResult.Success()) {
                        isSuccess = false;
                        result.AddIssues(receivedResult.Issues());
                    }
                }
                if (!isSuccess) {
                    result.SetStatus(TIssuesIds::DEFAULT_ERROR);
                }
                return result;
            });
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
            dropUser.SetMissingOk(settings.MissingOk);

            SendSchemeRequest(ev.Release()).Apply(
                [dropUserPromise](const TFuture<TGenericResult>& future) mutable {
                    dropUserPromise.SetValue(future.GetValue());
                }
            );

            return dropUserPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TGenericResult> Analyze(const TString& cluster, const NYql::TAnalyzeSettings& settings) override {
        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }
            
            auto analyzePromise = NewPromise<TGenericResult>();
            IActor* analyzeActor = new TAnalyzeActor(settings.TablePath, settings.Columns, analyzePromise);
            RegisterActor(analyzeActor);
            
            return analyzePromise.GetFuture();
        } catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    template <class TSettings>
    class IObjectModifier {
    public:
        using TYqlConclusionStatus = TConclusionSpecialStatus<TIssuesIds::EIssueCode, TIssuesIds::SUCCESS, TIssuesIds::DEFAULT_ERROR>;
    private:
        TKikimrIcGateway& Owner;
    protected:
        virtual TFuture<TYqlConclusionStatus> DoExecute(
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
                context.SetDatabase(Owner.Database);
                context.SetActorSystem(Owner.ActorSystem);
                return DoExecute(cBehaviour, settings, context).Apply([](const NThreading::TFuture<TYqlConclusionStatus>& f) {
                    if (f.HasValue() && !f.HasException() && f.GetValue().Ok()) {
                        TGenericResult result;
                        result.SetSuccess();
                        return NThreading::MakeFuture<TGenericResult>(result);
                    } else if (f.HasValue()) {
                        TGenericResult result;
                        result.SetStatus(f.GetValue().GetStatus());
                        auto issue = NYql::TIssue{f.GetValue().GetErrorMessage()};
                        issue.SetCode(f.GetValue().GetStatus(), NYql::TSeverityIds::S_ERROR);
                        result.AddIssue(issue);
                        return NThreading::MakeFuture<TGenericResult>(result);
                    } else {
                        TGenericResult result;
                        result.AddIssue(NYql::TIssue("Haven't reply"));
                        return NThreading::MakeFuture<TGenericResult>(result);
                    }
                });
            } catch (yexception& e) {
                return MakeFuture(ResultFromException<TGenericResult>(e));
            }
        }
    };

    class TObjectUpsert: public IObjectModifier<NYql::TUpsertObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TUpsertObjectSettings>;
    protected:
        virtual TFuture<TYqlConclusionStatus> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TUpsertObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override
        {
            return manager->GetOperationsManager()->UpsertObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    class TObjectCreate: public IObjectModifier<NYql::TCreateObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TCreateObjectSettings>;
    protected:
        virtual TFuture<TYqlConclusionStatus> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TCreateObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override {
            return manager->GetOperationsManager()->CreateObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    class TObjectAlter: public IObjectModifier<NYql::TAlterObjectSettings> {
    private:
        using TBase = IObjectModifier<NYql::TAlterObjectSettings>;
    protected:
        virtual TFuture<TYqlConclusionStatus> DoExecute(
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
        virtual TFuture<TYqlConclusionStatus> DoExecute(
            NMetadata::IClassBehaviour::TPtr manager, const NYql::TDropObjectSettings& settings,
            const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) override {
            return manager->GetOperationsManager()->DropObject(settings, TBase::GetNodeId(), manager, context);
        }
    public:
        using TBase::TBase;
    };

    TFuture<TGenericResult> UpsertObject(const TString& cluster, const NYql::TUpsertObjectSettings& settings) override {
        return TObjectUpsert(*this).Execute(cluster, settings);
    }

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

    TFuture<TGenericResult> RenameGroup(const TString& cluster, NYql::TRenameGroupSettings& settings) override {
        using TRequest = TEvTxUserProxy::TEvProposeTransaction;

        try {
            if (!CheckCluster(cluster)) {
                return InvalidCluster<TGenericResult>(cluster);
            }

            TString database;
            if (!GetDatabaseForLoginOperation(database)) {
                return MakeFuture(ResultFromError<TGenericResult>("Couldn't get domain name"));
            }

            TPromise<TGenericResult> renameGroupPromise = NewPromise<TGenericResult>();

            auto ev = MakeHolder<TRequest>();
            ev->Record.SetDatabaseName(database);
            if (UserToken) {
                ev->Record.SetUserToken(UserToken->GetSerializedToken());
            }
            auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
            schemeTx.SetWorkingDir(database);
            schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);
            auto& renameGroup = *schemeTx.MutableAlterLogin()->MutableRenameGroup();

            renameGroup.SetGroup(settings.GroupName);
            renameGroup.SetNewName(settings.NewName);

            SendSchemeRequest(ev.Release()).Apply(
                [renameGroupPromise](const TFuture<TGenericResult>& future) mutable {
                    renameGroupPromise.SetValue(future.GetValue());
                }
            );

            return renameGroupPromise.GetFuture();
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
            dropGroup.SetMissingOk(settings.MissingOk);

            SendSchemeRequest(ev.Release()).Apply(
                [dropGroupPromise](const TFuture<TGenericResult>& future) mutable {
                    dropGroupPromise.SetValue(future.GetValue());
                }
            );

            return dropGroupPromise.GetFuture();
        }
        catch (yexception& e) {
            return MakeFuture(ResultFromException<TGenericResult>(e));
        }
    }

    TFuture<TExecuteLiteralResult> ExecuteLiteral(const TString& program, const NKikimrMiniKQL::TType& resultType, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc) override {
        auto preparedQuery = std::make_unique<NKikimrKqp::TPreparedQuery>();
        auto& phyQuery = *preparedQuery->MutablePhysicalQuery();
        NKikimr::NKqp::IKqpGateway::TExecPhysicalRequest literalRequest(txAlloc);

        literalRequest.NeedTxId = false;
        literalRequest.MaxAffectedShards = 0;
        literalRequest.TotalReadSizeLimitBytes = 0;
        literalRequest.MkqlMemoryLimit = 100_MB;

        auto& transaction = *phyQuery.AddTransactions();
        transaction.SetType(NKqpProto::TKqpPhyTx::TYPE_COMPUTE);

        auto& stage = *transaction.AddStages();
        auto& stageProgram = *stage.MutableProgram();
        stageProgram.SetRuntimeVersion(NYql::NDqProto::RUNTIME_VERSION_YQL_1_0);
        stageProgram.SetRaw(program);
        stage.SetOutputsCount(1);

        auto& taskResult = *transaction.AddResults();
        *taskResult.MutableItemType() = resultType;
        auto& taskConnection = *taskResult.MutableConnection();
        taskConnection.SetStageIndex(0);

        NKikimr::NKqp::TPreparedQueryHolder queryHolder(preparedQuery.release(), txAlloc->HolderFactory.GetFunctionRegistry());

        NKikimr::NKqp::TQueryData::TPtr params = std::make_shared<NKikimr::NKqp::TQueryData>(txAlloc);

        literalRequest.Transactions.emplace_back(queryHolder.GetPhyTx(0), params);

        return ExecuteLiteral(std::move(literalRequest), params, 0).Apply([](const auto& future) {
            const auto& result = future.GetValue();

            TExecuteLiteralResult literalResult;

            if (result.Success()) {
                YQL_ENSURE(result.Results.size() == 1);
                literalResult.SetSuccess();
                literalResult.Result = result.Results[0];
            } else {
                literalResult.SetStatus(result.Status());
                literalResult.AddIssues(result.Issues());
            }

            return literalResult;
        });
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

        FillParameters(params, ev->Record.MutableRequest()->MutableYdbParameters());

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

        FillParameters(std::move(params), ev->Record.MutableRequest()->MutableYdbParameters());

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
        FillParameters(std::move(params), ev->Record.MutableRequest()->MutableYdbParameters());

        return SendKqpQueryStreamRequest(ev.Release(), target,
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

        FillParameters(std::move(params), ev->Record.MutableRequest()->MutableYdbParameters());

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

    TFuture<TQueryResult> ExecGenericQuery(const TString& cluster, const TString& query, TQueryData::TPtr params,
        const TAstQuerySettings& settings, const Ydb::Table::TransactionSettings& txSettings) override
    {
        YQL_ENSURE(cluster == Cluster);

        auto ev = MakeHolder<TEvKqp::TEvQueryRequest>();

        auto& request = *ev->Record.MutableRequest();
        request.SetCollectStats(settings.CollectStats);

        FillParameters(std::move(params), request.MutableYdbParameters());

        auto& txControl = *request.MutableTxControl();
        txControl.mutable_begin_tx()->CopyFrom(txSettings);
        txControl.set_commit_tx(true);

        return RunGenericQuery(query, NKikimrKqp::QUERY_ACTION_EXECUTE, std::move(ev));
    }

    TFuture<TQueryResult> ExplainGenericQuery(const TString& cluster, const TString& query) override {
        YQL_ENSURE(cluster == Cluster);

        return RunGenericQuery(query, NKikimrKqp::QUERY_ACTION_EXPLAIN, MakeHolder<TEvKqp::TEvQueryRequest>());
    }

    TFuture<TQueryResult> StreamExecGenericQuery(const TString& cluster, const TString& query,
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
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        ev->Record.MutableRequest()->SetQuery(query);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ev->Record.MutableRequest()->SetCollectStats(settings.CollectStats);

        FillParameters(std::move(params), ev->Record.MutableRequest()->MutableYdbParameters());

        auto& txControl = *ev->Record.MutableRequest()->MutableTxControl();
        txControl.mutable_begin_tx()->CopyFrom(txSettings);
        txControl.set_commit_tx(true);

        return SendKqpQueryStreamRequest(ev.Release(), target,
            [](TPromise<TQueryResult> promise, TResponse&& responseEv) {
            TQueryResult queryResult;
            queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
            KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
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

    TFuture<TQueryResult> SendKqpQueryStreamRequest(NKqp::TEvKqp::TEvQueryRequest* request,
        const NActors::TActorId& target, TKqpForwardStreamRequestHandler::TCallbackFunc callback)
    {
        auto promise = NewPromise<TQueryResult>();
        IActor* requestHandler = new TKqpForwardStreamRequestHandler(request, target, promise, callback);
        RegisterActor(requestHandler);

        return promise.GetFuture();
    }

    TFuture<TQueryResult> SendKqpGenericQueryRequest(TEvKqp::TEvQueryRequest* request, ui64 rowsLimit, ui64 sizeLimit,
        TKqpGenericQueryRequestHandler::TCallbackFunc callback)
    {
        auto promise = NewPromise<TQueryResult>();
        IActor* requestHandler = new TKqpGenericQueryRequestHandler(request, rowsLimit, sizeLimit,
            promise, callback);
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

    TFuture<TGenericResult> SendSchemeExecuterRequest(const TString&, const TMaybe<TString>& requestType, const std::shared_ptr<const NKikimr::NKqp::TKqpPhyTxHolder>& phyTx) override {
        auto promise = NewPromise<TGenericResult>();
        IActor* requestHandler = new TKqpSchemeExecuterRequestHandler(phyTx, QueryType, requestType, Database, UserToken, promise);
        RegisterActor(requestHandler);
        return promise.GetFuture();
    }

    template<typename TRpc>
    TFuture<TGenericResult> SendLocalRpcRequestNoResult(typename TRpc::TRequest&& proto, const TString& databse, const TString& token, const TMaybe<TString>& requestType = {}) {
        return NRpcService::DoLocalRpc<TRpc>(std::move(proto), databse, token, requestType, ActorSystem).Apply([](NThreading::TFuture<typename TRpc::TResponse> future) {

            return NThreading::MakeFuture(GenericResultFromSyncOperation(future.GetValue().operation()));
        });
    }

    TFuture<TQueryResult> RunGenericQuery(const TString& query, NKikimrKqp::EQueryAction action, THolder<TEvKqp::TEvQueryRequest> ev) {
        if (UserToken) {
            ev->Record.SetUserToken(UserToken->GetSerializedToken());
        }

        auto& request = *ev->Record.MutableRequest();
        request.SetDatabase(Database);
        request.SetAction(action);
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        request.SetQuery(query);
        request.SetKeepSession(false);

        return SendKqpGenericQueryRequest(ev.Release(), QueryServiceConfig.GetScriptResultRowsLimit(), QueryServiceConfig.GetScriptResultSizeLimit(),
            [] (TPromise<TQueryResult> promise, TEvKqp::TEvQueryResponse&& responseEv) {
                TQueryResult queryResult;
                queryResult.ProtobufArenaPtr.reset(new google::protobuf::Arena());
                KqpResponseToQueryResult(responseEv.Record.GetRef(), queryResult);
                promise.SetValue(std::move(queryResult));
            });
    }

    bool CheckCluster(const TString& cluster) {
        return cluster == Cluster;
    }

    bool GetDatabaseForLoginOperation(TString& database) {
        return NSchemeHelpers::SetDatabaseForLoginOperation(database, GetDomainLoginOnly(), GetDomainName(), GetDatabase());
    }

    bool GetPathPair(const TString& tableName, std::pair<TString, TString>& pathPair,
        TString& error, bool createDir)
    {
        return NSchemeHelpers::SplitTablePath(tableName, Database, pathPair, error, createDir);
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

    static void FillParameters(TQueryData::TPtr params, ::google::protobuf::Map<TBasicString<char>, Ydb::TypedValue>* output) {
        if (!params) {
            return;
        }

        auto& paramsMap = params->GetParamsProtobuf();
        output->insert(paramsMap.begin(), paramsMap.end());
    }

private:
    TString Cluster;
    const NKikimrKqp::EQueryType QueryType;
    TString Database;
    TActorSystem* ActorSystem;
    ui32 NodeId;
    TKqpRequestCounters::TPtr Counters;
    TAlignedPagePoolCounters AllocCounters;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::shared_ptr<IKqpTableMetadataLoader> MetadataLoader;
    NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
};

} // namespace

TIntrusivePtr<IKqpGateway> CreateKikimrIcGateway(const TString& cluster, NKikimrKqp::EQueryType queryType, const TString& database,
    std::shared_ptr<NYql::IKikimrGateway::IKqpTableMetadataLoader>&& metadataLoader, TActorSystem* actorSystem,
    ui32 nodeId, TKqpRequestCounters::TPtr counters, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig)
{
    return MakeIntrusive<TKikimrIcGateway>(cluster, queryType, database, std::move(metadataLoader), actorSystem, nodeId,
        counters, queryServiceConfig);
}

} // namespace NKqp
} // namespace NKikimr
