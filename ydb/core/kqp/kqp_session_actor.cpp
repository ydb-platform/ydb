#include "kqp_impl.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/common/kqp_transform.h>
#include <ydb/core/kqp/executer/kqp_executer.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/kqp/prepare/kqp_prepare.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/cputime.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/event_pb.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/string/printf.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

#define LOG_C(msg) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_E(msg) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_W(msg) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_N(msg) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_I(msg) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)
#define LOG_D(msg) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_SESSION, msg)



struct TKqpQueryState {
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    NKikimrKqp::TQueryRequest Request;
    TPreparedQueryConstPtr PreparedQuery;
    TKqpCompileResult::TConstPtr CompileResult;
    NKqpProto::TKqpStatsCompile CompileStats;
    TIntrusivePtr<TKikimrQueryContext> QueryCtx;
    TIntrusivePtr<TKqpTransactionContext> TxCtx;
    TVector<TVector<NKikimrMiniKQL::TResult>> TxResults;

    ui64 CurrentTx = 0;
    TString TraceId;

    TInstant StartTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;

    NKqpProto::TKqpStatsQuery Stats;

    TString UserToken;



    TString TxId; // User tx
    bool Commit = true;
};

struct TKqpCleanupCtx {
    ui64 AbortedTransactionsCount = 0;
    ui64 TransactionsToBeAborted = 0;
    std::vector<IKqpGateway::TExecPhysicalRequest> ExecuterAbortRequests;
    bool Final = false;
    TInstant Start;
};

EKikimrStatsMode GetStatsModeInt(const NKikimrKqp::TQueryRequest& queryRequest, EKikimrStatsMode minMode) {
    switch (queryRequest.GetStatsMode()) {
        case NYql::NDqProto::DQ_STATS_MODE_BASIC:
            return EKikimrStatsMode::Basic;
        case NYql::NDqProto::DQ_STATS_MODE_PROFILE:
            return EKikimrStatsMode::Profile;
        default:
            return std::max(EKikimrStatsMode::None, minMode);
    }
}

TKikimrQueryLimits GetQueryLimits(const TKqpWorkerSettings& settings) {
    const auto& queryLimitsProto = settings.Service.GetQueryLimits();
    const auto& phaseLimitsProto = queryLimitsProto.GetPhaseLimits();

    TKikimrQueryLimits queryLimits;
    auto& phaseLimits = queryLimits.PhaseLimits;
    phaseLimits.AffectedShardsLimit = phaseLimitsProto.GetAffectedShardsLimit();
    phaseLimits.ReadsetCountLimit = phaseLimitsProto.GetReadsetCountLimit();
    phaseLimits.ComputeNodeMemoryLimitBytes = phaseLimitsProto.GetComputeNodeMemoryLimitBytes();
    phaseLimits.TotalReadSizeLimitBytes = phaseLimitsProto.GetTotalReadSizeLimitBytes();

    return queryLimits;
}

class TKqpSessionActor : public TActorBootstrapped<TKqpSessionActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SESSION_ACTOR;
    }

    TIntrusivePtr<TKikimrConfiguration> CreateConfig(const TKqpSettings::TConstPtr& kqpSettings,
        const TKqpWorkerSettings& workerSettings)
    {
        auto cfg = MakeIntrusive<TKikimrConfiguration>();
        cfg->Init(kqpSettings->DefaultSettings.GetDefaultSettings(), workerSettings.Cluster,
                kqpSettings->Settings, false);
        return cfg;
    }

   TKqpSessionActor(const TActorId& owner, const TString& sessionId, const TKqpSettings::TConstPtr& kqpSettings,
            const TKqpWorkerSettings& workerSettings, TIntrusivePtr<TModuleResolverState> moduleResolverState,
            TIntrusivePtr<TKqpCounters> counters)
        : Owner(owner)
        , SessionId(sessionId)
        , Counters(counters)
        , Settings(workerSettings)
        , ModuleResolverState(moduleResolverState)
        , KqpSettings(kqpSettings)
        , Config(CreateConfig(kqpSettings, workerSettings))
        , ExplicitTransactions(*Config->_KqpMaxActiveTxPerSession.Get())
    {
        IdleDuration = TDuration::Seconds(*Config->_KqpSessionIdleTimeoutSec.Get());

        RequestCounters = MakeIntrusive<TKqpRequestCounters>();
        RequestCounters->Counters = Counters;
        RequestCounters->DbCounters = Settings.DbCounters;
        RequestCounters->TxProxyMon = MakeIntrusive<NTxProxy::TTxProxyMon>(AppData()->Counters);
    }

    void Bootstrap() {
        Counters->ReportSessionActorCreated(Settings.DbCounters);
        CreationTime = TInstant::Now();

        Become(&TKqpSessionActor::ReadyState);
        StartIdleTimer();
    }

    NYql::TKikimrQueryDeadlines GetQueryDeadlines(const NKikimrKqp::TQueryRequest& queryRequest) {
        NYql::TKikimrQueryDeadlines res;

        auto now = TAppData::TimeProvider->Now();
        if (queryRequest.GetCancelAfterMs()) {
            res.CancelAt = now + TDuration::MilliSeconds(queryRequest.GetCancelAfterMs());
        }

        auto timeoutMs = GetQueryTimeout(queryRequest.GetType(), queryRequest.GetTimeoutMs(), Settings.Service);
        res.TimeoutAt = now + timeoutMs;
        return res;
    }

    void MakeNewQueryState() {
        ++QueryId;
        YQL_ENSURE(!QueryState);
        QueryState = std::make_unique<TKqpQueryState>();
    }

    template<class T>
    void ForwardRequest(T& ev) {
        if (!WorkerId) {
            std::unique_ptr<IActor> workerActor(CreateKqpWorkerActor(Owner, SessionId, KqpSettings, Settings,
                    ModuleResolverState, Counters));
            WorkerId = RegisterWithSameMailbox(workerActor.release());
        }
        TlsActivationContext->Send(new IEventHandle(*WorkerId, SelfId(), ev->Release().Release(), ev->Flags, ev->Cookie,
                    nullptr, std::move(ev->TraceId)));
    }

    void ForwardResponse(TEvKqp::TEvQueryResponse::TPtr& ev) {
        TlsActivationContext->Send(new IEventHandle(Owner, SelfId(), ev->Release().Release(), ev->Flags, ev->Cookie,
                                    nullptr, std::move(ev->TraceId)));
        Cleanup();
    }

     TIntrusivePtr<TKqpTransactionContext> FindTransaction(const TString& id) {
         auto it = ExplicitTransactions.Find(id);
         if (it != ExplicitTransactions.End()) {
             auto& value = it.Value();
             value->Touch();
             return value;
         }

         return {};
     }

    void RemoveTransaction(const TString& txId) {
        auto it = ExplicitTransactions.FindWithoutPromote(txId);
        if (it != ExplicitTransactions.End()) {
            ExplicitTransactions.Erase(it);
        }
    }

    void CommitTx() {
        auto& queryRequest = QueryState->Request;

        YQL_ENSURE(queryRequest.HasTxControl());

        auto& txControl = queryRequest.GetTxControl();
        YQL_ENSURE(txControl.tx_selector_case() == Ydb::Table::TransactionControl::kTxId, "Can't commit transaction - "
                << " there is no TxId in Query's TxControl, queryRequest: " << queryRequest.DebugString());

        LOG_D("queryRequest TxControl: " << txControl.DebugString());

        QueryState->Commit = txControl.commit_tx();

        const auto& txId = txControl.tx_id();
        auto txCtx = FindTransaction(txId);
        YQL_ENSURE(txCtx, "Can't find txId: " << txId);
        QueryState->TxCtx = std::move(txCtx);
        QueryState->TxId = txId;
        ExecutePhyTx(/*query*/ nullptr, /*tx*/ nullptr, /*commit*/ true);

        Become(&TKqpSessionActor::ExecuteState);
    }

    static bool IsQueryTypeSupported(NKikimrKqp::EQueryType type) {
        switch (type) {
            case NKikimrKqp::QUERY_TYPE_SQL_DML:
            case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
                return true;

            // should not be compiled. TODO: forward to request executer
            // not supported yet
            case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            case NKikimrKqp::QUERY_TYPE_AST_SCAN:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            case NKikimrKqp::QUERY_TYPE_AST_DML:
            case NKikimrKqp::QUERY_TYPE_UNDEFINED:
                return false;
        }
    }

    void HandleReady(TEvKqp::TEvQueryRequest::TPtr& ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& event = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(event.GetTraceId(), event.GetRequest().GetSessionId());
        YQL_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        MakeNewQueryState();
        QueryState->Request.Swap(event.MutableRequest());
        auto& queryRequest = QueryState->Request;

        YQL_ENSURE(queryRequest.GetDatabase() == Settings.Database,
                "Wrong database, expected:" << Settings.Database << ", got: " << queryRequest.GetDatabase());

        YQL_ENSURE(queryRequest.HasAction());
        auto action = queryRequest.GetAction();

        LOG_D(requestInfo << "Received request,"
            << " proxyRequestId: " << proxyRequestId
            << " query: " << (queryRequest.HasQuery() ? queryRequest.GetQuery().Quote() : "")
            << " prepared: " << queryRequest.HasPreparedQuery()
            << " tx_control: " << queryRequest.HasTxControl()
            << " action: " << action
        );

        QueryState->Sender = ev->Sender;
        QueryState->ProxyRequestId = proxyRequestId;
        QueryState->TraceId = requestInfo.GetTraceId();
        QueryState->StartTime = TInstant::Now();
        QueryState->UserToken = event.GetUserToken();
        QueryState->QueryDeadlines = GetQueryDeadlines(queryRequest);

        switch (action) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
            case NKikimrKqp::QUERY_ACTION_PREPARE:
            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED: {
                YQL_ENSURE(queryRequest.HasType());
                auto type = queryRequest.GetType();
                YQL_ENSURE(type != NKikimrKqp::QUERY_TYPE_UNDEFINED, "query type is undefined");

                if (!IsQueryTypeSupported(type)) {
                    event.MutableRequest()->Swap(&QueryState->Request);
                    ForwardRequest(ev);
                    return;
                }
                break;
            }

            case NKikimrKqp::QUERY_ACTION_BEGIN_TX:
            case NKikimrKqp::QUERY_ACTION_ROLLBACK_TX:
                YQL_ENSURE(false, "BEGIN_TX and ROLLBACK_TX is not supported yet");
                return;
            case NKikimrKqp::QUERY_ACTION_COMMIT_TX:
                CommitTx();
                return;
            // not supported yet
            case NKikimrKqp::QUERY_ACTION_EXPLAIN:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
            case NKikimrKqp::QUERY_ACTION_PARSE:
                event.MutableRequest()->Swap(&QueryState->Request);
                ForwardRequest(ev);
                return;
        }

        StopIdleTimer();

        CompileQuery();
    }

    void CompileQuery() {
        YQL_ENSURE(QueryState);
        auto& queryRequest = QueryState->Request;

        TMaybe<TKqpQueryId> query;
        TMaybe<TString> uid;

        bool keepInCache = false;
        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery());
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            case NKikimrKqp::QUERY_ACTION_PREPARE:
                query = TKqpQueryId(Settings.Cluster, Settings.Database, queryRequest.GetQuery());
                keepInCache = true;
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED:
                uid = queryRequest.GetPreparedQuery();
                keepInCache = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            default:
                YQL_ENSURE(false);
        }

        auto compileDeadline = QueryState->QueryDeadlines.TimeoutAt;
        if (QueryState->QueryDeadlines.CancelAt) {
            compileDeadline = Min(compileDeadline, QueryState->QueryDeadlines.CancelAt);
        }

        auto compileRequestActor = CreateKqpCompileRequestActor(SelfId(), QueryState->UserToken, uid,
            std::move(query), keepInCache, compileDeadline, Settings.DbCounters);
        TlsActivationContext->ExecutorThread.RegisterActor(compileRequestActor);
        Become(&TKqpSessionActor::CompileState);
    }

    void HandleCompile(TEvKqp::TEvCompileResponse::TPtr& ev) {
        auto compileResult = ev->Get()->CompileResult;

        YQL_ENSURE(compileResult);
        YQL_ENSURE(QueryState);

        if (compileResult->Status != Ydb::StatusIds::SUCCESS) {
            if (ReplyQueryCompileError(compileResult)) {
                Cleanup();
                StartIdleTimer();
                Become(&TThis::ReadyState);
            } else {
                FinalCleanup();
            }
            return;
        }

        YQL_ENSURE(compileResult->PreparedQuery);
        const ui32 compiledVersion = compileResult->PreparedQuery->GetVersion();
        YQL_ENSURE(compiledVersion == NKikimrKqp::TPreparedQuery::VERSION_PHYSICAL_V1,
                "Invalid compiled version: " << compiledVersion);

        auto& queryRequest = QueryState->Request;
        if (queryRequest.GetAction() == NKikimrKqp::QUERY_ACTION_PREPARE) {
            if (ReplyPrepareResult(compileResult)) {
                Cleanup();
                StartIdleTimer();
                Become(&TThis::ReadyState);
            } else {
                FinalCleanup();
            }
            return;
        }

        QueryState->CompileResult = compileResult;
        QueryState->CompileStats.Swap(&ev->Get()->Stats);
        QueryState->PreparedQuery = compileResult->PreparedQuery;
        QueryState->Request.SetQuery(QueryState->PreparedQuery->GetText());

        PrepareQueryContext();

        Become(&TKqpSessionActor::ExecuteState);
        // Can reply inside (in case of deferred-only transactions) and become ReadyState
        ExecuteOrDeferr();
    }

     void SetIsolationLevel(const Ydb::Table::TransactionSettings& settings) {
         YQL_ENSURE(QueryState->TxCtx);
         auto& txCtx = QueryState->TxCtx;
         switch (settings.tx_mode_case()) {
             case Ydb::Table::TransactionSettings::kSerializableReadWrite:
                 txCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                 txCtx->Readonly = false;
                 break;

             case Ydb::Table::TransactionSettings::kOnlineReadOnly:
                 txCtx->EffectiveIsolationLevel = settings.online_read_only().allow_inconsistent_reads()
                     ? NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED
                     : NKikimrKqp::ISOLATION_LEVEL_READ_COMMITTED;
                 txCtx->Readonly = true;
                 break;

             case Ydb::Table::TransactionSettings::kStaleReadOnly:
                 txCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_READ_STALE;
                 txCtx->Readonly = true;
                 break;
             case Ydb::Table::TransactionSettings::TX_MODE_NOT_SET:
                 YQL_ENSURE(false, "tx_mode not set, settings: " << settings);
                 break;
         };
    }

    void RemoveOldTransactions() {
        if (ExplicitTransactions.Size() == *Config->_KqpMaxActiveTxPerSession.Get()) {
            auto it = ExplicitTransactions.FindOldest();
            auto idleDuration = TInstant::Now() - it.Value()->LastAccessTime;
            if (idleDuration.Seconds() >= *Config->_KqpTxIdleTimeoutSec.Get()) {
                it.Value()->Invalidate();
                AbortedTransactions.emplace_back(std::move(it.Value()));
                ExplicitTransactions.Erase(it);
            } else {
                YQL_ENSURE(false, "Too many transactions, current active: " << ExplicitTransactions.Size()
                        << " MaxTxPerSession: " << *Config->_KqpMaxActiveTxPerSession.Get());
            }
        }
    }

    void CreateNewTx() {
        RemoveOldTransactions();
        auto success = ExplicitTransactions.Insert(std::make_pair(QueryState->TxId, QueryState->TxCtx));
        YQL_ENSURE(success);
    }

    void PrepareQueryContext() {
        YQL_ENSURE(QueryState);
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& queryRequest = QueryState->Request;

        YQL_ENSURE(queryRequest.HasTxControl());
        auto& txControl = queryRequest.GetTxControl();

        QueryState->Commit = txControl.commit_tx();
        switch (txControl.tx_selector_case()) {
            case Ydb::Table::TransactionControl::kTxId: {
                TString txId = txControl.tx_id();
                auto it = ExplicitTransactions.Find(txId);
                YQL_ENSURE(it != ExplicitTransactions.End());
                QueryState->TxCtx = *it;
                QueryState->TxId = txId;
                QueryState->TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
                break;
            }
            case Ydb::Table::TransactionControl::kBeginTx: {
                QueryState->TxId = CreateGuidAsString();
                QueryState->TxCtx = MakeIntrusive<TKqpTransactionContext>(false);
                SetIsolationLevel(txControl.begin_tx());
                CreateNewTx();
                break;
           }
           case Ydb::Table::TransactionControl::TX_SELECTOR_NOT_SET:
               YQL_ENSURE(false);
        }

        QueryState->QueryCtx = MakeIntrusive<TKikimrQueryContext>();
        QueryState->QueryCtx->TimeProvider = TAppData::TimeProvider;
        QueryState->QueryCtx->RandomProvider = TAppData::RandomProvider;

        auto action = queryRequest.GetAction();
        auto queryType = queryRequest.GetType();

        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            YQL_ENSURE(queryType == NKikimrKqp::QUERY_TYPE_SQL_DML);
            queryType = NKikimrKqp::QUERY_TYPE_PREPARED_DML;
            action = NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED;
        }

        YQL_ENSURE(action == NKikimrKqp::QUERY_ACTION_EXECUTE_PREPARED,
                "Unexpected query action, expected: QUERY_ACTION_EXECUTE_PREPARED, got: " << action);
        YQL_ENSURE(queryType == NKikimrKqp::QUERY_TYPE_PREPARED_DML,
                "Unexpected query type, expected: QUERY_TYPE_PREPARED_DML, got: " << queryType);

        ParseParameters(std::move(*QueryState->Request.MutableParameters()), QueryState->QueryCtx->Parameters);
    }

    static void ParseParameters(NKikimrMiniKQL::TParams&& parameters, TKikimrParamsMap& map) {
        if (!parameters.HasType()) {
            return;
        }

        YQL_ENSURE(parameters.GetType().GetKind() == NKikimrMiniKQL::Struct, "Expected struct as query parameters type");
        auto& structType = *parameters.MutableType()->MutableStruct();

        for (ui32 i = 0; i < structType.MemberSize(); ++i) {
            const auto& memberName = structType.GetMember(i).GetName();
            YQL_ENSURE(i < parameters.GetValue().StructSize(), "Missing value for parameter: " << memberName);

            NKikimrMiniKQL::TParams param;
            param.MutableType()->Swap(structType.MutableMember(i)->MutableType());
            param.MutableValue()->Swap(parameters.MutableValue()->MutableStruct(i));

            auto result = map.emplace(memberName, std::move(param));
            YQL_ENSURE(result.second, "Duplicate parameter: " << memberName);
        }
    }

    bool ReplyPrepareResult(const TKqpCompileResult::TConstPtr& compileResult) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);

        auto ru = NRuCalc::CpuTimeToUnit(TDuration::MicroSeconds(QueryState->CompileStats.GetCpuTimeUs()));
        responseEv->Record.GetRef().SetConsumedRu(ru);

        return Reply(std::move(responseEv));
    }

    IKqpGateway::TExecPhysicalRequest PreparePhysicalRequest(TKqpQueryState *queryState) {
        IKqpGateway::TExecPhysicalRequest request;

        auto now = TAppData::TimeProvider->Now();
        if (queryState) {
            request.Timeout = queryState->QueryDeadlines.TimeoutAt - now;
            if (auto cancelAt = queryState->QueryDeadlines.CancelAt) {
                request.CancelAfter = cancelAt - now;
            }

            auto& queryRequest = queryState->Request;
            EKikimrStatsMode statsMode = GetStatsModeInt(queryRequest, EKikimrStatsMode::Basic);
            request.StatsMode = GetStatsMode(statsMode);

            request.Snapshot = queryState->TxCtx->GetSnapshot();
            request.IsolationLevel = *queryState->TxCtx->EffectiveIsolationLevel;
        } else {
            request.IsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
        }

        const auto& limits = GetQueryLimits(Settings);
        request.MaxAffectedShards = limits.PhaseLimits.AffectedShardsLimit;
        request.TotalReadSizeLimitBytes = limits.PhaseLimits.TotalReadSizeLimitBytes;
        request.MkqlMemoryLimit = limits.PhaseLimits.ComputeNodeMemoryLimitBytes;

        return request;
    }

    NKikimrMiniKQL::TParams* ValidateParameter(const TString& name, const NKikimrMiniKQL::TType& type) {
        Y_VERIFY(QueryState->QueryCtx, "for testing purpose");
        auto parameter = QueryState->QueryCtx->Parameters.FindPtr(name);
        if (!parameter) {
            if (type.GetKind() == NKikimrMiniKQL::ETypeKind::Optional) {
                auto& newParameter = QueryState->QueryCtx->Parameters[name];
                newParameter.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Optional);
                *newParameter.MutableType()->MutableOptional()->MutableItem() = type.GetOptional().GetItem();

                return &newParameter;
            }

            YQL_ENSURE(false, "Missing value for parameter: " << name);
            return nullptr;
        }

        YQL_ENSURE(IsSameType(parameter->GetType(), type), "Parameter " << name
                << " type mismatch, expected: " << type << ", actual: " << parameter->GetType());

        return parameter;
    }

    TKqpParamsMap PrepareParameters(const NKqpProto::TKqpPhyTx& tx) {
        for (const auto& paramDesc : QueryState->PreparedQuery->GetParameters()) {
            ValidateParameter(paramDesc.GetName(), paramDesc.GetType());
        }

        TKqpParamsMap paramsMap;

        for (const auto& paramBinding : tx.GetParamBindings()) {

            auto it = paramsMap.Values.emplace(paramBinding.GetName(),
                    *GetParamValue(/*ensure*/ true, *QueryState->QueryCtx, QueryState->TxResults, paramBinding));
            YQL_ENSURE(it.second);
        }

        return paramsMap;
    }

    bool ShouldAcquireLocks(const NKqpProto::TKqpPhyQuery& query) {
        auto& txCtx = *QueryState->TxCtx;

        if (*txCtx.EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
            return false;
        }

        if (txCtx.Locks.Broken()) {
            return false;  // Do not acquire locks after first lock issue
        }

        if (!txCtx.DeferredEffects.Empty()) {
            return true; // Acquire locks in read write tx
        }

        for (auto& tx : query.GetTransactions()) {
            if (tx.GetHasEffects()) {
                return true; // Acquire locks in read write tx
            }
        }

        if (!QueryState->Commit) {
            return true; // Is not a commit tx
        }

        if (txCtx.GetSnapshot().IsValid()) {
            return false; // It is a read only tx with snapshot, no need to acquire locks
        }

        return true;
    }

    static TKqpParamsMap GetParamsRefMap(const TParamValueMap& map) {
        TKqpParamsMap paramsMap;
        for (auto& [k, v] : map) {
            auto res = paramsMap.Values.emplace(k, NYql::NDq::TMkqlValueRef(v));
            YQL_ENSURE(res.second);
        }

        return paramsMap;
    }

    TParamValueMap CreateKqpValueMap(const NKqpProto::TKqpPhyTx& tx) {
        TParamValueMap paramsMap;
        for (const auto& paramBinding : tx.GetParamBindings()) {
            auto paramValueRef = *GetParamValue(/*ensure*/ true, *QueryState->QueryCtx, QueryState->TxResults,
                    paramBinding);

            NKikimrMiniKQL::TParams param;
            param.MutableType()->CopyFrom(paramValueRef.GetType());
            param.MutableValue()->CopyFrom(paramValueRef.GetValue());

            auto [it, success] = paramsMap.emplace(paramBinding.GetName(), std::move(param));
            YQL_ENSURE(success);
        }
        return paramsMap;
    }


    void ExecuteOrDeferr() {
        auto& txCtx = *QueryState->TxCtx;
        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
        if (!txCtx.DeferredEffects.Empty() && txCtx.Locks.Broken()) {
            ReplyProcessError(requestInfo, Ydb::StatusIds::ABORTED, "Error while AddDeferredEffect");
            return;
        }

        const NKqpProto::TKqpPhyQuery& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
        YQL_ENSURE(QueryState->CurrentTx < phyQuery.TransactionsSize());

        auto tx = std::shared_ptr<const NKqpProto::TKqpPhyTx>(QueryState->PreparedQuery,
                &phyQuery.GetTransactions(QueryState->CurrentTx));

        while (tx->GetHasEffects()) {
            if (!txCtx.AddDeferredEffect(tx, CreateKqpValueMap(*tx))) {
                ReplyProcessError(requestInfo, Ydb::StatusIds::BAD_REQUEST,
                        "Failed to mix queries with old- and new- engines");
                return;
            }
            if (QueryState->CurrentTx + 1 < phyQuery.TransactionsSize()) {
                ++QueryState->CurrentTx;
                tx = std::shared_ptr<const NKqpProto::TKqpPhyTx>(QueryState->PreparedQuery,
                        &phyQuery.GetTransactions(QueryState->CurrentTx));
            } else {
                tx = nullptr;
                break;
            }
        }

        bool commit = QueryState->Commit && QueryState->CurrentTx == phyQuery.TransactionsSize() - 1;
        if (tx || commit) {
            ExecutePhyTx(&phyQuery, std::move(tx), commit);
            ++QueryState->CurrentTx;
        } else {
            ReplySuccess();
        }
    }

    void ExecutePhyTx(const NKqpProto::TKqpPhyQuery* query, std::shared_ptr<const NKqpProto::TKqpPhyTx> tx, bool commit) {
        auto& txCtx = *QueryState->TxCtx;
        auto request = PreparePhysicalRequest(QueryState.get());
        LOG_D("ExecutePhyTx, tx: " << (void*)tx.get() << " commit: " << commit
                << " txCtx.DeferredEffects.size(): " << txCtx.DeferredEffects.Size());

        // TODO Handle timeouts -- request.Timeout, request.CancelAfter

        if (tx) {
            switch (tx->GetType()) {
                case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
                case NKqpProto::TKqpPhyTx::TYPE_DATA:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected physical tx type in data query: " << (ui32)tx->GetType());
            }

            request.Transactions.emplace_back(tx, PrepareParameters(*tx));
        } else {
            YQL_ENSURE(commit);
            if (txCtx.DeferredEffects.Empty() && !txCtx.Locks.HasLocks()) {
                ReplySuccess();
                return;
            }
        }

        if (commit) {
            Y_VERIFY_DEBUG(txCtx.DeferredEffects.Empty() || !txCtx.Locks.Broken());

            for (const auto& effect : txCtx.DeferredEffects) {
                YQL_ENSURE(!effect.Node);
                YQL_ENSURE(effect.PhysicalTx->GetType() == NKqpProto::TKqpPhyTx::TYPE_DATA);
                request.Transactions.emplace_back(effect.PhysicalTx, GetParamsRefMap(effect.Params));

                LOG_D("TExecPhysicalRequest, add DeferredEffect to Transaction,"
                       << " current Transactions.size(): " << request.Transactions.size());
            }

            if (!txCtx.DeferredEffects.Empty()) {
                request.PerShardKeysSizeLimitBytes = Config->_CommitPerShardKeysSizeLimitBytes.Get().GetRef();
            }

            if (txCtx.Locks.HasLocks()) {
                request.ValidateLocks = !(txCtx.GetSnapshot().IsValid() && txCtx.DeferredEffects.Empty());
                request.EraseLocks = true;
                LOG_D("TExecPhysicalRequest, tx has locks, ValidateLocks: " << request.ValidateLocks
                        << " EraseLocks: " << request.EraseLocks);

                for (auto& [lockId, lock] : txCtx.Locks.LocksMap) {
                    request.Locks.emplace_back(lock.GetValueRef(txCtx.Locks.LockType));
                }
            }
        } else if (ShouldAcquireLocks(*query)) {
            request.AcquireLocksTxId = txCtx.Locks.GetLockTxId();
        }

        SendToExecuter(std::move(request));
    }

    void SendToExecuter(IKqpGateway::TExecPhysicalRequest&& request) {
        auto executerActor = CreateKqpExecuter(std::move(request), Settings.Database,
                (QueryState && QueryState->UserToken) ? TMaybe<TString>(QueryState->UserToken) : Nothing(),
                RequestCounters);
        auto executerId = TlsActivationContext->ExecutorThread.RegisterActor(executerActor);
        LOG_D("Created new KQP executer: " << executerId);

        auto ev = std::make_unique<TEvTxUserProxy::TEvProposeKqpTransaction>(executerId);
        Send(MakeTxProxyID(), ev.release());
    }


    void HandleNoop(TEvKqpExecuter::TEvExecuterProgress::TPtr& /*ev*/) {
    }

    void HandleExecute(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto* response = ev->Get()->Record.MutableResponse();
        LOG_D("TEvTxResponse, CurrentTx: " << QueryState->CurrentTx << " response: " << response->DebugString());

        if (response->GetStatus() != Ydb::StatusIds::SUCCESS) {
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            ReplyProcessError(requestInfo, response->GetStatus(), "", response->MutableIssues());
            return;
        }

        // save tx results
        auto& txResult = *response->MutableResult();
        TVector<NKikimrMiniKQL::TResult> txResults;
        txResults.resize(txResult.ResultsSize());
        for (ui32 i = 0; i < txResult.ResultsSize(); ++i) {
            txResults[i].Swap(txResult.MutableResults(i));
        }

        QueryState->TxResults.emplace_back(std::move(txResults));

        if (txResult.HasStats()) {
            auto* exec = QueryState->Stats.AddExecutions();
            exec->Swap(txResult.MutableStats());
        }

        // locks merge
        if (txResult.HasLocks()) {
            auto& txCtx = *QueryState->TxCtx;
            const auto& locks = txResult.GetLocks();
            auto [success, issues] = MergeLocks(locks.GetType(), locks.GetValue(), txCtx);
            if (!success) {
                auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
                google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> message;
                for (const auto& i : issues) {
                    IssueToMessage(i, message.Add());
                }
                ReplyProcessError(requestInfo, Ydb::StatusIds::ABORTED,  "Error while locks merge", &message);
                return;
            }
        }

        if (QueryState->PreparedQuery &&
                QueryState->CurrentTx < QueryState->PreparedQuery->GetPhysicalQuery().TransactionsSize()) {
            ExecuteOrDeferr();
        } else {
            ReplySuccess();
        }
    }

    void FillStats(NKikimrKqp::TQueryResponse* response) {
        // TODO
        // Compile status
        // Execution stats (duration, comsumed RU)

        auto* resStats = response->MutableQueryStats();
        resStats->Swap(&QueryState->Stats);

        resStats->SetDurationUs((TInstant::Now() - QueryState->StartTime).MicroSeconds());
        //resStats->SetWorkerCpuTimeUs();
        resStats->MutableCompilation()->Swap(&QueryState->CompileStats);
    }

    void FillTxInfo(NKikimrKqp::TQueryResponse* response) {
        Y_VERIFY(QueryState);
        if (QueryState->TxId) {
            response->MutableTxMeta()->set_id(QueryState->TxId);
        }

        if (QueryState->Commit) {
            RemoveTransaction(QueryState->TxId);
        }

        if (QueryState->TxCtx) {
            auto txInfo = QueryState->TxCtx->GetInfo();
            LOG_N("txInfo"
                << " Status: " << txInfo.Status
                << " Kind: " << txInfo.Kind
                << " TotalDuration: " << txInfo.TotalDuration.SecondsFloat()*1e3
                << " ServerDuration: " << txInfo.ServerDuration.SecondsFloat()*1e3
                << " QueriesCount: " << txInfo.QueriesCount);
            Counters->ReportTransaction(Settings.DbCounters, txInfo);
        }
    }

    void ReplySuccess() {
        // return result
        auto resEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        std::shared_ptr<google::protobuf::Arena> arena(new google::protobuf::Arena());
        resEv->Record.Realloc(arena);

        auto *response = resEv->Record.GetRef().MutableResponse();

        FillStats(response);

        if (QueryState->Commit) {
            ResetTxState();
        }

        FillTxInfo(response);

        bool replyQueryId = false;
        bool replyQueryParameters = false;
        auto& queryRequest = QueryState->Request;
        switch (queryRequest.GetAction()) {
            case NKikimrKqp::QUERY_ACTION_PREPARE:
                replyQueryId = true;
                replyQueryParameters = true;
                break;

            case NKikimrKqp::QUERY_ACTION_EXECUTE:
                replyQueryParameters = replyQueryId = queryRequest.GetQueryCachePolicy().keep_in_cache();
                break;

            case NKikimrKqp::QUERY_ACTION_PARSE:
            case NKikimrKqp::QUERY_ACTION_VALIDATE:
                replyQueryParameters = true;
                break;

            default:
                break;
        }

        if (replyQueryParameters) {
            YQL_ENSURE(QueryState->PreparedQuery);
            response->MutableQueryParameters()->CopyFrom(
                    QueryState->PreparedQuery->GetParameters());
        }

        if (replyQueryId) {
            TString queryId;
            if (QueryState->CompileResult) {
                queryId = QueryState->CompileResult->Uid;
            } else {
                YQL_ENSURE(!Settings.LongSession);
                Y_PROTOBUF_SUPPRESS_NODISCARD QueryState->PreparedQuery->SerializeToString(&queryId);
            }

            response->SetPreparedQuery(queryId);
        }

        if (QueryState->PreparedQuery) {
            auto& phyQuery = QueryState->PreparedQuery->GetPhysicalQuery();
            for (auto& rb : phyQuery.GetResultBindings()) {
                auto txIndex = rb.GetTxResultBinding().GetTxIndex();
                auto resultIndex = rb.GetTxResultBinding().GetResultIndex();

                auto& txResults = QueryState->TxResults;
                YQL_ENSURE(txIndex < txResults.size());
                YQL_ENSURE(resultIndex < txResults[txIndex].size());

                IDataProvider::TFillSettings fillSettings;
                //TODO: shoud it be taken from PreparedQuery->GetResults().GetRowsLimit() ?
                fillSettings.RowsLimitPerWrite = Config->_ResultRowsLimit.Get().GetRef();
                auto* protoRes = KikimrResultToProto(txResults[txIndex][resultIndex], {}, fillSettings, arena.get());
                response->AddResults()->Swap(protoRes);
            }
        }

        resEv->Record.GetRef().SetYdbStatus(Ydb::StatusIds::SUCCESS);
        Reply(std::move(resEv));

        Cleanup();
    }

    bool ReplyQueryCompileError(const TKqpCompileResult::TConstPtr& compileResult) {
        auto responseEv = std::make_unique<TEvKqp::TEvQueryResponse>();
        FillCompileStatus(compileResult, responseEv->Record);
        responseEv->Record.GetRef().SetConsumedRu(1);
        return Reply(std::move(responseEv));
    }

    bool Reply(std::unique_ptr<TEvKqp::TEvQueryResponse> responseEv) {
        YQL_ENSURE(QueryState);

        auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);

        auto& queryRequest = QueryState->Request;
        auto queryDuration = TInstant::Now() - QueryState->StartTime;
        Y_VERIFY(Counters);
        Counters->ReportQueryLatency(Settings.DbCounters, queryRequest.GetAction(), queryDuration);

        auto& record = responseEv->Record.GetRef();
        auto& response = *record.MutableResponse();
        const auto& status = record.GetYdbStatus();

        response.SetSessionId(SessionId);

        Send(QueryState->Sender, responseEv.release(), 0, QueryState->ProxyRequestId);
        LOG_D(requestInfo << "Sent query response back to proxy, proxyRequestId: " << QueryState->ProxyRequestId
            << ", proxyId: " << QueryState->Sender.ToString());

        if (status == Ydb::StatusIds::INTERNAL_ERROR) {
            LOG_D(requestInfo << "SessionActor destroyed due to internal error");
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
            return false;
        }
        if (status == Ydb::StatusIds::BAD_SESSION) {
            LOG_D(requestInfo << "SessionActor destroyed due to session error");
            Counters->ReportSessionActorClosedError(Settings.DbCounters);
            return false;
        }

        return true;
    }

    void FillCompileStatus(const TKqpCompileResult::TConstPtr& compileResult,
        TEvKqp::TProtoArenaHolder<NKikimrKqp::TEvQueryResponse>& record)
    {
        auto& ev = record.GetRef();

        ev.SetYdbStatus(compileResult->Status);

        auto& response = *ev.MutableResponse();
        AddQueryIssues(response, compileResult->Issues);

        if (compileResult->Status == Ydb::StatusIds::SUCCESS) {
            response.SetPreparedQuery(compileResult->Uid);

            auto& preparedQuery = compileResult->PreparedQuery;
            response.MutableQueryParameters()->CopyFrom(preparedQuery->GetParameters());

            if (preparedQuery->KqlsSize() > 0) {
                response.SetQueryAst(preparedQuery->GetKqls(0).GetAst());
                response.SetQueryPlan(preparedQuery->GetKqls(0).GetPlan());
            }
        }
    }

    void Handle(TEvKqp::TEvPingSessionRequest::TPtr& ev) {
        ui64 proxyRequestId = ev->Cookie;
        auto& evRecord = ev->Get()->Record;
        auto requestInfo = TKqpRequestInfo(evRecord.GetTraceId(), evRecord.GetRequest().GetSessionId());
        YQL_ENSURE(requestInfo.GetSessionId() == SessionId,
                "Invalid session, expected: " << SessionId << ", got: " << requestInfo.GetSessionId());

        auto result = std::make_unique<TEvKqp::TEvPingSessionResponse>();
        auto& record = result->Record;
        record.SetStatus(Ydb::StatusIds::SUCCESS);
        auto sessionStatus = CurrentStateFunc() == &TThis::ReadyState
            ? Ydb::Table::KeepAliveResult::SESSION_STATUS_READY
            : Ydb::Table::KeepAliveResult::SESSION_STATUS_BUSY;
        record.MutableResponse()->SetSessionStatus(sessionStatus);
        StartIdleTimer();

        Send(ev->Sender, result.release(), 0, proxyRequestId);
    }

    void Handle(TEvKqp::TEvCloseSessionRequest::TPtr&) {
        if (QueryState) {
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            ReplyProcessError(requestInfo, Ydb::StatusIds::SESSION_EXPIRED,
                    "Request cancelled due to explicit session close request");
            // TODO Remove cleanup from ReplyProcessError since now it is possible
            // for TxCtx in ExplicitTransactions to leak
        }
        if (CleanupCtx) {
            CleanupCtx->Final = true;
        } else {
            FinalCleanup();
        }
    }

    void StartIdleTimer() {
        StopIdleTimer();

        ++IdleTimerId;
        IdleTimerActorId = CreateLongTimer(TlsActivationContext->AsActorContext(), IdleDuration,
                new IEventHandle(SelfId(), SelfId(), new TEvKqp::TEvIdleTimeout(IdleTimerId)));
        LOG_D("Created long timer for idle timeout, timer id: " << IdleTimerId
                << ", duration: " << IdleDuration << ", actor: " << IdleTimerActorId);
    }

    void StopIdleTimer() {
        if (IdleTimerActorId) {
            LOG_D("Destroying long timer actor for idle timout: " << IdleTimerActorId);
            Send(IdleTimerActorId, new TEvents::TEvPoisonPill());
        }
        IdleTimerActorId = TActorId();
    }

    void Handle(TEvKqp::TEvIdleTimeout::TPtr& ev) {
        auto timerId = ev->Get()->TimerId;
        LOG_D("Received TEvIdleTimeout in ready state, timer id: "
            << timerId << ", sender: " << ev->Sender);

        if (timerId == IdleTimerId) {
            LOG_N(TKqpRequestInfo("", SessionId) << "SessionActor idle timeout, worker destroyed");
            Counters->ReportSessionActorClosedIdle(Settings.DbCounters);
            FinalCleanup();
        }
    }

    void RollbackTx(TIntrusivePtr<TKqpTransactionContext> txCtx) {
        auto request = PreparePhysicalRequest(nullptr);

        request.EraseLocks = true;
        request.ValidateLocks = false;

        // Should tx with empty LocksMap be aborted?
        for (auto& [lockId, lock] : txCtx->Locks.LocksMap) {
            request.Locks.emplace_back(lock.GetValueRef(txCtx->Locks.LockType));
        }
        SendToExecuter(std::move(request));
    }

    void ResetTxState() {
        if (QueryState->TxCtx) {
            QueryState->TxCtx->ClearDeferredEffects();
            QueryState->TxCtx->Locks.Clear();
            QueryState->TxCtx->Finish();
        }
    }

    void FinalCleanup() {
        Cleanup(true);
    }

    void HandleNoop(TEvKqp::TEvIdleTimeout::TPtr&) {
    }

    void HandleCleanup(TEvKqpExecuter::TEvTxResponse::TPtr& ev) {
        auto& response = ev->Get()->Record.GetResponse();
        // TODO accumulate issues and compute overall status
        YQL_ENSURE(response.GetStatus() == Ydb::StatusIds::SUCCESS);
        YQL_ENSURE(CleanupCtx);
        ++CleanupCtx->AbortedTransactionsCount;
        if (CleanupCtx->AbortedTransactionsCount == CleanupCtx->TransactionsToBeAborted) {
            EndCleanup(CleanupCtx->Final);
        }
    }

    void EndCleanup(bool isFinal) {
        if (isFinal) {
            auto lifeSpan = TInstant::Now() - CreationTime;
            Counters->ReportSessionActorFinished(Settings.DbCounters, lifeSpan);
            Counters->ReportQueriesPerSessionActor(Settings.DbCounters, QueryId);

            auto closeEv = std::make_unique<TEvKqp::TEvCloseSessionResponse>();
            closeEv->Record.SetStatus(Ydb::StatusIds::SUCCESS);
            closeEv->Record.MutableResponse()->SetSessionId(SessionId);
            closeEv->Record.MutableResponse()->SetClosed(true);
            Send(Owner, closeEv.release());

            PassAway();
        } else {
            CleanupCtx.reset();
            StartIdleTimer();
            Become(&TKqpSessionActor::ReadyState);
        }
    }

    void Cleanup(bool isFinal = false) {
        if (isFinal) {
            for (auto it = ExplicitTransactions.Begin(); it != ExplicitTransactions.End(); ++it) {
                it.Value()->Invalidate();
                AbortedTransactions.emplace_back(std::move(it.Value()));
            }
            ExplicitTransactions.Clear();
        }

        if (AbortedTransactions.size()) {
            YQL_ENSURE(!CleanupCtx);
            CleanupCtx.reset(new TKqpCleanupCtx);
            CleanupCtx->Final = isFinal;
            CleanupCtx->AbortedTransactionsCount = 0;
            CleanupCtx->TransactionsToBeAborted = AbortedTransactions.size();
            // TODO Rollback one-by-one to avoid burst
            for (auto& txCtx : AbortedTransactions) {
                RollbackTx(txCtx);
            }
        }

        QueryState.reset();
        if (CleanupCtx) {
            Become(&TKqpSessionActor::CleanupState);
        } else {
            EndCleanup(isFinal);
        }
    }

    bool ReplyProcessError(const TKqpRequestInfo& requestInfo, Ydb::StatusIds::StatusCode ydbStatus,
            const TString& message, google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> *issues = {})
    {
        LOG_W(requestInfo << message);

        auto ev = std::make_unique<TEvKqp::TEvQueryResponse>();
        ev->Record.GetRef().SetYdbStatus(ydbStatus);

        auto* response = ev->Record.GetRef().MutableResponse();

        auto *queryIssue = response->AddQueryIssues();
        IssueToMessage(TIssue{message}, queryIssue);
        if (issues) {
            queryIssue->Mutableissues()->Swap(issues);
        }

        if (QueryState) {
            if (QueryState->TxCtx) {
                QueryState->TxCtx->Invalidate();
            }

            FillTxInfo(response);
        }

        bool canContinue = Reply(std::move(ev));
        Cleanup();
        return canContinue;
    }

    STATEFN(ReadyState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvQueryRequest, HandleReady);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);

                hFunc(TEvKqp::TEvQueryResponse, ForwardResponse);
            default:
                UnexpectedEvent("ReadyState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(CompileState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqp::TEvCompileResponse, HandleCompile);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("CompileState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleExecute);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("ExecuteState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }

    // optional -- only if there were any AbortedTransactions
    STATEFN(CleanupState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTxResponse, HandleCleanup);
                hFunc(TEvKqpExecuter::TEvExecuterProgress, HandleNoop);

                hFunc(TEvKqp::TEvPingSessionRequest, Handle);
                hFunc(TEvKqp::TEvCloseSessionRequest, Handle);
                hFunc(TEvKqp::TEvIdleTimeout, HandleNoop);
            default:
                UnexpectedEvent("CleanupState", ev);
            }
        } catch (const yexception& ex) {
            InternalError(ex.what());
        }
    }
private:
    void UnexpectedEvent(const TString& state, TAutoPtr<NActors::IEventHandle>& ev) {
        InternalError(TStringBuilder() << "TKqpSessionActor in state " << state << " recieve unexpected event " <<
                TypeName(*ev.Get()->GetBase()) << Sprintf("(0x%08" PRIx32 ")", ev->GetTypeRewrite()));
    }

    void InternalError(const TString& message) {
        LOG_E("Internal error, SelfId: " << SelfId() << ", message: " << message);
        if (QueryState) {
            auto requestInfo = TKqpRequestInfo(QueryState->TraceId, SessionId);
            bool canContinue = ReplyProcessError(requestInfo, Ydb::StatusIds::BAD_REQUEST, message);
            if (!canContinue) {
                PassAway();
            }
        } else {
            PassAway();
        }
    }

private:
    TActorId Owner;
    TString SessionId;

    TInstant CreationTime;
    TIntrusivePtr<TKqpCounters> Counters;
    TIntrusivePtr<TKqpRequestCounters> RequestCounters;
    TKqpWorkerSettings Settings;
    TIntrusivePtr<TModuleResolverState> ModuleResolverState;
    TKqpSettings::TConstPtr KqpSettings;
    std::optional<TActorId> WorkerId;

    std::unique_ptr<TKqpQueryState> QueryState;
    std::unique_ptr<TKqpCleanupCtx> CleanupCtx;
    ui32 QueryId = 0;
    TKikimrConfiguration::TPtr Config;
    TLRUCache<TString, TIntrusivePtr<TKqpTransactionContext>> ExplicitTransactions;
    std::vector<TIntrusivePtr<TKqpTransactionContext>> AbortedTransactions;

    TActorId IdleTimerActorId;
    ui32 IdleTimerId = 0;
    TDuration IdleDuration;
};

} // namespace

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpSessionActor(owner, sessionId, kqpSettings, workerSettings, moduleResolverState, counters);
}

}
}
