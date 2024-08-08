#pragma once

#include "kqp_query_stats.h"
#include "kqp_worker_common.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <ydb/core/base/cputime.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/common/kqp_timeouts.h>
#include <ydb/core/kqp/common/kqp_tx.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/simple/temp_tables.h>

#include <ydb/library/actors/core/monotonic_provider.h>

#include <util/generic/noncopyable.h>
#include <util/generic/string.h>

#include <map>
#include <memory>

namespace NKikimr::NKqp {

// basically it's a state that holds all the context
// about the specific query execution.
// it holds the unique pointer to the query request, which may include
// the context of user RPC (if session is on the same node with user RPC, which is the most
// common case).
class TKqpQueryState : public TNonCopyable {
public:
    class TQueryTxId {
    public:
        TQueryTxId() = default;
        TQueryTxId(const TQueryTxId& other);
        TQueryTxId& operator=(const TQueryTxId& id);

        void SetValue(const TTxId& id);
        TTxId GetValue();

        void Reset();

    private:
        TMaybe<TTxId> Id;
    };

    TKqpQueryState(TEvKqp::TEvQueryRequest::TPtr& ev, ui64 queryId, const TString& database, const TMaybe<TString>& applicationName,
        const TString& cluster, TKqpDbCountersPtr dbCounters, bool longSession, const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, const TString& sessionId, TMonotonic startedAt)
        : QueryId(queryId)
        , Database(database)
        , ApplicationName(applicationName)
        , Cluster(cluster)
        , DbCounters(dbCounters)
        , Sender(ev->Sender)
        , ProxyRequestId(ev->Cookie)
        , ParametersSize(ev->Get()->GetParametersSize())
        , RequestActorId(ev->Get()->GetRequestActorId())
        , IsDocumentApiRestricted_(IsDocumentApiRestricted(ev->Get()->GetRequestType()))
        , StartTime(TInstant::Now())
        , KeepSession(ev->Get()->GetKeepSession() || longSession)
        , UserToken(ev->Get()->GetUserToken())
        , StartedAt(startedAt)
    {
        RequestEv.reset(ev->Release().Release());
        if (tableServiceConfig.GetEnableImplicitQueryParameterTypes() && !RequestEv->GetYdbParameters().empty()) {
            QueryParameterTypes = std::make_shared<std::map<TString, Ydb::Type>>();
            for (const auto& [name, typedValue] : RequestEv->GetYdbParameters()) {
                QueryParameterTypes->insert({name, typedValue.Gettype()});
            }
        }

        SetQueryDeadlines(tableServiceConfig, queryServiceConfig);
        auto action = GetAction();
        KqpSessionSpan = NWilson::TSpan(
            TWilsonKqp::KqpSession, std::move(ev->TraceId),
            "Session.query." + NKikimrKqp::EQueryAction_Name(action), NWilson::EFlags::AUTO_END);
        if (RequestEv->GetUserRequestContext()) {
            UserRequestContext = RequestEv->GetUserRequestContext();
        } else {
            UserRequestContext = MakeIntrusive<TUserRequestContext>(RequestEv->GetTraceId(), Database, sessionId);
        }
        UserRequestContext->PoolId = RequestEv->GetPoolId();
        UserRequestContext->PoolConfig = RequestEv->GetPoolConfig();
    }

    // the monotonously growing counter, the ordinal number of the query,
    // executed by the session.
    // this counter may be used as a cookie by a session actor to reject events
    // with cookie less than current QueryId.
    ui64 QueryId = 0;
    TString Database;
    TMaybe<TString> ApplicationName;
    TString Cluster;
    TKqpDbCountersPtr DbCounters;
    TActorId Sender;
    ui64 ProxyRequestId = 0;
    std::unique_ptr<TEvKqp::TEvQueryRequest> RequestEv;
    ui64 ParametersSize = 0;
    TPreparedQueryHolder::TConstPtr PreparedQuery;
    TKqpCompileResult::TConstPtr CompileResult;
    TVector<NKikimrKqp::TParameterDescription> ResultParams;
    TKqpStatsCompile CompileStats;
    TIntrusivePtr<TKqpTransactionContext> TxCtx;
    TQueryData::TPtr QueryData;

    TActorId RequestActorId;

    ui64 CurrentTx = 0;
    TIntrusivePtr<TUserRequestContext> UserRequestContext;
    bool IsDocumentApiRestricted_ = false;

    TInstant StartTime;
    TInstant ContinueTime;
    NYql::TKikimrQueryDeadlines QueryDeadlines;
    TKqpQueryStats QueryStats;
    bool KeepSession = false;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    NActors::TMonotonic StartedAt;

    THashMap<NKikimr::TTableId, ui64> TableVersions;

    NLWTrace::TOrbit Orbit;
    NWilson::TSpan KqpSessionSpan;
    ETableReadType MaxReadType = ETableReadType::Other;

    TQueryTxId TxId; // User tx
    bool Commit = false;
    bool Commited = false;

    NTopic::TTopicOperations TopicOperations;
    TDuration CpuTime;
    std::optional<NCpuTime::TCpuTimer> CurrentTimer;

    std::shared_ptr<std::map<TString, Ydb::Type>> QueryParameterTypes;

    TKqpTempTablesState::TConstPtr TempTablesState;
    TMaybe<TActorId> PoolHandlerActor;

    THolder<NYql::TExprContext> SplittedCtx;
    TVector<NYql::TExprNode::TPtr> SplittedExprs;
    NYql::TExprNode::TPtr SplittedWorld;
    int NextSplittedExpr = 0;

    TString ReplayMessage;

    NYql::TIssues Issues;

    TVector<TQueryAst> Statements;
    TMaybe<TQueryTxId> ImplicitTxId = {}; // Implicit tx for all statements
    ui32 CurrentStatementId = 0;
    ui32 StatementResultIndex = 0;
    ui32 StatementResultSize = 0;

    TMaybe<TString> CommandTagName;

    NKikimrKqp::EQueryAction GetAction() const {
        return RequestEv->GetAction();
    }

    bool GetKeepSession() const {
        return RequestEv->GetKeepSession();
    }

    const TString& GetQuery() const {
        return RequestEv->GetQuery();
    }

    const TString& GetPreparedQuery() const {
        return RequestEv->GetPreparedQuery();
    }

    NKikimrKqp::EQueryType GetType() const {
        return RequestEv->GetType();
    }

    Ydb::Query::Syntax GetSyntax() const {
        return RequestEv->GetSyntax();
    }

    const TString& GetRequestType() const {
        return RequestEv->GetRequestType();
    }

    std::shared_ptr<std::map<TString, Ydb::Type>> GetQueryParameterTypes() const {
        return QueryParameterTypes;
    }

    TVector<NKikimrKqp::TParameterDescription> GetResultParams() const {
        return ResultParams;
    }

    void EnsureAction() {
        YQL_ENSURE(RequestEv->HasAction());
    }

    bool GetUsePublicResponseDataFormat() const {
        return RequestEv->GetUsePublicResponseDataFormat();
    }

    ui64 GetOutputChunkMaxSize() const {
        return RequestEv->GetOutputChunkMaxSize();
    }

    void UpdateTempTablesState(const TKqpTempTablesState& tempTablesState) {
        TempTablesState = std::make_shared<const TKqpTempTablesState>(tempTablesState);
    }

    void SetQueryDeadlines(const NKikimrConfig::TTableServiceConfig& tableService, const NKikimrConfig::TQueryServiceConfig& queryService) {
        auto now = TAppData::TimeProvider->Now();
        auto cancelAfter = RequestEv->GetCancelAfter();
        auto timeout = RequestEv->GetOperationTimeout();
        if (cancelAfter.MilliSeconds() > 0) {
            QueryDeadlines.CancelAt = now + cancelAfter;
        }

        auto timeoutMs = GetQueryTimeout(GetType(), timeout.MilliSeconds(), tableService, queryService);
        QueryDeadlines.TimeoutAt = now + timeoutMs;
    }

    bool HasTopicOperations() const {
        return RequestEv->HasTopicOperations();
    }

    bool GetQueryKeepInCache() const {
        return RequestEv->GetQueryKeepInCache();
    }

    const TString& GetDatabase() const {
        return RequestEv->GetDatabase();
    }

    bool IsSplitted() const {
        return !SplittedExprs.empty();
    }

    bool IsCreateTableAs() const {
        return IsSplitted();
    }

    // todo: gvit
    // fill this hash set only once on query compilation.
    void FillTables(const NKqpProto::TKqpPhyTx& phyTx) {
        auto addTable = [&](const NKqpProto::TKqpPhyTableId& table) {
            NKikimr::TTableId tableId(table.GetOwnerId(), table.GetTableId());
            auto it = TableVersions.find(tableId);
            if (it != TableVersions.end()) {
                Y_ENSURE(it->second == table.GetVersion());
            } else {
                TableVersions.emplace(tableId, table.GetVersion());
            }
        };
        for (const auto& stage : phyTx.GetStages()) {
            for (const auto& tableOp : stage.GetTableOps()) {
                addTable(tableOp.GetTable());
            }

            for (const auto& input : stage.GetInputs()) {
                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kStreamLookup) {
                    addTable(input.GetStreamLookup().GetTable());
                }

                if (input.GetTypeCase() == NKqpProto::TKqpPhyConnection::kSequencer) {
                    addTable(input.GetSequencer().GetTable());
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (source.GetTypeCase() == NKqpProto::TKqpSource::kReadRangesSource) {
                    addTable(source.GetReadRangesSource().GetTable());
                }
            }

            for (const auto& sink : stage.GetSinks()) {
                if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink) {
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>());
                    NKikimrKqp::TKqpTableSinkSettings settings;
                    YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                    addTable(settings.GetTable());
                }
            }
        }

        for (const auto& table : phyTx.GetTables()) {
            if (table.GetKind() == NKqpProto::EKqpPhyTableKind::TABLE_KIND_EXTERNAL) {
                addTable(table.GetId());
            }
        }
    }

    void FillViews(const google::protobuf::RepeatedPtrField< ::NKqpProto::TKqpTableInfo>& views);

    bool NeedCheckTableVersions() const {
        return CompileStats.FromCache;
    }

    TString ExtractQueryText() const {
        if (CompileResult) {
            if (CompileResult->Query) {
                return CompileResult->Query->Text;
            }
            return {};
        }
        return RequestEv->GetQuery();
    }

    const ::NKikimrKqp::TTopicOperationsRequest& GetTopicOperations() const {
        return RequestEv->GetTopicOperations();
    }

    bool NeedPersistentSnapshot() const {
        auto type = GetType();
        return (
            type == NKikimrKqp::QUERY_TYPE_SQL_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_AST_SCAN
        );
    }

    bool NeedSnapshot(const NYql::TKikimrConfiguration& config) const {
        return ::NKikimr::NKqp::NeedSnapshot(*TxCtx, config, /*rollback*/ false, Commit, PreparedQuery->GetPhysicalQuery());
    }

    bool ShouldCommitWithCurrentTx(const TKqpPhyTxHolder::TConstPtr& tx) {
        const auto& phyQuery = PreparedQuery->GetPhysicalQuery();
        if (!Commit) {
            return false;
        }

        if (CurrentTx + 1 < phyQuery.TransactionsSize()) {
            // commit can only be applied to the last transaction or perform separately at the end
            return false;
        }

        if (!tx) {
            // no physical transactions left, perform commit
            return true;
        }

        if (HasTxSinkInTx(tx)) {
            // At current time transactional internal sinks require separate tnx with commit.
            return false;
        }

        if (TxCtx->HasUncommittedChangesRead || AppData()->FeatureFlags.GetEnableForceImmediateEffectsExecution()) {
            YQL_ENSURE(TxCtx->EnableImmediateEffects);

            if (tx && tx->GetHasEffects()) {
                YQL_ENSURE(tx->ResultsSize() == 0);
                // commit can be applied to the last transaction with effects
                return CurrentTx + 1 == phyQuery.TransactionsSize();
            }

            return false;
        }

        // we can merge commit with last tx only for read-only transactions
        return !TxCtx->TxHasEffects();
    }

    bool ShouldAcquireLocks(const TKqpPhyTxHolder::TConstPtr& tx) {
        if (*TxCtx->EffectiveIsolationLevel != NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE) {
            return false;
        }

        // Inconsistent writes (CTAS) don't require locks.
        if (IsSplitted() && !HasTxSinkInTx(tx)) {
            return false;
        }

        if (TxCtx->Locks.GetLockTxId() && !TxCtx->Locks.Broken()) {
            return true;  // Continue to acquire locks
        }

        if (TxCtx->Locks.Broken()) {
            YQL_ENSURE(TxCtx->GetSnapshot().IsValid() && !TxCtx->TxHasEffects());
            return false;  // Do not acquire locks after first lock issue
        }

        if (TxCtx->TxHasEffects()) {
            return true; // Acquire locks in read write tx
        }

        const auto& query = PreparedQuery->GetPhysicalQuery();
        for (auto& tx : query.GetTransactions()) {
            if (tx.GetHasEffects()) {
                return true; // Acquire locks in read write tx
            }
        }

        if (!Commit) {
            return true; // Is not a commit tx
        }

        if (TxCtx->GetSnapshot().IsValid()) {
            return false; // It is a read only tx with snapshot, no need to acquire locks
        }

        return true;
    }

    TKqpPhyTxHolder::TConstPtr GetCurrentPhyTx() {
        const auto& phyQuery = PreparedQuery->GetPhysicalQuery();
        auto tx = PreparedQuery->GetPhyTxOrEmpty(CurrentTx);

        if (TxCtx->CanDeferEffects()) {
            // At current time sinks require separate tnx with commit.
            while (tx && tx->GetHasEffects() && !HasTxSinkInTx(tx)) {
                QueryData->CreateKqpValueMap(tx);
                bool success = TxCtx->AddDeferredEffect(tx, QueryData);
                YQL_ENSURE(success);
                if (CurrentTx + 1 < phyQuery.TransactionsSize()) {
                    ++CurrentTx;
                    tx = PreparedQuery->GetPhyTx(CurrentTx);
                } else {
                    tx = nullptr;
                }
            }
        }
        TxCtx->HasImmediateEffects |= tx && tx->GetHasEffects();

        return tx;
    }

    bool HasTxSinkInStage(const ::NKqpProto::TKqpPhyStage& stage) const {
        for (const auto& sink : stage.GetSinks()) {
            if (sink.GetTypeCase() == NKqpProto::TKqpSink::kInternalSink && sink.GetInternalSink().GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
                NKikimrKqp::TKqpTableSinkSettings settings;
                YQL_ENSURE(sink.GetInternalSink().GetSettings().UnpackTo(&settings), "Failed to unpack settings");
                if (!settings.GetInconsistentTx()) {
                    return true;
                }
            }
        }
        return false;
    }

    bool HasTxSink() const {
        const auto& query = PreparedQuery->GetPhysicalQuery();
        for (auto& tx : query.GetTransactions()) {
            for (const auto& stage : tx.GetStages()) {
                if (HasTxSinkInStage(stage)) {
                    return true;
                }
            }
        }
        return false;
    }

    bool HasTxSinkInTx(const TKqpPhyTxHolder::TConstPtr& tx) const {
        for (const auto& stage : tx->GetStages()) {
            if (HasTxSinkInStage(stage)) {
                return true;
            }
        }
        return false;
    }

    bool HasTxControl() const {
        return RequestEv->HasTxControl();
    }

    bool HasImplicitTx() const; // (only for QueryService API) user has not specified TxControl in the request. In this case we behave like Begin/Commit was specified.

    const ::Ydb::Table::TransactionControl& GetTxControl() const {
        return RequestEv->GetTxControl();
    }

    bool ProcessingLastStatement() const {
        return CurrentStatementId + 1 >= Statements.size();
    }

    void PrepareCurrentStatement() {
        QueryData = std::make_shared<TQueryData>(TxCtx->TxAlloc);
        PreparedQuery = {};
        CompileResult = {};
        CurrentTx = 0;
        TableVersions = {};
        MaxReadType = ETableReadType::Other;
        TopicOperations = {};
        ReplayMessage = {};
    }

    void PrepareNextStatement() {
        CurrentStatementId++;
        StatementResultIndex += StatementResultSize;
        StatementResultSize = 0;
        PrepareCurrentStatement();
    }

    void PrepareStatementTransaction(NKqpProto::TKqpPhyTx_EType txType) {
        if (!HasTxControl()) {
            switch (txType) {
                case NKqpProto::TKqpPhyTx::TYPE_SCHEME:
                    TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_UNDEFINED;
                    break;
                default:
                    TxCtx->EffectiveIsolationLevel = NKikimrKqp::ISOLATION_LEVEL_SERIALIZABLE;
            }
        }
    }

    // validate the compiled query response and ensure that all table versions are not
    // changed since the last compilation.
     bool EnsureTableVersions(const TEvTxProxySchemeCache::TEvNavigateKeySetResult& response);
    // builds a request to navigate schema of all tables, that participate in query
    // execution.
    std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySet> BuildNavigateKeySet();
    // same the context of the compiled query to the query state.
    bool SaveAndCheckCompileResult(TEvKqp::TEvCompileResponse* ev);
    bool SaveAndCheckParseResult(TEvKqp::TEvParseResponse&& ev);
    bool SaveAndCheckSplitResult(TEvKqp::TEvSplitResponse* ev);
    // build the compilation request.
    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildCompileRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr);
    // TODO(gvit): get rid of code duplication in these requests,
    // use only one of these requests.
    std::unique_ptr<TEvKqp::TEvRecompileRequest> BuildReCompileRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr);

    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildSplitRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr);
    std::unique_ptr<TEvKqp::TEvCompileRequest> BuildCompileSplittedRequest(std::shared_ptr<std::atomic<bool>> cookie, const TGUCSettings::TPtr& gUCSettingsPtr);

    bool ProcessingLastStatementPart();
    bool PrepareNextStatementPart();

    const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& GetYdbParameters() const {
        return RequestEv->GetYdbParameters();
    }

    Ydb::Table::QueryStatsCollection::Mode GetStatsMode() const {
        if (!RequestEv->HasCollectStats()) {
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        }

        auto cStats = RequestEv->GetCollectStats();
        if (cStats == Ydb::Table::QueryStatsCollection::STATS_COLLECTION_UNSPECIFIED) {
            return Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE;
        }

        return cStats;
    }

    bool ReportStats() const {
        return GetStatsMode() != Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE
            // always report stats for scripting subrequests
            || GetType() == NKikimrKqp::QUERY_TYPE_AST_DML
            || GetType() == NKikimrKqp::QUERY_TYPE_AST_SCAN
        ;
    }

    bool HasPreparedQuery() const {
        return RequestEv->HasPreparedQuery();
    }

    bool IsStreamResult() const {
        auto type = GetType();
        return (
            type == NKikimrKqp::QUERY_TYPE_AST_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_SQL_SCAN ||
            type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY ||
            type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY ||
            type == NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT
        );
    }

    bool IsInternalCall() const {
        return RequestEv->IsInternalCall();
    }

    void ResetTimer() {
        if (CurrentTimer) {
            CpuTime += CurrentTimer->GetTime();
            CurrentTimer.reset();
        }
    }

    TDuration GetCpuTime() {
        ResetTimer();
        return CpuTime;
    }

    // Returns nullptr in case of no local event
    google::protobuf::Arena* GetArena() {
        return RequestEv->GetArena();
    }

    bool GetCollectDiagnostics() {
        return RequestEv->GetCollectDiagnostics();
    }

    TDuration GetProgressStatsPeriod() {
        return RequestEv->GetProgressStatsPeriod();
    }

    //// Topic ops ////
    void AddOffsetsToTransaction();
    bool TryMergeTopicOffsets(const NTopic::TTopicOperations &operations, TString& message);
    std::unique_ptr<NSchemeCache::TSchemeCacheNavigate> BuildSchemeCacheNavigate();
    bool IsAccessDenied(const NSchemeCache::TSchemeCacheNavigate& response, TString& message);
    bool HasErrors(const NSchemeCache::TSchemeCacheNavigate& response, TString& message);
};


}
