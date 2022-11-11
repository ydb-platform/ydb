#pragma once

#include <ydb/core/kqp/common/kqp_transform.h>
#include <ydb/core/kqp/common/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

namespace NKikimr {
namespace NKqp {

class IKqpHost : public TThrRefBase {
public:
    using TQueryResult = IKqpGateway::TQueryResult;
    using TGenericResult = IKqpGateway::TGenericResult;
    using IAsyncQueryResult = NYql::IKikimrAsyncResult<TQueryResult>;
    using IAsyncQueryResultPtr = TIntrusivePtr<IAsyncQueryResult>;
    using IAsyncGenericResult = NYql::IKikimrAsyncResult<TGenericResult>;
    using IAsyncGenericResultPtr = TIntrusivePtr<IAsyncGenericResult>;

    struct TBeginTxResult : public NYql::NCommon::TOperationResult {
        TString TxId;
        ui32 EvictedTx = 0;
        ui32 CurrentActiveTx = 0;
        ui32 CurrentAbortedTx = 0;
    };

    struct TPrepareSettings {
        TMaybe<bool> DocumentApiRestricted;

        TString ToString() const {
            return TStringBuilder() << "TPrepareSettings{ DocumentApiRestricted: " << DocumentApiRestricted << " }";
        }
    };

    struct TExecScriptSettings {
        NYql::TKikimrQueryDeadlines Deadlines;
        NYql::EKikimrStatsMode StatsMode = NYql::EKikimrStatsMode::None;
    };

    virtual ~IKqpHost() {}

    virtual TMaybe<TKqpTransactionInfo> GetTransactionInfo() = 0;

    /* Transaction control */
    virtual TBeginTxResult BeginTransaction(NKikimrKqp::EIsolationLevel isolationLevel,
        bool readonly) = 0;

    virtual TGenericResult DeleteTransaction(const TString& txId) = 0;

    virtual IAsyncQueryResultPtr CommitTransaction(const TString& txId,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;
    virtual TQueryResult SyncCommitTransaction(const TString& txId,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual IAsyncQueryResultPtr RollbackTransaction(const TString& txId,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;
    virtual TQueryResult SyncRollbackTransaction(const TString& txId,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TMaybe<TKqpTransactionInfo> GetTransactionInfo(const TString& txId) = 0;

    virtual bool AbortTransaction(const TString& txId) = 0;
    virtual ui32 AbortAll() = 0;

    virtual IAsyncQueryResultPtr RollbackAborted() = 0;
    virtual TQueryResult SyncRollbackAborted() = 0;

    /* Data queries */
    virtual IAsyncQueryResultPtr ExplainDataQuery(const TString& query, bool isSql) = 0;
    virtual TQueryResult SyncExplainDataQuery(const TString& query, bool isSql) = 0;

    virtual IAsyncQueryResultPtr PrepareDataQuery(const TString& query, const TPrepareSettings& settings) = 0;
    virtual IAsyncQueryResultPtr PrepareDataQueryAst(const TString& query, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareDataQuery(const TString& query, const TPrepareSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExecuteDataQuery(const TString& txId, std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TQueryResult SyncExecuteDataQuery(const TString& txId, std::shared_ptr<const NKikimrKqp::TPreparedQuery>& query,
        NKikimrMiniKQL::TParams&& parameters, const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExecuteDataQuery(const TString& txId, const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual TQueryResult SyncExecuteDataQuery(const TString& txId, const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&&, const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    /* Scheme queries */
    virtual IAsyncQueryResultPtr ExecuteSchemeQuery(const TString& query, bool isSql) = 0;
    virtual TQueryResult SyncExecuteSchemeQuery(const TString& query, bool isSql) = 0;

    /* Scan queries */
    virtual IAsyncQueryResultPtr PrepareScanQuery(const TString& query, bool isSql, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareScanQuery(const TString& query, bool isSql, const TPrepareSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExecuteScanQuery(const TString& query, bool isSql,
        NKikimrMiniKQL::TParams&& parameters, const NActors::TActorId& target,
        const NYql::IKikimrQueryExecutor::TExecuteSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExplainScanQuery(const TString& query, bool isSql) = 0;

    /* Scripting */
    virtual IAsyncQueryResultPtr ValidateYqlScript(const TString& script) = 0;
    virtual TQueryResult SyncValidateYqlScript(const TString& script) = 0;

    virtual IAsyncQueryResultPtr ExplainYqlScript(const TString& script) = 0;
    virtual TQueryResult SyncExplainYqlScript(const TString& script) = 0;

    virtual IAsyncQueryResultPtr ExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const TExecScriptSettings& settings) = 0;
    virtual TQueryResult SyncExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const TExecScriptSettings& settings) = 0;

    virtual IAsyncQueryResultPtr StreamExecuteYqlScript(const TString& script, NKikimrMiniKQL::TParams&& parameters,
        const NActors::TActorId& target, const TExecScriptSettings& settings) = 0;
};

TIntrusivePtr<IKqpHost> CreateKqpHost(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, const TString& database, NYql::TKikimrConfiguration::TPtr config, NYql::IModuleResolver::TPtr moduleResolver,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry = nullptr, bool keepConfigChanges = false);

} // namespace NKqp
} // namespace NKikimr
