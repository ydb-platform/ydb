#pragma once

#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

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

    struct TPrepareSettings {
        TMaybe<bool> DocumentApiRestricted;
        TMaybe<bool> IsInternalCall;

        TString ToString() const {
            return TStringBuilder() << "TPrepareSettings{ DocumentApiRestricted: " << DocumentApiRestricted << " IsInternalCall: " << IsInternalCall << " }";
        }
    };

    struct TExecScriptSettings {
        NYql::TKikimrQueryDeadlines Deadlines;
        NYql::EKikimrStatsMode StatsMode = NYql::EKikimrStatsMode::None;
    };

    virtual ~IKqpHost() {}

    /* Data queries */
    virtual IAsyncQueryResultPtr ExplainDataQuery(const TString& query, bool isSql) = 0;
    virtual TQueryResult SyncExplainDataQuery(const TString& query, bool isSql) = 0;

    virtual IAsyncQueryResultPtr PrepareDataQuery(const TString& query, const TPrepareSettings& settings) = 0;
    virtual IAsyncQueryResultPtr PrepareDataQueryAst(const TString& query, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareDataQuery(const TString& query, const TPrepareSettings& settings) = 0;

    /* Scheme queries */
    virtual IAsyncQueryResultPtr ExecuteSchemeQuery(const TString& query, bool isSql) = 0;
    virtual TQueryResult SyncExecuteSchemeQuery(const TString& query, bool isSql) = 0;

    /* Scan queries */
    virtual IAsyncQueryResultPtr PrepareScanQuery(const TString& query, bool isSql, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareScanQuery(const TString& query, bool isSql, const TPrepareSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExplainScanQuery(const TString& query, bool isSql) = 0;

    /* Generic queries */
    virtual IAsyncQueryResultPtr PrepareQuery(const TString& query, const TPrepareSettings& settings) = 0;

    /* Federated queries */
    virtual IAsyncQueryResultPtr PrepareFederatedQuery(const TString& query, const TPrepareSettings& settings) = 0;

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
    NYql::IHTTPGateway::TPtr httpGateway, const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry = nullptr, bool keepConfigChanges = false, bool isInternalCall = false);

} // namespace NKqp
} // namespace NKikimr
