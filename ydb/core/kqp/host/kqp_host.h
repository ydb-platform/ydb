#pragma once

#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/host/kqp_translate.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

namespace NActors {
class TActorSystem;
} // namespace NActors

namespace NKikimr {
namespace NKqp {

struct TKqpQueryRef;

using TSqlVersion = ui16;

class IKqpHost : public TThrRefBase {
public:
    using TQueryResult = IKqpGateway::TQueryResult;
    using TGenericResult = IKqpGateway::TGenericResult;
    using IAsyncQueryResult = NYql::IKikimrAsyncResult<TQueryResult>;
    using IAsyncQueryResultPtr = TIntrusivePtr<IAsyncQueryResult>;
    using IAsyncGenericResult = NYql::IKikimrAsyncResult<TGenericResult>;
    using IAsyncGenericResultPtr = TIntrusivePtr<IAsyncGenericResult>;

    struct TExecSettings {
        TMaybe<bool> DocumentApiRestricted;
        TMaybe<bool> UsePgParser;
        TMaybe<TSqlVersion> SyntaxVersion;

        TString ToString() const {
            return TStringBuilder() << "TExecSettings{"
                << " DocumentApiRestricted: " << DocumentApiRestricted
                << " UsePgParser: " << UsePgParser
                << " SyntaxVersion: " << SyntaxVersion
                << " }";
        }
    };

    struct TPrepareSettings: public TExecSettings {
        TMaybe<bool> IsInternalCall;
        TMaybe<bool> ConcurrentResults;
        bool PerStatementResult;

        TString ToString() const {
            return TStringBuilder() << "TPrepareSettings{"
                << " DocumentApiRestricted: " << DocumentApiRestricted
                << " UsePgParser: " << UsePgParser
                << " SyntaxVersion: " << SyntaxVersion
                << " IsInternalCall: " << IsInternalCall
                << " ConcurrentResults: " << ConcurrentResults
                << " }";
        }
    };

    struct TExecScriptSettings {
        NYql::TKikimrQueryDeadlines Deadlines;
        NYql::EKikimrStatsMode StatsMode = NYql::EKikimrStatsMode::None;
        std::shared_ptr<NGRpcService::IRequestCtxMtSafe> RpcCtx;
        TMaybe<bool> UsePgParser;
        TMaybe<TSqlVersion> SyntaxVersion;
    };

    virtual ~IKqpHost() {}

    /* Data queries */
    virtual IAsyncQueryResultPtr ExplainDataQuery(const TKqpQueryRef& query, bool isSql) = 0;
    virtual TQueryResult SyncExplainDataQuery(const TKqpQueryRef& query, bool isSql) = 0;

    virtual IAsyncQueryResultPtr PrepareDataQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) = 0;
    virtual IAsyncQueryResultPtr PrepareDataQueryAst(const TKqpQueryRef& query, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareDataQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) = 0;

    /* Scheme queries */
    virtual IAsyncQueryResultPtr ExecuteSchemeQuery(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings) = 0;
    virtual TQueryResult SyncExecuteSchemeQuery(const TKqpQueryRef& query, bool isSql, const TExecSettings& settings) = 0;

    /* Scan queries */
    virtual IAsyncQueryResultPtr PrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& settings) = 0;
    virtual TQueryResult SyncPrepareScanQuery(const TKqpQueryRef& query, bool isSql, const TPrepareSettings& settings) = 0;

    virtual IAsyncQueryResultPtr ExplainScanQuery(const TKqpQueryRef& query, bool isSql) = 0;

    /* Generic queries */
    virtual IAsyncQueryResultPtr PrepareGenericQuery(const TKqpQueryRef& query, const TPrepareSettings& settings, NYql::TExprNode::TPtr expr = nullptr) = 0;

    /* Federated queries */
    virtual IAsyncQueryResultPtr PrepareGenericScript(const TKqpQueryRef& query, const TPrepareSettings& settings) = 0;

    /* Scripting */
    virtual IAsyncQueryResultPtr ValidateYqlScript(const TKqpQueryRef& script) = 0;
    virtual TQueryResult SyncValidateYqlScript(const TKqpQueryRef& script) = 0;

    virtual IAsyncQueryResultPtr ExplainYqlScript(const TKqpQueryRef& script) = 0;
    virtual TQueryResult SyncExplainYqlScript(const TKqpQueryRef& script) = 0;

    virtual IAsyncQueryResultPtr ExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings) = 0;
    virtual TQueryResult SyncExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const TExecScriptSettings& settings) = 0;

    virtual IAsyncQueryResultPtr StreamExecuteYqlScript(const TKqpQueryRef& script, const ::google::protobuf::Map<TProtoStringType, ::Ydb::TypedValue>& parameters,
        const NActors::TActorId& target, const TExecScriptSettings& settings) = 0;

    /* Split */
    struct TSplitResult {
        THolder<NYql::TExprContext> Ctx;
        TVector<NYql::TExprNode::TPtr> Exprs;
        NYql::TExprNode::TPtr World;
    };

    virtual TSplitResult SplitQuery(const TKqpQueryRef& query, const TPrepareSettings& settings) = 0;
};

TIntrusivePtr<IKqpHost> CreateKqpHost(TIntrusivePtr<IKqpGateway> gateway,
    const TString& cluster, const TString& database, NYql::TKikimrConfiguration::TPtr config, NYql::IModuleResolver::TPtr moduleResolver,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, const TGUCSettings::TPtr& gUCSettings,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, const TMaybe<TString>& applicationName = Nothing(), const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry = nullptr,
    bool keepConfigChanges = false, bool isInternalCall = false, TKqpTempTablesState::TConstPtr tempTablesState = nullptr,
    NActors::TActorSystem* actorSystem = nullptr /*take from TLS by default*/,
    NYql::TExprContext* ctx = nullptr, const TIntrusivePtr<TUserRequestContext>& userRequestContext = nullptr);

} // namespace NKqp
} // namespace NKikimr
