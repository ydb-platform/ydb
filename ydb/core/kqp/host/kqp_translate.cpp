#include "kqp_translate.h"

#include <ydb/library/yql/sql/sql.h>


namespace NKikimr {
namespace NKqp {

NSQLTranslation::EBindingsMode RemapBindingsMode(NKikimrConfig::TTableServiceConfig::EBindingsMode mode) {
    switch (mode) {
        case NKikimrConfig::TTableServiceConfig::BM_ENABLED:
            return NSQLTranslation::EBindingsMode::ENABLED;
        case NKikimrConfig::TTableServiceConfig::BM_DISABLED:
            return NSQLTranslation::EBindingsMode::DISABLED;
        case NKikimrConfig::TTableServiceConfig::BM_DROP_WITH_WARNING:
            return NSQLTranslation::EBindingsMode::DROP_WITH_WARNING;
        case NKikimrConfig::TTableServiceConfig::BM_DROP:
            return NSQLTranslation::EBindingsMode::DROP;
        default:
            return NSQLTranslation::EBindingsMode::ENABLED;
    }
}

NYql::EKikimrQueryType ConvertType(NKikimrKqp::EQueryType type) {
    switch (type) {
        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT:
            return NYql::EKikimrQueryType::YqlScript;

        case NKikimrKqp::QUERY_TYPE_SQL_SCRIPT_STREAMING:
            return NYql::EKikimrQueryType::YqlScriptStreaming;

        case NKikimrKqp::QUERY_TYPE_SQL_DML:
        case NKikimrKqp::QUERY_TYPE_AST_DML:
            return NYql::EKikimrQueryType::Dml;

        case NKikimrKqp::QUERY_TYPE_SQL_DDL:
            return NYql::EKikimrQueryType::Ddl;

        case NKikimrKqp::QUERY_TYPE_AST_SCAN:
        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
            return NYql::EKikimrQueryType::Scan;

        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
            return NYql::EKikimrQueryType::Query;

        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            return NYql::EKikimrQueryType::Script;

        case NKikimrKqp::QUERY_TYPE_PREPARED_DML:
        case NKikimrKqp::QUERY_TYPE_UNDEFINED:
            YQL_ENSURE(false, "Unexpected query type: " << type);
    }
}

NSQLTranslation::TTranslationSettings GetTranslationSettings(NYql::EKikimrQueryType queryType, const TMaybe<bool>& usePgParser, bool sqlAutoCommit,
        const TString& queryText, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, TMaybe<ui16>& sqlVersion, TString cluster,
        TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources,
        NYql::TExprContext& ctx, bool isEnablePgConstsToParams) {
    NSQLTranslation::TTranslationSettings settings{};

    if (usePgParser) {
        settings.PgParser = *usePgParser;
    }

    if (settings.PgParser) {
        settings.AutoParametrizeEnabled = isEnablePgConstsToParams;
        settings.AutoParametrizeValuesStmt = isEnablePgConstsToParams;
    }

    if (queryType == NYql::EKikimrQueryType::Scan || queryType == NYql::EKikimrQueryType::Query) {
        sqlVersion = sqlVersion ? *sqlVersion : 1;
    }

    if (sqlVersion) {
        settings.SyntaxVersion = *sqlVersion;

        if (*sqlVersion > 0) {
            // Restrict fallback to V0
            settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        }
    } else {
        settings.SyntaxVersion = kqpYqlSyntaxVersion;
        settings.V0Behavior = NSQLTranslation::EV0Behavior::Silent;
    }

    if (isEnableExternalDataSources) {
        settings.DynamicClusterProvider = NYql::KikimrProviderName;
        settings.BindingsMode = bindingsMode;
    }

    settings.InferSyntaxVersion = true;
    settings.V0ForceDisable = false;
    settings.WarnOnV0 = false;
    settings.DefaultCluster = cluster;
    settings.ClusterMapping = {
        {cluster, TString(NYql::KikimrProviderName)},
        {"pg_catalog", TString(NYql::PgProviderName)},
        {"information_schema", TString(NYql::PgProviderName)}
    };
    auto tablePathPrefix = kqpTablePathPrefix;
    if (!tablePathPrefix.empty()) {
        settings.PathPrefix = tablePathPrefix;
    }

    settings.EndOfQueryCommit = sqlAutoCommit;

    settings.Flags.insert("FlexibleTypes");
    settings.Flags.insert("AnsiLike");
    if (queryType == NYql::EKikimrQueryType::Scan
        || queryType == NYql::EKikimrQueryType::YqlScript
        || queryType == NYql::EKikimrQueryType::YqlScriptStreaming
        || queryType == NYql::EKikimrQueryType::Query
        || queryType == NYql::EKikimrQueryType::Script)
    {
        // We enable EmitAggApply for filter and aggregate pushdowns to Column Shards
        settings.Flags.insert("EmitAggApply");
    } else {
        settings.Flags.insert("DisableEmitStartsWith");
    }

    if (queryType == NYql::EKikimrQueryType::Query || queryType == NYql::EKikimrQueryType::Script)
    {
        settings.Flags.insert("AnsiOptionalAs");
        settings.Flags.insert("WarnOnAnsiAliasShadowing");
        settings.Flags.insert("AnsiCurrentRow");
        settings.Flags.insert("AnsiInForEmptyOrNullableItemsCollections");
    }

    if (queryParameters) {
        NSQLTranslation::TTranslationSettings versionSettings = settings;
        NYql::TIssues versionIssues;

        if (ParseTranslationSettings(queryText, versionSettings, versionIssues) && versionSettings.SyntaxVersion == 1) {
            for (const auto& [paramName, paramType] : *(queryParameters)) {
                auto type = NYql::ParseTypeFromYdbType(paramType, ctx);
                if (type != nullptr) {
                    if (paramName.StartsWith("$")) {
                        settings.DeclaredNamedExprs[paramName.substr(1)] = NYql::FormatType(type);
                    } else {
                        settings.DeclaredNamedExprs[paramName] = NYql::FormatType(type);
                    }
                }
            }
        }
    }

    return settings;
}

NYql::TAstParseResult ParseQuery(NYql::EKikimrQueryType queryType, const TMaybe<bool>& usePgParser, const TString& queryText,
        std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, bool isSql, bool sqlAutoCommit,
        TMaybe<ui16>& sqlVersion, bool& deprecatedSQL, TString cluster, TString kqpTablePathPrefix,
        ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources,
        NYql::TExprContext& ctx, bool isEnablePgConstsToParams) {
    NYql::TAstParseResult astRes;
    if (isSql) {
        auto settings = GetTranslationSettings(queryType, usePgParser, sqlAutoCommit, queryText, queryParameters, sqlVersion, cluster, kqpTablePathPrefix, kqpYqlSyntaxVersion, bindingsMode, isEnableExternalDataSources, ctx, isEnablePgConstsToParams);
        ui16 actualSyntaxVersion = 0;
        auto ast = NSQLTranslation::SqlToYql(queryText, settings, nullptr, &actualSyntaxVersion);
        deprecatedSQL = (actualSyntaxVersion == 0);
        sqlVersion = actualSyntaxVersion;
        return std::move(ast);
    } else {
        sqlVersion = {};
        deprecatedSQL = true;
        return NYql::ParseAst(queryText);

        // Do not check SQL constraints on s-expressions input, as it may come from both V0/V1.
        // Constraints were already checked on type annotation of SQL query.
    }
}

TQueryAst ParseQuery(NYql::EKikimrQueryType queryType, const TMaybe<Ydb::Query::Syntax>& syntax, const TString& queryText, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, bool isSql,
        TString cluster, TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources, bool isEnablePgConstsToParams) {
    bool deprecatedSQL;
    TMaybe<ui16> sqlVersion;
    TMaybe<bool> usePgParser;
    if (syntax)
        switch (*syntax) {
            case Ydb::Query::Syntax::SYNTAX_YQL_V1:
                usePgParser = false;
                break;
            case Ydb::Query::Syntax::SYNTAX_PG:
                usePgParser = true;
                break;
            default:
                break;
        }

    NYql::TExprContext ctx;
    bool sqlAutoCommit;
    if (queryType == NYql::EKikimrQueryType::YqlScript || queryType == NYql::EKikimrQueryType::YqlScriptStreaming) {
            sqlAutoCommit = true;
    } else {
        sqlAutoCommit = false;
    }
    auto astRes = ParseQuery(queryType, usePgParser, queryText, queryParameters, isSql, sqlAutoCommit, sqlVersion, deprecatedSQL, cluster, kqpTablePathPrefix, kqpYqlSyntaxVersion, bindingsMode, isEnableExternalDataSources, ctx, isEnablePgConstsToParams);
    return TQueryAst(std::make_shared<NYql::TAstParseResult>(std::move(astRes)), sqlVersion, deprecatedSQL);
}

TVector<TQueryAst> ParseStatements(NYql::EKikimrQueryType queryType, const TMaybe<bool>& usePgParser, const TString& queryText,
        std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, bool sqlAutoCommit,
        TMaybe<ui16>& sqlVersion, TString cluster, TString kqpTablePathPrefix,
        ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources,
        NYql::TExprContext& ctx, bool isEnablePgConstsToParams, bool isSql) {

    TVector<TQueryAst> result;
    NYql::TAstParseResult astRes;
    if (isSql) {
        auto settings = GetTranslationSettings(queryType, usePgParser, sqlAutoCommit, queryText, queryParameters, sqlVersion, cluster, kqpTablePathPrefix, kqpYqlSyntaxVersion, bindingsMode, isEnableExternalDataSources, ctx, isEnablePgConstsToParams);
        ui16 actualSyntaxVersion = 0;
        auto astStatements = NSQLTranslation::SqlToAstStatements(queryText, settings, nullptr, &actualSyntaxVersion);
        sqlVersion = actualSyntaxVersion;
        for (auto&& ast : astStatements) {
            result.push_back({std::make_shared<NYql::TAstParseResult>(std::move(ast)), sqlVersion, (actualSyntaxVersion == 0)});
        }
        return result;
    } else {
        sqlVersion = {};
        return {{std::make_shared<NYql::TAstParseResult>(NYql::ParseAst(queryText)), sqlVersion, true}};
    }
}

TVector<TQueryAst> ParseStatements(NYql::EKikimrQueryType queryType, const TMaybe<Ydb::Query::Syntax>& syntax, const TString& queryText, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters,
        TString cluster, TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources, bool isEnablePgConstsToParams, bool isSql,
        bool perStatementExecution) {
    if (!perStatementExecution) {
        return {ParseQuery(queryType, syntax, queryText, queryParameters, isSql, cluster, kqpTablePathPrefix, kqpYqlSyntaxVersion, bindingsMode, isEnableExternalDataSources, isEnablePgConstsToParams)};
    }
    TMaybe<ui16> sqlVersion;
    TMaybe<bool> usePgParser;
    if (syntax)
        switch (*syntax) {
            case Ydb::Query::Syntax::SYNTAX_YQL_V1:
                usePgParser = false;
                break;
            case Ydb::Query::Syntax::SYNTAX_PG:
                usePgParser = true;
                break;
            default:
                break;
        }

    NYql::TExprContext ctx;
    bool sqlAutoCommit;
    if (queryType == NYql::EKikimrQueryType::YqlScript || queryType == NYql::EKikimrQueryType::YqlScriptStreaming) {
            sqlAutoCommit = true;
    } else {
        sqlAutoCommit = false;
    }
    return ParseStatements(queryType, usePgParser, queryText, queryParameters, sqlAutoCommit, sqlVersion, cluster, kqpTablePathPrefix, kqpYqlSyntaxVersion, bindingsMode, isEnableExternalDataSources, ctx, isEnablePgConstsToParams, isSql);
}

} // namespace NKqp
} // namespace NKikimr
