#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/kqp/common/kqp.h>

namespace NKikimr {
namespace NKqp {

NSQLTranslation::EBindingsMode RemapBindingsMode(NKikimrConfig::TTableServiceConfig::EBindingsMode mode);

NYql::EKikimrQueryType ConvertType(NKikimrKqp::EQueryType type);

NSQLTranslation::TTranslationSettings GetTranslationSettings(NYql::EKikimrQueryType queryType, const TMaybe<bool>& usePgParser, bool sqlAutoCommit,
    const TString& queryText, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, TMaybe<ui16>& sqlVersion, TString cluster,
    TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources, NYql::TExprContext& ctx,
    bool isEnablePgConstsToParams);

NYql::TAstParseResult ParseQuery(NYql::EKikimrQueryType queryType, const TMaybe<bool>& usePgParser, const TString& queryText,
    std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, bool isSql, bool sqlAutoCommit, TMaybe<ui16>& sqlVersion,
    bool& deprecatedSQL, TString cluster, TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode,
    bool isEnableExternalDataSources, NYql::TExprContext& ctx, bool isEnablePgConstsToParams);

TQueryAst ParseQuery(NYql::EKikimrQueryType queryType, const TMaybe<Ydb::Query::Syntax>& syntax, const TString& queryText,
    std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters, bool isSql, TString cluster, TString kqpTablePathPrefix,
    ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources, bool isEnablePgConstsToParams);

TVector<TQueryAst> ParseStatements(NYql::EKikimrQueryType queryType, const TMaybe<Ydb::Query::Syntax>& syntax, const TString& queryText, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameters,
        TString cluster, TString kqpTablePathPrefix, ui16 kqpYqlSyntaxVersion, NSQLTranslation::EBindingsMode bindingsMode, bool isEnableExternalDataSources, bool isEnablePgConstsToParams, bool isSql, bool perStatementExecution);

} // namespace NKqp
} // namespace NKikimr
