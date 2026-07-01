#include "sql.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <google/protobuf/arena.h>

#include <util/string/builder.h>

namespace NSQLTranslation {

NYql::TAstParseResult SqlToYql(const TTranslators& translators, const TString& query, const TTranslationSettings& settings,
                               NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo, TTranslationSettings* effectiveSettings)
{
    NYql::TAstParseResult result;
    TTranslationSettings parsedSettings(settings);

    if (!ParseTranslationSettings(query, parsedSettings, result.Issues)) {
        return result;
    }
    if (effectiveSettings) {
        *effectiveSettings = parsedSettings;
    }

    google::protobuf::Arena arena;
    if (!parsedSettings.Arena) {
        parsedSettings.Arena = &arena;
    }

    if (parsedSettings.PgParser && parsedSettings.PGDisable) {
        result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                                              "PG syntax is disabled"));
        return result;
    }

    if (parsedSettings.PgParser) {
        return translators.PG->TextToAst(query, parsedSettings, warningRules, stmtParseInfo);
    }

    return translators.V1->TextToAst(query, parsedSettings, warningRules, nullptr);
}

google::protobuf::Message* SqlAST(const TTranslators& translators, const TString& query, const TString& queryName, NYql::TIssues& issues,
                                  size_t maxErrors, const TTranslationSettings& settings)
{
    TTranslationSettings parsedSettings(settings);
    if (!ParseTranslationSettings(query, parsedSettings, issues)) {
        return nullptr;
    }

    return translators.V1->TextToMessage(query, queryName, issues, maxErrors, parsedSettings);
}

ILexer::TPtr SqlLexer(const TTranslators& translators, const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings)
{
    TTranslationSettings parsedSettings(settings);
    if (!ParseTranslationSettings(query, parsedSettings, issues)) {
        return {};
    }

    return translators.V1->MakeLexer(parsedSettings);
}

NYql::TAstParseResult SqlASTToYql(const TTranslators& translators, const TString& query,
                                  const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings) {
    return translators.V1->TextAndMessageToAst(query, protoAst, hints, settings);
}

TVector<NYql::TAstParseResult> SqlToAstStatements(const TTranslators& translators, const TString& query,
                                                  const TTranslationSettings& settings, NYql::TWarningRules* warningRules,
                                                  TVector<NYql::TStmtParseInfo>* stmtParseInfo)
{
    TVector<NYql::TAstParseResult> result;
    NYql::TIssues issues;
    TTranslationSettings parsedSettings(settings);
    google::protobuf::Arena arena;
    if (!parsedSettings.Arena) {
        parsedSettings.Arena = &arena;
    }

    if (!ParseTranslationSettings(query, parsedSettings, issues)) {
        return {};
    }

    if (parsedSettings.PgParser && parsedSettings.PGDisable) {
        issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                                       "PG syntax is disabled"));
        return result;
    }

    if (parsedSettings.PgParser) {
        return translators.PG->TextToManyAst(query, parsedSettings, warningRules, stmtParseInfo);
    }

    return translators.V1->TextToManyAst(query, parsedSettings, warningRules, stmtParseInfo);
}

TTranslators::TTranslators(TTranslatorPtr v1, TTranslatorPtr pg)
    : V1(v1 ? v1 : MakeDummyTranslator("v1"))
    , PG(pg ? pg : MakeDummyTranslator("pg"))
{
}

} // namespace NSQLTranslation
