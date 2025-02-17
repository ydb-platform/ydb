#include "sql.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/v0/sql.h>
#include <yql/essentials/sql/v0/lexer/lexer.h>
#include <yql/essentials/sql/v1/sql.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/parser/pg_wrapper/interface/parser.h>

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

        if (!parsedSettings.DeclaredNamedExprs.empty() && !parsedSettings.PgParser && parsedSettings.SyntaxVersion != 1) {
            result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                "Externally declared named expressions not supported in V0 syntax"));
            return result;
        }

        if (parsedSettings.PgParser && parsedSettings.PGDisable) {
            result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                "PG syntax is disabled"));
            return result;
        }

        if (parsedSettings.PgParser) {
            return translators.PG->TextToAst(query, parsedSettings, warningRules, stmtParseInfo);
        }

        switch (parsedSettings.SyntaxVersion) {
            case 0:
                if (settings.V0ForceDisable || parsedSettings.V0Behavior == EV0Behavior::Disable) {
                    result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "V0 syntax is disabled"));
                    return result;
                }

                if (parsedSettings.AnsiLexer) {
                    result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "Ansi lexer is not supported in V0 syntax"));
                    return result;
                }

                return translators.V0->TextToAst(query, parsedSettings, warningRules, nullptr);
            case 1:
                return translators.V1->TextToAst(query, parsedSettings, warningRules, nullptr);
            default:
                result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return result;
        }
    }

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo, TTranslationSettings* effectiveSettings) {
        return SqlToYql(MakeAllTranslators(), query, settings, warningRules, stmtParseInfo, effectiveSettings);
    }

    google::protobuf::Message* SqlAST(const TTranslators& translators, const TString& query, const TString& queryName, NYql::TIssues& issues,
        size_t maxErrors, const TTranslationSettings& settings, ui16* actualSyntaxVersion)
    {
        TTranslationSettings parsedSettings(settings);
        if (!ParseTranslationSettings(query, parsedSettings, issues)) {
            return nullptr;
        }

        if (actualSyntaxVersion) {
            *actualSyntaxVersion = parsedSettings.SyntaxVersion;
        }

        switch (parsedSettings.SyntaxVersion) {
            case 0:
                if (settings.V0ForceDisable || settings.V0Behavior == EV0Behavior::Disable) {
                    issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "V0 syntax is disabled"));
                    return nullptr;
                }

                if (parsedSettings.AnsiLexer) {
                    issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "Ansi lexer is not supported in V0 syntax"));
                    return nullptr;
                }

                return translators.V0->TextToMessage(query, queryName, issues, maxErrors, settings);
            case 1:
                return translators.V1->TextToMessage(query, queryName, issues, maxErrors, parsedSettings);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return nullptr;
        }
    }

    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues,
        size_t maxErrors, const TTranslationSettings& settings, ui16* actualSyntaxVersion) {
        return SqlAST(MakeAllTranslators(), query, queryName, issues, maxErrors, settings, actualSyntaxVersion);
    }

    ILexer::TPtr SqlLexer(const TTranslators& translators, const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings, ui16* actualSyntaxVersion)
    {
        TTranslationSettings parsedSettings(settings);
        if (!ParseTranslationSettings(query, parsedSettings, issues)) {
            return {};
        }

        if (actualSyntaxVersion) {
            *actualSyntaxVersion = parsedSettings.SyntaxVersion;
        }

        switch (parsedSettings.SyntaxVersion) {
            case 0:
                if (settings.V0ForceDisable || settings.V0Behavior == EV0Behavior::Disable) {
                    issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "V0 syntax is disabled"));
                    return {};
                }

                if (parsedSettings.AnsiLexer) {
                    issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "Ansi lexer is not supported in V0 syntax"));
                    return {};
                }

                return translators.V0->MakeLexer(parsedSettings);
            case 1:
                return translators.V1->MakeLexer(parsedSettings);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return {};
        }
    }

    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings, ui16* actualSyntaxVersion) {
        return SqlLexer(MakeAllTranslators(), query, issues, settings, actualSyntaxVersion);
    }

    NYql::TAstParseResult SqlASTToYql(const TTranslators& translators, const TString& query,
        const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings) {
        NYql::TAstParseResult result;
        switch (settings.SyntaxVersion) {
            case 0:
                if (settings.V0ForceDisable || settings.V0Behavior == EV0Behavior::Disable) {
                    result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "V0 syntax is disabled"));
                    return result;
                }

                if (settings.AnsiLexer) {
                    result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "Ansi lexer is not supported in V0 syntax"));
                    return result;
                }

                return translators.V0->TextAndMessageToAst(query, protoAst, hints, settings);
            case 1:
                return translators.V1->TextAndMessageToAst(query, protoAst, hints, settings);
            default:
                result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << settings.SyntaxVersion));
                return result;
        }
    }

    NYql::TAstParseResult SqlASTToYql(const TString& query, const google::protobuf::Message& protoAst,
        const TSQLHints& hints, const TTranslationSettings& settings) {
        return SqlASTToYql(MakeAllTranslators(), query, protoAst, hints, settings);
    }

    TVector<NYql::TAstParseResult> SqlToAstStatements(const TTranslators& translators, const TString& query,
        const TTranslationSettings& settings, NYql::TWarningRules* warningRules, ui16* actualSyntaxVersion,
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

        if (actualSyntaxVersion) {
            *actualSyntaxVersion = parsedSettings.SyntaxVersion;
        }

        if (!parsedSettings.DeclaredNamedExprs.empty() && !parsedSettings.PgParser && parsedSettings.SyntaxVersion != 1) {
            issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                "Externally declared named expressions not supported in V0 syntax"));
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

        switch (parsedSettings.SyntaxVersion) {
            case 0:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    "V0 syntax is disabled"));
                return translators.V0->TextToManyAst(query, parsedSettings, warningRules, stmtParseInfo);
            case 1:
                return translators.V1->TextToManyAst(query, parsedSettings, warningRules, stmtParseInfo);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return {};
        }
    }

    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, ui16* actualSyntaxVersion, TVector<NYql::TStmtParseInfo>* stmtParseInfo) {
        return SqlToAstStatements(MakeAllTranslators(), query, settings, warningRules, actualSyntaxVersion, stmtParseInfo);
    }

    TTranslators MakeAllTranslators() {
        return TTranslators(
            NSQLTranslationV0::MakeTranslator(),
            NSQLTranslationV1::MakeTranslator(),
            NSQLTranslationPG::MakeTranslator()
        );
    }

    TTranslators::TTranslators(TTranslatorPtr v0, TTranslatorPtr v1, TTranslatorPtr pg)
        : V0(v0 ? v0 : MakeDummyTranslator("v0"))
        , V1(v1 ? v1 : MakeDummyTranslator("v1"))
        , PG(pg ? pg : MakeDummyTranslator("pg"))
    {}

}  // namespace NSQLTranslation
