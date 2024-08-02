#include "sql.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/sql/v0/sql.h>
#include <ydb/library/yql/sql/v0/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/sql.h>
#include <ydb/library/yql/sql/v1/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/proto_parser/proto_parser.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/parser.h>

#include <google/protobuf/arena.h>

#include <util/string/builder.h>

namespace NSQLTranslation {

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo)
    {
        NYql::TAstParseResult result;
        TTranslationSettings parsedSettings(settings);
        google::protobuf::Arena arena;
        if (!parsedSettings.Arena) {
            parsedSettings.Arena = &arena;
        }

        if (!ParseTranslationSettings(query, parsedSettings, result.Issues)) {
            return result;
        }

        if (!parsedSettings.DeclaredNamedExprs.empty() && !parsedSettings.PgParser && parsedSettings.SyntaxVersion != 1) {
            result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                "Externally declared named expressions not supported in V0 syntax"));
            return result;
        }

        if (parsedSettings.PgParser) {
            return NSQLTranslationPG::PGToYql(query, parsedSettings, stmtParseInfo);
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

                return NSQLTranslationV0::SqlToYql(query, parsedSettings, warningRules);
            case 1:
                return NSQLTranslationV1::SqlToYql(query, parsedSettings, warningRules);
            default:
                result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return result;
        }
    }

    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues,
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

                return NSQLTranslationV0::SqlAST(query, queryName, issues, maxErrors, settings.Arena);
            case 1:
                return NSQLTranslationV1::SqlAST(query, queryName, issues, maxErrors, parsedSettings.AnsiLexer, settings.Arena);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return nullptr;
        }
    }

    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings, ui16* actualSyntaxVersion)
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

                return NSQLTranslationV0::MakeLexer();
            case 1:
                return NSQLTranslationV1::MakeLexer(parsedSettings.AnsiLexer);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return {};
        }
    }

    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings) {
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

                return NSQLTranslationV0::SqlASTToYql(protoAst, settings);
            case 1:
                return NSQLTranslationV1::SqlASTToYql(protoAst, hints, settings);
            default:
                result.Issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL syntax version: " << settings.SyntaxVersion));
                return result;
        }
    }

    TVector<NYql::TAstParseResult> SqlToAstStatements(const TString& query, const TTranslationSettings& settings, NYql::TIssues& issues,
        NYql::TWarningRules* warningRules, ui16* actualSyntaxVersion, TVector<NYql::TStmtParseInfo>* stmtParseInfo)
    {
        TTranslationSettings parsedSettings(settings);
        google::protobuf::Arena arena;
        if (!parsedSettings.Arena) {
            parsedSettings.Arena = &arena;
        }

        if (!ParseTranslationSettings(query, parsedSettings, issues)) {
            issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                        "Can't parse settings fot parsing"));
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

        if (parsedSettings.PgParser) {
            return NSQLTranslationPG::PGToYqlStatements(query, parsedSettings, stmtParseInfo);
        }

        switch (parsedSettings.SyntaxVersion) {
            case 0:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                            "V0 syntax is disabled"));
                return {};
            case 1:
                return NSQLTranslationV1::SqlToAstStatements(query, parsedSettings, issues, warningRules, stmtParseInfo);
            default:
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(), NYql::TIssuesIds::DEFAULT_ERROR,
                            TStringBuilder() << "Unknown SQL syntax version: " << parsedSettings.SyntaxVersion));
                return {};
        }
    }

}  // namespace NSQLTranslation
