#include "yql_syntax.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NYdb {
    namespace NConsoleClient {
        using NSQLTranslation::ParseTranslationSettings;
        using NSQLTranslation::TTranslationSettings;
        using NYql::TIssues;

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8) {
            TTranslationSettings settings;
            TIssues issues;
            ParseTranslationSettings(queryUtf8, settings, issues);
            return settings.AnsiLexer;
        }

        bool IsOperationTokenName(TStringBuf tokenName) {
            return (
                tokenName == "EQUALS" ||
                tokenName == "EQUALS2" ||
                tokenName == "NOT_EQUALS" ||
                tokenName == "NOT_EQUALS2" ||
                tokenName == "LESS" ||
                tokenName == "LESS_OR_EQ" ||
                tokenName == "GREATER" ||
                tokenName == "GREATER_OR_EQ" ||
                tokenName == "SHIFT_LEFT" ||
                tokenName == "ROT_LEFT" ||
                tokenName == "AMPERSAND" ||
                tokenName == "PIPE" ||
                tokenName == "DOUBLE_PIPE" ||
                tokenName == "STRUCT_OPEN" ||
                tokenName == "STRUCT_CLOSE" ||
                tokenName == "PLUS" ||
                tokenName == "MINUS" ||
                tokenName == "TILDA" ||
                tokenName == "ASTERISK" ||
                tokenName == "SLASH" ||
                tokenName == "PERCENT" ||
                tokenName == "SEMICOLON" ||
                tokenName == "DOT" ||
                tokenName == "COMMA" ||
                tokenName == "LPAREN" ||
                tokenName == "RPAREN" ||
                tokenName == "QUESTION" ||
                tokenName == "COLON" ||
                tokenName == "COMMAT" ||
                tokenName == "DOUBLE_COMMAT" ||
                tokenName == "DOLLAR" ||
                tokenName == "LBRACE_CURLY" ||
                tokenName == "RBRACE_CURLY" ||
                tokenName == "CARET" ||
                tokenName == "NAMESPACE" ||
                tokenName == "ARROW" ||
                tokenName == "RBRACE_SQUARE" ||
                tokenName == "LBRACE_SQUARE");
        }

        bool IsPlainIdentifierTokenName(TStringBuf tokenName) {
            return tokenName == "ID_PLAIN";
        }

        bool IsQuotedIdentifierTokenName(TStringBuf tokenName) {
            return tokenName == "ID_QUOTED";
        }

        bool IsNamespaceTokenName(TStringBuf tokenName) {
            return tokenName == "NAMESPACE";
        }

        bool IsStringTokenName(TStringBuf tokenName) {
            return tokenName == "STRING_VALUE";
        }

        bool IsNumberTokenName(TStringBuf tokenName) {
            return tokenName == "DIGITS" ||
                   tokenName == "INTEGER_VALUE" ||
                   tokenName == "REAL" ||
                   tokenName == "BLOB";
        }

        bool IsCommentTokenName(TStringBuf tokenName) {
            return tokenName == "COMMENT";
        }

    } // namespace NConsoleClient
} // namespace NYdb
