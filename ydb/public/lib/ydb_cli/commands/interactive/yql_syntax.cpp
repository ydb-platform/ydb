#include "yql_syntax.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NYdb {
    namespace NConsoleClient {
        using NSQLTranslation::ParseTranslationSettings;
        using NSQLTranslation::TTranslationSettings;
        using NYql::TIssues;

        EYQLSyntaxMode QuerySyntaxMode(const TString& queryUtf8) {
            if (IsAnsiQuery(queryUtf8)) {
                return EYQLSyntaxMode::ANSI;
            }
            return EYQLSyntaxMode::Default;
        }

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8) {
            TTranslationSettings settings;
            TIssues issues;
            ParseTranslationSettings(queryUtf8, settings, issues);
            return settings.AnsiLexer;
        }

    } // namespace NConsoleClient
} // namespace NYdb
