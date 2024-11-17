#include "yql_syntax.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NYdb {
    namespace NConsoleClient {
        using NSQLTranslation::TTranslationSettings;
        using NSQLTranslation::ParseTranslationSettings;
        using NYql::TIssues;

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8) {
            TTranslationSettings settings;
            TIssues issues;
            ParseTranslationSettings(queryUtf8, settings, issues);
            return settings.AnsiLexer;
        }

    }
}
