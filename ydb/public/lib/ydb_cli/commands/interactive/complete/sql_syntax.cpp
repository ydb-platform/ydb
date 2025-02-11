#include "sql_syntax.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLComplete {

    using NSQLTranslation::ParseTranslationSettings;
    using NSQLTranslation::TTranslationSettings;
    using NYql::TIssues;

    ESqlSyntaxMode QuerySyntaxMode(const TString& query) {
        if (IsAnsiQuery(query)) {
            return ESqlSyntaxMode::ANSI;
        }
        return ESqlSyntaxMode::Default;
    }

    bool IsAnsiQuery(const TString& query) {
        TTranslationSettings settings;
        TIssues issues;
        ParseTranslationSettings(query, settings, issues);
        return settings.AnsiLexer;
    }

} // namespace NSQLComplete
