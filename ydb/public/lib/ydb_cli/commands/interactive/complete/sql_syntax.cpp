#include "sql_syntax.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

namespace NSQLComplete {

    using NSQLTranslation::ParseTranslationSettings;
    using NSQLTranslation::TTranslationSettings;
    using NYql::TIssues;

    EYQLSyntaxMode QuerySyntaxMode(const TString& queryUtf8) {
        if (IsAnsiQuery(queryUtf8)) {
            return EYQLSyntaxMode::ANSI;
        }
        return EYQLSyntaxMode::Default;
    }

    bool IsAnsiQuery(const TString& queryUtf8) {
        TTranslationSettings settings;
        TIssues issues;
        ParseTranslationSettings(queryUtf8, settings, issues);
        return settings.AnsiLexer;
    }

} // namespace NSQLComplete
