#include "translation_settings.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/utils/utf8.h>

#include <library/cpp/deprecated/split/split_iterator.h>

#include <util/string/split.h>

namespace {
using namespace NSQLTranslation;
class TAlwaysDiallowPolicy: public ISqlFeaturePolicy {
public:
    TAlwaysDiallowPolicy() = default;
    bool Allow() const override {
        return false;
    }
};

class TAlwaysAllowPolicy: public ISqlFeaturePolicy {
public:
    TAlwaysAllowPolicy() = default;
    bool Allow() const override {
        return true;
    }
};
} // namespace

namespace NSQLTranslation {
ISqlFeaturePolicy::TPtr ISqlFeaturePolicy::MakeAlwaysDisallow() {
    return new TAlwaysDiallowPolicy;
}

ISqlFeaturePolicy::TPtr ISqlFeaturePolicy::MakeAlwaysAllow() {
    return new TAlwaysAllowPolicy;
}

ISqlFeaturePolicy::TPtr ISqlFeaturePolicy::Make(bool allow) {
    return allow ? MakeAlwaysAllow() : MakeAlwaysDisallow();
}

TTranslationSettings::TTranslationSettings()
    : ModuleMapping({{"core", "/lib/yql/core.yqls"}})
    , BindingsMode(EBindingsMode::ENABLED)
    , Mode(ESqlMode::QUERY)
    , MaxErrors(SQL_MAX_PARSER_ERRORS)
    , EndOfQueryCommit(true)
    , EnableGenericUdfs(true)
    , AnsiLexer(false)
    , PgParser(false)
    , PGDisable(false)
    , DqDefaultAuto(ISqlFeaturePolicy::MakeAlwaysDisallow())
    , BlockDefaultAuto(ISqlFeaturePolicy::MakeAlwaysDisallow())
    , AssumeYdbOnClusterWithSlash(false)
{
}

bool TParsedSettings::ApplyTo(TTranslationSettings& settings, NYql::TIssues& issues) const {
    if (HasSyntaxV1) {
        // syntax_v1 is the only supported version; kept for backward compatibility with existing queries.
        (void)settings;
        (void)issues;
    }

    if (HasAnsiLexer) {
        settings.AnsiLexer = true;
    }

    if (HasPgParser) {
        settings.PgParser = true;
    }

    return true;
}

bool ParseTranslationSettingsFromComments(const TString& query, TParsedSettings& parsed, NYql::TIssues& issues) {
    if (!NYql::IsUtf8(query)) {
        issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, 0), NYql::TIssuesIds::DEFAULT_ERROR, "Invalid UTF8 input"));
        return false;
    }

    TSplitDelimiters lineDelimiters("\n\r");
    TDelimitersSplit linesSplit(query, lineDelimiters);
    auto lineIterator = linesSplit.Iterator();
    auto lineNumber = 0;

    while (!lineIterator.Eof()) {
        const TString& line = lineIterator.NextString();
        ++lineNumber;

        TVector<TStringBuf> parts;
        TSetDelimiter<const char> partsDelimiters(" \t");
        Split(line, partsDelimiters, parts);

        if (parts.empty()) {
            continue;
        }

        if (!parts[0].StartsWith("--!")) {
            break;
        }

        if (parts.size() > 1) {
            issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                                           "Bad translation settings format"));
            return false;
        }

        auto value = TString(parts[0].SubString(3, TString::npos));
        if (value.empty()) {
            continue;
        }

        if (value == "syntax_v0") {
            issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                                           "V0 syntax is not supported"));
            return false;
        } else if (value == "syntax_v1") {
            parsed.HasSyntaxV1 = true;
        } else if (value == "ansi_lexer") {
            parsed.HasAnsiLexer = true;
        } else if (value == "antlr4_parser") {
            // Is always turned on, ignore
        } else if (value == "syntax_pg") {
            parsed.HasPgParser = true;
        } else {
            issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                                           TStringBuilder() << "Unknown SQL translation setting: " << value));
            return false;
        }
    }

    return true;
}

bool ParseTranslationSettings(const TString& query, TTranslationSettings& settings, NYql::TIssues& issues) {
    TParsedSettings parsed;
    if (!ParseTranslationSettingsFromComments(query, parsed, issues)) {
        return false;
    }
    return parsed.ApplyTo(settings, issues);
}

} // namespace NSQLTranslation
