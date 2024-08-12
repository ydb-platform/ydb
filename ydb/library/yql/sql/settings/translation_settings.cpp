#include "translation_settings.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/utils/utf8.h>

#include <library/cpp/deprecated/split/split_iterator.h>

#include <util/string/split.h>
#include <util/system/env.h>

namespace {
    bool InTestEnvironment() {
        return GetEnv("YQL_LOCAL_ENVIRONMENT") || GetEnv("YA_TEST_RUNNER");
    }

    using namespace NSQLTranslation;
    class TAlwaysDiallowPolicy : public ISqlFeaturePolicy {
    public:
        TAlwaysDiallowPolicy() = default;
        bool Allow() const override {
            return false;
        }
    };

    class TAlwaysAllowPolicy : public ISqlFeaturePolicy {
    public:
        TAlwaysAllowPolicy() = default;
        bool Allow() const override {
            return true;
        }
    };
}

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
        : ModuleMapping({{"core", "/lib/yql/core.yql"}})
        , BindingsMode(EBindingsMode::ENABLED)
        , Mode(ESqlMode::QUERY)
        , MaxErrors(SQL_MAX_PARSER_ERRORS)
        , EndOfQueryCommit(true)
        , EnableGenericUdfs(true)
        , SyntaxVersion(1)
        , AnsiLexer(false)
        , PgParser(false)
        , InferSyntaxVersion(false)
        , V0Behavior(EV0Behavior::Disable)
        , V0ForceDisable(InTestEnvironment())
        , WarnOnV0(true)
        , V0WarnAsError(ISqlFeaturePolicy::MakeAlwaysDisallow())
        , DqDefaultAuto(ISqlFeaturePolicy::MakeAlwaysDisallow())
        , BlockDefaultAuto(ISqlFeaturePolicy::MakeAlwaysDisallow())
        , AssumeYdbOnClusterWithSlash(false)
    {}


    bool ParseTranslationSettings(const TString& query, TTranslationSettings& settings, NYql::TIssues& issues) {
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

            if ((value == "syntax_v0" || value == "syntax_v1") && !settings.InferSyntaxVersion) {
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                    "Explicit syntax version is not allowed"));
                return false;
            }

            if (value == "syntax_v0") {
                if (settings.V0ForceDisable || settings.V0Behavior == NSQLTranslation::EV0Behavior::Disable) {
                    issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                        "V0 syntax is disabled"));
                    return false;
                }

                settings.SyntaxVersion = 0;
            } else if (value == "syntax_v1") {
                settings.SyntaxVersion = 1;
            } else if (value == "ansi_lexer") {
                settings.AnsiLexer = true;
            } else if (value == "syntax_pg") {
                settings.PgParser = true;
            } else {
                issues.AddIssue(NYql::YqlIssue(NYql::TPosition(0, lineNumber), NYql::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown SQL translation setting: " << value));
                return false;
            }
        }

        return true;
    }

}  // namespace NSQLTranslation
