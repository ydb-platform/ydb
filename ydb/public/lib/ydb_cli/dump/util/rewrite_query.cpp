#include "rewrite_query.h"

#include <util/string/builder.h>

#include <re2/re2.h>

#include <format>

namespace NYdb::NDump {

bool RewriteCreateQuery(TString& query, std::string_view pattern, const std::string& dbPath, NYql::TIssues& issues) {
    const auto searchPattern = std::vformat(pattern, std::make_format_args("\\S+"));
    if (re2::RE2::Replace(&query, searchPattern, std::vformat(pattern, std::make_format_args(dbPath)))) {
        return true;
    }

    issues.AddIssue(TStringBuilder() << "Pattern: \"" << pattern << "\" was not found: " << query.Quote());
    return false;
}

}
