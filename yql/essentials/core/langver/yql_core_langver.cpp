#include "yql_core_langver.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <util/string/builder.h>

namespace NYql {

namespace {

void WriteVersion(TStringBuilder& builder, TLangVersion version) {
    TLangVersionBuffer buffer;
    TStringBuf str;
    if (!FormatLangVersion(version, buffer, str)) {
        builder << "unknown";
    } else {
        builder << str;
    }
}

}

bool CheckLangVersion(TLangVersion ver, TLangVersion max, TMaybe<TIssue>& issue) {
    if (ver != UnknownLangVersion && !IsValidLangVersion(ver)) {
        TStringBuilder builder;
        builder << "Invalid YQL language version: '";
        WriteVersion(builder, ver);
        builder << "'";
        issue = TIssue({}, builder);
        return false;
    }

    if (!IsAvailableLangVersion(ver, max)) {
        TStringBuilder builder;
        builder << "YQL language version '";
        WriteVersion(builder, ver);
        builder << "' is not available, maximum version is '";
        WriteVersion(builder, max);
        builder << "'";
        issue = TIssue({}, builder);
        return false;
    }

    if (IsDeprecatedLangVersion(ver, max)) {
        TStringBuilder builder;
        builder << "YQL language version '";
        WriteVersion(builder, ver);
        builder << "' is deprecated, consider to upgrade";
        issue = YqlIssue({}, EYqlIssueCode::TIssuesIds_EIssueCode_CORE_DEPRECATED_LANG_VER, builder);
        return true;
    }

    if (IsUnsupportedLangVersion(ver, max)) {
        TStringBuilder builder;
        builder << "YQL language version '";
        WriteVersion(builder, ver);
        builder << "' is unsupported, consider to upgrade. Queries may fail";
        issue = YqlIssue({}, EYqlIssueCode::TIssuesIds_EIssueCode_CORE_UNSUPPORTED_LANG_VER, builder);
        return true;
    }

    return true;
}

} // namespace NYql
