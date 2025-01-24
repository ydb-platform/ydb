#pragma once

#include <ydb-cpp-sdk/client/types/status/status.h>
#include <ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NYdb::NDump {

inline void AddPath(NYdb::NIssue::TIssues& issues, const TString& path) {
    issues.AddIssue(NYdb::NIssue::TIssue(TStringBuilder() << "Path: " << path)
        .SetCode(NYdb::NIssue::DEFAULT_ERROR, NYdb::NIssue::ESeverity::Info));
}

template <typename TResult>
inline TResult Result(const TMaybe<TString>& path = Nothing(), EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYdb::NIssue::TIssues issues;
    if (path) {
        AddPath(issues, *path);
    }
    if (error) {
        issues.AddIssue(NYdb::NIssue::TIssue(error));
    }
    return TResult(TStatus(code, std::move(issues)));
}

template <typename TResult>
inline TResult Result(EStatus code, const TString& error) {
    return Result<TResult>(Nothing(), code, error);
}

template <typename TResult>
inline TResult Result(const TString& path, TStatus&& status) {
    NYdb::NIssue::TIssues issues;
    AddPath(issues, path);
    issues.AddIssues(status.GetIssues());
    return TResult(TStatus(status.GetStatus(), std::move(issues)));
}

TStatus DescribeTable(NTable::TTableClient& tableClient, const TString& path, TMaybe<NTable::TTableDescription>& out);

NScheme::TDescribePathResult DescribePath(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TDescribePathSettings& settings = {});

TStatus MakeDirectory(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TMakeDirectorySettings& settings = {});

TStatus ModifyPermissions(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TModifyPermissionsSettings& settings = {});
}
