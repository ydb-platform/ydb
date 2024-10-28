#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

namespace NYdb::NDump {

inline void AddPath(NYql::TIssues& issues, const TString& path) {
    issues.AddIssue(NYql::TIssue(TStringBuilder() << "Path: " << path)
        .SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO));
}

template <typename TResult>
inline TResult Result(const TMaybe<TString>& path = Nothing(), EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYql::TIssues issues;
    if (path) {
        AddPath(issues, *path);
    }
    if (error) {
        issues.AddIssue(NYql::TIssue(error));
    }
    return TResult(TStatus(code, std::move(issues)));
}

template <typename TResult>
inline TResult Result(EStatus code, const TString& error) {
    return Result<TResult>(Nothing(), code, error);
}

template <typename TResult>
inline TResult Result(const TString& path, TStatus&& status) {
    NYql::TIssues issues;
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
