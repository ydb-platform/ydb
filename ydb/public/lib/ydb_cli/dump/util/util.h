#pragma once

#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/generic/maybe.h>

namespace NYdb {
namespace NDump {

template <typename TResult>
inline TResult Result(EStatus code = EStatus::SUCCESS, const TString& error = {}) {
    NYql::TIssues issues;
    if (error) {
        issues.AddIssue(NYql::TIssue(error));
    }
    return TResult(TStatus(code, std::move(issues))); 
}

template <typename TResult>
inline TResult Result(TStatus&& status) {
    return TResult(std::move(status));
}

TStatus DescribeTable(NTable::TTableClient& tableClient, const TString& path, TMaybe<NTable::TTableDescription>& out);

void ExponentialBackoff(TDuration& sleep, TDuration max = TDuration::Minutes(5));

NScheme::TDescribePathResult DescribePath(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TDescribePathSettings& settings = {});

TStatus MakeDirectory(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TMakeDirectorySettings& settings = {});

TStatus RemoveDirectory(
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TRemoveDirectorySettings& settings = {});

TStatus RemoveDirectoryRecursive(
    NTable::TTableClient& tableClient,
    NScheme::TSchemeClient& schemeClient,
    const TString& path,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true);

} // NDump
} // NYdb
