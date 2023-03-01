#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NYdb::NConsoleClient {

TStatus RemoveDirectoryRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    const TString& path,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true);

TStatus RemoveDirectoryRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    NTopic::TTopicClient& topicClient,
    const TString& path,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true);

}
