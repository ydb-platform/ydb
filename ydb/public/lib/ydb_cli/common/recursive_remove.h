#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYdb::NConsoleClient {

enum class ERecursiveRemovePrompt {
    Always,
    Once,
    Never,
};

struct TRemovePathRecursiveSettings : public NScheme::TRemoveDirectorySettings {
    FLUENT_SETTING_DEFAULT(bool, NotExistsIsOk, false);
};

bool Prompt(ERecursiveRemovePrompt mode, const TString& path, NScheme::ESchemeEntryType type, bool first = true);

TStatus RemoveDirectoryRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    const TString& path,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true,
    bool createProgressBar = true);

TStatus RemoveDirectoryRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    NTopic::TTopicClient* topicClient,
    NQuery::TQueryClient* queryClient,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true,
    bool createProgressBar = true);

TStatus RemovePathRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    NTopic::TTopicClient* topicClient,
    NQuery::TQueryClient* queryClient,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const TRemovePathRecursiveSettings& settings = {},
    bool createProgressBar = true);

}
