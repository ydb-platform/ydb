#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

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
    NTopic::TTopicClient& topicClient,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const NScheme::TRemoveDirectorySettings& settings = {},
    bool removeSelf = true,
    bool createProgressBar = true);

TStatus RemovePathRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    NTopic::TTopicClient& topicClient,
    const TString& path,
    ERecursiveRemovePrompt prompt,
    const TRemovePathRecursiveSettings& settings = {},
    bool createProgressBar = true);

}
