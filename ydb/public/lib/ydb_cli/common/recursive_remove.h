#pragma once

#include "progress_bar.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ranges>

namespace NYdb::NConsoleClient {

enum class ERecursiveRemovePrompt {
    Always,
    Once,
    Never,
};

struct TRemoveDirectoryRecursiveSettings : public NScheme::TRemoveDirectorySettings {
    using TSelf = TRemoveDirectoryRecursiveSettings;

    FLUENT_SETTING_DEFAULT(bool, NotExistsIsOk, false);
    FLUENT_SETTING_DEFAULT(bool, RemoveSelf, true);
    FLUENT_SETTING_DEFAULT(bool, CreateProgressBar, false);
    FLUENT_SETTING_DEFAULT(ERecursiveRemovePrompt, Prompt, ERecursiveRemovePrompt::Never);
};

bool Prompt(ERecursiveRemovePrompt mode, const TString& path, NScheme::ESchemeEntryType type, bool first = true);

TStatus RemoveDirectoryRecursive(
    const TDriver& driver,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings = {}
);

TStatus RemovePathRecursive(
    const TDriver& driver,
    const TString& path,
    const TRemoveDirectoryRecursiveSettings& settings = {}
);

namespace NInternal {

    using TRemover = std::function<TStatus(const NScheme::TSchemeEntry&)>;

    TRemover CreateDefaultRemover(
        NScheme::TSchemeClient& schemeClient,
        NTable::TTableClient& tableClient,
        NTopic::TTopicClient& topicClient,
        NQuery::TQueryClient& queryClient,
        NCoordination::TClient& coordinationClient,
        const TRemoveDirectoryRecursiveSettings& settings
    );

    bool MightHaveDependents(NScheme::ESchemeEntryType type);

    // Removes entries in parallel using a thread pool.
    // Falls back to sequential execution for single-entry batches.
    TStatus RemoveEntriesParallel(
        const TVector<const NScheme::TSchemeEntry*>& entries,
        TRemover& remover,
        TProgressBar* progressBar
    );

    // Removes entries grouped by path depth, deepest first (for correct directory ordering).
    // Within each depth level, entries are removed in parallel.
    TStatus RemoveEntriesByDepth(
        const TVector<const NScheme::TSchemeEntry*>& entries,
        TRemover& remover,
        TProgressBar* progressBar
    );

    template <typename TIterator>
    TStatus RemovePathsRecursive(
        TIterator begin,
        TIterator end,
        TRemover& remover,
        TProgressBar* progressBar,
        bool parallel = true
    ) {
        TVector<const NScheme::TSchemeEntry*> pass1, pass2;
        for (const NScheme::TSchemeEntry& entry : std::ranges::subrange(begin, end)) {
            if (MightHaveDependents(entry.Type)) {
                pass2.push_back(&entry);
            } else {
                pass1.push_back(&entry);
            }
        }

        if (parallel) {
            if (auto result = RemoveEntriesParallel(pass1, remover, progressBar); !result.IsSuccess()) {
                return result;
            }
            return RemoveEntriesByDepth(pass2, remover, progressBar);
        }

        for (const auto* entry : pass1) {
            auto result = remover(*entry);
            if (!result.IsSuccess()) {
                return result;
            }
            if (progressBar) {
                progressBar->AddProgress(1);
            }
        }
        for (const auto* entry : pass2) {
            auto result = remover(*entry);
            if (!result.IsSuccess()) {
                return result;
            }
            if (progressBar) {
                progressBar->AddProgress(1);
            }
        }
        return TStatus(EStatus::SUCCESS, {});
    }

}

template <typename TIterator>
TStatus RemovePathsRecursive(
    NScheme::TSchemeClient& schemeClient,
    NTable::TTableClient& tableClient,
    NTopic::TTopicClient& topicClient,
    NQuery::TQueryClient& queryClient,
    NCoordination::TClient& coordinationClient,
    TIterator begin,
    TIterator end,
    const TRemoveDirectoryRecursiveSettings& settings = {},
    // overloading of the remover is used only for tests
    NInternal::TRemover&& remover = {}
) {
    if (!remover) {
        remover = NInternal::CreateDefaultRemover(schemeClient, tableClient, topicClient, queryClient, coordinationClient, settings);
    }
    std::unique_ptr<TProgressBar> progressBar;
    if (settings.CreateProgressBar_) {
        progressBar = std::make_unique<TProgressBar>(end - begin);
    }
    const bool parallel = (settings.Prompt_ != ERecursiveRemovePrompt::Always);
    return NInternal::RemovePathsRecursive(begin, end, remover, progressBar.get(), parallel);
}

}
