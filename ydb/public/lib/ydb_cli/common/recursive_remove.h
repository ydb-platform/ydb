#pragma once

#include "progress_bar.h"
#include "recursive_list.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/generic/vector.h>

#include <functional>
#include <memory>
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
    FLUENT_SETTING_DEFAULT(ERecursiveListSkipMode, SkipMode, ERecursiveListSkipMode::Internal);
    FLUENT_SETTING_DEFAULT(ERecursiveRemovePrompt, Prompt, ERecursiveRemovePrompt::Never);
    FLUENT_SETTING_DEFAULT(ui32, Threads, 16);
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
        const TDriver& driver,
        const TRemoveDirectoryRecursiveSettings& settings
    );

    bool MightHaveDependents(NScheme::ESchemeEntryType type);

    // Calls the remover for the entries from up to `threads` threads, the current thread included.
    // After the first failure no new entries are taken; the first error is returned.
    TStatus RemoveInParallel(
        const TVector<const NScheme::TSchemeEntry*>& entries,
        TRemover& remover,
        TProgressBar* progressBar,
        ui32 threads
    );

    // Splits the entries into batches that must be removed one after another;
    // the entries of one batch do not depend on each other and can be removed in parallel.
    // The first batch holds all objects that nothing depends on (tables, topics, external tables, ...).
    // The others hold containers (directories, column stores, backup collections) and external data sources,
    // one batch per path depth from the deepest one up, so every container is removed after all its descendants.
    TVector<TVector<const NScheme::TSchemeEntry*>> SplitIntoRemoveBatches(
        const TVector<const NScheme::TSchemeEntry*>& entries
    );

    template <typename TIterator>
    TStatus RemovePathsRecursive(
        TIterator begin,
        TIterator end,
        TRemover& remover,
        TProgressBar* progressBar,
        ui32 threads
    ) {
        TVector<const NScheme::TSchemeEntry*> entries;
        for (const NScheme::TSchemeEntry& entry : std::ranges::subrange(begin, end)) {
            entries.push_back(&entry);
        }
        for (const auto& batch : SplitIntoRemoveBatches(entries)) {
            if (auto result = RemoveInParallel(batch, remover, progressBar, threads); !result.IsSuccess()) {
                return result;
            }
        }
        return TStatus(EStatus::SUCCESS, {});
    }

}

template <typename TIterator>
TStatus RemovePathsRecursive(
    TIterator begin,
    TIterator end,
    NInternal::TRemover& remover,
    const TRemoveDirectoryRecursiveSettings& settings = {}
) {
    std::unique_ptr<TProgressBar> progressBar;
    if (settings.CreateProgressBar_) {
        progressBar = std::make_unique<TProgressBar>(end - begin);
    }
    // questions about every object must be asked one by one
    const ui32 threads = settings.Prompt_ == ERecursiveRemovePrompt::Always ? 1 : settings.Threads_;
    return NInternal::RemovePathsRecursive(begin, end, remover, progressBar.get(), threads);
}

}
