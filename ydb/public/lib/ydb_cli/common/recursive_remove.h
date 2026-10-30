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

    template <typename TIterator>
    TStatus RemovePathsRecursive(
        TIterator begin,
        TIterator end,
        TRemover& remover,
        TProgressBar* progressBar
    ) {
        TVector<const NScheme::TSchemeEntry*> entriesToRemoveInSecondPass;
        for (const NScheme::TSchemeEntry& entry : std::ranges::subrange(begin, end)) {
            if (MightHaveDependents(entry.Type)) {
                entriesToRemoveInSecondPass.emplace_back(&entry);
                continue;
            }
            auto result = remover(entry);
            if (!result.IsSuccess()) {
                return result;
            }
            if (progressBar) {
                progressBar->AddProgress(1);
            }
        }
        for (const auto* entry : entriesToRemoveInSecondPass) {
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
    TIterator begin,
    TIterator end,
    NInternal::TRemover& remover,
    const TRemoveDirectoryRecursiveSettings& settings = {}
) {
    std::unique_ptr<TProgressBar> progressBar;
    if (settings.CreateProgressBar_) {
        progressBar = std::make_unique<TProgressBar>(end - begin);
    }
    return NInternal::RemovePathsRecursive(begin, end, remover, progressBar.get());
}

}
