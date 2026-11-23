#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

enum class ERecursiveListSkipMode {
    // Apply no system/backup exclusions (e.g. include .sys and ~backup_12345).
    // Other traversal filters still apply.
    None,
    // Skip directories starting with .sys (e.g. .sys_health), .metadata, .tmp,
    // or .backups, and entries of type SysView.
    System,
    // Skip system objects and temporary directories starting with ~backup_ or export-
    // (e.g. ~backup_12345 or export-12345). Other ~-prefixed names are traversed.
    Internal,
};

struct TRecursiveListSettings {
    using TSelf = TRecursiveListSettings;
    using TFilterOp = std::function<bool(const NScheme::TSchemeEntry&)>;

    FLUENT_SETTING_DEFAULT(NScheme::TListDirectorySettings, ListDirectorySettings, {});
    FLUENT_SETTING_DEFAULT(ERecursiveListSkipMode, SkipMode, ERecursiveListSkipMode::Internal);
    FLUENT_SETTING_DEFAULT(bool, SkipOldSecrets, true);
    FLUENT_SETTING_DEFAULT(TFilterOp, Filter, &NopFilter);

private:
    static inline bool NopFilter(const NScheme::TSchemeEntry&) {
        return true;
    }
};

struct TRecursiveListResult {
    TVector<NScheme::TSchemeEntry> Entries; // name of the entry contains full path
    TStatus Status; // last successful or first unsuccessful status
};

TRecursiveListResult RecursiveList(NScheme::TSchemeClient& client, const TString& path,
    const TRecursiveListSettings& settings = {}, bool addSelf = true);

}
