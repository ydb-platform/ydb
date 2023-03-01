#pragma once

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <util/generic/vector.h>

namespace NYdb::NConsoleClient {

struct TRecursiveListSettings {
    using TSelf = TRecursiveListSettings;
    using TFilterOp = std::function<bool(const NScheme::TSchemeEntry&)>;

    FLUENT_SETTING_DEFAULT(NScheme::TListDirectorySettings, ListDirectorySettings, {});
    FLUENT_SETTING_DEFAULT(bool, SkipSys, true);
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
