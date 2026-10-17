#pragma once

#include <util/generic/string.h>

#include <shared_mutex>

namespace NKikimr::NMetadata::NProvider {

class TConfig;

class TServiceOperator {
private:
    friend class TService;
    std::shared_mutex Lock;
    bool EnabledFlag = false;
    TString Path = ".metadata";
    static void Register(const TConfig& config);
public:
    static bool IsEnabled();
    static TString GetPath();
};

} // namespace NKikimr::NMetadata::NProvider
