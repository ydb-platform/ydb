#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>

namespace NKikimrConfig {
    class TPathRewriteConfig;
} // namespace NKikimrConfig

namespace NKikimr::NPathAliasing {

    class TPathNormalizer {
    public:
        TPathNormalizer() noexcept = default;
        explicit TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config,
            const NMonitoring::TDynamicCounterPtr& counters = {});

        bool HasRules() const noexcept { return bool(Impl); }
        TString NormalizePath(TStringBuf path) const;

    private:
        struct TImpl;
        std::shared_ptr<const TImpl> Impl;
    };

} // namespace NKikimr::NPathAliasing
