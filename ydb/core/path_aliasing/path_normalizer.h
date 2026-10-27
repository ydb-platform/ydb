#pragma once

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
        explicit TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config);

        bool HasRules() const noexcept { return bool(Impl); }
        TString NormalizePath(TStringBuf path) const;

    private:
        struct TImpl;
        std::shared_ptr<const TImpl> Impl;
    };

    struct TPathNormalizerFunction {
        std::shared_ptr<const TPathNormalizer> Normalizer;

        TString operator()(TStringBuf path) const {
            return Normalizer->NormalizePath(path);
        }
    };

} // namespace NKikimr::NPathAliasing
