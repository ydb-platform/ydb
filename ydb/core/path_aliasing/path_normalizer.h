#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

#include <memory>

namespace NKikimrConfig {
    class TPathRewriteConfig;
} // namespace NKikimrConfig

namespace NKikimr::NPathAliasing {

    // Structural validation of changed rewrite outputs, before owner validation.
    bool IsValidRewrittenPath(TStringBuf path) noexcept;

    // Immutable startup rules. Callers supply a logical absolute schema path and
    // retain the result as a resolved path; normalization is deliberately one-pass.
    class TPathNormalizer {
    public:
        TPathNormalizer() noexcept = default;
        explicit TPathNormalizer(const NKikimrConfig::TPathRewriteConfig& config);

        bool Empty() const noexcept;
        // False leaves output untouched; an identity match still returns true.
        bool TryRewritePath(TStringBuf absoluteLogicalPath, TString& output) const;
        TString NormalizePath(TStringBuf absoluteLogicalPath) const;
        TString GetFingerprint() const;

    private:
        struct TImpl;
        std::shared_ptr<const TImpl> Impl;
    };

} // namespace NKikimr::NPathAliasing
