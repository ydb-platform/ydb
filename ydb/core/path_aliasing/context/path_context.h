#pragma once

#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/library/conclusion/result.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NKikimr::NPathAliasing {

    enum class EPathRewriteOutcome {
        NoMatch,
        Identity,
        Rewritten,
    };

    struct TResolvedSchemaPath {
        TString Path;
        EPathRewriteOutcome Outcome = EPathRewriteOutcome::NoMatch;
    };

    // Immutable request namespace. Owning adapters, not this context, determine
    // which lexical forms their protocol accepts and how relative paths resolve.
    class TPathContext {
    public:
        TPathContext(TPathNormalizer normalizer, TMaybe<TString> rawDatabase);

        const TMaybe<TString>& GetLogicalDatabase() const noexcept;
        const TMaybe<TString>& GetDatabase() const noexcept;
        const TString& GetError() const noexcept;
        TString GetFingerprint() const;
        bool Empty() const noexcept;

        TConclusion<TResolvedSchemaPath> NormalizePath(const TString& completeLogicalCandidate) const;

    private:
        TPathNormalizer Normalizer_;
        TMaybe<TString> LogicalDatabase_;
        TMaybe<TString> Database_;
        TString Error_;
    };

} // namespace NKikimr::NPathAliasing
