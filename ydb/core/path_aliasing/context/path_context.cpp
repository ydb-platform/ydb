#include "path_context.h"

#include <ydb/core/base/path.h>

#include <utility>

namespace NKikimr::NPathAliasing {

    TPathContext::TPathContext(TPathNormalizer normalizer, TMaybe<TString> rawDatabase)
        : Normalizer_(std::move(normalizer))
        , LogicalDatabase_(std::move(rawDatabase))
        , Database_(LogicalDatabase_)
    {
        if (Normalizer_.Empty() || !LogicalDatabase_ || LogicalDatabase_->empty()) {
            return;
        }
        TString candidate = CanonizePath(*LogicalDatabase_);
        // CanonizePath represents an all-slash root as empty, but it is not an
        // absent database operand and must remain distinguishable for matching.
        if (candidate.empty()) {
            candidate = "/";
        }
        auto result = NormalizePath(candidate);
        if (result.IsFail()) {
            Error_ = result.GetErrorMessage();
        } else if (result->Outcome == EPathRewriteOutcome::Rewritten) {
            Database_ = std::move(result.DetachResult().Path);
        }
    }

    const TMaybe<TString>& TPathContext::GetLogicalDatabase() const noexcept {
        return LogicalDatabase_;
    }

    const TMaybe<TString>& TPathContext::GetDatabase() const noexcept {
        return Database_;
    }

    const TString& TPathContext::GetError() const noexcept {
        return Error_;
    }

    TString TPathContext::GetFingerprint() const {
        return Normalizer_.GetFingerprint();
    }

    bool TPathContext::Empty() const noexcept {
        return Normalizer_.Empty();
    }

    TConclusion<TResolvedSchemaPath> TPathContext::NormalizePath(const TString& completeLogicalCandidate) const {
        TString rewritten;
        if (completeLogicalCandidate.empty() ||
            !Normalizer_.TryRewritePath(completeLogicalCandidate, rewritten)) {
            return TResolvedSchemaPath{completeLogicalCandidate, EPathRewriteOutcome::NoMatch};
        }
        if (rewritten == completeLogicalCandidate) {
            return TResolvedSchemaPath{completeLogicalCandidate, EPathRewriteOutcome::Identity};
        }
        if (!IsValidRewrittenPath(rewritten)) {
            return TConclusionStatus::Fail(
                "Path rewrite produced an invalid target: expected a nonempty absolute schema path "
                "without NUL, '.' or '..' components");
        }
        rewritten = CanonizePath(rewritten);
        if (rewritten.empty()) {
            rewritten = "/";
        }
        return TResolvedSchemaPath{std::move(rewritten), EPathRewriteOutcome::Rewritten};
    }

} // namespace NKikimr::NPathAliasing
