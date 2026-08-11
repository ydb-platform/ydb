#pragma once

#include "regex_predicate.h"

#include <optional>


namespace NKikimr::NResourcePool {

inline constexpr i64 CLASSIFIER_RANK_OFFSET = 1000;
inline constexpr i64 CLASSIFIER_COUNT_LIMIT = 1000;

enum class EClassifierAction {
    Reject /* "reject" */,
};

struct TClassifierSettings {
    bool operator==(const TClassifierSettings& other) const = delete;

    [[nodiscard]] std::optional<TString> Validate() const;

    i64 Rank = -1;  // -1 = max rank + CLASSIFIER_RANK_OFFSET
    std::optional<TString> ResourcePool; // absent when Action is Reject
    std::optional<TString> MemberName;
    std::optional<TString> HasAppName;
    std::optional<TRegexPredicate> HasFullScan;
    std::optional<TRegexPredicate> HasPath;
    std::optional<bool> HasStream;
    std::optional<EClassifierAction> Action;
};

}  // namespace NKikimr::NResourcePool
