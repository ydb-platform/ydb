#pragma once

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/typetraits.h>
#include <util/generic/vector.h>

#include <string>

namespace NYdb::NConsoleClient {

Y_HAS_MEMBER(SrcPath);
Y_HAS_MEMBER(SrcPathDb);

inline bool IsExcluded(const char* str, const TVector<TRegExMatch>& exclusionPatterns) {
    for (const auto& pattern : exclusionPatterns) {
        if (pattern.Match(str)) {
            return true;
        }
    }

    return false;
}

inline bool IsExcluded(const std::string& str, const TVector<TRegExMatch>& exclusionPatterns) {
    return IsExcluded(str.c_str(), exclusionPatterns);
}

template <typename TItem>
bool IsItemExcluded(const TItem& item, const TVector<TRegExMatch>& exclusionPatterns) {
    if (IsExcluded(item.Src, exclusionPatterns)) {
        return true;
    }

    if constexpr (THasSrcPath<TItem>::value) {
        if (!item.SrcPath.empty() && IsExcluded(item.SrcPath, exclusionPatterns)) {
            return true;
        }
    }

    if constexpr (THasSrcPathDb<TItem>::value) {
        if (!item.SrcPathDb.empty() && IsExcluded(item.SrcPathDb, exclusionPatterns)) {
            return true;
        }
    }

    return false;
}

template <typename TSettings>
void ExcludeItems(TSettings& settings, const TVector<TRegExMatch>& exclusionPatterns) {
    auto items(std::move(settings.Item_));
    for (auto& item : items) {
        if (IsItemExcluded(item, exclusionPatterns)) {
            continue;
        }

        settings.AppendItem(std::move(item));
    }
}

}
