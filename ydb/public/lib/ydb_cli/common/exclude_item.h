#pragma once

#include <library/cpp/regex/pcre/regexp.h>

#include <util/generic/vector.h>

#include <string>

namespace NYdb::NConsoleClient {

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

template <typename TSettings>
void ExcludeItems(TSettings& settings, const TVector<TRegExMatch>& exclusionPatterns) {
    auto items(std::move(settings.Item_));
    for (const auto& item : items) {
        if (IsExcluded(item.Src, exclusionPatterns)) {
            continue;
        }

        settings.AppendItem({item.Src, item.Dst});
    }
}

}
