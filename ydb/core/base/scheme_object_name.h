#pragma once

#include <ydb/core/base/table_index.h>

#include <util/generic/string.h>

#include <cctype>

namespace NKikimr {

inline bool IsValidSchemeObjectName(const TString& name, bool allowSystemNames = false) {
    if (!allowSystemNames && name.StartsWith(SYSTEM_COLUMN_PREFIX)) {
        return false;
    }

    for (auto&& c : name) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_' && c != '-') {
            return false;
        }
    }

    return true;
}

}   // namespace NKikimr
