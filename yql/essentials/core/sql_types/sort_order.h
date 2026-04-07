#pragma once

#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NYql {

enum class ESortOrder {
    Asc,
    Desc,
    Unimportant,
};

inline IOutputStream& operator<<(IOutputStream& out, ESortOrder order) {
    switch (order) {
        case ESortOrder::Asc:
            return out << "Asc";
        case ESortOrder::Desc:
            return out << "Desc";
        case ESortOrder::Unimportant:
            return out << "Unimportant";
    }
}

inline bool TryParseSortOrderFromString(TStringBuf str, ESortOrder& order) {
    if (str == "Asc") {
        order = ESortOrder::Asc;
        return true;
    } else if (str == "Desc") {
        order = ESortOrder::Desc;
        return true;
    } else if (str == "Unimportant") {
        order = ESortOrder::Unimportant;
        return true;
    }
    return false;
}

} // namespace NYql
