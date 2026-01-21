#pragma once

#include <util/stream/output.h>
#include <util/string/cast.h>

namespace NYql::NWindow {

enum class EDirection {
    Preceding,
    Following,
};

constexpr EDirection InvertDirection(EDirection direction) {
    return direction == EDirection::Preceding ? EDirection::Following : EDirection::Preceding;
}

inline IOutputStream& operator<<(IOutputStream& out, EDirection direction) {
    switch (direction) {
        case EDirection::Preceding:
            return out << "Preceding";
        case EDirection::Following:
            return out << "Following";
    }
}

inline TString DirectionToString(EDirection direction) {
    switch (direction) {
        case EDirection::Preceding:
            return "Preceding";
        case EDirection::Following:
            return "Following";
    }
}

inline bool TryParseDirectionFromString(const TStringBuf& str, EDirection& direction) {
    if (str == "Preceding") {
        direction = EDirection::Preceding;
        return true;
    } else if (str == "Following") {
        direction = EDirection::Following;
        return true;
    }
    return false;
}

} // namespace NYql::NWindow
