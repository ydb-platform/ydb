#pragma once

#include <cstring>

namespace NYql::NUuid {

struct TUuid {
    char Data[16]; // NOLINT(modernize-avoid-c-arrays)

    TUuid() {
        std::memset(Data, 0, sizeof(Data));
    }

    bool operator==(const TUuid& other) const {
        return std::memcmp(Data, other.Data, sizeof(Data)) == 0;
    }

    bool operator!=(const TUuid& other) const {
        return !(*this == other);
    }
};

static_assert(sizeof(TUuid) == 16);

} // namespace NYql::NUuid
