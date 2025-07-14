#pragma once

#include <util/generic/yexception.h>

namespace NYql {
namespace NUuid {

struct TUuid {
    char Data[16];

    TUuid() {
        std::memset(Data, 0, sizeof(Data));
    }

    template <typename T>
    TUuid(const T* data, size_t size) {
        if (size != sizeof(Data)) {
            ythrow yexception() << "TUuid: expected " << sizeof(Data) << ", got " << size << " bytes";
        }
        std::memcpy(Data, data, sizeof(Data));
    }

    bool operator==(const TUuid& rhs) const {
        return std::memcmp(Data, rhs.Data, sizeof(Data)) == 0;
    }
    bool operator<(const TUuid& rhs) const {
        return std::memcmp(Data, rhs.Data, sizeof(Data)) < 0;
    }
    bool operator>(const TUuid& rhs) const {
        return std::memcmp(Data, rhs.Data, sizeof(Data)) > 0;
    }
};

}
}
