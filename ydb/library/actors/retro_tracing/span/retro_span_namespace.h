#pragma once

#include <util/system/types.h>

namespace NRetroTracing {

struct TSpanTypeNamespace {
    enum E : ui32 {
        UNINITIALIZED = 0,
        INTERCONNECT = 1,
        USERSPACE = 4096,
        END = (1 << 15),
    };

    constexpr static ui32 Size = 1 << 16;

    constexpr static ui32 Begin(E namespaceId) {
        return static_cast<ui32>(namespaceId) * Size;
    }

    constexpr static ui32 End(E namespaceId) {
        return Begin(namespaceId) + Size;
    }

    constexpr static E Get(ui32 spanType) {
        return static_cast<E>(spanType / Size);
    }
};

} // namespace NRetroTracing
