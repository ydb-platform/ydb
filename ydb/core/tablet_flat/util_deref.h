#pragma once

#include <util/system/compiler.h>
#include <type_traits>

namespace NKikimr {

    template<typename T>
    using TStdLayout = std::enable_if_t<std::is_standard_layout<T>::value, T>;

    template<typename T>
    using TStdLayoutOrVoid = std::enable_if_t<std::is_standard_layout<T>::value || std::is_void<T>::value, T>;

    template<typename T, typename = TStdLayout<T>>
    struct TDeref {
        static constexpr const T* At(const void *ptr, size_t off = 0) noexcept {
            return
                reinterpret_cast<const T*>(static_cast<const char*>(ptr) + off);
        }

        static constexpr T* At(void *ptr, size_t off = 0) noexcept {
            return reinterpret_cast<T*>(static_cast<char*>(ptr) + off);
        }

        static inline T Copy(const void *ptr, size_t off = 0) noexcept
        {
            T object;
            memcpy(&object, static_cast<const char*>(ptr) + off, sizeof(T));
            return object;
        }
    };

    template<typename T, typename = TStdLayout<T>>
    struct TStdPad {
        inline T operator*() const {
            return TDeref<T>::Copy(&Pad, 0);
        }

        const char Pad[sizeof(T)];
    } Y_PACKED;

}
