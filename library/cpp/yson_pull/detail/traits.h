#pragma once

#include <type_traits>

namespace NYsonPull {
    namespace NDetail {
        namespace NTraits {
            template <typename T, typename U>
            using if_signed = typename std::enable_if<
                std::is_signed<T>::value,
                U>::type;

            template <typename T, typename U>
            using if_unsigned = typename std::enable_if<
                std::is_unsigned<T>::value,
                U>::type;

            template <typename T>
            using to_unsigned = typename std::enable_if<
                std::is_signed<T>::value,
                typename std::make_unsigned<T>::type>::type;

            template <typename T>
            using to_signed = typename std::enable_if<
                std::is_unsigned<T>::value,
                typename std::make_signed<T>::type>::type;
        }
    }     // namespace NDetail
}
