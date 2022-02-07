#pragma once

#include "traits.h"

namespace NYsonPull {
    namespace NDetail {
        namespace NZigZag {
            //! Functions that provide coding of integers with property: 0 <= f(x) <= 2 * |x|

            template <typename TSigned>
            inline NTraits::to_unsigned<TSigned> encode(TSigned x) {
                using TUnsigned = NTraits::to_unsigned<TSigned>;
                constexpr auto rshift = sizeof(TSigned) * 8 - 1;
                return (static_cast<TUnsigned>(x) << 1) ^ static_cast<TUnsigned>(x >> rshift);
            }

            template <typename TUnsigned>
            inline NTraits::to_signed<TUnsigned> decode(TUnsigned x) {
                using TSigned = NTraits::to_signed<TUnsigned>;
                return static_cast<TSigned>(x >> 1) ^ -static_cast<TSigned>(x & 1);
            }
        }
    }     // namespace NDetail
}
