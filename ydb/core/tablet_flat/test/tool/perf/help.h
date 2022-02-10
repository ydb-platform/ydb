#pragma once

#include <tuple>
#include <cstddef>

namespace NKikiSched {

    namespace NHelp {

        template<size_t ...> struct TIndexes { };

        template<size_t N, size_t ...Obtained>
        struct TGenIx : TGenIx<N - 1, N - 1, Obtained...> { };

        template<size_t ...Obtained>
        struct TGenIx<0, Obtained...>
        {
            using Type = TIndexes<Obtained...>;
        };

        template<typename TAr1, typename TAr2>
        static inline void IsArrSized(const TAr1&, const TAr2&) noexcept
        {
            constexpr auto nums = std::tuple_size<TAr1>::value;

            static_assert(nums == std::tuple_size<TAr1>::value, "");
        }
    }

}
