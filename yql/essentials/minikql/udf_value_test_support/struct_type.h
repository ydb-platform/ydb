#pragma once

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>

#include <utility>
#include <algorithm>
#include <array>
#include <tuple>

namespace NYql::NUdf::NTest {

template <size_t N>
struct TStructMemberName {
    char Data[N];
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr TStructMemberName(const char (&str)[N]) {
        std::copy_n(str, N, Data);
    }
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator std::string_view() const {
        return std::string_view{Data, N - 1};
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator TStringBuf() const {
        return TStringBuf{Data, N - 1};
    }
};

template <TStructMemberName Name, typename T>
struct TStructMember {
    T Value;
    static constexpr TStringBuf MemberName() {
        return Name;
    }
};

template <typename... TMembers>
struct TStructType {
private:
    static consteval std::array<size_t, sizeof...(TMembers)> GetSortedIndexMapping() {
        constexpr std::array<TStringBuf, sizeof...(TMembers)> names = {
            TMembers::MemberName()...};

        std::array<size_t, sizeof...(TMembers)> indices{};
        for (size_t i = 0; i < indices.size(); ++i) {
            indices[i] = i;
        }

        std::sort(indices.begin(), indices.end(), [&](size_t lhs, size_t rhs) {
            return names[lhs] < names[rhs];
        });

        return indices;
    }

public:
    static constexpr auto SortedIndexMapping = GetSortedIndexMapping();

    std::tuple<TMembers...> Members;
};

} // namespace NYql::NUdf::NTest
