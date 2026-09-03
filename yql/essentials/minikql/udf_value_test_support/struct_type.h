#pragma once

#include <util/generic/fwd.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>

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
    constexpr explicit TStructMemberName(char fill) {
        std::fill_n(Data, N - 1, fill);
        Data[N - 1] = '\0';
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
    using TValueType = T;

    T Value;
    static constexpr TStringBuf MemberName() {
        return Name;
    }
};

template <typename... TMembers>
struct TStructType {
private:
    static consteval bool HasUniqueNames() {
        std::array<TStringBuf, sizeof...(TMembers)> names = {
            TMembers::MemberName()...};
        std::ranges::sort(names);
        return std::ranges::unique(names).empty();
    }

    static_assert(HasUniqueNames(), "TStructType members must have unique names");

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

    static consteval std::array<size_t, sizeof...(TMembers)> GetOriginalIndexMapping() {
        const auto sorted = GetSortedIndexMapping();
        std::array<size_t, sizeof...(TMembers)> inverse{};
        for (size_t sortedIdx = 0; sortedIdx < sorted.size(); ++sortedIdx) {
            inverse[sorted[sortedIdx]] = sortedIdx;
        }
        return inverse;
    }

public:
    static constexpr size_t FindMemberIndexByName(TStringBuf name) {
        constexpr std::array<TStringBuf, sizeof...(TMembers)> names = {
            TMembers::MemberName()...};
        for (size_t i = 0; i < names.size(); ++i) {
            if (names[i] == name) {
                return i;
            }
        }
        throw yexception() << "struct member not found in other struct type: " << name;
    }

    static constexpr auto SortedIndexMapping = GetSortedIndexMapping();
    static constexpr auto OriginalIndexMapping = GetOriginalIndexMapping();

    std::tuple<TMembers...> Members;
};

} // namespace NYql::NUdf::NTest
