#pragma once

#include "struct_type.h"

#include <yql/essentials/utils/random_data_generator/random_data_generator.h>

#include <util/generic/strbuf.h>

#include <array>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace NYql::NUdf::NTest {

template <typename... TMembers>
class TStructVariant {
    template <typename TMember>
    using TMemberValue = std::remove_cvref_t<decltype(std::declval<TMember>().Value)>;

    using TValues = std::variant<TMemberValue<TMembers>...>;
    static constexpr std::array<TStringBuf, sizeof...(TMembers)> Names{TMembers::MemberName()...};

public:
    TStructVariant() = default;

    template <typename TMember>
        requires(std::disjunction_v<std::is_same<TMember, TMembers>...>)
    explicit TStructVariant(TMember member)
        : Values_(std::in_place_index<IndexOfMember<TMember>()>, std::move(member.Value))
    {
    }

    TStringBuf Name() const {
        return Names[Values_.index()];
    }

    template <TStructMemberName MemberName>
    const auto& Value() const {
        return std::get<IndexOfName<MemberName>()>(Values_);
    }

    template <typename TVisitor>
    decltype(auto) VisitActive(TVisitor&& visitor) const {
        return std::visit(std::forward<TVisitor>(visitor), Values_);
    }

private:
    template <typename TMember>
    static constexpr size_t IndexOfMember() {
        constexpr std::array<bool, sizeof...(TMembers)> match{std::is_same_v<TMember, TMembers>...};
        for (size_t index = 0; index < match.size(); ++index) {
            if (match[index]) {
                return index;
            }
        }
        return match.size();
    }

    template <TStructMemberName MemberName>
    static constexpr size_t IndexOfName() {
        constexpr TStringBuf name = MemberName;
        for (size_t index = 0; index < Names.size(); ++index) {
            if (Names[index] == name) {
                return index;
            }
        }
        return Names.size();
    }

    TValues Values_;
};

} // namespace NYql::NUdf::NTest

namespace NKikimr::NMiniKQL::NPrivate {

template <typename... TMembers>
struct TRandomDataGenerator<NYql::NUdf::NTest::TStructVariant<TMembers...>> {
    using TVariant = NYql::NUdf::NTest::TStructVariant<TMembers...>;
    using TValues = std::variant<std::remove_cvref_t<decltype(std::declval<TMembers>().Value)>...>;
    using TSettings = typename TRandomDataGenerator<TValues>::TSettings;

    static TVariant Generate(IRandomProvider& provider, const TSettings& settings) {
        const TValues values = TRandomDataGenerator<TValues>::Generate(provider, settings);
        TVariant result;
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            Y_UNUSED(((values.index() == Is && (result = FromMember<Is>(std::get<Is>(values)), true)) || ...));
        }(std::index_sequence_for<TMembers...>{});
        return result;
    }

private:
    template <size_t Index, typename TValue>
    static TVariant FromMember(const TValue& value) {
        using TMember = std::tuple_element_t<Index, std::tuple<TMembers...>>;
        return TVariant(TMember{value});
    }
};

} // namespace NKikimr::NMiniKQL::NPrivate
