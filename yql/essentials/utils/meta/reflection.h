#pragma once

#include "ensure.h"
#include "preprocessor.h"
#include "small_string.h"
#include "struct.h"

#include <util/generic/strbuf.h>

#include <algorithm>
#include <cstddef>
#include <tuple>
#include <type_traits>
#include <utility>

namespace NYql::NReflection {

namespace NDetail {

template <typename T, typename... Ts>
concept CUnique = ((0 + ... + std::same_as<T, Ts>) == 1);

} // namespace NDetail

template <typename T>
struct TReflection;

template <typename T>
concept CReflecting = requires { TReflection<T>::SelfType(); };

template <TSmallString NameValue, auto MemberPtr>
class TCppField final {
public:
    using TMemberPtr = decltype(MemberPtr);

    static constexpr auto Name = NameValue;

    static_assert(!Name.empty());
    static_assert(std::is_member_object_pointer_v<TMemberPtr>);

    template <typename TReceiver>
    constexpr decltype(auto) Get(TReceiver&& receiver) const noexcept {
        return std::forward<TReceiver>(receiver).*MemberPtr;
    }
};

template <typename TStruct, size_t N, typename... TFields>
class TCppStruct final {
public:
    consteval TCppStruct(std::type_identity<TStruct>, TSmallString<N> name, TFields... fields)
        : Name_(name)
        , Fields_(std::move(fields)...)
    {
        static_assert(std::is_aggregate_v<TStruct>);
        static_assert((std::invocable<typename TFields::TMemberPtr, TStruct&> && ...));
        static_assert((NDetail::CUnique<TFields, TFields...> && ...));
        Ensure(AreFieldNamesUnique());
        Ensure(FieldsCount() == NYql::FieldsCount<TStruct>());
    }

    constexpr const TSmallString<N>& Name() const noexcept {
        return Name_;
    }

    template <typename TAction>
    constexpr void ForEachField(TAction f) const {
        ForEachFieldUnsafe([&]<size_t Index>(const auto& field) {
            Y_UNUSED(Index);
            f(field);
        });
    }

    template <typename TReceiver, typename TAction>
    constexpr void ForEachFieldValue(TReceiver&& receiver, TAction f) const {
        ForEachFieldUnsafe([&]<size_t Index>(const auto& field) {
            using TField = std::decay_t<decltype(field)>;
            f.template operator()<Index, TField::Name>(field.Get(std::forward<TReceiver>(receiver)));
        });
    }

private:
    template <typename TAction>
    Y_FORCE_INLINE constexpr void ForEachFieldUnsafe(TAction f) const {
        ForEachFieldUnsafe(std::move(f), std::make_index_sequence<FieldsCount()>{});
    }

    template <typename TAction, size_t... Indexes>
    Y_FORCE_INLINE constexpr void ForEachFieldUnsafe(TAction f, std::index_sequence<Indexes...>) const {
        (
            [&] {
                const auto& field = std::get<Indexes>(Fields_);
                f.template operator()<Indexes>(field);
            }(),
            ...);
    }

    consteval bool AreFieldNamesUnique() const {
        auto names = std::apply([](const auto&... fields) {
            using TArray = std::array<TStringBuf, sizeof...(TFields)>;
            return TArray{std::decay_t<decltype(fields)>::Name...};
        }, Fields_);
        std::ranges::sort(names);
        auto redundant = std::ranges::unique(names);
        return redundant.empty();
    }

    static consteval size_t FieldsCount() noexcept {
        return sizeof...(TFields);
    }

    TSmallString<N> Name_;
    std::tuple<TFields...> Fields_;
};

} // namespace NYql::NReflection

#define YQL_REFLECTION_DETAIL_FIELD(field) \
    , ::NYql::NReflection::TCppField<      \
          ::NYql::TSmallString(PP_STRINGIZE(field)), &TStruct::field>()

#define YQL_DEFINE_REFLECTING(type, field_seq)                            \
    template <>                                                           \
    struct TReflection<type> {                                            \
        static consteval auto SelfType() {                                \
            using TStruct = type;                                         \
            constexpr auto reflection = ::NYql::NReflection::TCppStruct(  \
                std::type_identity<TStruct>{},                            \
                ::NYql::TSmallString(#type)                               \
                    PP_FOR_EACH(YQL_REFLECTION_DETAIL_FIELD, field_seq)); \
            return reflection;                                            \
        }                                                                 \
    }
