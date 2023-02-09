#pragma once

#include <variant>

namespace NUnifiedAgent {
    template<class... Ts> struct TOverloaded : Ts... { using Ts::operator()...; };
    template<class... Ts> TOverloaded(Ts...) -> TOverloaded<Ts...>;

    template <class T, class... U>
    auto Visit(T&& variant, U&&... visitorOverloads) {
        return std::visit(TOverloaded{std::forward<U>(visitorOverloads)...}, std::forward<T>(variant));
    }

    template <typename TTarget, typename... TSourceTypes>
    auto CastTo(std::variant<TSourceTypes...>&& variant) {
        return Visit(variant, [](auto& p) -> TTarget { return std::move(p); });
    }
}
