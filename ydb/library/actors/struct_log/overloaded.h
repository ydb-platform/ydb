#pragma once

#include <utility>

namespace NKikimr::NStructuredLog {

template <class... Types> struct TOverloaded : Types... { using Types:: operator ()...; };

template <class... Types>
constexpr auto MakeOverloaded(Types&&... args) {
    return TOverloaded<Types...>{std::forward<Types>(args)...};
}

}
