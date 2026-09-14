#pragma once

#include <stdexcept>

namespace NYql {

consteval void Ensure(bool p) {
    if (!p) {
        throw std::invalid_argument("compile-time assertion failed");
    }
}

} // namespace NYql
