#pragma once

#include <concepts>

namespace NYql {
namespace NInternal {

[[noreturn]]
void DieWithCurrentException(const char* moduleName) noexcept;

} // namespace NInternal

auto WithAbortOnException(std::invocable auto action, const char* moduleName) noexcept {
    try {
        return action();
    } catch (...) {
        NInternal::DieWithCurrentException(moduleName);
    }
}

} // namespace NYql
