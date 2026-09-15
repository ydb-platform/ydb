#include "exception_utils.h"

#include <util/generic/overloaded.h>
#include <util/generic/yexception.h>

#include <concepts>
#include <exception>

namespace NYql::NInternal {

namespace {

using TSafeMessage = std::variant<const char*, TString>;

const char* GetCurrentMessageSafe(const TSafeMessage& message Y_LIFETIME_BOUND) noexcept {
    if (auto* str = std::get_if<TString>(&message)) {
        return str->c_str();
    }
    if (auto* ptr = std::get_if<const char*>(&message)) {
        return *ptr;
    }
    return "Message is corrupted";
}

TSafeMessage SafeCurrentExceptionMessage() noexcept {
    try {
        return CurrentExceptionMessage();
    } catch (...) {
        static constexpr char ErrorMessage[] = "Cannot take current exception message in WithAbortOnException";
        return TSafeMessage(ErrorMessage);
    }
}

} // namespace

void DieWithCurrentException(const char* moduleName) noexcept {
    Y_ABORT("Die inside WithAbortOnException of %s: %s", moduleName, GetCurrentMessageSafe(SafeCurrentExceptionMessage()));
}

} // namespace NYql::NInternal
