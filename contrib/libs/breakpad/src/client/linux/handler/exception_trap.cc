#include "exception_trap.h"

#include <cstdlib>
#include <cstring>

#include <string>
#include <typeinfo>

namespace google_breakpad {

// ExceptionDescriptor

void ExceptionDescriptor::initialize(const char* type_name, const char* message) noexcept {
    std::strncpy(type_name_.data(), type_name, type_name_.size() - 1);
    std::strncpy(message_.data(), message, message_.size() - 1);
}

bool ExceptionDescriptor::isEmpty() const noexcept {
    return message_[0] == '\0' && type_name_[0] == '\0';
}

const char* ExceptionDescriptor::getTypeName() const noexcept {
    return type_name_.data();
}

const char* ExceptionDescriptor::getMessage() const noexcept {
    return message_.data();
}

// ExceptionTrap

bool ExceptionTrap::initializeOnce(const char* type_name, const char* message) noexcept {
    if (!initialized_.test_and_set()) {
        descriptor_.initialize(type_name, message);
        return true;
    }

    return false;
}

ExceptionTrap& ExceptionTrap::getInstance() noexcept {
    static ExceptionTrap instance{};

    return instance;
}

void ExceptionTrap::setupTerminateHandler() {
    getInstance().original_terminate_handler_ = std::set_terminate(
        ExceptionTrap::terminate_handler);
}

const ExceptionDescriptor& ExceptionTrap::getCurrentException() const noexcept {
    return descriptor_;
}

[[ noreturn ]] void ExceptionTrap::terminate() const noexcept(detail::can_catch_within_noexcept) {
    if(original_terminate_handler_ != nullptr) {
        try {
            original_terminate_handler_();
        } catch (...) {} // suppress all exceptions to avoid terminate recursion
    }
    std::abort();
}

[[ noreturn ]] void ExceptionTrap::terminate_handler() noexcept(detail::can_catch_within_noexcept) {
    try {
        const auto& e_ptr = std::current_exception(); // can throw bad_alloc

        if (e_ptr != nullptr) {
            std::rethrow_exception(e_ptr);
        }
    } catch (const std::exception& e) {
        getInstance().initializeOnce(typeid(e).name(), e.what());
    } catch (const std::string& e) {
        getInstance().initializeOnce(typeid(e).name(), e.c_str());
    } catch (const char* e) {
        getInstance().initializeOnce(typeid(e).name(), e);
    } catch (...) {
        getInstance().initializeOnce("Unknown", "Unknown");
    }

    getInstance().terminate();
}

} //namespace google_breakpad
