#pragma once

#include <array>
#include <atomic>
#include <exception>

#include "google_breakpad/common/minidump_format.h"

namespace google_breakpad {

namespace detail {

#ifdef __ANDROID__
// In some versions of Android NDK the 'catch' blocks may have no affect in functions
// with 'noexcept' specifier.
constexpr bool can_catch_within_noexcept = false;
#else
constexpr bool can_catch_within_noexcept = true;
#endif

} // namespace detail

// ExceptionDescriptor holds exception information and may be safely passed
// between terminate handler context and signal handler context.
//
class ExceptionDescriptor {
public:
    // Copy |type_name| and |message| content to ExceptionDescriptor memory.
    // Not thread-safe. Async-signal-safe.
    // May be used in terminate handler.
    void initialize(const char* type_name, const char* message) noexcept;

     // isEmpty, getTypeName, getMessage are async-signal-safe.
     // May be used in signal handler.
    bool isEmpty() const noexcept;
    const char* getTypeName() const noexcept;
    const char* getMessage() const noexcept;

private:
    std::array<char, MAX_EXC_TYPE_LEN> type_name_{{'\0'}};
    std::array<char, MAX_EXC_MSG_LEN> message_{{'\0'}};
};

// Singleton class. Wraps default terminate handler with ExceptionTrap::terminate_handler.
// If terminate is caused by uncaught exception, ExceptionTrap stores information about exception
// into ExceptionDescriptor before default terminate handler will call.
class ExceptionTrap {
public:
    // Initialize single instance of ExceptionTrap if not initialized and return it.
    // Must be called at least once before using it in terminate handler or signal handler.
    static ExceptionTrap& getInstance() noexcept;

    // Replace current terminate handler by ExceptionTrap::terminate_handler.
    static void setupTerminateHandler();

    // Return information about first exception that ExceptionTrap is "caught".
    // If no exception was "caught", the empty ExceptionDescriptor will returned.
    // Async-signal-safe.
    const ExceptionDescriptor& getCurrentException() const noexcept;

private:
    bool initializeOnce(const char* type_name, const char* message) noexcept;

    // Call original terminate handler and abort.
    [[ noreturn ]] void terminate() const noexcept(detail::can_catch_within_noexcept);
    [[ noreturn ]] static void terminate_handler() noexcept(detail::can_catch_within_noexcept);

    std::atomic_flag initialized_ = ATOMIC_FLAG_INIT;
    ExceptionDescriptor descriptor_{};
    std::terminate_handler original_terminate_handler_{nullptr};
};

} //namespace google_breakpad
