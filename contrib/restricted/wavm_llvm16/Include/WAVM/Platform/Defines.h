#pragma once

#include <stdlib.h>
#include "WAVM/Inline/Config.h"

#define WAVM_SUPPRESS_UNUSED(variable) (void)(variable);

// Compiler-specific attributes that both GCC and MSVC implement.
#ifdef _MSC_VER
#define WAVM_FORCEINLINE __forceinline
#define WAVM_FORCENOINLINE __declspec(noinline)
#define WAVM_PACKED_STRUCT(definition)                                                             \
	__pragma(pack(push, 1)) definition;                                                            \
	__pragma(pack(pop))
#elif defined(__GNUC__)
#define WAVM_FORCEINLINE inline __attribute__((always_inline))
#define WAVM_FORCENOINLINE __attribute__((noinline))
#define WAVM_PACKED_STRUCT(definition) definition __attribute__((packed));
#else
#define WAVM_FORCEINLINE
#define WAVM_FORCENOINLINE
#define WAVM_PACKED_STRUCT(definition) definition
#endif

// GCC/Clang-only attributes.
#if defined(__GNUC__)
#define WAVM_NO_ASAN __attribute__((no_sanitize_address))
#define WAVM_RETURNS_TWICE __attribute__((returns_twice))
#define WAVM_VALIDATE_AS_PRINTF(formatStringIndex, firstFormatArgIndex)                            \
	__attribute__((format(printf, formatStringIndex, firstFormatArgIndex)))
#define WAVM_UNLIKELY(condition) __builtin_expect(condition, 0)
#else
#define WAVM_NO_ASAN
#define WAVM_RETURNS_TWICE
#define WAVM_VALIDATE_AS_PRINTF(formatStringIndex, firstFormatArgIndex)
#define WAVM_UNLIKELY(condition) (condition)
#endif

// The attribute to disable UBSAN on a function is different between GCC and Clang.
#if defined(__clang__) && WAVM_ENABLE_UBSAN
#define WAVM_NO_UBSAN __attribute__((no_sanitize("undefined")))
#elif defined(__GNUC__) && WAVM_ENABLE_UBSAN
#define WAVM_NO_UBSAN __attribute__((no_sanitize_undefined))
#else
#define WAVM_NO_UBSAN
#endif

// WAVM_DEBUG_TRAP macro: breaks a debugger if one is attached, or aborts the program if not.
#ifdef _MSC_VER
#define WAVM_DEBUG_TRAP() __debugbreak()
#elif defined(__GNUC__) && !WAVM_ENABLE_LIBFUZZER && (defined(__i386__) || defined(__x86_64__))
#define WAVM_DEBUG_TRAP() __asm__ __volatile__("int3")
#elif defined(__GNUC__) && !WAVM_ENABLE_LIBFUZZER && defined(__aarch64__)
// See https://github.com/scottt/debugbreak/blob/master/debugbreak.h#L101
#define WAVM_DEBUG_TRAP() __asm__ __volatile__(".inst 0xe7f001f0")
#else
// Use abort() instead of trap instructions when fuzzing, since
// libfuzzer doesn't handle the breakpoint trap.
#define WAVM_DEBUG_TRAP() abort()
#endif

// Define WAVM_DEBUG to 0 or 1 depending on whether it's a debug build.
#if defined(NDEBUG)
#define WAVM_DEBUG 0
#else
#define WAVM_DEBUG 1
#endif

// Define a macro to align a struct that is valid in C99: don't use C++ alignas or C11 _Alignas.
#if defined(__GNUC__)
#define WAVM_ALIGNED_STRUCT(alignment, def) def __attribute__((aligned(alignment)));
#elif defined(_MSC_VER)
#define WAVM_ALIGNED_STRUCT(alignment, def) __declspec(align(alignment)) def;
#else
#error Please add a definition of WAVM_ALIGNED_STRUCT for your toolchain.
#endif

// Provide a macro to wrap calls to the C standard library functions that disables the MSVC error
// that they should be replaced by the safer but unportable MSVC secure CRT functions.
#ifdef _MSC_VER
#define WAVM_SCOPED_DISABLE_SECURE_CRT_WARNINGS(code)                                              \
	__pragma(warning(push)) __pragma(warning(disable : 4996)) code __pragma(warning(pop))
#else
#define WAVM_SCOPED_DISABLE_SECURE_CRT_WARNINGS(code) code
#endif

// Suppress warnings when falling through to another case level in a switch statement.
#if defined(__has_cpp_attribute) && __has_cpp_attribute(fallthrough)
#define WAVM_FALLTHROUGH [[fallthrough]]
#elif defined(__has_attribute)
#if __has_attribute(__fallthrough__)
#define WAVM_FALLTHROUGH __attribute__((fallthrough))
#endif
#endif

#ifndef WAVM_FALLTHROUGH
#define WAVM_FALLTHROUGH                                                                           \
	do                                                                                             \
	{                                                                                              \
	} while(0) /* fallthrough */
#endif
