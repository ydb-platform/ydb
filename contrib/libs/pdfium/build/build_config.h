// Copyright 2024 The Chromium Authors
// Compatibility shim for building pdfium outside of Chromium.
// Provides BUILDFLAG(IS_*) macros based on standard platform defines.

#ifndef BUILD_BUILD_CONFIG_H_
#define BUILD_BUILD_CONFIG_H_

// BUILDFLAG macro: expands BUILDFLAG(IS_LINUX) to BUILDFLAG_INTERNAL_IS_LINUX()
#define BUILDFLAG(flag) (BUILDFLAG_INTERNAL_##flag())

// Platform detection
#if defined(_WIN32) || defined(_WIN64)
#define BUILDFLAG_INTERNAL_IS_WIN() 1
#else
#define BUILDFLAG_INTERNAL_IS_WIN() 0
#endif

#if defined(__APPLE__)
#define BUILDFLAG_INTERNAL_IS_APPLE() 1
#else
#define BUILDFLAG_INTERNAL_IS_APPLE() 0
#endif

#if defined(__ANDROID__)
#define BUILDFLAG_INTERNAL_IS_ANDROID() 1
#else
#define BUILDFLAG_INTERNAL_IS_ANDROID() 0
#endif

#if defined(__linux__) && !defined(__ANDROID__)
#define BUILDFLAG_INTERNAL_IS_LINUX() 1
#else
#define BUILDFLAG_INTERNAL_IS_LINUX() 0
#endif

#if defined(__linux__) && !defined(__ANDROID__)
#define BUILDFLAG_INTERNAL_IS_CHROMEOS() 0
#else
#define BUILDFLAG_INTERNAL_IS_CHROMEOS() 0
#endif

#if defined(__EMSCRIPTEN__)
#define BUILDFLAG_INTERNAL_IS_ASMJS() 1
#else
#define BUILDFLAG_INTERNAL_IS_ASMJS() 0
#endif

#if defined(__native_client__)
#define BUILDFLAG_INTERNAL_IS_NACL() 1
#else
#define BUILDFLAG_INTERNAL_IS_NACL() 0
#endif

// POSIX: everything except Windows and NaCl
#if !defined(_WIN32) && !defined(__native_client__)
#define BUILDFLAG_INTERNAL_IS_POSIX() 1
#else
#define BUILDFLAG_INTERNAL_IS_POSIX() 0
#endif

// Architecture detection
#if defined(__x86_64__) || defined(_M_X64)
#define BUILDFLAG_INTERNAL_IS_X86_64() 1
#else
#define BUILDFLAG_INTERNAL_IS_X86_64() 0
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#define BUILDFLAG_INTERNAL_IS_ARM64() 1
#else
#define BUILDFLAG_INTERNAL_IS_ARM64() 0
#endif

// wchar_t size detection (Chromium-style macros used by pdfium)
#if defined(_WIN32)
#define WCHAR_T_IS_16_BIT
#else
#define WCHAR_T_IS_32_BIT
#endif

// Compiler detection (Chromium-style macros used by pdfium)
#if defined(__GNUC__) || defined(__clang__)
#if !defined(COMPILER_GCC)
#define COMPILER_GCC
#endif
#elif defined(_MSC_VER)
#if !defined(COMPILER_MSVC)
#define COMPILER_MSVC
#endif
#endif

// CPU architecture detection (Chromium-style macros used by pdfium)
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#if !defined(ARCH_CPU_X86_FAMILY)
#define ARCH_CPU_X86_FAMILY
#endif
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#if !defined(ARCH_CPU_ARM64)
#define ARCH_CPU_ARM64
#endif
#endif

#if defined(__arm__) && !defined(__aarch64__)
#if !defined(ARCH_CPU_ARMEL)
#define ARCH_CPU_ARMEL
#endif
#endif

#endif  // BUILD_BUILD_CONFIG_H_
