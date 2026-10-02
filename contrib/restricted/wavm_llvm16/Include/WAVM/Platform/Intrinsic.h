#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

#ifdef _WIN32
#include <intrin.h>
#endif

// __has_builtin is a Clang-specific way to test whether a builtin is supported
#ifndef __has_builtin
#define __has_builtin(x) 0
#endif

namespace WAVM {
	// The number of bytes in a cache line: assume 64 for now.
	static constexpr Uptr numCacheLineBytes = 64;

	// countLeadingZeroes returns the number of leading zeroes, or the bit width of the input if no
	// bits are set.
	inline U32 countLeadingZeroes(U32 value)
	{
#ifdef _WIN32
		// BitScanReverse returns 0 if the input is 0.
		unsigned long result;
		return _BitScanReverse(&result, value) ? (31 - result) : 32;
#elif defined(__GNUC__)
		return value == 0 ? 32 : __builtin_clz(value);
#else
#error Unsupported compiler
#endif
	}

	inline U64 countLeadingZeroes(U64 value)
	{
#ifdef _WIN64
		unsigned long result;
		return _BitScanReverse64(&result, value) ? (63 - result) : 64;
#elif defined(__GNUC__)
		return value == 0 ? 64 : __builtin_clzll(value);
#else
		U64 result = countLeadingZeroes(U32(value >> 32));
		if(result == 32) { result += countLeadingZeroes(U32(value)); }
		return result;
#endif
	}

	// countTrailingZeroes returns the number of trailing zeroes, or the bit width of the input if
	// no bits are set.
	inline U32 countTrailingZeroes(U32 value)
	{
#ifdef _WIN32
		// BitScanForward returns 0 if the input is 0.
		unsigned long result;
		return _BitScanForward(&result, value) ? result : 32;
#elif defined(__GNUC__)
		return value == 0 ? 32 : __builtin_ctz(value);
#else
#error Unsupported compiler
#endif
	}
	inline U64 countTrailingZeroes(U64 value)
	{
#ifdef _WIN64
		unsigned long result;
		return _BitScanForward64(&result, value) ? result : 64;
#elif defined(__GNUC__)
		return value == 0 ? 64 : __builtin_ctzll(value);
#else
		U64 result = countTrailingZeroes(U32(value));
		if(result == 32) { result += countTrailingZeroes(U32(value >> 32)); }
		return result;
#endif
	}

	inline U64 floorLogTwo(U64 value) { return value <= 1 ? 0 : 63 - countLeadingZeroes(value); }
	inline U32 floorLogTwo(U32 value) { return value <= 1 ? 0 : 31 - countLeadingZeroes(value); }
	inline U64 ceilLogTwo(U64 value)
	{
		return value <= 1 ? 0 : 63 - countLeadingZeroes(value * 2 - 1);
	}
	inline U32 ceilLogTwo(U32 value)
	{
		return value <= 1 ? 0 : 31 - countLeadingZeroes(value * 2 - 1);
	}

	constexpr inline U64 branchlessMin(U64 value, U64 maxValue)
	{
		return value < maxValue ? value : maxValue;
	}
	constexpr inline U32 branchlessMin(U32 value, U32 maxValue)
	{
		return value < maxValue ? value : maxValue;
	}

#if __GNUC__ >= 5 || __has_builtin(__builtin_add_overflow)
	inline bool addAndCheckOverflow(U64 a, U64 b, U64* out)
	{
		return __builtin_add_overflow(a, b, out);
	}
	inline bool addAndCheckOverflow(I64 a, I64 b, I64* out)
	{
		return __builtin_add_overflow(a, b, out);
	}
#else
	inline bool addAndCheckOverflow(U64 a, U64 b, U64* out)
	{
		*out = a + b;
		return *out < a;
	}

	inline bool addAndCheckOverflow(I64 a, I64 b, I64* out)
	{
		// Do the add with U64 because signed overflow is UB.
		*out = I64(U64(a) + U64(b));
		return (a < 0 && b < 0 && *out > 0) || (a > 0 && b > 0 && *out < 0);
	}
#endif

	// Byte-wise memcpy and memset: these are different from the C library versions because in the
	// event of a trap while reading/writing memory, they guarantee that every byte preceding the
	// byte that trapped will have been written.
	// On X86 they use "rep movsb" (for copy) and "rep stosb" (for set) which are microcoded on
	// Intel Ivy Bridge and later processors, and supposedly competitive with the C library
	// functions. On other architectures, it will fall back to a C loop, which is not likely to be
	// competitive with the C library function.

	inline void bytewiseMemCopy(volatile U8* dest, const U8* source, Uptr numBytes)
	{
#ifdef _WIN32
		__movsb((U8*)dest, source, numBytes);
#elif defined(__i386__) || defined(__x86_64__)
		asm volatile("rep movsb"
					 : "=D"(dest), "=S"(source), "=c"(numBytes)
					 : "0"(dest), "1"(source), "2"(numBytes)
					 : "memory");
#else
		for(Uptr index = 0; index < numBytes; ++index) { dest[index] = source[index]; }
#endif
	}

	inline void bytewiseMemCopyReverse(volatile U8* dest, const U8* source, Uptr numBytes)
	{
		for(Uptr index = 0; index < numBytes; ++index)
		{ dest[numBytes - index - 1] = source[numBytes - index - 1]; }
	}

	inline void bytewiseMemSet(volatile U8* dest, U8 value, Uptr numBytes)
	{
#ifdef _WIN32
		__stosb((U8*)dest, value, numBytes);
#elif defined(__i386__) || defined(__x86_64__)
		asm volatile("rep stosb"
					 : "=D"(dest), "=a"(value), "=c"(numBytes)
					 : "0"(dest), "1"(value), "2"(numBytes)
					 : "memory");
#else
		for(Uptr index = 0; index < numBytes; ++index) { dest[index] = value; }
#endif
	}
}
