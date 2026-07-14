#pragma once

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM {

	// An emulated 128-bit integer type
	// They have the range (-2^127..+2^127), with a single NaN value.
	// In general, NaN is propagated though operations: op(x, NaN) = NaN.
	// Unlike the arithmetic operators on C's built in integer types, the I128 operators detect
	// overflow and turn it into NaN values. There's a non-exhaustive set of alternative functions
	// that can be used to do arithmetic modulo 2^127: e.g. mulmod127 and mulAndCheckForOverflow.
	struct alignas(16) I128
	{
		constexpr I128() : lowU64(0), highU64(0) {}
		constexpr I128(I32 value) : lowI64(value), highI64(value >= 0 ? 0 : -1) {}
		constexpr I128(U32 value) : lowU64(value), highU64(0) {}
		constexpr I128(I64 value) : lowI64(value), highI64(value >= 0 ? 0 : -1) {}
		constexpr I128(U64 value) : lowU64(value), highU64(0) {}
		constexpr I128(const I128& copyee) : lowU64(copyee.lowU64), highU64(copyee.highU64) {}

		static constexpr I128 nan() { return I128(U64(0), INT64_MIN); }
		static constexpr I128 min() { return I128(U64(1), INT64_MIN); }
		static constexpr I128 max() { return I128(UINT64_MAX, INT64_MAX); }

		// Conversion to smaller integer types.
		explicit operator U8() const;
		explicit operator I8() const;
		explicit operator U16() const;
		explicit operator I16() const;
		explicit operator U32() const;
		explicit operator I32() const;
		explicit operator U64() const;
		explicit operator I64() const;

		// Conversion to FP types.
		explicit operator F32() const;
		explicit operator F64() const;

		// Overflow handling
		friend bool isNaN(I128 i) { return i.highI64 == INT64_MIN && i.lowU64 == 0; }
		friend I128 trapOnNaN(I128 i)
		{
			if(isNaN(i)) { WAVM_DEBUG_TRAP(); }
			return i;
		}
		friend I128 flushNaNToMax(I128 i) { return isNaN(i) ? max() : i; }
		friend I128 flushNaNToMin(I128 i) { return isNaN(i) ? min() : i; }
		friend I128 flushNaNToZero(I128 i) { return isNaN(i) ? I128() : i; }

		// Unary operators
		friend I128 operator+(I128 a);
		friend I128 operator-(I128 a);

		friend I128 operator~(I128 a);

		friend I128 abs(I128 a);

		// Binary operators that will return nan on overflow.
		friend I128 operator+(I128 a, I128 b);
		friend I128 operator-(I128 a, I128 b);
		friend I128 operator*(I128 a, I128 b);
		friend I128 operator/(I128 a, I128 b);
		friend I128 operator%(I128 a, I128 b);

		friend I128 operator>>(I128 a, I128 b);
		friend I128 operator<<(I128 a, I128 b);

		friend I128 operator^(I128 a, I128 b);
		friend I128 operator&(I128 a, I128 b);
		friend I128 operator|(I128 a, I128 b);

		// Addition and multiplication that return a value modulo 2^127 instead of overflowing.
		friend I128 mulmod127(I128 a, I128 b);
		friend I128 addmod127(I128 a, I128 b);

		// Addition and multiplication that will write a result modulo 2^127 to out and return
		// whether it overflowed.
		friend bool addAndCheckOverflow(I128 a, I128 b, I128* out);
		friend bool mulAndCheckOverflow(I128 a, I128 b, I128* out);

		// Comparison operators
		friend bool operator>(I128 a, I128 b);
		friend bool operator>=(I128 a, I128 b);
		friend bool operator<(I128 a, I128 b);
		friend bool operator<=(I128 a, I128 b);
		friend bool operator==(I128 a, I128 b);
		friend bool operator!=(I128 a, I128 b);

		// Assignment operators

		I128& operator=(const I128& copyee);

		I128& operator+=(I128 b) { return *this = *this + b; }
		I128& operator-=(I128 b) { return *this = *this - b; }
		I128& operator*=(I128 b) { return *this = *this * b; }
		I128& operator>>=(I128 b) { return *this = *this >> b; }
		I128& operator<<=(I128 b) { return *this = *this << b; }
		I128& operator^=(I128 b) { return *this = *this ^ b; }
		I128& operator&=(I128 b) { return *this = *this & b; }
		I128& operator|=(I128 b) { return *this = *this | b; }
		I128& operator/=(I128 b) { return *this = *this / b; }
		I128& operator%=(I128 b) { return *this = *this % b; }

	private:
		union
		{
			U64 lowU64;
			I64 lowI64;
		};
		union
		{
			U64 highU64;
			I64 highI64;
		};

		static I128 udivmod(I128 a, I128 b, I128& outRemainder);

		template<typename Float> Float asFloat() const;

		constexpr I128(I64 inLow, I64 inHigh) : lowI64(inLow), highI64(inHigh) {}
		constexpr I128(U64 inLow, I64 inHigh) : lowU64(inLow), highI64(inHigh) {}
		constexpr I128(U64 inLow, U64 inHigh) : lowU64(inLow), highU64(inHigh) {}
	};
}

#include "Impl/I128Impl.h"
