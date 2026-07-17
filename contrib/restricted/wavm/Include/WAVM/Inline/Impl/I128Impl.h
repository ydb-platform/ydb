// IWYU pragma: private, include "WAVM/Inline/I128.h"
// You should only include this file indirectly by including I128.h.
#pragma once

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/FloatComponents.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Platform/Intrinsic.h"

namespace WAVM {
	inline I128::operator U8() const
	{
		if(isNaN(*this) || highI64 || lowU64 > UINT8_MAX) { WAVM_DEBUG_TRAP(); }
		return U8(lowU64);
	}

	inline I128::operator I8() const
	{
		if(isNaN(*this) || highI64 > 0 || highI64 < -1 || (highI64 == -1 && lowI64 < INT8_MIN)
		   || (highI64 == 0 && lowI64 > INT8_MAX))
		{ WAVM_DEBUG_TRAP(); }

		return I8(lowI64);
	}

	inline I128::operator U16() const
	{
		if(isNaN(*this) || highI64 || lowU64 > UINT16_MAX) { WAVM_DEBUG_TRAP(); }
		return U8(lowU64);
	}

	inline I128::operator I16() const
	{
		if(isNaN(*this) || highI64 > 0 || highI64 < -1 || (highI64 == -1 && lowI64 < INT16_MIN)
		   || (highI64 == 0 && lowI64 > INT16_MAX))
		{ WAVM_DEBUG_TRAP(); }

		return I16(lowI64);
	}

	inline I128::operator U32() const
	{
		if(isNaN(*this) || highI64 || lowU64 > UINT32_MAX) { WAVM_DEBUG_TRAP(); }
		return U32(lowU64);
	}

	inline I128::operator I32() const
	{
		if(isNaN(*this) || highI64 > 0 || highI64 < -1 || (highI64 == -1 && lowI64 < INT32_MIN)
		   || (highI64 == 0 && lowI64 > INT32_MAX))
		{ WAVM_DEBUG_TRAP(); }

		return I32(lowI64);
	}

	inline I128::operator U64() const
	{
		if(isNaN(*this) || highI64) { WAVM_DEBUG_TRAP(); }
		return lowU64;
	}

	inline I128::operator I64() const
	{
		if(isNaN(*this) || highI64 > 0 || highI64 < -1 || (highI64 == -1 && lowI64 >= 0)
		   || (highI64 == 0 && lowI64 < 0))
		{ WAVM_DEBUG_TRAP(); }
		return lowI64;
	}

	inline I128::operator F32() const { return asFloat<F32>(); }
	inline I128::operator F64() const { return asFloat<F64>(); }

	template<typename Float> Float I128::asFloat() const
	{
		using Components = FloatComponents<Float>;
		using Bits = typename Components::Bits;

		Components components;

		if(isNaN(*this))
		{
			components.bits.sign = 0;
			components.bits.exponent = Components::maxExponentBits;
			components.bits.significand = Components::canonicalSignificand;
			return components.value;
		}

		// Determine the sign, and compute the absolute value of the I128.
		components.bits.sign = *this < 0 ? 1 : 0;
		I128 absoluteI128 = components.bits.sign ? -*this : *this;

		// Count the number of leading zeroes in the absolute I128.
		U64 leadingZeroes = countLeadingZeroes(absoluteI128.highU64);
		if(leadingZeroes == 64)
		{
			leadingZeroes += countLeadingZeroes(absoluteI128.lowU64);
			if(leadingZeroes == 128)
			{
				components.bits.exponent = 0;
				components.bits.significand = 0;
				return components.value;
			}
		}
		// leadingZeroes is in the range 0<=leadingZeroes<128.

		// Calculate the float exponent from the number of leading zeroes.
		const U64 exponent = 127 - leadingZeroes;
		components.bits.exponent = Bits(exponent + Components::exponentBias);

		// Shift the most-significant set bit to be the MSB in a U64.
		const U64 significand64 = (absoluteI128 << leadingZeroes).highU64;

		// Shift the most-significant set bit down to the 53rd bit, and take the 52 bits below it as
		// the significand.
		components.bits.significand = significand64 >> 11;

		return components.value;
	}

	inline I128 operator+(I128 a) { return a; }

	inline I128 operator-(I128 a)
	{
		// This doesn't need to explicitly check for NaN, because negation preserves NaN.
		I128 result;
		result.highU64 = ~a.highU64;
		result.lowU64 = ~a.lowU64;
		result.highU64 += addAndCheckOverflow(result.lowU64, 1, &result.lowU64) ? 1 : 0;
		return result;
	}

	inline I128 abs(I128 a)
	{
		// This doesn't need to explicitly check for NaN, because negation preserves NaN.
		I128 result;
		I64 sign = a.highI64 >> 63;
		result.lowU64 = a.lowU64 ^ sign;
		result.highU64 = (a.highU64 ^ sign) + ((result.lowU64 - U64(sign) < result.lowU64) ? 1 : 0);
		result.lowU64 -= U64(sign);
		return result;
	}

	inline I128 operator~(I128 a) { return isNaN(a) ? a : I128(~a.lowU64, ~a.highI64); }

	inline bool addAndCheckOverflow(I128 a, I128 b, I128* out)
	{
		if(isNaN(a) || isNaN(b))
		{
			*out = I128::nan();
			return false;
		}

		bool overflowed = false;
		if(addAndCheckOverflow(a.highI64, b.highI64, &out->highI64)) { overflowed = true; }

		if(addAndCheckOverflow(a.lowU64, b.lowU64, &out->lowU64))
		{
			if(addAndCheckOverflow(out->highI64, 1, &out->highI64)) { overflowed = true; }
		}
		return overflowed;
	}

	inline I128 addmod127(I128 a, I128 b)
	{
		I128 result;
		if(addAndCheckOverflow(a, b, &result)) { result = -result; }
		return result;
	}

	inline I128 operator+(I128 a, I128 b)
	{
		I128 result;
		if(addAndCheckOverflow(a, b, &result)) { result = I128::nan(); }
		return result;
	}

	inline I128 operator-(I128 a, I128 b) { return a + -b; }

	inline bool mulAndCheckOverflow(I128 a, I128 b, I128* out)
	{
		if(isNaN(a) || isNaN(b))
		{
			*out = I128::nan();
			return false;
		}

		// Remove the sign from the operands.
		const I64 sign = (a.highI64 >> 63) ^ (b.highI64 >> 63);
		a = abs(a);
		b = abs(b);

		// Calculate a matrix of 64-bit products of all the 32-bit components of the operands.
		const U64 a0 = a.lowU64 & 0xffffffff;
		const U64 a1 = a.lowU64 >> 32;
		const U64 a2 = a.highU64 & 0xffffffff;
		const U64 a3 = a.highU64 >> 32;
		const U64 b0 = b.lowU64 & 0xffffffff;
		const U64 b1 = b.lowU64 >> 32;
		const U64 b2 = b.highU64 & 0xffffffff;
		const U64 b3 = b.highU64 >> 32;
		const U64 m00 = a0 * b0;
		const U64 m01 = a0 * b1;
		const U64 m02 = a0 * b2;
		const U64 m03 = a0 * b3;
		const U64 m10 = a1 * b0;
		const U64 m11 = a1 * b1;
		const U64 m12 = a1 * b2;
		const U64 m13 = a1 * b3;
		const U64 m20 = a2 * b0;
		const U64 m21 = a2 * b1;
		const U64 m22 = a2 * b2;
		const U64 m23 = a2 * b3;
		const U64 m30 = a3 * b0;
		const U64 m31 = a3 * b1;
		const U64 m32 = a3 * b2;
		const U64 m33 = a3 * b3;

		// Add all the 64-bit products together, checking for overflow.
		bool overflowed = false;
		out->lowU64 = m00;
		out->highU64 = U64(addAndCheckOverflow(out->lowU64, m01 << 32, &out->lowU64));
		out->highU64 += U64(addAndCheckOverflow(out->lowU64, m10 << 32, &out->lowU64));
		out->highU64 += (m01 >> 32) + (m10 >> 32);
		overflowed = addAndCheckOverflow(out->highU64, m02, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m11, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m20, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m03 << 32, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m12 << 32, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m21 << 32, &out->highU64) || overflowed;
		overflowed = addAndCheckOverflow(out->highU64, m30 << 32, &out->highU64) || overflowed;

		// Check if the product overflowed the 63 non-sign bits in highU64.
		overflowed = out->highI64 < 0 || overflowed;
		out->highU64 &= ~(U64(1) << 63);

		// Check if the product overflowed the I128 completely.
		overflowed = m13 || m22 || m31 || m23 || m32 || m33 || overflowed;

		// Restore the correct sign for the product.
		out->highI64 ^= sign;
		out->lowI64 ^= sign;
		addAndCheckOverflow(*out, -sign, out);

		return overflowed;
	}

	inline I128 mulmod127(I128 a, I128 b)
	{
		I128 result;
		mulAndCheckOverflow(a, b, &result);
		return result;
	}

	inline I128 operator*(I128 a, I128 b)
	{
		I128 result;
		return mulAndCheckOverflow(a, b, &result) ? I128::nan() : result;
	}

	inline I128 operator/(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b) || b == 0) { return I128::nan(); }

		I128 remainder;
		I64 sign = (a.highI64 >> 63) ^ (b.highI64 >> 63); // sign of quotient
		a = abs(a);
		b = abs(b);
		return (I128::udivmod(a, b, remainder) ^ I128(sign, sign))
			   - I128(sign, sign); // negate if sign == -1
	}

	inline I128 operator%(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		if(b == 0) { return I128::nan(); }

		I128 s = b.highI64 >> 63; // s = b < 0 ? -1 : 0
		b = (b ^ s) - s;          // negate if s == -1
		s = a.highI64 >> 63;      // s = a < 0 ? -1 : 0
		a = (a ^ s) - s;          // negate if s == -1
		I128 remainder;
		I128::udivmod(a, b, remainder);
		return (remainder ^ s) - s; // negate if s == -1
	}

	inline I128 operator>>(I128 a, I128 b)
	{
		if(b == 0) { return a; }
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		I128 result;
		U64 b64 = b.lowU64;

		b64 &= 127;
		if(b64 == 0) { result = a; }
		else if(b64 & 64)
		{
			// 64 <= b < 128
			result.highI64 = a.highI64 >> 63;
			result.lowI64 = a.highI64 >> (b64 - 64);
		}
		else
		{
			// 0 <= |b| < 64
			result.highI64 = a.highI64 >> b64;
			result.lowU64 = (a.highI64 << (64 - b64)) | (a.lowU64 >> b64);
		}
		return result;
	}

	inline I128 operator<<(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		I128 result;
		U64 b64 = b.lowU64;

		b64 &= 127;
		if(b64 == 0) { result = a; }
		else if(b64 & 64)
		{
			// 64 <= b < 128
			result.lowU64 = 0;
			result.highI64 = a.lowU64 << (b64 - 64);
		}
		else
		{
			// 0 <= |b| < 64
			result.lowU64 = a.lowU64 << b64;
			result.highU64 = (a.highU64 << b64) | (a.lowU64 >> (64 - b64));
		}
		return result;
	}

	inline I128 operator^(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		return I128(a.lowU64 ^ b.lowU64, a.highI64 ^ b.highI64);
	}

	inline I128 operator&(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		return I128(a.lowU64 & b.lowU64, a.highI64 & b.highI64);
	}

	inline I128 operator|(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return I128::nan(); }

		return I128(a.lowU64 | b.lowU64, a.highI64 | b.highI64);
	}

	inline bool operator>(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return false; }
		return a.highI64 > b.highI64 || (a.highI64 == b.highI64 && a.lowI64 > b.lowI64);
	}

	inline bool operator>=(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return false; }
		return a.highI64 > b.highI64 || (a.highI64 == b.highI64 && a.lowI64 >= b.lowI64);
	}

	inline bool operator<(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return false; }
		return a.highI64 < b.highI64 || (a.highI64 == b.highI64 && a.lowI64 < b.lowI64);
	}

	inline bool operator<=(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return false; }
		return a.highI64 < b.highI64 || (a.highI64 == b.highI64 && a.lowI64 <= b.lowI64);
	}

	inline bool operator==(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return false; }
		return a.highI64 == b.highI64 && a.lowU64 == b.lowU64;
	}

	inline bool operator!=(I128 a, I128 b)
	{
		if(isNaN(a) || isNaN(b)) { return true; }
		return a.highI64 != b.highI64 || a.lowU64 != b.lowU64;
	}

	inline I128& I128::operator=(const I128& copyee)
	{
		if(this != &copyee)
		{
			lowU64 = copyee.lowU64;
			highU64 = copyee.highU64;
		}
		return *this;
	}

	inline I128 I128::udivmod(I128 n, I128 d, I128& outRemainder)
	{
		if(isNaN(n) || isNaN(d)) { return I128::nan(); }

		I128 q;
		I128 r;
		U64 sr;
		// special cases, X is unknown, K != 0
		if(n.highU64 == 0)
		{
			if(d.highU64 == 0)
			{
				// 0 X
				// ---
				// 0 X
				outRemainder = n.lowU64 % d.lowU64;
				return n.lowU64 / d.lowU64;
			}
			// 0 X
			// ---
			// K X
			return {0, n.lowU64};
		}
		// n.highU64 != 0
		if(d.lowU64 == 0)
		{
			// Error if the divisor is zero.
			WAVM_ERROR_UNLESS(d.highU64 != 0);

			// d.highU64 != 0
			if(n.lowU64 == 0)
			{
				// K 0
				// ---
				// K 0
				r.highU64 = n.highU64 % d.highU64;
				r.lowU64 = 0;
				outRemainder = r;
				return n.highU64 / d.highU64;
			}
			// K K
			// ---
			// K 0
			if((d.highU64 & (d.highU64 - 1)) == 0) /* if d is a power of 2 */
			{
				r.lowU64 = n.lowU64;
				r.highU64 = n.highU64 & (d.highU64 - 1);
				outRemainder = r;
				return n.highU64 >> countTrailingZeroes(d.highU64);
			}
			// K K
			// ---
			// K 0
			sr = countLeadingZeroes(d.highU64) - countLeadingZeroes(n.highU64);
			// 0 <= sr <= 64 - 2 or sr large
			if(sr > 64 - 2)
			{
				outRemainder = n;
				return 0;
			}
			++sr;
			// 1 <= sr <= 64 - 1
			// q = n << (128 - sr);
			q.lowU64 = 0;
			q.highU64 = n.lowU64 << (64 - sr);
			// r = n >> sr;
			r.highU64 = n.highU64 >> sr;
			r.lowU64 = (n.highU64 << (64 - sr)) | (n.lowU64 >> sr);
		}
		else /* d.lowU64 != 0 */
		{
			if(d.highU64 == 0)
			{
				// K X
				// ---
				// 0 K
				if((d.lowU64 & (d.lowU64 - 1)) == 0) /* if d is a power of 2 */
				{
					r = n.lowU64 & (d.lowU64 - 1);
					if(d.lowU64 == 1)
					{
						outRemainder = r;
						return n;
					}
					sr = countTrailingZeroes(d.lowU64);
					q.highU64 = n.highU64 >> sr;
					q.lowU64 = (n.highU64 << (64 - sr)) | (n.lowU64 >> sr);
					outRemainder = r;
					return q;
				}
				// K X
				// ---
				// 0 K
				sr = 1 + 64 + countLeadingZeroes(d.lowU64) - countLeadingZeroes(n.highU64);
				// 2 <= sr <= 128 - 1
				// q = n << (128 - sr);
				// r = n >> sr;
				if(sr == 64)
				{
					q.lowU64 = 0;
					q.highU64 = n.lowU64;
					r.highU64 = 0;
					r.lowU64 = n.highU64;
				}
				else if(sr < 64) /* 2 <= sr <= 64 - 1 */
				{
					q.lowU64 = 0;
					q.highU64 = n.lowU64 << (64 - sr);
					r.highU64 = n.highU64 >> sr;
					r.lowU64 = (n.highU64 << (64 - sr)) | (n.lowU64 >> sr);
				}
				else /* 64 + 1 <= sr <= 128 - 1 */
				{
					q.lowU64 = n.lowU64 << (128 - sr);
					q.highU64 = (n.highU64 << (128 - sr)) | (n.lowU64 >> (sr - 64));
					r.highU64 = 0;
					r.lowU64 = n.highU64 >> (sr - 64);
				}
			}
			else
			{
				// K X
				// ---
				// K K
				sr = countLeadingZeroes(d.highU64) - countLeadingZeroes(n.highU64);
				// 0 <= sr <= 64 - 1 or sr large
				if(sr > 64 - 1)
				{
					outRemainder = n;
					return 0;
				}
				++sr;
				// 1 <= sr <= 64
				// q = n << (128 - sr);
				// r = n >> sr;
				q.lowU64 = 0;
				if(sr == 64)
				{
					q.highU64 = n.lowU64;
					r.highU64 = 0;
					r.lowU64 = n.highU64;
				}
				else
				{
					r.highU64 = n.highU64 >> sr;
					r.lowU64 = (n.highU64 << (64 - sr)) | (n.lowU64 >> sr);
					q.highU64 = n.lowU64 << (64 - sr);
				}
			}
		}
		// Not a special case
		// q and r are initialized with:
		// q = n << (128 - sr);
		// r = n >> sr;
		// 1 <= sr <= 128 - 1
		U64 carry = 0;
		for(; sr > 0; --sr)
		{
			// r:q = ((r:q)  << 1) | carry
			r.highU64 = (r.highU64 << 1) | (r.lowU64 >> (64 - 1));
			r.lowU64 = (r.lowU64 << 1) | (q.highU64 >> (64 - 1));
			q.highU64 = (q.highU64 << 1) | (q.lowU64 >> (64 - 1));
			q.lowU64 = (q.lowU64 << 1) | carry;
			// carry = 0;
			// if (r >= d)
			// {
			//     r -= d;
			//      carry = 1;
			// }
			const I128 s = (d - r - 1).highI64 >> 63;
			carry = s.lowU64 & 1;
			r -= d & s;
		}
		q = (q << 1) | carry;
		outRemainder = r;
		return q;
	}
}
