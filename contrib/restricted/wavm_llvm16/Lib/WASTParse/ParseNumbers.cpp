#include <stdint.h>
#include <cmath>
#include <string>
#include "Lexer.h"
#include "Parse.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/FloatComponents.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Mutex.h"

using namespace WAVM;
using namespace WAVM::WAST;

// Parses an optional + or - sign and returns true if a - sign was parsed.
// If either a + or - sign is parsed, nextChar is advanced past it.
static bool parseSign(const char*& nextChar)
{
	if(*nextChar == '-')
	{
		++nextChar;
		return true;
	}
	else if(*nextChar == '+')
	{
		++nextChar;
	}
	return false;
}

// Parses an unsigned integer from hexits, starting with "0x", and advancing nextChar past the
// parsed hexits. be called for input that's already been accepted by the lexer as a hexadecimal
// integer.
static U64 parseHexUnsignedInt(const char*& nextChar, ParseState* parseState, U64 maxValue)
{
	const char* firstHexit = nextChar;
	WAVM_ASSERT(nextChar[0] == '0' && (nextChar[1] == 'x' || nextChar[1] == 'X'));
	nextChar += 2;

	U64 result = 0;
	U8 hexit = 0;
	while(true)
	{
		if(*nextChar == '_')
		{
			++nextChar;
			continue;
		}
		if(!tryParseHexit(nextChar, hexit)) { break; }
		if(result > (maxValue - hexit) / 16)
		{
			parseErrorf(parseState, firstHexit, "integer literal is too large");
			result = maxValue;
			while(tryParseHexit(nextChar, hexit)) {};
			break;
		}
		WAVM_ASSERT(result * 16 + hexit >= result);
		result = result * 16 + hexit;
	}
	return result;
}

// Parses an unsigned integer from digits, advancing nextChar past the parsed digits.
// Assumes it will only be called for input that's already been accepted by the lexer as a decimal
// integer.
static U64 parseDecimalUnsignedInt(const char*& nextChar,
								   ParseState* parseState,
								   U64 maxValue,
								   const char* context)
{
	U64 result = 0;
	const char* firstDigit = nextChar;
	while(true)
	{
		if(*nextChar == '_')
		{
			++nextChar;
			continue;
		}
		if(*nextChar < '0' || *nextChar > '9') { break; }

		const U8 digit = *nextChar - '0';
		++nextChar;

		if(result > U64(maxValue - digit) / 10)
		{
			parseErrorf(parseState, firstDigit, "%s is too large", context);
			result = maxValue;
			while((*nextChar >= '0' && *nextChar <= '9') || *nextChar == '_') { ++nextChar; };
			break;
		}
		WAVM_ASSERT(result * 10 + digit >= result);
		result = result * 10 + digit;
	};
	return result;
}

// Parses a floating-point NaN, advancing nextChar past the parsed characters.
// Assumes it will only be called for input that's already been accepted by the lexer as a literal
// NaN.
template<typename Float> Float parseNaN(const char*& nextChar, ParseState* parseState)
{
	const char* firstChar = nextChar;

	typedef FloatComponents<Float> FloatComponents;
	FloatComponents resultComponents;
	resultComponents.bits.sign = parseSign(nextChar) ? 1 : 0;
	resultComponents.bits.exponent = FloatComponents::maxExponentBits;

	WAVM_ASSERT(nextChar[0] == 'n' && nextChar[1] == 'a' && nextChar[2] == 'n');
	nextChar += 3;

	if(*nextChar == ':')
	{
		++nextChar;

		const U64 significandBits
			= parseHexUnsignedInt(nextChar, parseState, FloatComponents::maxSignificand);
		if(!significandBits)
		{
			parseErrorf(parseState, firstChar, "NaN significand must be non-zero");
			resultComponents.bits.significand = 1;
		}
		resultComponents.bits.significand = typename FloatComponents::Bits(significandBits);
	}
	else
	{
		// If the NaN's significand isn't specified, just set the top bit.
		resultComponents.bits.significand = typename FloatComponents::Bits(1)
											<< (FloatComponents::numSignificandBits - 1);
	}

	return resultComponents.value;
}

// Parses a floating-point infinity. Does not advance nextChar.
// Assumes it will only be called for input that's already been accepted by the lexer as a literal
// infinity.
template<typename Float> Float parseInfinity(const char* nextChar)
{
	// Floating point infinite is represented by max exponent with a zero significand.
	typedef FloatComponents<Float> FloatComponents;
	FloatComponents resultComponents;
	resultComponents.bits.sign = parseSign(nextChar) ? 1 : 0;
	resultComponents.bits.exponent = FloatComponents::maxExponentBits;
	resultComponents.bits.significand = 0;
	return resultComponents.value;
}

template<typename Float> Float strtox(const char* firstChar, char** endChar);
template<> F32 strtox(const char* firstChar, char** endChar) { return strtof(firstChar, endChar); }
template<> F64 strtox(const char* firstChar, char** endChar) { return strtod(firstChar, endChar); }

// Parses a floating point literal, advancing nextChar past the parsed characters. Assumes it will
// only be called for input that's already been accepted by the lexer as a float literal.
template<typename Float> Float parseDecimalFloat(const char*& nextChar, ParseState* parseState)
{
	// Scan the token's characters for underscores, and make a copy of it without the underscores
	// for strtof/strtod.
	const char* firstChar = nextChar;
	std::string noUnderscoreString;
	bool hasUnderscores = false;
	while(true)
	{
		const char c = *nextChar;

		// Determine whether the next character is still part of the number.
		const bool isNumericChar = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
								   || (c >= 'A' && c <= 'F') || c == 'x' || c == 'X' || c == 'p'
								   || c == 'P' || c == '+' || c == '-' || c == '.' || c == '_';
		if(!isNumericChar) { break; }

		if(c == '_' && !hasUnderscores)
		{
			// If this is the first underscore encountered, copy the preceding characters of the
			// number to a std::string.
			noUnderscoreString = std::string(firstChar, nextChar);
			hasUnderscores = true;
		}
		else if(c != '_' && hasUnderscores)
		{
			// If an underscore has previously been encountered, copy non-underscore characters to
			// that string.
			noUnderscoreString += *nextChar;
		}

		++nextChar;
	};

	// Pass the underscore-free string to parseNonSpecialF64 instead of the original input string.
	const char* noUnderscoreFirstChar = firstChar;
	if(hasUnderscores) { noUnderscoreFirstChar = noUnderscoreString.c_str(); }

	// Use libc strtof/strtod to parse decimal floats.
	char* endChar = nullptr;
	Float result = strtox<Float>(noUnderscoreFirstChar, &endChar);
	if(endChar == noUnderscoreFirstChar)
	{ Errors::fatalf("strtof/strtod failed to parse number accepted by lexer"); }

	if(std::isinf(result)) { parseErrorf(parseState, firstChar, "float literal is too large"); }

	return result;
}

static bool roundSignificand(U64& significand64, bool hasNonZeroTailBits, U64 numSignificandBits)
{
	// Round to the nearest even significand.
	const bool lsb = significand64 & (U64(1) << (64 - numSignificandBits));
	const bool halfLSB = significand64 & (U64(1) << (64 - numSignificandBits - 1));
	hasNonZeroTailBits |= !!(significand64 & ((U64(1) << (64 - numSignificandBits - 1)) - 1));
	if(halfLSB && (lsb || hasNonZeroTailBits))
	{
		U64 addend = U64(1) << (64 - numSignificandBits);
		const bool overflowed = significand64 + addend < significand64;
		significand64 += addend;
		return overflowed;
	}
	return false;
}

// Parses a hexadecimal floating point literal, advancing nextChar past the parsed characters.
// Assumes it will only be called for input that's already been accepted by the lexer as a
// hexadecimal float literal.
template<typename Float> Float parseHexFloat(const char*& nextChar, ParseState* parseState)
{
	const char* firstChar = nextChar;

	typedef FloatComponents<Float> FloatComponents;
	typedef typename FloatComponents::Bits FloatBits;
	FloatComponents resultComponents;

	resultComponents.bits.sign = parseSign(nextChar) ? 1 : 0;

	WAVM_ASSERT(nextChar[0] == '0' && (nextChar[1] == 'x' || nextChar[1] == 'X'));
	nextChar += 2;

	// Parse hexits into a 64-bit fixed point number, keeping track of where the point is in
	// exponent.
	U64 significand64 = 0;
	bool hasSeenPoint = false;
	bool hasNonZeroTailBits = false;
	I64 exponent = 0;
	while(true)
	{
		U8 hexit = 0;
		if(tryParseHexit(nextChar, hexit))
		{
			// Once there are too many hexits to accumulate in the 64-bit fixed point number, ignore
			// the hexits, but continue to update exponent so we get an accurate but imprecise
			// number.
			if(significand64 <= UINT64_MAX / 16)
			{
				WAVM_ASSERT(significand64 * 16 + hexit >= significand64);
				significand64 = significand64 * 16 + hexit;
				exponent -= hasSeenPoint ? 4 : 0;
			}
			else
			{
				hasNonZeroTailBits |= hexit != 0;
				exponent += hasSeenPoint ? 0 : 4;
			}
		}
		else if(*nextChar == '_')
		{
			++nextChar;
		}
		else if(*nextChar == '.')
		{
			WAVM_ASSERT(!hasSeenPoint);
			hasSeenPoint = true;
			++nextChar;
		}
		else
		{
			break;
		}
	}

	// Parse an optional exponent.
	if(*nextChar == 'p' || *nextChar == 'P')
	{
		++nextChar;
		const bool isExponentNegative = parseSign(nextChar);
		const U64 userExponent = parseDecimalUnsignedInt(
			nextChar, parseState, U64(-I64(INT32_MIN)), "float literal exponent");
		exponent = isExponentNegative ? exponent - userExponent : exponent + userExponent;
	}

	if(!significand64)
	{
		// If both the integer and fractional part are zero, just return +/- zero.
		resultComponents.bits.exponent = 0;
		resultComponents.bits.significand = 0;
		return resultComponents.value;
	}
	else
	{
		// Normalize the significand so the most significand set bit is in the MSB.
		const U64 leadingZeroes = countLeadingZeroes(significand64);
		significand64 <<= leadingZeroes;
		exponent += 64;
		exponent -= leadingZeroes;

		if(exponent - 1 < FloatComponents::minNormalExponent)
		{
			// Renormalize the significand for an exponent of FloatComponents::minNormalExponent.
			const U64 subnormalShift = U64(I64(FloatComponents::minNormalExponent) - exponent);
			if(subnormalShift >= 64) { significand64 = 0; }
			else
			{
				hasNonZeroTailBits = significand64 & ((U64(1) << subnormalShift) - 1);
				significand64 >>= subnormalShift;
			}
			exponent += subnormalShift;

			// Round the significand as if it's subnormal.
			const bool overflowed = roundSignificand(
				significand64, hasNonZeroTailBits, FloatComponents::numSignificandBits);
			if(overflowed)
			{
				// If rounding the subnormal significand overflowed, then it rounded up to the
				// smallest normal number.
				resultComponents.bits.exponent = 1;
				resultComponents.bits.significand = 0;
			}
			else
			{
				// Subnormal significands are encoded as if their exponent is minNormalExponent, but
				// without the implicit leading 1.
				resultComponents.bits.exponent = 0;
				resultComponents.bits.significand
					= FloatBits(significand64 >> (64 - FloatComponents::numSignificandBits));
			}
		}
		else
		{
			// Round the significand.
			if(roundSignificand(
				   significand64, hasNonZeroTailBits, FloatComponents::numSignificandBits + 1))
			{
				significand64 = U64(1) << 63;
				++exponent;
			}

			// Drop the implicit leading 1 for normal encodings.
			WAVM_ASSERT(significand64 & (U64(1) << 63));
			significand64 <<= 1;
			--exponent;

			if(exponent > FloatComponents::maxNormalExponent)
			{
				// If the number is out of range, produce an error and return infinity.
				resultComponents.bits.exponent = FloatComponents::maxExponentBits;
				resultComponents.bits.significand = FloatComponents::maxSignificand;
				parseErrorf(parseState, firstChar, "hexadecimal float literal is out of range");
			}
			else
			{
				// Encode a normal floating point value.
				WAVM_ASSERT(exponent >= FloatComponents::minNormalExponent);
				WAVM_ASSERT(exponent <= FloatComponents::maxNormalExponent);
				resultComponents.bits.exponent
					= FloatBits(exponent + FloatComponents::exponentBias);
				resultComponents.bits.significand
					= FloatBits(significand64 >> (64 - FloatComponents::numSignificandBits));
			}
		}
	}

	return resultComponents.value;
}

// Tries to parse an numeric literal token as an integer, advancing cursor->nextToken.
// Returns true if it matched a token.
template<typename UnsignedInt>
bool tryParseInt(CursorState* cursor,
				 UnsignedInt& outUnsignedInt,
				 I64 minSignedValue,
				 U64 maxUnsignedValue,
				 bool allowSign = true)
{
	if(cursor->nextToken->type != t_decimalInt && cursor->nextToken->type != t_hexInt)
	{ return false; }

	const char* nextChar = cursor->parseState->string + cursor->nextToken->begin;
	const bool isNegative = allowSign && parseSign(nextChar);
	if(!allowSign && (*nextChar == '-' || *nextChar == '+')) { return false; }

	const U64 u64
		= cursor->nextToken->type == t_decimalInt
			  ? parseDecimalUnsignedInt(nextChar,
										cursor->parseState,
										isNegative ? -U64(minSignedValue) : maxUnsignedValue,
										"int literal")
			  : parseHexUnsignedInt(nextChar,
									cursor->parseState,
									isNegative ? -U64(minSignedValue) : maxUnsignedValue);

	if(minSignedValue == 0 && isNegative)
	{
		outUnsignedInt = 0;
		return false;
	}
	else
	{
		outUnsignedInt = isNegative ? UnsignedInt(-u64) : UnsignedInt(u64);

		++cursor->nextToken;
		WAVM_ASSERT(nextChar <= cursor->parseState->string + cursor->nextToken->begin);

		return true;
	}
}

// Tries to parse a numeric literal literal token as a float, advancing cursor->nextToken.
// Returns true if it matched a token.
template<typename Float> bool tryParseFloat(CursorState* cursor, Float& outFloat)
{
	const char* nextChar = cursor->parseState->string + cursor->nextToken->begin;
	switch(cursor->nextToken->type)
	{
	case t_decimalInt:
	case t_decimalFloat: outFloat = parseDecimalFloat<Float>(nextChar, cursor->parseState); break;
	case t_hexInt:
	case t_hexFloat: outFloat = parseHexFloat<Float>(nextChar, cursor->parseState); break;
	case t_floatNaN: outFloat = parseNaN<Float>(nextChar, cursor->parseState); break;
	case t_floatInf: outFloat = parseInfinity<Float>(nextChar); break;
	default:
		parseErrorf(cursor->parseState, cursor->nextToken, "expected float literal");
		return false;
	};

	++cursor->nextToken;
	WAVM_ASSERT(nextChar <= cursor->parseState->string + cursor->nextToken->begin);

	return true;
}

bool WAST::tryParseU64(CursorState* cursor, U64& outI64)
{
	return tryParseInt<U64>(cursor, outI64, 0, UINT64_MAX);
}

bool WAST::tryParseUptr(CursorState* cursor, Uptr& outUptr)
{
	return tryParseInt<Uptr>(cursor, outUptr, 0, UINTPTR_MAX);
}

U8 WAST::parseU8(CursorState* cursor, bool allowSign)
{
	U8 result;
	if(!tryParseInt<U8>(cursor, result, 0, 255, allowSign))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected u8 literal");
		throw RecoverParseException();
	}
	return result;
}

U32 WAST::parseU32(CursorState* cursor)
{
	U32 result;
	if(!tryParseInt<U32>(cursor, result, 0, UINT32_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected u32 literal");
		throw RecoverParseException();
	}
	return result;
}

U64 WAST::parseU64(CursorState* cursor)
{
	U64 result;
	if(!tryParseInt<U64>(cursor, result, 0, UINT64_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected u64 literal");
		throw RecoverParseException();
	}
	return result;
}

I8 WAST::parseI8(CursorState* cursor)
{
	U32 result;
	if(!tryParseInt<U32>(cursor, result, INT8_MIN, UINT8_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected i8 literal");
		throw RecoverParseException();
	}
	return I8(result);
}

I16 WAST::parseI16(CursorState* cursor)
{
	U32 result;
	if(!tryParseInt<U32>(cursor, result, INT16_MIN, UINT16_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected i16 literal");
		throw RecoverParseException();
	}
	return I16(result);
}

I32 WAST::parseI32(CursorState* cursor)
{
	U32 result;
	if(!tryParseInt<U32>(cursor, result, INT32_MIN, UINT32_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected i32 literal");
		throw RecoverParseException();
	}
	return I32(result);
}

I64 WAST::parseI64(CursorState* cursor)
{
	U64 result;
	if(!tryParseInt<U64>(cursor, result, INT64_MIN, UINT64_MAX))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected i64 literal");
		throw RecoverParseException();
	}
	return I64(result);
}

F32 WAST::parseF32(CursorState* cursor)
{
	F32 result;
	if(!tryParseFloat(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected f32 literal");
		throw RecoverParseException();
	}
	return result;
}

F64 WAST::parseF64(CursorState* cursor)
{
	F64 result;
	if(!tryParseFloat(cursor, result))
	{
		parseErrorf(cursor->parseState, cursor->nextToken, "expected f64 literal");
		throw RecoverParseException();
	}
	return result;
}

V128 WAST::parseV128(CursorState* cursor)
{
	V128 result;
	switch(cursor->nextToken->type)
	{
	case t_i8x16:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 16; ++laneIndex)
		{ result.i8x16[laneIndex] = parseI8(cursor); }
		break;
	case t_i16x8:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 8; ++laneIndex)
		{ result.i16x8[laneIndex] = parseI16(cursor); }
		break;
	case t_i32x4:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{ result.i32x4[laneIndex] = parseI32(cursor); }
		break;
	case t_i64x2:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{ result.i64x2[laneIndex] = parseI64(cursor); }
		break;
	case t_f32x4:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 4; ++laneIndex)
		{ result.f32x4[laneIndex] = parseF32(cursor); }
		break;
	case t_f64x2:
		++cursor->nextToken;
		for(Uptr laneIndex = 0; laneIndex < 2; ++laneIndex)
		{ result.f64x2[laneIndex] = parseF64(cursor); }
		break;
	default:
		parseErrorf(cursor->parseState,
					cursor->nextToken,
					"expected 'i8x6', 'i16x8', 'i32x4', 'i64x2', 'f32x4', or 'f64x2'");
		throw RecoverParseException();
	};

	return result;
}
