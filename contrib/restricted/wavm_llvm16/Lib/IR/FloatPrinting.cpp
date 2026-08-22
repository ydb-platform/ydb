#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/FloatComponents.h"

using namespace WAVM;

// Prints a floating point value to a string, using the WebAssembly syntax for text floats.
template<typename Float> static std::string floatAsString(Float f)
{
	static constexpr Uptr numSignificandHexits = FloatComponents<Float>::numSignificandHexits;

	FloatComponents<Float> components;
	components.value = f;

	if(components.bits.exponent == FloatComponents<Float>::maxExponentBits
	   && components.bits.significand == 0)
	{
		// Handle infinity.
		return components.bits.sign ? "-inf" : "+inf";
	}
	else
	{
		// Needs at least numSignificandHexits + 12 chars.
		// -nan:0x<significand>
		// -0x1.<significand>p+1023\0
		char buffer[FloatComponents<Float>::numSignificandHexits + 12];
		char* nextChar = buffer;

		if(components.bits.exponent == FloatComponents<Float>::maxExponentBits)
		{
			// Handle NaN.
			*nextChar++ = components.bits.sign ? '-' : '+';
			*nextChar++ = 'n';
			*nextChar++ = 'a';
			*nextChar++ = 'n';
			*nextChar++ = ':';
			*nextChar++ = '0';
			*nextChar++ = 'x';

			// Print the significand hexits.
			for(Uptr hexitIndex = 0; hexitIndex < numSignificandHexits; ++hexitIndex)
			{
				const U8 hexitValue
					= (components.bits.significand >> ((numSignificandHexits - hexitIndex - 1) * 4))
					  & 0xf;
				*nextChar++ = hexitValue >= 10 ? ('a' + hexitValue - 10) : ('0' + hexitValue);
			}
		}
		else
		{
			// Handle non-special floats.
			if(components.bits.sign) { *nextChar++ = '-'; }
			*nextChar++ = '0';
			*nextChar++ = 'x';

			// If the exponent bits are non-zero, then it's a normal float with an implicit
			// leading 1 bit.
			U64 significand64 = U64(components.bits.significand)
								<< (64 - FloatComponents<Float>::numSignificandBits);
			if(components.bits.exponent != 0) { *nextChar++ = '1'; }
			else
			{
				// If the exponent bits are zero, then it's a denormal float without an implicit
				// leading 1 bit. The significand is effectively shifted left 1 bit to replace
				// that implicit bit.
				*nextChar++ = '0' + (significand64 >> 63) % 2;
				significand64 <<= 1;
			}
			*nextChar++ = '.';

			// Print the significand hexits.
			for(Uptr hexitIndex = 0; hexitIndex < numSignificandHexits; ++hexitIndex)
			{
				const U8 hexitValue = U8(significand64 >> (64 - hexitIndex * 4 - 4)) & 0xf;
				*nextChar++ = hexitValue >= 10 ? ('a' + hexitValue - 10) : ('0' + hexitValue);
			}

			// For non-special floats, print the exponent.
			*nextChar++ = 'p';

			// Print the exponent sign.
			Iptr exponent = Uptr(components.bits.exponent) - FloatComponents<Float>::exponentBias;
			if(exponent > 0) { *nextChar++ = '+'; }
			else
			{
				*nextChar++ = '-';
				exponent = -exponent;
			}

			// Print the exponent digits.
			WAVM_ASSERT(exponent < 10000);
			const Uptr numDigits
				= exponent >= 1000 ? 4 : exponent >= 100 ? 3 : exponent >= 10 ? 2 : 1;
			for(Uptr digitIndex = 0; digitIndex < numDigits; ++digitIndex)
			{
				nextChar[numDigits - digitIndex - 1] = '0' + exponent % 10;
				exponent /= 10;
			}
			nextChar += numDigits;
		}

		WAVM_ASSERT(nextChar < buffer + sizeof(buffer));
		*nextChar = 0;

		return buffer;
	}
}

std::string WAVM::IR::asString(F32 f32) { return floatAsString(f32); }

std::string WAVM::IR::asString(F64 f64) { return floatAsString(f64); }
