#pragma once

#include "WAVM/Inline/BasicTypes.h"

namespace WAVM {
	template<typename Float> struct FloatComponents;

	// The components of a 64-bit float
	template<> struct FloatComponents<F64>
	{
		typedef U64 Bits;
		typedef F64 Float;

		static constexpr I64 maxSignificand = 0xfffffffffffff;
		static constexpr Uptr numSignificandBits = 52;
		static constexpr Uptr numSignificandHexits = 13;
		static constexpr I64 canonicalSignificand = 0x8000000000000ull;

		static constexpr I64 denormalExponent = -1023;
		static constexpr I64 minNormalExponent = -1022;
		static constexpr I64 maxNormalExponent = 1023;
		static constexpr I64 exponentBias = 1023;
		static constexpr I64 maxExponentBits = 0x7ff;

		union
		{
			struct
			{
				U64 significand : 52;
				U64 exponent : 11;
				U64 sign : 1;
			} bits;
			Float value;
			Bits bitcastInt;
		};
	};

	// The components of a 32-bit float.
	template<> struct FloatComponents<F32>
	{
		typedef U32 Bits;
		typedef F32 Float;

		static constexpr I32 maxSignificand = 0x7fffff;
		static constexpr Uptr numSignificandBits = 23;
		static constexpr Uptr numSignificandHexits = 6;
		static constexpr I32 canonicalSignificand = 0x400000;

		static constexpr I32 denormalExponent = -127;
		static constexpr I32 minNormalExponent = -126;
		static constexpr I32 maxNormalExponent = 127;
		static constexpr I32 exponentBias = 127;
		static constexpr I32 maxExponentBits = 0xff;

		union
		{
			struct
			{
				U32 significand : 23;
				U32 exponent : 8;
				U32 sign : 1;
			} bits;
			Float value;
			Bits bitcastInt;
		};
	};
}
