#pragma once

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"

namespace WAVM {
	// A stream that uses a combination of a PRNG and input data to produce pseudo-random values.
	struct RandomStream
	{
		RandomStream(const U8* inData, Uptr numBytes)
		: next(inData), end(inData + numBytes), denominator(0), numerator(0), seed(0)
		{
			refill();
		}

		// Returns a pseudo-random value between 0 and maxResult, inclusive.
		template<typename Result> Result get(Result maxResult)
		{
			Result result = Result(get64(maxResult));
			WAVM_ASSERT(result <= maxResult);
			return result;
		}

	private:
		const U8* next;
		const U8* end;

		U64 denominator;
		U64 numerator;

		U64 seed;

		void refill()
		{
			while(denominator <= UINT32_MAX)
			{
				if(next < end) { numerator += (denominator + 1) * *next++; }
				denominator += 255 * (denominator + 1);
			};
		}

		U32 get32(U32 maxResult)
		{
			if(maxResult == 0) { return 0; }

			WAVM_ASSERT(denominator >= maxResult);
			seed ^= numerator;
			const U32 result = U32(seed % (U64(maxResult) + 1));
			seed /= (U64(maxResult) + 1);
			numerator /= (U64(maxResult) + 1);
			denominator /= (U64(maxResult) + 1);
			seed = 6364136223846793005 * seed + 1442695040888963407;
			refill();
			return result;
		}

		U64 get64(U64 maxResult)
		{
			U64 result = get32(U32(maxResult));
			result += U64(get32(U32(maxResult >> 32))) << 32;
			WAVM_ASSERT(result <= maxResult);
			return result;
		}
	};
}
