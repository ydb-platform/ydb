#include <Windows.h>
#include <bcrypt.h>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Random.h"

using namespace WAVM;
using namespace WAVM::Platform;

void Platform::getCryptographicRNG(U8* outRandomBytes, Uptr numBytes)
{
	WAVM_ERROR_UNLESS(numBytes <= ULONG_MAX);

	WAVM_ERROR_UNLESS(!BCryptGenRandom(
		nullptr, outRandomBytes, ULONG(numBytes), BCRYPT_USE_SYSTEM_PREFERRED_RNG));
}
