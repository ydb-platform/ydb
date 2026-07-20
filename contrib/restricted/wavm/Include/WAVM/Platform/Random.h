#pragma once

#include "WAVM/Inline/BasicTypes.h"

namespace WAVM { namespace Platform {
	WAVM_API void getCryptographicRNG(U8* outRandomBytes, Uptr numBytes);
}}
