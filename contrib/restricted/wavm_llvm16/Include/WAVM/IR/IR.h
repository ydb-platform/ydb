#pragma once

#include "WAVM/Inline/BasicTypes.h"

namespace WAVM { namespace IR {
	static constexpr U64 maxMemory32Pages = 65536;           // 2^16 pages -> 2^32 bytes
	static constexpr U64 maxMemory64Pages = 281474976710656; // 2^48 pages -> 2^64 bytes
	static constexpr U64 maxTable32Elems = UINT32_MAX;
	static constexpr U64 maxTable64Elems = UINT64_MAX;
	static constexpr Uptr numBytesPerPage = 65536;
	static constexpr Uptr numBytesPerPageLog2 = 16;
	static constexpr Uptr maxReturnValues = 16;
}}
