#pragma once

#include <vector>
#include "WAVM/IR/Validate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Logging/Logging.h"

namespace WAVM { namespace IR {
	struct Module;
}}

namespace WAVM { namespace WASM {
	// The magic number that is at the beginning of every WASM binary module.
	static constexpr U8 magicNumber[4] = {0x00, 0x61, 0x73, 0x6d};

	// Saves a binary module.
	WAVM_API std::vector<U8> saveBinaryModule(const IR::Module& module);

	// Loads a binary module, returning either an error or a module.
	// If true is returned, the load succeeded, and outModule contains the loaded module.
	// If false is returned, the load failed. If outError != nullptr, *outError will contain the
	// error that caused the load to fail.
	struct LoadError
	{
		enum class Type
		{
			malformed,
			invalid
		};
		Type type;
		std::string message;
	};
	WAVM_API bool loadBinaryModule(const U8* wasmBytes,
								   Uptr numWASMBytes,
								   IR::Module& outModule,
								   LoadError* outError = nullptr);
}}
