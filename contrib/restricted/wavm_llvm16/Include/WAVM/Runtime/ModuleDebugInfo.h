#pragma once

#include <memory>
#include <string>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Runtime {

	//! Parses WebAssembly DWARF (.debug_*) from original .wasm bytes for trap stack
	//! symbolication (Wasmtime-style wasm_backtrace_details).
	struct ModuleDebugInfo
	{
		struct Location
		{
			std::string fileName;
			Uptr line = 0;
		};

		WAVM_API static std::shared_ptr<ModuleDebugInfo> tryCreate(const U8* wasmBytes,
																   Uptr numWasmBytes);

		WAVM_API bool lookup(U32 codeSectionRelativePc, Location& outLocation) const;

		ModuleDebugInfo(const ModuleDebugInfo&) = delete;
		ModuleDebugInfo& operator=(const ModuleDebugInfo&) = delete;
		~ModuleDebugInfo();

	private:
		struct Impl;
		std::unique_ptr<Impl> impl;

		explicit ModuleDebugInfo(std::unique_ptr<Impl>&& inImpl);
	};

}}
