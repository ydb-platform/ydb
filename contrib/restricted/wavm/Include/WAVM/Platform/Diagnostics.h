#pragma once

#include <string>
#include <vector>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/InlineArray.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	//
	// Call stack and exceptions
	//

	// Describes a call stack.
	struct CallStack
	{
		static constexpr Uptr maxFrames = 32;

		struct Frame
		{
			Uptr ip;
		};

		InlineArray<Frame, maxFrames> frames;

		~CallStack() = default;
	};

	// Describes the source of an instruction in a native module.
	struct InstructionSource
	{
		std::string module;
		std::string function;
		Uptr instructionOffset;

		friend std::string asString(const InstructionSource& source)
		{
			std::string result = source.module;
			if(source.function.size())
			{
				result += '!';
				result += source.function;
			}
			result += '+';
			result += std::to_string(source.instructionOffset);
			return result;
		}
	};

	// Captures the execution context of the caller.
	WAVM_API CallStack captureCallStack(Uptr numOmittedFramesFromTop = 0);

	// Looks up the source of an instruction from a native module.
	WAVM_API bool getInstructionSourceByAddress(Uptr ip, InstructionSource& outSource);

	// Prints a profile of heap and virtual memory allocations.
	WAVM_API void printMemoryProfile();

	WAVM_API void registerVirtualAllocation(Uptr numBytes);
	WAVM_API void deregisterVirtualAllocation(Uptr numBytes);
}}
