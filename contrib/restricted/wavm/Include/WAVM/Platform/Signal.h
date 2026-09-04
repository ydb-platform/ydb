#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Diagnostics.h"

namespace WAVM { namespace Platform {
	struct Signal
	{
		enum class Type
		{
			invalid = 0,
			accessViolation,
			stackOverflow,
			intDivideByZeroOrOverflow
		};

		Type type = Type::invalid;

		union
		{
			struct
			{
				Uptr address;
			} accessViolation;
		};
	};

	WAVM_API bool catchSignals(void (*thunk)(void*),
							   bool (*filter)(void*, Signal, CallStack&&),
							   void* argument);

	WAVM_API void registerEHFrames(const U8* imageBase, const U8* ehFrames, Uptr numBytes);
	WAVM_API void deregisterEHFrames(const U8* imageBase, const U8* ehFrames, Uptr numBytes);
}}
