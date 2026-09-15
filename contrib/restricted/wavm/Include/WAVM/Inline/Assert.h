#pragma once

#include <cstdarg>
#include <stdexcept>
#include "WAVM/Inline/Config.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Error.h"

#define WAVM_ENABLE_ASSERTS 1

#ifndef WAVM_STRINGIZE
#define WAVM_STRINGIZE_DETAIL(x) #x
#define WAVM_STRINGIZE(x) WAVM_STRINGIZE_DETAIL(x)
#endif

#define WAVM_ASSERT(condition)                                                                                       \
	if(!(condition))                                                                                                 \
	{                                                                                                                \
		const char* message = "WAVM assertion failed at " __FILE__ ":" WAVM_STRINGIZE(__LINE__) " (" #condition ")"; \
		throw std::runtime_error(message);                                                                           \
	}

#define WAVM_ERROR_UNLESS(condition)                                                                                    \
	if(!(condition))                                                                                                    \
	{                                                                                                                   \
		const char* message = "WAVM error unless failed at " __FILE__ ":" WAVM_STRINGIZE(__LINE__) " (" #condition ")"; \
		throw std::runtime_error(message);                                                                              \
	}

#define WAVM_UNREACHABLE(condition)                                                                             \
	while(true)                                                                                                 \
	{                                                                                                           \
		const char* message = "WAVM unreachable at " __FILE__ ":" WAVM_STRINGIZE(__LINE__) " (" #condition ")"; \
		throw std::runtime_error(message);                                                                      \
	}
