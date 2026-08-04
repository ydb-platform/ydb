#pragma once

#include <stdarg.h>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

// Debug logging.
namespace WAVM { namespace Log {
	// Allow filtering the logging by category.
	enum Category
	{
		error,
		debug,
		metrics,
		output,
		traceValidation,
		traceCompilation,
		num
	};
	WAVM_API void setCategoryEnabled(Category category, bool enable);
	WAVM_API bool isCategoryEnabled(Category category);

	// Print some categorized, formatted string, and flush the output. Newline is not included.
	WAVM_API void printf(Category category, const char* format, ...) WAVM_VALIDATE_AS_PRINTF(2, 3);
	WAVM_API void vprintf(Category category, const char* format, va_list argList);

	// Set a function that is called whenever something is logged, instead of writing it to
	// stdout/stderr. setOutputFunction(nullptr) will reset the output function to the initial
	// state, and redirect log messages to stdout/stderr again.
	// outputFunction may be called from any thread without any locking, so it must be thread-safe.
	typedef void OutputFunction(Category category, const char* message, Uptr numChars);
	WAVM_API void setOutputFunction(OutputFunction* outputFunction);
}}
