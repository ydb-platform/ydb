#pragma once

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

// Performs a compile-time check of log arguments validity.
// Valid argument lists are:
// 1. (format, args...)
// 2. (error, format, args...)
// If format is not a string literal or argument list
// is not valid, no check is made -- macro turns to
// a no-op.
#define STATIC_ANALYSIS_CHECK_LOG_FORMAT(...)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define STATIC_ANALYSIS_INL_H_
#include "static_analysis-inl.h"
#undef STATIC_ANALYSIS_INL_H_
