/*******************************************************************************
 * tlx/logger.hpp
 *
 * Simple logging methods using ostream output.
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_LOGGER_HEADER
#define TLX_LOGGER_HEADER

#include <tlx/logger/core.hpp>

namespace tlx {

//! Explicitly specify the condition for logging
#define LOGC(cond) TLX_LOGC(cond)

//! Default logging method: output if the local debug variable is true.
#define LOG LOGC(debug)

//! Override default output: never or always output log.
#define LOG0 LOGC(false)
#define LOG1 LOGC(true)

//! Explicitly specify the condition for logging
#define sLOGC(cond) TLX_sLOGC(cond)

//! Default logging method: output if the local debug variable is true.
#define sLOG sLOGC(debug)

//! Override default output: never or always output log.
#define sLOG0 sLOGC(false)
#define sLOG1 sLOGC(true)

} // namespace tlx

#endif // !TLX_LOGGER_HEADER

/******************************************************************************/
