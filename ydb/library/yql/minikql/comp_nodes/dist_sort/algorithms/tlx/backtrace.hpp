/*******************************************************************************
 * tlx/backtrace.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2008-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_BACKTRACE_HEADER
#define TLX_BACKTRACE_HEADER

#include <tlx/define/attribute_format_printf.hpp>

#include <cstdio>

namespace tlx {

//! \name Stack Backtrace Printing
//! \{

/*!
 * Print a plain hex stack backtrace of the called function to FILE* out.
 */
void print_raw_backtrace(FILE* out = stderr, unsigned int max_frames = 63);

/*!
 * Print a plain hex stack backtrace of the called function to FILE* out,
 * prefixed with the given printf formatted output.
 */
void print_raw_backtrace(FILE* out, unsigned int max_frames,
                         const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(3, 4);

/*!
 * Print a demangled stack backtrace of the caller function to FILE* out.
 *
 * \warning The binary has to be compiled with <tt>-rdynamic</tt> for meaningful
 * output.
 */
void print_cxx_backtrace(FILE* out = stderr, unsigned int max_frames = 63);

/*!
 * Install SIGSEGV signal handler and output backtrace on segmentation fault.
 * Compile with `-rdynamic` for more useful output.
 */
void enable_segv_backtrace();

//! \}

} // namespace tlx

#endif // !TLX_BACKTRACE_HEADER

/******************************************************************************/
