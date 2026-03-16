/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file
 *
 * Buffer safe printf functions for portability to archaic platforms.
 */

#ifndef OPAL_PRINTF_H
#define OPAL_PRINTF_H

#include "opal_config.h"

#include <stdarg.h>
#include <stdlib.h>

BEGIN_C_DECLS

/**
 * Writes to a string under the control of a format string
 * that specifies how subsequent arguments are converted for output.
 *
 * @param str   Output string buffer
 * @param size  Size of string buffer
 * @param fmt   Output format
 * @return      Length of output string
 *
 * At most size-1 characters are printed into the output string (the
 * size'th character then gets the terminating `\0'); if the return
 * value is greater than or equal to the size argument, the string was
 * too short and some of the printed characters were discarded.  The
 * output is always null-terminated.
 *
 * Returns the number of characters that would have been printed if
 * the size were unlimited (again, not including the final `\0').
 *
 * THIS IS A PORTABILITY FEATURE: USE snprintf() in CODE.
 */
OPAL_DECLSPEC int  opal_snprintf(char *str, size_t size, const char *fmt, ...) __opal_attribute_format__(__printf__, 3, 4);


/**
 * Writes to a string under the control of a format string that
 * specifies how arguments accessed via the variable-length argument
 * facilities of stdarg(3) are converted for output.
 *
 * @param str   Output string buffer
 * @param size  Size of string buffer
 * @param fmt   Output format
 * @param ap    Variable argument list pointer
 * @return      Length of output string
 *
 * At most size-1 characters are printed into the output string (the
 * size'th character then gets the terminating `\0'); if the return
 * value is greater than or equal to the size argument, the string was
 * too short and some of the printed characters were discarded.  The
 * output is always null-terminated.
 *
 * Returns the number of characters that would have been printed if
 * the size were unlimited (again, not including the final `\0').
 *
 * THIS IS A PORTABILITY FEATURE: USE vsnprintf() in CODE.
 */
OPAL_DECLSPEC int  opal_vsnprintf(char *str, size_t size, const char *fmt, va_list ap) __opal_attribute_format__(__printf__, 3, 0);

/**
 * Allocates and writes to a string under the control of a format
 * string that specifies how subsequent arguments are converted for
 * output.
 *
 * @param *ptr  Pointer to output string buffer
 * @param fmt   Output format
 * @return      Length of output string
 *
 * Sets *ptr to be a pointer to a buffer sufficiently large to hold
 * the formatted string.  This pointer should be passed to free(3) to
 * release the allocated storage when it is no longer needed.  If
 * sufficient space cannot be allocated, asprintf() and vasprintf()
 * will return -1 and set ret to be a NULL pointer.
 *
 * Returns the number of characters printed.
 *
 * THIS IS A PORTABILITY FEATURE: USE asprintf() in CODE.
 */
OPAL_DECLSPEC int  opal_asprintf(char **ptr, const char *fmt, ...) __opal_attribute_format__(__printf__, 2, 3);


/**
 * Allocates and writes to a string under the control of a format
 * string that specifies how arguments accessed via the
 * variable-length argument facilities of stdarg(3) are converted for
 * output.
 *
 * @param *ptr  Pointer to output string buffer
 * @param fmt   Output format
 * @param ap    Variable argument list pointer
 * @return      Length of output string
 *
 * Sets *ptr to be a pointer to a buffer sufficiently large to hold
 * the formatted string.  This pointer should be passed to free(3) to
 * release the allocated storage when it is no longer needed.  If
 * sufficient space cannot be allocated, asprintf() and vasprintf()
 * will return -1 and set ret to be a NULL pointer.
 *
 * Returns the number of characters printed.
 *
 * THIS IS A PORTABILITY FEATURE: USE vasprintf() in CODE.
 */
OPAL_DECLSPEC int  opal_vasprintf(char **ptr, const char *fmt, va_list ap) __opal_attribute_format__(__printf__, 2, 0);


END_C_DECLS

#endif /* OPAL_PRINTF_H */

