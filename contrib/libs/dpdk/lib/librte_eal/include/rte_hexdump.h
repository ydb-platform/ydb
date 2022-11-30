/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2014 Intel Corporation
 */

#ifndef _RTE_HEXDUMP_H_
#define _RTE_HEXDUMP_H_

/**
 * @file
 * Simple API to dump out memory in a special hex format.
 */

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
* Dump out memory in a special hex dump format.
*
* @param f
*		A pointer to a file for output
* @param title
*		If not NULL this string is printed as a header to the output.
* @param buf
*		This is the buffer address to print out.
* @param len
*		The number of bytes to dump out
* @return
*		None.
*/

extern void
rte_hexdump(FILE *f, const char * title, const void * buf, unsigned int len);

/**
* Dump out memory in a hex format with colons between bytes.
*
* @param f
*		A pointer to a file for output
* @param title
*		If not NULL this string is printed as a header to the output.
* @param buf
*		This is the buffer address to print out.
* @param len
*		The number of bytes to dump out
* @return
*		None.
*/

void
rte_memdump(FILE *f, const char * title, const void * buf, unsigned int len);


#ifdef __cplusplus
}
#endif

#endif /* _RTE_HEXDUMP_H_ */
