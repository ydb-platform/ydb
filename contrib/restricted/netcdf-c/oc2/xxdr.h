/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information.
*/

/*
 * Copyright (c) 2009, Sun Microsystems, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * - Neither the name of Sun Microsystems, Inc. nor the names of its
 *   contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *      from: @(#)xdr.h 1.19 87/04/22 SMI
 *      from: @(#)xdr.h        2.2 88/07/29 4.0 RPCSRC
 *      $FreeBSD: src/include/rpc/xdr.h,v 1.23 2003/03/07 13:19:40 nectar Exp $
 *      $NetBSD: xdr.h,v 1.19 2000/07/17 05:00:45 matt Exp $
 */

/* Define our own implementation of the needed
   elements of XDR. Assumes read-only
*/

#ifndef XXDR_H
#define XXDR_H

/*
 * This is the number of bytes per unit of external data.
 */
#define XDRUNIT        (4)

#if 1
/* faster version when XDRUNIT is a power of two */
# define RNDUP(x)  ((off_t)(((x) + XDRUNIT - 1) & ~(XDRUNIT - 1)))
#else /* old version */
#define RNDUP(x)  ((off_t)((((x) + XDRUNIT - 1) / XDRUNIT) \
    * XDRUNIT))
#endif

#if 0
/* signature: void swapinline16(unsigned short* sp) */
#define swapinline16(ip) \
{ \
    char dst[2]; \
    char* src = (char*)(sp); \
    dst[0] = src[1]; \
    dst[1] = src[0]; \
    *(sp) = *((unsigned short*)dst); \
}

/* signature: void swapinline32(unsigned int* ip) */
#define swapinline32(ip) \
{ \
    char dst[4]; \
    char* src = (char*)(ip); \
    dst[0] = src[3]; \
    dst[1] = src[2]; \
    dst[2] = src[1]; \
    dst[3] = src[0]; \
    *(ip) = *((unsigned int*)dst); \
}

/* signature: void swapinline64(unsigned long long* ip) */
#define swapinline64(ip) \
{ \
    char dst[8]; \
    char* src = (char*)(ip); \
    dst[0] = src[7]; \
    dst[1] = src[6]; \
    dst[2] = src[5]; \
    dst[3] = src[4]; \
    dst[4] = src[3]; \
    dst[5] = src[2]; \
    dst[6] = src[1]; \
    dst[7] = src[0]; \
    *ip = *((unsigned long long*)dst); \
}
#endif /*0*/

#ifdef OCIGNORE
/* Warning dst and src should not be the same memory (assert &iswap != &i) */
#define xxdrntoh(dst,src) if(xxdr_network_order){dst=src;}else{swapinline32(dst,src);}
#define xxdrntohll(dst,src) if(xxdr_network_order){dst=src;}else{swapinline64(dst,src);}
#define xxdrhton(dst,src) xxdrntoh(dst,src)
#define xxdrhtonll(dst,src) xxdrntohll(dst,src)
#endif

/* Double needs special handling */
extern void xxdrntohdouble(char*,double*);

/*
 * The XDR handle.
 * Contains operation which is being applied to the stream,
 * an operations vector for the particular implementation (e.g. see xdr_mem.c),
 * and two private fields for the use of the particular implementation.
 */
typedef struct XXDR XXDR; /* forward */

/* Assume |off_t| == |void*| */
struct XXDR {
  char* data;
  off_t pos; /* relative to data; may be a cache of underlying stream pos */
  int valid;         /* 1=>underlying stream pos == pos */
  off_t base; /* beginning of data in case bod != 0*/
  off_t length; /* total size of available data (relative to base)*/
  /* Define minimum needed case specific operators */
  int (*getbytes)(XXDR*,char*,off_t);
  int (*setpos)(XXDR*,off_t);
  off_t (*getpos)(XXDR*);
  off_t (*getavail)(XXDR*);
  void (*free)(XXDR*); /* xdr kind specific free function */
};

/* Track network order */
extern int xxdr_network_order; /* does this machine match network order? */

/* Read-only operations */

/* Get exactly count bytes from underlying
   stream; unlike opaque, this will
   not round up to the XDRUNIT boundary
*/
extern int xxdr_getbytes(XXDR*,char*,off_t);

/* get a single unsigned char from underlying stream*/
extern int xxdr_uchar(XXDR* , unsigned char*);

/* get a single unsigned short from underlying stream*/
extern int xxdr_ushort(XXDR* , unsigned short*);

/* get an int from underlying stream*/
extern int xxdr_uint(XXDR* , unsigned int*);

/* get an int from underlying stream*/
extern int xxdr_ulonglong(XXDR* , unsigned long long*);

/* get a float from underlying stream*/
extern int xxdr_float(XXDR* , float*);

/* get a double from underlying stream*/
extern int xxdr_double(XXDR* , double*);

/* get some bytes from underlying stream;
   Warning: will read up to the next XDRUNIT boundary
*/
extern int xxdr_opaque(XXDR*, char*, off_t);

/* get counted string from underlying stream*/
extern int xxdr_string(XXDR*, char**, off_t*);

/* returns bytes off from beginning*/
extern off_t xxdr_getpos(XXDR*);

/* reposition the stream*/
extern int xxdr_setpos(XXDR*, off_t);

/* get remaining data available starting at current position */
extern off_t xxdr_getavail(XXDR*);

/* free up XXDR  structure */
void xxdr_free(XXDR*);

/* File and memory creators */
extern XXDR* xxdr_filecreate(FILE* file, off_t bod);
extern XXDR* xxdr_memcreate(char* mem, off_t memsize, off_t bod);

/* Misc */
extern int xxdr_skip(XXDR* xdrs, off_t len); /* WARNING: will skip exactly len bytes;
                                                any rounding must be done by caller */

extern int xxdr_skip_strings(XXDR* xdrs, off_t n);

extern unsigned int xxdr_roundup(off_t n); /* procedural version of RNDUP macro */

extern void xxdr_init(void);

/* Define some inlines */
#define xxdr_length(xdrs) ((xdrs)->length)

#endif /*XXDR_H*/

