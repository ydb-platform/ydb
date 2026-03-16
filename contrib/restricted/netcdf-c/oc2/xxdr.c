/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

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

#undef ENDIAN_VALIDATE
#undef XXDRTRACE

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#ifdef HAVE_STDARG_H
#include <stdarg.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#ifdef _MSC_VER
#include <wchar.h>
#include <sys/types.h>
#endif

#ifdef ENDIAN_VALIDATE
#include <arpa/inet.h>
#endif

#include "ncexternl.h"
#include "ncutil.h"
#include "xxdr.h"


int xxdr_network_order; /* network order is big endian */
static int xxdr_big_endian; /* what is this machine? */

#ifdef XXDRTRACE
static void
xxdrtrace(XXDR* xdr, char* where, off_t arg)
{
fprintf(stderr,"xxdr: %s: arg=%ld ; pos=%ld len=%ld\n",
        where,arg,(long)xdr->pos,(long)xdr->length);
fflush(stderr);
}
#else
#define xxdrtrace(x,y,z)
#endif

/* Read-only operations */

int xxdr_getbytes(XXDR* xdrs, char* memory, off_t count)
{
    if(!memory) return 0;
    if(!xdrs->getbytes(xdrs,memory,count))
	return 0;
    return 1;
}

/* get a (unsigned) char from underlying stream*/
int
xxdr_uchar(XXDR* xdr, unsigned char* ip)
{
   unsigned int ii;
   if(!ip) return 0;
   if(!xdr->getbytes(xdr,(char*)&ii,(off_t)sizeof(unsigned int)))
	return 0;
    /*convert from network order*/
    if(!xxdr_network_order) {
        swapinline32(&ii);
    }
    *ip = (unsigned char)ii;
    return 1;
}

/* get an unsigned short from underlying stream*/
int
xxdr_ushort(XXDR* xdr, unsigned short* ip)
{
   unsigned int ii;
   if(!ip) return 0;
   if(!xdr->getbytes(xdr,(char*)&ii,(off_t)sizeof(unsigned int)))
	return 0;
    /*convert from network order*/
    if(!xxdr_network_order) {
        swapinline32(&ii);
    }
    *ip = (unsigned short)ii;
    return 1;
}

/* get a unsigned int from underlying stream*/
int
xxdr_uint(XXDR* xdr, unsigned int* ip)
{
   if(!ip) return 0;
   if(!xdr->getbytes(xdr,(char*)ip,(off_t)sizeof(*ip)))
	return 0;
    /*convert from network order*/
    if(!xxdr_network_order) {
        swapinline32(ip);
    }
    return 1;
}

/* get a long long  from underlying stream*/
int
xxdr_ulonglong(XXDR* xdr, unsigned long long* llp)
{
   /* Pull two units */
   if(!llp) return 0;
   if(!xdr->getbytes(xdr,(char*)llp,(off_t)sizeof(*llp)))
       return 0;
   /* Convert to signed/unsigned  */
   /*convert from network order*/
   if(!xxdr_network_order) {
       swapinline64(llp);
   }
   return 1;
}

/* get some bytes from underlying stream;
   will move xdrs pointer to next XDRUNIT boundary*/
int
xxdr_opaque(XXDR* xdr, char* mem, off_t count)
{
    off_t pos,rounded;
    if(!xdr->getbytes(xdr,mem,count))
	return 0;
    pos = xxdr_getpos(xdr);
    rounded = RNDUP(pos);
    return xxdr_skip(xdr,(rounded - pos));
}

/* get counted string from underlying stream*/
int
xxdr_string(XXDR* xdrs, char** sp, off_t* lenp)
{
    char* s;
    unsigned int len;
    if(!xxdr_uint(xdrs,&len)) return 0;
    s = (char*)malloc((size_t)len+1);
    if(s == NULL) return 0;
    if(!xxdr_opaque(xdrs,s,(off_t)len)) {
	free((void*)s);	
	return 0;
    }
    s[len] = '\0'; /* make sure it is null terminated */
    if(sp) *sp = s;
    if(lenp) *lenp = len;
    /* xxdr_opaque will have skipped any trailing bytes */
    return 1;    
}

/* returns bytes off from beginning*/
off_t
xxdr_getpos(XXDR* xdr)
{
    return xdr->getpos(xdr);
}

/* reposition the stream*/
int
xxdr_setpos(XXDR* xdr, off_t pos)
{
    return xdr->setpos(xdr,pos);
}

/* returns total available starting at current position */
off_t
xxdr_getavail(XXDR* xdr)
{
    return xdr->getavail(xdr);
}

/* free up XXDR  structure */
void
xxdr_free(XXDR* xdr)
{
    xdr->free(xdr);    
}

/***********************************/

/* Skip exactly "len" bytes in the input; any rounding must be done by the caller*/
int
xxdr_skip(XXDR* xdrs, off_t len)
{
    off_t pos;
    pos = xxdr_getpos(xdrs);
    pos = (pos + len);
    /* Removed the following; pos is unsigned. jhrg 9/30/13 */
    /* if(pos < 0) pos = 0; */
    return xxdr_setpos(xdrs,pos);
}

/* skip "n" string/bytestring instances in the input*/
int
xxdr_skip_strings(XXDR* xdrs, off_t n)
{
    while(n-- > 0) {
        unsigned int slen;
        off_t slenz;
	if(!xxdr_uint(xdrs,&slen)) return 0;
	slenz = (off_t)slen;
	slenz = RNDUP(slenz);
	if(xxdr_skip(xdrs,slenz)) return 0;
    }
    return 1;
}

unsigned int
xxdr_roundup(off_t n)
{
    return (unsigned int)RNDUP(n);
}

unsigned int
ocbyteswap(unsigned int i)
{
    unsigned int swap,b0,b1,b2,b3;
    b0 = (i>>24) & 0x000000ff;
    b1 = (i>>16) & 0x000000ff;
    b2 = (i>>8) & 0x000000ff;
    b3 = (i) & 0x000000ff;
    swap = (b0 | (b1 << 8) | (b2 << 16) | (b3 << 24));
    return swap;
}

/**************************************************/
/* File based xdr */
static void
xxdr_filefree(XXDR* xdrs)
{
    if(xdrs != NULL) {
        (void)fflush((FILE *)xdrs->data);
        free(xdrs);
    }
}

static int
xxdr_filegetbytes(XXDR* xdrs, char* addr, off_t len)
{
    int ok = 1;

xxdrtrace(xdrs,"getbytes",len);
    if(len < 0) len = 0;
    if(!xdrs->valid)
    {
        if(fseek((FILE *)xdrs->data, (long)(xdrs->pos + xdrs->base), 0) != 0) {
	    ok=0;
	    goto done;
	}
	xdrs->valid = 1;
    }
    if(xdrs->pos + len > xdrs->length)
        return 0;
    if(len > 0) {
        size_t count = fread(addr, (size_t)len, (size_t)1, (FILE*)xdrs->data);
        if(count == 0) {
	    ok=0;
	    goto done;
	}
    }
    xdrs->pos += len;
done:
    return ok;
}

static off_t
xxdr_filegetpos(XXDR* xdrs)
{
xxdrtrace(xdrs,"getpos",0);
    return xdrs->pos;
}

static off_t
xxdr_filegetavail(XXDR* xdrs)
{
xxdrtrace(xdrs,"getavail",0);
    return (xdrs->length - xdrs->pos);
}

static int
xxdr_filesetpos(XXDR* xdrs, off_t pos) 
{ 
    int ok = 1;
xxdrtrace(xdrs,"setpos",pos);
    if(pos == xdrs->pos) goto done;
    if(pos < 0) pos = 0;
    if(pos > xdrs->length) {ok=0;goto done;}
    xdrs->pos = pos;
    xdrs->valid = 0;
done:
    return ok;
}


/*
Modified to track the current position to avoid the file io
operation.  Not sure if this worth the effort because I
don't actually know the cost to doing an fseek
*/

/*
 * Initialize a stdio xdr stream.
 * Sets the xdr stream handle xdrs for use on the stream file.
 * Operation flag is set to op.
 */
XXDR*
xxdr_filecreate(FILE* file, off_t base)
{
    XXDR* xdrs = (XXDR*)calloc(1,sizeof(XXDR));
    if(xdrs != NULL) {
        xdrs->data = (void*)file;
        xdrs->base = base;
        xdrs->pos = 0;
	xdrs->valid = 0;
        if(fseek(file,0L,SEEK_END)) {
	    free(xdrs);
	    return NULL;
        }
	xdrs->length = (off_t)ftell(file);
	xdrs->length -= xdrs->base;
        xdrs->getbytes = xxdr_filegetbytes;
        xdrs->setpos = xxdr_filesetpos;
        xdrs->getpos = xxdr_filegetpos;
        xdrs->getavail = xxdr_filegetavail;
        xdrs->free = xxdr_filefree;
    }
xxdrtrace(xdrs,"create",base);
    return xdrs;
}

/**************************************************/
/* memory based xdr */

static void
xxdr_memfree(XXDR* xdrs)
{
    if(xdrs != NULL) {
        free(xdrs);
    }
}

static int
xxdr_memgetbytes(XXDR* xdrs, char* addr, off_t len)
{
    int ok = 1;

xxdrtrace(xdrs,"getbytes",len);
    if(len < 0) len = 0;
    if(xdrs->pos+len > xdrs->length) {ok=0; goto done;}
    if(len > 0) {
        memcpy(addr,(char*)xdrs->data+xdrs->base+xdrs->pos, (size_t)len);
    }
    xdrs->pos += len;
done:
    return ok;
}

static off_t
xxdr_memgetpos(XXDR* xdrs)
{
xxdrtrace(xdrs,"getpos",0);
    return xdrs->pos;
}

static off_t
xxdr_memgetavail(XXDR* xdrs)
{
xxdrtrace(xdrs,"getavail",0);
    return (xdrs->length - xdrs->pos);
}


static int
xxdr_memsetpos(XXDR* xdrs, off_t pos) 
{ 
    int ok = 1;
xxdrtrace(xdrs,"setpos",pos);
    if(pos == xdrs->pos) goto done;
    if(pos > xdrs->length) {ok=0; goto done;}
    xdrs->pos = pos;
done:
    return ok;
}

/*
Modified to track the current position to avoid the file io
operation.  Not sure if this worth the effort because I
don't actually know the cost to doing an fseek
*/

/*
 * Initialize a stdio xdr stream.
 * Sets the xdr stream handle xdrs for use on the
 * given memory starting at base offset.
 */
XXDR*
xxdr_memcreate(char* mem, off_t memsize, off_t base)
{
    XXDR* xdrs = (XXDR*)calloc(1,sizeof(XXDR));
    if(xdrs != NULL) {
	/* zero base memory */
        xdrs->data = (void*)(mem + base);
	xdrs->base = 0;
        xdrs->length = memsize - base;
        xdrs->pos = 0;
        xdrs->getbytes = xxdr_memgetbytes;
        xdrs->setpos = xxdr_memsetpos;
        xdrs->getpos = xxdr_memgetpos;
        xdrs->getavail = xxdr_memgetavail;
        xdrs->free = xxdr_memfree;
    }
xxdrtrace(xdrs,"create",base);
    return xdrs;
}

/* Float utility types */

/* get a float from underlying stream*/
int
xxdr_float(XXDR* xdr, float* fp)
{
   int status = 0;
   float f;
   unsigned int* data = (unsigned int*)&f;
   /* Pull one unit directly into a float */
   status = xxdr_uint(xdr,data);
   if(status && fp)
	*fp = f;
   return status;
}

/* Get a double from underlying stream */
int
xxdr_double(XXDR* xdr, double* dp)
{
   int status = 0;
   char data[2*XDRUNIT];
   /* Pull two units */
   status = xxdr_opaque(xdr,data,(off_t)2*XDRUNIT);
   if(status && dp) {
	xxdrntohdouble(data,dp);
   }
   return status;
}

/* Double needs special handling */
void
xxdrntohdouble(char* c8, double* dp)
{
    unsigned int ii[2];
    memcpy(ii,c8,(size_t)2*XDRUNIT);
    if(!xxdr_big_endian) {
	unsigned int tmp;
	/* reverse byte order */
	swapinline32(&ii[0]);
	swapinline32(&ii[1]);
	/* interchange ii[0] and ii[1] */
	tmp = ii[0];
	ii[0] = ii[1];
	ii[1] = tmp;
    }
    /* use memcpy avoid type punning */
    if(dp) memcpy(dp, ii, sizeof(double));
}

void
xxdr_init()
{
    /* Compute if we are same as network order v-a-v xdr */
    int testint = 0x00000001;
    char *byte = (char *)&testint;
    xxdr_big_endian = (byte[0] == 0 ? 1 : 0);
    xxdr_network_order = xxdr_big_endian;
#ifdef ENDIAN_VALIDATE
    /* validate using ntohl */
    if(ntohl(testint) == testint) {
	if(!xxdr_network_order) {
	    fprintf(stderr,"xxdr_init: endian mismatch\n");
	    fflush(stderr);
	    exit(1);	
	}
    }
#endif

}
