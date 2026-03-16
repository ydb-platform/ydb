/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *   $Header: /upc/share/CVS/netcdf-3/ncgen/offsets.c,v 1.1 2009/09/25 18:22:40 dmh Exp $
 *********************************************************************/

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
This code is a variation of the H5detect.c code from HDF5.
Author: D. Heimbigner 10/7/2008
*/

#include "config.h"
#include        <stdlib.h>
#include        <stdio.h>
#include        <string.h>
#include        <assert.h>
#include        "nclog.h"

#ifdef OFFSETTEST

static void* emalloc(size_t);

typedef int nc_type;
typedef struct nc_vlen_t {
    size_t len;
    void* p;
} nc_vlen_t;

#define	NC_NAT 	        0	/* NAT = 'Not A Type' (c.f. NaN) */
#define	NC_BYTE         1	/* signed 1 byte integer */
#define	NC_CHAR 	2	/* ISO/ASCII character */
#define	NC_SHORT 	3	/* signed 2 byte integer */
#define	NC_INT 	        4	/* signed 4 byte integer */
#define	NC_FLOAT 	5	/* single precision floating point number */
#define	NC_DOUBLE 	6	/* double precision floating point number */
#define	NC_UBYTE 	7	/* unsigned 1 byte int */
#define	NC_USHORT 	8	/* unsigned 2-byte int */
#define	NC_UINT 	9	/* unsigned 4-byte int */
#define	NC_INT64 	10	/* signed 8-byte int */
#define	NC_UINT64 	11	/* unsigned 8-byte int */
#define	NC_STRING 	12	/* string */
#define	NC_STRING 	12	/* string */
#define	NC_VLEN 	13	
#define	NC_OPAQUE 	14	
#define	NC_ENUM 	15	
#define	NC_COMPOUND 	16	
#endif

#include        "netcdf.h"
#include        "ncoffsets.h"


/*
The heart of this is the following macro,
which computes the offset of a field x
when preceded by a char field.
The assumptions appear to be as follows:
1. the offset produced in this situation indicates
   the alignment for x relative in such a way that it
   depends only on the types that precede it in the struct.
2. the compiler does not reorder fields.
3. arrays are tightly packed.
4. nested structs are aligned according to their first member
   (this actually follows from C language requirement that
    a struct can legally be cast to an instance of its first member).
Given the alignments for the various common primitive types,
it is assumed that one can use them anywhere to construct
the layout of a struct of such types.
It seems to work for HDF5 for a wide variety of machines.
Note that technically, this is compiler dependent, but in practice
all compilers seem to mimic the gcc rules.
*/

#define COMP_ALIGNMENT(DST,TYPE)  {\
    struct {char f1; TYPE x;} tmp; \
    DST.type_name = #TYPE ;        \
    DST.alignment = (size_t)((char*)(&(tmp.x)) - (char*)(&tmp));}

#if 0
char* ctypenames[NCTYPES] = {
(char*)NULL,
"char","unsigned char",
"short","unsigned short",
"int","unsigned int",
"long long","unsigned long long",
"float","double",
"void*","nc_vlen_t"
};
#endif

static NCtypealignvec vec[NC_NCTYPES];
static NCtypealignset set;
static int NC_alignments_computed = 0;

/* Argument is a netcdf type class, except compound|ENUM  */
int
NC_class_alignment(int ncclass, size_t* alignp)
{
    int stat = NC_NOERR;
    NCalignment* align = NULL;
    int index = 0;
    if(!NC_alignments_computed)
	NC_compute_alignments();
    switch (ncclass) {
      case NC_BYTE: index = NC_UCHARINDEX; break;
      case NC_CHAR: index = NC_CHARINDEX; break;
      case NC_SHORT: index = NC_SHORTINDEX; break;
      case NC_INT: index = NC_INTINDEX; break;
      case NC_FLOAT: index = NC_FLOATINDEX; break;
      case NC_DOUBLE: index = NC_DOUBLEINDEX; break;
      case NC_UBYTE: index = NC_UCHARINDEX; break;
      case NC_USHORT: index = NC_USHORTINDEX; break;
      case NC_UINT: index = NC_UINTINDEX; break;
      case NC_INT64: index = NC_LONGLONGINDEX; break;
      case NC_UINT64: index = NC_ULONGLONGINDEX; break;
      case NC_STRING: index = NC_PTRINDEX; break;
      /* Here class matters */
      case NC_VLEN: index = NC_NCVLENINDEX; break;
      case NC_OPAQUE: index = NC_UCHARINDEX; break;
      case NC_ENUM: /* fall thru */
      case NC_COMPOUND: /* fall thru */
      default:
	nclog(NCLOGERR,"nc_class_alignment: class code %d cannot be aligned",ncclass);
	goto done;
    }
    align = &vec[index];
    if(alignp) *alignp = align->alignment;
done:
    return stat;
}

void
NC_compute_alignments(void)
{
    if(NC_alignments_computed) return;
    /* Compute the alignments for all the common C data types*/
    /* First for the struct*/
    /* initialize*/
    memset((void*)&set,0,sizeof(set));
    memset((void*)vec,0,sizeof(vec));

    COMP_ALIGNMENT(set.charalign,char);
    COMP_ALIGNMENT(set.ucharalign,unsigned char);
    COMP_ALIGNMENT(set.shortalign,short);
    COMP_ALIGNMENT(set.ushortalign,unsigned short);
    COMP_ALIGNMENT(set.intalign,int);
    COMP_ALIGNMENT(set.uintalign,unsigned int);
    COMP_ALIGNMENT(set.longlongalign,long long);
    COMP_ALIGNMENT(set.ulonglongalign,unsigned long long);
    COMP_ALIGNMENT(set.floatalign,float);
    COMP_ALIGNMENT(set.doublealign,double);
    COMP_ALIGNMENT(set.ptralign,void*);
    COMP_ALIGNMENT(set.ncvlenalign,nc_vlen_t);

    /* Then the vector*/
    COMP_ALIGNMENT(vec[NC_CHARINDEX],char);
    COMP_ALIGNMENT(vec[NC_UCHARINDEX],unsigned char); 
    COMP_ALIGNMENT(vec[NC_SHORTINDEX],short);
    COMP_ALIGNMENT(vec[NC_USHORTINDEX],unsigned short);
    COMP_ALIGNMENT(vec[NC_INTINDEX],int);
    COMP_ALIGNMENT(vec[NC_UINTINDEX],unsigned int);
    COMP_ALIGNMENT(vec[NC_LONGLONGINDEX],long long);
    COMP_ALIGNMENT(vec[NC_ULONGLONGINDEX],unsigned long long);
    COMP_ALIGNMENT(vec[NC_FLOATINDEX],float);
    COMP_ALIGNMENT(vec[NC_DOUBLEINDEX],double);
    COMP_ALIGNMENT(vec[NC_PTRINDEX],void*);
    COMP_ALIGNMENT(vec[NC_NCVLENINDEX],nc_vlen_t);
    NC_alignments_computed = 1;
}

#ifdef OFFSETTEST

/* Compute the alignment of TYPE when it is preceded
   by a field of type TYPE1
*/
#define COMP_ALIGNMENT1(DST,TYPE1,TYPE)  {\
    struct {TYPE1 f1; TYPE x;} tmp; \
    DST.type_name = #TYPE ;        \
    DST.alignment = (size_t)((char*)(&(tmp.x)) - (char*)(&tmp));}

/* Compute the alignment of TYPE when it is preceded
   by a field of type TYPE1 and a field of type TYPE2
*/
#define COMP_ALIGNMENT2(DST,TYPE1,TYPE2,TYPE)  {\
    struct {TYPE1 f1, TYPE2 f2; TYPE x;} tmp;   \
    DST.type_name = #TYPE ;                      \
    DST.alignment = (size_t)((char*)(&(tmp.x)) - (char*)(&tmp));}

/* Compute the alignment of TYPE when it is preceded
   by a field of type TYPE1 and a field of type TYPE2
*/
#define COMP_SIZE0(DST,TYPE1,TYPE2)  {\
    struct {TYPE1 c; TYPE2 x;} tmp; \
    DST = sizeof(tmp); }

static char*
padname(char* name)
{
#define MAX 20
    if(name == NULL) name = "null";
    int len = strlen(name);
    if(len > MAX) len = MAX;
    char* s = (char*)emalloc(MAX+1);
    memset(s,' ',MAX);
    s[MAX+1] = '\0';
    strncpy(s,name,len);
    return s;
}

static void
verify(NCtypealignvec* vec)
{
    int i,j;
    NCtypealignvec* vec16;
    NCtypealignvec* vec32;
    int* sizes8;
    int* sizes16;
    int* sizes32;

    vec16 = (NCtypealignvec*)emalloc(sizeof(NCtypealignvec)*NCTYPES);
    vec32 = (NCtypealignvec*)emalloc(sizeof(NCtypealignvec)*NCTYPES);
    sizes8 = (int*)emalloc(sizeof(int)*NCTYPES);
    sizes16 = (int*)emalloc(sizeof(int)*NCTYPES);
    sizes32 = (int*)emalloc(sizeof(int)*NCTYPES);

    COMP_SIZE0(sizes8[1],char,char);
    COMP_SIZE0(sizes8[2],unsigned char,char);
    COMP_SIZE0(sizes8[3],short,char);
    COMP_SIZE0(sizes8[4],unsigned short,char);
    COMP_SIZE0(sizes8[5],int,char);
    COMP_SIZE0(sizes8[6],unsigned int,char);
    COMP_SIZE0(sizes8[7],long long,char);
    COMP_SIZE0(sizes8[8],unsigned long long,char);
    COMP_SIZE0(sizes8[9],float,char);
    COMP_SIZE0(sizes8[10],double,char) ;
    COMP_SIZE0(sizes8[11],void*,char);
    COMP_SIZE0(sizes8[12],nc_vlen_t,char);

    COMP_SIZE0(sizes16[1],char,short);
    COMP_SIZE0(sizes16[2],unsigned char,short);
    COMP_SIZE0(sizes16[3],short,short);
    COMP_SIZE0(sizes16[4],unsigned short,short);
    COMP_SIZE0(sizes16[5],int,short);
    COMP_SIZE0(sizes16[6],unsigned int,short);
    COMP_SIZE0(sizes16[7],long long,short);
    COMP_SIZE0(sizes16[8],unsigned long long,short);
    COMP_SIZE0(sizes16[9],float,short);
    COMP_SIZE0(sizes16[10],double,short) ;
    COMP_SIZE0(sizes16[11],void*,short);
    COMP_SIZE0(sizes16[12],nc_vlen_t*,short);

    COMP_SIZE0(sizes32[1],char,int);
    COMP_SIZE0(sizes32[2],unsigned char,int);
    COMP_SIZE0(sizes32[3],short,int);
    COMP_SIZE0(sizes32[4],unsigned short,int);
    COMP_SIZE0(sizes32[5],int,int);
    COMP_SIZE0(sizes32[6],unsigned int,int);
    COMP_SIZE0(sizes32[7],long long,int);
    COMP_SIZE0(sizes32[8],unsigned long long,int);
    COMP_SIZE0(sizes32[9],float,int);
    COMP_SIZE0(sizes32[10],double,int) ;
    COMP_SIZE0(sizes32[11],void*,int);
    COMP_SIZE0(sizes32[12],nc_vlen_t*,int);

    COMP_ALIGNMENT1(vec16[1],char,short);
    COMP_ALIGNMENT1(vec16[2],unsigned char,short);
    COMP_ALIGNMENT1(vec16[3],short,short);
    COMP_ALIGNMENT1(vec16[4],unsigned short,short);
    COMP_ALIGNMENT1(vec16[5],int,short);
    COMP_ALIGNMENT1(vec16[6],unsigned int,short);
    COMP_ALIGNMENT1(vec32[7],long long,short);
    COMP_ALIGNMENT1(vec32[8],unsigned long long,short);
    COMP_ALIGNMENT1(vec16[9],float,short);
    COMP_ALIGNMENT1(vec16[10],double,short);
    COMP_ALIGNMENT1(vec16[11],void*,short);
    COMP_ALIGNMENT1(vec16[12],nc_vlen_t*,short);

    COMP_ALIGNMENT1(vec32[1],char,short);
    COMP_ALIGNMENT1(vec32[2],unsigned char,short);
    COMP_ALIGNMENT1(vec32[3],char,short);
    COMP_ALIGNMENT1(vec32[4],unsigned short,short);
    COMP_ALIGNMENT1(vec32[5],int,int);
    COMP_ALIGNMENT1(vec32[6],unsigned int,int);
    COMP_ALIGNMENT1(vec32[7],long long,int);
    COMP_ALIGNMENT1(vec32[8],unsigned long long,int);
    COMP_ALIGNMENT1(vec32[9],float,int);
    COMP_ALIGNMENT1(vec32[10],double,int);
    COMP_ALIGNMENT1(vec32[11],void*,int);
    COMP_ALIGNMENT1(vec32[12],nc_vlen_t*,int);

    for(i=0;i<NCTYPES;i++) {
	printf("%s: size=%2d  alignment=%2d\n",
		padname(vec[i].type_name),sizes8[i],vec[i].alignment);
    }
    for(i=0;i<NCTYPES;i++) {
	printf("short vs %s: size=%2d  alignment=%2d\n",
		padname(vec[i].type_name),sizes16[i],vec16[i].alignment);
    }
    for(i=0;i<NCTYPES;i++) {
	printf("int vs %s: size=%2d  alignment=%2d\n",
		padname(vec[i].type_name),sizes32[i],vec32[i].alignment);
    }

}

void *
emalloc(size_t bytes) {
    size_t *memory;
    memory = malloc(bytes);
    if(memory == 0) {
	printf("malloc failed\n");
	exit(2);
    }
    return memory;
}

int
main(int argc, char** argv)
{
    int i;

    compute_alignments();

    verify(vec);

/*
    for(i=0;i<NCTYPES;i++) {
	printf("%s:\talignment=%d\n",vec[i].type_name,vec[i].alignment);
    }
*/
    exit(0);
}
#endif /*OFFSETTEST*/
