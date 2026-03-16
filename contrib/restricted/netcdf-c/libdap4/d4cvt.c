/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#include "config.h"
#include <assert.h>
#include "netcdf.h"

#ifdef _MSC_VER
#include <crtdbg.h>
#endif

/*
Code taken directly from libdap4/dapcvt.c
*/

/* Forward */
static size_t nctypesizeof(nc_type nctype);


int
NCD4_convert(nc_type srctype, nc_type dsttype, char* memory0, char* value0, size_t count)
{
    int ncstat = NC_NOERR;
    size_t i;
    char* memory = memory0;
    char* value = value0;

    /* In order to deal with the DAP upgrade problem,
	try to preserve the bit patterns
    */

    /* Provide space and pointer casts for intermediate results */
    signed char ncbyte;
    signed char* ncbytep;
    char ncchar;
    char* nccharp;
    short ncshort;
    short* ncshortp;
    int ncint;
    int* ncintp;
    float ncfloat;
    float* ncfloatp;
    double ncdouble;
    double* ncdoublep;
    unsigned char ncubyte;
    unsigned char* ncubytep;
    unsigned short ncushort;
    unsigned short* ncushortp;
    unsigned int ncuint;
    unsigned int* ncuintp;
    long long ncint64;
    long long* ncint64p;
    unsigned long long ncuint64;
    unsigned long long* ncuint64p;


#define CASE(nc1,nc2) (nc1*256+nc2)
#define CUT8(e) ((unsigned char)((e) & 0xff))
#define CUT16(e) ((unsigned short)((e) & 0xffff))
#define CUT32(e) ((unsigned int)((e) & 0xffffffff))
#define ARM(vs,ncs,ts,vd,ncd,td) \
case CASE(ncs,ncd):\
    vs##p = (ts *)value;\
    vs = *vs##p;\
    vd##p = (td *)memory;\
    *vd##p = (td)vs;\
    break;

    for(i=0;i<count;i++) {

        switch (CASE(srctype,dsttype)) {
ARM(ncchar,NC_CHAR,char,ncchar,NC_CHAR,char)
ARM(ncchar,NC_CHAR,char,ncbyte,NC_BYTE,signed char)
ARM(ncchar,NC_CHAR,char,ncubyte,NC_UBYTE,unsigned char)
ARM(ncchar,NC_CHAR,char,ncshort,NC_SHORT,short)
ARM(ncchar,NC_CHAR,char,ncushort,NC_USHORT,unsigned short)
ARM(ncchar,NC_CHAR,char,ncint,NC_INT,int)
ARM(ncchar,NC_CHAR,char,ncuint,NC_UINT,unsigned int)
ARM(ncchar,NC_CHAR,char,ncint64,NC_INT64,long long)
ARM(ncchar,NC_CHAR,char,ncuint64,NC_UINT64,unsigned long long)
ARM(ncchar,NC_CHAR,char,ncfloat,NC_FLOAT,float)
ARM(ncchar,NC_CHAR,char,ncdouble,NC_DOUBLE,double)
ARM(ncbyte,NC_BYTE,signed char,ncchar,NC_CHAR,char)
ARM(ncbyte,NC_BYTE,signed char,ncbyte,NC_BYTE,signed char)
ARM(ncbyte,NC_BYTE,signed char,ncubyte,NC_UBYTE,unsigned char)
ARM(ncbyte,NC_BYTE,signed char,ncshort,NC_SHORT,short)
ARM(ncbyte,NC_BYTE,signed char,ncushort,NC_USHORT,unsigned short)
ARM(ncbyte,NC_BYTE,signed char,ncint,NC_INT,int)
ARM(ncbyte,NC_BYTE,signed char,ncuint,NC_UINT,unsigned int)
ARM(ncbyte,NC_BYTE,signed char,ncint64,NC_INT64,long long)
ARM(ncbyte,NC_BYTE,signed char,ncuint64,NC_UINT64,unsigned long long)
ARM(ncbyte,NC_BYTE,signed char,ncfloat,NC_FLOAT,float)
ARM(ncbyte,NC_BYTE,signed char,ncdouble,NC_DOUBLE,double)
ARM(ncubyte,NC_UBYTE,unsigned char,ncchar,NC_CHAR,char)
ARM(ncubyte,NC_UBYTE,unsigned char,ncbyte,NC_BYTE,signed char)
ARM(ncubyte,NC_UBYTE,unsigned char,ncubyte,NC_UBYTE,unsigned char)
ARM(ncubyte,NC_UBYTE,unsigned char,ncshort,NC_SHORT,short)
ARM(ncubyte,NC_UBYTE,unsigned char,ncushort,NC_USHORT,unsigned short)
ARM(ncubyte,NC_UBYTE,unsigned char,ncint,NC_INT,int)
ARM(ncubyte,NC_UBYTE,unsigned char,ncuint,NC_UINT,unsigned int)
ARM(ncubyte,NC_UBYTE,unsigned char,ncint64,NC_INT64,long long)
ARM(ncubyte,NC_UBYTE,unsigned char,ncuint64,NC_UINT64,unsigned long long)
ARM(ncubyte,NC_UBYTE,unsigned char,ncfloat,NC_FLOAT,float)
ARM(ncubyte,NC_UBYTE,unsigned char,ncdouble,NC_DOUBLE,double)
ARM(ncshort,NC_SHORT,short,ncchar,NC_CHAR,char)
ARM(ncshort,NC_SHORT,short,ncbyte,NC_BYTE,signed char)
ARM(ncshort,NC_SHORT,short,ncubyte,NC_UBYTE,unsigned char)
ARM(ncshort,NC_SHORT,short,ncshort,NC_SHORT,short)
ARM(ncshort,NC_SHORT,short,ncushort,NC_USHORT,unsigned short)
ARM(ncshort,NC_SHORT,short,ncint,NC_INT,int)
ARM(ncshort,NC_SHORT,short,ncuint,NC_UINT,unsigned int)
ARM(ncshort,NC_SHORT,short,ncint64,NC_INT64,long long)
ARM(ncshort,NC_SHORT,short,ncuint64,NC_UINT64,unsigned long long)
ARM(ncshort,NC_SHORT,short,ncfloat,NC_FLOAT,float)
ARM(ncshort,NC_SHORT,short,ncdouble,NC_DOUBLE,double)
ARM(ncushort,NC_USHORT,unsigned short,ncchar,NC_CHAR,char)
ARM(ncushort,NC_USHORT,unsigned short,ncbyte,NC_BYTE,signed char)
ARM(ncushort,NC_USHORT,unsigned short,ncubyte,NC_UBYTE,unsigned char)
ARM(ncushort,NC_USHORT,unsigned short,ncshort,NC_SHORT,short)
ARM(ncushort,NC_USHORT,unsigned short,ncushort,NC_USHORT,unsigned short)
ARM(ncushort,NC_USHORT,unsigned short,ncint,NC_INT,int)
ARM(ncushort,NC_USHORT,unsigned short,ncuint,NC_UINT,unsigned int)
ARM(ncushort,NC_USHORT,unsigned short,ncint64,NC_INT64,long long)
ARM(ncushort,NC_USHORT,unsigned short,ncuint64,NC_UINT64,unsigned long long)
ARM(ncushort,NC_USHORT,unsigned short,ncfloat,NC_FLOAT,float)
ARM(ncushort,NC_USHORT,unsigned short,ncdouble,NC_DOUBLE,double)
ARM(ncint,NC_INT,int,ncchar,NC_CHAR,char)
ARM(ncint,NC_INT,int,ncbyte,NC_BYTE,signed char)
ARM(ncint,NC_INT,int,ncubyte,NC_UBYTE,unsigned char)
ARM(ncint,NC_INT,int,ncshort,NC_SHORT,short)
ARM(ncint,NC_INT,int,ncushort,NC_USHORT,unsigned short)
ARM(ncint,NC_INT,int,ncint,NC_INT,int)
ARM(ncint,NC_INT,int,ncuint,NC_UINT,unsigned int)
ARM(ncint,NC_INT,int,ncint64,NC_INT64,long long)
ARM(ncint,NC_INT,int,ncuint64,NC_UINT64,unsigned long long)
ARM(ncint,NC_INT,int,ncfloat,NC_FLOAT,float)
ARM(ncint,NC_INT,int,ncdouble,NC_DOUBLE,double)
ARM(ncuint,NC_UINT,unsigned int,ncchar,NC_CHAR,char)
ARM(ncuint,NC_UINT,unsigned int,ncbyte,NC_BYTE,signed char)
ARM(ncuint,NC_UINT,unsigned int,ncubyte,NC_UBYTE,unsigned char)
ARM(ncuint,NC_UINT,unsigned int,ncshort,NC_SHORT,short)
ARM(ncuint,NC_UINT,unsigned int,ncushort,NC_USHORT,unsigned short)
ARM(ncuint,NC_UINT,unsigned int,ncint,NC_INT,int)
ARM(ncuint,NC_UINT,unsigned int,ncuint,NC_UINT,unsigned int)
ARM(ncuint,NC_UINT,unsigned int,ncint64,NC_INT64,long long)
ARM(ncuint,NC_UINT,unsigned int,ncuint64,NC_UINT64,unsigned long long)
ARM(ncuint,NC_UINT,unsigned int,ncfloat,NC_FLOAT,float)
ARM(ncuint,NC_UINT,unsigned int,ncdouble,NC_DOUBLE,double)
ARM(ncint64,NC_INT64,long long,ncchar,NC_CHAR,char)
ARM(ncint64,NC_INT64,long long,ncbyte,NC_BYTE,signed char)
ARM(ncint64,NC_INT64,long long,ncubyte,NC_UBYTE,unsigned char)
ARM(ncint64,NC_INT64,long long,ncshort,NC_SHORT,short)
ARM(ncint64,NC_INT64,long long,ncushort,NC_USHORT,unsigned short)
ARM(ncint64,NC_INT64,long long,ncint,NC_INT,int)
ARM(ncint64,NC_INT64,long long,ncuint,NC_UINT,unsigned int)
ARM(ncint64,NC_INT64,long long,ncint64,NC_INT64,long long)
ARM(ncint64,NC_INT64,long long,ncuint64,NC_UINT64,unsigned long long)
ARM(ncint64,NC_INT64,long long,ncfloat,NC_FLOAT,float)
ARM(ncint64,NC_INT64,long long,ncdouble,NC_DOUBLE,double)
ARM(ncuint64,NC_UINT64,unsigned long long,ncchar,NC_CHAR,char)
ARM(ncuint64,NC_UINT64,unsigned long long,ncbyte,NC_BYTE,signed char)
ARM(ncuint64,NC_UINT64,unsigned long long,ncubyte,NC_UBYTE,unsigned char)
ARM(ncuint64,NC_UINT64,unsigned long long,ncshort,NC_SHORT,short)
ARM(ncuint64,NC_UINT64,unsigned long long,ncushort,NC_USHORT,unsigned short)
ARM(ncuint64,NC_UINT64,unsigned long long,ncint,NC_INT,int)
ARM(ncuint64,NC_UINT64,unsigned long long,ncuint,NC_UINT,unsigned int)
ARM(ncuint64,NC_UINT64,unsigned long long,ncint64,NC_INT64,long long)
ARM(ncuint64,NC_UINT64,unsigned long long,ncuint64,NC_UINT64,unsigned long long)
ARM(ncuint64,NC_UINT64,unsigned long long,ncfloat,NC_FLOAT,float)
ARM(ncuint64,NC_UINT64,unsigned long long,ncdouble,NC_DOUBLE,double)
ARM(ncfloat,NC_FLOAT,float,ncchar,NC_CHAR,char)
ARM(ncfloat,NC_FLOAT,float,ncbyte,NC_BYTE,signed char)
ARM(ncfloat,NC_FLOAT,float,ncubyte,NC_UBYTE,unsigned char)
ARM(ncfloat,NC_FLOAT,float,ncshort,NC_SHORT,short)
ARM(ncfloat,NC_FLOAT,float,ncushort,NC_USHORT,unsigned short)
ARM(ncfloat,NC_FLOAT,float,ncint,NC_INT,int)
ARM(ncfloat,NC_FLOAT,float,ncuint,NC_UINT,unsigned int)
ARM(ncfloat,NC_FLOAT,float,ncint64,NC_INT64,long long)
ARM(ncfloat,NC_FLOAT,float,ncuint64,NC_UINT64,unsigned long long)
ARM(ncfloat,NC_FLOAT,float,ncfloat,NC_FLOAT,float)
ARM(ncfloat,NC_FLOAT,float,ncdouble,NC_DOUBLE,double)
ARM(ncdouble,NC_DOUBLE,double,ncchar,NC_CHAR,char)
ARM(ncdouble,NC_DOUBLE,double,ncbyte,NC_BYTE,signed char)
ARM(ncdouble,NC_DOUBLE,double,ncubyte,NC_UBYTE,unsigned char)
ARM(ncdouble,NC_DOUBLE,double,ncshort,NC_SHORT,short)
ARM(ncdouble,NC_DOUBLE,double,ncushort,NC_USHORT,unsigned short)
ARM(ncdouble,NC_DOUBLE,double,ncint,NC_INT,int)
ARM(ncdouble,NC_DOUBLE,double,ncuint,NC_UINT,unsigned int)
ARM(ncdouble,NC_DOUBLE,double,ncint64,NC_INT64,long long)
ARM(ncdouble,NC_DOUBLE,double,ncuint64,NC_UINT64,unsigned long long)
ARM(ncdouble,NC_DOUBLE,double,ncfloat,NC_FLOAT,float)
ARM(ncdouble,NC_DOUBLE,double,ncdouble,NC_DOUBLE,double)
	
        default: ncstat = NC_EINVAL; goto fail;
        }
        value += nctypesizeof(srctype);
        memory += nctypesizeof(dsttype);
    }

fail:
    return (ncstat);
}

static size_t
nctypesizeof(nc_type nctype)
{
    switch (nctype) {
    case NC_CHAR:	return sizeof(char);
    case NC_BYTE:	return sizeof(signed char);
    case NC_UBYTE:	return sizeof(unsigned char);
    case NC_SHORT:	return sizeof(short);
    case NC_USHORT:	return sizeof(unsigned short);
    case NC_INT:	return sizeof(int);
    case NC_UINT:	return sizeof(unsigned int);
    case NC_INT64:	return sizeof(long long);
    case NC_UINT64:	return sizeof(unsigned long long);
    case NC_FLOAT:	return sizeof(float);
    case NC_DOUBLE:	return sizeof(double);
    case NC_STRING:	return sizeof(char*);
    default: assert(0);
    }
    return 0;
}
