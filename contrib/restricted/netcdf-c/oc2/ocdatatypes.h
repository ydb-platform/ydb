/* Copyright 2018, UCAR/Unidata and OPeNDAP, Inc.
   See the COPYRIGHT file for more information. */

#ifndef OCDATATYPES_H
#define OCDATATYPES_H

/* Define some useful info about the supported
   primitive datatypes*/

#define DCHAR char
#define DBYTE signed char
#define DUBYTE unsigned char
#define DINT16 short
#define DUINT16 unsigned short
#define DINT32 int
#define DUINT32 unsigned int
#define DINT64 int
#define DUINT64 unsigned int
#define DFLOAT32 float
#define DFLOAT64 double

#define	OC_CHAR_MIN	((char)0x00)
#define	OC_CHAR_MAX	((char)0xff)
#define	OC_BYTE_MIN	-128
#define	OC_BYTE_MAX	127
#define	OC_UBYTE_MIN	0
#define	OC_UBYTE_MAX	255U
#define	OC_INT16_MIN	-32768
#define	OC_INT16_MAX	32767
#define	OC_UINT16_MIN	0
#define	OC_UINT16_MAX	65535U
#define	OC_INT32_MIN	(-2147483647 - 1)
#define	OC_INT32_MAX	2147483647
#define	OC_UINT32_MIN	0
#define	OC_UINT32_MAX	4294967295U
#define OC_INT64_MIN    (-9223372036854775807LL-1)
#define OC_INT64_MAX    (9223372036854775807LL)
#define OC_UINT64_MIN   0LL
#define OC_UINT64_MAX   (18446744073709551615ULL)
#define	OC_FLOAT32_MAX	3.402823466E+38F	/* max decimal value of a "float" */
#define	OC_FLOAT32_MIN	(-OC_FLOAT_MAX)
#define	OC_FLOAT64_MAX	1.7976931348623157E+308	/* max decimal value of a double */
#define	OC_FLOAT64_MIN	(-OC_FLOAT64_MAX)

/* Similar to netcdf*/
#define OC_FILL_CHAR	((char)0)
#define OC_FILL_BYTE	((signed char)-127)
#define OC_FILL_UBYTE   (255)
#define OC_FILL_INT16	((short)-32767)
#define OC_FILL_UINT16  (65535)
#define OC_FILL_INT32	(-2147483647L)
#define OC_FILL_UINT32    (4294967295U)
#define OC_FILL_INT64   ((long long)-9223372036854775806LL)
#define OC_FILL_UINT64  ((unsigned long long)18446744073709551614ULL)
#define OC_FILL_FLOAT32	(9.9692099683868690e+36f) /* near 15 * 2^119 */
#define OC_FILL_FLOAT64	(9.9692099683868690e+36)
#define OC_FILL_STRING  ""
#define OC_FILL_URL  ""


#endif /*OCDATATYPES_H*/
