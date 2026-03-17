/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#ifndef OPAL_ARCH_H_HAS_BEEN_INCLUDED
#define OPAL_ARCH_H_HAS_BEEN_INCLUDED

#include "opal_config.h"

#include <float.h>
#include <assert.h>


/***************************************************
** This file tries to classify the most relevant
** platforms regarding their data representation.
** Three aspects are important:
** - byte ordering (little or big endian)
** - integer representation
** - floating point representation.

** In addition, don't forget about the C/Fortran problems.
**
*****************************************************/


/*****************************************************************
** Part 1: Integer representation.
**
** The following data types are considered relevant:
**
** short
** int
** long
** long long
** integer (fortran)
**
** The fortran integer is dismissed here, since there is no
** platform known to me, were fortran and C-integer do not match
**
** The following abbriviations are introduced:
**
** a) il32 (int long are 32 bits) (e.g. IA32 LINUX, SGI n32, SUN)
**
**    short:     16 (else it would appear in the name)
**    int:       32
**    long:      32
**    long long: 64
**
** b) il64 ( int long are 64 bits) (e.g. Cray T3E )
**    short:     32
**    int:       64
**    long:      64
**    long long: 64
**
** c) l64 (long are 64 bits) (e.g. SGI 64 IRIX, NEC SX5)
**
**     short:     16
**     int:       32
**     long:      64
**     long long: 64
**
***********************************************************************/

/*********************************************************************
**  Part 2: Floating point representation
**
**  The following datatypes are considered relevant
**
**   float
**   double
**   long double
**   real
**   double precision
**
**   Unfortunatly, here we have to take care, whether float and real,
**   respectively double and double precision do match...
**
**  a) fr32 (float and real are 32 bits) (e.g. SGI n32 and 64, SUN, NEC SX5,...)
**     float:       32
**     double:      64
**     long double: 128
**     real:        32
**     double prec.:64
**
**  a1) fr32ld96 (float and real 32, long double 96) (e.g. IA32 LINUX gcc/icc)
**     see a), except long double is 96
**
**  a2) fr32ld64  (e.g. IBM )
**     see a), except long double is 64
**
**  b) cray ( e.g. Cray T3E)
**     float:       32
**     double:      64
**     long double: 64
**     real:        64
**     double prec.:64
**
**
**  Problem: long double is really treated differently on every machine. Therefore,
**  we are storing besides the length of the long double also the length of the mantisee,
**  and the number of *relevant* bits in the exponent. Here are the values:
**
**  Architecture   sizeof(long double) mantisee  relevant bits for exp.
**
**  SGIn32/64:     128                 107       10
**  SUN(sparc):    128                 113       14
**  IA64:          128                 64        14
**  IA32:          96                  64        14
**  Alpha:         128                 113       14
**                 64                  53        10 (gcc)
**  IBM:           64                  53        10
**                (128                 106       10) (special flags required).
**  SX5:           128                 105       22
**
** We will not implement all of these routiens, but we consider them
** now when defining the header-settings
**
***********************************************************************/

/********************************************************************
**
** Classification of machines:
**
** IA32 LINUX: il32, fr32ld96, little endian
** SUN:        il32, fr32,     big endian
** SGI n32:    il32, fr32,     big endian
** SGI 64:     l64,  fr32,     big endian
** NEC SX5:    l64,  fr32      big endian
** Cray T3E:   il64, cray,     big endian
** Cray X1:    i32(+), fr32,   big endian
** IBM:        il32, fr32ld64, big endian
** ALPHA:      l64,  fr32,     little endian
** ITANIUM:    l64,  fr32,     little endian
**
**
** + sizeof ( long long ) not known
** ? alpha supports both, big and little endian
***********************************************************************/


/* Current conclusions:
** we need at the moment three settings:
** - big/little endian ?
** - is long 32 or 64 bits ?
** - is long double 64, 96 or 128 bits ?
** - no. of rel. bits in the exponent of a long double ( 10 or 14 )
** - no. of bits of the mantiss of a long double ( 53, 64, 105, 106, 107, 113 )
**
** To store this in a 32 bit integer, we use the following definition:
**
**     1        2        3        4
** 12345678 12345678 12345678 12345678
**
** 1. Byte:
**   bits 1 & 2: 00 (header) (to recognize the correct end)
**   bits 3 & 4: encoding: 00 = little, 01 = big
**   bits 5 & 6: reserved for later use. currently set to 00
**   bits 7 & 8: reserved for later use. currently set to 00
** 2. Byte:
**   bits 1 & 2: length of long: 00 = 32, 01 = 64
**   bits 3 & 4: lenght of long long (not used currently, set to 00).
**   bits 5 & 6: length of C/C++ bool (00 = 8, 01 = 16, 10 = 32)
**   bits 7 & 8: length of Fortran Logical (00 = 8, 01 = 16, 10 = 32)
** 3. Byte:
**   bits 1 & 2: length of long double: 00=64, 01=96,10 = 128
**   bits 3 & 4: no. of rel. bits in the exponent: 00 = 10, 01 = 14)
**   bits 5 - 7: no. of bits of mantisse ( 000 = 53,  001 = 64, 010 = 105,
**                                         011 = 106, 100 = 107,101 = 113 )
**   bit      8: intel or sparc representation of mantisse (0 = sparc,
**                                         1 = intel )
** 4. Byte:
**   bits 1 & 2: 11 (header) (to recognize the correct end)
**   bits 3 & 4: reserved for later use. currently set to 11
**   bits 5 & 6: reserved for later use. currently set to 11
**   bits 7 & 8: reserved for later use. currently set to 11
*/

/* These masks implement the specification above above */

#define OPAL_ARCH_HEADERMASK      0x03000000 /* set the fields for the header */
#define OPAL_ARCH_HEADERMASK2     0x00000003 /* other end, needed for checks */
#define OPAL_ARCH_UNUSEDMASK      0xfc000000 /* mark the unused fields */

/* BYTE 1 */
#define OPAL_ARCH_ISBIGENDIAN     0x00000008

/* BYTE 2 */
#define OPAL_ARCH_LONGISxx        0x0000c000  /* mask for sizeof long */
#define OPAL_ARCH_LONGIS64        0x00001000
#define OPAL_ARCH_LONGLONGISxx    0x00003000  /* mask for sizeof long long */

#define OPAL_ARCH_BOOLISxx        0x00000c00  /* mask for sizeof bool */
#define OPAL_ARCH_BOOLIS8         0x00000000  /* bool is 8 bits */
#define OPAL_ARCH_BOOLIS16        0x00000400  /* bool is 16 bits */
#define OPAL_ARCH_BOOLIS32        0x00000800  /* bool is 32 bits */

#define OPAL_ARCH_LOGICALISxx     0x00000300  /* mask for sizeof Fortran logical */
#define OPAL_ARCH_LOGICALIS8      0x00000000  /* logical is 8 bits */
#define OPAL_ARCH_LOGICALIS16     0x00000100  /* logical is 16 bits */
#define OPAL_ARCH_LOGICALIS32     0x00000200  /* logical is 32 bits */

/* BYTE 3 */
#define OPAL_ARCH_LONGDOUBLEIS96  0x00020000
#define OPAL_ARCH_LONGDOUBLEIS128 0x00010000

#define OPAL_ARCH_LDEXPSIZEIS15   0x00080000

#define OPAL_ARCH_LDMANTDIGIS64   0x00400000
#define OPAL_ARCH_LDMANTDIGIS105  0x00200000
#define OPAL_ARCH_LDMANTDIGIS106  0x00600000
#define OPAL_ARCH_LDMANTDIGIS107  0x00100000
#define OPAL_ARCH_LDMANTDIGIS113  0x00500000

#define OPAL_ARCH_LDISINTEL       0x00800000

BEGIN_C_DECLS

/* Local Architecture */
OPAL_DECLSPEC extern uint32_t opal_local_arch;

/* Initialize architecture and determine all but fortran logical fields */
OPAL_DECLSPEC int opal_arch_init(void);

OPAL_DECLSPEC int32_t opal_arch_checkmask ( uint32_t *var, uint32_t mask );

/* Set fortran logical fields after init, to keep fortran out of opal... */
OPAL_DECLSPEC int opal_arch_set_fortran_logical_size(uint32_t size);

END_C_DECLS

#endif  /* OPAL_ARCH_H_HAS_BEEN_INCLUDED */

