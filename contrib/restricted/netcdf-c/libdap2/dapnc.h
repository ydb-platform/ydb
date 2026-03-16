/*********************************************************************
 *   Copyright 2018, UCAR/Unidata
 *   See netcdf/COPYRIGHT file for copying and redistribution conditions.
 *********************************************************************/
#ifndef DAPNC_H
#define DAPNC_H

/* Take some types from libsrc4/netcdf.h*/
#define	NC_UBYTE 	7	/* unsigned 1 byte int */
#define	NC_USHORT 	8	/* unsigned 2-byte int */
#define	NC_UINT 	9	/* unsigned 4-byte int */
#define	NC_INT64 	10	/* signed 8-byte int */
#define	NC_UINT64 	11	/* unsigned 8-byte int */
#define	NC_STRING 	12	/* string */
#define	NC_VLEN 	13	/* used internally for vlen types */
#define	NC_OPAQUE 	14	/* used internally for opaque types */
#define	NC_ENUM 	15	/* used internally for enum types */
#define	NC_COMPOUND 	16	/* used internally for compound types */

#endif /*DAPNC_H*/
