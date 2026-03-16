/*
 *	Copyright 2018, University Corporation for Atmospheric Research
 *	See netcdf/COPYRIGHT file for copying and redistribution conditions.
 */

#ifndef _NCX_H_
#define _NCX_H_

#include "ncdispatch.h"
#include "ncio.h"
#include "fbits.h"

#ifndef HAVE_STDINT_H
#error #include "pstdint.h"
#else
#include <stdint.h>
#endif /* HAVE_STDINT_H */

/*
 * An external data representation interface.

 *
 * This started out as a general replacement for ONC XDR,
 * specifically, the xdrmem family of functions.
 *
 * We eventually realized that we could write more portable
 * code if we decoupled any association between the 'C' types
 * and the external types. (XDR has this association between the 'C'
 * types and the external representations, like xdr_int() takes
 * an int argument and goes to an external int representation.)
 * So, now there is a matrix of functions.
 *
 */

#include "config.h" /* output of 'configure' */
#include "rnd.h"
#include <stddef.h> /* size_t */
#include <errno.h>
#include <sys/types.h> /* off_t */
#include "ncdispatch.h"

#if defined(_CRAY) && !defined(_CRAYIEEE) && !defined(__crayx1)
#define CRAYFLOAT 1 /* CRAY Floating point */
#elif defined(_SX) && defined(_FLOAT2)	/* NEC SUPER-UX in CRAY mode */
#define CRAYFLOAT 1 /* CRAY Floating point */
#endif

#ifndef __cplusplus
  #if __STDC_VERSION__ == 199901L /* C99 */
  /* "inline" is a keyword */
  #elif _MSC_VER >= 1500 /* MSVC 9 or newer */
    #define inline __inline
  #elif __GNUC__ >= 3 /* GCC 3 or newer */
    #define inline __inline
  #else /* Unknown or ancient */
    #define inline
  #endif
#endif

/*
 * External sizes of the primitive elements.
 */
#define X_SIZEOF_CHAR		1
#define X_SIZEOF_SHORT		2
#define X_SIZEOF_INT		4	/* xdr_int */
#if 0
#define X_SIZEOF_LONG		8 */	/* alias */
#endif
#define X_SIZEOF_FLOAT		4
#define X_SIZEOF_DOUBLE		8

/* additional data types in CDF-5 */
#define X_SIZEOF_UBYTE		1
#define X_SIZEOF_USHORT		2
#define X_SIZEOF_UINT		4
#define X_SIZEOF_LONGLONG	8
#define X_SIZEOF_ULONGLONG	8
#define X_SIZEOF_INT64		8
#define X_SIZEOF_UINT64		8

/*
 * For now, netcdf is limited to 32 bit sizes,
 * If compiled with support for "large files", then
 * netcdf will use a 64 bit off_t and it can then write a file
 * using 64 bit offsets.
 *  see also X_SIZE_MAX, X_OFF_MAX below
 */
#define X_SIZEOF_OFF_T		(sizeof(off_t))
#define X_SIZEOF_SIZE_T		X_SIZEOF_INT

/*
 * limits of the external representation
 */
#define X_SCHAR_MIN	(-128)
#define X_SCHAR_MAX	127
#define X_UCHAR_MAX	255U
#define X_SHORT_MIN	(-32768)
#define X_SHRT_MIN	X_SHORT_MIN	/* alias compatible with limits.h */
#define X_SHORT_MAX	32767
#define X_SHRT_MAX	X_SHORT_MAX	/* alias compatible with limits.h */
#define X_USHORT_MAX	65535U
#define X_USHRT_MAX	X_USHORT_MAX	/* alias compatible with limits.h */
#define X_INT_MIN	(-2147483647-1)
#define X_INT_MAX	2147483647
#define X_UINT_MAX	4294967295U
#define X_INT64_MIN	(-9223372036854775807LL-1LL)
#define X_INT64_MAX	9223372036854775807LL
#define X_UINT64_MAX	18446744073709551615ULL
#define X_FLOAT_MAX	3.402823466e+38f
#define X_FLOAT_MIN	(-X_FLOAT_MAX)
#define X_FLT_MAX	X_FLOAT_MAX	/* alias compatible with limits.h */
#if CRAYFLOAT
/* ldexp(1. - ldexp(.5 , -46), 1024) */
#define X_DOUBLE_MAX    1.79769313486230e+308
#else
/* scalb(1. - scalb(.5 , -52), 1024) */
#define X_DOUBLE_MAX	1.7976931348623157e+308
#endif
#define X_DOUBLE_MIN	(-X_DOUBLE_MAX)
#define X_DBL_MAX	X_DOUBLE_MAX	/* alias compatible with limits.h */

#define X_SIZE_MAX	X_UINT_MAX
#define X_OFF_MAX	X_INT_MAX


/* Begin ncx_len */

/*
 * ncx_len_xxx() interfaces are defined as macros below,
 * These give the length of an array of nelems of the type.
 * N.B. The 'char' and 'short' interfaces give the X_ALIGNED length.
 */
#define X_ALIGN			4	/* a.k.a. BYTES_PER_XDR_UNIT */

#define ncx_len_char(nelems) \
	_RNDUP((nelems), X_ALIGN)

#define ncx_len_short(nelems) \
	(((nelems) + (nelems)%2)  * X_SIZEOF_SHORT)

#define ncx_len_int(nelems) \
	((nelems) * X_SIZEOF_INT)

#define ncx_len_long(nelems) \
	((nelems) * X_SIZEOF_LONG)

#define ncx_len_float(nelems) \
	((nelems) * X_SIZEOF_FLOAT)

#define ncx_len_double(nelems) \
	((nelems) * X_SIZEOF_DOUBLE)

#define ncx_len_ubyte(nelems) \
	_RNDUP((nelems), X_ALIGN)

#define ncx_len_ushort(nelems) \
	(((nelems) + (nelems)%2)  * X_SIZEOF_USHORT)

#define ncx_len_uint(nelems) \
	((nelems) * X_SIZEOF_UINT)

#define ncx_len_int64(nelems) \
	((nelems) * X_SIZEOF_INT64)

#define ncx_len_uint64(nelems) \
	((nelems) * X_SIZEOF_UINT64)

/* End ncx_len */

#ifndef HAVE_SCHAR
typedef signed char schar;
#endif

/*
 * Primitive numeric conversion functions.
 * The `put' functions convert from native internal
 * type to the external type, while the `get' functions
 * convert from the external to the internal.
 *
 * These take the form
 *	int ncx_get_{external_type}_{internal_type}(
 *		const void *xp,
 *		internal_type *ip
 *	);
 *	int ncx_put_{external_type}_{internal_type}(
 *		void *xp,
 *		const internal_type *ip
 *	);
 * where
 *	`external_type' and `internal_type' chosen from
 *		schar
 *		uchar
 *		short
 *		ushort
 *		int
 *		uint
 *		float
 *		double
 *		longlong == int64
 *	        ulonglong == uint64
 *
 * Not all combinations make sense.
 * We may not implement all combinations that make sense.
 * The netcdf functions that use this ncx interface don't
 * use these primitive conversion functions. They use the
 * aggregate conversion functions declared below.
 *
 * Storage for a single element of external type is at the `void * xp'
 * argument.
 *
 * Storage for a single element of internal type is at `ip' argument.
 *
 * These functions return 0 (NC_NOERR) when no error occurred,
 * or NC_ERANGE when the value being converted is too large.
 * When NC_ERANGE occurs, an undefined (implementation dependent)
 * conversion may have occurred.
 *
 * Note that loss of precision may occur silently.
 *
 */

#if 0
extern int
ncx_get_schar_schar(const void *xp, schar *ip);
extern int
ncx_get_schar_uchar(const void *xp, uchar *ip);
extern int
ncx_get_schar_short(const void *xp, short *ip);
extern int
ncx_get_schar_int(const void *xp, int *ip);
extern int
ncx_get_schar_long(const void *xp, long *ip);
extern int
ncx_get_schar_float(const void *xp, float *ip);
extern int
ncx_get_schar_double(const void *xp, double *ip);

extern int
ncx_put_schar_schar(void *xp, const schar *ip, void *fillp);
extern int
ncx_put_schar_uchar(void *xp, const uchar *ip, void *fillp);
extern int
ncx_put_schar_short(void *xp, const short *ip, void *fillp);
extern int
ncx_put_schar_int(void *xp, const int *ip, void *fillp);
extern int
ncx_put_schar_long(void *xp, const long *ip, void *fillp);
extern int
ncx_put_schar_float(void *xp, const float *ip, void *fillp);
extern int
ncx_put_schar_double(void *xp, const double *ip, void *fillp);
#endif

/*
 * Other primitive conversion functions
 * N.B. slightly different interface
 * Used by netcdf.
 */

/* ncx_get_int_size_t */
extern int
ncx_get_size_t(const void **xpp, size_t *ulp);
/* ncx_get_int_off_t */
extern int
ncx_get_off_t(const void **xpp, off_t *lp, size_t sizeof_off_t);

/* ncx_put_int_size_t */
extern int
ncx_put_size_t(void **xpp, const size_t *ulp);
/* ncx_put_int_off_t */
extern int
ncx_put_off_t(void **xpp, const off_t *lp, size_t sizeof_off_t);

extern int
ncx_get_int32(const void **xpp, int *ip);
extern int
ncx_get_int64(const void **xpp, long long *ip);
extern int
ncx_put_int32(void **xpp, const int ip);
extern int
ncx_put_int64(void **xpp, const long long ip);

extern int
ncx_get_uint32(const void **xpp, unsigned int *ip);
extern int
ncx_get_uint64(const void **xpp, unsigned long long *ip);
extern int
ncx_put_uint32(void **xpp, const unsigned int ip);
extern int
ncx_put_uint64(void **xpp, const unsigned long long ip);

extern int ncx_get_int_int(const void *xp, int *ip);
extern int ncx_put_int_int(void *xp, const int *ip, void *fillp);

/*
 * Aggregate numeric conversion functions.
 * Convert an array.  Replaces xdr_array(...).
 * These functions are used by netcdf. Unlike the xdr
 * interface, we optimize for aggregate conversions.
 * This functions should be implemented to take advantage
 * of multiple processor / parallel hardware where available.
 *
 * These take the form
 *	int ncx_getn_{external_type}_{internal_type}(
 *		const void *xpp,
 *		size_t nelems,
 *		internal_type *ip
 *	);
 *	int ncx_putn_{external_type}_{internal_type}(
 *		void **xpp,
 *		size_t nelems,
 *		const internal_type *ip
 *	);
 * Where the types are as in the primitive numeric conversion functions.
 *
 * The value of the pointer to pointer argument, *xpp, is
 * expected to reference storage for `nelems' of the external
 * type.  On return, it modified to reference just past the last
 * converted external element.
 *
 * The types whose external size is less than X_ALIGN also have `pad'
 * interfaces. These round (and zero fill on put) *xpp up to X_ALIGN
 * boundaries. (This is the usual xdr behavior.)
 *
 * The `ip' argument should point to an array of `nelems' of
 * internal_type.
 *
 * Range errors (NC_ERANGE) for a individual values in the array
 * DO NOT terminate the array conversion. All elements are converted,
 * with some having undefined values.
 * If any range error occurs, the function returns NC_ERANGE.
 *
 */

/*---- schar ----------------------------------------------------------------*/
extern int
ncx_getn_schar_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_schar_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_schar_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_schar_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_schar_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_schar_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_schar_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_schar_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_schar_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_schar_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_schar_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_pad_getn_schar_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_pad_getn_schar_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_pad_getn_schar_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_pad_getn_schar_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_pad_getn_schar_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_pad_getn_schar_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_pad_getn_schar_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_pad_getn_schar_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_pad_getn_schar_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_pad_getn_schar_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_pad_getn_schar_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_schar_schar (void **xpp, size_t nelems, const schar   *ip, void *fillp);
extern int
ncx_putn_schar_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_schar_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_schar_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_schar_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_schar_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_schar_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_schar_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_schar_double(void **xpp, size_t nelems, const double  *ip, void *fillp);
extern int
ncx_putn_schar_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_schar_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

extern int
ncx_pad_putn_schar_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_pad_putn_schar_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_pad_putn_schar_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_pad_putn_schar_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_pad_putn_schar_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_pad_putn_schar_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_pad_putn_schar_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_pad_putn_schar_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_pad_putn_schar_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_pad_putn_schar_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_pad_putn_schar_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- uchar ----------------------------------------------------------------*/
extern int
ncx_getn_uchar_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_uchar_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_uchar_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_uchar_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_uchar_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_uchar_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_uchar_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_uchar_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_uchar_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_uchar_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_uchar_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_pad_getn_uchar_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_pad_getn_uchar_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_pad_getn_uchar_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_pad_getn_uchar_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_pad_getn_uchar_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_pad_getn_uchar_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_pad_getn_uchar_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_pad_getn_uchar_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_pad_getn_uchar_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_pad_getn_uchar_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_pad_getn_uchar_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_uchar_schar (void **xpp, size_t nelems, const schar   *ip, void *fillp);
extern int
ncx_putn_uchar_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_uchar_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_uchar_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_uchar_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_uchar_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_uchar_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_uchar_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_uchar_double(void **xpp, size_t nelems, const double  *ip, void *fillp);
extern int
ncx_putn_uchar_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_uchar_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

extern int
ncx_pad_putn_uchar_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_pad_putn_uchar_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_pad_putn_uchar_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_pad_putn_uchar_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_pad_putn_uchar_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_pad_putn_uchar_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_pad_putn_uchar_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_pad_putn_uchar_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_pad_putn_uchar_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_pad_putn_uchar_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_pad_putn_uchar_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- short ----------------------------------------------------------------*/
extern int
ncx_getn_short_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_short_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_short_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_short_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_short_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_short_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_short_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_short_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_short_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_short_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_short_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_pad_getn_short_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_pad_getn_short_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_pad_getn_short_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_pad_getn_short_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_pad_getn_short_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_pad_getn_short_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_pad_getn_short_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_pad_getn_short_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_pad_getn_short_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_pad_getn_short_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_pad_getn_short_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_short_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_short_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_short_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_short_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_short_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_short_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_short_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_short_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_short_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_short_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_short_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

extern int
ncx_pad_putn_short_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_pad_putn_short_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_pad_putn_short_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_pad_putn_short_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_pad_putn_short_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_pad_putn_short_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_pad_putn_short_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_pad_putn_short_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_pad_putn_short_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_pad_putn_short_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_pad_putn_short_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- ushort ---------------------------------------------------------------*/
extern int
ncx_getn_ushort_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_ushort_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_ushort_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_ushort_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_ushort_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_ushort_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_ushort_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_ushort_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_ushort_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_ushort_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_ushort_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_pad_getn_ushort_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_pad_getn_ushort_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_pad_getn_ushort_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_pad_getn_ushort_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_pad_getn_ushort_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_pad_getn_ushort_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_pad_getn_ushort_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_pad_getn_ushort_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_pad_getn_ushort_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_pad_getn_ushort_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_pad_getn_ushort_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_ushort_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_ushort_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_ushort_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_ushort_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_ushort_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_ushort_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_ushort_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_ushort_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_ushort_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_ushort_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_ushort_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

extern int
ncx_pad_putn_ushort_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_pad_putn_ushort_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_pad_putn_ushort_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_pad_putn_ushort_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_pad_putn_ushort_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_pad_putn_ushort_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_pad_putn_ushort_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_pad_putn_ushort_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_pad_putn_ushort_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_pad_putn_ushort_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_pad_putn_ushort_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- int ------------------------------------------------------------------*/
extern int
ncx_getn_int_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_int_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_int_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_int_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_int_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_int_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_int_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_long_long (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_int_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_int_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_int_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_int_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_int_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_int_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_int_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_int_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_int_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_int_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_int_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_long_long (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_int_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_int_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_int_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_int_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- uint -----------------------------------------------------------------*/
extern int
ncx_getn_uint_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_uint_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_uint_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_uint_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_uint_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_uint_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_uint_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_long_long (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_uint_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_uint_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_uint_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_uint_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_uint_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_uint_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_uint_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_uint_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_uint_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_uint_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_uint_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_long_long (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_uint_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_uint_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_uint_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_uint_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- float ----------------------------------------------------------------*/
extern int
ncx_getn_float_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_float_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_float_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_float_ushort(const void **xpp, size_t nelems, ushort *ip);
extern int
ncx_getn_float_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_float_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_float_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_float_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_float_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_float_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_float_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_float_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_float_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_float_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_float_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_float_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_float_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_float_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_float_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_float_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_float_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_float_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- double ---------------------------------------------------------------*/
extern int
ncx_getn_double_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_double_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_double_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_double_ushort(const void **xpp, size_t nelems, ushort  *ip);
extern int
ncx_getn_double_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_double_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_double_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_double_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_double_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_double_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_double_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_double_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_double_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_double_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_double_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_double_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_double_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_double_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_double_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_double_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_double_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_double_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- longlong ----------------------------------------------------------------*/
extern int
ncx_getn_longlong_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_longlong_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_longlong_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_longlong_ushort(const void **xpp, size_t nelems, ushort  *ip);
extern int
ncx_getn_longlong_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_longlong_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_longlong_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_longlong_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_longlong_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_longlong_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_longlong_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_longlong_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_longlong_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_longlong_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_longlong_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_longlong_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_longlong_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_longlong_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_longlong_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_longlong_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_longlong_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_longlong_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);

/*---- ulonglong ---------------------------------------------------------------*/
extern int
ncx_getn_ulonglong_schar (const void **xpp, size_t nelems, schar  *ip);
extern int
ncx_getn_ulonglong_uchar (const void **xpp, size_t nelems, uchar  *ip);
extern int
ncx_getn_ulonglong_short (const void **xpp, size_t nelems, short  *ip);
extern int
ncx_getn_ulonglong_ushort(const void **xpp, size_t nelems, ushort  *ip);
extern int
ncx_getn_ulonglong_int   (const void **xpp, size_t nelems, int    *ip);
extern int
ncx_getn_ulonglong_uint  (const void **xpp, size_t nelems, uint   *ip);
extern int
ncx_getn_ulonglong_long  (const void **xpp, size_t nelems, long   *ip);
extern int
ncx_getn_ulonglong_float (const void **xpp, size_t nelems, float  *ip);
extern int
ncx_getn_ulonglong_double(const void **xpp, size_t nelems, double *ip);
extern int
ncx_getn_ulonglong_longlong (const void **xpp, size_t nelems, longlong  *ip);
extern int
ncx_getn_ulonglong_ulonglong(const void **xpp, size_t nelems, ulonglong *ip);

extern int
ncx_putn_ulonglong_schar (void **xpp, size_t nelems, const schar  *ip, void *fillp);
extern int
ncx_putn_ulonglong_uchar (void **xpp, size_t nelems, const uchar  *ip, void *fillp);
extern int
ncx_putn_ulonglong_short (void **xpp, size_t nelems, const short  *ip, void *fillp);
extern int
ncx_putn_ulonglong_ushort(void **xpp, size_t nelems, const ushort *ip, void *fillp);
extern int
ncx_putn_ulonglong_int   (void **xpp, size_t nelems, const int    *ip, void *fillp);
extern int
ncx_putn_ulonglong_uint  (void **xpp, size_t nelems, const uint   *ip, void *fillp);
extern int
ncx_putn_ulonglong_long  (void **xpp, size_t nelems, const long   *ip, void *fillp);
extern int
ncx_putn_ulonglong_float (void **xpp, size_t nelems, const float  *ip, void *fillp);
extern int
ncx_putn_ulonglong_double(void **xpp, size_t nelems, const double *ip, void *fillp);
extern int
ncx_putn_ulonglong_longlong (void **xpp, size_t nelems, const longlong  *ip, void *fillp);
extern int
ncx_putn_ulonglong_ulonglong(void **xpp, size_t nelems, const ulonglong *ip, void *fillp);


/*
 * Other aggregate conversion functions.
 */

/* read ASCII characters */
extern int
ncx_getn_text(const void **xpp, size_t nchars, char *cp);
extern int
ncx_pad_getn_text(const void **xpp, size_t nchars, char *cp);

/* write ASCII characters */
extern int
ncx_putn_text(void **xpp, size_t nchars, const char *cp);
extern int
ncx_pad_putn_text(void **xpp, size_t nchars, const char *cp);


/* for symmetry */
#define ncx_getn_char_char(xpp, nelems, fillp) ncx_getn_text(xpp, nelems, fillp)
#define ncx_putn_char_char(xpp, nelems, fillp) ncx_putn_text(xpp, nelems, fillp)

/* read opaque data */
extern int
ncx_getn_void(const void **xpp, size_t nchars, void *vp);
extern int
ncx_pad_getn_void(const void **xpp, size_t nchars, void *vp);

/* write opaque data */
extern int
ncx_putn_void(void **xpp, size_t nchars, const void *vp);
extern int
ncx_pad_putn_void(void **xpp, size_t nchars, const void *vp);

#endif /* _NCX_H_ */
