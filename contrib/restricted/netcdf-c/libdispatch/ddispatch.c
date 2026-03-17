/*
Copyright (c) 1998-2018 University Corporation for Atmospheric Research/Unidata
See LICENSE.txt for license information.
*/

#include "config.h"
#include "ncdispatch.h"
#include "ncuri.h"
#include "nclog.h"
#include "ncbytes.h"
#include "ncrc.h"
#include "ncoffsets.h"
#include "ncpathmgr.h"
#include "ncxml.h"
#include "nc4internal.h"

/* Required for getcwd, other functions. */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

/* Required for getcwd, other functions. */
#ifdef _WIN32
#include <direct.h>
#endif

#if defined(NETCDF_ENABLE_BYTERANGE) || defined(NETCDF_ENABLE_DAP) || defined(NETCDF_ENABLE_DAP4)
#include <curl/curl.h>
#endif

#ifdef NETCDF_ENABLE_S3
#include "ncs3sdk.h"
#endif

#define MAXPATH 1024

/* Define vectors of zeros and ones for use with various nc_get_varX functions */
/* Note, this form of initialization fails under Cygwin */
size_t NC_coord_zero[NC_MAX_VAR_DIMS] = {0};
size_t NC_coord_one[NC_MAX_VAR_DIMS] = {1};
ptrdiff_t NC_stride_one[NC_MAX_VAR_DIMS] = {1};

/*
static nc_type longtype = (sizeof(long) == sizeof(int)?NC_INT:NC_INT64);
static nc_type ulongtype = (sizeof(unsigned long) == sizeof(unsigned int)?NC_UINT:NC_UINT64);
*/

/* Allow dispatch to do general initialization and finalization */
int
NCDISPATCH_initialize(void)
{
    int status = NC_NOERR;
    int i;
    NCglobalstate* globalstate = NULL;

    for(i=0;i<NC_MAX_VAR_DIMS;i++) {
        NC_coord_zero[i] = 0;
        NC_coord_one[i]  = 1;
        NC_stride_one[i] = 1;
    }

    globalstate = NC_getglobalstate(); /* will allocate and clear */

    /* Capture temp dir*/
    {
	char* tempdir = NULL;
#if defined _WIN32 || defined __MSYS__ || defined __CYGWIN__
        tempdir = getenv("TEMP");
#else
	tempdir = "/tmp";
#endif
        if(tempdir == NULL) {
	    fprintf(stderr,"Cannot find a temp dir; using ./\n");
	    tempdir = ".";
	}
	globalstate->tempdir= strdup(tempdir);
    }

    /* Capture $HOME */
    {
#if defined(_WIN32) && !defined(__MINGW32__)
        char* home = getenv("USERPROFILE");
#else
        char* home = getenv("HOME");
#endif
        if(home == NULL) {
	    /* use cwd */
	    home = malloc(MAXPATH+1);
	    NCgetcwd(home,MAXPATH);
        } else
	    home = strdup(home); /* make it always free'able */
	assert(home != NULL);
        NCpathcanonical(home,&globalstate->home);
	nullfree(home);
    }
 
    /* Capture $CWD */
    {
        char cwdbuf[4096];

        cwdbuf[0] = '\0';
	(void)NCgetcwd(cwdbuf,sizeof(cwdbuf));

        if(strlen(cwdbuf) == 0) {
	    /* use tempdir */
	    strcpy(cwdbuf, globalstate->tempdir);
	}
        globalstate->cwd = strdup(cwdbuf);
    }

    ncloginit();

    /* Now load RC Files */
    ncrc_initialize();

    /* Compute type alignments */
    NC_compute_alignments();

#if defined(NETCDF_ENABLE_BYTERANGE) || defined(NETCDF_ENABLE_DAP) || defined(NETCDF_ENABLE_DAP4)
    /* Initialize curl if it is being used */
    {
        CURLcode cstat = curl_global_init(CURL_GLOBAL_ALL);
	if(cstat != CURLE_OK)
	    status = NC_ECURL;
    }
#endif

    return status;
}

int
NCDISPATCH_finalize(void)
{
    int status = NC_NOERR;
#if defined(NETCDF_ENABLE_BYTERANGE) || defined(NETCDF_ENABLE_DAP) || defined(NETCDF_ENABLE_DAP4)
    curl_global_cleanup();
#endif
#if defined(NETCDF_ENABLE_DAP4)
   ncxml_finalize();
#endif
    NC_freeglobalstate(); /* should be one of the last things done */
    return status;
}

/**************************************************/
/** \defgroup atomic_types Atomic Type functions */
/** \{

\ingroup atomic_types
*/

/* The sizes of types may vary from platform to platform, but within
 * netCDF files, type sizes are fixed. */
#define NC_CHAR_LEN sizeof(char)      /**< @internal Size of char. */
#define NC_STRING_LEN sizeof(char *)  /**< @internal Size of char *. */
#define NC_BYTE_LEN 1     /**< @internal Size of byte. */
#define NC_SHORT_LEN 2    /**< @internal Size of short. */
#define NC_INT_LEN 4      /**< @internal Size of int. */
#define NC_FLOAT_LEN 4    /**< @internal Size of float. */
#define NC_DOUBLE_LEN 8   /**< @internal Size of double. */
#define NC_INT64_LEN 8    /**< @internal Size of int64. */

/** @internal Names of atomic types. */
const char* nc4_atomic_name[NUM_ATOMIC_TYPES] = {"none", "byte", "char",
                                           "short", "int", "float",
                                           "double", "ubyte",
                                           "ushort", "uint",
                                           "int64", "uint64", "string"};
static const size_t nc4_atomic_size[NUM_ATOMIC_TYPES] = {0, NC_BYTE_LEN, NC_CHAR_LEN, NC_SHORT_LEN,
                                                      NC_INT_LEN, NC_FLOAT_LEN, NC_DOUBLE_LEN,
                                                      NC_BYTE_LEN, NC_SHORT_LEN, NC_INT_LEN, NC_INT64_LEN,
                                                      NC_INT64_LEN, NC_STRING_LEN};

/**
 * @internal Get the name and size of an atomic type. For strings, 1 is
 * returned.
 *
 * @param typeid1 Type ID.
 * @param name Gets the name of the type.
 * @param size Gets the size of one element of the type in bytes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Dennis Heimbigner
 */
int
NC4_inq_atomic_type(nc_type typeid1, char *name, size_t *size)
{
    if (typeid1 >= NUM_ATOMIC_TYPES)
	return NC_EBADTYPE;
    if (name)
            strcpy(name, nc4_atomic_name[typeid1]);
    if (size)
            *size = nc4_atomic_size[typeid1];
    return NC_NOERR;
}

/**
 * @internal Get the id and size of an atomic type by name.
 *
 * @param name [in] the name of the type.
 * @param idp [out] the type index of the type.
 * @param sizep [out] the size of one element of the type in bytes.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADID Bad ncid.
 * @return ::NC_EBADTYPE Type not found.
 * @author Dennis Heimbigner
 */
int
NC4_lookup_atomic_type(const char *name, nc_type* idp, size_t *sizep)
{
    int i;

    if (name == NULL || strlen(name) == 0)
	return NC_EBADTYPE;
    for(i=0;i<NUM_ATOMIC_TYPES;i++) {
	if(strcasecmp(name,nc4_atomic_name[i])==0) {	
	    if(idp) *idp = i;
            if(sizep) *sizep = nc4_atomic_size[i];
	    return NC_NOERR;
        }
    }
    return NC_EBADTYPE;
}

/**
 * @internal Get the id of an atomic type from the name.
 *
 * @param ncid File and group ID.
 * @param name Name of type
 * @param typeidp Pointer that will get the type ID.
 *
 * @return ::NC_NOERR No error.
 * @return ::NC_EBADTYPE Type not found.
 * @author Ed Hartnett
 */
int
NC4_inq_atomic_typeid(int ncid, const char *name, nc_type *typeidp)
{
    int i;

    NC_UNUSED(ncid);

    /* Handle atomic types. */
    for (i = 0; i < NUM_ATOMIC_TYPES; i++) {
        if (!strcmp(name, nc4_atomic_name[i]))
        {
            if (typeidp)
                *typeidp = i;
	    return NC_NOERR;
        }
    }
    return NC_EBADTYPE;
}

/**
 * @internal Get the class of a type
 *
 * @param xtype NetCDF type ID.
 * @param type_class Pointer that gets class of type, NC_INT,
 * NC_FLOAT, NC_CHAR, or NC_STRING, NC_ENUM, NC_VLEN, NC_COMPOUND, or
 * NC_OPAQUE.
 *
 * @return ::NC_NOERR No error.
 * @author Ed Hartnett, Dennis Heimbigner
 */
int
NC4_get_atomic_typeclass(nc_type xtype, int *type_class)
{
    assert(type_class);
    switch (xtype) {
        case NC_BYTE:
        case NC_UBYTE:
        case NC_SHORT:
        case NC_USHORT:
        case NC_INT:
        case NC_UINT:
        case NC_INT64:
        case NC_UINT64:
            /* NC_INT is class used for all integral types */
            *type_class = NC_INT;
            break;
        case NC_FLOAT:
        case NC_DOUBLE:
            /* NC_FLOAT is class used for all floating-point types */
            *type_class = NC_FLOAT;
            break;
        case NC_CHAR:
            *type_class = NC_CHAR;
            break;
        case NC_STRING:
            *type_class = NC_STRING;
            break;
        default:
	   return NC_EBADTYPE;
        }
    return NC_NOERR;
}

/** \} */

/**************************************************/
/** \defgroup alignment Alignment functions. */

/** \{

\ingroup alignment
*/

/**
Provide a function to store global data alignment
information.
Repeated calls to nc_set_alignment will overwrite any existing values.

If defined, then for every file created or opened after the call to
nc_set_alignment, and for every new variable added to the file, the
most recently set threshold and alignment values will be applied
to that variable.

The nc_set_alignment function causes new data written to a
netCDF-4 file to be aligned on disk to a specified block
size. To be effective, alignment should be the system disk block
size, or a multiple of it. This setting is effective with MPI
I/O and other parallel systems.

This is a trade-off of write speed versus file size. Alignment
leaves holes between file objects. The default of no alignment
writes file objects contiguously, without holes. Alignment has
no impact on file readability.

Alignment settings apply only indirectly, through the file open
functions. Call nc_set_alignment first, then nc_create or
nc_open for one or more files. Current alignment settings are
locked in when each file is opened, then forgotten when the same
file is closed. For illustration, it is possible to write
different files at the same time with different alignments, by
interleaving nc_set_alignment and nc_open calls.

Alignment applies to all newly written low-level file objects at
or above the threshold size, including chunks of variables,
attributes, and internal infrastructure. Alignment is not locked
in to a data variable. It can change between data chunks of the
same variable, based on a file's history.

Refer to H5Pset_alignment in HDF5 documentation for more
specific details, interactions, and additional rules.

@param threshold The minimum size to which alignment is applied.
@param alignment The alignment value.

@return ::NC_NOERR No error.
@return ::NC_EINVAL Invalid input.
@author Dennis Heimbigner
@ingroup datasets
*/
int
nc_set_alignment(int threshold, int alignment)
{
    NCglobalstate* gs = NC_getglobalstate();
    gs->alignment.threshold = threshold;
    gs->alignment.alignment = alignment;
    gs->alignment.defined = 1;
    return NC_NOERR;
}

/**
Provide get function to retrieve global data alignment
information.

The nc_get_alignment function return the last values set by
nc_set_alignment.  If nc_set_alignment has not been called, then
it returns the value 0 for both threshold and alignment.

@param thresholdp Return the current minimum size to which alignment is applied or zero.
@param alignmentp Return the current alignment value or zero.

@return ::NC_NOERR No error.
@return ::NC_EINVAL Invalid input.
@author Dennis Heimbigner
@ingroup datasets
*/

int
nc_get_alignment(int* thresholdp, int* alignmentp)
{
    NCglobalstate* gs = NC_getglobalstate();
    if(thresholdp) *thresholdp = gs->alignment.threshold;
    if(alignmentp) *alignmentp = gs->alignment.alignment;
    return NC_NOERR;
}

/** \} */
