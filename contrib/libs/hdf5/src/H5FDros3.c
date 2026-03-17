/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://www.hdfgroup.org/licenses.               *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Read-Only S3 Virtual File Driver (VFD)
 *
 * Purpose:
 *
 *     Provide read-only access to files hosted on Amazon's S3 service.
 *     Relies on "s3comms" utility layer to implement the AWS REST API.
 */

#ifdef H5_HAVE_ROS3_VFD
/* This source code file is part of the H5FD driver module */
#include "H5FDdrvr_module.h"
#endif

#include "H5private.h"   /* Generic Functions        */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5FDprivate.h" /* File drivers             */
#include "H5FDros3.h"    /* ros3 file driver         */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management        */
#include "H5FDs3comms.h" /* S3 Communications        */

#ifdef H5_HAVE_ROS3_VFD

/* toggle function call prints: 1 turns on
 */
#define ROS3_DEBUG 0

/* toggle stats collection and reporting
 */
#define ROS3_STATS 0

/* Max size of the cache, in bytes */
#define ROS3_MAX_CACHE_SIZE 16777216

/* The driver identification number, initialized at runtime
 */
static hid_t H5FD_ROS3_g = 0;

/* Session/security token property name
 */
#define ROS3_TOKEN_PROP_NAME "ros3_token_prop"

#if ROS3_STATS

/* arbitrarily large value, such that any reasonable size read will be "less"
 * than this value and set a true minimum
 * not 0 because that may be a valid recorded minimum in degenerate cases
 */
#define ROS3_STATS_STARTING_MIN 0xfffffffful

/* Configuration definitions for stats collection and breakdown
 *
 * 2^10 = 1024
 *     Reads up to 1024 bytes (1 kB) fall in bin 0
 * 2^(10+(1*16)) = 2^26 = 64MB
 *     Reads of 64MB or greater fall in "overflow" bin[BIN_COUNT]
 */
#define ROS3_STATS_BASE        2
#define ROS3_STATS_INTERVAL    1
#define ROS3_STATS_START_POWER 10
#define ROS3_STATS_BIN_COUNT   16 /* MUST BE GREATER THAN 0 */

/*
 * Calculate `BASE ^ (START_POWER + (INTERVAL * bin_i))`
 * Stores result at `(unsigned long long *) out_ptr`.
 * Used in computing boundaries between stats bins.
 */
#define ROS3_STATS_POW(bin_i, out_ptr)                                                                       \
    {                                                                                                        \
        unsigned long long donotshadowresult = 1;                                                            \
        unsigned           donotshadowindex  = 0;                                                            \
        for (donotshadowindex = 0;                                                                           \
             donotshadowindex < (((bin_i)*ROS3_STATS_INTERVAL) + ROS3_STATS_START_POWER);                    \
             donotshadowindex++) {                                                                           \
            donotshadowresult *= ROS3_STATS_BASE;                                                            \
        }                                                                                                    \
        *(out_ptr) = donotshadowresult;                                                                      \
    }

/* array to hold pre-computed boundaries for stats bins
 */
static unsigned long long ros3_stats_boundaries[ROS3_STATS_BIN_COUNT];

/***************************************************************************
 *
 * Structure: ros3_statsbin
 *
 * Purpose:
 *
 *     Structure for storing per-file ros3 VFD usage statistics.
 *
 *
 *
 * `count` (unsigned long long)
 *
 *     Number of reads with size in this bin's range.
 *
 * `bytes` (unsigned long long)
 *
 *     Total number of bytes read through this bin.
 *
 * `min` (unsigned long long)
 *
 *     Smallest read size in this bin.
 *
 * `max` (unsigned long long)
 *
 *     Largest read size in this bin.
 *
 ***************************************************************************/
typedef struct {
    unsigned long long count;
    unsigned long long bytes;
    unsigned long long min;
    unsigned long long max;
} ros3_statsbin;

#endif /* ROS3_STATS */

/***************************************************************************
 *
 * Structure: H5FD_ros3_t
 *
 * Purpose:
 *
 *     H5FD_ros3_t is a structure used to store all information needed to
 *     maintain R/O access to a single HDF5 file that has been stored as a
 *     S3 object.  This structure is created when such a file is "opened" and
 *     discarded when it is "closed".
 *
 *     Presents an S3 object as a file to the HDF5 library.
 *
 *
 *
 * `pub` (H5FD_t)
 *
 *     Instance of H5FD_t which contains all fields common to all VFDs.
 *     It must be the first item in this structure, since at higher levels,
 *     this structure will be treated as an instance of H5FD_t.
 *
 * `fa` (H5FD_ros3_fapl_t)
 *
 *     Instance of `H5FD_ros3_fapl_t` containing the S3 configuration data
 *     needed to "open" the HDF5 file.
 *
 * `eoa` (haddr_t)
 *
 *     End of addressed space in file. After open, it should always
 *     equal the file size.
 *
 * `s3r_handle` (s3r_t *)
 *
 *     Instance of S3 Request handle associated with the target resource.
 *     Responsible for communicating with remote host and presenting file
 *     contents as indistinguishable from a file on the local filesystem.
 *
 * *** present only if ROS3_SATS is flagged to enable stats collection ***
 *
 * `meta` (ros3_statsbin[])
 * `raw` (ros3_statsbin[])
 *
 *     Only present if ros3 stats collection is enabled.
 *
 *     Arrays of `ros3_statsbin` structures to record raw- and metadata reads.
 *
 *     Records count and size of reads performed by the VFD, and is used to
 *     print formatted usage statistics to stdout upon VFD shutdown.
 *
 *     Reads of each raw- and metadata type are recorded in an individual bin
 *     determined by the size of the read.  The last bin of each type is
 *     reserved for "big" reads, with no defined upper bound.
 *
 * *** end ROS3_STATS ***
 *
 ***************************************************************************/
typedef struct H5FD_ros3_t {
    H5FD_t           pub;
    H5FD_ros3_fapl_t fa;
    haddr_t          eoa;
    s3r_t           *s3r_handle;
    uint8_t         *cache;
    size_t           cache_size;
#if ROS3_STATS
    ros3_statsbin meta[ROS3_STATS_BIN_COUNT + 1];
    ros3_statsbin raw[ROS3_STATS_BIN_COUNT + 1];
#endif
} H5FD_ros3_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *                  Only included if it may be used -- ROS3 VFD is enabled.
 *
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))

/* Prototypes */
static herr_t  H5FD__ros3_term(void);
static void   *H5FD__ros3_fapl_get(H5FD_t *_file);
static void   *H5FD__ros3_fapl_copy(const void *_old_fa);
static herr_t  H5FD__ros3_fapl_free(void *_fa);
static H5FD_t *H5FD__ros3_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__ros3_close(H5FD_t *_file);
static int     H5FD__ros3_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__ros3_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__ros3_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__ros3_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__ros3_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__ros3_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__ros3_read(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__ros3_write(H5FD_t *_file, H5FD_mem_t type, hid_t fapl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD__ros3_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);

static herr_t H5FD__ros3_validate_config(const H5FD_ros3_fapl_t *fa);

static herr_t H5FD__ros3_str_token_copy(const char *name, size_t size, void *_value);
static int    H5FD__ros3_str_token_cmp(const void *_value1, const void *_value2, size_t size);
static herr_t H5FD__ros3_str_token_close(const char *name, size_t size, void *_value);
static herr_t H5FD__ros3_str_token_delete(hid_t prop_id, const char *name, size_t size, void *_value);

static const H5FD_class_t H5FD_ros3_g = {
    H5FD_CLASS_VERSION,       /* struct version       */
    H5FD_ROS3_VALUE,          /* value                */
    "ros3",                   /* name                 */
    MAXADDR,                  /* maxaddr              */
    H5F_CLOSE_WEAK,           /* fc_degree            */
    H5FD__ros3_term,          /* terminate            */
    NULL,                     /* sb_size              */
    NULL,                     /* sb_encode            */
    NULL,                     /* sb_decode            */
    sizeof(H5FD_ros3_fapl_t), /* fapl_size            */
    H5FD__ros3_fapl_get,      /* fapl_get             */
    H5FD__ros3_fapl_copy,     /* fapl_copy            */
    H5FD__ros3_fapl_free,     /* fapl_free            */
    0,                        /* dxpl_size            */
    NULL,                     /* dxpl_copy            */
    NULL,                     /* dxpl_free            */
    H5FD__ros3_open,          /* open                 */
    H5FD__ros3_close,         /* close                */
    H5FD__ros3_cmp,           /* cmp                  */
    H5FD__ros3_query,         /* query                */
    NULL,                     /* get_type_map         */
    NULL,                     /* alloc                */
    NULL,                     /* free                 */
    H5FD__ros3_get_eoa,       /* get_eoa              */
    H5FD__ros3_set_eoa,       /* set_eoa              */
    H5FD__ros3_get_eof,       /* get_eof              */
    H5FD__ros3_get_handle,    /* get_handle           */
    H5FD__ros3_read,          /* read                 */
    H5FD__ros3_write,         /* write                */
    NULL,                     /* read_vector          */
    NULL,                     /* write_vector         */
    NULL,                     /* read_selection       */
    NULL,                     /* write_selection      */
    NULL,                     /* flush                */
    H5FD__ros3_truncate,      /* truncate             */
    NULL,                     /* lock                 */
    NULL,                     /* unlock               */
    NULL,                     /* del                  */
    NULL,                     /* ctl                  */
    H5FD_FLMAP_DICHOTOMY      /* fl_map               */
};

/* Declare a free list to manage the H5FD_ros3_t struct */
H5FL_DEFINE_STATIC(H5FD_ros3_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD_ros3_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the ros3 driver.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_ros3_init(void)
{
    hid_t ret_value = H5I_INVALID_HID;
#if ROS3_STATS
    unsigned int bin_i;
#endif

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

#if ROS3_DEBUG
    fprintf(stdout, "H5FD_ros3_init() called.\n");
#endif

    if (H5I_VFL != H5I_get_type(H5FD_ROS3_g)) {
        H5FD_ROS3_g = H5FD_register(&H5FD_ros3_g, sizeof(H5FD_class_t), false);
        if (H5I_INVALID_HID == H5FD_ROS3_g) {
            HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register ros3");
        }
    }

#if ROS3_STATS
    /* pre-compute statsbin boundaries
     */
    for (bin_i = 0; bin_i < ROS3_STATS_BIN_COUNT; bin_i++) {
        unsigned long long value = 0;

        ROS3_STATS_POW(bin_i, &value)
        ros3_stats_boundaries[bin_i] = value;
    }
#endif

    ret_value = H5FD_ROS3_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_ros3_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__ros3_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_term() called.\n");
#endif

    /* Reset VFL ID */
    H5FD_ROS3_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__ros3_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_ros3
 *
 * Purpose:     Modify the file access property list to use the H5FD_ROS3
 *              driver defined in this source file.  All driver specific
 *              properties are passed in as a pointer to a suitably
 *              initialized instance of H5FD_ros3_fapl_t
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_ros3(hid_t fapl_id, const H5FD_ros3_fapl_t *fa)
{
    H5P_genplist_t *plist     = NULL; /* Property list pointer */
    herr_t          ret_value = FAIL;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*#", fapl_id, fa);

    assert(fa != NULL);

#if ROS3_DEBUG
    fprintf(stdout, "H5Pset_fapl_ros3() called.\n");
#endif

    plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS);
    if (plist == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    if (FAIL == H5FD__ros3_validate_config(fa))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid ros3 config");

    ret_value = H5P_set_driver(plist, H5FD_ROS3, (const void *)fa, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_ros3() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_validate_config()
 *
 * Purpose:     Test to see if the supplied instance of H5FD_ros3_fapl_t
 *              contains internally consistent data.  Return SUCCEED if so,
 *              and FAIL otherwise.
 *
 *              Note the difference between internally consistent and
 *              correct.  As we will have to try to access the target
 *              object to determine whether the supplied data is correct,
 *              we will settle for internal consistency at this point
 *
 * Return:      SUCCEED if instance of H5FD_ros3_fapl_t contains internally
 *              consistent data, FAIL otherwise.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_validate_config(const H5FD_ros3_fapl_t *fa)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(fa != NULL);

    if (fa->version != H5FD_CURR_ROS3_FAPL_T_VERSION)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Unknown H5FD_ros3_fapl_t version");

    /* if set to authenticate, region and id cannot be empty strings */
    if (fa->authenticate == true)
        if ((fa->aws_region[0] == '\0') || (fa->secret_id[0] == '\0'))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Inconsistent authentication information");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_validate_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_ros3
 *
 * Purpose:     Returns information about the ros3 file access property
 *              list though the function arguments.
 *
 * Return:      Success:        Non-negative
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_ros3(hid_t fapl_id, H5FD_ros3_fapl_t *fa_dst /*out*/)
{
    const H5FD_ros3_fapl_t *fa_src    = NULL;
    H5P_genplist_t         *plist     = NULL;
    herr_t                  ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, fa_dst);

#if ROS3_DEBUG
    fprintf(stdout, "H5Pget_fapl_ros3() called.\n");
#endif

    if (fa_dst == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "fa_dst is NULL");

    plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS);
    if (plist == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");

    if (H5FD_ROS3 != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");

    fa_src = (const H5FD_ros3_fapl_t *)H5P_peek_driver_info(plist);
    if (fa_src == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");

    /* Copy the ros3 fapl data out */
    H5MM_memcpy(fa_dst, fa_src, sizeof(H5FD_ros3_fapl_t));

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_ros3() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_fapl_get
 *
 * Purpose:     Gets a file access property list which could be used to
 *              create an identical file.
 *
 * Return:      Success:        Ptr to new file access property list value.
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__ros3_fapl_get(H5FD_t *_file)
{
    H5FD_ros3_t      *file      = (H5FD_ros3_t *)_file;
    H5FD_ros3_fapl_t *fa        = NULL;
    void             *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    fa = (H5FD_ros3_fapl_t *)H5MM_calloc(sizeof(H5FD_ros3_fapl_t));
    if (fa == NULL)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the fields of the structure */
    H5MM_memcpy(fa, &(file->fa), sizeof(H5FD_ros3_fapl_t));

    /* Set return value */
    ret_value = fa;

done:
    if (ret_value == NULL)
        if (fa != NULL)
            H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_fapl_copy
 *
 * Purpose:     Copies the ros3-specific file access properties.
 *
 * Return:      Success:        Ptr to a new property list
 *
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__ros3_fapl_copy(const void *_old_fa)
{
    const H5FD_ros3_fapl_t *old_fa    = (const H5FD_ros3_fapl_t *)_old_fa;
    H5FD_ros3_fapl_t       *new_fa    = NULL;
    void                   *ret_value = NULL;

    FUNC_ENTER_PACKAGE

    new_fa = (H5FD_ros3_fapl_t *)H5MM_malloc(sizeof(H5FD_ros3_fapl_t));
    if (new_fa == NULL)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    H5MM_memcpy(new_fa, old_fa, sizeof(H5FD_ros3_fapl_t));
    ret_value = new_fa;

done:
    if (ret_value == NULL)
        if (new_fa != NULL)
            H5MM_xfree(new_fa);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_fapl_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_fapl_free
 *
 * Purpose:     Frees the ros3-specific file access properties.
 *
 * Return:      SUCCEED (cannot fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_fapl_free(void *_fa)
{
    H5FD_ros3_fapl_t *fa = (H5FD_ros3_fapl_t *)_fa;

    FUNC_ENTER_PACKAGE_NOERR

    assert(fa != NULL); /* sanity check */

    H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__ros3_fapl_free() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_ros3_token
 *
 * Purpose:     Returns session/security token of the ros3 file access
 *              property list though the function arguments.
 *
 * Return:      Success:        Non-negative
 *
 *              Failure:        Negative
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_ros3_token(hid_t fapl_id, size_t size, char *token_dst /*out*/)
{
    H5P_genplist_t *plist = NULL;
    char           *token_src;
    htri_t          token_exists;
    size_t          tokenlen  = 0;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "izx", fapl_id, size, token_dst);

#if ROS3_DEBUG
    fprintf(stdout, "H5Pget_fapl_ros3_token() called.\n");
#endif

    if (size == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "size cannot be zero.");
    if (token_dst == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "token_dst is NULL");

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5FD_ROS3 != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if ((token_exists = H5P_exist_plist(plist, ROS3_TOKEN_PROP_NAME)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "failed to check if property token exists in plist");
    if (token_exists) {
        if (H5P_get(plist, ROS3_TOKEN_PROP_NAME, &token_src) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get token value");
    }

    /* Copy the token data out */
    tokenlen = strlen(token_src);
    if (size <= tokenlen) {
        tokenlen = size - 1;
    }
    H5MM_memcpy(token_dst, token_src, sizeof(char) * tokenlen);
    token_dst[tokenlen] = '\0';

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_ros3_token() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_str_token_copy()
 *
 * Purpose:     Create a copy of the token string.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_str_token_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *_value)
{
    char **value     = (char **)_value;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_str_token_copy() called.\n");
#endif

    if (*value)
        if (NULL == (*value = strdup(*value)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't copy string property token");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_str_token_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_str_token_cmp()
 *
 * Purpose:     Compares two token strings with each other.
 *
 * Return:
 *     - Equivalent:     0
 *     - Not Equivalent: non-zero value
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__ros3_str_token_cmp(const void *_value1, const void *_value2, size_t H5_ATTR_UNUSED size)
{
    char *const *value1    = (char *const *)_value1;
    char *const *value2    = (char *const *)_value2;
    int          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    if (*value1) {
        if (*value2)
            ret_value = strcmp(*value1, *value2);
        else
            ret_value = 1;
    }
    else {
        if (*value2)
            ret_value = -1;
        else
            ret_value = 0;
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_str_token_cmp */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_str_token_close()
 *
 * Purpose:     Closes/frees the memory associated to the token string.
 *              Currently, it is an empty implementation since there no
 *              additional treatment needed for this property.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_str_token_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *_value)
{
    char **value     = (char **)_value;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    if (*value)
        free(*value);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_str_token_close */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_str_token_delete()
 *
 * Purpose:     Deletes the property token from the property list and frees
 *              the memory associated to the token string.
 *              Currently, it is an empty implementation since there no
 *              additional treatment needed for this property.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_str_token_delete(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                            size_t H5_ATTR_UNUSED size, void *_value)
{
    char **value     = (char **)_value;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    if (*value)
        free(*value);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_str_token_delete */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_ros3_token()
 *
 * Purpose:     Modify the file access property list to use the H5FD_ROS3
 *              driver defined in this source file by adding or
 *              modifying the session/security token property.
 *
 * Return:      SUCCEED/FAIL
 *
 * Programmer:  Jan-Willem Blokland
 *              2023-05-26
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_ros3_token(hid_t fapl_id, const char *token)
{
    H5P_genplist_t *plist = NULL;
    char           *token_src;
    htri_t          token_exists;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*s", fapl_id, token);

#if ROS3_DEBUG
    fprintf(stdout, "H5Pset_fapl_ros3_token() called.\n");
#endif

    if (fapl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5FD_ROS3 != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (strlen(token) > H5FD_ROS3_MAX_SECRET_TOK_LEN)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL,
                    "specified token exceeds the internally specified maximum string length");

    if ((token_exists = H5P_exist_plist(plist, ROS3_TOKEN_PROP_NAME)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "failed to check if property token exists in plist");

    if (token_exists) {
        if (H5P_get(plist, ROS3_TOKEN_PROP_NAME, &token_src) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "unable to get token value");

        H5MM_memcpy(token_src, token, strlen(token) + 1);
    }
    else {
        token_src = (char *)malloc(sizeof(char) * (H5FD_ROS3_MAX_SECRET_TOK_LEN + 1));
        if (token_src == NULL)
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "cannot make space for token_src variable.");
        H5MM_memcpy(token_src, token, strlen(token) + 1);
        if (H5P_insert(plist, ROS3_TOKEN_PROP_NAME, sizeof(char *), &token_src, NULL, NULL, NULL, NULL,
                       H5FD__ros3_str_token_delete, H5FD__ros3_str_token_copy, H5FD__ros3_str_token_cmp,
                       H5FD__ros3_str_token_close) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "unable to register property in plist");
    }

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_ros3_token() */

#if ROS3_STATS
/*----------------------------------------------------------------------------
 *
 * Function: ros3_reset_stats()
 *
 * Purpose:
 *
 *     Reset the stats collection elements in this virtual file structure.
 *
 *     Clears any set data in stats bins; initializes/zeroes values.
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *         - Occurs if the file is invalid somehow
 *
 *----------------------------------------------------------------------------
 */
static herr_t
ros3_reset_stats(H5FD_ros3_t *file)
{
    unsigned i         = 0;
    herr_t   ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    printf("ros3_reset_stats() called\n");
#endif

    if (file == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file was null");

    for (i = 0; i <= ROS3_STATS_BIN_COUNT; i++) {
        file->raw[i].bytes = 0;
        file->raw[i].count = 0;
        file->raw[i].min   = (unsigned long long)ROS3_STATS_STARTING_MIN;
        file->raw[i].max   = 0;

        file->meta[i].bytes = 0;
        file->meta[i].count = 0;
        file->meta[i].min   = (unsigned long long)ROS3_STATS_STARTING_MIN;
        file->meta[i].max   = 0;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end ros3_reset_stats() */
#endif /* ROS3_STATS */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_open()
 *
 * Purpose:
 *
 *     Create and/or opens a file as an HDF5 file.
 *
 *     Any flag except H5F_ACC_RDONLY will cause an error.
 *
 *     Name (as received from `H5FD_open()`) must conform to web url:
 *         NAME   :: HTTP "://" DOMAIN [PORT] ["/" [URI] [QUERY] ]
 *         HTTP   :: "http" [ "s" ]
 *         DOMAIN :: e.g., "mybucket.host.org"
 *         PORT   :: ":" <number> (e.g., ":9000" )
 *         URI    :: <string> (e.g., "path/to/resource.hd5" )
 *         QUERY  :: "?" <string> (e.g., "arg1=param1&arg2=param2")
 *
 * Return:
 *
 *     Success: A pointer to a new file data structure.
 *              The public fields will be initialized by the caller, which is
 *              always H5FD_open().
 *
 *     Failure: NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__ros3_open(const char *url, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_ros3_t     *file = NULL;
    struct tm       *now  = NULL;
    char             iso8601now[ISO8601_SIZE];
    unsigned char    signing_key[SHA256_DIGEST_LENGTH];
    s3r_t           *handle = NULL;
    H5FD_ros3_fapl_t fa;
    H5P_genplist_t  *plist = NULL;
    htri_t           token_exists;
    char            *token;
    H5FD_t          *ret_value = NULL;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_open() called.\n");
#endif

    /* Sanity check on file offsets */
    HDcompile_assert(sizeof(HDoff_t) >= sizeof(size_t));

    /* Check arguments */
    if (!url || !*url)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");
    if (flags != H5F_ACC_RDONLY)
        HGOTO_ERROR(H5E_ARGS, H5E_UNSUPPORTED, NULL, "only Read-Only access allowed");

    if (FAIL == H5Pget_fapl_ros3(fapl_id, &fa))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "can't get property list");

    if (CURLE_OK != curl_global_init(CURL_GLOBAL_DEFAULT))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "unable to initialize curl global (placeholder flags)");

    /* Session/security token */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, NULL, "not a file access property list");
    if ((token_exists = H5P_exist_plist(plist, ROS3_TOKEN_PROP_NAME)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "failed to check if property token exists in plist");
    if (token_exists) {
        if (H5P_get(plist, ROS3_TOKEN_PROP_NAME, &token) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "unable to get token value");
    }

    /* open file; procedure depends on whether or not the fapl instructs to
     * authenticate requests or not.
     */
    if (fa.authenticate == true) {
        /* compute signing key (part of AWS/S3 REST API)
         * can be re-used by user/key for 7 days after creation.
         * find way to reuse/share
         */
        now = gmnow();
        assert(now != NULL);
        if (ISO8601NOW(iso8601now, now) != (ISO8601_SIZE - 1))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "problem while writing iso8601 timestamp");
        if (FAIL == H5FD_s3comms_signing_key(signing_key, (const char *)fa.secret_key,
                                             (const char *)fa.aws_region, (const char *)iso8601now))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "problem while computing signing key");

        if (token_exists)
            handle = H5FD_s3comms_s3r_open(url, (const char *)fa.aws_region, (const char *)fa.secret_id,
                                           (const unsigned char *)signing_key, (const char *)token);
        else
            handle = H5FD_s3comms_s3r_open(url, (const char *)fa.aws_region, (const char *)fa.secret_id,
                                           (const unsigned char *)signing_key, "");
    }
    else
        handle = H5FD_s3comms_s3r_open(url, NULL, NULL, NULL, NULL);

    if (handle == NULL)
        /* If we want to check CURL's say on the matter in a controlled
         * fashion, this is the place to do it, but would need to make a
         * few minor changes to s3comms `s3r_t` and `s3r_read()`.
         */
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "could not open");

    /* create new file struct */
    file = H5FL_CALLOC(H5FD_ros3_t);
    if (file == NULL)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct");

    file->s3r_handle = handle;
    H5MM_memcpy(&(file->fa), &fa, sizeof(H5FD_ros3_fapl_t));

#if ROS3_STATS
    if (FAIL == ros3_reset_stats(file))
        HGOTO_ERROR(H5E_INTERNAL, H5E_UNINITIALIZED, NULL, "unable to reset file statistics");
#endif /* ROS3_STATS */

    /* Cache the initial bytes of the file */
    {
        size_t filesize = H5FD_s3comms_s3r_get_filesize(file->s3r_handle);

        file->cache_size = (filesize < ROS3_MAX_CACHE_SIZE) ? filesize : ROS3_MAX_CACHE_SIZE;

        if (NULL == (file->cache = (uint8_t *)H5MM_calloc(file->cache_size)))
            HGOTO_ERROR(H5E_VFL, H5E_NOSPACE, NULL, "unable to allocate cache memory");
        if (H5FD_s3comms_s3r_read(file->s3r_handle, 0, file->cache_size, file->cache) == FAIL)
            HGOTO_ERROR(H5E_VFL, H5E_READERROR, NULL, "unable to execute read");
    }

    ret_value = (H5FD_t *)file;

done:
    if (ret_value == NULL) {
        if (handle != NULL)
            if (FAIL == H5FD_s3comms_s3r_close(handle))
                HDONE_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, NULL, "unable to close s3 file handle");
        if (file != NULL) {
            H5MM_xfree(file->cache);
            file = H5FL_FREE(H5FD_ros3_t, file);
        }
        curl_global_cleanup(); /* early cleanup because open failed */
    }                          /* end if null return value (error) */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_open() */

#if ROS3_STATS
/*----------------------------------------------------------------------------
 *
 * Function: ros3_fprint_stats()
 *
 * Purpose:
 *
 *     Tabulate and pretty-print statistics for this virtual file.
 *
 *     Should be called upon file close.
 *
 *     Shows number of reads and bytes read, broken down by
 *     "raw" (H5FD_MEM_DRAW)
 *     or "meta" (any other flag)
 *
 *     Prints filename and listing of total number of reads and bytes read,
 *     both as a grand total and separate  meta- and raw data reads.
 *
 *     If any reads were done, prints out two tables:
 *
 *     1. overview of raw- and metadata reads
 *         - min (smallest size read)
 *         - average of size read
 *             - k,M,G suffixes by powers of 1024 (2^10)
 *         - max (largest size read)
 *     2. tabulation of "bins", sepraring reads into exponentially-larger
 *        ranges of size.
 *         - columns for number of reads, total bytes, and average size, with
 *           separate sub-colums for raw- and metadata reads.
 *         - each row represents one bin, identified by the top of its range
 *
 *     Bin ranges can be modified with pound-defines at the top of this file.
 *
 *     Bins without any reads in their bounds are not printed.
 *
 *     An "overflow" bin is also present, to catch "big" reads.
 *
 *     Output for all bins (and range ceiling and average size report)
 *     is divied by powers of 1024. By corollary, four digits before the decimal
 *     is valid.
 *
 *     - 41080 bytes is represented by 40.177k, not 41.080k
 *     - 1004.831M represents approx. 1052642000 bytes
 *
 * Return:
 *
 *     - SUCCESS: `SUCCEED`
 *     - FAILURE: `FAIL`
 *         - occurs if the file passed in is invalid
 *         - TODO: if stream is invalid? how can we check this?
 *
 *----------------------------------------------------------------------------
 */
static herr_t
ros3_fprint_stats(FILE *stream, const H5FD_ros3_t *file)
{
    herr_t             ret_value    = SUCCEED;
    parsed_url_t      *purl         = NULL;
    unsigned           i            = 0;
    unsigned long      count_meta   = 0;
    unsigned long      count_raw    = 0;
    double             average_meta = 0.0;
    double             average_raw  = 0.0;
    unsigned long long min_meta     = (unsigned long long)ROS3_STATS_STARTING_MIN;
    unsigned long long min_raw      = (unsigned long long)ROS3_STATS_STARTING_MIN;
    unsigned long long max_meta     = 0;
    unsigned long long max_raw      = 0;
    unsigned long long bytes_raw    = 0;
    unsigned long long bytes_meta   = 0;
    double             re_dub       = 0.0; /* reusable double variable */
    unsigned           suffix_i     = 0;
    const char         suffixes[]   = {' ', 'K', 'M', 'G', 'T', 'P'};

    FUNC_ENTER_PACKAGE

    if (stream == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file stream cannot be null");
    if (file == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file cannot be null");
    if (file->s3r_handle == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "s3 request handle cannot be null");
    if (file->s3r_handle->purl == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "parsed url structure cannot be null");
    purl = file->s3r_handle->purl;

    /******************
     * PRINT FILENAME *
     ******************/

    fprintf(stream, "stats for %s://%s", purl->scheme, purl->host);
    if (purl->port != NULL && purl->port[0] != '\0')
        fprintf(stream, ":%s", purl->port);
    if (purl->query != NULL && purl->query[0] != '\0') {
        if (purl->path != NULL && purl->path[0] != '\0')
            fprintf(stream, "/%s", purl->path);
        else
            fprintf(stream, "/");
        fprintf(stream, "?%s", purl->query);
    }
    else if (purl->path != NULL && purl->path[0] != '\0') {
        fprintf(stream, "/%s", purl->path);
    }
    fprintf(stream, "\n");

    /*******************
     * AGGREGATE STATS *
     *******************/

    for (i = 0; i <= ROS3_STATS_BIN_COUNT; i++) {
        const ros3_statsbin *r = &file->raw[i];
        const ros3_statsbin *m = &file->meta[i];

        if (m->min < min_meta)
            min_meta = m->min;
        if (r->min < min_raw)
            min_raw = r->min;
        if (m->max > max_meta)
            max_meta = m->max;
        if (r->max > max_raw)
            max_raw = r->max;

        count_raw += r->count;
        count_meta += m->count;
        bytes_raw += r->bytes;
        bytes_meta += m->bytes;
    }
    if (count_raw > 0)
        average_raw = (double)bytes_raw / (double)count_raw;
    if (count_meta > 0)
        average_meta = (double)bytes_meta / (double)count_meta;

    /******************
     * PRINT OVERVIEW *
     ******************/

    fprintf(stream, "TOTAL READS: %llu  (%llu meta, %llu raw)\n", count_raw + count_meta, count_meta,
            count_raw);
    fprintf(stream, "TOTAL BYTES: %llu  (%llu meta, %llu raw)\n", bytes_raw + bytes_meta, bytes_meta,
            bytes_raw);

    if (count_raw + count_meta == 0)
        goto done;

    /*************************
     * PRINT AGGREGATE STATS *
     *************************/

    fprintf(stream, "SIZES     meta      raw\n");
    fprintf(stream, "  min ");
    if (count_meta == 0)
        fprintf(stream, "   0.000  ");
    else {
        re_dub = (double)min_meta;
        for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
            re_dub /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        fprintf(stream, "%8.3lf%c ", re_dub, suffixes[suffix_i]);
    }

    if (count_raw == 0)
        fprintf(stream, "   0.000 \n");
    else {
        re_dub = (double)min_raw;
        for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
            re_dub /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        fprintf(stream, "%8.3lf%c\n", re_dub, suffixes[suffix_i]);
    }

    fprintf(stream, "  avg ");
    re_dub = (double)average_meta;
    for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
        re_dub /= 1024.0;
    assert(suffix_i < sizeof(suffixes));
    fprintf(stream, "%8.3lf%c ", re_dub, suffixes[suffix_i]);

    re_dub = (double)average_raw;
    for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
        re_dub /= 1024.0;
    assert(suffix_i < sizeof(suffixes));
    fprintf(stream, "%8.3lf%c\n", re_dub, suffixes[suffix_i]);

    fprintf(stream, "  max ");
    re_dub = (double)max_meta;
    for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
        re_dub /= 1024.0;
    assert(suffix_i < sizeof(suffixes));
    fprintf(stream, "%8.3lf%c ", re_dub, suffixes[suffix_i]);

    re_dub = (double)max_raw;
    for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
        re_dub /= 1024.0;
    assert(suffix_i < sizeof(suffixes));
    fprintf(stream, "%8.3lf%c\n", re_dub, suffixes[suffix_i]);

    /******************************
     * PRINT INDIVIDUAL BIN STATS *
     ******************************/

    fprintf(stream, "BINS             # of reads      total bytes         average size\n");
    fprintf(stream, "    up-to      meta     raw     meta      raw       meta      raw\n");

    for (i = 0; i <= ROS3_STATS_BIN_COUNT; i++) {
        const ros3_statsbin *m;
        const ros3_statsbin *r;
        unsigned long long   range_end = 0;
        char                 bm_suffix = ' '; /* bytes-meta */
        double               bm_val    = 0.0;
        char                 br_suffix = ' '; /* bytes-raw */
        double               br_val    = 0.0;
        char                 am_suffix = ' '; /* average-meta */
        double               am_val    = 0.0;
        char                 ar_suffix = ' '; /* average-raw */
        double               ar_val    = 0.0;

        m = &file->meta[i];
        r = &file->raw[i];
        if (r->count == 0 && m->count == 0)
            continue;

        range_end = ros3_stats_boundaries[i];

        if (i == ROS3_STATS_BIN_COUNT) {
            range_end = ros3_stats_boundaries[i - 1];
            fprintf(stream, ">");
        }
        else
            fprintf(stream, " ");

        bm_val = (double)m->bytes;
        for (suffix_i = 0; bm_val >= 1024.0; suffix_i++)
            bm_val /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        bm_suffix = suffixes[suffix_i];

        br_val = (double)r->bytes;
        for (suffix_i = 0; br_val >= 1024.0; suffix_i++)
            br_val /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        br_suffix = suffixes[suffix_i];

        if (m->count > 0)
            am_val = (double)(m->bytes) / (double)(m->count);
        for (suffix_i = 0; am_val >= 1024.0; suffix_i++)
            am_val /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        am_suffix = suffixes[suffix_i];

        if (r->count > 0)
            ar_val = (double)(r->bytes) / (double)(r->count);
        for (suffix_i = 0; ar_val >= 1024.0; suffix_i++)
            ar_val /= 1024.0;
        assert(suffix_i < sizeof(suffixes));
        ar_suffix = suffixes[suffix_i];

        re_dub = (double)range_end;
        for (suffix_i = 0; re_dub >= 1024.0; suffix_i++)
            re_dub /= 1024.0;
        assert(suffix_i < sizeof(suffixes));

        fprintf(stream, " %8.3f%c %7d %7d %8.3f%c %8.3f%c %8.3f%c %8.3f%c\n", re_dub,
                suffixes[suffix_i], /* bin ceiling      */
                m->count,           /* metadata reads   */
                r->count,           /* raw data reads    */
                bm_val, bm_suffix,  /* metadata bytes   */
                br_val, br_suffix,  /* raw data bytes    */
                am_val, am_suffix,  /* metadata average */
                ar_val, ar_suffix); /* raw data average  */

        fflush(stream);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)

} /* ros3_fprint_stats */
#endif /* ROS3_STATS */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_close()
 *
 * Purpose:
 *
 *     Close an HDF5 file.
 *
 * Return:
 *
 *     SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_close(H5FD_t H5_ATTR_UNUSED *_file)
{
    H5FD_ros3_t *file      = (H5FD_ros3_t *)_file;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_close() called.\n");
#endif

    /* Sanity checks */
    assert(file != NULL);
    assert(file->s3r_handle != NULL);

    /* Close the underlying request handle
     */
    if (FAIL == H5FD_s3comms_s3r_close(file->s3r_handle))
        HGOTO_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "unable to close S3 request handle");

#if ROS3_STATS
    /* TODO: mechanism to re-target stats printout */
    if (ros3_fprint_stats(stdout, file) == FAIL)
        HGOTO_ERROR(H5E_INTERNAL, H5E_ERROR, FAIL, "problem while writing file statistics");
#endif /* ROS3_STATS */

    /* Release the file info */
    H5MM_xfree(file->cache);
    file = H5FL_FREE(H5FD_ros3_t, file);

done:
    curl_global_cleanup(); /* cleanup to answer init on open */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_close() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_cmp()
 *
 * Purpose:
 *
 *     Compares two files belonging to this driver using an arbitrary
 *     (but consistent) ordering:
 *
 *     + url scheme
 *     + url host
 *     + url port
 *     + url path
 *     + url query
 *     + fapl aws_region
 *     + fapl secret_id
 *     + fapl secret_key
 *
 *     tl;dr -> check URL, check crentials
 *
 * Return:
 *
 *     - Equivalent:      0
 *     - Not Equivalent: -1
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__ros3_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_ros3_t  *f1        = (const H5FD_ros3_t *)_f1;
    const H5FD_ros3_t  *f2        = (const H5FD_ros3_t *)_f2;
    const parsed_url_t *purl1     = NULL;
    const parsed_url_t *purl2     = NULL;
    int                 ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_cmp() called.\n");
#endif

    assert(f1->s3r_handle != NULL);
    assert(f2->s3r_handle != NULL);

    purl1 = (const parsed_url_t *)f1->s3r_handle->purl;
    purl2 = (const parsed_url_t *)f2->s3r_handle->purl;
    assert(purl1 != NULL);
    assert(purl2 != NULL);
    assert(purl1->scheme != NULL);
    assert(purl2->scheme != NULL);
    assert(purl1->host != NULL);
    assert(purl2->host != NULL);

    /* URL: SCHEME */
    if (strcmp(purl1->scheme, purl2->scheme))
        HGOTO_DONE(-1);

    /* URL: HOST */
    if (strcmp(purl1->host, purl2->host))
        HGOTO_DONE(-1);

    /* URL: PORT */
    if (purl1->port && purl2->port) {
        if (strcmp(purl1->port, purl2->port))
            HGOTO_DONE(-1);
    }
    else if (purl1->port)
        HGOTO_DONE(-1);
    else if (purl2->port)
        HGOTO_DONE(-1);

    /* URL: PATH */
    if (purl1->path && purl2->path) {
        if (strcmp(purl1->path, purl2->path))
            HGOTO_DONE(-1);
    }
    else if (purl1->path && !purl2->path)
        HGOTO_DONE(-1);
    else if (purl2->path && !purl1->path)
        HGOTO_DONE(-1);

    /* URL: QUERY */
    if (purl1->query && purl2->query) {
        if (strcmp(purl1->query, purl2->query))
            HGOTO_DONE(-1);
    }
    else if (purl1->query && !purl2->query)
        HGOTO_DONE(-1);
    else if (purl2->query && !purl1->query)
        HGOTO_DONE(-1);

    /* FAPL: AWS_REGION */
    if (f1->fa.aws_region[0] != '\0' && f2->fa.aws_region[0] != '\0') {
        if (strcmp(f1->fa.aws_region, f2->fa.aws_region))
            HGOTO_DONE(-1);
    }
    else if (f1->fa.aws_region[0] != '\0')
        HGOTO_DONE(-1);
    else if (f2->fa.aws_region[0] != '\0')
        HGOTO_DONE(-1);

    /* FAPL: SECRET_ID */
    if (f1->fa.secret_id[0] != '\0' && f2->fa.secret_id[0] != '\0') {
        if (strcmp(f1->fa.secret_id, f2->fa.secret_id))
            HGOTO_DONE(-1);
    }
    else if (f1->fa.secret_id[0] != '\0')
        HGOTO_DONE(-1);
    else if (f2->fa.secret_id[0] != '\0')
        HGOTO_DONE(-1);

    /* FAPL: SECRET_KEY */
    if (f1->fa.secret_key[0] != '\0' && f2->fa.secret_key[0] != '\0') {
        if (strcmp(f1->fa.secret_key, f2->fa.secret_key))
            HGOTO_DONE(-1);
    }
    else if (f1->fa.secret_key[0] != '\0')
        HGOTO_DONE(-1);
    else if (f2->fa.secret_key[0] != '\0')
        HGOTO_DONE(-1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__ros3_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 *              Note that since the ROS3 VFD is read only, most flags
 *              are irrelevant.
 *
 *              The term "set" is highly misleading...
 *              stores/copies the supported flags in the out-pointer `flags`.
 *
 * Return:      SUCCEED (Can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_query(const H5FD_t H5_ATTR_UNUSED *_file, unsigned long *flags)
{
    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_query() called.\n");
#endif

    /* Set the VFL feature flags that this driver supports */
    if (flags) {
        *flags = 0;
        /* OK to perform data sieving for faster raw data reads & writes */
        *flags |= H5FD_FEAT_DATA_SIEVE;
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FD__ros3_query() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_get_eoa()
 *
 * Purpose:
 *
 *     Gets the end-of-address marker for the file. The EOA marker
 *     is the first address past the last byte allocated in the
 *     format address space.
 *
 * Return:
 *
 *     The end-of-address marker.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__ros3_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_ros3_t *file = (const H5FD_ros3_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_get_eoa() called.\n");
#endif

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__ros3_get_eoa() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_set_eoa()
 *
 * Purpose:
 *
 *     Set the end-of-address marker for the file.
 *
 * Return:
 *
 *      SUCCEED  (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_ros3_t *file = (H5FD_ros3_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_set_eoa() called.\n");
#endif

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5FD__ros3_set_eoa() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_get_eof()
 *
 * Purpose:
 *
 *     Returns the end-of-file marker.
 *
 * Return:
 *
 *     EOF: the first address past the end of the "file", either the
 *     filesystem file or the HDF5 file.
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__ros3_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_ros3_t *file = (const H5FD_ros3_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_get_eof() called.\n");
#endif

    FUNC_LEAVE_NOAPI(H5FD_s3comms_s3r_get_filesize(file->s3r_handle))
} /* end H5FD__ros3_get_eof() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_get_handle()
 *
 * Purpose:
 *
 *     Returns the S3 Request handle (s3r_t) of ros3 file driver.
 *
 * Returns:
 *
 *     SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_ros3_t *file      = (H5FD_ros3_t *)_file;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_get_handle() called.\n");
#endif

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");

    *file_handle = file->s3r_handle;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_get_handle() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_read()
 *
 * Purpose
 *
 *     Reads SIZE bytes of data from FILE beginning at address ADDR
 *     into buffer BUF according to data transfer properties in DXPL_ID.
 *
 * Return:
 *
 *     Success: `SUCCEED`
 *         - Result is stored in caller-supplied buffer BUF.
 *     Failure: `FAIL`
 *         - Unable to complete read.
 *         - Contents of buffer `buf` are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf)
{
    H5FD_ros3_t *file      = (H5FD_ros3_t *)_file;
    size_t       filesize  = 0;
    herr_t       ret_value = SUCCEED;
#if ROS3_STATS
    /* working variables for storing stats */
    ros3_statsbin *bin   = NULL;
    unsigned       bin_i = 0;
#endif /* ROS3_STATS */

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_read() called.\n");
#endif

    assert(file);
    assert(file->cache);
    assert(file->s3r_handle);
    assert(buf);

    filesize = H5FD_s3comms_s3r_get_filesize(file->s3r_handle);

    if ((addr > filesize) || ((addr + size) > filesize))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "range exceeds file address");

    /* Copy from the cache when accessing the first N bytes of the file.
     * Saves network I/O operations when opening files.
     */
    if (addr + size < file->cache_size) {
        memcpy(buf, file->cache + addr, size);
    }
    else {
        if (H5FD_s3comms_s3r_read(file->s3r_handle, addr, size, buf) == FAIL)
            HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "unable to execute read");

#if ROS3_STATS

        /* Find which "bin" this read fits in. Can be "overflow" bin.  */
        for (bin_i = 0; bin_i < ROS3_STATS_BIN_COUNT; bin_i++)
            if ((unsigned long long)size < ros3_stats_boundaries[bin_i])
                break;
        bin = (type == H5FD_MEM_DRAW) ? &file->raw[bin_i] : &file->meta[bin_i];

        /* Store collected stats in appropriate bin */
        if (bin->count == 0) {
            bin->min = size;
            bin->max = size;
        }
        else {
            if (size < bin->min)
                bin->min = size;
            if (size > bin->max)
                bin->max = size;
        }
        bin->count++;
        bin->bytes += (unsigned long long)size;

#endif /* ROS3_STATS */
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_read() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_write()
 *
 * Purpose:
 *
 *     Write bytes to file.
 *     UNSUPPORTED IN READ-ONLY ROS3 VFD.
 *
 * Return:
 *
 *     FAIL (Not possible with Read-Only S3 file.)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_write(H5FD_t H5_ATTR_UNUSED *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id,
                 haddr_t H5_ATTR_UNUSED addr, size_t H5_ATTR_UNUSED size, const void H5_ATTR_UNUSED *buf)
{
    herr_t ret_value = FAIL;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_write() called.\n");
#endif

    HGOTO_ERROR(H5E_VFL, H5E_UNSUPPORTED, FAIL, "cannot write to read-only file.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD__ros3_write() */

/*-------------------------------------------------------------------------
 *
 * Function: H5FD__ros3_truncate()
 *
 * Purpose:
 *
 *     Makes sure that the true file size is the same (or larger)
 *     than the end-of-address.
 *
 *     NOT POSSIBLE ON READ-ONLY S3 FILES.
 *
 * Return:
 *
 *     FAIL (Not possible on Read-Only S3 files.)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__ros3_truncate(H5FD_t H5_ATTR_UNUSED *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#if ROS3_DEBUG
    fprintf(stdout, "H5FD__ros3_truncate() called.\n");
#endif

    HGOTO_ERROR(H5E_VFL, H5E_UNSUPPORTED, FAIL, "cannot truncate read-only file.");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__ros3_truncate() */

#endif /* H5_HAVE_ROS3_VFD */
