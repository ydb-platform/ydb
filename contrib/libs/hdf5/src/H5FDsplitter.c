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
 * Purpose:     The Splitter VFD implements a file driver which relays all the
 *              VFD calls to an underlying VFD, and send all the write calls to
 *              another underlying VFD. Maintains two files simultaneously.
 */

/* This source code file is part of the H5FD driver module */
#include "H5FDdrvr_module.h"

#include "H5private.h"    /* Generic Functions        */
#include "H5Eprivate.h"   /* Error handling           */
#include "H5Fprivate.h"   /* File access              */
#include "H5FDprivate.h"  /* File drivers             */
#include "H5FDsplitter.h" /* Splitter file driver     */
#include "H5FLprivate.h"  /* Free Lists               */
#include "H5Iprivate.h"   /* IDs                      */
#include "H5MMprivate.h"  /* Memory management        */
#include "H5Pprivate.h"   /* Property lists           */

/* The driver identification number, initialized at runtime */
static hid_t H5FD_SPLITTER_g = 0;

/* Driver-specific file access properties */
typedef struct H5FD_splitter_fapl_t {
    hid_t rw_fapl_id;                                /* fapl for the R/W channel       */
    hid_t wo_fapl_id;                                /* fapl for the W/O channel       */
    char  wo_path[H5FD_SPLITTER_PATH_MAX + 1];       /* file name for the W/O channel */
    char  log_file_path[H5FD_SPLITTER_PATH_MAX + 1]; /* file to record errors reported by the W/O channel */
    bool  ignore_wo_errs;                            /* true to ignore errors on the W/O channel */
} H5FD_splitter_fapl_t;

/* The information of this splitter */
typedef struct H5FD_splitter_t {
    H5FD_t               pub;     /* public stuff, must be first    */
    unsigned             version; /* version of the H5FD_splitter_vfd_config_t structure used */
    H5FD_splitter_fapl_t fa;      /* driver-specific file access properties */
    H5FD_t              *rw_file; /* pointer of R/W channel */
    H5FD_t              *wo_file; /* pointer of W/O channel */
    FILE                *logfp;   /* Log file pointer */
} H5FD_splitter_t;

/*
 * These macros check for overflow of various quantities.  These macros
 * assume that HDoff_t is signed and haddr_t and size_t are unsigned.
 *
 * ADDR_OVERFLOW:   Checks whether a file address of type `haddr_t'
 *                  is too large to be represented by the second argument
 *                  of the file seek function.
 *
 * SIZE_OVERFLOW:   Checks whether a buffer size of type `hsize_t' is too
 *                  large to be represented by the `size_t' type.
 *
 * REGION_OVERFLOW: Checks whether an address and size pair describe data
 *                  which can be addressed entirely by the second
 *                  argument of the file seek function.
 */
#define MAXADDR          (((haddr_t)1 << (8 * sizeof(HDoff_t) - 1)) - 1)
#define ADDR_OVERFLOW(A) (HADDR_UNDEF == (A) || ((A) & ~(haddr_t)MAXADDR))
#define SIZE_OVERFLOW(Z) ((Z) & ~(hsize_t)MAXADDR)
#define REGION_OVERFLOW(A, Z)                                                                                \
    (ADDR_OVERFLOW(A) || SIZE_OVERFLOW(Z) || HADDR_UNDEF == (A) + (Z) || (HDoff_t)((A) + (Z)) < (HDoff_t)(A))

/* This macro provides a wrapper for shared fail-log-ignore behavior
 * for errors arising in the splitter's W/O channel.
 * Logs an error entry in a log file, if the file exists.
 * If not set to ignore errors, registers an error with the library.
 */
#define H5FD_SPLITTER_WO_ERROR(file, funcname, errmajor, errminor, ret, mesg)                                \
    {                                                                                                        \
        H5FD__splitter_log_error((file), (funcname), (mesg));                                                \
        if (false == (file)->fa.ignore_wo_errs)                                                              \
            HGOTO_ERROR((errmajor), (errminor), (ret), (mesg));                                              \
    }

#define H5FD_SPLITTER_DEBUG_OP_CALLS 0 /* debugging print toggle; 0 disables */

#if H5FD_SPLITTER_DEBUG_OP_CALLS
#define H5FD_SPLITTER_LOG_CALL(name)                                                                         \
    do {                                                                                                     \
        printf("called %s()\n", (name));                                                                     \
        fflush(stdout);                                                                                      \
    } while (0)
#else
#define H5FD_SPLITTER_LOG_CALL(name) /* no-op */
#endif                               /* H5FD_SPLITTER_DEBUG_OP_CALLS */

/* Private functions */

/* Print error messages from W/O channel to log file */
static herr_t H5FD__splitter_log_error(const H5FD_splitter_t *file, const char *atfunc, const char *msg);
static int    H5FD__copy_plist(hid_t fapl_id, hid_t *id_out_ptr);

/* Prototypes */
static herr_t  H5FD__splitter_term(void);
static herr_t  H5FD__splitter_populate_config(H5FD_splitter_vfd_config_t *vfd_config,
                                              H5FD_splitter_fapl_t       *fapl_out);
static herr_t  H5FD__splitter_get_default_wo_path(char *new_path, size_t new_path_len,
                                                  const char *base_filename);
static hsize_t H5FD__splitter_sb_size(H5FD_t *_file);
static herr_t  H5FD__splitter_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/);
static herr_t  H5FD__splitter_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf);
static void   *H5FD__splitter_fapl_get(H5FD_t *_file);
static void   *H5FD__splitter_fapl_copy(const void *_old_fa);
static herr_t  H5FD__splitter_fapl_free(void *_fapl);
static H5FD_t *H5FD__splitter_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__splitter_close(H5FD_t *_file);
static int     H5FD__splitter_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__splitter_query(const H5FD_t *_file, unsigned long *flags /* out */);
static herr_t  H5FD__splitter_get_type_map(const H5FD_t *_file, H5FD_mem_t *type_map);
static haddr_t H5FD__splitter_alloc(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
static herr_t  H5FD__splitter_free(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size);
static haddr_t H5FD__splitter_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type);
static herr_t  H5FD__splitter_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr);
static haddr_t H5FD__splitter_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type);
static herr_t  H5FD__splitter_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle);
static herr_t  H5FD__splitter_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                   void *buf);
static herr_t  H5FD__splitter_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                    const void *buf);
static herr_t  H5FD__splitter_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__splitter_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__splitter_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__splitter_unlock(H5FD_t *_file);
static herr_t  H5FD__splitter_delete(const char *filename, hid_t fapl_id);
static herr_t  H5FD__splitter_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input,
                                  void **output);

static const H5FD_class_t H5FD_splitter_g = {
    H5FD_CLASS_VERSION,           /* struct version       */
    H5FD_SPLITTER_VALUE,          /* value                */
    "splitter",                   /* name                 */
    MAXADDR,                      /* maxaddr              */
    H5F_CLOSE_WEAK,               /* fc_degree            */
    H5FD__splitter_term,          /* terminate            */
    H5FD__splitter_sb_size,       /* sb_size              */
    H5FD__splitter_sb_encode,     /* sb_encode            */
    H5FD__splitter_sb_decode,     /* sb_decode            */
    sizeof(H5FD_splitter_fapl_t), /* fapl_size            */
    H5FD__splitter_fapl_get,      /* fapl_get             */
    H5FD__splitter_fapl_copy,     /* fapl_copy            */
    H5FD__splitter_fapl_free,     /* fapl_free            */
    0,                            /* dxpl_size            */
    NULL,                         /* dxpl_copy            */
    NULL,                         /* dxpl_free            */
    H5FD__splitter_open,          /* open                 */
    H5FD__splitter_close,         /* close                */
    H5FD__splitter_cmp,           /* cmp                  */
    H5FD__splitter_query,         /* query                */
    H5FD__splitter_get_type_map,  /* get_type_map         */
    H5FD__splitter_alloc,         /* alloc                */
    H5FD__splitter_free,          /* free                 */
    H5FD__splitter_get_eoa,       /* get_eoa              */
    H5FD__splitter_set_eoa,       /* set_eoa              */
    H5FD__splitter_get_eof,       /* get_eof              */
    H5FD__splitter_get_handle,    /* get_handle           */
    H5FD__splitter_read,          /* read                 */
    H5FD__splitter_write,         /* write                */
    NULL,                         /* read_vector          */
    NULL,                         /* write_vector         */
    NULL,                         /* read_selection       */
    NULL,                         /* write_selection      */
    H5FD__splitter_flush,         /* flush                */
    H5FD__splitter_truncate,      /* truncate             */
    H5FD__splitter_lock,          /* lock                 */
    H5FD__splitter_unlock,        /* unlock               */
    H5FD__splitter_delete,        /* del                  */
    H5FD__splitter_ctl,           /* ctl                  */
    H5FD_FLMAP_DICHOTOMY          /* fl_map               */
};

/* Declare a free list to manage the H5FD_splitter_t struct */
H5FL_DEFINE_STATIC(H5FD_splitter_t);

/* Declare a free list to manage the H5FD_splitter_fapl_t struct */
H5FL_DEFINE_STATIC(H5FD_splitter_fapl_t);

/*-------------------------------------------------------------------------
 * Function:    H5FD_splitter_init
 *
 * Purpose:     Initialize the splitter driver by registering it with the
 *              library.
 *
 * Return:      Success:    The driver ID for the splitter driver.
 *              Failure:    Negative
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_splitter_init(void)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_NOAPI_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    if (H5I_VFL != H5I_get_type(H5FD_SPLITTER_g))
        H5FD_SPLITTER_g = H5FDregister(&H5FD_splitter_g);

    ret_value = H5FD_SPLITTER_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_splitter_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__splitter_term
 *
 * Purpose:     Shut down the splitter VFD.
 *
 * Returns:     SUCCEED (Can't fail)
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Reset VFL ID */
    H5FD_SPLITTER_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__splitter_term() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__copy_plist
 *
 * Purpose:     Sanity-wrapped H5P_copy_plist() for each channel.
 *              Utility function for operation in multiple locations.
 *
 * Return:      0 on success, -1 on error.
 *-------------------------------------------------------------------------
 */
static int
H5FD__copy_plist(hid_t fapl_id, hid_t *id_out_ptr)
{
    int             ret_value = 0;
    H5P_genplist_t *plist_ptr = NULL;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(id_out_ptr != NULL);

    if (false == H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, -1, "not a file access property list");

    plist_ptr = (H5P_genplist_t *)H5I_object(fapl_id);
    if (NULL == plist_ptr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, -1, "unable to get property list");

    *id_out_ptr = H5P_copy_plist(plist_ptr, false);
    if (H5I_INVALID_HID == *id_out_ptr)
        HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, -1, "unable to copy file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__copy_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_splitter
 *
 * Purpose:     Sets the file access property list to use the
 *              splitter driver.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_splitter(hid_t fapl_id, H5FD_splitter_vfd_config_t *vfd_config)
{
    H5FD_splitter_fapl_t *info      = NULL;
    H5P_genplist_t       *plist_ptr = NULL;
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*#", fapl_id, vfd_config);

    H5FD_SPLITTER_LOG_CALL(__func__);

    if (H5FD_SPLITTER_MAGIC != vfd_config->magic)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid configuration (magic number mismatch)");
    if (H5FD_CURR_SPLITTER_VFD_CONFIG_VERSION != vfd_config->version)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid config (version number mismatch)");
    if (NULL == (plist_ptr = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a valid property list");

    info = H5FL_CALLOC(H5FD_splitter_fapl_t);
    if (NULL == info)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate file access property list struct");

    if (H5FD__splitter_populate_config(vfd_config, info) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't setup driver configuration");

    ret_value = H5P_set_driver(plist_ptr, H5FD_SPLITTER, info, NULL);

done:
    if (info)
        info = H5FL_FREE(H5FD_splitter_fapl_t, info);

    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fapl_splitter() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_splitter
 *
 * Purpose:     Returns information about the splitter file access property
 *              list through the structure config.
 *
 *              Will fail if config is received without pre-set valid
 *              magic and version information.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_splitter(hid_t fapl_id, H5FD_splitter_vfd_config_t *config /*out*/)
{
    const H5FD_splitter_fapl_t *fapl_ptr     = NULL;
    H5FD_splitter_fapl_t       *default_fapl = NULL;
    H5P_genplist_t             *plist_ptr    = NULL;
    herr_t                      ret_value    = SUCCEED;

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, config);

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (config == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "config pointer is null");
    if (H5FD_SPLITTER_MAGIC != config->magic)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "info-out pointer invalid (magic number mismatch)");
    if (H5FD_CURR_SPLITTER_VFD_CONFIG_VERSION != config->version)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "info-out pointer invalid (version unsafe)");

    /* Pre-set out FAPL IDs with intent to replace these values */
    config->rw_fapl_id = H5I_INVALID_HID;
    config->wo_fapl_id = H5I_INVALID_HID;

    /* Check and get the splitter fapl */
    if (NULL == (plist_ptr = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5FD_SPLITTER != H5P_peek_driver(plist_ptr))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    fapl_ptr = (const H5FD_splitter_fapl_t *)H5P_peek_driver_info(plist_ptr);
    if (NULL == fapl_ptr) {
        if (NULL == (default_fapl = H5FL_CALLOC(H5FD_splitter_fapl_t)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate file access property list struct");
        if (H5FD__splitter_populate_config(NULL, default_fapl) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't initialize driver configuration info");
        fapl_ptr = default_fapl;
    }

    strncpy(config->wo_path, fapl_ptr->wo_path, H5FD_SPLITTER_PATH_MAX + 1);
    strncpy(config->log_file_path, fapl_ptr->log_file_path, H5FD_SPLITTER_PATH_MAX + 1);
    config->ignore_wo_errs = fapl_ptr->ignore_wo_errs;

    /* Copy R/W and W/O FAPLs */
    if (H5FD__copy_plist(fapl_ptr->rw_fapl_id, &(config->rw_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't copy R/W FAPL");
    if (H5FD__copy_plist(fapl_ptr->wo_fapl_id, &(config->wo_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't copy W/O FAPL");

done:
    if (default_fapl)
        H5FL_FREE(H5FD_splitter_fapl_t, default_fapl);

    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_splitter() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_populate_config
 *
 * Purpose:    Populates a H5FD_splitter_fapl_t structure with the provided
 *             values, supplying defaults where values are not provided.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_populate_config(H5FD_splitter_vfd_config_t *vfd_config, H5FD_splitter_fapl_t *fapl_out)
{
    H5P_genplist_t *def_plist;
    H5P_genplist_t *plist;
    bool            free_config = false;
    herr_t          ret_value   = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(fapl_out);

    memset(fapl_out, 0, sizeof(H5FD_splitter_fapl_t));

    if (!vfd_config) {
        vfd_config = H5MM_calloc(sizeof(H5FD_splitter_vfd_config_t));
        if (NULL == vfd_config)
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate file access property list struct");

        vfd_config->magic      = H5FD_SPLITTER_MAGIC;
        vfd_config->version    = H5FD_CURR_SPLITTER_VFD_CONFIG_VERSION;
        vfd_config->rw_fapl_id = H5P_DEFAULT;
        vfd_config->wo_fapl_id = H5P_DEFAULT;

        free_config = true;
    }

    /* Make sure that the W/O channel supports write-only capability.
     * Some drivers (e.g. family or multi) do revision of the superblock
     * in-memory, causing problems in that channel.
     * Uses the feature flag H5FD_FEAT_DEFAULT_VFD_COMPATIBLE as the
     * determining attribute.
     */
    if (H5P_DEFAULT != vfd_config->wo_fapl_id) {
        H5FD_class_t      *wo_driver = NULL;
        H5FD_driver_prop_t wo_driver_prop;
        H5P_genplist_t    *wo_plist_ptr    = NULL;
        unsigned long      wo_driver_flags = 0;

        wo_plist_ptr = (H5P_genplist_t *)H5I_object(vfd_config->wo_fapl_id);
        if (NULL == wo_plist_ptr)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
        if (H5P_peek(wo_plist_ptr, H5F_ACS_FILE_DRV_NAME, &wo_driver_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get driver ID & info");
        wo_driver = (H5FD_class_t *)H5I_object(wo_driver_prop.driver_id);
        if (NULL == wo_driver)
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "invalid driver ID in file access property list");
        if (H5FD_driver_query(wo_driver, &wo_driver_flags) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't query VFD flags");
        if (0 == (H5FD_FEAT_DEFAULT_VFD_COMPATIBLE & wo_driver_flags))
            HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "unsuitable W/O driver");
    } /* end if W/O VFD is non-default */

    fapl_out->ignore_wo_errs = vfd_config->ignore_wo_errs;
    strncpy(fapl_out->wo_path, vfd_config->wo_path, H5FD_SPLITTER_PATH_MAX + 1);
    fapl_out->wo_path[H5FD_SPLITTER_PATH_MAX] = '\0';
    strncpy(fapl_out->log_file_path, vfd_config->log_file_path, H5FD_SPLITTER_PATH_MAX + 1);
    fapl_out->log_file_path[H5FD_SPLITTER_PATH_MAX] = '\0';
    fapl_out->rw_fapl_id                            = H5P_FILE_ACCESS_DEFAULT; /* pre-set value */
    fapl_out->wo_fapl_id                            = H5P_FILE_ACCESS_DEFAULT; /* pre-set value */

    if (NULL == (def_plist = (H5P_genplist_t *)H5I_object(H5P_FILE_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Set non-default channel FAPL IDs in splitter configuration info */
    if (H5P_DEFAULT != vfd_config->rw_fapl_id) {
        if (false == H5P_isa_class(vfd_config->rw_fapl_id, H5P_FILE_ACCESS))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");
        fapl_out->rw_fapl_id = vfd_config->rw_fapl_id;
    }
    else {
        /* Use copy of default file access property list for R/W channel FAPL ID.
         * The Sec2 driver is explicitly set on the FAPL ID, as the default
         * driver might have been replaced with the Splitter VFD, which
         * would cause recursion badness.
         */
        if ((fapl_out->rw_fapl_id = H5P_copy_plist(def_plist, false)) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTCOPY, FAIL, "can't copy property list");
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_out->rw_fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
        if (H5P_set_driver_by_value(plist, H5_VFD_SEC2, NULL, true) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't set default driver on R/W channel FAPL");
    }
    if (H5P_DEFAULT != vfd_config->wo_fapl_id) {
        if (false == H5P_isa_class(vfd_config->wo_fapl_id, H5P_FILE_ACCESS))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");
        fapl_out->wo_fapl_id = vfd_config->wo_fapl_id;
    }
    else {
        /* Use copy of default file access property list for W/O channel FAPL ID.
         * The Sec2 driver is explicitly set on the FAPL ID, as the default
         * driver might have been replaced with the Splitter VFD, which
         * would cause recursion badness.
         */
        if ((fapl_out->wo_fapl_id = H5P_copy_plist(def_plist, false)) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTCOPY, FAIL, "can't copy property list");
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_out->wo_fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
        if (H5P_set_driver_by_value(plist, H5_VFD_SEC2, NULL, true) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't set default driver on R/W channel FAPL");
    }

done:
    if (free_config && vfd_config)
        H5MM_free(vfd_config);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_populate_config() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_get_default_wo_path
 *
 * Purpose:     Given a base filename, returns a default filename for the
 *              W/O channel file by appending '_wo' to the base file name.
 *
 * Return:      Non-negative on Success/Negative on Failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_get_default_wo_path(char *new_path, size_t new_path_len, const char *base_filename)
{
    const char *suffix           = "_wo";
    size_t      old_filename_len = 0;
    char       *file_extension   = NULL;
    herr_t      ret_value        = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(new_path);
    assert(base_filename);

    /* Check that output buffer can hold base filename + `_wo` suffix */
    old_filename_len = strlen(base_filename);
    if (old_filename_len > H5FD_SPLITTER_PATH_MAX - strlen(suffix) - 1)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "filename exceeds max length");

    /* Determine if filename contains a ".h5" extension. */
    if ((file_extension = strstr(base_filename, ".h5"))) {
        /* Insert the suffix between the filename and ".h5" extension. */
        strcpy(new_path, base_filename);
        file_extension = strstr(new_path, ".h5");
        sprintf(file_extension, "%s%s", suffix, ".h5");
    }
    else if ((file_extension = strrchr(base_filename, '.'))) {
        char *new_extension_loc = NULL;

        /* If the filename doesn't contain a ".h5" extension, but contains
         * AN extension, just insert the suffix before that extension.
         */
        strcpy(new_path, base_filename);
        new_extension_loc = strrchr(new_path, '.');
        sprintf(new_extension_loc, "%s%s", suffix, file_extension);
    }
    else {
        /* If the filename doesn't contain an extension at all, just insert
         * the suffix at the end of the filename.
         */
        snprintf(new_path, new_path_len, "%s%s", base_filename, suffix);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_get_default_wo_path() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_flush
 *
 * Purpose:     Flushes all data to disk for both channels.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_flush(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool closing)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Public API for dxpl "context" */
    if (H5FDflush(file->rw_file, dxpl_id, closing) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTFLUSH, FAIL, "unable to flush R/W file");
    if (H5FDflush(file->wo_file, dxpl_id, closing) < 0)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTFLUSH, FAIL, "unable to flush W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_read
 *
 * Purpose:     Reads SIZE bytes of data from the R/W channel, beginning at
 *              address ADDR into buffer BUF according to data transfer
 *              properties in DXPL_ID.
 *
 * Return:      Success:    SUCCEED
 *                          The read result is written into the BUF buffer
 *                          which should be allocated by the caller.
 *              Failure:    FAIL
 *                          The contents of BUF are undefined.
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                    size_t size, void *buf)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(file && file->pub.cls);
    assert(buf);

    /* Check for overflow conditions */
    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "addr undefined, addr = %llu", (unsigned long long)addr);
    if (REGION_OVERFLOW(addr, size))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, FAIL, "addr overflow, addr = %llu", (unsigned long long)addr);

    /* Only read from R/W channel */
    /* Public API for dxpl "context" */
    if (H5FDread(file->rw_file, type, dxpl_id, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "Reading from R/W channel failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_write
 *
 * Purpose:     Writes SIZE bytes of data to R/W and W/O channels, beginning
 *              at address ADDR from buffer BUF according to data transfer
 *              properties in DXPL_ID.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                     const void *buf)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    H5P_genplist_t  *plist_ptr = NULL;
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    if (NULL == (plist_ptr = (H5P_genplist_t *)H5I_object(dxpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Write to each file */
    /* Public API for dxpl "context" */
    if (H5FDwrite(file->rw_file, type, dxpl_id, addr, size, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "R/W file write failed");
    if (H5FDwrite(file->wo_file, type, dxpl_id, addr, size, buf) < 0)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_WRITEERROR, FAIL, "unable to write W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_fapl_get
 *
 * Purpose:     Returns a file access property list which indicates how the
 *              specified file is being accessed. The return list could be
 *              used to access another file the same way.
 *
 * Return:      Success:    Ptr to new file access property list with all
 *                          members copied from the file struct.
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5FD__splitter_fapl_get(H5FD_t *_file)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    void            *ret_value = NULL;

    FUNC_ENTER_PACKAGE_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    ret_value = H5FD__splitter_fapl_copy(&(file->fa));

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_fapl_get() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_fapl_copy
 *
 * Purpose:     Copies the file access properties.
 *
 * Return:      Success:    Pointer to a new property list info structure.
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static void *
H5FD__splitter_fapl_copy(const void *_old_fa)
{
    const H5FD_splitter_fapl_t *old_fa_ptr = (const H5FD_splitter_fapl_t *)_old_fa;
    H5FD_splitter_fapl_t       *new_fa_ptr = NULL;
    void                       *ret_value  = NULL;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(old_fa_ptr);

    new_fa_ptr = H5FL_CALLOC(H5FD_splitter_fapl_t);
    if (NULL == new_fa_ptr)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate log file FAPL");

    H5MM_memcpy(new_fa_ptr, old_fa_ptr, sizeof(H5FD_splitter_fapl_t));
    strncpy(new_fa_ptr->wo_path, old_fa_ptr->wo_path, H5FD_SPLITTER_PATH_MAX + 1);
    strncpy(new_fa_ptr->log_file_path, old_fa_ptr->log_file_path, H5FD_SPLITTER_PATH_MAX + 1);

    /* Copy R/W and W/O FAPLs */
    if (H5FD__copy_plist(old_fa_ptr->rw_fapl_id, &(new_fa_ptr->rw_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't copy R/W FAPL");
    if (H5FD__copy_plist(old_fa_ptr->wo_fapl_id, &(new_fa_ptr->wo_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't copy W/O FAPL");

    ret_value = (void *)new_fa_ptr;

done:
    if (NULL == ret_value)
        if (new_fa_ptr)
            new_fa_ptr = H5FL_FREE(H5FD_splitter_fapl_t, new_fa_ptr);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_fapl_copy() */

/*--------------------------------------------------------------------------
 * Function:    H5FD__splitter_fapl_free
 *
 * Purpose:     Releases the file access lists
 *
 * Return:      SUCCEED/FAIL
 *--------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_fapl_free(void *_fapl)
{
    H5FD_splitter_fapl_t *fapl      = (H5FD_splitter_fapl_t *)_fapl;
    herr_t                ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(fapl);

    if (H5I_dec_ref(fapl->rw_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't close R/W FAPL ID");
    if (H5I_dec_ref(fapl->wo_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't close W/O FAPL ID");

    /* Free the property list */
    fapl = H5FL_FREE(H5FD_splitter_fapl_t, fapl);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_fapl_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_open
 *
 * Purpose:     Create and/or opens a file as an HDF5 file.
 *
 * Return:      Success:    A pointer to a new file data structure. The
 *                          public fields will be initialized by the
 *                          caller, which is always H5FD_open().
 *              Failure:    NULL
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__splitter_open(const char *name, unsigned flags, hid_t splitter_fapl_id, haddr_t maxaddr)
{
    H5FD_splitter_t            *file_ptr     = NULL; /* Splitter VFD info */
    const H5FD_splitter_fapl_t *fapl_ptr     = NULL; /* Driver-specific property list */
    H5FD_splitter_fapl_t       *default_fapl = NULL;
    H5P_genplist_t             *plist_ptr    = NULL;
    H5FD_t                     *ret_value    = NULL;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
    if (ADDR_OVERFLOW(maxaddr))
        HGOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");
    if (H5FD_SPLITTER != H5Pget_driver(splitter_fapl_id))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "driver is not splitter");

    file_ptr = (H5FD_splitter_t *)H5FL_CALLOC(H5FD_splitter_t);
    if (NULL == file_ptr)
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate file struct");
    file_ptr->fa.rw_fapl_id = H5I_INVALID_HID;
    file_ptr->fa.wo_fapl_id = H5I_INVALID_HID;

    /* Get the driver-specific file access properties */
    plist_ptr = (H5P_genplist_t *)H5I_object(splitter_fapl_id);
    if (NULL == plist_ptr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
    fapl_ptr = (const H5FD_splitter_fapl_t *)H5P_peek_driver_info(plist_ptr);
    if (NULL == fapl_ptr) {
        if (NULL == (default_fapl = H5FL_CALLOC(H5FD_splitter_fapl_t)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "unable to allocate file access property list struct");
        if (H5FD__splitter_populate_config(NULL, default_fapl) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't initialize driver configuration info");

        /* If W/O path is not set, use base filename with '_wo' suffix */
        if (*default_fapl->wo_path == '\0')
            if (H5FD__splitter_get_default_wo_path(default_fapl->wo_path, H5FD_SPLITTER_PATH_MAX + 1, name) <
                0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't generate default filename for W/O channel");

        fapl_ptr = default_fapl;
    }

    /* Copy simpler info */
    strncpy(file_ptr->fa.wo_path, fapl_ptr->wo_path, H5FD_SPLITTER_PATH_MAX + 1);
    strncpy(file_ptr->fa.log_file_path, fapl_ptr->log_file_path, H5FD_SPLITTER_PATH_MAX + 1);
    file_ptr->fa.ignore_wo_errs = fapl_ptr->ignore_wo_errs;

    /* Copy R/W and W/O channel FAPLs. */
    if (H5FD__copy_plist(fapl_ptr->rw_fapl_id, &(file_ptr->fa.rw_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't copy R/W FAPL");
    if (H5FD__copy_plist(fapl_ptr->wo_fapl_id, &(file_ptr->fa.wo_fapl_id)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "can't copy W/O FAPL");

    /* Prepare log file if necessary.
     * If application wants to ignore the errors from W/O channel and
     * provided a name for the log file, then open it
     */
    if (!file_ptr->logfp) {
        if (file_ptr->fa.log_file_path[0] != '\0') {
            file_ptr->logfp = fopen(file_ptr->fa.log_file_path, "w");
            if (file_ptr->logfp == NULL)
                HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "unable to open log file");
        } /* end if logfile path given */
    }     /* end if logfile pointer/handle does not exist */

    file_ptr->rw_file = H5FD_open(name, flags, fapl_ptr->rw_fapl_id, HADDR_UNDEF);
    if (!file_ptr->rw_file)
        HGOTO_ERROR(H5E_VFL, H5E_CANTOPENFILE, NULL, "unable to open R/W file");

    file_ptr->wo_file = H5FD_open(fapl_ptr->wo_path, flags, fapl_ptr->wo_fapl_id, HADDR_UNDEF);
    if (!file_ptr->wo_file)
        H5FD_SPLITTER_WO_ERROR(file_ptr, __func__, H5E_VFL, H5E_CANTOPENFILE, NULL, "unable to open W/O file")

    ret_value = (H5FD_t *)file_ptr;

done:
    if (default_fapl)
        H5FL_FREE(H5FD_splitter_fapl_t, default_fapl);

    if (NULL == ret_value) {
        if (file_ptr) {
            if (H5I_INVALID_HID != file_ptr->fa.rw_fapl_id)
                H5I_dec_ref(file_ptr->fa.rw_fapl_id);
            if (H5I_INVALID_HID != file_ptr->fa.wo_fapl_id)
                H5I_dec_ref(file_ptr->fa.wo_fapl_id);
            if (file_ptr->rw_file)
                H5FD_close(file_ptr->rw_file);
            if (file_ptr->wo_file)
                H5FD_close(file_ptr->wo_file);
            if (file_ptr->logfp)
                fclose(file_ptr->logfp);
            H5FL_FREE(H5FD_splitter_t, file_ptr);
        }
    } /* end if error */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_close
 *
 * Purpose:     Closes files on both read-write and write-only channels.
 *
 * Return:      Success:    SUCCEED
 *              Failure:    FAIL, file not closed.
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_close(H5FD_t *_file)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);

    if (H5I_dec_ref(file->fa.rw_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_ARGS, FAIL, "can't close R/W FAPL");
    if (H5I_dec_ref(file->fa.wo_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_ARGS, FAIL, "can't close W/O FAPL");

    if (file->rw_file)
        if (H5FD_close(file->rw_file) == FAIL)
            HGOTO_ERROR(H5E_VFL, H5E_CANTCLOSEFILE, FAIL, "unable to close R/W file");
    if (file->wo_file)
        if (H5FD_close(file->wo_file) == FAIL)
            H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTCLOSEFILE, FAIL,
                                   "unable to close W/O file")

    if (file->logfp) {
        fclose(file->logfp);
        file->logfp = NULL;
    }

    /* Release the file info */
    file = H5FL_FREE(H5FD_splitter_t, file);
    file = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_get_eoa
 *
 * Purpose:     Returns the end-of-address marker for the file. The EOA
 *              marker is the first address past the last byte allocated in
 *              the format address space.
 *
 * Return:      Success:    The end-of-address-marker
 *
 *              Failure:    HADDR_UNDEF
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__splitter_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_splitter_t *file      = (const H5FD_splitter_t *)_file;
    haddr_t                ret_value = HADDR_UNDEF;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->rw_file);

    if ((ret_value = H5FD_get_eoa(file->rw_file, type)) == HADDR_UNDEF)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, HADDR_UNDEF, "unable to get eoa");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_get_eoa */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__)

    /* Sanity check */
    assert(file);
    assert(file->rw_file);
    assert(file->wo_file);

    if (H5FD_set_eoa(file->rw_file, type, addr) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "H5FDset_eoa failed for R/W file");

    if (H5FD_set_eoa(file->wo_file, type, addr) < 0)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTSET, FAIL, "unable to set EOA for W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_get_eof
 *
 * Purpose:     Returns the end-of-address marker for the file. The EOA
 *              marker is the first address past the last byte allocated in
 *              the format address space.
 *
 * Return:      Success:    The end-of-address-marker
 *
 *              Failure:    HADDR_UNDEF
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__splitter_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_splitter_t *file      = (const H5FD_splitter_t *)_file;
    haddr_t                ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->rw_file);

    if (HADDR_UNDEF == (ret_value = H5FD_get_eof(file->rw_file, type)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, HADDR_UNDEF, "unable to get eof");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_get_eof */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_truncate
 *
 * Purpose:     Notify driver to truncate the file back to the allocated size.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(file);
    assert(file->rw_file);
    assert(file->wo_file);

    if (H5FDtruncate(file->rw_file, dxpl_id, closing) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTUPDATE, FAIL, "unable to truncate R/W file");

    if (H5FDtruncate(file->wo_file, dxpl_id, closing) < 0)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTUPDATE, FAIL, "unable to truncate W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_truncate */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_sb_size
 *
 * Purpose:     Obtains the number of bytes required to store the driver file
 *              access data in the HDF5 superblock.
 *
 * Return:      Success:    Number of bytes required.
 *
 *              Failure:    0 if an error occurs or if the driver has no
 *                          data to store in the superblock.
 *
 * NOTE: no public API for H5FD_sb_size, it needs to be added
 *-------------------------------------------------------------------------
 */
static hsize_t
H5FD__splitter_sb_size(H5FD_t *_file)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    hsize_t          ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->rw_file);

    if (file->rw_file)
        ret_value = H5FD_sb_size(file->rw_file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_sb_size */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_sb_encode
 *
 * Purpose:     Encode driver-specific data into the output arguments.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->rw_file);

    if (file->rw_file && H5FD_sb_encode(file->rw_file, name, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTENCODE, FAIL, "unable to encode the superblock in R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_sb_encode */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_sb_decode
 *
 * Purpose:     Decodes the driver information block.
 *
 * Return:      SUCCEED/FAIL
 *
 * NOTE: no public API for H5FD_sb_size, need to add
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Sanity check */
    assert(file);
    assert(file->rw_file);

    if (H5FD_sb_load(file->rw_file, name, buf) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDECODE, FAIL, "unable to decode the superblock in R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_sb_decode */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_cmp
 *
 * Purpose:     Compare the keys of two files.
 *
 * Return:      Success:    A value like strcmp()
 *              Failure:    Must never fail
 *-------------------------------------------------------------------------
 */
static int
H5FD__splitter_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_splitter_t *f1        = (const H5FD_splitter_t *)_f1;
    const H5FD_splitter_t *f2        = (const H5FD_splitter_t *)_f2;
    herr_t                 ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(f1);
    assert(f2);

    ret_value = H5FD_cmp(f1->rw_file, f2->rw_file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_cmp */

/*--------------------------------------------------------------------------
 * Function:    H5FD__splitter_get_handle
 *
 * Purpose:     Returns a pointer to the file handle of low-level virtual
 *              file driver.
 *
 * Return:      SUCCEED/FAIL
 *--------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(file->rw_file);
    assert(file_handle);

    /* Only do for R/W channel */
    if (H5FD_get_vfd_handle(file->rw_file, file->fa.rw_fapl_id, file_handle) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "unable to get handle of R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_get_handle */

/*--------------------------------------------------------------------------
 * Function:    H5FD__splitter_lock
 *
 * Purpose:     Sets a file lock.
 *
 * Return:      SUCCEED/FAIL
 *--------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_lock(H5FD_t *_file, bool rw)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file; /* VFD file struct */
    herr_t           ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    assert(file);
    assert(file->rw_file);

    /* Place the lock on each file */
    if (H5FD_lock(file->rw_file, rw) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTLOCKFILE, FAIL, "unable to lock R/W file");

    if (file->wo_file != NULL)
        if (H5FD_lock(file->wo_file, rw) < 0)
            H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTLOCKFILE, FAIL, "unable to lock W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_lock */

/*--------------------------------------------------------------------------
 * Function:    H5FD__splitter_unlock
 *
 * Purpose:     Removes a file lock.
 *
 * Return:      SUCCEED/FAIL
 *--------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_unlock(H5FD_t *_file)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file; /* VFD file struct */
    herr_t           ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(file->rw_file);

    /* Remove the lock on each file */
    if (H5FD_unlock(file->rw_file) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTUNLOCKFILE, FAIL, "unable to unlock R/W file");

    if (file->wo_file != NULL)
        if (H5FD_unlock(file->wo_file) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTUNLOCKFILE, FAIL, "unable to unlock W/O file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_unlock */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_ctl
 *
 * Purpose:     Splitter VFD version of the ctl callback.
 *
 *              The desired operation is specified by the op_code
 *              parameter.
 *
 *              The flags parameter controls management of op_codes that
 *              are unknown to the callback
 *
 *              The input and output parameters allow op_code specific
 *              input and output
 *
 *              At present, this VFD supports no op codes of its own and
 *              simply passes ctl calls on to the R/W channel VFD.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input, void **output)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file;
    herr_t           ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(file);

    switch (op_code) {
        /* Unknown op code */
        default:
            if (flags & H5FD_CTL_ROUTE_TO_TERMINAL_VFD_FLAG) {
                /* Pass ctl call down to R/W channel VFD */
                if (H5FDctl(file->rw_file, op_code, flags, input, output) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "VFD ctl request failed");
            }
            else {
                /* If no valid VFD routing flag is specified, fail for unknown op code
                 * if H5FD_CTL_FAIL_IF_UNKNOWN_FLAG flag is set.
                 */
                if (flags & H5FD_CTL_FAIL_IF_UNKNOWN_FLAG)
                    HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL,
                                "VFD ctl request failed (unknown op code and fail if unknown flag is set)");
            }

            break;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_ctl() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_splitter_t *file      = (const H5FD_splitter_t *)_file;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    if (file) {
        assert(file);
        assert(file->rw_file);

        if (H5FDquery(file->rw_file, flags) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTLOCK, FAIL, "unable to query R/W file");
    }
    else {
        /* There is no file. Because this is a pure passthrough VFD,
         * it has no features of its own.
         */
        if (flags)
            *flags = 0;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_alloc
 *
 * Purpose:     Allocate file memory.
 *
 * Return:      Address of allocated space (HADDR_UNDEF if error).
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__splitter_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file; /* VFD file struct */
    haddr_t          ret_value = HADDR_UNDEF;              /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(file->rw_file);

    /* Allocate memory for each file, only return the return value for R/W file. */
    if ((ret_value = H5FDalloc(file->rw_file, type, dxpl_id, size)) == HADDR_UNDEF)
        HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, HADDR_UNDEF, "unable to allocate for R/W file");

    if (H5FDalloc(file->wo_file, type, dxpl_id, size) == HADDR_UNDEF)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTINIT, HADDR_UNDEF,
                               "unable to alloc for W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_get_type_map
 *
 * Purpose:     Retrieve the memory type mapping for this file
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_get_type_map(const H5FD_t *_file, H5FD_mem_t *type_map)
{
    const H5FD_splitter_t *file      = (const H5FD_splitter_t *)_file;
    herr_t                 ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(file->rw_file);

    /* Retrieve memory type mapping for R/W channel only */
    if (H5FD_get_fs_type_map(file->rw_file, type_map) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "unable to allocate for R/W file");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_get_type_map() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_free
 *
 * Purpose:     Free the resources for the splitter VFD.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_free(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size)
{
    H5FD_splitter_t *file      = (H5FD_splitter_t *)_file; /* VFD file struct */
    herr_t           ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(file->rw_file);

    if (H5FDfree(file->rw_file, type, dxpl_id, addr, size) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "unable to free for R/W file");

    if (H5FDfree(file->wo_file, type, dxpl_id, addr, size) < 0)
        H5FD_SPLITTER_WO_ERROR(file, __func__, H5E_VFL, H5E_CANTINIT, FAIL, "unable to free for W/O file")

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_free() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_delete(const char *filename, hid_t fapl_id)
{
    const H5FD_splitter_fapl_t *fapl_ptr     = NULL;
    H5FD_splitter_fapl_t       *default_fapl = NULL;
    H5P_genplist_t             *plist;
    herr_t                      ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(filename);

    /* Get the driver info */
    if (H5P_FILE_ACCESS_DEFAULT == fapl_id) {
        if (NULL == (default_fapl = H5FL_CALLOC(H5FD_splitter_fapl_t)))
            HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate file access property list struct");
        if (H5FD__splitter_populate_config(NULL, default_fapl) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't initialize driver configuration info");

        /* If W/O path is not set, use base filename with '_wo' suffix */
        if (*default_fapl->wo_path == '\0')
            if (H5FD__splitter_get_default_wo_path(default_fapl->wo_path, H5FD_SPLITTER_PATH_MAX + 1,
                                                   filename) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't generate default filename for W/O channel");

        fapl_ptr = default_fapl;
    }
    else {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
        if (NULL == (fapl_ptr = (const H5FD_splitter_fapl_t *)H5P_peek_driver_info(plist))) {
            if (NULL == (default_fapl = H5FL_CALLOC(H5FD_splitter_fapl_t)))
                HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL,
                            "unable to allocate file access property list struct");
            if (H5FD__splitter_populate_config(NULL, default_fapl) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't initialize driver configuration info");

            /* If W/O path is not set, use base filename with '_wo' suffix */
            if (*default_fapl->wo_path == '\0')
                if (H5FD__splitter_get_default_wo_path(default_fapl->wo_path, H5FD_SPLITTER_PATH_MAX + 1,
                                                       filename) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL,
                                "can't generate default filename for W/O channel");

            fapl_ptr = default_fapl;
        }
    }

    if (H5FDdelete(filename, fapl_ptr->rw_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete file");
    if (H5FDdelete(fapl_ptr->wo_path, fapl_ptr->wo_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete W/O channel file");

done:
    if (default_fapl)
        H5FL_FREE(H5FD_splitter_fapl_t, default_fapl);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__splitter_log_error
 *
 * Purpose:     Log an error from the W/O channel appropriately.
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__splitter_log_error(const H5FD_splitter_t *file, const char *atfunc, const char *msg)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE_NOERR

    H5FD_SPLITTER_LOG_CALL(__func__);

    /* Check arguments */
    assert(file);
    assert(atfunc && *atfunc);
    assert(msg && *msg);

    if (file->logfp != NULL) {
        size_t size;
        char  *s;

        size = strlen(atfunc) + strlen(msg) + 3; /* ':', ' ', '\n' */
        s    = (char *)H5MM_malloc(sizeof(char) * (size + 1));
        if (NULL == s)
            ret_value = FAIL;
        else if (size < (size_t)snprintf(s, size + 1, "%s: %s\n", atfunc, msg))
            ret_value = FAIL;
        else if (size != fwrite(s, 1, size, file->logfp))
            ret_value = FAIL;
        H5MM_free(s);
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__splitter_log_error() */
