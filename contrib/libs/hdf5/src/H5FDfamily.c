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
 * Purpose:    Implements a family of files that acts as a single hdf5
 *        file.  The purpose is to be able to split a huge file on a
 *        64-bit platform, transfer all the <2GB members to a 32-bit
 *        platform, and then access the entire huge file on the 32-bit
 *        platform.
 *
 *        All family members are logically the same size although their
 *        physical sizes may vary.  The logical member size is
 *        determined by looking at the physical size of the first member
 *        when the file is opened.  When creating a file family, the
 *        first member is created with a predefined physical size
 *        (actually, this happens when the file family is flushed, and
 *        can be quite time consuming on file systems that don't
 *        implement holes, like nfs).
 *
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions            */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling              */
#include "H5Fprivate.h"  /* File access                */
#include "H5FDprivate.h" /* File drivers                */
#include "H5FDfamily.h"  /* Family file driver             */
#include "H5Iprivate.h"  /* IDs                      */
#include "H5MMprivate.h" /* Memory management            */
#include "H5Pprivate.h"  /* Property lists            */

/* The size of the member name buffers */
#define H5FD_FAM_MEMB_NAME_BUF_SIZE 4096

/* Default member size - 100 MiB */
#define H5FD_FAM_DEF_MEM_SIZE ((hsize_t)(100 * H5_MB))

/* The driver identification number, initialized at runtime */
static hid_t H5FD_FAMILY_g = 0;

/* The description of a file belonging to this driver. */
typedef struct H5FD_family_t {
    H5FD_t   pub;          /*public stuff, must be first        */
    hid_t    memb_fapl_id; /*file access property list for members    */
    hsize_t  memb_size;    /*actual size of each member file    */
    hsize_t  pmem_size;    /*member size passed in from property    */
    unsigned nmembs;       /*number of family members        */
    unsigned amembs;       /*number of member slots allocated    */
    H5FD_t **memb;         /*dynamic array of member pointers    */
    haddr_t  eoa;          /*end of allocated addresses        */
    char    *name;         /*name generator printf format        */
    unsigned flags;        /*flags for opening additional members    */

    /* Information from properties set by 'h5repart' tool */
    hsize_t mem_newsize; /*new member size passed in as private
                          * property. It's used only by h5repart */
    bool repart_members; /* Whether to mark the superblock dirty
                          * when it is loaded, so that the family
                          * member sizes can be re-encoded       */
} H5FD_family_t;

/* Driver-specific file access properties */
typedef struct H5FD_family_fapl_t {
    hsize_t memb_size;    /*size of each member            */
    hid_t   memb_fapl_id; /*file access property list of each memb*/
} H5FD_family_fapl_t;

/* Private routines */
static herr_t H5FD__family_get_default_config(H5FD_family_fapl_t *fa_out);
static char  *H5FD__family_get_default_printf_filename(const char *old_filename);

/* Callback prototypes */
static herr_t  H5FD__family_term(void);
static void   *H5FD__family_fapl_get(H5FD_t *_file);
static void   *H5FD__family_fapl_copy(const void *_old_fa);
static herr_t  H5FD__family_fapl_free(void *_fa);
static hsize_t H5FD__family_sb_size(H5FD_t *_file);
static herr_t  H5FD__family_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/);
static herr_t  H5FD__family_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf);
static H5FD_t *H5FD__family_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__family_close(H5FD_t *_file);
static int     H5FD__family_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD__family_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__family_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__family_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t eoa);
static haddr_t H5FD__family_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__family_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__family_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                 void *_buf /*out*/);
static herr_t  H5FD__family_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                  const void *_buf);
static herr_t  H5FD__family_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__family_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD__family_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD__family_unlock(H5FD_t *_file);
static herr_t  H5FD__family_delete(const char *filename, hid_t fapl_id);

/* The class struct */
static const H5FD_class_t H5FD_family_g = {
    H5FD_CLASS_VERSION,         /* struct version       */
    H5FD_FAMILY_VALUE,          /* value                */
    "family",                   /* name                 */
    HADDR_MAX,                  /* maxaddr              */
    H5F_CLOSE_WEAK,             /* fc_degree            */
    H5FD__family_term,          /* terminate            */
    H5FD__family_sb_size,       /* sb_size              */
    H5FD__family_sb_encode,     /* sb_encode            */
    H5FD__family_sb_decode,     /* sb_decode            */
    sizeof(H5FD_family_fapl_t), /* fapl_size            */
    H5FD__family_fapl_get,      /* fapl_get             */
    H5FD__family_fapl_copy,     /* fapl_copy            */
    H5FD__family_fapl_free,     /* fapl_free            */
    0,                          /* dxpl_size            */
    NULL,                       /* dxpl_copy            */
    NULL,                       /* dxpl_free            */
    H5FD__family_open,          /* open                 */
    H5FD__family_close,         /* close                */
    H5FD__family_cmp,           /* cmp                  */
    H5FD__family_query,         /* query                */
    NULL,                       /* get_type_map         */
    NULL,                       /* alloc                */
    NULL,                       /* free                 */
    H5FD__family_get_eoa,       /* get_eoa              */
    H5FD__family_set_eoa,       /* set_eoa              */
    H5FD__family_get_eof,       /* get_eof              */
    H5FD__family_get_handle,    /* get_handle           */
    H5FD__family_read,          /* read                 */
    H5FD__family_write,         /* write                */
    NULL,                       /* read_vector          */
    NULL,                       /* write_vector         */
    NULL,                       /* read_selection       */
    NULL,                       /* write_selection      */
    H5FD__family_flush,         /* flush                */
    H5FD__family_truncate,      /* truncate             */
    H5FD__family_lock,          /* lock                 */
    H5FD__family_unlock,        /* unlock               */
    H5FD__family_delete,        /* del                  */
    NULL,                       /* ctl                  */
    H5FD_FLMAP_DICHOTOMY        /* fl_map               */
};

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_get_default_config
 *
 * Purpose:     Populates a H5FD_family_fapl_t structure with default
 *              values.
 *
 * Return:      Non-negative on Success/Negative on Failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_get_default_config(H5FD_family_fapl_t *fa_out)
{
    H5P_genplist_t *def_plist;
    H5P_genplist_t *plist;
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(fa_out);

    fa_out->memb_size = H5FD_FAM_DEF_MEM_SIZE;

    /* Use copy of default file access property list for member FAPL ID.
     * The Sec2 driver is explicitly set on the member FAPL ID, as the
     * default driver might have been replaced with the Family VFD, which
     * would cause recursion badness in the child members.
     */
    if (NULL == (def_plist = (H5P_genplist_t *)H5I_object(H5P_FILE_ACCESS_DEFAULT)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if ((fa_out->memb_fapl_id = H5P_copy_plist(def_plist, false)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTCOPY, FAIL, "can't copy property list");
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fa_out->memb_fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5P_set_driver_by_value(plist, H5_VFD_SEC2, NULL, true) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't set default driver on member FAPL");

done:
    if (ret_value < 0 && fa_out->memb_fapl_id >= 0) {
        if (H5I_dec_ref(fa_out->memb_fapl_id) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't decrement ref. count on member FAPL ID");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_get_default_config() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_get_default_printf_filename
 *
 * Purpose:     Given a filename, allocates and returns a new filename
 *              buffer that contains the given filename modified into this
 *              VFD's printf-style format. For example, the filename
 *              "file1.h5" would be modified to "file1-%06d.h5". This would
 *              allow for member filenames such as "file1-000000.h5",
 *              "file1-000001.h5", etc. The caller is responsible for
 *              freeing the returned buffer.
 *
 * Return:      Non-negative on Success/Negative on Failure
 *
 *-------------------------------------------------------------------------
 */
static char *
H5FD__family_get_default_printf_filename(const char *old_filename)
{
    const char *suffix           = "-%06d";
    size_t      old_filename_len = 0;
    size_t      new_filename_len = 0;
    char       *file_extension   = NULL;
    char       *tmp_buffer       = NULL;
    char       *ret_value        = NULL;

    FUNC_ENTER_PACKAGE

    assert(old_filename);

    old_filename_len = strlen(old_filename);
    if (0 == old_filename_len)
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, NULL, "invalid filename");

    new_filename_len = old_filename_len + strlen(suffix) + 1;
    if (NULL == (tmp_buffer = H5MM_malloc(new_filename_len)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, NULL, "can't allocate new filename buffer");

    /* Determine if filename contains a ".h5" extension. */
    if ((file_extension = strstr(old_filename, ".h5"))) {
        /* Insert the printf format between the filename and ".h5" extension. */
        strcpy(tmp_buffer, old_filename);
        file_extension = strstr(tmp_buffer, ".h5");
        sprintf(file_extension, "%s%s", suffix, ".h5");
    }
    else if ((file_extension = strrchr(old_filename, '.'))) {
        char *new_extension_loc = NULL;

        /* If the filename doesn't contain a ".h5" extension, but contains
         * AN extension, just insert the printf format before that extension.
         */
        strcpy(tmp_buffer, old_filename);
        new_extension_loc = strrchr(tmp_buffer, '.');
        sprintf(new_extension_loc, "%s%s", suffix, file_extension);
    }
    else {
        /* If the filename doesn't contain an extension at all, just insert
         * the printf format at the end of the filename.
         */
        snprintf(tmp_buffer, new_filename_len, "%s%s", old_filename, suffix);
    }

    ret_value = tmp_buffer;

done:
    if (!ret_value)
        H5MM_xfree(tmp_buffer);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_get_default_printf_filename() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_family_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the family driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_family_init(void)
{
    hid_t ret_value = H5I_INVALID_HID;

    FUNC_ENTER_NOAPI_NOERR

    if (H5I_VFL != H5I_get_type(H5FD_FAMILY_g))
        H5FD_FAMILY_g = H5FD_register(&H5FD_family_g, sizeof(H5FD_class_t), false);

    /* Set return value */
    ret_value = H5FD_FAMILY_g;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5FD_family_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__family_term
 *
 * Purpose:    Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__family_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset VFL ID */
    H5FD_FAMILY_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__family_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_family
 *
 * Purpose:    Sets the file access property list FAPL_ID to use the family
 *        driver. The MEMB_SIZE is the size in bytes of each file
 *        member (used only when creating a new file) and the
 *        MEMB_FAPL_ID is a file access property list to be used for
 *        each family member.
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_family(hid_t fapl_id, hsize_t msize, hid_t memb_fapl_id)
{
    herr_t             ret_value;
    H5FD_family_fapl_t fa = {0, H5I_INVALID_HID};
    H5P_genplist_t    *plist; /* Property list pointer */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ihi", fapl_id, msize, memb_fapl_id);

    /* Check arguments */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    if (H5P_DEFAULT == memb_fapl_id) {
        /* Get default configuration for member FAPL */
        if (H5FD__family_get_default_config(&fa) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get default driver configuration info");
    }
    else if (true != H5P_isa_class(memb_fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");

    /* Initialize driver specific information. */
    fa.memb_size = msize;
    if (H5P_DEFAULT != memb_fapl_id)
        fa.memb_fapl_id = memb_fapl_id;

    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    ret_value = H5P_set_driver(plist, H5FD_FAMILY, &fa, NULL);

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_family
 *
 * Purpose:    Returns information about the family file access property
 *             list though the function arguments.
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_family(hid_t fapl_id, hsize_t *msize /*out*/, hid_t *memb_fapl_id /*out*/)
{
    H5P_genplist_t           *plist; /* Property list pointer */
    const H5FD_family_fapl_t *fa;
    herr_t                    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, msize, memb_fapl_id);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");
    if (H5FD_FAMILY != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "incorrect VFL driver");
    if (NULL == (fa = (const H5FD_family_fapl_t *)H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "bad VFL driver info");
    if (msize)
        *msize = fa->memb_size;
    if (memb_fapl_id) {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fa->memb_fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access list");
        *memb_fapl_id = H5P_copy_plist(plist, true);
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_fapl_get
 *
 * Purpose:    Gets a file access property list which could be used to
 *             create an identical file.
 *
 * Return:    Success:    Ptr to new file access property list.
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__family_fapl_get(H5FD_t *_file)
{
    H5FD_family_t      *file = (H5FD_family_t *)_file;
    H5FD_family_fapl_t *fa   = NULL;
    H5P_genplist_t     *plist;            /* Property list pointer */
    void               *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL == (fa = (H5FD_family_fapl_t *)H5MM_calloc(sizeof(H5FD_family_fapl_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    fa->memb_size = file->memb_size;
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(file->memb_fapl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
    fa->memb_fapl_id = H5P_copy_plist(plist, false);

    /* Set return value */
    ret_value = fa;

done:
    if (ret_value == NULL)
        if (fa != NULL)
            H5MM_xfree(fa);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_fapl_copy
 *
 * Purpose:    Copies the family-specific file access properties.
 *
 * Return:    Success:    Ptr to a new property list
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD__family_fapl_copy(const void *_old_fa)
{
    const H5FD_family_fapl_t *old_fa = (const H5FD_family_fapl_t *)_old_fa;
    H5FD_family_fapl_t       *new_fa = NULL;
    H5P_genplist_t           *plist;            /* Property list pointer */
    void                     *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    if (NULL == (new_fa = (H5FD_family_fapl_t *)H5MM_malloc(sizeof(H5FD_family_fapl_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy the fields of the structure */
    H5MM_memcpy(new_fa, old_fa, sizeof(H5FD_family_fapl_t));

    /* Deep copy the property list objects in the structure */
    if (old_fa->memb_fapl_id == H5P_FILE_ACCESS_DEFAULT) {
        if (H5I_inc_ref(new_fa->memb_fapl_id, false) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTINC, NULL, "unable to increment ref count on VFL driver");
    } /* end if */
    else {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(old_fa->memb_fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
        new_fa->memb_fapl_id = H5P_copy_plist(plist, false);
    } /* end else */

    /* Set return value */
    ret_value = new_fa;

done:
    if (ret_value == NULL)
        if (new_fa != NULL)
            H5MM_xfree(new_fa);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_fapl_free
 *
 * Purpose:    Frees the family-specific file access properties.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_fapl_free(void *_fa)
{
    H5FD_family_fapl_t *fa        = (H5FD_family_fapl_t *)_fa;
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (H5I_dec_ref(fa->memb_fapl_id) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't close driver ID");
    H5MM_xfree(fa);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_sb_size
 *
 * Purpose:    Returns the size of the private information to be stored in
 *        the superblock.
 *
 * Return:    Success:    The super block driver data size.
 *
 *            Failure:    never fails
 *
 *-------------------------------------------------------------------------
 */
static hsize_t
H5FD__family_sb_size(H5FD_t H5_ATTR_UNUSED *_file)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* 8 bytes field for the size of member file size field should be
     * enough for now. */
    FUNC_LEAVE_NOAPI(8)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_sb_encode
 *
 * Purpose:    Encode driver information for the superblock. The NAME
 *        argument is a nine-byte buffer which will be initialized with
 *        an eight-character name/version number and null termination.
 *
 *        The encoding is the member file size and name template.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/)
{
    H5FD_family_t *file = (H5FD_family_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* Name and version number */
    strncpy(name, "NCSAfami", (size_t)9);
    name[8] = '\0';

    /* Store member file size.  Use the member file size from the property here.
     * This is to guarantee backward compatibility.  If a file is created with
     * v1.6 library and the driver info isn't saved in the superblock.  We open
     * it with v1.8, the FILE->MEMB_SIZE will be the actual size of the first
     * member file (see H5FD__family_open).  So it isn't safe to use FILE->MEMB_SIZE.
     * If the file is created with v1.8, the correctness of FILE->PMEM_SIZE is
     * checked in H5FD__family_sb_decode. SLU - 2009/3/21
     */
    UINT64ENCODE(buf, (uint64_t)file->pmem_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__family_sb_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_sb_decode
 *
 * Purpose:     This function has 2 separate purpose.  One is to decodes the
 *              superblock information for this driver. The NAME argument is
 *              the eight-character (plus null termination) name stored in i
 *              the file.  The FILE argument is updated according to the
 *              information in the superblock.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_sb_decode(H5FD_t *_file, const char H5_ATTR_UNUSED *name, const unsigned char *buf)
{
    H5FD_family_t *file = (H5FD_family_t *)_file;
    uint64_t       msize;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Read member file size. Skip name template for now although it's saved. */
    UINT64DECODE(buf, msize);

    /* For h5repart only. Private property of new member size is used to signal
     * h5repart is being used to change member file size.  h5repart will open
     * files for read and write.  When the files are closed, metadata will be
     * flushed to the files and updated to this new size */
    if (file->mem_newsize)
        file->memb_size = file->pmem_size = file->mem_newsize;
    else {
        /* Default - use the saved member size */
        if (file->pmem_size == H5F_FAMILY_DEFAULT)
            file->pmem_size = msize;

        /* Check if member size from file access property is correct */
        if (msize != file->pmem_size)
            HGOTO_ERROR(H5E_FILE, H5E_BADVALUE, FAIL,
                        "Family member size should be %lu.  But the size from file access property is %lu",
                        (unsigned long)msize, (unsigned long)file->pmem_size);

        /* Update member file size to the size saved in the superblock.
         * That's the size intended to be. */
        file->memb_size = msize;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_sb_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_open
 *
 * Purpose:    Creates and/or opens a family of files as an HDF5 file.
 *
 * Return:    Success:    A pointer to a new file dat structure. The
 *                public fields will be initialized by the
 *                caller, which is always H5FD_open().
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
/* Disable warning for "format not a string literal" here -QAK */
/*
 *      This pragma only needs to surround the snprintf() calls with
 *      memb_name & temp in the code below, but early (4.4.7, at least) gcc only
 *      allows diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_CLANG_DIAG_OFF("format-nonliteral")
static H5FD_t *
H5FD__family_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_family_t *file      = NULL;
    char          *memb_name = NULL, *temp = NULL;
    hsize_t        eof            = HADDR_UNDEF;
    bool           default_config = false;
    unsigned       t_flags        = flags & ~H5F_ACC_CREAT;
    H5FD_t        *ret_value      = NULL;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    if (!name || !*name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");

    /* Initialize file from file access properties */
    if (NULL == (file = (H5FD_family_t *)H5MM_calloc(sizeof(H5FD_family_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to allocate file struct");
    if (H5P_FILE_ACCESS_DEFAULT == fapl_id) {
        H5FD_family_fapl_t default_fa;

        /* Get default configuration */
        if (H5FD__family_get_default_config(&default_fa) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get default driver configuration info");

        file->memb_fapl_id = default_fa.memb_fapl_id;
        file->memb_size    = H5FD_FAM_DEF_MEM_SIZE; /* Actual member size to be updated later */
        file->pmem_size    = H5FD_FAM_DEF_MEM_SIZE; /* Member size passed in through property */
        file->mem_newsize  = 0;                     /*New member size used by h5repart only       */

        default_config = true;
    } /* end if */
    else {
        H5P_genplist_t           *plist; /* Property list pointer */
        const H5FD_family_fapl_t *fa;
        H5FD_family_fapl_t        default_fa;

        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
        if (NULL == (fa = (const H5FD_family_fapl_t *)H5P_peek_driver_info(plist))) {
            if (H5FD__family_get_default_config(&default_fa) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get default family VFD configuration");
            fa             = &default_fa;
            default_config = true;
        }

        /* Check for new family file size. It's used by h5repart only. */
        if (H5P_exist_plist(plist, H5F_ACS_FAMILY_NEWSIZE_NAME) > 0) {
            /* Get the new family file size */
            if (H5P_get(plist, H5F_ACS_FAMILY_NEWSIZE_NAME, &file->mem_newsize) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get new family member size");

            /* Set flag for later */
            file->repart_members = true;
        } /* end if */

        if (fa->memb_fapl_id == H5P_FILE_ACCESS_DEFAULT) {
            if (H5I_inc_ref(fa->memb_fapl_id, false) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTINC, NULL, "unable to increment ref count on VFL driver");
            file->memb_fapl_id = fa->memb_fapl_id;
        } /* end if */
        else {
            if (NULL == (plist = (H5P_genplist_t *)H5I_object(fa->memb_fapl_id)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");
            file->memb_fapl_id = H5P_copy_plist(plist, false);
        }                                /* end else */
        file->memb_size = fa->memb_size; /* Actual member size to be updated later */
        file->pmem_size = fa->memb_size; /* Member size passed in through property */

        if (default_config && H5I_dec_ref(fa->memb_fapl_id) < 0)
            HGOTO_ERROR(H5E_ID, H5E_CANTDEC, NULL, "can't decrement ref. count on member FAPL");
    } /* end else */
    file->name  = H5MM_strdup(name);
    file->flags = flags;

    /* Allocate space for the string buffers */
    if (NULL == (memb_name = (char *)H5MM_malloc(H5FD_FAM_MEMB_NAME_BUF_SIZE)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "unable to allocate member name");
    if (NULL == (temp = (char *)H5MM_malloc(H5FD_FAM_MEMB_NAME_BUF_SIZE)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, NULL, "unable to allocate temporary member name");

    /* Check that names are unique */
    snprintf(memb_name, H5FD_FAM_MEMB_NAME_BUF_SIZE, name, 0);
    snprintf(temp, H5FD_FAM_MEMB_NAME_BUF_SIZE, name, 1);
    if (!strcmp(memb_name, temp)) {
        if (default_config) {
            temp = H5MM_xfree(temp);
            if (NULL == (temp = H5FD__family_get_default_printf_filename(name)))
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get default printf-style filename");
            name = temp;
        }
        else
            HGOTO_ERROR(H5E_FILE, H5E_FILEEXISTS, NULL, "file names not unique");
    }

    /* Open all the family members */
    while (1) {
        snprintf(memb_name, H5FD_FAM_MEMB_NAME_BUF_SIZE, name, file->nmembs);

        /* Enlarge member array */
        if (file->nmembs >= file->amembs) {
            unsigned n = MAX(64, 2 * file->amembs);
            H5FD_t **x;

            assert(n > 0);
            if (NULL == (x = (H5FD_t **)H5MM_realloc(file->memb, n * sizeof(H5FD_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "unable to reallocate members");
            file->amembs = n;
            file->memb   = x;
        } /* end if */

        /*
         * Attempt to open file. If the first file cannot be opened then fail;
         * otherwise an open failure means that we've reached the last member.
         * Allow H5F_ACC_CREAT only on the first family member.
         */
        H5E_BEGIN_TRY
        {
            file->memb[file->nmembs] =
                H5FDopen(memb_name, (0 == file->nmembs ? flags : t_flags), file->memb_fapl_id, HADDR_UNDEF);
        }
        H5E_END_TRY
        if (!file->memb[file->nmembs]) {
            if (0 == file->nmembs)
                HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, NULL, "unable to open member file");
            H5E_clear_stack(NULL);
            break;
        }
        file->nmembs++;
    }

    /* If the file is reopened and there's only one member file existing, this file may be
     * smaller than the size specified through H5Pset_fapl_family().  Update the actual
     * member size.
     */
    if ((eof = H5FDget_eof(file->memb[0], H5FD_MEM_DEFAULT)))
        file->memb_size = eof;

    ret_value = (H5FD_t *)file;

done:
    /* Release resources */
    if (memb_name)
        H5MM_xfree(memb_name);
    if (temp)
        H5MM_xfree(temp);

    /* Cleanup and fail */
    if (ret_value == NULL && file != NULL) {
        unsigned nerrors = 0; /* Number of errors closing member files */
        unsigned u;           /* Local index variable */

        /* Close as many members as possible. Use private function here to avoid clearing
         * the error stack. We need the error message to indicate wrong member file size. */
        for (u = 0; u < file->nmembs; u++)
            if (file->memb[u])
                if (H5FD_close(file->memb[u]) < 0)
                    nerrors++;
        if (nerrors)
            HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, NULL, "unable to close member files");

        if (file->memb)
            H5MM_xfree(file->memb);
        if (H5I_dec_ref(file->memb_fapl_id) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTDEC, NULL, "can't close driver ID");
        if (file->name)
            H5MM_xfree(file->name);
        H5MM_xfree(file);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_open() */
H5_GCC_CLANG_DIAG_ON("format-nonliteral")

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_close
 *
 * Purpose:    Closes a family of files.
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative with as many members closed as
 *                possible. The only subsequent operation
 *                permitted on the file is a close operation.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_close(H5FD_t *_file)
{
    H5FD_family_t *file    = (H5FD_family_t *)_file;
    unsigned       nerrors = 0;         /* Number of errors while closing member files */
    unsigned       u;                   /* Local index variable */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Close as many members as possible. Use private function here to avoid clearing
     * the error stack. We need the error message to indicate wrong member file size. */
    for (u = 0; u < file->nmembs; u++) {
        if (file->memb[u]) {
            if (H5FD_close(file->memb[u]) < 0)
                nerrors++;
            else
                file->memb[u] = NULL;
        } /* end if */
    }     /* end for */
    if (nerrors)
        /* Push error, but keep going*/
        HDONE_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "unable to close member files");

    /* Clean up other stuff */
    if (H5I_dec_ref(file->memb_fapl_id) < 0)
        /* Push error, but keep going*/
        HDONE_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't close driver ID");
    H5MM_xfree(file->memb);
    H5MM_xfree(file->name);
    H5MM_xfree(file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_cmp
 *
 * Purpose:    Compares two file families to see if they are the same. It
 *        does this by comparing the first member of the two families.
 *
 * Return:    Success:    like strcmp()
 *
 *            Failure:    never fails (arguments were checked by the
 *                caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD__family_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_family_t *f1        = (const H5FD_family_t *)_f1;
    const H5FD_family_t *f2        = (const H5FD_family_t *)_f2;
    int                  ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(f1->nmembs >= 1 && f1->memb[0]);
    assert(f2->nmembs >= 1 && f2->memb[0]);

    ret_value = H5FDcmp(f1->memb[0], f2->memb[0]);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_query
 *
 * Purpose:    Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:    Success:    non-negative
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_query(const H5FD_t *_file, unsigned long *flags /* out */)
{
    const H5FD_family_t *file = (const H5FD_family_t *)_file; /* Family VFD info */

    FUNC_ENTER_PACKAGE_NOERR

    /* Set the VFL feature flags that this driver supports */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations */
        *flags |= H5FD_FEAT_ACCUMULATE_METADATA; /* OK to accumulate metadata for faster writes. */
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */

        /* Check for flags that are set by h5repart */
        if (file && file->repart_members)
            *flags |= H5FD_FEAT_DIRTY_DRVRINFO_LOAD; /* Mark the superblock dirty when it is loaded (so the
                                                        family member sizes are rewritten) */
    }                                                /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__family_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_get_eoa
 *
 * Purpose:    Returns the end-of-address marker for the file. The EOA
 *        marker is the first address past the last byte allocated in
 *        the format address space.
 *
 * Return:    Success:    The end-of-address-marker
 *
 *            Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__family_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_family_t *file = (const H5FD_family_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    FUNC_LEAVE_NOAPI(file->eoa)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_set_eoa
 *
 * Purpose:    Set the end-of-address marker for the file.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
/* Disable warning for "format not a string literal" here -QAK */
/*
 *      This pragma only needs to surround the snprintf() call with
 *      memb_name in the code below, but early (4.4.7, at least) gcc only
 *      allows diagnostic pragmas to be toggled outside of functions.
 */
H5_GCC_CLANG_DIAG_OFF("format-nonliteral")
static herr_t
H5FD__family_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t abs_eoa)
{
    H5FD_family_t *file      = (H5FD_family_t *)_file;
    haddr_t        addr      = abs_eoa;
    char          *memb_name = NULL;
    unsigned       u;                   /* Local index variable */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Allocate space for the member name buffer */
    if (NULL == (memb_name = (char *)H5MM_malloc(H5FD_FAM_MEMB_NAME_BUF_SIZE)))
        HGOTO_ERROR(H5E_FILE, H5E_CANTALLOC, FAIL, "unable to allocate member name");

    for (u = 0; addr || u < file->nmembs; u++) {

        /* Enlarge member array */
        if (u >= file->amembs) {
            unsigned n = MAX(64, 2 * file->amembs);
            H5FD_t **x = (H5FD_t **)H5MM_realloc(file->memb, n * sizeof(H5FD_t *));

            if (!x)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate memory block");
            file->amembs = n;
            file->memb   = x;
            file->nmembs = u;
        } /* end if */

        /* Create another file if necessary */
        if (u >= file->nmembs || !file->memb[u]) {
            file->nmembs = MAX(file->nmembs, u + 1);
            snprintf(memb_name, H5FD_FAM_MEMB_NAME_BUF_SIZE, file->name, u);
            H5E_BEGIN_TRY
            {
                H5_CHECK_OVERFLOW(file->memb_size, hsize_t, haddr_t);
                file->memb[u] = H5FDopen(memb_name, file->flags | H5F_ACC_CREAT, file->memb_fapl_id,
                                         (haddr_t)file->memb_size);
            }
            H5E_END_TRY
            if (NULL == file->memb[u])
                HGOTO_ERROR(H5E_FILE, H5E_CANTOPENFILE, FAIL, "unable to open member file");
        } /* end if */

        /* Set the EOA marker for the member */
        /* (Note compensating for base address addition in internal routine) */
        H5_CHECK_OVERFLOW(file->memb_size, hsize_t, haddr_t);
        if (addr > (haddr_t)file->memb_size) {
            if (H5FD_set_eoa(file->memb[u], type, ((haddr_t)file->memb_size - file->pub.base_addr)) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to set file eoa");
            addr -= file->memb_size;
        } /* end if */
        else {
            if (H5FD_set_eoa(file->memb[u], type, (addr - file->pub.base_addr)) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTINIT, FAIL, "unable to set file eoa");
            addr = 0;
        } /* end else */
    }     /* end for */

    file->eoa = abs_eoa;

done:
    /* Release resources */
    if (memb_name)
        H5MM_xfree(memb_name);

    FUNC_LEAVE_NOAPI(ret_value)
}
H5_GCC_CLANG_DIAG_ON("format-nonliteral")

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_get_eof
 *
 * Purpose:    Returns the end-of-file marker, which is the greater of
 *        either the total family size or the current EOA marker.
 *
 * Return:    Success:    End of file address, the first address past
 *                the end of the family of files or the current
 *                EOA, whichever is larger.
 *
 *            Failure:          HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__family_get_eof(const H5FD_t *_file, H5FD_mem_t type)
{
    const H5FD_family_t *file = (const H5FD_family_t *)_file;
    haddr_t              eof  = 0;
    int                  i;                       /* Local index variable */
    haddr_t              ret_value = HADDR_UNDEF; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Find the last member that has a non-zero EOF and break out of the loop
     * with `i' equal to that member. If all members have zero EOF then exit
     * loop with i==0.
     */
    assert(file->nmembs > 0);
    for (i = (int)file->nmembs - 1; i >= 0; --i) {
        if ((eof = H5FD_get_eof(file->memb[i], type)) != 0)
            break;
        if (0 == i)
            break;
    } /* end for */

    /* Adjust for base address for file */
    eof += file->pub.base_addr;

    /*
     * The file size is the number of members before the i'th member plus the
     * size of the i'th member.
     */
    eof += ((unsigned)i) * file->memb_size;

    /* Set return value */
    ret_value = eof;

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:       H5FD__family_get_handle
 *
 * Purpose:        Returns the file handle of FAMILY file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle)
{
    H5FD_family_t  *file = (H5FD_family_t *)_file;
    H5P_genplist_t *plist;
    hsize_t         offset;
    int             memb;
    herr_t          ret_value = FAIL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get the plist structure and family offset */
    if (NULL == (plist = H5P_object_verify(fapl, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");
    if (H5P_get(plist, H5F_ACS_FAMILY_OFFSET_NAME, &offset) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get offset for family driver");

    if (offset > (file->memb_size * file->nmembs))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "offset is bigger than file size");
    memb = (int)(offset / file->memb_size);

    ret_value = H5FD_get_vfd_handle(file->memb[memb], fapl, file_handle);

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_read
 *
 * Purpose:    Reads SIZE bytes of data from FILE beginning at address ADDR
 *        into buffer BUF according to data transfer properties in
 *        DXPL_ID.
 *
 * Return:    Success:    Zero. Result is stored in caller-supplied
 *                buffer BUF.
 *
 *            Failure:    -1, contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                  void *_buf /*out*/)
{
    H5FD_family_t  *file = (H5FD_family_t *)_file;
    unsigned char  *buf  = (unsigned char *)_buf;
    haddr_t         sub;
    size_t          req;
    hsize_t         tempreq;
    unsigned        u;                   /* Local index variable */
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Get the member data transfer property list. If the transfer property
     * list does not belong to this driver then assume defaults
     */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(dxpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Read from each member */
    while (size > 0) {
        H5_CHECKED_ASSIGN(u, unsigned, addr / file->memb_size, hsize_t);

        sub = addr % file->memb_size;

        /* This check is for mainly for IA32 architecture whose size_t's size
         * is 4 bytes, to prevent overflow when user application is trying to
         * write files bigger than 4GB. */
        tempreq = file->memb_size - sub;
        if (tempreq > SIZE_MAX)
            tempreq = SIZE_MAX;
        req = MIN(size, (size_t)tempreq);

        assert(u < file->nmembs);

        if (H5FDread(file->memb[u], type, dxpl_id, sub, req, buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "member file read failed");

        addr += req;
        buf += req;
        size -= req;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_write
 *
 * Purpose:    Writes SIZE bytes of data to FILE beginning at address ADDR
 *        from buffer BUF according to data transfer properties in
 *        DXPL_ID.
 *
 * Return:    Success:    Zero
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, const void *_buf)
{
    H5FD_family_t       *file = (H5FD_family_t *)_file;
    const unsigned char *buf  = (const unsigned char *)_buf;
    haddr_t              sub;
    size_t               req;
    hsize_t              tempreq;
    unsigned             u;                   /* Local index variable */
    H5P_genplist_t      *plist;               /* Property list pointer */
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Get the member data transfer property list. If the transfer property
     * list does not belong to this driver then assume defaults.
     */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(dxpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

    /* Write to each member */
    while (size > 0) {
        H5_CHECKED_ASSIGN(u, unsigned, addr / file->memb_size, hsize_t);

        sub = addr % file->memb_size;

        /* This check is for mainly for IA32 architecture whose size_t's size
         * is 4 bytes, to prevent overflow when user application is trying to
         * write files bigger than 4GB. */
        tempreq = file->memb_size - sub;
        if (tempreq > SIZE_MAX)
            tempreq = SIZE_MAX;
        req = MIN(size, (size_t)tempreq);

        assert(u < file->nmembs);

        if (H5FDwrite(file->memb[u], type, dxpl_id, sub, req, buf) < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "member file write failed");

        addr += req;
        buf += req;
        size -= req;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_flush
 *
 * Purpose:    Flushes all family members.
 *
 * Return:    Success:    0
 *            Failure:    -1, as many files flushed as possible.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_flush(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool closing)
{
    H5FD_family_t *file = (H5FD_family_t *)_file;
    unsigned       u, nerrors = 0;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    for (u = 0; u < file->nmembs; u++)
        if (file->memb[u] && H5FD_flush(file->memb[u], closing) < 0)
            nerrors++;

    if (nerrors)
        HGOTO_ERROR(H5E_IO, H5E_BADVALUE, FAIL, "unable to flush member files");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_truncate
 *
 * Purpose:    Truncates all family members.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1, as many files truncated as possible.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool closing)
{
    H5FD_family_t *file = (H5FD_family_t *)_file;
    unsigned       u, nerrors = 0;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    for (u = 0; u < file->nmembs; u++)
        if (file->memb[u] && H5FD_truncate(file->memb[u], closing) < 0)
            nerrors++;

    if (nerrors)
        HGOTO_ERROR(H5E_IO, H5E_BADVALUE, FAIL, "unable to flush member files");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_lock
 *
 * Purpose:     To place an advisory lock on a file.
 *              The lock type to apply depends on the parameter "rw":
 *                      true--opens for write: an exclusive lock
 *                      false--opens for read: a shared lock
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_lock(H5FD_t *_file, bool rw)
{
    H5FD_family_t *file = (H5FD_family_t *)_file; /* VFD file struct */
    unsigned       u;                             /* Local index variable */
    herr_t         ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Place the lock on all the member files */
    for (u = 0; u < file->nmembs; u++)
        if (file->memb[u])
            if (H5FD_lock(file->memb[u], rw) < 0)
                break;

    /* If one of the locks failed, try to unlock the locked member files
     * in an attempt to return to a fully unlocked state.
     */
    if (u < file->nmembs) {
        unsigned v; /* Local index variable */

        for (v = 0; v < u; v++) {
            if (H5FD_unlock(file->memb[v]) < 0)
                /* Push error, but keep going */
                HDONE_ERROR(H5E_IO, H5E_CANTUNLOCKFILE, FAIL, "unable to unlock member files");
        } /* end for */
        HGOTO_ERROR(H5E_IO, H5E_CANTLOCKFILE, FAIL, "unable to lock member files");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_unlock
 *
 * Purpose:     To remove the existing lock on the file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_unlock(H5FD_t *_file)
{
    H5FD_family_t *file = (H5FD_family_t *)_file; /* VFD file struct */
    unsigned       u;                             /* Local index variable */
    herr_t         ret_value = SUCCEED;           /* Return value */

    FUNC_ENTER_PACKAGE

    /* Remove the lock on the member files */
    for (u = 0; u < file->nmembs; u++)
        if (file->memb[u])
            if (H5FD_unlock(file->memb[u]) < 0)
                HGOTO_ERROR(H5E_IO, H5E_CANTUNLOCKFILE, FAIL, "unable to unlock member files");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_unlock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__family_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__family_delete(const char *filename, hid_t fapl_id)
{
    H5P_genplist_t           *plist;
    const H5FD_family_fapl_t *fa;
    H5FD_family_fapl_t        default_fa     = {0, H5I_INVALID_HID};
    bool                      default_config = false;
    hid_t                     memb_fapl_id   = H5I_INVALID_HID;
    unsigned                  current_member;
    char                     *member_name  = NULL;
    char                     *temp         = NULL;
    herr_t                    delete_error = FAIL;
    herr_t                    ret_value    = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(filename);

    /* Get the driver info (for the member fapl)
     * The family_open call accepts H5P_DEFAULT, so we'll accept that here, too.
     */
    if (H5P_FILE_ACCESS_DEFAULT == fapl_id) {
        if (H5FD__family_get_default_config(&default_fa) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get default family VFD configuration");
        memb_fapl_id   = default_fa.memb_fapl_id;
        default_config = true;
    }
    else {
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
        if (NULL == (fa = (const H5FD_family_fapl_t *)H5P_peek_driver_info(plist))) {
            if (H5FD__family_get_default_config(&default_fa) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get default family VFD configuration");
            fa             = &default_fa;
            default_config = true;
        }
        memb_fapl_id = fa->memb_fapl_id;
    }

    /* Allocate space for the string buffers */
    if (NULL == (member_name = (char *)H5MM_malloc(H5FD_FAM_MEMB_NAME_BUF_SIZE)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate member name");
    if (NULL == (temp = (char *)H5MM_malloc(H5FD_FAM_MEMB_NAME_BUF_SIZE)))
        HGOTO_ERROR(H5E_VFL, H5E_CANTALLOC, FAIL, "unable to allocate temporary member name");

    /* Sanity check to make sure that generated names are unique */
    H5_GCC_CLANG_DIAG_OFF("format-nonliteral")
    snprintf(member_name, H5FD_FAM_MEMB_NAME_BUF_SIZE, filename, 0);
    snprintf(temp, H5FD_FAM_MEMB_NAME_BUF_SIZE, filename, 1);
    H5_GCC_CLANG_DIAG_ON("format-nonliteral")

    if (!strcmp(member_name, temp)) {
        if (default_config) {
            temp = H5MM_xfree(temp);
            if (NULL == (temp = H5FD__family_get_default_printf_filename(filename)))
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get default printf-style filename");
            filename = temp;
        }
        else
            HGOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL,
                        "provided file name cannot generate unique sub-files");
    }

    /* Delete all the family members */
    current_member = 0;
    while (1) {
        /* Fix up the filename with the current member's number */
        H5_GCC_CLANG_DIAG_OFF("format-nonliteral")
        snprintf(member_name, H5FD_FAM_MEMB_NAME_BUF_SIZE, filename, current_member);
        H5_GCC_CLANG_DIAG_ON("format-nonliteral")

        /* Attempt to delete the member files. If the first file throws an error
         * we always consider this an error. With subsequent member files, however,
         * errors usually mean that we hit the last member file so we ignore them.
         *
         * Note that this means that any missing files in the family will leave
         * undeleted members behind.
         */
        H5E_BEGIN_TRY
        {
            delete_error = H5FD_delete(member_name, memb_fapl_id);
        }
        H5E_END_TRY
        if (FAIL == delete_error) {
            if (0 == current_member)
                HGOTO_ERROR(H5E_VFL, H5E_CANTDELETEFILE, FAIL, "unable to delete member file");
            else
                H5E_clear_stack(NULL);
            break;
        }
        current_member++;
    } /* end while */

done:
    if (member_name)
        H5MM_xfree(member_name);
    if (temp)
        H5MM_xfree(temp);

    /* Only close memb_fapl_id if we created one from the default configuration */
    if (default_fa.memb_fapl_id >= 0 && H5I_dec_ref(default_fa.memb_fapl_id) < 0)
        HDONE_ERROR(H5E_VFL, H5E_CANTDEC, FAIL, "can't decrement ref. count on member FAPL ID");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__family_delete() */
