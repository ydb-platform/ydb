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

/****************/
/* Module Setup */
/****************/

#define H5G_FRIEND     /*suppress error about including H5Gpkg   */
#include "H5Lmodule.h" /* This source code file is part of the H5L module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5ACprivate.h" /* Metadata cache                       */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Fprivate.h"  /* Files                                */
#include "H5Gpkg.h"      /* Groups                               */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5Lpkg.h"      /* Links                                */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Opublic.h"   /* File objects                         */
#include "H5Pprivate.h"  /* Property lists                       */
#include "H5VLprivate.h" /* Virtual Object Layer                 */

/****************/
/* Local Macros */
/****************/

/* Size of local link name buffer for traversing external links */
#define H5L_EXT_TRAVERSE_BUF_SIZE 256

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/
static hid_t   H5L__extern_traverse(const char *link_name, hid_t cur_group, const void *udata,
                                    size_t udata_size, hid_t lapl_id, hid_t dxpl_id);
static ssize_t H5L__extern_query(const char *link_name, const void *udata, size_t udata_size,
                                 void *buf /*out*/, size_t buf_size);

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Default External Link link class */
static const H5L_class_t H5L_EXTERN_LINK_CLASS[1] = {{
    H5L_LINK_CLASS_T_VERS, /* H5L_class_t version            */
    H5L_TYPE_EXTERNAL,     /* Link type id number            */
    "external",            /* Link name for debugging        */
    NULL,                  /* Creation callback              */
    NULL,                  /* Move callback                  */
    NULL,                  /* Copy callback                  */
    H5L__extern_traverse,  /* The actual traversal function  */
    NULL,                  /* Deletion callback              */
    H5L__extern_query      /* Query callback                 */
}};

/*-------------------------------------------------------------------------
 * Function:	H5L__extern_traverse
 *
 * Purpose:    Default traversal function for external links. This can
 *              be overridden using H5Lregister().
 *
 *              Given a filename and path packed into the link udata,
 *              attempts to open an object within an external file.
 *              If the H5L_ELINK_PREFIX_NAME property is set in the
 *              link access property list, appends that prefix to the
 *              filename being opened.
 *
 * Return:    ID of the opened object on success/H5I_INVALID_HID on failure
 *
 *-------------------------------------------------------------------------
 */
static hid_t
H5L__extern_traverse(const char H5_ATTR_UNUSED *link_name, hid_t cur_group, const void *_udata,
                     size_t H5_ATTR_UNUSED udata_size, hid_t lapl_id, hid_t H5_ATTR_UNUSED dxpl_id)
{
    H5P_genplist_t    *plist;                              /* Property list pointer */
    H5G_loc_t          root_loc;                           /* Location of root group in external file */
    H5G_loc_t          loc;                                /* Location of object */
    H5F_t             *ext_file = NULL;                    /* File struct for external file */
    const uint8_t     *p        = (const uint8_t *)_udata; /* Pointer into external link buffer */
    const char        *file_name;                    /* Name of file containing external link's object */
    const char        *obj_name;                     /* Name external link's object */
    size_t             fname_len;                    /* Length of external link file name */
    unsigned           intent;                       /* File access permissions */
    H5L_elink_cb_t     cb_info;                      /* Callback info struct */
    hid_t              fapl_id    = H5I_INVALID_HID; /* File access property list for external link's file */
    void              *ext_obj    = NULL;            /* External link's object */
    hid_t              ext_obj_id = H5I_INVALID_HID; /* ID for external link's object */
    H5I_type_t         opened_type;                  /* ID type of external link's object */
    char              *parent_group_name = NULL;     /* Temporary pointer to group name */
    char               local_group_name[H5L_EXT_TRAVERSE_BUF_SIZE]; /* Local buffer to hold group name */
    H5P_genplist_t    *fa_plist;                                    /* File access property list pointer */
    H5F_close_degree_t fc_degree    = H5F_CLOSE_WEAK;               /* File close degree for target file */
    char              *elink_prefix = NULL;                         /* Pointer to elink prefix */
    hid_t              ret_value    = H5I_INVALID_HID;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(p);

    /* Check external link version & flags */
    if (((*p >> 4) & 0x0F) > H5L_EXT_VERSION)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, H5I_INVALID_HID, "bad version number for external link");
    if ((*p & 0x0F) & ~H5L_EXT_FLAGS_ALL)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, H5I_INVALID_HID, "bad flags for external link");
    p++;

    /* Gather some information from the external link's user data */
    file_name = (const char *)p;
    fname_len = strlen(file_name);
    obj_name  = (const char *)p + fname_len + 1;

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(lapl_id, H5P_LINK_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, H5I_INVALID_HID, "can't find object for ID");

    /* Get the fapl_id set for lapl_id if any */
    if (H5P_get(plist, H5L_ACS_ELINK_FAPL_NAME, &fapl_id) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5I_INVALID_HID, "can't get fapl for links");

    /* Get the location for the group holding the external link */
    if (H5G_loc(cur_group, &loc) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, H5I_INVALID_HID, "can't get object location");

    /* get the access flags set for lapl_id if any */
    if (H5P_get(plist, H5L_ACS_ELINK_FLAGS_NAME, &intent) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5I_INVALID_HID, "can't get elink file access flags");

    /* get the file access mode flags for the parent file, if they were not set
     * on lapl_id */
    if (intent == H5F_ACC_DEFAULT)
        intent = H5F_INTENT(loc.oloc->file);

    if ((fapl_id == H5P_DEFAULT) && ((fapl_id = H5F_get_access_plist(loc.oloc->file, false)) < 0))
        HGOTO_ERROR(H5E_LINK, H5E_CANTGET, H5I_INVALID_HID, "can't get parent's file access property list");

    /* Get callback_info */
    if (H5P_get(plist, H5L_ACS_ELINK_CB_NAME, &cb_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5I_INVALID_HID, "can't get elink callback info");

    /* Get file access property list */
    if (NULL == (fa_plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, H5I_INVALID_HID, "can't find object for ID");

    /* Make callback if it exists */
    if (cb_info.func) {
        const char *parent_file_name;   /* Parent file name */
        size_t      group_name_len = 0; /* Length of parent group name */

        /* Get parent file name */
        parent_file_name = H5F_OPEN_NAME(loc.oloc->file);

        /* Query length of parent group name */
        if (H5G_get_name(&loc, NULL, (size_t)0, &group_name_len, NULL) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, H5I_INVALID_HID, "unable to retrieve length of group name");

        /* Account for null terminator */
        group_name_len++;

        /* Check if we need to allocate larger buffer */
        if (group_name_len > sizeof(local_group_name)) {
            if (NULL == (parent_group_name = (char *)H5MM_malloc(group_name_len)))
                HGOTO_ERROR(H5E_LINK, H5E_CANTALLOC, H5I_INVALID_HID,
                            "can't allocate buffer to hold group name, group_name_len = %zu", group_name_len);
        } /* end if */
        else
            parent_group_name = local_group_name;

        /* Get parent group name */
        if (H5G_get_name(&loc, parent_group_name, group_name_len, NULL, NULL) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CANTGET, H5I_INVALID_HID, "unable to retrieve group name");

        /* Make callback */
        if ((cb_info.func)(parent_file_name, parent_group_name, file_name, obj_name, &intent, fapl_id,
                           cb_info.user_data) < 0)
            HGOTO_ERROR(H5E_LINK, H5E_CALLBACK, H5I_INVALID_HID, "traversal operator failed");

        /* Check access flags */
        if ((intent & H5F_ACC_TRUNC) || (intent & H5F_ACC_EXCL))
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, H5I_INVALID_HID, "invalid file open flags");
    } /* end if */

    /* Set file close degree for new file to "weak" */
    if (H5P_set(fa_plist, H5F_ACS_CLOSE_DEGREE_NAME, &fc_degree) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, H5I_INVALID_HID, "can't set file close degree");

    /* Get the current elink prefix */
    if (H5P_peek(plist, H5L_ACS_ELINK_PREFIX_NAME, &elink_prefix) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, H5I_INVALID_HID, "can't get external link prefix");

    /* Search for the target file */
    if (NULL == (ext_file = H5F_prefix_open_file(loc.oloc->file, H5F_PREFIX_ELINK, elink_prefix, file_name,
                                                 intent, fapl_id)))
        HGOTO_ERROR(H5E_LINK, H5E_CANTOPENFILE, H5I_INVALID_HID,
                    "unable to open external file, external link file name = '%s'", file_name);

    /* Retrieve the "group location" for the file's root group */
    if (H5G_root_loc(ext_file, &root_loc) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_BADVALUE, H5I_INVALID_HID, "unable to create location for file");

    /* Open the object referenced in the external file */
    if (NULL == (ext_obj = H5O_open_name(&root_loc, obj_name, &opened_type)))
        HGOTO_ERROR(H5E_LINK, H5E_CANTOPENOBJ, H5I_INVALID_HID, "unable to open object");

    /* Get an ID for the external link's object */
    if ((ext_obj_id = H5VL_wrap_register(opened_type, ext_obj, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register external link object");

    /* Set return value */
    ret_value = ext_obj_id;

done:
    /* XXX (VOL MERGE): Probably also want to consider closing ext_obj here on failures */
    /* Release resources */
    if (fapl_id > 0 && H5I_dec_ref(fapl_id) < 0)
        HDONE_ERROR(H5E_ID, H5E_CANTRELEASE, H5I_INVALID_HID,
                    "unable to close ID for file access property list");
    if (ext_file && H5F_efc_close(loc.oloc->file, ext_file) < 0)
        HDONE_ERROR(H5E_LINK, H5E_CANTCLOSEFILE, H5I_INVALID_HID, "problem closing external file");
    if (parent_group_name && parent_group_name != local_group_name)
        parent_group_name = (char *)H5MM_xfree(parent_group_name);
    if (ret_value < 0) {
        /* Close object if it's open and something failed */
        if (ext_obj_id >= 0 && H5I_dec_ref(ext_obj_id) < 0)
            HDONE_ERROR(H5E_ID, H5E_CANTRELEASE, H5I_INVALID_HID, "unable to close ID for external object");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__extern_traverse() */

/*-------------------------------------------------------------------------
 * Function:	H5L__extern_query
 *
 * Purpose:    Default query function for external links. This can
 *              be overridden using H5Lregister().
 *
 *              Returns the size of the link's user data. If a buffer of
 *              is provided, copies at most buf_size bytes of the udata
 *              into it.
 *
 * Return:    Size of buffer on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static ssize_t
H5L__extern_query(const char H5_ATTR_UNUSED *link_name, const void *_udata, size_t udata_size,
                  void *buf /*out*/, size_t buf_size)
{
    const uint8_t *udata     = (const uint8_t *)_udata; /* Pointer to external link buffer */
    ssize_t        ret_value = SUCCEED;                 /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check external link version & flags */
    if (((*udata >> 4) & 0x0F) != H5L_EXT_VERSION)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, FAIL, "bad version number for external link");
    if ((*udata & 0x0F) & ~H5L_EXT_FLAGS_ALL)
        HGOTO_ERROR(H5E_LINK, H5E_CANTDECODE, FAIL, "bad flags for external link");

    /* If the buffer is NULL, skip writing anything in it and just return
     * the size needed */
    if (buf) {
        if (udata_size < buf_size)
            buf_size = udata_size;

        /* Copy the udata verbatim up to buf_size */
        H5MM_memcpy(buf, udata, buf_size);
    } /* end if */

    /* Set return value */
    ret_value = (ssize_t)udata_size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L__extern_query() */

/*-------------------------------------------------------------------------
 * Function: H5L_register_external
 *
 * Purpose: Registers default "External Link" link class.
 *              Use during library initialization or to restore the default
 *              after users change it.
 *
 * Return: Non-negative on success/ negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5L_register_external(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (H5L_register(H5L_EXTERN_LINK_CLASS) < 0)
        HGOTO_ERROR(H5E_LINK, H5E_NOTREGISTERED, FAIL, "unable to register external link class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5L_register_external() */
