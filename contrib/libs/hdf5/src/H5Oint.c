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

/*-------------------------------------------------------------------------
 *
 * Created:     H5O.c
 *
 * Purpose:     Internal object header routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Omodule.h" /* This source code file is part of the H5O module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5CXprivate.h" /* API Contexts                             */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* File access                              */
#include "H5FLprivate.h" /* Free lists                               */
#include "H5FOprivate.h" /* File objects                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5Lprivate.h"  /* Links                                    */
#include "H5MFprivate.h" /* File memory management                   */
#include "H5MMprivate.h" /* Memory management                        */
#include "H5Opkg.h"      /* Object headers                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* User data for recursive traversal over objects from a group */
typedef struct {
    hid_t          obj_id;    /* The ID for the starting group */
    H5G_loc_t     *start_loc; /* Location of starting group */
    H5SL_t        *visited;   /* Skip list for tracking visited nodes */
    H5O_iterate2_t op;        /* Application callback */
    void          *op_data;   /* Application's op data */
    unsigned       fields;    /* Selection of object info */
} H5O_iter_visit_ud_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

static herr_t H5O__delete_oh(H5F_t *f, H5O_t *oh);
static herr_t H5O__obj_type_real(const H5O_t *oh, H5O_type_t *obj_type);
static herr_t H5O__get_hdr_info_real(const H5O_t *oh, H5O_hdr_info_t *hdr);
static herr_t H5O__free_visit_visited(void *item, void *key, void *operator_data /*in,out*/);
static herr_t H5O__visit_cb(hid_t group, const char *name, const H5L_info2_t *linfo, void *_udata);
static const H5O_obj_class_t *H5O__obj_class_real(const H5O_t *oh);
static herr_t                 H5O__reset_info2(H5O_info2_t *oinfo);

/*********************/
/* Package Variables */
/*********************/

/* Header message ID to class mapping
 *
 * Remember to increment H5O_MSG_TYPES in H5Opkg.h when adding a new
 * message.
 */
const H5O_msg_class_t *const H5O_msg_class_g[] = {
    H5O_MSG_NULL,     /*0x0000 Null                            */
    H5O_MSG_SDSPACE,  /*0x0001 Dataspace                       */
    H5O_MSG_LINFO,    /*0x0002 Link information                */
    H5O_MSG_DTYPE,    /*0x0003 Datatype                        */
    H5O_MSG_FILL,     /*0x0004 Old data storage -- fill value  */
    H5O_MSG_FILL_NEW, /*0x0005 New data storage -- fill value  */
    H5O_MSG_LINK,     /*0x0006 Link                            */
    H5O_MSG_EFL,      /*0x0007 Data storage -- external data files */
    H5O_MSG_LAYOUT,   /*0x0008 Data Layout                     */
#ifdef H5O_ENABLE_BOGUS
    H5O_MSG_BOGUS_VALID, /*0x0009 "Bogus valid" (for testing)     */
#else                    /* H5O_ENABLE_BOGUS */
    NULL, /*0x0009 "Bogus valid" (for testing)     */
#endif                   /* H5O_ENABLE_BOGUS */
    H5O_MSG_GINFO,       /*0x000A Group information               */
    H5O_MSG_PLINE,       /*0x000B Data storage -- filter pipeline */
    H5O_MSG_ATTR,        /*0x000C Attribute                       */
    H5O_MSG_NAME,        /*0x000D Object name                     */
    H5O_MSG_MTIME,       /*0x000E Object modification date and time */
    H5O_MSG_SHMESG,      /*0x000F File-wide shared message table  */
    H5O_MSG_CONT,        /*0x0010 Object header continuation      */
    H5O_MSG_STAB,        /*0x0011 Symbol table                    */
    H5O_MSG_MTIME_NEW,   /*0x0012 New Object modification date and time */
    H5O_MSG_BTREEK,      /*0x0013 Non-default v1 B-tree 'K' values */
    H5O_MSG_DRVINFO,     /*0x0014 Driver info settings            */
    H5O_MSG_AINFO,       /*0x0015 Attribute information           */
    H5O_MSG_REFCOUNT,    /*0x0016 Object's ref. count             */
    H5O_MSG_FSINFO,      /*0x0017 Free-space manager info         */
    H5O_MSG_MDCI,        /*0x0018 Metadata cache image            */
    H5O_MSG_UNKNOWN      /*0x0019 Placeholder for unknown message */
};

/* Format version bounds for object header */
const unsigned H5O_obj_ver_bounds[] = {
    H5O_VERSION_1,     /* H5F_LIBVER_EARLIEST */
    H5O_VERSION_2,     /* H5F_LIBVER_V18 */
    H5O_VERSION_2,     /* H5F_LIBVER_V110 */
    H5O_VERSION_2,     /* H5F_LIBVER_V112 */
    H5O_VERSION_LATEST /* H5F_LIBVER_LATEST */
};

/* Declare a free list to manage the H5O_t struct */
H5FL_DEFINE(H5O_t);

/* Declare a free list to manage the H5O_mesg_t sequence information */
H5FL_SEQ_DEFINE(H5O_mesg_t);

/* Declare a free list to manage the H5O_chunk_t sequence information */
H5FL_SEQ_DEFINE(H5O_chunk_t);

/* Declare a free list to manage the chunk image information */
H5FL_BLK_DEFINE(chunk_image);

/* Declare external the free list for H5O_cont_t sequences */
H5FL_SEQ_EXTERN(H5O_cont_t);

/* The canonical 'undefined' token */
const H5O_token_t H5O_TOKEN_UNDEF_g = {
    {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}};

/*****************************/
/* Library Private Variables */
/*****************************/

/* Declare external the free list for time_t's */
H5FL_EXTERN(time_t);

/* Declare external the free list for H5_obj_t's */
H5FL_EXTERN(H5_obj_t);

/*******************/
/* Local Variables */
/*******************/

/* Header object ID to class mapping */
/*
 * Initialize the object class info table.  Begin with the most general types
 * and end with the most specific. For instance, any object that has a
 * datatype message is a datatype but only some of them are datasets.
 */
static const H5O_obj_class_t *const H5O_obj_class_g[] = {
    H5O_OBJ_DATATYPE, /* Datatype object (H5O_TYPE_NAMED_DATATYPE - 2) */
    H5O_OBJ_DATASET,  /* Dataset object (H5O_TYPE_DATASET - 1) */
    H5O_OBJ_GROUP,    /* Group object (H5O_TYPE_GROUP - 0) */
};

/*-------------------------------------------------------------------------
 * Function:    H5O_init
 *
 * Purpose:     Initialize the interface from some other layer.
 *
 * Return:      Success:        non-negative
 *              Failure:        negative
 *-------------------------------------------------------------------------
 */
herr_t
H5O_init(void)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* H5O interface sanity checks */
    HDcompile_assert(H5O_MSG_TYPES == NELMTS(H5O_msg_class_g));
    HDcompile_assert(sizeof(H5O_fheap_id_t) == H5O_FHEAP_ID_LEN);

    HDcompile_assert(H5O_UNKNOWN_ID < H5O_MSG_TYPES);

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5O__set_version
 *
 * Purpose:     Sets the correct version to encode the object header.
 *              Chooses the oldest version possible, unless the file's
 *              low bound indicates otherwise.
 *
 * Return:  Success:    Non-negative
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__set_version(H5F_t *f, H5O_t *oh, uint8_t oh_flags, bool store_msg_crt_idx)
{
    uint8_t version;             /* Message version */
    herr_t  ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(oh);

    /* Set the correct version to encode object header with */
    if (store_msg_crt_idx || (oh_flags & H5O_HDR_ATTR_CRT_ORDER_TRACKED))
        version = H5O_VERSION_LATEST;
    else
        version = H5O_VERSION_1;

    /* Upgrade to the version indicated by the file's low bound if higher */
    version = (uint8_t)MAX(version, (uint8_t)H5O_obj_ver_bounds[H5F_LOW_BOUND(f)]);

    /* Version bounds check */
    if (version > H5O_obj_ver_bounds[H5F_HIGH_BOUND(f)])
        HGOTO_ERROR(H5E_OHDR, H5E_BADRANGE, FAIL, "object header version out of bounds");

    /* Set the message version */
    oh->version = version;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__set_version() */

/*-------------------------------------------------------------------------
 * Function:    H5O_create
 *
 * Purpose:    Creates a new object header. Allocates space for it and
 *              then calls an initialization function. The object header
 *              is opened for write access and should eventually be
 *              closed by calling H5O_close().
 *
 * Return:    Success:    Non-negative, the ENT argument contains
 *                information about the object header,
 *                including its address.
 *
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_create(H5F_t *f, size_t size_hint, size_t initial_rc, hid_t ocpl_id, H5O_loc_t *loc /*out*/)
{
    H5O_t *oh        = NULL;
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(loc);
    assert(true == H5P_isa_class(ocpl_id, H5P_OBJECT_CREATE));

    /* create object header in freelist
     * header version is set internally
     */
    oh = H5O_create_ohdr(f, ocpl_id);
    if (NULL == oh)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "Can't instantiate object header");

    /* apply object header information to file
     */
    if (H5O_apply_ohdr(f, oh, ocpl_id, size_hint, initial_rc, loc) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, FAIL, "Can't apply object header to file");

done:
    if ((FAIL == ret_value) && (NULL != oh) && (H5O__free(oh, true) < 0))
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "can't delete object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_create() */

/*-----------------------------------------------------------------------------
 * Function:   H5O_create_ohdr
 *
 * Purpose:    Create the object header, set version and flags.
 *
 * Return:     Success: Pointer to the newly-crated header object.
 *             Failure: NULL
 *
 *-----------------------------------------------------------------------------
 */
H5O_t *
H5O_create_ohdr(H5F_t *f, hid_t ocpl_id)
{
    H5P_genplist_t *oc_plist;
    H5O_t          *oh = NULL; /* Object header in Freelist */
    uint8_t         oh_flags;  /* Initial status flags */
    H5O_t          *ret_value = NULL;

    FUNC_ENTER_NOAPI(NULL)

    assert(f);
    assert(true == H5P_isa_class(ocpl_id, H5P_OBJECT_CREATE));

    /* Check for invalid access request */
    if (0 == (H5F_INTENT(f) & H5F_ACC_RDWR))
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "no write intent on file");

    oh = H5FL_CALLOC(H5O_t);
    if (NULL == oh)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    oc_plist = (H5P_genplist_t *)H5I_object(ocpl_id);
    if (NULL == oc_plist)
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, NULL, "not a property list");

    /* Get any object header status flags set by properties */
    if (H5P_DATASET_CREATE_DEFAULT == ocpl_id) {
        /* If the OCPL is the default DCPL, we can get the header flags from the
         * API context. Otherwise we have to call H5P_get */
        if (H5CX_get_ohdr_flags(&oh_flags) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get object header flags");
    }
    else {
        if (H5P_get(oc_plist, H5O_CRT_OHDR_FLAGS_NAME, &oh_flags) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get object header flags");
    }

    if (H5O__set_version(f, oh, oh_flags, H5F_STORE_MSG_CRT_IDX(f)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, NULL, "can't set version of object header");

    oh->flags = oh_flags;

    ret_value = oh;

done:
    if ((NULL == ret_value) && (NULL != oh) && (H5O__free(oh, true) < 0))
        HDONE_ERROR(H5E_OHDR, H5E_CANTFREE, NULL, "can't delete object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O_create_ohdr() */

/*-----------------------------------------------------------------------------
 * Function:   H5O_apply_ohdr
 *
 * Purpose:    Initialize and set the object header in the file.
 *             Record some information at `loc_out`.
 *
 * Return:     Success: SUCCEED (0) (non-negative value)
 *             Failure: FAIL (-1) (negative value)
 *
 *-----------------------------------------------------------------------------
 */
herr_t
H5O_apply_ohdr(H5F_t *f, H5O_t *oh, hid_t ocpl_id, size_t size_hint, size_t initial_rc, H5O_loc_t *loc_out)
{
    haddr_t         oh_addr;
    size_t          oh_size;
    H5P_genplist_t *oc_plist     = NULL;
    unsigned        insert_flags = H5AC__NO_FLAGS_SET;
    herr_t          ret_value    = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(loc_out);
    assert(oh);
    assert(true == H5P_isa_class(ocpl_id, H5P_OBJECT_CREATE));

    /* Allocate at least a reasonable size for the object header */
    size_hint = H5O_ALIGN_F(f, MAX(H5O_MIN_SIZE, size_hint));

    oh->sizeof_size = H5F_SIZEOF_SIZE(f);
    oh->sizeof_addr = H5F_SIZEOF_ADDR(f);
    oh->swmr_write  = !!(H5F_INTENT(f) & H5F_ACC_SWMR_WRITE); /* funky cast */

#ifdef H5O_ENABLE_BAD_MESG_COUNT
    /* Check whether the "bad message count" property is set */
    if (0 < H5P_exist_plist(oc_plist, H5O_BAD_MESG_COUNT_NAME))
        /* Get bad message count flag -- from property list */
        if (H5P_get(oc_plist, H5O_BAD_MESG_COUNT_NAME, &oh->store_bad_mesg_count) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't get bad message count flag");
#endif /* H5O_ENABLE_BAD_MESG_COUNT */

    /* Create object header proxy if doing SWMR writes */
    if (oh->swmr_write) {
        oh->proxy = H5AC_proxy_entry_create();
        if (NULL == oh->proxy)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, FAIL, "can't create object header proxy");
    }
    else {
        oh->proxy = NULL;
    }

    oc_plist = (H5P_genplist_t *)H5I_object(ocpl_id);
    if (NULL == oc_plist)
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a property list");

    /* Initialize version-specific fields */
    if (oh->version > H5O_VERSION_1) {
        /* Initialize all time fields */
        if (oh->flags & H5O_HDR_STORE_TIMES)
            oh->atime = oh->mtime = oh->ctime = oh->btime = H5_now();
        else
            oh->atime = oh->mtime = oh->ctime = oh->btime = 0;

        if (H5F_STORE_MSG_CRT_IDX(f))
            /* flag to record message creation indices */
            oh->flags |= H5O_HDR_ATTR_CRT_ORDER_TRACKED;

        /* Get attribute storage phase change values -- from property list */
        if (H5P_get(oc_plist, H5O_CRT_ATTR_MAX_COMPACT_NAME, &oh->max_compact) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get max. # of compact attributes");
        if (H5P_get(oc_plist, H5O_CRT_ATTR_MIN_DENSE_NAME, &oh->min_dense) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get min. # of dense attributes");

        /* Check for non-default attribute storage phase change values */
        if (H5O_CRT_ATTR_MAX_COMPACT_DEF != oh->max_compact || H5O_CRT_ATTR_MIN_DENSE_DEF != oh->min_dense)
            oh->flags |= H5O_HDR_ATTR_STORE_PHASE_CHANGE;

            /* Determine correct value for chunk #0 size bits */
/* Avoid compiler warning on 32-bit machines */
#if H5_SIZEOF_SIZE_T > H5_SIZEOF_INT32_T
        if (size_hint > 4294967295UL)
            oh->flags |= H5O_HDR_CHUNK0_8;
        else if (size_hint > 65535)
            oh->flags |= H5O_HDR_CHUNK0_4;
        else if (size_hint > 255)
            oh->flags |= H5O_HDR_CHUNK0_2;
#else
        if (size_hint > 65535)
            oh->flags |= H5O_HDR_CHUNK0_4;
        else if (size_hint > 255)
            oh->flags |= H5O_HDR_CHUNK0_2;
#endif
    }
    else {
        /* Reset unused time fields */
        oh->atime = oh->mtime = oh->ctime = oh->btime = 0;
    } /* end if/else header version >1 */

    /* Compute total size of initial object header */
    /* (i.e. object header prefix and first chunk) */
    oh_size = (size_t)H5O_SIZEOF_HDR(oh) + size_hint;

    /* Allocate disk space for header and first chunk */
    oh_addr = H5MF_alloc(f, H5FD_MEM_OHDR, (hsize_t)oh_size);
    if (HADDR_UNDEF == oh_addr)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "file allocation failed for object header");

    /* Create the chunk list */
    oh->nchunks       = 1;
    oh->alloc_nchunks = 1;
    oh->chunk         = H5FL_SEQ_MALLOC(H5O_chunk_t, (size_t)oh->alloc_nchunks);
    if (NULL == oh->chunk)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the first chunk */
    oh->chunk[0].addr = oh_addr;
    oh->chunk[0].size = oh_size;
    oh->chunk[0].gap  = 0;

    /* Allocate enough space for the first chunk */
    /* (including space for serializing the object header prefix */
    oh->chunk[0].image = H5FL_BLK_CALLOC(chunk_image, oh_size);
    if (NULL == oh->chunk[0].image)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    oh->chunk[0].chunk_proxy = NULL;

    /* Put magic # for object header in first chunk */
    if (H5O_VERSION_1 < oh->version)
        H5MM_memcpy(oh->chunk[0].image, H5O_HDR_MAGIC, (size_t)H5_SIZEOF_MAGIC);

    /* Create the message list */
    oh->nmesgs       = 1;
    oh->alloc_nmesgs = H5O_NMESGS;
    oh->mesg         = H5FL_SEQ_CALLOC(H5O_mesg_t, oh->alloc_nmesgs);
    if (NULL == oh->mesg)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");

    /* Initialize the initial "null" message; covers the entire first chunk */
    oh->mesg[0].type   = H5O_MSG_NULL;
    oh->mesg[0].dirty  = true;
    oh->mesg[0].native = NULL;
    oh->mesg[0].raw =
        oh->chunk[0].image + H5O_SIZEOF_HDR(oh) - H5O_SIZEOF_CHKSUM_OH(oh) + H5O_SIZEOF_MSGHDR_OH(oh);
    oh->mesg[0].raw_size = size_hint - (size_t)H5O_SIZEOF_MSGHDR_OH(oh);
    oh->mesg[0].chunkno  = 0;

    /* Check for non-zero initial refcount on the object header */
    if (initial_rc > 0) {
        /* Set the initial refcount & pin the header when its inserted */
        oh->rc = initial_rc;
        insert_flags |= H5AC__PIN_ENTRY_FLAG;
    }

    /* Set metadata tag in API context */
    H5_BEGIN_TAG(oh_addr)

    /* Cache object header */
    if (H5AC_insert_entry(f, H5AC_OHDR, oh_addr, oh, insert_flags) < 0)
        HGOTO_ERROR_TAG(H5E_OHDR, H5E_CANTINSERT, FAIL, "unable to cache object header");

    /* Reset object header pointer, now that it's been inserted into the cache */
    oh = NULL;

    /* Reset metadata tag in API context */
    H5_END_TAG

    /* Set up object location */
    loc_out->file = f;
    loc_out->addr = oh_addr;

    if (H5O_open(loc_out) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5O_apply_ohdr() */

/*-------------------------------------------------------------------------
 * Function:    H5O_open
 *
 * Purpose:    Opens an object header which is described by the symbol table
 *        entry OBJ_ENT.
 *
 * Return:    Non-negative on success/Negative on failure
 *-------------------------------------------------------------------------
 */
herr_t
H5O_open(H5O_loc_t *loc)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /* Check args */
    assert(loc);
    assert(loc->file);

#ifdef H5O_DEBUG
    if (H5DEBUG(O))
        fprintf(H5DEBUG(O), "> %" PRIuHADDR "\n", loc->addr);
#endif

    /* Turn off the variable for holding file or increment open-lock counters */
    if (loc->holding_file)
        loc->holding_file = false;
    else
        H5F_INCR_NOPEN_OBJS(loc->file);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_open() */

/*-------------------------------------------------------------------------
 * Function:    H5O_open_name
 *
 * Purpose:     Opens an object by name
 *
 * Return:      Success:    Pointer to object data
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O_open_name(const H5G_loc_t *loc, const char *name, H5I_type_t *opened_type)
{
    H5G_loc_t  obj_loc;           /* Location used to open group */
    H5G_name_t obj_path;          /* Opened object group hier. path */
    H5O_loc_t  obj_oloc;          /* Opened object object location */
    bool       loc_found = false; /* Entry at 'name' found */
    void      *ret_value = NULL;  /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Check args */
    assert(loc);
    assert(name && *name);

    /* Set up opened group location to fill in */
    obj_loc.oloc = &obj_oloc;
    obj_loc.path = &obj_path;
    H5G_loc_reset(&obj_loc);

    /* Find the object's location */
    if (H5G_loc_find(loc, name, &obj_loc /*out*/) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, NULL, "object not found");
    loc_found = true;

    /* Open the object */
    if (NULL == (ret_value = H5O_open_by_loc(&obj_loc, opened_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open object");

done:
    if (NULL == ret_value)
        if (loc_found && H5G_loc_free(&obj_loc) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, NULL, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_open_name() */

/*-------------------------------------------------------------------------
 * Function:    H5O__open_by_idx
 *
 * Purpose:     Internal routine to open an object by index within group
 *
 * Return:      Success:    Pointer to object data
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O__open_by_idx(const H5G_loc_t *loc, const char *name, H5_index_t idx_type, H5_iter_order_t order,
                 hsize_t n, H5I_type_t *opened_type)
{
    H5G_loc_t  obj_loc;           /* Location used to open group */
    H5G_name_t obj_path;          /* Opened object group hier. path */
    H5O_loc_t  obj_oloc;          /* Opened object object location */
    bool       loc_found = false; /* Entry at 'name' found */
    void      *ret_value = NULL;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);

    /* Set up opened group location to fill in */
    obj_loc.oloc = &obj_oloc;
    obj_loc.path = &obj_path;
    H5G_loc_reset(&obj_loc);

    /* Find the object's location, according to the order in the index */
    if (H5G_loc_find_by_idx(loc, name, idx_type, order, n, &obj_loc /*out*/) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, NULL, "group not found");
    loc_found = true;

    /* Open the object */
    if (NULL == (ret_value = H5O_open_by_loc(&obj_loc, opened_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open object");

done:
    /* Release the object location if we failed after copying it */
    if (NULL == ret_value)
        if (loc_found && H5G_loc_free(&obj_loc) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, NULL, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__open_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5O__open_by_addr
 *
 * Purpose:     Internal routine to open an object by its address
 *
 * Return:      Success:    Pointer to object data
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O__open_by_addr(const H5G_loc_t *loc, haddr_t addr, H5I_type_t *opened_type)
{
    H5G_loc_t  obj_loc;          /* Location used to open group */
    H5G_name_t obj_path;         /* Opened object group hier. path */
    H5O_loc_t  obj_oloc;         /* Opened object object location */
    void      *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(loc);

    /* Set up opened group location to fill in */
    obj_loc.oloc = &obj_oloc;
    obj_loc.path = &obj_path;
    H5G_loc_reset(&obj_loc);
    obj_loc.oloc->addr = addr;
    obj_loc.oloc->file = loc->oloc->file;
    H5G_name_reset(obj_loc.path); /* objects opened through this routine don't have a path name */

    /* Open the object */
    if (NULL == (ret_value = H5O_open_by_loc(&obj_loc, opened_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__open_by_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5O_open_by_loc
 *
 * Purpose:     Opens an object
 *
 * Return:      Success:    Pointer to object data
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O_open_by_loc(const H5G_loc_t *obj_loc, H5I_type_t *opened_type)
{
    const H5O_obj_class_t *obj_class;        /* Class of object for location */
    void                  *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    assert(obj_loc);

    /* Get the object class for this location */
    if (NULL == (obj_class = H5O__obj_class(obj_loc->oloc)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to determine object class");

    /* Call the object class's 'open' routine */
    assert(obj_class->open);
    if (NULL == (ret_value = obj_class->open(obj_loc, opened_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_open_by_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5O_close
 *
 * Purpose:    Closes an object header that was previously open.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_close(H5O_loc_t *loc, bool *file_closed /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(loc);
    assert(loc->file);
    assert(H5F_NOPEN_OBJS(loc->file) > 0);

    /* Set the file_closed flag to the default value.
     * This flag lets downstream code know if the file struct is
     * still accessible and/or likely to contain useful data.
     * It's needed by the evict-on-close code. Clients can ignore
     * this value by passing in NULL.
     */
    if (file_closed)
        *file_closed = false;

    /* Decrement open-lock counters */
    H5F_DECR_NOPEN_OBJS(loc->file);

#ifdef H5O_DEBUG
    if (H5DEBUG(O)) {
        if (false == H5F_ID_EXISTS(loc->file) && 1 == H5F_NREFS(loc->file))
            fprintf(H5DEBUG(O), "< %" PRIuHADDR " auto %lu remaining\n", loc->addr,
                    (unsigned long)H5F_NOPEN_OBJS(loc->file));
        else
            fprintf(H5DEBUG(O), "< %" PRIuHADDR "\n", loc->addr);
    }
#endif

    /*
     * If the file open object count has reached the number of open mount points
     * (each of which has a group open in the file) attempt to close the file.
     */
    if (H5F_NOPEN_OBJS(loc->file) == H5F_NMOUNTS(loc->file))
        /* Attempt to close down the file hierarchy */
        if (H5F_try_close(loc->file, file_closed) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCLOSEFILE, FAIL, "problem attempting file close");

    /* Release location information */
    if (H5O_loc_free(loc) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "problem attempting to free location");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_close() */

/*-------------------------------------------------------------------------
 * Function:    H5O__link_oh
 *
 * Purpose:     Adjust the link count for an open object header by adding
 *              ADJUST to the link count.
 *
 * Return:      Success:    New link count
 *
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
int
H5O__link_oh(H5F_t *f, int adjust, H5O_t *oh, bool *deleted)
{
    haddr_t addr      = H5O_OH_GET_ADDR(oh); /* Object header address */
    int     ret_value = -1;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(oh);
    assert(deleted);

    /* Check for adjusting link count */
    if (adjust) {
        if (adjust < 0) {
            /* Check for too large of an adjustment */
            if ((unsigned)(-adjust) > oh->nlink)
                HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, (-1), "link count would be negative");

            /* Adjust the link count for the object header */
            oh->nlink = (unsigned)((int)oh->nlink + adjust);

            /* Mark object header as dirty in cache */
            if (H5AC_mark_entry_dirty(oh) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTMARKDIRTY, (-1), "unable to mark object header as dirty");

            /* Check if the object should be deleted */
            if (oh->nlink == 0) {
                /* Check if the object is still open by the user */
                if (H5FO_opened(f, addr) != NULL) {
                    /* Flag the object to be deleted when it's closed */
                    if (H5FO_mark(f, addr, true) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, (-1), "can't mark object for deletion");
                } /* end if */
                else {
                    /* Mark the object header for deletion */
                    *deleted = true;
                } /* end else */
            }     /* end if */
        }         /* end if */
        else {
            /* A new object, or one that will be deleted */
            if (0 == oh->nlink) {
                /* Check if the object is currently open, but marked for deletion */
                if (H5FO_marked(f, addr)) {
                    /* Remove "delete me" flag on the object */
                    if (H5FO_mark(f, addr, false) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, (-1), "can't mark object for deletion");
                } /* end if */
            }     /* end if */

            /* Adjust the link count for the object header */
            oh->nlink = (unsigned)((int)oh->nlink + adjust);

            /* Mark object header as dirty in cache */
            if (H5AC_mark_entry_dirty(oh) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTMARKDIRTY, (-1), "unable to mark object header as dirty");
        } /* end if */

        /* Check for operations on refcount message */
        if (oh->version > H5O_VERSION_1) {
            /* Check if the object has a refcount message already */
            if (oh->has_refcount_msg) {
                /* Check for removing refcount message */
                if (oh->nlink <= 1) {
                    if (H5O__msg_remove_real(f, oh, H5O_MSG_REFCOUNT, H5O_ALL, NULL, NULL, true) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, (-1), "unable to delete refcount message");
                    oh->has_refcount_msg = false;
                } /* end if */
                /* Update refcount message with new link count */
                else {
                    H5O_refcount_t refcount = oh->nlink;

                    if (H5O__msg_write_real(f, oh, H5O_MSG_REFCOUNT, H5O_MSG_FLAG_DONTSHARE, 0, &refcount) <
                        0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTUPDATE, (-1), "unable to update refcount message");
                } /* end else */
            }     /* end if */
            else {
                /* Check for adding refcount message to object */
                if (oh->nlink > 1) {
                    H5O_refcount_t refcount = oh->nlink;

                    if (H5O__msg_append_real(f, oh, H5O_MSG_REFCOUNT, H5O_MSG_FLAG_DONTSHARE, 0, &refcount) <
                        0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, (-1), "unable to create new refcount message");
                    oh->has_refcount_msg = true;
                } /* end if */
            }     /* end else */
        }         /* end if */
    }             /* end if */

    /* Set return value */
    ret_value = (int)oh->nlink;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__link_oh() */

/*-------------------------------------------------------------------------
 * Function:    H5O_link
 *
 * Purpose:    Adjust the link count for an object header by adding
 *        ADJUST to the link count.
 *
 * Return:    Success:    New link count
 *
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
int
H5O_link(const H5O_loc_t *loc, int adjust)
{
    H5O_t *oh        = NULL;
    bool   deleted   = false; /* Whether the object was deleted */
    int    ret_value = -1;    /* Return value */

    FUNC_ENTER_NOAPI_TAG(loc->addr, FAIL)

    /* check args */
    assert(loc);
    assert(loc->file);
    assert(H5_addr_defined(loc->addr));

    /* Pin the object header */
    if (NULL == (oh = H5O_pin(loc)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPIN, FAIL, "unable to pin object header");

    /* Call the "real" link routine */
    if ((ret_value = H5O__link_oh(loc->file, adjust, oh, &deleted)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_LINKCOUNT, FAIL, "unable to adjust object link count");

done:
    if (oh && H5O_unpin(oh) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPIN, FAIL, "unable to unpin object header");
    if (ret_value >= 0 && deleted && H5O_delete(loc->file, loc->addr) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTDELETE, FAIL, "can't delete object from file");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_link() */

/*-------------------------------------------------------------------------
 * Function:    H5O_protect
 *
 * Purpose:    Wrapper around H5AC_protect for use during a H5O_protect->
 *              H5O_msg_append->...->H5O_msg_append->H5O_unprotect sequence of calls
 *              during an object's creation.
 *
 * Return:    Success:    Pointer to the object header structure for the
 *                              object.
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5O_t *
H5O_protect(const H5O_loc_t *loc, unsigned prot_flags, bool pin_all_chunks)
{
    H5O_t          *oh = NULL;        /* Object header protected */
    H5O_cache_ud_t  udata;            /* User data for protecting object header */
    H5O_cont_msgs_t cont_msg_info;    /* Continuation message info */
    unsigned        file_intent;      /* R/W intent on file */
    H5O_t          *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_TAG(loc->addr, NULL)

    /* check args */
    assert(loc);
    assert(loc->file);

    /* prot_flags may only contain the H5AC__READ_ONLY_FLAG */
    assert((prot_flags & (unsigned)(~H5AC__READ_ONLY_FLAG)) == 0);

    /* Check for valid address */
    if (!H5_addr_defined(loc->addr))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "address undefined");

    /* Check for write access on the file */
    file_intent = H5F_INTENT(loc->file);
    if ((0 == (prot_flags & H5AC__READ_ONLY_FLAG)) && (0 == (file_intent & H5F_ACC_RDWR)))
        HGOTO_ERROR(H5E_OHDR, H5E_BADVALUE, NULL, "no write intent on file");

    /* Construct the user data for protect callback */
    udata.made_attempt            = false;
    udata.v1_pfx_nmesgs           = 0;
    udata.chunk0_size             = 0;
    udata.oh                      = NULL;
    udata.free_oh                 = false;
    udata.common.f                = loc->file;
    udata.common.file_intent      = file_intent;
    udata.common.merged_null_msgs = 0;
    memset(&cont_msg_info, 0, sizeof(cont_msg_info));
    udata.common.cont_msg_info = &cont_msg_info;
    udata.common.addr          = loc->addr;

    /* Lock the object header into the cache */
    if (NULL == (oh = (H5O_t *)H5AC_protect(loc->file, H5AC_OHDR, loc->addr, &udata, prot_flags)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to load object header");

    /* Check if there are any continuation messages to process */
    if (cont_msg_info.nmsgs > 0) {
        size_t             curr_msg;  /* Current continuation message to process */
        H5O_chk_cache_ud_t chk_udata; /* User data for loading chunk */

        /* Sanity check - we should only have continuation messages to process
         *      when the object header is actually loaded from the file.
         */
        assert(udata.made_attempt == true);
        assert(cont_msg_info.msgs);

        /* Construct the user data for protecting chunks */
        chk_udata.decoding                = true;
        chk_udata.oh                      = oh;
        chk_udata.chunkno                 = UINT_MAX; /* Set to invalid value, for better error detection */
        chk_udata.common.f                = loc->file;
        chk_udata.common.file_intent      = file_intent;
        chk_udata.common.merged_null_msgs = udata.common.merged_null_msgs;
        chk_udata.common.cont_msg_info    = &cont_msg_info;

        /* Read in continuation messages, until there are no more */
        /* (Note that loading chunks could increase the # of continuation
         *      messages if new ones are found - QAK, 19/11/2016)
         */
        curr_msg = 0;
        while (curr_msg < cont_msg_info.nmsgs) {
            H5O_chunk_proxy_t *chk_proxy; /* Proxy for chunk, to bring it into memory */
#ifndef NDEBUG
            size_t chkcnt = oh->nchunks; /* Count of chunks (for sanity checking) */
#endif                                   /* NDEBUG */

            /* Bring the chunk into the cache */
            /* (which adds to the object header) */
            chk_udata.common.addr = cont_msg_info.msgs[curr_msg].addr;
            chk_udata.size        = cont_msg_info.msgs[curr_msg].size;
            if (NULL == (chk_proxy = (H5O_chunk_proxy_t *)H5AC_protect(loc->file, H5AC_OHDR_CHK,
                                                                       cont_msg_info.msgs[curr_msg].addr,
                                                                       &chk_udata, prot_flags)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to load object header chunk");

            /* Sanity check */
            assert(chk_proxy->oh == oh);
            assert(chk_proxy->chunkno == chkcnt);
            assert(oh->nchunks == (chkcnt + 1));

            /* Release the chunk from the cache */
            if (H5AC_unprotect(loc->file, H5AC_OHDR_CHK, cont_msg_info.msgs[curr_msg].addr, chk_proxy,
                               H5AC__NO_FLAGS_SET) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to release object header chunk");

            /* Advance to next continuation message */
            curr_msg++;
        } /* end while */

        /* Release any continuation messages built up */
        cont_msg_info.msgs = (H5O_cont_t *)H5FL_SEQ_FREE(H5O_cont_t, cont_msg_info.msgs);

        /* Pass back out some of the chunk's user data */
        udata.common.merged_null_msgs = chk_udata.common.merged_null_msgs;
    } /* end if */

    /* Check for incorrect # of object header messages, if we've just loaded
     *  this object header from the file
     */
    if (udata.made_attempt) {
/* Don't enforce the error on an incorrect # of object header messages bug
 *      unless strict format checking is enabled.  This allows for older
 *      files, created with a version of the library that had a bug in tracking
 *      the correct # of header messages to be read in without the library
 *      erroring out here. -QAK
 */
#ifdef H5_STRICT_FORMAT_CHECKS
        /* Check for incorrect # of messages in v1 object header */
        if (oh->version == H5O_VERSION_1 &&
            (oh->nmesgs + udata.common.merged_null_msgs) != udata.v1_pfx_nmesgs)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, NULL, "corrupt object header - incorrect # of messages");
#endif /* H5_STRICT_FORMAT_CHECKS */
    }  /* end if */

#ifdef H5O_DEBUG
    H5O__assert(oh);
#endif /* H5O_DEBUG */

    /* Pin the other chunks also when requested, so that the object header
     *  proxy can be set up.
     */
    if (pin_all_chunks && oh->nchunks > 1) {
        unsigned u; /* Local index variable */

        /* Sanity check */
        assert(oh->swmr_write);

        /* Iterate over chunks > 0 */
        for (u = 1; u < oh->nchunks; u++) {
            H5O_chunk_proxy_t *chk_proxy; /* Chunk proxy */

            /* Protect chunk */
            if (NULL == (chk_proxy = H5O__chunk_protect(loc->file, oh, u)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to protect object header chunk");

            /* Pin chunk proxy*/
            if (H5AC_pin_protected_entry(chk_proxy) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTPIN, NULL, "unable to pin object header chunk");

            /* Unprotect chunk */
            if (H5O__chunk_unprotect(loc->file, chk_proxy, false) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to unprotect object header chunk");

            /* Preserve chunk proxy pointer for later */
            oh->chunk[u].chunk_proxy = chk_proxy;
        } /* end for */

        /* Set the flag for the unprotect */
        oh->chunks_pinned = true;
    } /* end if */

    /* Set return value */
    ret_value = oh;

done:
    if (ret_value == NULL && oh) {
        /* Release any continuation messages built up */
        if (cont_msg_info.msgs)
            cont_msg_info.msgs = (H5O_cont_t *)H5FL_SEQ_FREE(H5O_cont_t, cont_msg_info.msgs);

        if (H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to release object header");
    }

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_protect() */

/*-------------------------------------------------------------------------
 * Function:    H5O_pin
 *
 * Purpose:    Pin an object header down for use during a sequence of message
 *              operations, which prevents the object header from being
 *              evicted from the cache.
 *
 * Return:    Success:    Pointer to the object header structure for the
 *                              object.
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5O_t *
H5O_pin(const H5O_loc_t *loc)
{
    H5O_t *oh        = NULL; /* Object header */
    H5O_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* check args */
    assert(loc);

    /* Get header */
    if (NULL == (oh = H5O_protect(loc, H5AC__NO_FLAGS_SET, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to protect object header");

    /* Increment the reference count on the object header */
    /* (which will pin it, if appropriate) */
    if (H5O__inc_rc(oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINC, NULL, "unable to increment reference count on object header");

    /* Set the return value */
    ret_value = oh;

done:
    /* Release the object header from the cache */
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_pin() */

/*-------------------------------------------------------------------------
 * Function:    H5O_unpin
 *
 * Purpose:    Unpin an object header, allowing it to be evicted from the
 *              metadata cache.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_unpin(H5O_t *oh)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(oh);

    /* Decrement the reference count on the object header */
    /* (which will unpin it, if appropriate) */
    if (H5O__dec_rc(oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement reference count on object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_unpin() */

/*-------------------------------------------------------------------------
 * Function:    H5O_unprotect
 *
 * Purpose:    Wrapper around H5AC_unprotect for use during a H5O_protect->
 *              H5O_msg_append->...->H5O_msg_append->H5O_unprotect sequence of calls
 *              during an object's creation.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_unprotect(const H5O_loc_t *loc, H5O_t *oh, unsigned oh_flags)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(loc);
    assert(oh);

    /* Unpin the other chunks */
    if (oh->chunks_pinned && oh->nchunks > 1) {
        unsigned u; /* Local index variable */

        /* Sanity check */
        assert(oh->swmr_write);

        /* Iterate over chunks > 0 */
        for (u = 1; u < oh->nchunks; u++) {
            if (NULL != oh->chunk[u].chunk_proxy) {
                /* Release chunk proxy */
                if (H5AC_unpin_entry(oh->chunk[u].chunk_proxy) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPIN, FAIL, "unable to unpin object header chunk");
                oh->chunk[u].chunk_proxy = NULL;
            } /* end if */
        }     /* end for */

        /* Reet the flag from the unprotect */
        oh->chunks_pinned = false;
    } /* end if */

    /* Unprotect the object header */
    if (H5AC_unprotect(loc->file, H5AC_OHDR, oh->chunk[0].addr, oh, oh_flags) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_unprotect() */

/*-------------------------------------------------------------------------
 * Function:    H5O_touch_oh
 *
 * Purpose:    If FORCE is non-zero then create a modification time message
 *        unless one already exists.  Then update any existing
 *        modification time message with the current time.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_touch_oh(H5F_t *f, H5O_t *oh, bool force)
{
    H5O_chunk_proxy_t *chk_proxy   = NULL;  /* Chunk that message is in */
    bool               chk_dirtied = false; /* Flag for unprotecting chunk */
    time_t             now;                 /* Current time */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    assert(f);
    assert(oh);

    /* Check if this object header is tracking times */
    if (oh->flags & H5O_HDR_STORE_TIMES) {
        /* Get current time */
        now = H5_now();

        /* Check version, to determine how to store time information */
        if (oh->version == H5O_VERSION_1) {
            size_t idx; /* Index of modification time message to update */

            /* Look for existing message */
            for (idx = 0; idx < oh->nmesgs; idx++)
                if (H5O_MSG_MTIME == oh->mesg[idx].type || H5O_MSG_MTIME_NEW == oh->mesg[idx].type)
                    break;

            /* Create a new message, if necessary */
            if (idx == oh->nmesgs) {
                unsigned mesg_flags = 0; /* Flags for message in object header */

                /* If we would have to create a new message, but we aren't 'forcing' it, get out now */
                if (!force)
                    HGOTO_DONE(SUCCEED); /*nothing to do*/

                /* Allocate space for the modification time message */
                if (H5O__msg_alloc(f, oh, H5O_MSG_MTIME_NEW, &mesg_flags, &now, &idx) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL,
                                "unable to allocate space for modification time message");

                /* Set the message's flags if appropriate */
                oh->mesg[idx].flags = (uint8_t)mesg_flags;
            } /* end if */

            /* Protect chunk */
            if (NULL == (chk_proxy = H5O__chunk_protect(f, oh, oh->mesg[idx].chunkno)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header chunk");

            /* Allocate 'native' space, if necessary */
            if (NULL == oh->mesg[idx].native) {
                if (NULL == (oh->mesg[idx].native = H5FL_MALLOC(time_t)))
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL,
                                "memory allocation failed for modification time message");
            } /* end if */

            /* Update the message */
            *((time_t *)(oh->mesg[idx].native)) = now;

            /* Mark the message as dirty */
            oh->mesg[idx].dirty = true;
            chk_dirtied         = true;
        } /* end if */
        else {
            /* XXX: For now, update access time & change fields in the object header
             * (will need to add some code to update modification time appropriately)
             */
            oh->atime = oh->ctime = now;

            /* Mark object header as dirty in cache */
            if (H5AC_mark_entry_dirty(oh) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTMARKDIRTY, FAIL, "unable to mark object header as dirty");
        } /* end else */
    }     /* end if */

done:
    /* Release chunk */
    if (chk_proxy && H5O__chunk_unprotect(f, chk_proxy, chk_dirtied) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to unprotect object header chunk");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_touch_oh() */

/*-------------------------------------------------------------------------
 * Function:    H5O_touch
 *
 * Purpose:    Touch an object by setting the modification time to the
 *        current time and marking the object as dirty.  Unless FORCE
 *        is non-zero, nothing happens if there is no MTIME message in
 *        the object header.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_touch(const H5O_loc_t *loc, bool force)
{
    H5O_t   *oh        = NULL;               /* Object header to modify */
    unsigned oh_flags  = H5AC__NO_FLAGS_SET; /* Flags for unprotecting object header */
    herr_t   ret_value = SUCCEED;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(loc);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__NO_FLAGS_SET, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Create/Update the modification time message */
    if (H5O_touch_oh(loc->file, oh, force) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "unable to update object modification time");

    /* Mark object header as changed */
    oh_flags |= H5AC__DIRTIED_FLAG;

done:
    if (oh && H5O_unprotect(loc, oh, oh_flags) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_touch() */

#ifdef H5O_ENABLE_BOGUS

/*-------------------------------------------------------------------------
 * Function:    H5O_bogus_oh
 *
 * Purpose:    Create a "bogus" message unless one already exists.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_bogus_oh(H5F_t *f, H5O_t *oh, unsigned bogus_id, unsigned mesg_flags)
{
    size_t           idx;                 /* Local index variable */
    H5O_msg_class_t *type;                /* Message class type */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    assert(f);
    assert(oh);

    /* Look for existing message */
    for (idx = 0; idx < oh->nmesgs; idx++)
        if (H5O_MSG_BOGUS_VALID == oh->mesg[idx].type || H5O_MSG_BOGUS_INVALID == oh->mesg[idx].type)
            break;

    /* Create a new message */
    if (idx == oh->nmesgs) {
        H5O_bogus_t *bogus; /* Pointer to the bogus information */

        /* Allocate the native message in memory */
        if (NULL == (bogus = H5MM_malloc(sizeof(H5O_bogus_t))))
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "memory allocation failed for 'bogus' message");

        /* Update the native value */
        bogus->u = H5O_BOGUS_VALUE;

        if (bogus_id == H5O_BOGUS_VALID_ID)
            type = H5O_MSG_BOGUS_VALID;
        else if (bogus_id == H5O_BOGUS_INVALID_ID)
            type = H5O_MSG_BOGUS_INVALID;
        else
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "invalid ID for 'bogus' message");

        /* Allocate space in the object header for bogus message */
        if (H5O__msg_alloc(f, oh, type, &mesg_flags, bogus, &idx) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to allocate space for 'bogus' message");

        /* Point to "bogus" information (take it over) */
        oh->mesg[idx].native = bogus;

        /* Set the appropriate flags for the message */
        oh->mesg[idx].flags = mesg_flags;

        /* Mark the message and object header as dirty */
        oh->mesg[idx].dirty     = true;
        oh->cache_info.is_dirty = true;
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_bogus_oh() */
#endif /* H5O_ENABLE_BOGUS */

/*-------------------------------------------------------------------------
 * Function:    H5O_delete
 *
 * Purpose:    Delete an object header from a file.  This frees the file
 *              space used for the object header (and it's continuation blocks)
 *              and also walks through each header message and asks it to
 *              remove all the pieces of the file referenced by the header.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_delete(H5F_t *f, haddr_t addr)
{
    H5O_t    *oh = NULL;                     /* Object header information */
    H5O_loc_t loc;                           /* Object location for object to delete */
    unsigned  oh_flags = H5AC__NO_FLAGS_SET; /* Flags for unprotecting object header */
    bool      corked;
    herr_t    ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(addr, FAIL)

    /* Check args */
    assert(f);
    assert(H5_addr_defined(addr));

    /* Set up the object location */
    loc.file         = f;
    loc.addr         = addr;
    loc.holding_file = false;

    /* Get the object header information */
    if (NULL == (oh = H5O_protect(&loc, H5AC__NO_FLAGS_SET, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Delete object */
    if (H5O__delete_oh(f, oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, FAIL, "can't delete object from file");

    /* Uncork cache entries with tag: addr */
    if (H5AC_cork(f, addr, H5AC__GET_CORKED, &corked) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to retrieve an object's cork status");
    if (corked)
        if (H5AC_cork(f, addr, H5AC__UNCORK, NULL) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTUNCORK, FAIL, "unable to uncork an object");

    /* Mark object header as deleted */
    oh_flags = H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;

done:
    if (oh && H5O_unprotect(&loc, oh, oh_flags) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_PROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5O__delete_oh
 *
 * Purpose:    Internal function to:
 *              Delete an object header from a file.  This frees the file
 *              space used for the object header (and it's continuation blocks)
 *              and also walks through each header message and asks it to
 *              remove all the pieces of the file referenced by the header.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__delete_oh(H5F_t *f, H5O_t *oh)
{
    H5O_mesg_t *curr_msg; /* Pointer to current message being operated on */
    unsigned    u;
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(f);
    assert(oh);

    /* Walk through the list of object header messages, asking each one to
     * delete any file space used
     */
    for (u = 0, curr_msg = &oh->mesg[0]; u < oh->nmesgs; u++, curr_msg++) {
        /* Free any space referred to in the file from this message */
        if (H5O__delete_mesg(f, oh, curr_msg) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTDELETE, FAIL,
                        "unable to delete file space for object header message");
    } /* end for */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__delete_oh() */

/*-------------------------------------------------------------------------
 * Function:    H5O_obj_type
 *
 * Purpose:    Retrieves the type of object pointed to by `loc'.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_obj_type(const H5O_loc_t *loc, H5O_type_t *obj_type)
{
    H5O_t *oh        = NULL;    /* Object header for location */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(loc->addr, FAIL)

    /* Load the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Retrieve the type of the object */
    if (H5O__obj_type_real(oh, obj_type) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to determine object type");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_obj_type() */

/*-------------------------------------------------------------------------
 * Function:    H5O__obj_type_real
 *
 * Purpose:    Returns the type of object pointed to by `oh'.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__obj_type_real(const H5O_t *oh, H5O_type_t *obj_type)
{
    const H5O_obj_class_t *obj_class; /* Class of object for header */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(oh);
    assert(obj_type);

    /* Look up class for object header */
    if (NULL == (obj_class = H5O__obj_class_real(oh))) {
        /* Clear error stack from "failed" class lookup */
        H5E_clear_stack(NULL);

        /* Set type to "unknown" */
        *obj_type = H5O_TYPE_UNKNOWN;
    }
    else
        /* Set object type */
        *obj_type = obj_class->type;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__obj_type_real() */

/*-------------------------------------------------------------------------
 * Function:    H5O__obj_class
 *
 * Purpose:     Returns the class of object pointed to by 'loc'.
 *
 * Return:      Success:    An object class
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
const H5O_obj_class_t *
H5O__obj_class(const H5O_loc_t *loc)
{
    H5O_t                 *oh        = NULL; /* Object header for location */
    const H5O_obj_class_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(loc->addr)

    /* Load the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, NULL, "unable to load object header");

    /* Test whether entry qualifies as a particular type of object */
    if (NULL == (ret_value = H5O__obj_class_real(oh)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to determine object type");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, NULL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O__obj_class() */

/*-------------------------------------------------------------------------
 * Function:    H5O__obj_class_real
 *
 * Purpose:    Returns the class of object pointed to by `oh'.
 *
 * Return:    Success:    An object class
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static const H5O_obj_class_t *
H5O__obj_class_real(const H5O_t *oh)
{
    size_t                 i;                /* Local index variable */
    const H5O_obj_class_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(oh);

    /* Test whether entry qualifies as a particular type of object */
    /* (Note: loop is in reverse order, to test specific objects first) */
    for (i = NELMTS(H5O_obj_class_g); i > 0; --i) {
        htri_t isa; /* Is entry a particular type? */

        if ((isa = (H5O_obj_class_g[i - 1]->isa)(oh)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "unable to determine object type");
        else if (isa)
            HGOTO_DONE(H5O_obj_class_g[i - 1]);
    }

    if (0 == i)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, NULL, "unable to determine object type");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__obj_class_real() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_loc
 *
 * Purpose:    Gets the object location for an object given its ID.
 *
 * Return:    Success:    Pointer to H5O_loc_t
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
H5O_loc_t *
H5O_get_loc(hid_t object_id)
{
    H5O_loc_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    switch (H5I_get_type(object_id)) {
        case H5I_GROUP:
            if (NULL == (ret_value = H5O_OBJ_GROUP->get_oloc(object_id)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get object location from group ID");
            break;

        case H5I_DATASET:
            if (NULL == (ret_value = H5O_OBJ_DATASET->get_oloc(object_id)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get object location from dataset ID");
            break;

        case H5I_DATATYPE:
            if (NULL == (ret_value = H5O_OBJ_DATATYPE->get_oloc(object_id)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, NULL, "unable to get object location from datatype ID");
            break;

        case H5I_MAP:
            HGOTO_ERROR(H5E_OHDR, H5E_BADTYPE, NULL, "maps not supported in native VOL connector");

        case H5I_UNINIT:
        case H5I_BADID:
        case H5I_FILE:
        case H5I_DATASPACE:
        case H5I_ATTR:
        case H5I_VFL:
        case H5I_VOL:
        case H5I_GENPROP_CLS:
        case H5I_GENPROP_LST:
        case H5I_ERROR_CLASS:
        case H5I_ERROR_MSG:
        case H5I_ERROR_STACK:
        case H5I_SPACE_SEL_ITER:
        case H5I_EVENTSET:
        case H5I_NTYPES:
        default:
            HGOTO_ERROR(H5E_OHDR, H5E_BADTYPE, NULL, "invalid object type");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_get_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_reset
 *
 * Purpose:    Reset a object location to an empty state
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_reset(H5O_loc_t *loc)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(loc);

    /* Clear the object location to an empty state */
    memset(loc, 0, sizeof(H5O_loc_t));
    loc->addr = HADDR_UNDEF;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_loc_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_copy
 *
 * Purpose:     Copy object location information, according to the depth.
 *
 * Return:    Success:    Non-negative
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_copy(H5O_loc_t *dst, H5O_loc_t *src, H5_copy_depth_t depth)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(src);
    assert(dst);
    assert(depth == H5_COPY_SHALLOW || depth == H5_COPY_DEEP);

    /* Invoke correct routine */
    if (depth == H5_COPY_SHALLOW)
        H5O_loc_copy_shallow(dst, src);
    else
        H5O_loc_copy_deep(dst, src);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_loc_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_copy_shallow
 *
 * Purpose:     Shallow copy object location information.  Copies all the field
 *              values from the source to the destination, but not copying
 *              objects pointed to.  (i.e. destination "takes ownership" of
 *              objects pointed to)
 *
 * Return:    Success:    Non-negative
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_copy_shallow(H5O_loc_t *dst, H5O_loc_t *src)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(src);
    assert(dst);

    /* Copy the top level information */
    H5MM_memcpy(dst, src, sizeof(H5O_loc_t));

    /* Reset the source location, as the destination 'owns' it now */
    H5O_loc_reset(src);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_loc_copy_shallow() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_copy_deep
 *
 * Purpose:     Deep copy object location information.  Copies all the fields
 *              from the source to the destination, deep copying objects
 *              pointed to.
 *
 * Return:    Success:    Non-negative
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_copy_deep(H5O_loc_t *dst, const H5O_loc_t *src)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(src);
    assert(dst);

    /* Copy the top level information */
    H5MM_memcpy(dst, src, sizeof(H5O_loc_t));

    /* If the original entry was holding open the file, this one should
     * hold it open, too.
     */
    if (src->holding_file)
        H5F_INCR_NOPEN_OBJS(dst->file);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_loc_copy_deep() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_hold_file
 *
 * Purpose:    Have this object header hold a file open until it is
 *              released.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_hold_file(H5O_loc_t *loc)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments */
    assert(loc);
    assert(loc->file);

    /* If this location is not already holding its file open, do so. */
    if (!loc->holding_file) {
        H5F_INCR_NOPEN_OBJS(loc->file);
        loc->holding_file = true;
    }

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O_loc_hold_file() */

/*-------------------------------------------------------------------------
 * Function:    H5O_loc_free
 *
 * Purpose:    Release resources used by this object header location.
 *              Not to be confused with H5O_close; this is used on
 *              locations that don't correspond to open objects.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_loc_free(H5O_loc_t *loc)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

    /* Check arguments */
    assert(loc);

    /* If this location is holding its file open try to close the file. */
    if (loc->holding_file) {
        H5F_DECR_NOPEN_OBJS(loc->file);
        loc->holding_file = false;
        if (H5F_NOPEN_OBJS(loc->file) <= 0) {
            if (H5F_try_close(loc->file, NULL) < 0)
                HGOTO_ERROR(H5E_FILE, H5E_CANTCLOSEFILE, FAIL, "can't close file");
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_loc_free() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_hdr_info
 *
 * Purpose:    Retrieve the object header information for an object
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_hdr_info(const H5O_loc_t *loc, H5O_hdr_info_t *hdr)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(loc);
    assert(hdr);

    /* Reset the object header info structure */
    memset(hdr, 0, sizeof(*hdr));

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTLOAD, FAIL, "unable to load object header");

    /* Get the information for the object header */
    if (H5O__get_hdr_info_real(oh, hdr) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve object header info");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_PROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_get_hdr_info() */

/*-------------------------------------------------------------------------
 * Function:    H5O__get_hdr_info_real
 *
 * Purpose:    Internal routine to retrieve the object header information for an object
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__get_hdr_info_real(const H5O_t *oh, H5O_hdr_info_t *hdr)
{
    const H5O_mesg_t  *curr_msg;   /* Pointer to current message being operated on */
    const H5O_chunk_t *curr_chunk; /* Pointer to current message being operated on */
    unsigned           u;          /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(oh);
    assert(hdr);

    /* Set the version for the object header */
    hdr->version = oh->version;

    /* Set the number of messages & chunks */
    H5_CHECKED_ASSIGN(hdr->nmesgs, unsigned, oh->nmesgs, size_t);
    H5_CHECKED_ASSIGN(hdr->nchunks, unsigned, oh->nchunks, size_t);

    /* Set the status flags */
    hdr->flags = oh->flags;

    /* Iterate over all the messages, accumulating message size & type information */
    hdr->space.meta   = (hsize_t)H5O_SIZEOF_HDR(oh) + (hsize_t)(H5O_SIZEOF_CHKHDR_OH(oh) * (oh->nchunks - 1));
    hdr->space.mesg   = 0;
    hdr->space.free   = 0;
    hdr->mesg.present = 0;
    hdr->mesg.shared  = 0;
    for (u = 0, curr_msg = &oh->mesg[0]; u < oh->nmesgs; u++, curr_msg++) {
        uint64_t type_flag; /* Flag for message type */

        /* Accumulate space usage information, based on the type of message */
        if (H5O_NULL_ID == curr_msg->type->id)
            hdr->space.free += (hsize_t)((size_t)H5O_SIZEOF_MSGHDR_OH(oh) + curr_msg->raw_size);
        else if (H5O_CONT_ID == curr_msg->type->id)
            hdr->space.meta += (hsize_t)((size_t)H5O_SIZEOF_MSGHDR_OH(oh) + curr_msg->raw_size);
        else {
            hdr->space.meta += (hsize_t)H5O_SIZEOF_MSGHDR_OH(oh);
            hdr->space.mesg += curr_msg->raw_size;
        } /* end else */

        /* Set flag to indicate presence of message type */
        type_flag = ((uint64_t)1) << curr_msg->type->id;
        hdr->mesg.present |= type_flag;

        /* Set flag if the message is shared in some way */
        if (curr_msg->flags & H5O_MSG_FLAG_SHARED)
            hdr->mesg.shared |= type_flag;
    } /* end for */

    /* Iterate over all the chunks, adding any gaps to the free space */
    hdr->space.total = 0;
    for (u = 0, curr_chunk = &oh->chunk[0]; u < oh->nchunks; u++, curr_chunk++) {
        /* Accumulate the size of the header on disk */
        hdr->space.total += curr_chunk->size;

        /* If the chunk has a gap, add it to the free space */
        hdr->space.free += curr_chunk->gap;
    } /* end for */

    /* Sanity check that all the bytes are accounted for */
    assert(hdr->space.total == (hdr->space.free + hdr->space.meta + hdr->space.mesg));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__get_hdr_info_real() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_info
 *
 * Purpose:     Retrieve the data model information for an object
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_info(const H5O_loc_t *loc, H5O_info2_t *oinfo, unsigned fields)
{
    const H5O_obj_class_t *obj_class;           /* Class of object for header */
    H5O_t                 *oh        = NULL;    /* Object header */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(loc->addr, FAIL)

    /* Check args */
    assert(loc);
    assert(oinfo);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Get class for object */
    if (NULL == (obj_class = H5O__obj_class_real(oh)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to determine object class");

    /* Reset the object info structure */
    if (H5O__reset_info2(oinfo) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't reset object data struct");

    /* Get basic information, if requested */
    if (fields & H5O_INFO_BASIC) {
        /* Retrieve the file's fileno */
        H5F_GET_FILENO(loc->file, oinfo->fileno);

        /* Set the object's address into the token */
        if (H5VL_native_addr_to_token(loc->file, H5I_FILE, loc->addr, &oinfo->token) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTSERIALIZE, FAIL, "can't serialize address into object token");

        /* Retrieve the type of the object */
        oinfo->type = obj_class->type;

        /* Set the object's reference count */
        oinfo->rc = oh->nlink;
    }

    /* Get time information, if requested */
    if (fields & H5O_INFO_TIME) {
        if (oh->version > H5O_VERSION_1) {
            oinfo->atime = oh->atime;
            oinfo->mtime = oh->mtime;
            oinfo->ctime = oh->ctime;
            oinfo->btime = oh->btime;
        } /* end if */
        else {
            htri_t exists; /* Flag if header message of interest exists */

            /* No information for access & modification fields */
            /* (we stopped updating the "modification time" header message for
             *      raw data changes, so the "modification time" header message
             *      is closest to the 'change time', in POSIX terms - QAK)
             */
            oinfo->atime = 0;
            oinfo->mtime = 0;
            oinfo->btime = 0;

            /* Might be information for modification time */
            if ((exists = H5O_msg_exists_oh(oh, H5O_MTIME_ID)) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "unable to check for MTIME message");
            if (exists > 0) {
                /* Get "old style" modification time info */
                if (NULL == H5O_msg_read_oh(loc->file, oh, H5O_MTIME_ID, &oinfo->ctime))
                    HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't read MTIME message");
            } /* end if */
            else {
                /* Check for "new style" modification time info */
                if ((exists = H5O_msg_exists_oh(oh, H5O_MTIME_NEW_ID)) < 0)
                    HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "unable to check for MTIME_NEW message");
                if (exists > 0) {
                    /* Get "new style" modification time info */
                    if (NULL == H5O_msg_read_oh(loc->file, oh, H5O_MTIME_NEW_ID, &oinfo->ctime))
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't read MTIME_NEW message");
                } /* end if */
                else
                    oinfo->ctime = 0;
            } /* end else */
        }     /* end else */
    }         /* end if */

    /* Retrieve # of attributes */
    if (fields & H5O_INFO_NUM_ATTRS)
        if (H5O__attr_count_real(loc->file, oh, &oinfo->num_attrs) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve attribute count");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_get_info() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_native_info
 *
 * Purpose:     Retrieve the native file-format information for an object
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_native_info(const H5O_loc_t *loc, H5O_native_info_t *oinfo, unsigned fields)
{
    const H5O_obj_class_t *obj_class;           /* Class of object for header */
    H5O_t                 *oh        = NULL;    /* Object header */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_TAG(loc->addr, FAIL)

    /* Check args */
    assert(loc);
    assert(oinfo);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Get class for object */
    if (NULL == (obj_class = H5O__obj_class_real(oh)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to determine object class");

    /* Reset the object info structure */
    memset(oinfo, 0, sizeof(*oinfo));

    /* Get the information for the object header, if requested */
    if (fields & H5O_NATIVE_INFO_HDR)
        if (H5O__get_hdr_info_real(oh, &oinfo->hdr) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve object header info");

    /* Get B-tree & heap metadata storage size, if requested */
    if (fields & H5O_NATIVE_INFO_META_SIZE) {
        /* Check for 'bh_info' callback for this type of object */
        if (obj_class->bh_info)
            /* Call the object's class 'bh_info' routine */
            if ((obj_class->bh_info)(loc, oh, &oinfo->meta_size.obj) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve object's btree & heap info");

        /* Get B-tree & heap info for any attributes */
        if (H5O__attr_bh_info(loc->file, oh, &oinfo->meta_size.attr) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "can't retrieve attribute btree & heap info");
    } /* end if */

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5O_get_native_info() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_create_plist
 *
 * Purpose:    Retrieve the object creation properties for an object
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_create_plist(const H5O_loc_t *loc, H5P_genplist_t *oc_plist)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(loc);
    assert(oc_plist);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Set property values, if they were used for the object */
    if (oh->version > H5O_VERSION_1) {
        uint8_t ohdr_flags; /* "User-visible" object header status flags */

        /* Set attribute storage values */
        if (H5P_set(oc_plist, H5O_CRT_ATTR_MAX_COMPACT_NAME, &oh->max_compact) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL,
                        "can't set max. # of compact attributes in property list");
        if (H5P_set(oc_plist, H5O_CRT_ATTR_MIN_DENSE_NAME, &oh->min_dense) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTSET, FAIL, "can't set min. # of dense attributes in property list");

        /* Mask off non-"user visible" flags */
        H5_CHECKED_ASSIGN(ohdr_flags, uint8_t,
                          oh->flags & (H5O_HDR_ATTR_CRT_ORDER_TRACKED | H5O_HDR_ATTR_CRT_ORDER_INDEXED |
                                       H5O_HDR_STORE_TIMES),
                          int);

        /* Set object header flags */
        if (H5P_set(oc_plist, H5O_CRT_OHDR_FLAGS_NAME, &ohdr_flags) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object header flags");
    } /* end if */

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_get_create_plist() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_nlinks
 *
 * Purpose:    Retrieve the number of link messages read in from the file
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_nlinks(const H5O_loc_t *loc, hsize_t *nlinks)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(loc);
    assert(nlinks);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Retrieve the # of link messages seen when the object header was loaded */
    *nlinks = oh->link_msgs_seen;

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_get_nlinks() */

/*-------------------------------------------------------------------------
 * Function:    H5O_obj_create
 *
 * Purpose:    Creates an object, in an abstract manner.
 *
 * Return:    Success:    Pointer to object opened
 *        Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
void *
H5O_obj_create(H5F_t *f, H5O_type_t obj_type, void *crt_info, H5G_loc_t *obj_loc)
{
    size_t u;                /* Local index variable */
    void  *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Sanity checks */
    assert(f);
    assert(obj_type >= H5O_TYPE_GROUP && obj_type <= H5O_TYPE_NAMED_DATATYPE);
    assert(crt_info);
    assert(obj_loc);

    /* Iterate through the object classes */
    for (u = 0; u < NELMTS(H5O_obj_class_g); u++) {
        /* Check for correct type of object to create */
        if (H5O_obj_class_g[u]->type == obj_type) {
            /* Call the object class's 'create' routine */
            assert(H5O_obj_class_g[u]->create);
            if (NULL == (ret_value = H5O_obj_class_g[u]->create(f, crt_info, obj_loc)))
                HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, NULL, "unable to open object");

            /* Break out of loop */
            break;
        } /* end if */
    }     /* end for */
    assert(ret_value);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_obj_create() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_oh_addr
 *
 * Purpose:    Retrieve the address of the object header
 *
 * Note:    This routine participates in the "Inlining C struct access"
 *        pattern, don't call it directly, use the appropriate macro
 *        defined in H5Oprivate.h.
 *
 * Return:    Success:    Valid haddr_t
 *        Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
haddr_t
H5O_get_oh_addr(const H5O_t *oh)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(oh);
    assert(oh->chunk);

    FUNC_LEAVE_NOAPI(oh->chunk[0].addr)
} /* end H5O_get_oh_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_oh_flags
 *
 *-------------------------------------------------------------------------
 */
uint8_t
H5O_get_oh_flags(const H5O_t *oh)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR
    assert(oh);
    FUNC_LEAVE_NOAPI(oh->flags) /* flags can be 0 */
} /* H5O_get_oh_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_oh_mtime
 *
 * Purpose:     Retrieve an object's modification time. Assumes that the
 *              caller has verified that accessing this variable is appropriate
 *              to the header in question.
 *
 *-------------------------------------------------------------------------
 */
time_t
H5O_get_oh_mtime(const H5O_t *oh)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR
    assert(oh);
    assert(oh->mtime);
    FUNC_LEAVE_NOAPI(oh->mtime)
} /* H5O_get_oh_mtime() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_oh_version
 *
 *-------------------------------------------------------------------------
 */
uint8_t
H5O_get_oh_version(const H5O_t *oh)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR
    assert(oh);
    assert(oh->version);
    FUNC_LEAVE_NOAPI(oh->version)
} /* H5O_get_oh_version() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_rc_and_type
 *
 * Purpose:    Retrieve an object's reference count and type
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_get_rc_and_type(const H5O_loc_t *loc, unsigned *rc, H5O_type_t *otype)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(loc);

    /* Get the object header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to load object header");

    /* Set the object's reference count */
    if (rc)
        *rc = oh->nlink;

    /* Retrieve the type of the object */
    if (otype)
        if (H5O__obj_type_real(oh, otype) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTINIT, FAIL, "unable to determine object type");

done:
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_get_rc_and_type() */

/*-------------------------------------------------------------------------
 * Function:    H5O__free_visit_visited
 *
 * Purpose:     Free the key for an object visited during a group traversal
 *
 * Return:      Non-negative on success, negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__free_visit_visited(void *item, void H5_ATTR_UNUSED *key, void H5_ATTR_UNUSED *operator_data /*in,out*/)
{
    FUNC_ENTER_PACKAGE_NOERR

    item = H5FL_FREE(H5_obj_t, item);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__free_visit_visited() */

/*-------------------------------------------------------------------------
 * Function:    H5O__visit_cb
 *
 * Purpose:     Callback function for recursively visiting objects from a group
 *
 * Return:    Success:        Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__visit_cb(hid_t H5_ATTR_UNUSED group, const char *name, const H5L_info2_t *linfo, void *_udata)
{
    H5O_iter_visit_ud_t *udata = (H5O_iter_visit_ud_t *)_udata; /* User data for callback */
    H5G_loc_t            obj_loc;                               /* Location of object */
    H5G_name_t           obj_path;                              /* Object's group hier. path */
    H5O_loc_t            obj_oloc;                              /* Object's object location */
    bool                 obj_found = false;                     /* Object at 'name' found */
    herr_t               ret_value = H5_ITER_CONT;              /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(name);
    assert(linfo);
    assert(udata);

    /* Check if this is a hard link */
    if (linfo->type == H5L_TYPE_HARD) {
        H5_obj_t obj_pos; /* Object "position" for this object */

        /* Set up opened group location to fill in */
        obj_loc.oloc = &obj_oloc;
        obj_loc.path = &obj_path;
        H5G_loc_reset(&obj_loc);

        /* Find the object using the LAPL passed in */
        /* (Correctly handles mounted files) */
        if (H5G_loc_find(udata->start_loc, name, &obj_loc /*out*/) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, H5_ITER_ERROR, "object not found");
        obj_found = true;

        /* Construct unique "position" for this object */
        H5F_GET_FILENO(obj_oloc.file, obj_pos.fileno);
        obj_pos.addr = obj_oloc.addr;

        /* Check if we've seen the object the link references before */
        if (NULL == H5SL_search(udata->visited, &obj_pos)) {
            H5O_info2_t oinfo; /* Object info */

            /* Get the object's info */
            if (H5O_get_info(&obj_oloc, &oinfo, udata->fields) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, H5_ITER_ERROR, "unable to get object info");

            /* Make the application callback */
            ret_value = (udata->op)(udata->obj_id, name, &oinfo, udata->op_data);

            /* Check for continuing to visit objects */
            if (ret_value == H5_ITER_CONT) {
                /* If its ref count is > 1, we add it to the list of visited objects */
                /* (because it could come up again during traversal) */
                if (oinfo.rc > 1) {
                    H5_obj_t *new_node; /* New object node for visited list */

                    /* Allocate new object "position" node */
                    if ((new_node = H5FL_MALLOC(H5_obj_t)) == NULL)
                        HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, H5_ITER_ERROR, "can't allocate object node");

                    /* Set node information */
                    *new_node = obj_pos;

                    /* Add to list of visited objects */
                    if (H5SL_insert(udata->visited, new_node, new_node) < 0)
                        HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, H5_ITER_ERROR,
                                    "can't insert object node into visited list");
                } /* end if */
            }     /* end if */
        }         /* end if */
    }             /* end if */

done:
    /* Release resources */
    if (obj_found && H5G_loc_free(&obj_loc) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, H5_ITER_ERROR, "can't free location");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__visit_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5O__visit
 *
 * Purpose:     Recursively visit an object and all the objects reachable
 *              from it.  If the starting object is a group, all the objects
 *              linked to from that group will be visited.  Links within
 *              each group are visited according to the order within the
 *              specified index (unless the specified index does not exist for
 *              a particular group, then the "name" index is used).
 *
 *              NOTE: Soft links and user-defined links are ignored during
 *              this operation.
 *
 *              NOTE: Each _object_ reachable from the initial group will only
 *              be visited once.  If multiple hard links point to the same
 *              object, the first link to the object's path (according to the
 *              iteration index and iteration order given) will be used to in
 *              the callback about the object.
 *
 * Note:        Add a parameter "fields" to indicate selection of object info.
 *
 * Return:      Success:    The return value of the first operator that
 *                          returns non-zero, or zero if all members were
 *                          processed with no operator returning non-zero.
 *
 *              Failure:    Negative if something goes wrong within the
 *                          library, or the negative value returned by one
 *                          of the operators.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__visit(H5G_loc_t *loc, const char *obj_name, H5_index_t idx_type, H5_iter_order_t order,
           H5O_iterate2_t op, void *op_data, unsigned fields)
{
    H5O_iter_visit_ud_t udata;                       /* User data for callback */
    H5G_loc_t           obj_loc;                     /* Location used to open object */
    H5G_name_t          obj_path;                    /* Opened object group hier. path */
    H5O_loc_t           obj_oloc;                    /* Opened object object location */
    bool                loc_found = false;           /* Entry at 'name' found */
    H5O_info2_t         oinfo;                       /* Object info struct */
    void               *obj = NULL;                  /* Object */
    H5I_type_t          opened_type;                 /* ID type of object */
    hid_t               obj_id    = H5I_INVALID_HID; /* ID of object */
    herr_t              ret_value = FAIL;            /* Return value */

    FUNC_ENTER_PACKAGE

    /* Portably initialize user data struct to zeros */
    memset(&udata, 0, sizeof(udata));

    /* Check args */
    assert(loc);

    /* Set up opened group location to fill in */
    obj_loc.oloc = &obj_oloc;
    obj_loc.path = &obj_path;
    H5G_loc_reset(&obj_loc);

    /* Find the object's location */
    if (H5G_loc_find(loc, obj_name, &obj_loc /*out*/) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_NOTFOUND, FAIL, "object not found");
    loc_found = true;

    /* Get the object's info */
    if (H5O_get_info(&obj_oloc, &oinfo, fields) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, FAIL, "unable to get object info");

    /* Open the object */
    /* (Takes ownership of the obj_loc information) */
    if (NULL == (obj = H5O_open_by_loc(&obj_loc, &opened_type)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTOPENOBJ, FAIL, "unable to open object");

    /* Get an ID for the visited object */
    if ((obj_id = H5VL_wrap_register(opened_type, obj, true)) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTREGISTER, FAIL, "unable to register visited object");

    /* Make callback for starting object */
    if ((ret_value = op(obj_id, ".", &oinfo, op_data)) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "can't visit objects");

    /* Check return value of first callback */
    if (ret_value != H5_ITER_CONT)
        HGOTO_DONE(ret_value);

    /* Check for object being a group */
    if (oinfo.type == H5O_TYPE_GROUP) {
        H5G_loc_t start_loc; /* Location of starting group */
        H5G_loc_t vis_loc;   /* Location of visited group */

        /* Get the location of the starting group */
        if (H5G_loc(obj_id, &start_loc) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a location");

        /* Set up user data for visiting links */
        udata.obj_id    = obj_id;
        udata.start_loc = &start_loc;
        udata.op        = op;
        udata.op_data   = op_data;
        udata.fields    = fields;

        /* Create skip list to store visited object information */
        if ((udata.visited = H5SL_create(H5SL_TYPE_OBJ, NULL)) == NULL)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTCREATE, FAIL, "can't create skip list for visited objects");

        /* If its ref count is > 1, we add it to the list of visited objects */
        /* (because it could come up again during traversal) */
        if (oinfo.rc > 1) {
            H5_obj_t *obj_pos; /* New object node for visited list */

            /* Allocate new object "position" node */
            if ((obj_pos = H5FL_MALLOC(H5_obj_t)) == NULL)
                HGOTO_ERROR(H5E_OHDR, H5E_NOSPACE, FAIL, "can't allocate object node");

            /* Construct unique "position" for this object */
            obj_pos->fileno = oinfo.fileno;

            /* De-serialize object token into an object address */
            if (H5VL_native_token_to_addr(loc->oloc->file, H5I_FILE, oinfo.token, &(obj_pos->addr)) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTUNSERIALIZE, FAIL,
                            "can't deserialize object token into address");

            /* Add to list of visited objects */
            if (H5SL_insert(udata.visited, obj_pos, obj_pos) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTINSERT, FAIL, "can't insert object node into visited list");
        }

        /* Get the location of the visited group */
        if (H5G_loc(obj_id, &vis_loc) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a location");

        /* Call internal group visitation routine */
        if ((ret_value = H5G_visit(&vis_loc, ".", idx_type, order, H5O__visit_cb, &udata)) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_BADITER, FAIL, "object visitation failed");
    } /* end if */

done:
    /* XXX (VOL MERGE): Probably also want to consider closing obj here on failures */
    if (obj_id != H5I_INVALID_HID) {
        if (H5I_dec_app_ref(obj_id) < 0)
            HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "unable to close object");
    }
    else if (loc_found && H5G_loc_free(&obj_loc) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTRELEASE, FAIL, "can't free location");

    if (udata.visited)
        H5SL_destroy(udata.visited, H5O__free_visit_visited, NULL);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__visit() */

/*-------------------------------------------------------------------------
 * Function:    H5O__inc_rc
 *
 * Purpose:    Increments the reference count on an object header
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__inc_rc(H5O_t *oh)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(oh);

    /* Pin the object header when the reference count goes above 0 */
    if (oh->rc == 0)
        if (H5AC_pin_protected_entry(oh) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTPIN, FAIL, "unable to pin object header");

    /* Increment reference count */
    oh->rc++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__inc_rc() */

/*-------------------------------------------------------------------------
 * Function:    H5O__dec_rc
 *
 * Purpose:    Decrements the reference count on an object header
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__dec_rc(H5O_t *oh)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    if (!oh)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid object header");

    /* Decrement reference count */
    oh->rc--;

    /* Unpin the object header when the reference count goes back to 0 */
    if (oh->rc == 0)
        if (H5AC_unpin_entry(oh) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTUNPIN, FAIL, "unable to unpin object header");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__dec_rc() */

/*-------------------------------------------------------------------------
 * Function:   H5O_dec_rc_by_loc
 *
 * Purpose:    Decrement the refcount of an object header, using its
 *              object location information.
 *
 * Return:     Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O_dec_rc_by_loc(const H5O_loc_t *loc)
{
    H5O_t *oh        = NULL;    /* Object header */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(loc);

    /* Get header */
    if (NULL == (oh = H5O_protect(loc, H5AC__READ_ONLY_FLAG, false)))
        HGOTO_ERROR(H5E_OHDR, H5E_CANTPROTECT, FAIL, "unable to protect object header");

    /* Decrement the reference count on the object header */
    /* (which will unpin it, if appropriate) */
    if (H5O__dec_rc(oh) < 0)
        HGOTO_ERROR(H5E_OHDR, H5E_CANTDEC, FAIL, "unable to decrement reference count on object header");

done:
    /* Release the object header from the cache */
    if (oh && H5O_unprotect(loc, oh, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_OHDR, H5E_CANTUNPROTECT, FAIL, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_dec_rc_by_loc() */

/*-------------------------------------------------------------------------
 * Function:    H5O_get_proxy
 *
 * Purpose:    Retrieve the proxy for the object header.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
H5AC_proxy_entry_t *
H5O_get_proxy(const H5O_t *oh)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check args */
    assert(oh);

    FUNC_LEAVE_NOAPI(oh->proxy)
} /* end H5O_get_proxy() */

/*-------------------------------------------------------------------------
 * Function:    H5O__free
 *
 * Purpose:    Destroys an object header.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5O__free(H5O_t *oh, bool H5_ATTR_NDEBUG_UNUSED force)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(oh);
    assert(0 == oh->rc);

    /* Destroy chunks */
    if (oh->chunk) {
        for (u = 0; u < oh->nchunks; u++)
            oh->chunk[u].image = H5FL_BLK_FREE(chunk_image, oh->chunk[u].image);

        oh->chunk = (H5O_chunk_t *)H5FL_SEQ_FREE(H5O_chunk_t, oh->chunk);
    } /* end if */

    /* Destroy messages */
    if (oh->mesg) {
        for (u = 0; u < oh->nmesgs; u++) {
#ifndef NDEBUG
            /* Verify that message is clean, unless it could have been marked
             * dirty by decoding, or if this is a forced free (in case of
             * failure during creation of the object some messages may be dirty)
             */
            if (oh->ndecode_dirtied && oh->mesg[u].dirty)
                oh->ndecode_dirtied--;
            else if (!force)
                assert(oh->mesg[u].dirty == 0);
#endif /* NDEBUG */

            H5O__msg_free_mesg(&oh->mesg[u]);
        } /* end for */

        /* Make sure we accounted for all the messages dirtied by decoding */
        assert(!oh->ndecode_dirtied);

        oh->mesg = (H5O_mesg_t *)H5FL_SEQ_FREE(H5O_mesg_t, oh->mesg);
    } /* end if */

    /* Destroy the proxy */
    if (oh->proxy)
        if (H5AC_proxy_entry_dest(oh->proxy) < 0)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTFREE, FAIL, "unable to destroy virtual entry used for proxy");

    /* destroy object header */
    oh = H5FL_FREE(H5O_t, oh);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O__free() */

/*-------------------------------------------------------------------------
 * Function:    H5O__reset_info2
 *
 * Purpose:     Resets/initializes an H5O_info2_t struct.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5O__reset_info2(H5O_info2_t *oinfo)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Reset the passed-in info struct */
    memset(oinfo, 0, sizeof(H5O_info2_t));
    oinfo->type  = H5O_TYPE_UNKNOWN;
    oinfo->token = H5O_TOKEN_UNDEF;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5O__reset_info2() */
