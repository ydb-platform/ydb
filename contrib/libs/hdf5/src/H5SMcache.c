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
 * Created:		H5SMcache.c
 *
 * Purpose:		Implement shared message metadata cache methods.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5SMmodule.h" /* This source code file is part of the H5SM module */

/***********/
/* Headers */
/***********/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access                          */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5SMpkg.h"     /* Shared object header messages        */
#include "H5WBprivate.h" /* Wrapped Buffers                      */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/* Metadata cache (H5AC) callbacks */
static herr_t H5SM__cache_table_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5SM__cache_table_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5SM__cache_table_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5SM__cache_table_image_len(const void *thing, size_t *image_len);
static herr_t H5SM__cache_table_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5SM__cache_table_free_icr(void *thing);

static herr_t H5SM__cache_list_get_initial_load_size(void *udata, size_t *image_len);
static htri_t H5SM__cache_list_verify_chksum(const void *image_ptr, size_t len, void *udata_ptr);
static void  *H5SM__cache_list_deserialize(const void *image, size_t len, void *udata, bool *dirty);
static herr_t H5SM__cache_list_image_len(const void *thing, size_t *image_len);
static herr_t H5SM__cache_list_serialize(const H5F_t *f, void *image, size_t len, void *thing);
static herr_t H5SM__cache_list_free_icr(void *thing);

/*********************/
/* Package Variables */
/*********************/

/* H5SM inherits cache-like properties from H5AC */
const H5AC_class_t H5AC_SOHM_TABLE[1] = {{
    H5AC_SOHM_TABLE_ID,                      /* Metadata client ID */
    "shared message table",                  /* Metadata client name (for debugging) */
    H5FD_MEM_SOHM_TABLE,                     /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,                /* Client class behavior flags */
    H5SM__cache_table_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                    /* 'get_final_load_size' callback */
    H5SM__cache_table_verify_chksum,         /* 'verify_chksum' callback */
    H5SM__cache_table_deserialize,           /* 'deserialize' callback */
    H5SM__cache_table_image_len,             /* 'image_len' callback */
    NULL,                                    /* 'pre_serialize' callback */
    H5SM__cache_table_serialize,             /* 'serialize' callback */
    NULL,                                    /* 'notify' callback */
    H5SM__cache_table_free_icr,              /* 'free_icr' callback */
    NULL,                                    /* 'fsf_size' callback */
}};

const H5AC_class_t H5AC_SOHM_LIST[1] = {{
    H5AC_SOHM_LIST_ID,                      /* Metadata client ID */
    "shared message list",                  /* Metadata client name (for debugging) */
    H5FD_MEM_SOHM_TABLE,                    /* File space memory type for client */
    H5AC__CLASS_NO_FLAGS_SET,               /* Client class behavior flags */
    H5SM__cache_list_get_initial_load_size, /* 'get_initial_load_size' callback */
    NULL,                                   /* 'get_final_load_size' callback */
    H5SM__cache_list_verify_chksum,         /* 'verify_chksum' callback */
    H5SM__cache_list_deserialize,           /* 'deserialize' callback */
    H5SM__cache_list_image_len,             /* 'image_len' callback */
    NULL,                                   /* 'pre_serialize' callback */
    H5SM__cache_list_serialize,             /* 'serialize' callback */
    NULL,                                   /* 'notify' callback */
    H5SM__cache_list_free_icr,              /* 'free_icr' callback */
    NULL,                                   /* 'fsf_size' callback */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_get_initial_load_size()
 *
 * Purpose:	Return the size of the master table of Shared Object Header
 *		Message indexes on disk.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_table_get_initial_load_size(void *_udata, size_t *image_len)
{
    const H5SM_table_cache_ud_t *udata = (const H5SM_table_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->f);
    assert(image_len);

    /* Set the image length size */
    *image_len = H5SM_TABLE_SIZE(udata->f);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__cache_table_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5SM__cache_table_verify_chksum(const void *_image, size_t len, void H5_ATTR_UNUSED *_udata)
{
    const uint8_t *image = (const uint8_t *)_image; /* Pointer into raw data buffer */
    uint32_t       stored_chksum;                   /* Stored metadata checksum value */
    uint32_t       computed_chksum;                 /* Computed metadata checksum value */
    htri_t         ret_value = true;                /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, len, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__cache_table_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_deserialize
 *
 * Purpose:	Given a buffer containing the on disk representation of the
 *		master table of Shared Object Header Message indexes, deserialize
 *		the table, copy the contents into a newly allocated instance of
 *		H5SM_master_table_t, and return a pointer to the new instance.
 *
 * Return:      Success:        Pointer to in core representation
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5SM__cache_table_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                              bool H5_ATTR_UNUSED *dirty)
{
    H5F_t                 *f;            /* File pointer -- from user data */
    H5SM_master_table_t   *table = NULL; /* Shared message table that we deserializing */
    H5SM_table_cache_ud_t *udata = (H5SM_table_cache_ud_t *)_udata; /* Pointer to user data */
    const uint8_t         *image = (const uint8_t *)_image;         /* Pointer into input buffer */
    uint32_t               stored_chksum;                           /* Stored metadata checksum value */
    size_t                 u;                                       /* Counter variable for index headers */
    void                  *ret_value = NULL;                        /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(image);
    assert(len > 0);
    assert(udata);
    assert(udata->f);
    f = udata->f;
    assert(dirty);

    /* Verify that we're reading version 0 of the table; this is the only
     * version defined so far.
     */
    assert(H5F_SOHM_VERS(f) == HDF5_SHAREDHEADER_VERSION);

    /* Allocate space for the master table in memory */
    if (NULL == (table = H5FL_CALLOC(H5SM_master_table_t)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Read number of indexes and version from file superblock */
    table->num_indexes = H5F_SOHM_NINDEXES(f);
    assert(table->num_indexes > 0);

    /* Compute the size of the SOHM table header on disk.  This is the "table"
     * itself plus each index within the table
     */
    table->table_size = H5SM_TABLE_SIZE(f);
    assert(table->table_size == len);

    /* Check magic number */
    if (memcmp(image, H5SM_TABLE_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, NULL, "bad SOHM table signature");
    image += H5_SIZEOF_MAGIC;

    /* Allocate space for the index headers in memory*/
    if (NULL == (table->indexes =
                     (H5SM_index_header_t *)H5FL_ARR_MALLOC(H5SM_index_header_t, (size_t)table->num_indexes)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, NULL, "memory allocation failed for SOHM indexes");

    /* Read in the index headers */
    for (u = 0; u < table->num_indexes; ++u) {
        /* Verify correct version of index list */
        if (H5SM_LIST_VERSION != *image++)
            HGOTO_ERROR(H5E_SOHM, H5E_VERSION, NULL, "bad shared message list version number");

        /* Type of the index (list or B-tree) */
        table->indexes[u].index_type = (H5SM_index_type_t)*image++;

        /* Type of messages in the index */
        UINT16DECODE(image, table->indexes[u].mesg_types);

        /* Minimum size of message to share */
        UINT32DECODE(image, table->indexes[u].min_mesg_size);

        /* List cutoff; fewer than this number and index becomes a list */
        UINT16DECODE(image, table->indexes[u].list_max);

        /* B-tree cutoff; more than this number and index becomes a B-tree */
        UINT16DECODE(image, table->indexes[u].btree_min);

        /* Number of messages shared */
        UINT16DECODE(image, table->indexes[u].num_messages);

        /* Address of the actual index */
        H5F_addr_decode(f, &image, &(table->indexes[u].index_addr));

        /* Address of the index's heap */
        H5F_addr_decode(f, &image, &(table->indexes[u].heap_addr));

        /* Compute the size of a list index for this SOHM index */
        table->indexes[u].list_size = H5SM_LIST_SIZE(f, table->indexes[u].list_max);
    } /* end for */

    /* checksum verification already done in verify_chksum cb */

    /* Read in checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) == table->table_size);

    /* Set return value */
    ret_value = table;

done:
    if (!ret_value && table)
        if (H5SM__table_free(table) < 0)
            HDONE_ERROR(H5E_SOHM, H5E_CANTFREE, NULL, "unable to destroy sohm table");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__cache_table_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_image_len
 *
 * Purpose:	Compute the size in bytes of the specified instance of
 *		H5SM_master_table_t on disk, and return it in *image_len.
 *		On failure, the value of *image_len is undefined.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_table_image_len(const void *_thing, size_t *image_len)
{
    const H5SM_master_table_t *table =
        (const H5SM_master_table_t *)_thing; /* Shared message table to query */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(table);
    assert(table->cache_info.type == H5AC_SOHM_TABLE);
    assert(image_len);

    *image_len = table->table_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__cache_table_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_serialize
 *
 * Purpose:	Serialize the contents of the supplied shared message table, and
 *		load this data into the supplied buffer.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_table_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5SM_master_table_t *table = (H5SM_master_table_t *)_thing; /* Shared message table to encode */
    uint8_t             *image = (uint8_t *)_image;             /* Pointer into raw data buffer */
    uint32_t             computed_chksum;                       /* Computed metadata checksum value */
    size_t               u;                                     /* Counter variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(image);
    assert(table);
    assert(table->cache_info.type == H5AC_SOHM_TABLE);
    assert(table->table_size == len);

    /* Verify that we're writing version 0 of the table; this is the only
     * version defined so far.
     */
    assert(H5F_SOHM_VERS(f) == HDF5_SHAREDHEADER_VERSION);

    /* Encode magic number */
    H5MM_memcpy(image, H5SM_TABLE_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* Encode each index header */
    for (u = 0; u < table->num_indexes; ++u) {
        /* Version for this list */
        *image++ = H5SM_LIST_VERSION;

        /* Is message index a list or a B-tree? */
        *image++ = (uint8_t)table->indexes[u].index_type;

        /* Type of messages in the index */
        UINT16ENCODE(image, table->indexes[u].mesg_types);

        /* Minimum size of message to share */
        UINT32ENCODE(image, table->indexes[u].min_mesg_size);

        /* List cutoff; fewer than this number and index becomes a list */
        UINT16ENCODE(image, table->indexes[u].list_max);

        /* B-tree cutoff; more than this number and index becomes a B-tree */
        UINT16ENCODE(image, table->indexes[u].btree_min);

        /* Number of messages shared */
        UINT16ENCODE(image, table->indexes[u].num_messages);

        /* Address of the actual index */
        H5F_addr_encode(f, &image, table->indexes[u].index_addr);

        /* Address of the index's heap */
        H5F_addr_encode(f, &image, table->indexes[u].heap_addr);
    } /* end for */

    /* Compute checksum on buffer */
    computed_chksum = H5_checksum_metadata(_image, (table->table_size - H5SM_SIZEOF_CHECKSUM), 0);
    UINT32ENCODE(image, computed_chksum);

    /* sanity check */
    assert((size_t)(image - ((uint8_t *)_image)) == table->table_size);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__cache_table_serialize() */

/*****************************************/
/* no H5SM_cache_table_notify() function */
/*****************************************/

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_table_free_icr
 *
 * Purpose:	Free memory used by the SOHM table.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_table_free_icr(void *_thing)
{
    H5SM_master_table_t *table     = (H5SM_master_table_t *)_thing; /* Shared message table to release */
    herr_t               ret_value = SUCCEED;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(table);
    assert(table->cache_info.type == H5AC_SOHM_TABLE);

    /* Destroy Shared Object Header Message table */
    if (H5SM__table_free(table) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTRELEASE, FAIL, "unable to free shared message table");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM_cache_table_free_icr() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_list_get_initial_load_size()
 *
 * Purpose:	Return the on disk size of list of SOHM messages.  In this case,
 *		we simply look up the size in the user data, and return that value
 *		in *image_len.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_list_get_initial_load_size(void *_udata, size_t *image_len)
{
    const H5SM_list_cache_ud_t *udata = (const H5SM_list_cache_ud_t *)_udata; /* User data for callback */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(udata);
    assert(udata->header);
    assert(udata->header->list_size > 0);
    assert(image_len);

    /* Set the image length size */
    *image_len = udata->header->list_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__cache_list_get_initial_load_size() */

/*-------------------------------------------------------------------------
 * Function:	H5SM__cache_list_verify_chksum
 *
 * Purpose:     Verify the computed checksum of the data structure is the
 *              same as the stored chksum.
 *
 * Return:      Success:        true/false
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5SM__cache_list_verify_chksum(const void *_image, size_t H5_ATTR_UNUSED len, void *_udata)
{
    const uint8_t        *image = (const uint8_t *)_image;        /* Pointer into raw data buffer */
    H5SM_list_cache_ud_t *udata = (H5SM_list_cache_ud_t *)_udata; /* User data for callback */
    size_t                chk_size;         /* Exact size of the node with checksum at the end */
    uint32_t              stored_chksum;    /* Stored metadata checksum value */
    uint32_t              computed_chksum;  /* Computed metadata checksum value */
    htri_t                ret_value = true; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(image);
    assert(udata);

    /* Exact size with checksum at the end */
    chk_size = H5SM_LIST_SIZE(udata->f, udata->header->num_messages);

    /* Get stored and computed checksums */
    H5F_get_checksums(image, chk_size, &stored_chksum, &computed_chksum);

    if (stored_chksum != computed_chksum)
        ret_value = false;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__cache_list_verify_chksum() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_list_deserialize
 *
 * Purpose:	Given a buffer containing the on disk image of a list of
 *		SOHM message, deserialize the list, load it into a newly allocated
 *		instance of H5SM_list_t, and return a pointer to same.
 *
 * Return:      Success:        Pointer to in core representation
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5SM__cache_list_deserialize(const void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_udata,
                             bool H5_ATTR_UNUSED *dirty)
{
    H5SM_list_t          *list  = NULL;                           /* The SOHM list being read in */
    H5SM_list_cache_ud_t *udata = (H5SM_list_cache_ud_t *)_udata; /* User data for callback */
    H5SM_bt2_ctx_t        ctx;                                    /* Message encoding context */
    const uint8_t        *image = (const uint8_t *)_image;        /* Pointer into input buffer */
    uint32_t              stored_chksum;                          /* Stored metadata checksum value */
    size_t                u;                                      /* Counter variable for messages in list */
    void                 *ret_value = NULL;                       /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(image);
    assert(len > 0);
    assert(udata);
    assert(udata->header);
    assert(udata->header->list_size == len);
    assert(dirty);

    /* Allocate space for the SOHM list data structure */
    if (NULL == (list = H5FL_MALLOC(H5SM_list_t)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, NULL, "memory allocation failed");
    memset(&list->cache_info, 0, sizeof(H5AC_info_t));

    /* Allocate list in memory as an array*/
    if (NULL == (list->messages = (H5SM_sohm_t *)H5FL_ARR_MALLOC(H5SM_sohm_t, udata->header->list_max)))
        HGOTO_ERROR(H5E_SOHM, H5E_NOSPACE, NULL, "file allocation failed for SOHM list");
    list->header = udata->header;

    /* Check magic number */
    if (memcmp(image, H5SM_LIST_MAGIC, (size_t)H5_SIZEOF_MAGIC) != 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, NULL, "bad SOHM list signature");
    image += H5_SIZEOF_MAGIC;

    /* Read messages into the list array */
    ctx.sizeof_addr = H5F_SIZEOF_ADDR(udata->f);
    for (u = 0; u < udata->header->num_messages; u++) {
        if (H5SM__message_decode(image, &(list->messages[u]), &ctx) < 0)
            HGOTO_ERROR(H5E_SOHM, H5E_CANTLOAD, NULL, "can't decode shared message");

        image += H5SM_SOHM_ENTRY_SIZE(udata->f);
    } /* end for */

    /* checksum verification already done in verify_chksum cb */

    /* Read in checksum */
    UINT32DECODE(image, stored_chksum);

    /* Sanity check */
    assert((size_t)(image - (const uint8_t *)_image) <= udata->header->list_size);

    /* Initialize the rest of the array */
    for (u = udata->header->num_messages; u < udata->header->list_max; u++)
        list->messages[u].location = H5SM_NO_LOC;

    /* Set return value */
    ret_value = list;

done:
    if (!ret_value && list) {
        if (list->messages)
            list->messages = H5FL_ARR_FREE(H5SM_sohm_t, list->messages);
        list = H5FL_FREE(H5SM_list_t, list);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__cache_list_deserialize() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_list_image_len
 *
 * Purpose:	Get the size of the shared message list on disk.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_list_image_len(const void *_thing, size_t *image_len)
{
    const H5SM_list_t *list = (const H5SM_list_t *)_thing; /* Shared message list to query */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(list);
    assert(list->cache_info.type == H5AC_SOHM_LIST);
    assert(list->header);
    assert(image_len);

    *image_len = list->header->list_size;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5SM__cache_list_image_len() */

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_list_serialize
 *
 * Purpose:	Serialize the contents of the supplied shared message list, and
 *		load this data into the supplied buffer.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_list_serialize(const H5F_t *f, void *_image, size_t H5_ATTR_NDEBUG_UNUSED len, void *_thing)
{
    H5SM_list_t   *list = (H5SM_list_t *)_thing; /* Instance being serialized */
    H5SM_bt2_ctx_t ctx;                          /* Message encoding context */
    uint8_t       *image = (uint8_t *)_image;    /* Pointer into raw data buffer */
    uint32_t       computed_chksum;              /* Computed metadata checksum value */
    size_t         mesgs_serialized;             /* Number of messages serialized */
    size_t         u;                            /* Local index variable */
    herr_t         ret_value = SUCCEED;          /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(image);
    assert(list);
    assert(list->cache_info.type == H5AC_SOHM_LIST);
    assert(list->header);
    assert(list->header->list_size == len);

    /* Encode magic number */
    H5MM_memcpy(image, H5SM_LIST_MAGIC, (size_t)H5_SIZEOF_MAGIC);
    image += H5_SIZEOF_MAGIC;

    /* serialize messages from the messages array */
    mesgs_serialized = 0;
    ctx.sizeof_addr  = H5F_SIZEOF_ADDR(f);
    for (u = 0; ((u < list->header->list_max) && (mesgs_serialized < list->header->num_messages)); u++) {
        if (list->messages[u].location != H5SM_NO_LOC) {
            if (H5SM__message_encode(image, &(list->messages[u]), &ctx) < 0)
                HGOTO_ERROR(H5E_SOHM, H5E_CANTFLUSH, FAIL, "unable to serialize shared message");

            image += H5SM_SOHM_ENTRY_SIZE(f);
            ++mesgs_serialized;
        } /* end if */
    }     /* end for */

    assert(mesgs_serialized == list->header->num_messages);

    /* Compute checksum on buffer */
    computed_chksum = H5_checksum_metadata(_image, (size_t)(image - (uint8_t *)_image), 0);
    UINT32ENCODE(image, computed_chksum);

    /* sanity check */
    assert((size_t)(image - (uint8_t *)_image) <= list->header->list_size);

    /* Clear memory */
    memset(image, 0, (list->header->list_size - (size_t)(image - (uint8_t *)_image)));

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5SM__cache_list_serialize() */

/****************************************/
/* no H5SM_cache_list_notify() function */
/****************************************/

/*-------------------------------------------------------------------------
 * Function:    H5SM__cache_list_free_icr
 *
 * Purpose:	Free all memory used by the list.
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5SM__cache_list_free_icr(void *_thing)
{
    H5SM_list_t *list      = (H5SM_list_t *)_thing; /* Shared message list to release */
    herr_t       ret_value = SUCCEED;               /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(list);
    assert(list->cache_info.type == H5AC_SOHM_LIST);

    /* Destroy Shared Object Header Message list */
    if (H5SM__list_free(list) < 0)
        HGOTO_ERROR(H5E_SOHM, H5E_CANTRELEASE, FAIL, "unable to free shared message list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5O_cache_list_free_icr() */
