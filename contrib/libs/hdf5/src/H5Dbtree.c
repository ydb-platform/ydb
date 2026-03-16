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

/* Purpose:     v1 B-tree indexed (chunked) I/O functions.  The chunks are
 *              given a multi-dimensional index which is used as a lookup key
 *              in a B-tree that maps chunk index to disk address.
 */

/****************/
/* Module Setup */
/****************/

#include "H5Dmodule.h" /* This source code file is part of the H5D module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                    */
#include "H5Bprivate.h"  /* B-link trees                         */
#include "H5Dpkg.h"      /* Datasets                             */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Fprivate.h"  /* Files                                */
#include "H5FDprivate.h" /* File drivers                         */
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5MFprivate.h" /* File space management                */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Oprivate.h"  /* Object headers                       */
#include "H5Sprivate.h"  /* Dataspaces                           */
#include "H5VMprivate.h" /* Vector and array functions           */

/****************/
/* Local Macros */
/****************/

#define H5D_BTREE_IDX_IS_OPEN(idx_info) (NULL != (idx_info)->storage->u.btree.shared)

/******************/
/* Local Typedefs */
/******************/

/*
 * B-tree key.  A key contains the minimum logical N-dimensional coordinates and
 * the logical size of the chunk to which this key refers.  The
 * fastest-varying dimension is assumed to reference individual bytes of the
 * array, so a 100-element 1-d array of 4-byte integers would really be a 2-d
 * array with the slow varying dimension of size 100 and the fast varying
 * dimension of size 4 (the storage dimensionality has very little to do with
 * the real dimensionality).
 *
 * Only the first few values of the OFFSET and SIZE fields are actually
 * stored on disk, depending on the dimensionality.
 *
 * The chunk's file address is part of the B-tree and not part of the key.
 */
typedef struct H5D_btree_key_t {
    hsize_t  scaled[H5O_LAYOUT_NDIMS]; /*logical offset to start */
    uint32_t nbytes;                   /*size of stored data */
    unsigned filter_mask;              /*excluded filters */
} H5D_btree_key_t;

/* B-tree callback info for iteration over chunks */
typedef struct H5D_btree_it_ud_t {
    H5D_chunk_common_ud_t common; /* Common info for B-tree user data (must be first) */
    H5D_chunk_cb_func_t   cb;     /* Chunk callback routine */
    void                 *udata;  /* User data for chunk callback routine */
} H5D_btree_it_ud_t;

/* B-tree callback info for debugging */
typedef struct H5D_btree_dbg_t {
    H5D_chunk_common_ud_t common; /* Common info for B-tree user data (must be first) */
    unsigned              ndims;  /* Number of dimensions */
} H5D_btree_dbg_t;

/********************/
/* Local Prototypes */
/********************/

static herr_t H5D__btree_shared_free(void *_shared);
static herr_t H5D__btree_shared_create(const H5F_t *f, H5O_storage_chunk_t *store,
                                       const H5O_layout_chunk_t *layout);

/* B-tree iterator callbacks */
static int H5D__btree_idx_iterate_cb(H5F_t *f, const void *left_key, haddr_t addr, const void *right_key,
                                     void *_udata);

/* B-tree callbacks */
static H5UC_t   *H5D__btree_get_shared(const H5F_t *f, const void *_udata);
static herr_t    H5D__btree_new_node(H5F_t *f, H5B_ins_t, void *_lt_key, void *_udata, void *_rt_key,
                                     haddr_t *addr_p /*out*/);
static int       H5D__btree_cmp2(void *_lt_key, void *_udata, void *_rt_key);
static int       H5D__btree_cmp3(void *_lt_key, void *_udata, void *_rt_key);
static htri_t    H5D__btree_found(H5F_t *f, haddr_t addr, const void *_lt_key, bool *found, void *_udata);
static H5B_ins_t H5D__btree_insert(H5F_t *f, haddr_t addr, void *_lt_key, bool *lt_key_changed, void *_md_key,
                                   void *_udata, void *_rt_key, bool *rt_key_changed,
                                   haddr_t *new_node /*out*/);
static H5B_ins_t H5D__btree_remove(H5F_t *f, haddr_t addr, void *_lt_key, bool *lt_key_changed, void *_udata,
                                   void *_rt_key, bool *rt_key_changed);
static herr_t    H5D__btree_decode_key(const H5B_shared_t *shared, const uint8_t *raw, void *_key);
static herr_t    H5D__btree_encode_key(const H5B_shared_t *shared, uint8_t *raw, const void *_key);
static herr_t H5D__btree_debug_key(FILE *stream, int indent, int fwidth, const void *key, const void *udata);

/* Chunked layout indexing callbacks */
static herr_t H5D__btree_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t *space,
                                  haddr_t dset_ohdr_addr);
static herr_t H5D__btree_idx_create(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__btree_idx_open(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__btree_idx_close(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__btree_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open);
static bool   H5D__btree_idx_is_space_alloc(const H5O_storage_chunk_t *storage);
static herr_t H5D__btree_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                                    const H5D_t *dset);
static herr_t H5D__btree_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata);
static herr_t H5D__btree_idx_load_metadata(const H5D_chk_idx_info_t *idx_info);
static int    H5D__btree_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb,
                                     void *chunk_udata);
static herr_t H5D__btree_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata);
static herr_t H5D__btree_idx_delete(const H5D_chk_idx_info_t *idx_info);
static herr_t H5D__btree_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src,
                                        const H5D_chk_idx_info_t *idx_info_dst);
static herr_t H5D__btree_idx_copy_shutdown(H5O_storage_chunk_t *storage_src,
                                           H5O_storage_chunk_t *storage_dst);
static herr_t H5D__btree_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *size);
static herr_t H5D__btree_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr);
static herr_t H5D__btree_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream);
static herr_t H5D__btree_idx_dest(const H5D_chk_idx_info_t *idx_info);

/*********************/
/* Package Variables */
/*********************/

/* v1 B-tree indexed chunk I/O ops */
const H5D_chunk_ops_t H5D_COPS_BTREE[1] = {{
    false,                         /* v1 B-tree indices does not support SWMR access */
    H5D__btree_idx_init,           /* insert */
    H5D__btree_idx_create,         /* create */
    H5D__btree_idx_open,           /* open */
    H5D__btree_idx_close,          /* close */
    H5D__btree_idx_is_open,        /* is_open */
    H5D__btree_idx_is_space_alloc, /* is_space_alloc */
    H5D__btree_idx_insert,         /* insert */
    H5D__btree_idx_get_addr,       /* get_addr */
    H5D__btree_idx_load_metadata,  /* load_metadata */
    NULL,                          /* resize */
    H5D__btree_idx_iterate,        /* iterate */
    H5D__btree_idx_remove,         /* remove */
    H5D__btree_idx_delete,         /* delete */
    H5D__btree_idx_copy_setup,     /* copy_setup */
    H5D__btree_idx_copy_shutdown,  /* copy_shutdown */
    H5D__btree_idx_size,           /* size */
    H5D__btree_idx_reset,          /* reset */
    H5D__btree_idx_dump,           /* dump */
    H5D__btree_idx_dest            /* destroy */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/* inherits B-tree like properties from H5B */
static H5B_class_t H5B_BTREE[1] = {{
    H5B_CHUNK_ID,            /* id */
    sizeof(H5D_btree_key_t), /* sizeof_nkey */
    H5D__btree_get_shared,   /* get_shared */
    H5D__btree_new_node,     /* new */
    H5D__btree_cmp2,         /* cmp2 */
    H5D__btree_cmp3,         /* cmp3 */
    H5D__btree_found,        /* found */
    H5D__btree_insert,       /* insert */
    false,                   /* follow min branch? */
    false,                   /* follow max branch? */
    H5B_LEFT,                /* critical key */
    H5D__btree_remove,       /* remove */
    H5D__btree_decode_key,   /* decode */
    H5D__btree_encode_key,   /* encode */
    H5D__btree_debug_key     /* debug */
}};

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage H5O_layout_chunk_t objects */
H5FL_DEFINE_STATIC(H5O_layout_chunk_t);

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_get_shared
 *
 * Purpose:     Returns the shared B-tree info for the specified UDATA.
 *
 * Return:      Success:    Pointer to the raw B-tree page for this dataset
 *
 *              Failure:    Can't fail
 *
 *-------------------------------------------------------------------------
 */
static H5UC_t *
H5D__btree_get_shared(const H5F_t H5_ATTR_UNUSED *f, const void *_udata)
{
    const H5D_chunk_common_ud_t *udata = (const H5D_chunk_common_ud_t *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    assert(udata);
    assert(udata->storage);
    assert(udata->storage->idx_type == H5D_CHUNK_IDX_BTREE);
    assert(udata->storage->u.btree.shared);

    /* Return the pointer to the ref-count object */
    FUNC_LEAVE_NOAPI(udata->storage->u.btree.shared)
} /* end H5D__btree_get_shared() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_new_node
 *
 * Purpose:     Adds a new entry to an i-storage B-tree.  We can assume
 *              that the domain represented by UDATA doesn't intersect the
 *              domain already represented by the B-tree.
 *
 * Return:      Success:    Non-negative. The address of leaf is returned
 *                          through the ADDR argument.  It is also added
 *                          to the UDATA.
 *
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_new_node(H5F_t H5_ATTR_NDEBUG_UNUSED *f, H5B_ins_t op, void *_lt_key, void *_udata, void *_rt_key,
                    haddr_t *addr_p /*out*/)
{
    H5D_btree_key_t *lt_key = (H5D_btree_key_t *)_lt_key;
    H5D_btree_key_t *rt_key = (H5D_btree_key_t *)_rt_key;
    H5D_chunk_ud_t  *udata  = (H5D_chunk_ud_t *)_udata;
    unsigned         u;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(f);
    assert(lt_key);
    assert(rt_key);
    assert(udata);
    assert(udata->common.layout->ndims > 0 && udata->common.layout->ndims < H5O_LAYOUT_NDIMS);
    assert(addr_p);

    /* Set address */
    assert(H5_addr_defined(udata->chunk_block.offset));
    assert(udata->chunk_block.length > 0);
    *addr_p = udata->chunk_block.offset;

    /*
     * The left key describes the storage of the UDATA chunk being
     * inserted into the tree.
     */
    H5_CHECKED_ASSIGN(lt_key->nbytes, uint32_t, udata->chunk_block.length, hsize_t);
    lt_key->filter_mask = udata->filter_mask;
    for (u = 0; u < udata->common.layout->ndims; u++)
        lt_key->scaled[u] = udata->common.scaled[u];

    /*
     * The right key might already be present.  If not, then add a zero-width
     * chunk.
     */
    if (H5B_INS_LEFT != op) {
        rt_key->nbytes      = 0;
        rt_key->filter_mask = 0;
        for (u = 0; u < udata->common.layout->ndims; u++) {
            assert(udata->common.scaled[u] + 1 > udata->common.scaled[u]);
            rt_key->scaled[u] = udata->common.scaled[u] + 1;
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_new_node() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_cmp2
 *
 * Purpose:     Compares two keys sort of like strcmp().  The UDATA pointer
 *              is only to supply extra information not carried in the keys
 *              (in this case, the dimensionality) and is not compared
 *              against the keys.
 *
 * Return:      Success:    -1 if LT_KEY is less than RT_KEY;
 *                          1 if LT_KEY is greater than RT_KEY;
 *                          0 if LT_KEY and RT_KEY are equal.
 *
 *              Failure:    FAIL (same as LT_KEY < RT_KEY)
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__btree_cmp2(void *_lt_key, void *_udata, void *_rt_key)
{
    H5D_btree_key_t       *lt_key    = (H5D_btree_key_t *)_lt_key;
    H5D_btree_key_t       *rt_key    = (H5D_btree_key_t *)_rt_key;
    H5D_chunk_common_ud_t *udata     = (H5D_chunk_common_ud_t *)_udata;
    int                    ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(lt_key);
    assert(rt_key);
    assert(udata);
    assert(udata->layout->ndims > 0 && udata->layout->ndims <= H5O_LAYOUT_NDIMS);

    /* Compare the offsets but ignore the other fields */
    ret_value = H5VM_vector_cmp_u(udata->layout->ndims, lt_key->scaled, rt_key->scaled);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_cmp2() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_cmp3
 *
 * Purpose:     Compare the requested datum UDATA with the left and right
 *              keys of the B-tree.
 *
 * Return:      Success:    negative if the min_corner of UDATA is less
 *                          than the min_corner of LT_KEY.
 *
 *                          positive if the min_corner of UDATA is
 *                          greater than or equal the min_corner of
 *                          RT_KEY.
 *
 *                          zero otherwise.  The min_corner of UDATA is
 *                          not necessarily contained within the address
 *                          space represented by LT_KEY, but a key that
 *                          would describe the UDATA min_corner address
 *                          would fall lexicographically between LT_KEY
 *                          and RT_KEY.
 *
 *              Failure:    FAIL (same as UDATA < LT_KEY)
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__btree_cmp3(void *_lt_key, void *_udata, void *_rt_key)
{
    H5D_btree_key_t       *lt_key    = (H5D_btree_key_t *)_lt_key;
    H5D_btree_key_t       *rt_key    = (H5D_btree_key_t *)_rt_key;
    H5D_chunk_common_ud_t *udata     = (H5D_chunk_common_ud_t *)_udata;
    int                    ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    assert(lt_key);
    assert(rt_key);
    assert(udata);
    assert(udata->layout->ndims > 0 && udata->layout->ndims <= H5O_LAYOUT_NDIMS);

    /* Special case for faster checks on 1-D chunks */
    /* (Checking for ndims==2 because last dimension is the datatype size) */
    /* The additional checking for the right key is necessary due to the */
    /* slightly odd way the library initializes the right-most node in the */
    /* indexed storage B-tree... */
    /* (Dump the B-tree with h5debug to look at it) -QAK */
    if (udata->layout->ndims == 2) {
        if (udata->scaled[0] > rt_key->scaled[0])
            ret_value = 1;
        else if (udata->scaled[0] == rt_key->scaled[0] && udata->scaled[1] >= rt_key->scaled[1])
            ret_value = 1;
        else if (udata->scaled[0] < lt_key->scaled[0])
            ret_value = (-1);
    } /* end if */
    else {
        if (H5VM_vector_ge_u(udata->layout->ndims, udata->scaled, rt_key->scaled))
            ret_value = 1;
        else if (H5VM_vector_lt_u(udata->layout->ndims, udata->scaled, lt_key->scaled))
            ret_value = (-1);
    } /* end else */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_cmp3() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_found
 *
 * Purpose:     This function is called when the B-tree search engine has
 *              found the leaf entry that points to a chunk of storage that
 *              contains the beginning of the logical address space
 *              represented by UDATA.  The LT_KEY is the left key (the one
 *              that describes the chunk) and RT_KEY is the right key (the
 *              one that describes the next or last chunk).
 *
 * Note:        It's possible that the chunk isn't really found.  For
 *              instance, in a sparse dataset the requested chunk might fall
 *              between two stored chunks in which case this function is
 *              called with the maximum stored chunk indices less than the
 *              requested chunk indices.
 *
 * Return:      Non-negative on success with information about the
 *              chunk returned through the UDATA argument, if *FOUND is
 *              true.
 *              Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static htri_t
H5D__btree_found(H5F_t H5_ATTR_UNUSED *f, haddr_t addr, const void *_lt_key, bool *found, void *_udata)
{
    H5D_chunk_ud_t        *udata  = (H5D_chunk_ud_t *)_udata;
    const H5D_btree_key_t *lt_key = (const H5D_btree_key_t *)_lt_key;
    unsigned               u;
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(lt_key);
    assert(found);
    assert(udata);

    /* Is this *really* the requested chunk? */
    for (u = 0; u < udata->common.layout->ndims; u++)
        if (udata->common.scaled[u] >= (lt_key->scaled[u] + 1)) {
            *found = false;
            HGOTO_DONE(SUCCEED);
        }

    /* Initialize return values */
    assert(lt_key->nbytes > 0);
    udata->chunk_block.offset = addr;
    udata->chunk_block.length = lt_key->nbytes;
    udata->filter_mask        = lt_key->filter_mask;
    *found                    = true;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_found() */

/*-------------------------------------------------------------------------
 * Function:    H5D__chunk_disjoint
 *
 * Purpose:     Determines if two chunks are disjoint.
 *
 * Return:      Success:    false if they are not disjoint.
 *                          true if they are disjoint.
 *
 * Note:        Assumes that the chunk offsets are scaled coordinates
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__chunk_disjoint(unsigned n, const hsize_t *scaled1, const hsize_t *scaled2)
{
    unsigned u;                 /* Local index variable */
    bool     ret_value = false; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(n);
    assert(scaled1);
    assert(scaled2);

    /* Loop over two chunks, detecting disjointness and getting out quickly */
    for (u = 0; u < n; u++)
        if ((scaled1[u] + 1) <= scaled2[u] || (scaled2[u] + 1) <= scaled1[u])
            HGOTO_DONE(true);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__chunk_disjoint() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_insert
 *
 * Purpose:     This function is called when the B-tree insert engine finds
 *              the node to use to insert new data.  The UDATA argument
 *              points to a struct that describes the logical addresses being
 *              added to the file.  This function allocates space for the
 *              data and returns information through UDATA describing a
 *              file chunk to receive (part of) the data.
 *
 *              The LT_KEY is always the key describing the chunk of file
 *              memory at address ADDR. On entry, UDATA describes the logical
 *              addresses for which storage is being requested (through the
 *              `offset' and `size' fields). On return, UDATA describes the
 *              logical addresses contained in a chunk on disk.
 *
 * Return:      Success:    An insertion command for the caller, one of
 *                          the H5B_INS_* constants.  The address of the
 *                          new chunk is returned through the NEW_NODE
 *                          argument.
 *
 *              Failure:    H5B_INS_ERROR
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5D__btree_insert(H5F_t H5_ATTR_NDEBUG_UNUSED *f, haddr_t H5_ATTR_NDEBUG_UNUSED addr, void *_lt_key,
                  bool *lt_key_changed, void *_md_key, void *_udata, void *_rt_key,
                  bool H5_ATTR_UNUSED *rt_key_changed, haddr_t *new_node_p /*out*/)
{
    H5D_btree_key_t *lt_key = (H5D_btree_key_t *)_lt_key;
    H5D_btree_key_t *md_key = (H5D_btree_key_t *)_md_key;
    H5D_btree_key_t *rt_key = (H5D_btree_key_t *)_rt_key;
    H5D_chunk_ud_t  *udata  = (H5D_chunk_ud_t *)_udata;
    int              cmp;
    unsigned         u;
    H5B_ins_t        ret_value = H5B_INS_ERROR; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check args */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(lt_key);
    assert(lt_key_changed);
    assert(md_key);
    assert(udata);
    assert(rt_key);
    assert(new_node_p);

    cmp = H5D__btree_cmp3(lt_key, udata, rt_key);
    assert(cmp <= 0);

    if (cmp < 0) {
        /* Negative indices not supported yet */
        HGOTO_ERROR(H5E_STORAGE, H5E_UNSUPPORTED, H5B_INS_ERROR, "internal error");
    }
    else if (H5VM_vector_eq_u(udata->common.layout->ndims, udata->common.scaled, lt_key->scaled) &&
             lt_key->nbytes > 0) {
        /*
         * Already exists.  If the new size is not the same as the old size
         * then we should reallocate storage.
         */
        if (lt_key->nbytes != udata->chunk_block.length) {
            /* Set node's address (already re-allocated by main chunk routines) */
            assert(H5_addr_defined(udata->chunk_block.offset));
            *new_node_p = udata->chunk_block.offset;
            H5_CHECKED_ASSIGN(lt_key->nbytes, uint32_t, udata->chunk_block.length, hsize_t);
            lt_key->filter_mask = udata->filter_mask;
            *lt_key_changed     = true;
            ret_value           = H5B_INS_CHANGE;
        }
        else {
            /* Already have address in udata, from main chunk routines */
            assert(H5_addr_defined(udata->chunk_block.offset));
            ret_value = H5B_INS_NOOP;
        }
    }
    else if (H5D__chunk_disjoint(udata->common.layout->ndims, lt_key->scaled, udata->common.scaled)) {
        assert(H5D__chunk_disjoint(udata->common.layout->ndims, rt_key->scaled, udata->common.scaled));
        /*
         * Split this node, inserting the new new node to the right of the
         * current node.  The MD_KEY is where the split occurs.
         */
        H5_CHECKED_ASSIGN(md_key->nbytes, uint32_t, udata->chunk_block.length, hsize_t);
        md_key->filter_mask = udata->filter_mask;
        for (u = 0; u < udata->common.layout->ndims; u++)
            md_key->scaled[u] = udata->common.scaled[u];

        assert(H5_addr_defined(udata->chunk_block.offset));
        *new_node_p = udata->chunk_block.offset;
        ret_value   = H5B_INS_RIGHT;
    }
    else {
        HGOTO_ERROR(H5E_IO, H5E_UNSUPPORTED, H5B_INS_ERROR, "internal error");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_remove
 *
 * Purpose:     Removes chunks that are no longer necessary in the B-tree.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5D__btree_remove(H5F_t *f, haddr_t addr, void *_lt_key /*in,out */, bool *lt_key_changed /*out */,
                  void H5_ATTR_UNUSED *_udata /*in,out */, void H5_ATTR_UNUSED *_rt_key /*in,out */,
                  bool *rt_key_changed /*out */)
{
    H5D_btree_key_t *lt_key    = (H5D_btree_key_t *)_lt_key;
    H5B_ins_t        ret_value = H5B_INS_REMOVE; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Remove raw data chunk from file */
    H5_CHECK_OVERFLOW(lt_key->nbytes, uint32_t, hsize_t);
    if (H5MF_xfree(f, H5FD_MEM_DRAW, addr, (hsize_t)lt_key->nbytes) < 0)
        HGOTO_ERROR(H5E_STORAGE, H5E_CANTFREE, H5B_INS_ERROR, "unable to free chunk");

    /* Mark keys as unchanged */
    *lt_key_changed = false;
    *rt_key_changed = false;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_decode_key
 *
 * Purpose:     Decodes a raw key into a native key for the B-tree
 *
 * Return:      SUCCEED/FAIL
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_decode_key(const H5B_shared_t *shared, const uint8_t *raw, void *_key)
{
    const H5O_layout_chunk_t *layout;                        /* Chunk layout description */
    H5D_btree_key_t          *key = (H5D_btree_key_t *)_key; /* Pointer to decoded key */
    hsize_t                   tmp_offset;                    /* Temporary coordinate offset, from file */
    herr_t                    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(shared);
    assert(raw);
    assert(key);
    layout = (const H5O_layout_chunk_t *)shared->udata;
    assert(layout);

    if (layout->ndims > H5O_LAYOUT_NDIMS)
        HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "bad number of dimensions");

    UINT32DECODE(raw, key->nbytes);
    UINT32DECODE(raw, key->filter_mask);
    for (unsigned u = 0; u < layout->ndims; u++) {
        if (layout->dim[u] == 0)
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "chunk size must be > 0, dim = %u ", u);

        /* Retrieve coordinate offset */
        UINT64DECODE(raw, tmp_offset);
        if (0 != (tmp_offset % layout->dim[u]))
            HGOTO_ERROR(H5E_DATASET, H5E_BADVALUE, FAIL, "bad coordinate offset");

        /* Convert to a scaled offset */
        key->scaled[u] = tmp_offset / layout->dim[u];
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_decode_key() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_encode_key
 *
 * Purpose:     Encode a key from native format to raw format.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_encode_key(const H5B_shared_t *shared, uint8_t *raw, const void *_key)
{
    const H5O_layout_chunk_t *layout; /* Chunk layout description */
    const H5D_btree_key_t    *key = (const H5D_btree_key_t *)_key;
    hsize_t                   tmp_offset; /* Temporary coordinate offset, from file */
    unsigned                  u;          /* Local index variable */

    FUNC_ENTER_PACKAGE_NOERR

    /* check args */
    assert(shared);
    assert(raw);
    assert(key);
    layout = (const H5O_layout_chunk_t *)shared->udata;
    assert(layout);
    assert(layout->ndims > 0 && layout->ndims <= H5O_LAYOUT_NDIMS);

    /* encode */
    UINT32ENCODE(raw, key->nbytes);
    UINT32ENCODE(raw, key->filter_mask);
    for (u = 0; u < layout->ndims; u++) {
        /* Compute coordinate offset from scaled offset */
        tmp_offset = key->scaled[u] * layout->dim[u];
        UINT64ENCODE(raw, tmp_offset);
    } /* end for */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_encode_key() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_debug_key
 *
 * Purpose:     Prints a key.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_debug_key(FILE *stream, int indent, int fwidth, const void *_key, const void *_udata)
{
    const H5D_btree_key_t *key   = (const H5D_btree_key_t *)_key;
    const H5D_btree_dbg_t *udata = (const H5D_btree_dbg_t *)_udata;
    unsigned               u;

    FUNC_ENTER_PACKAGE_NOERR

    assert(key);

    fprintf(stream, "%*s%-*s %u bytes\n", indent, "", fwidth, "Chunk size:", (unsigned)key->nbytes);
    fprintf(stream, "%*s%-*s 0x%08x\n", indent, "", fwidth, "Filter mask:", key->filter_mask);
    fprintf(stream, "%*s%-*s {", indent, "", fwidth, "Logical offset:");
    for (u = 0; u < udata->ndims; u++)
        fprintf(stream, "%s%" PRIuHSIZE, u ? ", " : "", (key->scaled[u] * udata->common.layout->dim[u]));
    fputs("}\n", stream);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_debug_key() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_shared_free
 *
 * Purpose:     Free "local" B-tree shared info
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_shared_free(void *_shared)
{
    H5B_shared_t *shared    = (H5B_shared_t *)_shared;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the chunk layout information */
    shared->udata = H5FL_FREE(H5O_layout_chunk_t, shared->udata);

    /* Chain up to the generic B-tree shared info free routine */
    if (H5B_shared_free(shared) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "can't free shared B-tree info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_shared_free() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_shared_create
 *
 * Purpose:     Create & initialize B-tree shared info
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_shared_create(const H5F_t *f, H5O_storage_chunk_t *store, const H5O_layout_chunk_t *layout)
{
    H5B_shared_t       *shared;              /* Shared B-tree node info */
    H5O_layout_chunk_t *my_layout = NULL;    /* Pointer to copy of layout info */
    size_t              sizeof_rkey;         /* Size of raw (disk) key	     */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Set the raw key size */
    sizeof_rkey = 4 +                /*storage size		*/
                  4 +                /*filter mask		*/
                  layout->ndims * 8; /*dimension indices	*/

    /* Allocate & initialize global info for the shared structure */
    if (NULL == (shared = H5B_shared_new(f, H5B_BTREE, sizeof_rkey)))
        HGOTO_ERROR(H5E_DATASET, H5E_NOSPACE, FAIL, "memory allocation failed for shared B-tree info");

    /* Set up the "local" information for this dataset's chunks */
    if (NULL == (my_layout = H5FL_MALLOC(H5O_layout_chunk_t)))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "can't allocate chunk layout");
    H5MM_memcpy(my_layout, layout, sizeof(H5O_layout_chunk_t));
    shared->udata = my_layout;

    /* Make shared B-tree info reference counted */
    if (NULL == (store->u.btree.shared = H5UC_create(shared, H5D__btree_shared_free)))
        HGOTO_ERROR(H5E_DATASET, H5E_NOSPACE, FAIL, "can't create ref-count wrapper for shared B-tree info");

done:
    if (ret_value < 0)
        if (my_layout)
            my_layout = H5FL_FREE(H5O_layout_chunk_t, my_layout);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_shared_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_init
 *
 * Purpose:     Initialize the indexing information for a dataset.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_init(const H5D_chk_idx_info_t *idx_info, const H5S_t H5_ATTR_UNUSED *space,
                    haddr_t dset_ohdr_addr)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(dset_ohdr_addr));

    idx_info->storage->u.btree.dset_ohdr_addr = dset_ohdr_addr;

    /* Allocate the shared structure */
    if (H5D__btree_shared_create(idx_info->f, idx_info->storage, idx_info->layout) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "can't create wrapper for shared B-tree info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_init() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_create
 *
 * Purpose:     Creates a new indexed-storage B-tree and initializes the
 *              layout struct with information about the storage.  The
 *              struct should be immediately written to the object header.
 *
 *              This function must be called before passing LAYOUT to any
 *              of the other indexed storage functions!
 *
 * Return:      Non-negative on success (with the LAYOUT argument
 *              initialized and ready to write to an object header).
 *              Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_create(const H5D_chk_idx_info_t *idx_info)
{
    H5D_chunk_common_ud_t udata;               /* User data for B-tree callback */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(!H5_addr_defined(idx_info->storage->idx_addr));

    /* Initialize "user" data for B-tree callbacks, etc. */
    udata.layout  = idx_info->layout;
    udata.storage = idx_info->storage;

    /* Create the v1 B-tree for the chunk index */
    if (H5B_create(idx_info->f, H5B_BTREE, &udata, &(idx_info->storage->idx_addr) /*out*/) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create B-tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_create() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_open
 *
 * Purpose:     Opens an existing B-tree. Currently a no-op.
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_open(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_close
 *
 * Purpose:     Closes an existing B-tree. Currently a no-op.
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_close(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_close() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_is_open
 *
 * Purpose:     Query if the index is opened or not
 *
 * Return:      SUCCEED (can't fail)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_is_open(const H5D_chk_idx_info_t *idx_info, bool *is_open)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(idx_info);
    assert(idx_info->storage);
    assert(H5D_CHUNK_IDX_BTREE == idx_info->storage->idx_type);
    assert(is_open);

    *is_open = H5D_BTREE_IDX_IS_OPEN(idx_info);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_is_open() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_is_space_alloc
 *
 * Purpose:     Query if space is allocated for index method
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static bool
H5D__btree_idx_is_space_alloc(const H5O_storage_chunk_t *storage)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check args */
    assert(storage);

    FUNC_LEAVE_NOAPI((bool)H5_addr_defined(storage->idx_addr))
} /* end H5D__btree_idx_is_space_alloc() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_insert
 *
 * Purpose:     Insert chunk entry into the indexing structure.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_insert(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata,
                      const H5D_t H5_ATTR_UNUSED *dset)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /*
     * Create the chunk it if it doesn't exist, or reallocate the chunk if
     * its size changed.
     */
    if (H5B_insert(idx_info->f, H5B_BTREE, idx_info->storage->idx_addr, udata) < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "unable to allocate chunk");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__btree_idx_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_get_addr
 *
 * Purpose:     Get the file address of a chunk if file space has been
 *              assigned.  Save the retrieved information in the udata
 *              supplied.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_get_addr(const H5D_chk_idx_info_t *idx_info, H5D_chunk_ud_t *udata)
{
    bool   found;               /* Whether chunk was found */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->layout->ndims > 0);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Go get the chunk information from the B-tree */
    found = false;
    if (H5B_find(idx_info->f, H5B_BTREE, idx_info->storage->idx_addr, &found, udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTFIND, FAIL, "can't check for chunk in B-tree");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__btree_idx_get_addr() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_load_metadata
 *
 * Purpose:     Load additional chunk index metadata beyond the chunk index
 *              itself. Currently a no-op.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_load_metadata(const H5D_chk_idx_info_t H5_ATTR_UNUSED *idx_info)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* NO OP */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_load_metadata() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_iterate_cb
 *
 * Purpose:     Translate the B-tree specific chunk record into a generic
 *              form and make the callback to the generic chunk callback
 *              routine.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__btree_idx_iterate_cb(H5F_t H5_ATTR_UNUSED *f, const void *_lt_key, haddr_t addr,
                          const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    H5D_btree_it_ud_t     *udata  = (H5D_btree_it_ud_t *)_udata;      /* User data */
    const H5D_btree_key_t *lt_key = (const H5D_btree_key_t *)_lt_key; /* B-tree key for chunk */
    H5D_chunk_rec_t        chunk_rec;                                 /* Generic chunk record for callback */
    int                    ret_value = -1;                            /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check for memcpy() */
    HDcompile_assert(offsetof(H5D_chunk_rec_t, nbytes) == offsetof(H5D_btree_key_t, nbytes));
    HDcompile_assert(sizeof(chunk_rec.nbytes) == sizeof(lt_key->nbytes));
    HDcompile_assert(offsetof(H5D_chunk_rec_t, scaled) == offsetof(H5D_btree_key_t, scaled));
    HDcompile_assert(sizeof(chunk_rec.scaled) == sizeof(lt_key->scaled));
    HDcompile_assert(offsetof(H5D_chunk_rec_t, filter_mask) == offsetof(H5D_btree_key_t, filter_mask));
    HDcompile_assert(sizeof(chunk_rec.filter_mask) == sizeof(lt_key->filter_mask));

    /* Compose generic chunk record for callback */
    H5MM_memcpy(&chunk_rec, lt_key, sizeof(*lt_key));
    chunk_rec.chunk_addr = addr;

    /* Make "generic chunk" callback */
    if ((ret_value = (udata->cb)(&chunk_rec, udata->udata)) < 0)
        HERROR(H5E_DATASET, H5E_CALLBACK, "failure in generic chunk iterator callback");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__btree_idx_iterate_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_iterate
 *
 * Purpose:     Iterate over the chunks in an index, making a callback
 *              for each one.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static int
H5D__btree_idx_iterate(const H5D_chk_idx_info_t *idx_info, H5D_chunk_cb_func_t chunk_cb, void *chunk_udata)
{
    H5D_btree_it_ud_t udata;          /* User data for B-tree iterator callback */
    int               ret_value = -1; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(chunk_cb);
    assert(chunk_udata);

    /* Initialize userdata */
    memset(&udata, 0, sizeof udata);
    udata.common.layout  = idx_info->layout;
    udata.common.storage = idx_info->storage;
    udata.cb             = chunk_cb;
    udata.udata          = chunk_udata;

    /* Iterate over existing chunks */
    if ((ret_value = H5B_iterate(idx_info->f, H5B_BTREE, idx_info->storage->idx_addr,
                                 H5D__btree_idx_iterate_cb, &udata)) < 0)
        HERROR(H5E_DATASET, H5E_BADITER, "unable to iterate over chunk B-tree");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_remove
 *
 * Purpose:     Remove chunk from index.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_remove(const H5D_chk_idx_info_t *idx_info, H5D_chunk_common_ud_t *udata)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(H5_addr_defined(idx_info->storage->idx_addr));
    assert(udata);

    /* Remove the chunk from the v1 B-tree index and release the space for the
     * chunk (in the B-tree callback).
     */
    if (H5B_remove(idx_info->f, H5B_BTREE, idx_info->storage->idx_addr, udata) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL, "unable to remove chunk entry");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5D__btree_idx_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_delete
 *
 * Purpose:     Delete index and raw data storage for entire dataset
 *              (i.e. all chunks)
 *
 * Return:      Success:    Non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_delete(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    /* Check if the index data structure has been allocated */
    if (H5_addr_defined(idx_info->storage->idx_addr)) {
        H5O_storage_chunk_t   tmp_storage; /* Local copy of storage info */
        H5D_chunk_common_ud_t udata;       /* User data for B-tree operations */

        /* Set up temporary chunked storage info */
        tmp_storage = *idx_info->storage;

        /* Set up the shared structure */
        if (H5D__btree_shared_create(idx_info->f, &tmp_storage, idx_info->layout) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTINIT, FAIL, "can't create wrapper for shared B-tree info");

        /* Set up B-tree user data */
        memset(&udata, 0, sizeof udata);
        udata.layout  = idx_info->layout;
        udata.storage = &tmp_storage;

        /* Delete entire B-tree */
        if (H5B_delete(idx_info->f, H5B_BTREE, tmp_storage.idx_addr, &udata) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTDELETE, FAIL, "unable to delete chunk B-tree");

        /* Release the shared B-tree page */
        if (NULL == tmp_storage.u.btree.shared)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "ref-counted page nil");
        if (H5UC_DEC(tmp_storage.u.btree.shared) < 0)
            HGOTO_ERROR(H5E_DATASET, H5E_CANTFREE, FAIL, "unable to decrement ref-counted page");
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_copy_setup
 *
 * Purpose:     Set up any necessary information for copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_copy_setup(const H5D_chk_idx_info_t *idx_info_src, const H5D_chk_idx_info_t *idx_info_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE_TAG(H5AC__COPIED_TAG)

    assert(idx_info_src);
    assert(idx_info_src->f);
    assert(idx_info_src->pline);
    assert(idx_info_src->layout);
    assert(idx_info_src->storage);
    assert(idx_info_dst);
    assert(idx_info_dst->f);
    assert(idx_info_dst->pline);
    assert(idx_info_dst->layout);
    assert(idx_info_dst->storage);
    assert(!H5_addr_defined(idx_info_dst->storage->idx_addr));

    /* Create shared B-tree info for each file */
    if (H5D__btree_shared_create(idx_info_src->f, idx_info_src->storage, idx_info_src->layout) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "can't create wrapper for source shared B-tree info");
    if (H5D__btree_shared_create(idx_info_dst->f, idx_info_dst->storage, idx_info_dst->layout) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL,
                    "can't create wrapper for destination shared B-tree info");

    /* Create the root of the B-tree that describes chunked storage in the dest. file */
    if (H5D__btree_idx_create(idx_info_dst) < 0)
        HGOTO_ERROR(H5E_IO, H5E_CANTINIT, FAIL, "unable to initialize chunked storage");
    assert(H5_addr_defined(idx_info_dst->storage->idx_addr));

done:
    FUNC_LEAVE_NOAPI_TAG(ret_value)
} /* end H5D__btree_idx_copy_setup() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_copy_shutdown
 *
 * Purpose:     Shutdown any information from copying chunks
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_copy_shutdown(H5O_storage_chunk_t *storage_src, H5O_storage_chunk_t *storage_dst)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(storage_src);
    assert(storage_dst);

    /* Decrement refcount on shared B-tree info */
    if (H5UC_DEC(storage_src->u.btree.shared) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to decrement ref-counted page");
    if (H5UC_DEC(storage_dst->u.btree.shared) < 0)
        HGOTO_ERROR(H5E_DATASET, H5E_CANTDEC, FAIL, "unable to decrement ref-counted page");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_copy_shutdown() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_size
 *
 * Purpose:     Retrieve the amount of index storage for chunked dataset
 *
 * Return:      Success:        Non-negative
 *              Failure:        negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_size(const H5D_chk_idx_info_t *idx_info, hsize_t *index_size)
{
    H5D_chunk_common_ud_t udata;               /* User-data for loading B-tree nodes */
    H5B_info_t            bt_info;             /* B-tree info */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Check args */
    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);
    assert(index_size);

    /* Initialize B-tree node user-data */
    memset(&udata, 0, sizeof udata);
    udata.layout  = idx_info->layout;
    udata.storage = idx_info->storage;

    /* Get metadata information for B-tree */
    if (H5B_get_info(idx_info->f, H5B_BTREE, idx_info->storage->idx_addr, &bt_info, NULL, &udata) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to iterate over chunk B-tree");

    /* Set the size of the B-tree */
    *index_size = bt_info.size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_size() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_reset
 *
 * Purpose:     Reset indexing information.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_reset(H5O_storage_chunk_t *storage, bool reset_addr)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(storage);

    /* Reset index info */
    if (reset_addr)
        storage->idx_addr = HADDR_UNDEF;
    storage->u.btree.shared = NULL;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_dump
 *
 * Purpose:     Dump indexing information to a stream.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_dump(const H5O_storage_chunk_t *storage, FILE *stream)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(storage);
    assert(stream);

    fprintf(stream, "    Address: %" PRIuHADDR "\n", storage->idx_addr);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5D__btree_idx_dump() */

/*-------------------------------------------------------------------------
 * Function:    H5D__btree_idx_dest
 *
 * Purpose:     Release indexing information in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5D__btree_idx_dest(const H5D_chk_idx_info_t *idx_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(idx_info);
    assert(idx_info->f);
    assert(idx_info->pline);
    assert(idx_info->layout);
    assert(idx_info->storage);

    /* Free the raw B-tree node buffer */
    if (NULL == idx_info->storage->u.btree.shared)
        HGOTO_ERROR(H5E_IO, H5E_CANTFREE, FAIL, "ref-counted page nil");
    if (H5UC_DEC(idx_info->storage->u.btree.shared) < 0)
        HGOTO_ERROR(H5E_IO, H5E_CANTFREE, FAIL, "unable to decrement ref-counted page");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D__btree_idx_dest() */

/*-------------------------------------------------------------------------
 * Function:    H5D_btree_debug
 *
 * Purpose:     Debugs a B-tree node for indexed raw data storage.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5D_btree_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, unsigned ndims,
                const uint32_t *dim)
{
    H5D_btree_dbg_t     udata;               /* User data for B-tree callback */
    H5O_storage_chunk_t storage;             /* Storage information for B-tree callback */
    H5O_layout_chunk_t  layout;              /* Layout information for B-tree callback */
    bool                shared_init = false; /* Whether B-tree shared info is initialized */
    unsigned            u;                   /* Local index variable */
    herr_t              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Reset "fake" storage info */
    memset(&storage, 0, sizeof(storage));
    storage.idx_type = H5D_CHUNK_IDX_BTREE;

    /* Reset "fake" layout info */
    memset(&layout, 0, sizeof(layout));
    layout.ndims = ndims;
    for (u = 0; u < ndims; u++)
        layout.dim[u] = dim[u];

    /* Allocate the shared structure */
    if (H5D__btree_shared_create(f, &storage, &layout) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "can't create wrapper for shared B-tree info");
    shared_init = true;

    /* Set up user data for callback */
    udata.common.layout  = &layout;
    udata.common.storage = &storage;
    udata.common.scaled  = NULL;
    udata.ndims          = ndims;

    /* Dump the records for the B-tree */
    (void)H5B_debug(f, addr, stream, indent, fwidth, H5B_BTREE, &udata);

done:
    if (shared_init) {
        /* Free the raw B-tree node buffer */
        if (NULL == storage.u.btree.shared)
            HDONE_ERROR(H5E_IO, H5E_CANTFREE, FAIL, "ref-counted shared info nil");
        else if (H5UC_DEC(storage.u.btree.shared) < 0)
            HDONE_ERROR(H5E_IO, H5E_CANTFREE, FAIL, "unable to decrement ref-counted shared info");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5D_btree_debug() */
