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
 * Created:	    H5Gnode.c
 *
 * Purpose:     Functions for handling symbol table nodes.  A
 *              symbol table node is a small collection of symbol
 *              table entries.	A B-tree usually points to the
 *              symbol table nodes for any given symbol table.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions        */
#include "H5ACprivate.h" /* Metadata cache           */
#include "H5Eprivate.h"  /* Error handling           */
#include "H5Fprivate.h"  /* File access              */
#include "H5FLprivate.h" /* Free Lists               */
#include "H5Gpkg.h"      /* Groups                   */
#include "H5HLprivate.h" /* Local Heaps              */
#include "H5MFprivate.h" /* File memory management   */
#include "H5MMprivate.h" /* Memory management        */
#include "H5Ppublic.h"   /* Property Lists           */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/*
 * Each key field of the B-link tree that points to symbol table
 * nodes consists of this structure...
 */
typedef struct H5G_node_key_t {
    size_t offset; /*offset into heap for name          */
} H5G_node_key_t;

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* B-tree callbacks */
static H5UC_t   *H5G__node_get_shared(const H5F_t *f, const void *_udata);
static herr_t    H5G__node_create(H5F_t *f, H5B_ins_t op, void *_lt_key, void *_udata, void *_rt_key,
                                  haddr_t *addr_p /*out*/);
static int       H5G__node_cmp2(void *_lt_key, void *_udata, void *_rt_key);
static int       H5G__node_cmp3(void *_lt_key, void *_udata, void *_rt_key);
static herr_t    H5G__node_found(H5F_t *f, haddr_t addr, const void *_lt_key, bool *found, void *_udata);
static H5B_ins_t H5G__node_insert(H5F_t *f, haddr_t addr, void *_lt_key, bool *lt_key_changed, void *_md_key,
                                  void *_udata, void *_rt_key, bool *rt_key_changed,
                                  haddr_t *new_node_p /*out*/);
static H5B_ins_t H5G__node_remove(H5F_t *f, haddr_t addr, void *lt_key, bool *lt_key_changed, void *udata,
                                  void *rt_key, bool *rt_key_changed);
static herr_t    H5G__node_decode_key(const H5B_shared_t *shared, const uint8_t *raw, void *_key);
static herr_t    H5G__node_encode_key(const H5B_shared_t *shared, uint8_t *raw, const void *_key);
static herr_t H5G__node_debug_key(FILE *stream, int indent, int fwidth, const void *key, const void *udata);

/*********************/
/* Package Variables */
/*********************/

/* H5G inherits B-tree like properties from H5B */
H5B_class_t H5B_SNODE[1] = {{
    H5B_SNODE_ID,           /*id            */
    sizeof(H5G_node_key_t), /*sizeof_nkey   */
    H5G__node_get_shared,   /*get_shared    */
    H5G__node_create,       /*new           */
    H5G__node_cmp2,         /*cmp2          */
    H5G__node_cmp3,         /*cmp3          */
    H5G__node_found,        /*found         */
    H5G__node_insert,       /*insert        */
    true,                   /*follow min branch?    */
    true,                   /*follow max branch?    */
    H5B_RIGHT,              /*critical key  */
    H5G__node_remove,       /*remove        */
    H5G__node_decode_key,   /*decode        */
    H5G__node_encode_key,   /*encode        */
    H5G__node_debug_key     /*debug         */
}};

/* Declare a free list to manage the H5G_node_t struct */
H5FL_DEFINE(H5G_node_t);

/* Declare a free list to manage sequences of H5G_entry_t's */
H5FL_SEQ_DEFINE(H5G_entry_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:    H5G__node_get_shared
 *
 * Purpose:     Returns the shared B-tree info for the specified UDATA.
 *
 * Return:      Success:    Pointer to the raw B-tree page for this file's groups
 *
 *              Failure:	Can't fail
 *
 *-------------------------------------------------------------------------
 */
static H5UC_t *
H5G__node_get_shared(const H5F_t *f, const void H5_ATTR_UNUSED *_udata)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(f);

    /* Return the pointer to the ref-count object */
    FUNC_LEAVE_NOAPI(H5F_GRP_BTREE_SHARED(f))
} /* end H5G__node_get_shared() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_decode_key
 *
 * Purpose:     Decodes a raw key into a native key.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_decode_key(const H5B_shared_t *shared, const uint8_t *raw, void *_key)
{
    H5G_node_key_t *key = (H5G_node_key_t *)_key;

    FUNC_ENTER_PACKAGE_NOERR

    assert(shared);
    assert(raw);
    assert(key);

    H5_DECODE_LENGTH_LEN(raw, key->offset, shared->sizeof_len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__node_decode_key() */

/*-------------------------------------------------------------------------
 * Function:	H5G__node_encode_key
 *
 * Purpose:     Encodes a native key into a raw key.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_encode_key(const H5B_shared_t *shared, uint8_t *raw, const void *_key)
{
    const H5G_node_key_t *key = (const H5G_node_key_t *)_key;

    FUNC_ENTER_PACKAGE_NOERR

    assert(shared);
    assert(raw);
    assert(key);

    H5_ENCODE_LENGTH_LEN(raw, key->offset, shared->sizeof_len);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__node_encode_key() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_debug_key
 *
 * Purpose:     Prints a key.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_debug_key(FILE *stream, int indent, int fwidth, const void *_key, const void *_udata)
{
    const H5G_node_key_t  *key   = (const H5G_node_key_t *)_key;
    const H5G_bt_common_t *udata = (const H5G_bt_common_t *)_udata;

    FUNC_ENTER_PACKAGE_NOERR

    assert(key);

    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Heap offset:", (unsigned)key->offset);

    if (udata->heap) {
        const char *s;

        fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Name:");

        if ((s = (const char *)H5HL_offset_into(udata->heap, key->offset)) != NULL)
            fprintf(stream, "%s\n", s);
    } /* end if */
    else
        fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Cannot get name; heap address not specified\n");

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__node_debug_key() */

/*-------------------------------------------------------------------------
 * Function:	H5G__node_free
 *
 * Purpose:     Destroy a symbol table node in memory.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__node_free(H5G_node_t *sym)
{
    FUNC_ENTER_PACKAGE_NOERR

    /*
     * Check arguments.
     */
    assert(sym);

    /* Verify that node is clean */
    assert(sym->cache_info.is_dirty == false);

    if (sym->entry)
        sym->entry = H5FL_SEQ_FREE(H5G_entry_t, sym->entry);
    sym = H5FL_FREE(H5G_node_t, sym);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__node_free() */

/*-------------------------------------------------------------------------
 * Function:	H5G__node_create
 *
 * Purpose:	Creates a new empty symbol table node.	This function is
 *          called by the B-tree insert function for an empty tree.	 It
 *          is also called internally to split a symbol node with LT_KEY
 *          and RT_KEY null pointers.
 *
 * Return:  Success:    Non-negative.   The address of symbol table
 *                      node is returned through the ADDR_P argument.
 *
 *          Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_create(H5F_t *f, H5B_ins_t H5_ATTR_UNUSED op, void *_lt_key, void H5_ATTR_UNUSED *_udata,
                 void *_rt_key, haddr_t *addr_p /*out*/)
{
    H5G_node_key_t *lt_key    = (H5G_node_key_t *)_lt_key;
    H5G_node_key_t *rt_key    = (H5G_node_key_t *)_rt_key;
    H5G_node_t     *sym       = NULL;
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5B_INS_FIRST == op);

    if (NULL == (sym = H5FL_CALLOC(H5G_node_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "memory allocation failed");
    sym->node_size = H5G_NODE_SIZE(f);
    if (HADDR_UNDEF == (*addr_p = H5MF_alloc(f, H5FD_MEM_BTREE, (hsize_t)sym->node_size)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to allocate file space");
    if (NULL == (sym->entry = H5FL_SEQ_CALLOC(H5G_entry_t, (size_t)(2 * H5F_SYM_LEAF_K(f)))))
        HGOTO_ERROR(H5E_SYM, H5E_CANTALLOC, FAIL, "memory allocation failed");

    if (H5AC_insert_entry(f, H5AC_SNODE, *addr_p, sym, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to cache symbol table leaf node");
    /*
     * The left and right symbols in an empty tree are both the
     * empty string stored at offset zero by the H5G functions. This
     * allows the comparison functions to work correctly without knowing
     * that there are no symbols.
     */
    if (lt_key)
        lt_key->offset = 0;
    if (rt_key)
        rt_key->offset = 0;

done:
    if (ret_value < 0) {
        if (sym != NULL) {
            if (sym->entry != NULL)
                sym->entry = H5FL_SEQ_FREE(H5G_entry_t, sym->entry);
            sym = H5FL_FREE(H5G_node_t, sym);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_create() */

/*-------------------------------------------------------------------------
 * Function:	H5G__node_cmp2
 *
 * Purpose:	Compares two keys from a B-tree node (LT_KEY and RT_KEY).
 *          The UDATA pointer supplies extra data not contained in the
 *          keys (in this case, the heap address).
 *
 * Return:  Success:    negative if LT_KEY is less than RT_KEY.
 *
 *                      positive if LT_KEY is greater than RT_KEY.
 *
 *                      zero if LT_KEY and RT_KEY are equal.
 *
 *          Failure:    FAIL (same as LT_KEY<RT_KEY)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_cmp2(void *_lt_key, void *_udata, void *_rt_key)
{
    H5G_bt_common_t *udata  = (H5G_bt_common_t *)_udata;
    H5G_node_key_t  *lt_key = (H5G_node_key_t *)_lt_key;
    H5G_node_key_t  *rt_key = (H5G_node_key_t *)_rt_key;
    const char      *s1, *s2;
    int              ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(udata && udata->heap);
    assert(lt_key);
    assert(rt_key);

    /* Get pointers to string names */
    if ((s1 = (const char *)H5HL_offset_into(udata->heap, lt_key->offset)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get key name");
    if ((s2 = (const char *)H5HL_offset_into(udata->heap, rt_key->offset)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get key name");

    /* Set return value */
    ret_value = strcmp(s1, s2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__node_cmp2() */

/*-------------------------------------------------------------------------
 * Function:	H5G__node_cmp3
 *
 * Purpose:	Compares two keys from a B-tree node (LT_KEY and RT_KEY)
 *          against another key (not necessarily the same type)
 *          pointed to by UDATA.
 *
 * Return:  Success:    negative if the UDATA key is less than
 *                      or equal to the LT_KEY
 *
 *                      positive if the UDATA key is greater
 *                      than the RT_KEY.
 *
 *                      zero if the UDATA key falls between
 *                      the LT_KEY (exclusive) and the
 *                      RT_KEY (inclusive).
 *
 *          Failure:    FAIL (same as UDATA < LT_KEY)
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_cmp3(void *_lt_key, void *_udata, void *_rt_key)
{
    H5G_bt_common_t *udata  = (H5G_bt_common_t *)_udata;
    H5G_node_key_t  *lt_key = (H5G_node_key_t *)_lt_key;
    H5G_node_key_t  *rt_key = (H5G_node_key_t *)_rt_key;
    const char      *s;
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(udata && udata->heap);
    assert(lt_key);
    assert(rt_key);

    /* left side */
    if ((s = (const char *)H5HL_offset_into(udata->heap, lt_key->offset)) == NULL)
        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get key name");
    if (strcmp(udata->name, s) <= 0)
        ret_value = (-1);
    else {
        /* right side */
        if ((s = (const char *)H5HL_offset_into(udata->heap, rt_key->offset)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get key name");
        if (strcmp(udata->name, s) > 0)
            ret_value = 1;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_cmp3() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_found
 *
 * Purpose:     The B-tree search engine has found the symbol table node
 *              which contains the requested symbol if the symbol exists.
 *              This function should examine that node for the symbol and
 *              return information about the symbol through the UDATA
 *              structure which contains the symbol name on function
 *              entry.
 *
 *              If the operation flag in UDATA is H5G_OPER_FIND, then
 *              the entry is copied from the symbol table to the UDATA
 *              entry field.  Otherwise the entry is copied from the
 *              UDATA entry field to the symbol table.
 *
 * Return:      Success:    Non-negative (true/false) if found and data
 *                          returned through the UDATA pointer, if *FOUND is true.
 *              Failure:    Negative if not found.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5G__node_found(H5F_t *f, haddr_t addr, const void H5_ATTR_UNUSED *_lt_key, bool *found, void *_udata)
{
    H5G_bt_lkp_t *udata = (H5G_bt_lkp_t *)_udata;
    H5G_node_t   *sn    = NULL;
    unsigned      lt = 0, idx = 0, rt;
    int           cmp = 1;
    const char   *s;
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(found);
    assert(udata && udata->common.heap);

    /*
     * Load the symbol table node for exclusive access.
     */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, FAIL, "unable to protect symbol table node");

    /*
     * Binary search.
     */
    rt = sn->nsyms;
    while (lt < rt && cmp) {
        idx = (lt + rt) / 2;

        if ((s = (const char *)H5HL_offset_into(udata->common.heap, sn->entry[idx].name_off)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to get symbol table name");
        cmp = strcmp(udata->common.name, s);

        if (cmp < 0)
            rt = idx;
        else
            lt = idx + 1;
    } /* end while */

    if (cmp)
        *found = false;
    else {
        /* Set the 'found it' flag */
        *found = true;

        /* Call user's callback operator */
        if ((udata->op)(&sn->entry[idx], udata->op_data) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_BADITER, FAIL, "iterator callback failed");
    } /* end else */

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to release symbol table node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_found() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_insert
 *
 * Purpose:     The B-tree insertion engine has found the symbol table node
 *              which should receive the new symbol/address pair.  This
 *              function adds it to that node unless it already existed.
 *
 *              If the node has no room for the symbol then the node is
 *              split into two nodes.  The original node contains the
 *              low values and the new node contains the high values.
 *              The new symbol table entry is added to either node as
 *              appropriate.  When a split occurs, this function will
 *              write the maximum key of the low node to the MID buffer
 *              and return the address of the new node.
 *
 *              If the new key is larger than RIGHT then update RIGHT
 *              with the new key.
 *
 * Return:      Success:    An insertion command for the caller, one of
 *                          the H5B_INS_* constants.  The address of the
 *                          new node, if any, is returned through the
 *                          NEW_NODE_P argument.  NEW_NODE_P might not be
 *                          initialized if the return value is H5B_INS_NOOP.
 *
 *              Failure:    H5B_INS_ERROR, NEW_NODE_P might not be initialized.
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5G__node_insert(H5F_t *f, haddr_t addr, void H5_ATTR_UNUSED *_lt_key, bool H5_ATTR_UNUSED *lt_key_changed,
                 void *_md_key, void *_udata, void *_rt_key, bool *rt_key_changed, haddr_t *new_node_p)
{
    H5G_node_key_t *md_key = (H5G_node_key_t *)_md_key;
    H5G_node_key_t *rt_key = (H5G_node_key_t *)_rt_key;
    H5G_bt_ins_t   *udata  = (H5G_bt_ins_t *)_udata;
    H5G_node_t     *sn = NULL, *snrt = NULL;
    unsigned        sn_flags = H5AC__NO_FLAGS_SET, snrt_flags = H5AC__NO_FLAGS_SET;
    const char     *s;
    unsigned        lt  = 0, rt; /* Binary search cntrs	*/
    int             cmp = 1, idx = -1;
    H5G_node_t     *insert_into = NULL; /*node that gets new entry*/
    H5G_entry_t     ent;                /* Entry to insert in node */
    H5B_ins_t       ret_value = H5B_INS_ERROR;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(md_key);
    assert(rt_key);
    assert(udata && udata->common.heap);
    assert(new_node_p);

    /*
     * Load the symbol node.
     */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5B_INS_ERROR, "unable to protect symbol table node");

    /*
     * Where does the new symbol get inserted?	We use a binary search.
     */
    rt = sn->nsyms;
    while (lt < rt) {
        idx = (int)((lt + rt) / 2);
        if ((s = (const char *)H5HL_offset_into(udata->common.heap, sn->entry[idx].name_off)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "unable to get symbol table name");

        /* Check if symbol is already present */
        if (0 == (cmp = strcmp(udata->common.name, s)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, H5B_INS_ERROR, "symbol is already present in symbol table");

        if (cmp < 0)
            rt = (unsigned)idx;
        else
            lt = (unsigned)(idx + 1);
    } /* end while */
    idx += cmp > 0 ? 1 : 0;

    /* Convert link information & name to symbol table entry */
    if (H5G__ent_convert(f, udata->common.heap, udata->common.name, udata->lnk, udata->obj_type,
                         udata->crt_info, &ent) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTCONVERT, H5B_INS_ERROR, "unable to convert link");

    /* Determine where to place entry in node */
    if (sn->nsyms >= 2 * H5F_SYM_LEAF_K(f)) {
        /*
         * The node is full.  Split it into a left and right
         * node and return the address of the new right node (the
         * left node is at the same address as the original node).
         */
        ret_value = H5B_INS_RIGHT;

        /* The right node */
        if (H5G__node_create(f, H5B_INS_FIRST, NULL, NULL, NULL, new_node_p /*out*/) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5B_INS_ERROR, "unable to split symbol table node");

        if (NULL == (snrt = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, *new_node_p, f, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5B_INS_ERROR, "unable to split symbol table node");

        H5MM_memcpy(snrt->entry, sn->entry + H5F_SYM_LEAF_K(f), H5F_SYM_LEAF_K(f) * sizeof(H5G_entry_t));
        snrt->nsyms = H5F_SYM_LEAF_K(f);
        snrt_flags |= H5AC__DIRTIED_FLAG;

        /* The left node */
        memset(sn->entry + H5F_SYM_LEAF_K(f), 0, H5F_SYM_LEAF_K(f) * sizeof(H5G_entry_t));
        sn->nsyms = H5F_SYM_LEAF_K(f);
        sn_flags |= H5AC__DIRTIED_FLAG;

        /* The middle key */
        md_key->offset = sn->entry[sn->nsyms - 1].name_off;

        /* Where to insert the new entry? */
        if (idx <= (int)H5F_SYM_LEAF_K(f)) {
            insert_into = sn;
            if (idx == (int)H5F_SYM_LEAF_K(f))
                md_key->offset = ent.name_off;
        } /* end if */
        else {
            idx -= (int)H5F_SYM_LEAF_K(f);
            insert_into = snrt;
            if (idx == (int)H5F_SYM_LEAF_K(f)) {
                rt_key->offset  = ent.name_off;
                *rt_key_changed = true;
            } /* end if */
        }     /* end else */
    }         /* end if */
    else {
        /* Where to insert the new entry? */
        ret_value = H5B_INS_NOOP;
        sn_flags |= H5AC__DIRTIED_FLAG;
        insert_into = sn;
        if (idx == (int)sn->nsyms) {
            rt_key->offset  = ent.name_off;
            *rt_key_changed = true;
        } /* end if */
    }     /* end else */

    /* Move entries down to make room for new entry */
    assert(idx >= 0);
    memmove(insert_into->entry + idx + 1, insert_into->entry + idx,
            (insert_into->nsyms - (unsigned)idx) * sizeof(H5G_entry_t));

    /* Copy new entry into table */
    H5G__ent_copy(&(insert_into->entry[idx]), &ent, H5_COPY_SHALLOW);

    /* Increment # of symbols in table */
    insert_into->nsyms += 1;

done:
    if (snrt && H5AC_unprotect(f, H5AC_SNODE, *new_node_p, snrt, snrt_flags) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5B_INS_ERROR, "unable to release symbol table node");
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, sn_flags) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5B_INS_ERROR, "unable to release symbol table node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_insert() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_remove
 *
 * Purpose: The B-tree removal engine has found the symbol table node
 *          which should contain the name which is being removed.  This
 *          function removes the name from the symbol table and
 *          decrements the link count on the object to which the name
 *          points.
 *
 *          If the udata->name parameter is set to NULL, then remove
 *          all entries in this symbol table node.  This only occurs
 *          during the deletion of the entire group, so don't bother
 *          freeing individual name entries in the local heap, the group's
 *          symbol table removal code will just free the entire local
 *          heap eventually.  Do reduce the link counts for each object
 *          however.
 *
 * Return:  Success:    If all names are removed from the symbol
 *                      table node then H5B_INS_REMOVE is returned;
 *                      otherwise H5B_INS_NOOP is returned.
 *
 *          Failure:    H5B_INS_ERROR
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5G__node_remove(H5F_t *f, haddr_t addr, void H5_ATTR_NDEBUG_UNUSED *_lt_key /*in,out*/,
                 bool H5_ATTR_UNUSED *lt_key_changed /*out*/, void *_udata /*in,out*/,
                 void *_rt_key /*in,out*/, bool *rt_key_changed /*out*/)
{
    H5G_node_key_t *rt_key   = (H5G_node_key_t *)_rt_key;
    H5G_bt_rm_t    *udata    = (H5G_bt_rm_t *)_udata;
    H5G_node_t     *sn       = NULL;
    unsigned        sn_flags = H5AC__NO_FLAGS_SET;
    unsigned        lt = 0, rt, idx = 0;
    int             cmp       = 1;
    H5B_ins_t       ret_value = H5B_INS_ERROR;

    FUNC_ENTER_PACKAGE

    /* Check arguments */
    assert(f);
    assert(H5_addr_defined(addr));
    assert((H5G_node_key_t *)_lt_key);
    assert(rt_key);
    assert(udata && udata->common.heap);

    /* Load the symbol table */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5B_INS_ERROR, "unable to protect symbol table node");

    /* "Normal" removal of a single entry from the symbol table node */
    if (udata->common.name != NULL) {
        H5O_link_t lnk;           /* Constructed link for replacement */
        size_t     link_name_len; /* Length of string in local heap */

        /* Find the name with a binary search */
        rt = sn->nsyms;
        while (lt < rt && cmp) {
            const char *s; /* Pointer to string in local heap */

            idx = (lt + rt) / 2;
            if ((s = (const char *)H5HL_offset_into(udata->common.heap, sn->entry[idx].name_off)) == NULL)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "unable to get symbol table name");
            cmp = strcmp(udata->common.name, s);
            if (cmp < 0)
                rt = idx;
            else
                lt = idx + 1;
        } /* end while */

        if (cmp)
            HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, H5B_INS_ERROR, "name not found");

        /* Get a pointer to the name of the link */
        if (NULL == (lnk.name = (char *)H5HL_offset_into(udata->common.heap, sn->entry[idx].name_off)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "unable to get link name");
        link_name_len = strlen(lnk.name) + 1;

        /* Set up rest of link structure */
        lnk.corder_valid = false;
        lnk.corder       = 0;
        lnk.cset         = H5T_CSET_ASCII;
        if (sn->entry[idx].type == H5G_CACHED_SLINK) {
            lnk.type = H5L_TYPE_SOFT;
            if (NULL == (lnk.u.soft.name = (char *)H5HL_offset_into(udata->common.heap,
                                                                    sn->entry[idx].cache.slink.lval_offset)))
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "unable to get link name");
        } /* end if */
        else {
            lnk.type = H5L_TYPE_HARD;
            assert(H5_addr_defined(sn->entry[idx].header));
            lnk.u.hard.addr = sn->entry[idx].header;
        } /* end else */

        /* Replace any object names */
        if (H5G__link_name_replace(f, udata->grp_full_path_r, &lnk) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "unable to get object type");

        /* Decrement the ref. count for hard links */
        if (lnk.type == H5L_TYPE_HARD) {
            H5O_loc_t tmp_oloc; /* Temporary object location */

            /* Build temporary object location */
            tmp_oloc.file = f;
            tmp_oloc.addr = lnk.u.hard.addr;

            if (H5O_link(&tmp_oloc, -1) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, H5B_INS_ERROR, "unable to decrement object link count");
        } /* end if */
        else {
            /* Remove the soft link's value from the local heap */
            if (lnk.u.soft.name) {
                size_t soft_link_len; /* Length of string in local heap */

                soft_link_len = strlen(lnk.u.soft.name) + 1;
                if (H5HL_remove(f, udata->common.heap, sn->entry[idx].cache.slink.lval_offset,
                                soft_link_len) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, H5B_INS_ERROR,
                                "unable to remove soft link from local heap");
            } /* end if */
        }     /* end else */

        /* Remove the link's name from the local heap */
        if (H5HL_remove(f, udata->common.heap, sn->entry[idx].name_off, link_name_len) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, H5B_INS_ERROR, "unable to remove link name from local heap");

        /* Remove the entry from the symbol table node */
        if (1 == sn->nsyms) {
            /*
             * We are about to remove the only symbol in this node.  Free this
             * node and indicate that the pointer to this node in the B-tree
             * should be removed also.
             */
            assert(0 == idx);
            sn->nsyms = 0;
            sn_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;
            ret_value = H5B_INS_REMOVE;
        }
        else if (0 == idx) {
            /*
             * We are about to remove the left-most entry from the symbol table
             * node but there are other entries to the right.  No key values
             * change.
             */
            sn->nsyms -= 1;
            sn_flags |= H5AC__DIRTIED_FLAG;
            memmove(sn->entry + idx, sn->entry + idx + 1, (sn->nsyms - idx) * sizeof(H5G_entry_t));
            ret_value = H5B_INS_NOOP;
        }
        else if (idx + 1 == sn->nsyms) {
            /*
             * We are about to remove the right-most entry from the symbol table
             * node but there are other entries to the left.  The right key
             * should be changed to reflect the new right-most entry.
             */
            sn->nsyms -= 1;
            sn_flags |= H5AC__DIRTIED_FLAG;
            rt_key->offset  = sn->entry[sn->nsyms - 1].name_off;
            *rt_key_changed = true;
            ret_value       = H5B_INS_NOOP;
        }
        else {
            /*
             * We are about to remove an entry from the middle of a symbol table
             * node.
             */
            sn->nsyms -= 1;
            sn_flags |= H5AC__DIRTIED_FLAG;
            memmove(sn->entry + idx, sn->entry + idx + 1, (sn->nsyms - idx) * sizeof(H5G_entry_t));
            ret_value = H5B_INS_NOOP;
        } /* end else */
    }     /* end if */
    /* Remove all entries from node, during B-tree deletion */
    else {
        H5O_loc_t tmp_oloc; /* Temporary object location */

        /* Build temporary object location */
        tmp_oloc.file = f;

        /* Reduce the link count for all entries in this node */
        for (idx = 0; idx < sn->nsyms; idx++) {
            if (!(H5G_CACHED_SLINK == sn->entry[idx].type)) {
                /* Decrement the reference count */
                assert(H5_addr_defined(sn->entry[idx].header));
                tmp_oloc.addr = sn->entry[idx].header;

                if (H5O_link(&tmp_oloc, -1) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTDELETE, H5B_INS_ERROR,
                                "unable to decrement object link count");
            } /* end if */
        }     /* end for */

        /*
         * We are about to remove all the symbols in this node.  Free this
         * node and indicate that the pointer to this node in the B-tree
         * should be removed also.
         */
        sn->nsyms = 0;
        sn_flags |= H5AC__DIRTIED_FLAG | H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;
        ret_value = H5B_INS_REMOVE;
    } /* end else */

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, sn_flags) < 0)
        HDONE_ERROR(H5E_SYM, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to release symbol table node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_remove() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_iterate
 *
 * Purpose:     This function gets called during a group iterate operation.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5G__node_iterate(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr,
                  const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    H5G_bt_it_it_t *udata = (H5G_bt_it_it_t *)_udata;
    H5G_node_t     *sn    = NULL;
    H5G_entry_t    *ents; /* Pointer to entries in this node */
    unsigned        u;    /* Local index variable */
    int             ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(udata && udata->heap);

    /* Protect the symbol table node & local heap while we iterate over entries */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    /*
     * Iterate over the symbol table node entries.
     */
    for (u = 0, ents = sn->entry; u < sn->nsyms && ret_value == H5_ITER_CONT; u++) {
        if (udata->skip > 0)
            --udata->skip;
        else {
            H5O_link_t  lnk;  /* Link for entry */
            const char *name; /* Pointer to link name in heap */

            /* Get the pointer to the name of the link in the heap */
            if ((name = (const char *)H5HL_offset_into(udata->heap, ents[u].name_off)) == NULL)
                HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5_ITER_ERROR, "unable to get symbol table node name");

            /* Convert the entry to a link */
            if (H5G__ent_to_link(&lnk, udata->heap, &ents[u], name) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTCONVERT, H5_ITER_ERROR,
                            "unable to convert symbol table entry to link");

            /* Make the callback */
            ret_value = (udata->op)(&lnk, udata->op_data);

            /* Release memory for link object */
            if (H5O_msg_reset(H5O_LINK_ID, &lnk) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTFREE, H5_ITER_ERROR, "unable to release link message");
        } /* end else */

        /* Increment the number of entries passed through */
        /* (whether we skipped them or not) */
        if (udata->final_ent)
            (*udata->final_ent)++;
    } /* end for */
    if (ret_value < 0)
        HERROR(H5E_SYM, H5E_CANTNEXT, "iteration operator failed");

done:
    /* Release resources */
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_iterate() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_sumup
 *
 * Purpose:     This function gets called during a group iterate operation
 *              to return total number of members in the group.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5G__node_sumup(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr,
                const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    hsize_t    *num_objs  = (hsize_t *)_udata;
    H5G_node_t *sn        = NULL;
    int         ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(num_objs);

    /* Find the object node and add the number of symbol entries. */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    *num_objs += sn->nsyms;

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_sumup() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_by_idx
 *
 * Purpose:     This function gets called during a group iterate operation
 *              to return object name by giving idx.
 *
 * Return:      0 if object isn't found in this node; 1 if object is found;
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5G__node_by_idx(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr,
                 const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    H5G_bt_it_idx_common_t *udata     = (H5G_bt_it_idx_common_t *)_udata;
    H5G_node_t             *sn        = NULL;
    int                     ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(udata);

    /* Get a pointer to the symbol table node */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    /* Find the node, locate the object symbol table entry and retrieve the name */
    if (udata->idx >= udata->num_objs && udata->idx < (udata->num_objs + sn->nsyms)) {
        hsize_t ent_idx; /* Entry index in this node */

        /* Compute index of entry */
        ent_idx = udata->idx - udata->num_objs;

        /* Call 'by index' callback */
        assert(udata->op);
        if ((udata->op)(&sn->entry[ent_idx], udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5B_INS_ERROR, "'by index' callback failed");

        /* Indicate that we found the entry we are interested in */
        ret_value = H5_ITER_STOP;
    } /* end if */
    else
        udata->num_objs += sn->nsyms;

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_by_idx() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_init
 *
 * Purpose:     This function gets called during a file opening to initialize
 *              global information about group B-tree nodes for file.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__node_init(H5F_t *f)
{
    H5B_shared_t *shared;              /* Shared B-tree node info  */
    size_t        sizeof_rkey;         /* Size of raw (disk) key   */
    herr_t        ret_value = SUCCEED; /* Return value             */

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(f);

    /* Set the raw key size */
    sizeof_rkey = H5F_SIZEOF_SIZE(f); /*name offset */

    /* Allocate & initialize global info for the shared structure */
    if (NULL == (shared = H5B_shared_new(f, H5B_SNODE, sizeof_rkey)))
        HGOTO_ERROR(H5E_BTREE, H5E_NOSPACE, FAIL, "memory allocation failed for shared B-tree info");

    /* Set up the "local" information for this file's groups */
    /* <none> */

    /* Make shared B-tree info reference counted */
    if (H5F_SET_GRP_BTREE_SHARED(f, H5UC_create(shared, H5B_shared_free)) < 0)
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "can't create ref-count wrapper for shared B-tree info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_init() */

/*-------------------------------------------------------------------------
 * Function:    H5G_node_close
 *
 * Purpose:     This function gets called during a file close to shutdown
 *              global information about group B-tree nodes for file.
 *
 * Return:      Non-negative on success
 *              Negative on failure
 *
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_node_close(const H5F_t *f)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Check arguments. */
    assert(f);

    /* Free the raw B-tree node buffer */
    if (H5F_GRP_BTREE_SHARED(f))
        H5UC_DEC(H5F_GRP_BTREE_SHARED(f));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G_node_close */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_copy
 *
 * Purpose:     This function gets called during a group iterate operation
 *              to copy objects of this node into a new location.
 *
 * Return:      0(zero) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5G__node_copy(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr, const void H5_ATTR_UNUSED *_rt_key,
               void *_udata)
{
    H5G_bt_it_cpy_t *udata    = (H5G_bt_it_cpy_t *)_udata;
    const H5O_loc_t *src_oloc = udata->src_oloc;
    H5O_copy_t      *cpy_info = udata->cpy_info;
    H5HL_t          *heap     = NULL;
    H5G_node_t      *sn       = NULL;
    unsigned int     i; /* Local index variable */
    int              ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /* Check arguments. */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(udata);

    /* load the symbol table into memory from the source file */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    /* get the base address of the heap */
    if (NULL == (heap = H5HL_protect(f, udata->src_heap_addr, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, H5_ITER_ERROR, "unable to protect symbol name");

    /* copy object in this node one by one */
    for (i = 0; i < sn->nsyms; i++) {
        H5G_entry_t *src_ent =
            &(sn->entry[i]);             /* Convenience variable to refer to current source group entry */
        H5O_link_t          lnk;         /* Link to insert */
        const char         *name;        /* Name of source object */
        H5G_entry_t         tmp_src_ent; /* Temporary copy. Change will not affect the cache */
        H5O_type_t          obj_type = H5O_TYPE_UNKNOWN; /* Target object type */
        H5G_copy_file_ud_t *cpy_udata;                   /* Copy file udata */
        H5G_obj_create_t    gcrt_info;                   /* Group creation info */

        /* expand soft link */
        if (H5G_CACHED_SLINK == src_ent->type && cpy_info->expand_soft_link) {
            haddr_t    obj_addr;  /* Address of object pointed to by soft link */
            H5G_loc_t  grp_loc;   /* Group location holding soft link */
            H5G_name_t grp_path;  /* Path for group holding soft link */
            char      *link_name; /* Pointer to value of soft link */

            /* Make a temporary copy, so that it will not change the info in the cache */
            H5MM_memcpy(&tmp_src_ent, src_ent, sizeof(H5G_entry_t));

            /* Set up group location for soft link to start in */
            H5G_name_reset(&grp_path);
            grp_loc.path = &grp_path;
            H5_GCC_CLANG_DIAG_OFF("cast-qual")
            grp_loc.oloc = (H5O_loc_t *)src_oloc;
            H5_GCC_CLANG_DIAG_ON("cast-qual")

            /* Get pointer to link value in local heap */
            if ((link_name = (char *)H5HL_offset_into(heap, tmp_src_ent.cache.slink.lval_offset)) == NULL)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, H5_ITER_ERROR, "unable to get link name");

            /* Check if the object pointed by the soft link exists in the source file */
            if (H5G__loc_addr(&grp_loc, link_name, &obj_addr) >= 0) {
                tmp_src_ent.header = obj_addr;
                src_ent            = &tmp_src_ent;
            } /* end if */
            else
                H5E_clear_stack(NULL); /* discard any errors from a dangling soft link */
        }                              /* if ((H5G_CACHED_SLINK == src_ent->type)... */

        /* Check if object in source group is a hard link */
        if (H5_addr_defined(src_ent->header)) {
            H5O_loc_t new_dst_oloc; /* Copied object location in destination */
            H5O_loc_t tmp_src_oloc; /* Temporary object location for source object */

            /* Set up copied object location to fill in */
            H5O_loc_reset(&new_dst_oloc);
            new_dst_oloc.file = udata->dst_file;

            /* Build temporary object location for source */
            H5O_loc_reset(&tmp_src_oloc);
            tmp_src_oloc.file = f;
            tmp_src_oloc.addr = src_ent->header;

            /* Copy the shared object from source to destination */
            if (H5O_copy_header_map(&tmp_src_oloc, &new_dst_oloc, cpy_info, true, &obj_type,
                                    (void **)&cpy_udata) < 0)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTCOPY, H5_ITER_ERROR, "unable to copy object");

            /* Set up object creation info for symbol table insertion.  Only
             * case so far is for inserting old-style groups (for caching stab
             * info). */
            if (obj_type == H5O_TYPE_GROUP) {
                gcrt_info.gcpl_id    = H5P_DEFAULT;
                gcrt_info.cache_type = cpy_udata->cache_type;
                gcrt_info.cache      = cpy_udata->cache;
            } /* end if */

            /* Construct link information for eventual insertion */
            lnk.type        = H5L_TYPE_HARD;
            lnk.u.hard.addr = new_dst_oloc.addr;
        } /* ( H5_addr_defined(src_ent->header)) */
        else if (H5G_CACHED_SLINK == src_ent->type) {
            /* it is a soft link */
            /* Set object type to unknown */
            obj_type = H5O_TYPE_UNKNOWN;

            /* Construct link information for eventual insertion */
            lnk.type = H5L_TYPE_SOFT;
            if ((lnk.u.soft.name = (char *)H5HL_offset_into(heap, src_ent->cache.slink.lval_offset)) == NULL)
                HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, H5_ITER_ERROR, "unable to get link name");
        } /* else if */
        else
            assert(0 && "Unknown entry type");

        /* Set up common link data */
        lnk.cset         = H5F_DEFAULT_CSET; /* XXX: Allow user to set this */
        lnk.corder       = 0;                /* Creation order is not tracked for old-style links */
        lnk.corder_valid = false;            /* Creation order is not valid */
        /* lnk.name = name; */               /* This will be set in callback */

        /* Determine name of source object */
        if ((name = (const char *)H5HL_offset_into(heap, src_ent->name_off)) == NULL)
            HGOTO_ERROR(H5E_OHDR, H5E_CANTGET, H5_ITER_ERROR, "unable to get source object name");

        /* Set copied metadata tag */
        H5_BEGIN_TAG(H5AC__COPIED_TAG)

        /* Insert the new object in the destination file's group */
        /* (Don't increment the link count - that's already done above for hard links) */
        if (H5G__stab_insert_real(udata->dst_file, udata->dst_stab, name, &lnk, obj_type,
                                  (obj_type == H5O_TYPE_GROUP ? &gcrt_info : NULL)) < 0)
            HGOTO_ERROR_TAG(H5E_DATATYPE, H5E_CANTINIT, H5_ITER_ERROR, "unable to insert the name");

        /* Reset metadata tag */
        H5_END_TAG

    } /* end of for (i=0; i<sn->nsyms; i++) */

done:
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to unprotect symbol name");

    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_build_table
 *
 * Purpose:     B-link tree callback for building table of links
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
int
H5G__node_build_table(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t addr,
                      const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    H5G_bt_it_bt_t *udata = (H5G_bt_it_bt_t *)_udata;
    H5G_node_t     *sn    = NULL; /* Symbol table node */
    unsigned        u;            /* Local index variable */
    int             ret_value = H5_ITER_CONT;

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(udata && udata->heap);

    /*
     * Save information about the symbol table node since we can't lock it
     * because we're about to call an application function.
     */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, H5_ITER_ERROR, "unable to load symbol table node");

    /* Check if the link table needs to be extended */
    if ((udata->ltable->nlinks + sn->nsyms) >= udata->alloc_nlinks) {
        size_t      na = MAX((udata->ltable->nlinks + sn->nsyms),
                             (udata->alloc_nlinks * 2)); /* Double # of links allocated */
        H5O_link_t *x;                                   /* Pointer to larger array of links */

        /* Re-allocate the link table */
        if (NULL == (x = (H5O_link_t *)H5MM_realloc(udata->ltable->lnks, sizeof(H5O_link_t) * na)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5_ITER_ERROR, "memory allocation failed");
        udata->ltable->lnks = x;
    } /* end if */

    /* Iterate over the symbol table node entries, adding to link table */
    for (u = 0; u < sn->nsyms; u++) {
        const char *name;   /* Pointer to link name in heap */
        size_t      linkno; /* Link allocated */

        /* Get pointer to link's name in the heap */
        if ((name = (const char *)H5HL_offset_into(udata->heap, sn->entry[u].name_off)) == NULL)
            HGOTO_ERROR(H5E_SYM, H5E_CANTGET, H5_ITER_ERROR, "unable to get symbol table link name");

        /* Determine the link to operate on in the table */
        linkno = udata->ltable->nlinks++;

        /* Convert the entry to a link */
        if (H5G__ent_to_link(&udata->ltable->lnks[linkno], udata->heap, &sn->entry[u], name) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTCONVERT, H5_ITER_ERROR,
                        "unable to convert symbol table entry to link");
    } /* end for */

done:
    /* Release the locked items */
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, H5_ITER_ERROR, "unable to release object header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__node_build_table() */

/*-------------------------------------------------------------------------
 * Function:    H5G__node_iterate_size
 *
 * Purpose:     This function gets called by H5B_iterate_helper()
 *              to gather storage info for SNODs.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__node_iterate_size(H5F_t *f, const void H5_ATTR_UNUSED *_lt_key, haddr_t H5_ATTR_UNUSED addr,
                       const void H5_ATTR_UNUSED *_rt_key, void *_udata)
{
    hsize_t *stab_size = (hsize_t *)_udata; /* User data */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(f);
    assert(stab_size);

    *stab_size += H5G_NODE_SIZE(f);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__node_iterate_size() */

/*-------------------------------------------------------------------------
 * Function:    H5G_node_debug
 *
 * Purpose:     Prints debugging information about a symbol table node
 *              or a B-tree node for a symbol table B-tree.
 *
 * Return:      0(zero) on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_node_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, haddr_t heap_addr)
{
    H5G_node_t *sn   = NULL;
    H5HL_t     *heap = NULL;
    unsigned    u;                   /* Local index variable */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Pin the heap down in memory */
    if (heap_addr > 0 && H5_addr_defined(heap_addr))
        if (NULL == (heap = H5HL_protect(f, heap_addr, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, FAIL, "unable to protect symbol table heap");

    /*
     * If we couldn't load the symbol table node, then try loading the
     * B-tree node.
     */
    if (NULL == (sn = (H5G_node_t *)H5AC_protect(f, H5AC_SNODE, addr, f, H5AC__READ_ONLY_FLAG))) {
        H5G_bt_common_t udata; /*data to pass through B-tree	*/

        H5E_clear_stack(NULL); /* discard that error */
        udata.heap = heap;
        if (H5B_debug(f, addr, stream, indent, fwidth, H5B_SNODE, &udata) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTLOAD, FAIL, "unable to debug B-tree node");
    } /* end if */
    else {
        fprintf(stream, "%*sSymbol Table Node...\n", indent, "");
        fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Dirty:", sn->cache_info.is_dirty ? "Yes" : "No");
        fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
                "Size of Node (in bytes):", (unsigned)sn->node_size);
        fprintf(stream, "%*s%-*s %u of %u\n", indent, "", fwidth, "Number of Symbols:", sn->nsyms,
                (unsigned)(2 * H5F_SYM_LEAF_K(f)));

        indent += 3;
        fwidth = MAX(0, fwidth - 3);
        for (u = 0; u < sn->nsyms; u++) {
            fprintf(stream, "%*sSymbol %u:\n", indent - 3, "", u);

            if (heap) {
                const char *s = (const char *)H5HL_offset_into(heap, sn->entry[u].name_off);

                if (s)
                    fprintf(stream, "%*s%-*s `%s'\n", indent, "", fwidth, "Name:", s);
            } /* end if */
            else
                fprintf(stream, "%*s%-*s\n", indent, "", fwidth,
                        "Warning: Invalid heap address given, name not displayed!");

            H5G__ent_debug(sn->entry + u, stream, indent, fwidth, heap);
        } /* end for */
    }     /* end if */

done:
    if (sn && H5AC_unprotect(f, H5AC_SNODE, addr, sn, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to release symbol table node");
    if (heap && H5HL_unprotect(heap) < 0)
        HDONE_ERROR(H5E_SYM, H5E_PROTECT, FAIL, "unable to unprotect symbol table heap");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_node_debug() */
