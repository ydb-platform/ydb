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
 * Created:         H5B.c
 *
 * Purpose:		Implements balanced, sibling-linked, N-ary trees
 *			capable of storing any type of data with unique key
 *			values.
 *
 *			A B-link-tree is a balanced tree where each node has
 *			a pointer to its left and right siblings.  A
 *			B-link-tree is a rooted tree having the following
 *			properties:
 *
 *			1. Every node, x, has the following fields:
 *
 *			   a. level[x], the level in the tree at which node
 *			      x appears.  Leaf nodes are at level zero.
 *
 *			   b. n[x], the number of children pointed to by the
 *			      node.  Internal nodes point to subtrees while
 *			      leaf nodes point to arbitrary data.
 *
 *			   c. The child pointers themselves, child[x,i] such
 *			      that 0 <= i < n[x].
 *
 *			   d. n[x]+1 key values stored in increasing
 *			      order:
 *
 *				key[x,0] < key[x,1] < ... < key[x,n[x]].
 *
 *			   e. left[x] is a pointer to the node's left sibling
 *			      or the null pointer if this is the left-most
 *			      node at this level in the tree.
 *
 *			   f. right[x] is a pointer to the node's right
 *			      sibling or the null pointer if this is the
 *			      right-most node at this level in the tree.
 *
 *			3. The keys key[x,i] partition the key spaces of the
 *			   children of x:
 *
 *			      key[x,i] <= key[child[x,i],j] <= key[x,i+1]
 *
 *			   for any valid combination of i and j.
 *
 *			4. There are lower and upper bounds on the number of
 *			   child pointers a node can contain.  These bounds
 *			   can be expressed in terms of a fixed integer k>=2
 *			   called the `minimum degree' of the B-tree.
 *
 *			   a. Every node other than the root must have at least
 *			      k child pointers and k+1 keys.  If the tree is
 *			      nonempty, the root must have at least one child
 *			      pointer and two keys.
 *
 *			   b. Every node can contain at most 2k child pointers
 *			      and 2k+1 keys.  A node is `full' if it contains
 *			      exactly 2k child pointers and 2k+1 keys.
 *
 *			5. When searching for a particular value, V, and
 *			   key[V] = key[x,i] for some node x and entry i,
 *			   then:
 *
 *			   a. If i=0 the child[0] is followed.
 *
 *			   b. If i=n[x] the child[n[x]-1] is followed.
 *
 *			   c. Otherwise, the child that is followed
 *			      (either child[x,i-1] or child[x,i]) is
 *			      determined by the type of object to which the
 *			      leaf nodes of the tree point and is controlled
 *			      by the key comparison function registered for
 *			      that type of B-tree.
 *
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Bmodule.h" /* This source code file is part of the H5B module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Bpkg.h"      /* B-link trees				*/
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MFprivate.h" /* File memory management		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Pprivate.h"  /* Property lists                       */

/****************/
/* Local Macros */
/****************/
#define H5B_SIZEOF_HDR(F)                                                                                    \
    (H5_SIZEOF_MAGIC +       /*magic number				  */                                                          \
     4 +                     /*type, level, num entries		  */                                                \
     2 * H5F_SIZEOF_ADDR(F)) /*left and right sibling addresses	  */

/* Default initializer for H5B_ins_ud_t */
#define H5B_INS_UD_T_NULL                                                                                    \
    {                                                                                                        \
        NULL, HADDR_UNDEF, H5AC__NO_FLAGS_SET                                                                \
    }

/******************/
/* Local Typedefs */
/******************/

/* "user data" for iterating over B-tree (collects B-tree metadata size) */
typedef struct H5B_iter_ud_t {
    H5B_info_t *bt_info; /* Information about B-tree */
    void       *udata;   /* Node type's 'udata' for loading & iterator callback */
} H5B_info_ud_t;

/* Convenience struct for the arguments needed to unprotect a b-tree after a
 * call to H5B__iterate_helper() or H5B__split() */
typedef struct H5B_ins_ud_t {
    H5B_t   *bt;          /* B-tree */
    haddr_t  addr;        /* B-tree address */
    unsigned cache_flags; /* Cache flags for H5AC_unprotect() */
} H5B_ins_ud_t;

/********************/
/* Local Prototypes */
/********************/
static H5B_ins_t H5B__insert_helper(H5F_t *f, H5B_ins_ud_t *bt_ud, const H5B_class_t *type, uint8_t *lt_key,
                                    bool *lt_key_changed, uint8_t *md_key, void *udata, uint8_t *rt_key,
                                    bool *rt_key_changed, H5B_ins_ud_t *split_bt_ud /*out*/);
static herr_t H5B__insert_child(H5B_t *bt, unsigned *bt_flags, unsigned idx, haddr_t child, H5B_ins_t anchor,
                                const void *md_key);
static herr_t H5B__split(H5F_t *f, H5B_ins_ud_t *bt_ud, unsigned idx, void *udata,
                         H5B_ins_ud_t *split_bt_ud /*out*/);
static H5B_t *H5B__copy(const H5B_t *old_bt);

/*********************/
/* Package Variables */
/*********************/

/* Declare a free list to manage the haddr_t sequence information */
H5FL_SEQ_DEFINE(haddr_t);

/* Declare a PQ free list to manage the native block information */
H5FL_BLK_DEFINE(native_block);

/* Declare a free list to manage the H5B_t struct */
H5FL_DEFINE(H5B_t);

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Declare a free list to manage the H5B_shared_t struct */
H5FL_DEFINE_STATIC(H5B_shared_t);

/* Declare a free list to manage the raw page information */
H5FL_BLK_DEFINE_STATIC(page);

/* Declare a free list to manage the native key offset sequence information */
H5FL_SEQ_DEFINE_STATIC(size_t);

/*-------------------------------------------------------------------------
 * Function:	H5B_create
 *
 * Purpose:	Creates a new empty B-tree leaf node.  The UDATA pointer is
 *		passed as an argument to the sizeof_rkey() method for the
 *		B-tree.
 *
 * Return:	Success:	Non-negative, and the address of new node is
 *				returned through the ADDR_P argument.
 *
 * 		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_create(H5F_t *f, const H5B_class_t *type, void *udata, haddr_t *addr_p /*out*/)
{
    H5B_t        *bt        = NULL;
    H5B_shared_t *shared    = NULL; /* Pointer to shared B-tree info */
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(addr_p);

    /*
     * Allocate file and memory data structures.
     */
    if (NULL == (bt = H5FL_MALLOC(H5B_t)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "memory allocation failed for B-tree root node");
    memset(&bt->cache_info, 0, sizeof(H5AC_info_t));
    bt->level     = 0;
    bt->left      = HADDR_UNDEF;
    bt->right     = HADDR_UNDEF;
    bt->nchildren = 0;
    if (NULL == (bt->rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree node buffer");
    H5UC_INC(bt->rc_shared);
    shared = (H5B_shared_t *)H5UC_GET_OBJ(bt->rc_shared);
    assert(shared);
    if (NULL == (bt->native = H5FL_BLK_MALLOC(native_block, shared->sizeof_keys)) ||
        NULL == (bt->child = H5FL_SEQ_MALLOC(haddr_t, (size_t)shared->two_k)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "memory allocation failed for B-tree root node");
    if (HADDR_UNDEF == (*addr_p = H5MF_alloc(f, H5FD_MEM_BTREE, (hsize_t)shared->sizeof_rnode)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "file allocation failed for B-tree root node");

    /*
     * Cache the new B-tree node.
     */
    if (H5AC_insert_entry(f, H5AC_BT, *addr_p, bt, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "can't add B-tree root node to cache");
#ifdef H5B_DEBUG
    H5B__assert(f, *addr_p, shared->type, udata);
#endif

done:
    if (ret_value < 0) {
        if (shared && shared->sizeof_rnode > 0) {
            H5_CHECK_OVERFLOW(shared->sizeof_rnode, size_t, hsize_t);
            (void)H5MF_xfree(f, H5FD_MEM_BTREE, *addr_p, (hsize_t)shared->sizeof_rnode);
        } /* end if */
        if (bt)
            /* Destroy B-tree node */
            if (H5B__node_dest(bt) < 0)
                HDONE_ERROR(H5E_BTREE, H5E_CANTFREE, FAIL, "unable to destroy B-tree node");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_create() */ /*lint !e818 Can't make udata a pointer to const */

/*-------------------------------------------------------------------------
 * Function:	H5B_find
 *
 * Purpose:	Locate the specified information in a B-tree and return
 *		that information by filling in fields of the caller-supplied
 *		UDATA pointer depending on the type of leaf node
 *		requested.  The UDATA can point to additional data passed
 *		to the key comparison function.
 *
 * Note:	This function does not follow the left/right sibling
 *		pointers since it assumes that all nodes can be reached
 *		from the parent node.
 *
 * Return:	Non-negative (true/false) on success (if found, values returned
 *              through the UDATA argument). Negative on failure (if not found,
 *              UDATA is undefined).
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_find(H5F_t *f, const H5B_class_t *type, haddr_t addr, bool *found, void *udata)
{
    H5B_t         *bt = NULL;
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       idx = 0, lt = 0, rt; /* Final, left & right key indices */
    int            cmp       = 1;       /* Key comparison value */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(type->decode);
    assert(type->cmp3);
    assert(type->found);
    assert(H5_addr_defined(addr));

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /*
     * Perform a binary search to locate the child which contains
     * the thing for which we're searching.
     */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load B-tree node");

    rt = bt->nchildren;
    while (lt < rt && cmp) {
        idx = (lt + rt) / 2;
        /* compare */
        if ((cmp = (type->cmp3)(H5B_NKEY(bt, shared, idx), udata, H5B_NKEY(bt, shared, (idx + 1)))) < 0)
            rt = idx;
        else
            lt = idx + 1;
    } /* end while */

    /* Check if not found */
    if (cmp)
        *found = false;
    else {
        /*
         * Follow the link to the subtree or to the data node.
         */
        assert(idx < bt->nchildren);

        if (bt->level > 0) {
            if ((ret_value = H5B_find(f, type, bt->child[idx], found, udata)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "can't lookup key in subtree");
        } /* end if */
        else {
            if ((ret_value = (type->found)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), found, udata)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "can't lookup key in leaf node");
        } /* end else */
    }     /* end else */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_find() */

/*-------------------------------------------------------------------------
 * Function:	H5B__split
 *
 * Purpose:	Split a single node into two nodes.  The old node will
 *		contain the left children and the new node will contain the
 *		right children.
 *
 *		The UDATA pointer is passed to the sizeof_rkey() method but is
 *		otherwise unused.
 *
 *		The BT_UD argument is a pointer to a protected B-tree
 *		node.
 *
 * Return:	Non-negative on success (The address of the new node is
 *              returned through the NEW_ADDR argument). Negative on failure.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B__split(H5F_t *f, H5B_ins_ud_t *bt_ud, unsigned idx, void *udata, H5B_ins_ud_t *split_bt_ud /*out*/)
{
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       nleft, nright;       /* Number of keys in left & right halves */
    double         split_ratios[3];     /* B-tree split ratios */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(bt_ud);
    assert(bt_ud->bt);
    assert(H5_addr_defined(bt_ud->addr));
    assert(split_bt_ud);
    assert(!split_bt_ud->bt);

    /*
     * Initialize variables.
     */
    shared = (H5B_shared_t *)H5UC_GET_OBJ(bt_ud->bt->rc_shared);
    assert(shared);
    assert(bt_ud->bt->nchildren == shared->two_k);

    /* Get B-tree split ratios */
    if (H5CX_get_btree_split_ratios(split_ratios) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree split ratios");

#ifdef H5B_DEBUG
    if (H5DEBUG(B)) {
        const char *side;

        if (!H5_addr_defined(bt_ud->bt->left) && !H5_addr_defined(bt_ud->bt->right))
            side = "ONLY";
        else if (!H5_addr_defined(bt_ud->bt->right))
            side = "RIGHT";
        else if (!H5_addr_defined(bt_ud->bt->left))
            side = "LEFT";
        else
            side = "MIDDLE";
        fprintf(H5DEBUG(B), "H5B__split: %3u {%5.3f,%5.3f,%5.3f} %6s", shared->two_k, split_ratios[0],
                split_ratios[1], split_ratios[2], side);
    }
#endif

    /*
     * Decide how to split the children of the old node among the old node
     * and the new node.
     */
    if (!H5_addr_defined(bt_ud->bt->right))
        nleft = (unsigned)((double)shared->two_k * split_ratios[2]); /*right*/
    else if (!H5_addr_defined(bt_ud->bt->left))
        nleft = (unsigned)((double)shared->two_k * split_ratios[0]); /*left*/
    else
        nleft = (unsigned)((double)shared->two_k * split_ratios[1]); /*middle*/

    /*
     * Keep the new child in the same node as the child that split.  This can
     * result in nodes that have an unused child when data is written
     * sequentially, but it simplifies stuff below.
     */
    if (idx < nleft && nleft == shared->two_k)
        --nleft;
    else if (idx >= nleft && 0 == nleft)
        nleft++;
    nright = shared->two_k - nleft;
#ifdef H5B_DEBUG
    if (H5DEBUG(B))
        fprintf(H5DEBUG(B), " split %3d/%-3d\n", nleft, nright);
#endif

    /*
     * Create the new B-tree node.
     */
    if (H5B_create(f, shared->type, udata, &split_bt_ud->addr /*out*/) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to create B-tree");
    cache_udata.f         = f;
    cache_udata.type      = shared->type;
    cache_udata.rc_shared = bt_ud->bt->rc_shared;
    if (NULL == (split_bt_ud->bt =
                     (H5B_t *)H5AC_protect(f, H5AC_BT, split_bt_ud->addr, &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree");
    split_bt_ud->bt->level = bt_ud->bt->level;

    /*
     * Copy data from the old node to the new node.
     */

    split_bt_ud->cache_flags = H5AC__DIRTIED_FLAG;
    H5MM_memcpy(split_bt_ud->bt->native, bt_ud->bt->native + nleft * shared->type->sizeof_nkey,
                (nright + 1) * shared->type->sizeof_nkey);
    H5MM_memcpy(split_bt_ud->bt->child, &bt_ud->bt->child[nleft], nright * sizeof(haddr_t));

    split_bt_ud->bt->nchildren = nright;

    /*
     * Truncate the old node.
     */
    bt_ud->cache_flags |= H5AC__DIRTIED_FLAG;
    bt_ud->bt->nchildren = nleft;

    /*
     * Update other sibling pointers.
     */
    split_bt_ud->bt->left  = bt_ud->addr;
    split_bt_ud->bt->right = bt_ud->bt->right;

    if (H5_addr_defined(bt_ud->bt->right)) {
        H5B_t *tmp_bt;

        if (NULL ==
            (tmp_bt = (H5B_t *)H5AC_protect(f, H5AC_BT, bt_ud->bt->right, &cache_udata, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load right sibling");

        tmp_bt->left = split_bt_ud->addr;

        if (H5AC_unprotect(f, H5AC_BT, bt_ud->bt->right, tmp_bt, H5AC__DIRTIED_FLAG) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");
    } /* end if */

    bt_ud->bt->right = split_bt_ud->addr;
    assert(bt_ud->cache_flags & H5AC__DIRTIED_FLAG);

done:
    if (ret_value < 0) {
        if (split_bt_ud->bt &&
            H5AC_unprotect(f, H5AC_BT, split_bt_ud->addr, split_bt_ud->bt, split_bt_ud->cache_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");
        split_bt_ud->bt          = NULL;
        split_bt_ud->addr        = HADDR_UNDEF;
        split_bt_ud->cache_flags = H5AC__NO_FLAGS_SET;
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__split() */

/*-------------------------------------------------------------------------
 * Function:	H5B_insert
 *
 * Purpose:	Adds a new item to the B-tree.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_insert(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata)
{
    /*
     * These are defined this way to satisfy alignment constraints.
     */
    uint64_t _lt_key[128], _md_key[128], _rt_key[128];
    uint8_t *lt_key = (uint8_t *)_lt_key;
    uint8_t *md_key = (uint8_t *)_md_key;
    uint8_t *rt_key = (uint8_t *)_rt_key;

    bool           lt_key_changed = false, rt_key_changed = false;
    haddr_t        old_root_addr = HADDR_UNDEF;
    unsigned       level;
    H5B_ins_ud_t   bt_ud       = H5B_INS_UD_T_NULL; /* (Old) root node */
    H5B_ins_ud_t   split_bt_ud = H5B_INS_UD_T_NULL; /* Split B-tree node */
    H5B_t         *new_root_bt = NULL;              /* New root node */
    H5UC_t        *rc_shared;                       /* Ref-counted shared info */
    H5B_shared_t  *shared;                          /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;                     /* User-data for metadata cache callback */
    H5B_ins_t      my_ins    = H5B_INS_ERROR;
    herr_t         ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    assert(f);
    assert(type);
    assert(type->sizeof_nkey <= sizeof _lt_key);
    assert(H5_addr_defined(addr));

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /* Protect the root node */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    bt_ud.addr            = addr;
    if (NULL == (bt_ud.bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to locate root of B-tree");

    /* Insert the object */
    if ((int)(my_ins = H5B__insert_helper(f, &bt_ud, type, lt_key, &lt_key_changed, md_key, udata, rt_key,
                                          &rt_key_changed, &split_bt_ud /*out*/)) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to insert key");

    /* Check if the root node split */
    if (H5B_INS_NOOP == my_ins) {
        /* The root node did not split - just return */
        assert(!split_bt_ud.bt);
        HGOTO_DONE(SUCCEED);
    } /* end if */
    assert(H5B_INS_RIGHT == my_ins);
    assert(split_bt_ud.bt);
    assert(H5_addr_defined(split_bt_ud.addr));

    /* Get level of old root */
    level = bt_ud.bt->level;

    /* update left and right keys */
    if (!lt_key_changed)
        H5MM_memcpy(lt_key, H5B_NKEY(bt_ud.bt, shared, 0), type->sizeof_nkey);
    if (!rt_key_changed)
        H5MM_memcpy(rt_key, H5B_NKEY(split_bt_ud.bt, shared, split_bt_ud.bt->nchildren), type->sizeof_nkey);

    /*
     * Copy the old root node to some other file location and make the new root
     * at the old root's previous address.  This prevents the B-tree from
     * "moving".
     */
    H5_CHECK_OVERFLOW(shared->sizeof_rnode, size_t, hsize_t);
    if (HADDR_UNDEF == (old_root_addr = H5MF_alloc(f, H5FD_MEM_BTREE, (hsize_t)shared->sizeof_rnode)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, FAIL, "unable to allocate file space to move root");

    /*
     * Move the node to the new location
     */

    /* Make a copy of the old root information */
    if (NULL == (new_root_bt = H5B__copy(bt_ud.bt)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTCOPY, FAIL, "unable to copy old root");

    /* Unprotect the old root so we can move it.  Also force it to be marked
     * dirty so it is written to the new location. */
    if (H5AC_unprotect(f, H5AC_BT, bt_ud.addr, bt_ud.bt, H5AC__DIRTIED_FLAG) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release old root");
    bt_ud.bt = NULL; /* Make certain future references will be caught */

    /* Move the location of the old root on the disk */
    if (H5AC_move_entry(f, H5AC_BT, bt_ud.addr, old_root_addr) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, FAIL, "unable to move B-tree root node");
    bt_ud.addr = old_root_addr;

    /* Update the split b-tree's left pointer to point to the new location */
    split_bt_ud.bt->left = bt_ud.addr;
    split_bt_ud.cache_flags |= H5AC__DIRTIED_FLAG;

    /* clear the old root info at the old address (we already copied it) */
    new_root_bt->left  = HADDR_UNDEF;
    new_root_bt->right = HADDR_UNDEF;

    /* Set the new information for the copy */
    new_root_bt->level     = level + 1;
    new_root_bt->nchildren = 2;

    new_root_bt->child[0] = bt_ud.addr;
    H5MM_memcpy(H5B_NKEY(new_root_bt, shared, 0), lt_key, shared->type->sizeof_nkey);

    new_root_bt->child[1] = split_bt_ud.addr;
    H5MM_memcpy(H5B_NKEY(new_root_bt, shared, 1), md_key, shared->type->sizeof_nkey);
    H5MM_memcpy(H5B_NKEY(new_root_bt, shared, 2), rt_key, shared->type->sizeof_nkey);

    /* Insert the modified copy of the old root into the file again */
    if (H5AC_insert_entry(f, H5AC_BT, addr, new_root_bt, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTFLUSH, FAIL, "unable to add old B-tree root node to cache");

done:
    if (ret_value < 0)
        if (new_root_bt && H5B__node_dest(new_root_bt) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTRELEASE, FAIL, "unable to free B-tree root node");

    if (bt_ud.bt)
        if (H5AC_unprotect(f, H5AC_BT, bt_ud.addr, bt_ud.bt, bt_ud.cache_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to unprotect old root");

    if (split_bt_ud.bt)
        if (H5AC_unprotect(f, H5AC_BT, split_bt_ud.addr, split_bt_ud.bt, split_bt_ud.cache_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to unprotect new child");

#ifdef H5B_DEBUG
    if (ret_value >= 0)
        H5B__assert(f, addr, type, udata);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_insert() */

/*-------------------------------------------------------------------------
 * Function:	H5B__insert_child
 *
 * Purpose:	Insert a child to the left or right of child[IDX] depending
 *		on whether ANCHOR is H5B_INS_LEFT or H5B_INS_RIGHT. The BT
 *		argument is a pointer to a protected B-tree node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B__insert_child(H5B_t *bt, unsigned *bt_flags, unsigned idx, haddr_t child, H5B_ins_t anchor,
                  const void *md_key)
{
    H5B_shared_t *shared; /* Pointer to shared B-tree info */
    uint8_t      *base;   /* Base offset for move */

    FUNC_ENTER_PACKAGE_NOERR

    assert(bt);
    assert(bt_flags);
    assert(H5_addr_defined(child));
    shared = (H5B_shared_t *)H5UC_GET_OBJ(bt->rc_shared);
    assert(shared);
    assert(bt->nchildren < shared->two_k);

    /* Check for inserting right-most key into node (common when just appending
     * records to an unlimited dimension chunked dataset)
     */
    base = H5B_NKEY(bt, shared, (idx + 1));
    if ((idx + 1) == bt->nchildren) {
        /* Make room for the new key */
        H5MM_memcpy(base + shared->type->sizeof_nkey, base,
                    shared->type->sizeof_nkey); /* No overlap possible - memcpy() OK */
        H5MM_memcpy(base, md_key, shared->type->sizeof_nkey);

        /* The MD_KEY is the left key of the new node */
        if (H5B_INS_RIGHT == anchor)
            idx++; /* Don't have to memmove() child addresses down, just add new child */
        else
            /* Make room for the new child address */
            bt->child[idx + 1] = bt->child[idx];
    } /* end if */
    else {
        /* Make room for the new key */
        memmove(base + shared->type->sizeof_nkey, base, (bt->nchildren - idx) * shared->type->sizeof_nkey);
        H5MM_memcpy(base, md_key, shared->type->sizeof_nkey);

        /* The MD_KEY is the left key of the new node */
        if (H5B_INS_RIGHT == anchor)
            idx++;

        /* Make room for the new child address */
        memmove(bt->child + idx + 1, bt->child + idx, (bt->nchildren - idx) * sizeof(haddr_t));
    } /* end if */

    bt->child[idx] = child;
    bt->nchildren += 1;

    /* Mark node as dirty */
    *bt_flags |= H5AC__DIRTIED_FLAG;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5B_insert_child() */

/*-------------------------------------------------------------------------
 * Function:	H5B__insert_helper
 *
 * Purpose:	Inserts the item UDATA into the tree rooted at ADDR and having
 *		the specified type.
 *
 *		On return, if LT_KEY_CHANGED is non-zero, then LT_KEY is
 *		the new native left key.  Similarly for RT_KEY_CHANGED
 *		and RT_KEY.
 *
 *		If the node splits, then MD_KEY contains the key that
 *		was split between the two nodes (that is, the key that
 *		appears as the max key in the left node and the min key
 *		in the right node).
 *
 * Return:	Success:	A B-tree operation.  The address of the new
 *				node, if the node splits, is returned through
 *				the NEW_NODE_P argument. The new node is always
 *				to the right of the previous node.  This
 *				function is called recursively and the return
 *				value influences the behavior of the caller.
 *				See also, declaration of H5B_ins_t.
 *
 *		Failure:	H5B_INS_ERROR
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5B__insert_helper(H5F_t *f, H5B_ins_ud_t *bt_ud, const H5B_class_t *type, uint8_t *lt_key,
                   bool *lt_key_changed, uint8_t *md_key, void *udata, uint8_t *rt_key, bool *rt_key_changed,
                   H5B_ins_ud_t *split_bt_ud /*out*/)
{
    H5B_t         *bt;                                  /* Convenience pointer to B-tree */
    H5UC_t        *rc_shared;                           /* Ref-counted shared info */
    H5B_shared_t  *shared;                              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;                         /* User-data for metadata cache callback */
    unsigned       lt = 0, idx = 0, rt;                 /* Left, final & right index values */
    int            cmp             = -1;                /* Key comparison value */
    H5B_ins_ud_t   child_bt_ud     = H5B_INS_UD_T_NULL; /* Child B-tree */
    H5B_ins_ud_t   new_child_bt_ud = H5B_INS_UD_T_NULL; /* Newly split child B-tree */
    H5B_ins_t      my_ins          = H5B_INS_ERROR;
    H5B_ins_t      ret_value       = H5B_INS_ERROR; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments
     */
    assert(f);
    assert(bt_ud);
    assert(bt_ud->bt);
    assert(H5_addr_defined(bt_ud->addr));
    assert(type);
    assert(type->decode);
    assert(type->cmp3);
    assert(type->new_node);
    assert(lt_key);
    assert(lt_key_changed);
    assert(rt_key);
    assert(rt_key_changed);
    assert(split_bt_ud);
    assert(!split_bt_ud->bt);
    assert(!H5_addr_defined(split_bt_ud->addr));
    assert(split_bt_ud->cache_flags == H5AC__NO_FLAGS_SET);

    bt = bt_ud->bt;

    *lt_key_changed = false;
    *rt_key_changed = false;

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, H5B_INS_ERROR,
                    "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /*
     * Use a binary search to find the child that will receive the new
     * data.  When the search completes IDX points to the child that
     * should get the new data.
     */
    rt = bt->nchildren;

    while (lt < rt && cmp) {
        idx = (lt + rt) / 2;
        if ((cmp = (type->cmp3)(H5B_NKEY(bt, shared, idx), udata, H5B_NKEY(bt, shared, idx + 1))) < 0)
            rt = idx;
        else
            lt = idx + 1;
    } /* end while */

    /* Set up user data for cache callbacks */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;

    if (0 == bt->nchildren) {
        /*
         * The value being inserted will be the only value in this tree. We
         * must necessarily be at level zero.
         */
        assert(0 == bt->level);
        if ((type->new_node)(f, H5B_INS_FIRST, H5B_NKEY(bt, shared, 0), udata, H5B_NKEY(bt, shared, 1),
                             bt->child + 0 /*out*/) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, H5B_INS_ERROR, "unable to create leaf node");
        bt->nchildren = 1;
        bt_ud->cache_flags |= H5AC__DIRTIED_FLAG;
        idx = 0;

        if (type->follow_min) {
            if ((int)(my_ins = (type->insert)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), lt_key_changed,
                                              md_key, udata, H5B_NKEY(bt, shared, idx + 1), rt_key_changed,
                                              &new_child_bt_ud.addr /*out*/)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "unable to insert first leaf node");
        } /* end if */
        else
            my_ins = H5B_INS_NOOP;
    }
    else if (cmp < 0 && idx == 0) {
        if (bt->level > 0) {
            /*
             * The value being inserted is less than any value in this tree.
             * Follow the minimum branch out of this node to a subtree.
             */
            child_bt_ud.addr = bt->child[idx];
            if (NULL == (child_bt_ud.bt = (H5B_t *)H5AC_protect(f, H5AC_BT, child_bt_ud.addr, &cache_udata,
                                                                H5AC__NO_FLAGS_SET)))
                HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to load node");

            if ((int)(my_ins = H5B__insert_helper(
                          f, &child_bt_ud, type, H5B_NKEY(bt, shared, idx), lt_key_changed, md_key, udata,
                          H5B_NKEY(bt, shared, idx + 1), rt_key_changed, &new_child_bt_ud /*out*/)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert minimum subtree");
        }
        else if (type->follow_min) {
            /*
             * The value being inserted is less than any leaf node out of this
             * current node.  Follow the minimum branch to a leaf node and let
             * the subclass handle the problem.
             */
            if ((int)(my_ins = (type->insert)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), lt_key_changed,
                                              md_key, udata, H5B_NKEY(bt, shared, idx + 1), rt_key_changed,
                                              &new_child_bt_ud.addr /*out*/)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert minimum leaf node");
        }
        else {
            /*
             * The value being inserted is less than any leaf node out of the
             * current node. Create a new minimum leaf node out of this B-tree
             * node. This node is not empty (handled above).
             */
            my_ins = H5B_INS_LEFT;
            H5MM_memcpy(md_key, H5B_NKEY(bt, shared, idx), type->sizeof_nkey);
            if ((type->new_node)(f, H5B_INS_LEFT, H5B_NKEY(bt, shared, idx), udata, md_key,
                                 &new_child_bt_ud.addr /*out*/) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert minimum leaf node");
            *lt_key_changed = true;
        } /* end else */

#ifdef H5_STRICT_FORMAT_CHECKS
        /* Since we are to the left of the leftmost key there must not be a left
         * sibling */
        if (H5_addr_defined(bt->left))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR,
                        "internal error: likely corrupt key values");
#endif /* H5_STRICT_FORMAT_CHECKS */
    }
    else if (cmp > 0 && idx + 1 >= bt->nchildren) {
        if (bt->level > 0) {
            /*
             * The value being inserted is larger than any value in this tree.
             * Follow the maximum branch out of this node to a subtree.
             */
            idx              = bt->nchildren - 1;
            child_bt_ud.addr = bt->child[idx];
            if (NULL == (child_bt_ud.bt = (H5B_t *)H5AC_protect(f, H5AC_BT, child_bt_ud.addr, &cache_udata,
                                                                H5AC__NO_FLAGS_SET)))
                HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to load node");

            if ((int)(my_ins = H5B__insert_helper(
                          f, &child_bt_ud, type, H5B_NKEY(bt, shared, idx), lt_key_changed, md_key, udata,
                          H5B_NKEY(bt, shared, idx + 1), rt_key_changed, &new_child_bt_ud /*out*/)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert maximum subtree");
        }
        else if (type->follow_max) {
            /*
             * The value being inserted is larger than any leaf node out of the
             * current node.  Follow the maximum branch to a leaf node and let
             * the subclass handle the problem.
             */
            idx = bt->nchildren - 1;
            if ((int)(my_ins = (type->insert)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), lt_key_changed,
                                              md_key, udata, H5B_NKEY(bt, shared, idx + 1), rt_key_changed,
                                              &new_child_bt_ud.addr /*out*/)) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert maximum leaf node");
        }
        else {
            /*
             * The value being inserted is larger than any leaf node out of the
             * current node.  Create a new maximum leaf node out of this B-tree
             * node.
             */
            idx    = bt->nchildren - 1;
            my_ins = H5B_INS_RIGHT;
            H5MM_memcpy(md_key, H5B_NKEY(bt, shared, idx + 1), type->sizeof_nkey);
            if ((type->new_node)(f, H5B_INS_RIGHT, md_key, udata, H5B_NKEY(bt, shared, idx + 1),
                                 &new_child_bt_ud.addr /*out*/) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert maximum leaf node");
            *rt_key_changed = true;
        } /* end else */

#ifdef H5_STRICT_FORMAT_CHECKS
        /* Since we are to the right of the rightmost key there must not be a
         * right sibling */
        if (H5_addr_defined(bt->right))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR,
                        "internal error: likely corrupt key values");
#endif /* H5_STRICT_FORMAT_CHECKS */
    }
    else if (cmp) {
        /*
         * We couldn't figure out which branch to follow out of this node. THIS
         * IS A MAJOR PROBLEM THAT NEEDS TO BE FIXED --rpm.
         */
        assert("INTERNAL HDF5 ERROR (contact rpm)" && 0);
#ifdef NDEBUG
        HDabort();
#endif /* NDEBUG */
    }
    else if (bt->level > 0) {
        /*
         * Follow a branch out of this node to another subtree.
         */
        assert(idx < bt->nchildren);
        child_bt_ud.addr = bt->child[idx];
        if (NULL == (child_bt_ud.bt = (H5B_t *)H5AC_protect(f, H5AC_BT, child_bt_ud.addr, &cache_udata,
                                                            H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to load node");

        if ((int)(my_ins = H5B__insert_helper(f, &child_bt_ud, type, H5B_NKEY(bt, shared, idx),
                                              lt_key_changed, md_key, udata, H5B_NKEY(bt, shared, idx + 1),
                                              rt_key_changed, &new_child_bt_ud /*out*/)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert subtree");
    }
    else {
        /*
         * Follow a branch out of this node to a leaf node of some other type.
         */
        assert(idx < bt->nchildren);
        if ((int)(my_ins = (type->insert)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), lt_key_changed,
                                          md_key, udata, H5B_NKEY(bt, shared, idx + 1), rt_key_changed,
                                          &new_child_bt_ud.addr /*out*/)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert leaf node");
    }
    assert((int)my_ins >= 0);

    /*
     * Update the left and right keys of the current node.
     */
    if (*lt_key_changed) {
        bt_ud->cache_flags |= H5AC__DIRTIED_FLAG;
        if (idx > 0) {
            assert(type->critical_key == H5B_LEFT);
            assert(!(H5B_INS_LEFT == my_ins || H5B_INS_RIGHT == my_ins));
            *lt_key_changed = false;
        } /* end if */
        else
            H5MM_memcpy(lt_key, H5B_NKEY(bt, shared, idx), type->sizeof_nkey);
    } /* end if */
    if (*rt_key_changed) {
        bt_ud->cache_flags |= H5AC__DIRTIED_FLAG;
        if (idx + 1 < bt->nchildren) {
            assert(type->critical_key == H5B_RIGHT);
            assert(!(H5B_INS_LEFT == my_ins || H5B_INS_RIGHT == my_ins));
            *rt_key_changed = false;
        } /* end if */
        else
            H5MM_memcpy(rt_key, H5B_NKEY(bt, shared, idx + 1), type->sizeof_nkey);
    } /* end if */

    /*
     * Handle changes/additions to children
     */
    assert(!(bt->level == 0) != !(child_bt_ud.bt));
    if (H5B_INS_CHANGE == my_ins) {
        /*
         * The insertion simply changed the address for the child.
         */
        assert(!child_bt_ud.bt);
        assert(bt->level == 0);
        bt->child[idx] = new_child_bt_ud.addr;
        bt_ud->cache_flags |= H5AC__DIRTIED_FLAG;
    }
    else if (H5B_INS_LEFT == my_ins || H5B_INS_RIGHT == my_ins) {
        unsigned *tmp_bt_flags_ptr = NULL;
        H5B_t    *tmp_bt;

        /*
         * If this node is full then split it before inserting the new child.
         */
        if (bt->nchildren == shared->two_k) {
            if (H5B__split(f, bt_ud, idx, udata, split_bt_ud /*out*/) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTSPLIT, H5B_INS_ERROR, "unable to split node");
            if (idx < bt->nchildren) {
                tmp_bt           = bt;
                tmp_bt_flags_ptr = &bt_ud->cache_flags;
            }
            else {
                idx -= bt->nchildren;
                tmp_bt           = split_bt_ud->bt;
                tmp_bt_flags_ptr = &split_bt_ud->cache_flags;
            }
        } /* end if */
        else {
            tmp_bt           = bt;
            tmp_bt_flags_ptr = &bt_ud->cache_flags;
        } /* end else */

        /* Insert the child */
        if (H5B__insert_child(tmp_bt, tmp_bt_flags_ptr, idx, new_child_bt_ud.addr, my_ins, md_key) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTINSERT, H5B_INS_ERROR, "can't insert child");
    } /* end else-if */

    /*
     * If this node split, return the mid key (the one that is shared
     * by the left and right node).
     */
    if (split_bt_ud->bt) {
        H5MM_memcpy(md_key, H5B_NKEY(split_bt_ud->bt, shared, 0), type->sizeof_nkey);
        ret_value = H5B_INS_RIGHT;
#ifdef H5B_DEBUG
        /*
         * The max key in the original left node must be equal to the min key
         * in the new node.
         */
        cmp = (type->cmp2)(H5B_NKEY(bt, shared, bt->nchildren), udata, H5B_NKEY(split_bt_ud->bt, shared, 0));
        assert(0 == cmp);
#endif
    } /* end if */
    else
        ret_value = H5B_INS_NOOP;

done:
    if (child_bt_ud.bt)
        if (H5AC_unprotect(f, H5AC_BT, child_bt_ud.addr, child_bt_ud.bt, child_bt_ud.cache_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to unprotect child");

    if (new_child_bt_ud.bt)
        if (H5AC_unprotect(f, H5AC_BT, new_child_bt_ud.addr, new_child_bt_ud.bt,
                           new_child_bt_ud.cache_flags) < 0)
            HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to unprotect new child");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_insert_helper() */

/*-------------------------------------------------------------------------
 * Function:	H5B__iterate_helper
 *
 * Purpose:	Calls the list callback for each leaf node of the
 *		B-tree, passing it the caller's UDATA structure.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B__iterate_helper(H5F_t *f, const H5B_class_t *type, haddr_t addr, H5B_operator_t op, void *udata)
{
    H5B_t         *bt = NULL;                /* Pointer to current B-tree node */
    H5UC_t        *rc_shared;                /* Ref-counted shared info */
    H5B_shared_t  *shared;                   /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;              /* User-data for metadata cache callback */
    unsigned       u;                        /* Local index variable */
    herr_t         ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(H5_addr_defined(addr));
    assert(op);
    assert(udata);

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /* Protect the initial/current node */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5_ITER_ERROR, "unable to load B-tree node");

    /* Iterate over node's children */
    for (u = 0; u < bt->nchildren && ret_value == H5_ITER_CONT; u++) {
        if (bt->level > 0)
            ret_value = H5B__iterate_helper(f, type, bt->child[u], op, udata);
        else
            ret_value = (*op)(f, H5B_NKEY(bt, shared, u), bt->child[u], H5B_NKEY(bt, shared, u + 1), udata);
        if (ret_value < 0)
            HERROR(H5E_BTREE, H5E_BADITER, "B-tree iteration failed");
    } /* end for */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5_ITER_ERROR, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__iterate_helper() */

/*-------------------------------------------------------------------------
 * Function:	H5B_iterate
 *
 * Purpose:	Calls the list callback for each leaf node of the
 *		B-tree, passing it the UDATA structure.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_iterate(H5F_t *f, const H5B_class_t *type, haddr_t addr, H5B_operator_t op, void *udata)
{
    herr_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(H5_addr_defined(addr));
    assert(op);
    assert(udata);

    /* Iterate over the B-tree records */
    if ((ret_value = H5B__iterate_helper(f, type, addr, op, udata)) < 0)
        HERROR(H5E_BTREE, H5E_BADITER, "B-tree iteration failed");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_iterate() */

/*-------------------------------------------------------------------------
 * Function:	H5B__remove_helper
 *
 * Purpose:	The recursive part of removing an item from a B-tree.  The
 *		sub B-tree that is being considered is located at ADDR and
 *		the item to remove is described by UDATA.  If the removed
 *		item falls at the left or right end of the current level then
 *		it might be necessary to adjust the left and/or right keys
 *		(LT_KEY and/or RT_KEY) to to indicate that they changed by
 * 		setting LT_KEY_CHANGED and/or RT_KEY_CHANGED.
 *
 * Return:	Success:	A B-tree operation, see comments for
 *				H5B_ins_t declaration.  This function is
 *				called recursively and the return value
 *				influences the actions of the caller. It is
 *				also called by H5B_remove().
 *
 *		Failure:	H5B_INS_ERROR, a negative value.
 *
 *-------------------------------------------------------------------------
 */
static H5B_ins_t
H5B__remove_helper(H5F_t *f, haddr_t addr, const H5B_class_t *type, int level, uint8_t *lt_key /*out*/,
                   bool *lt_key_changed /*out*/, void *udata, uint8_t *rt_key /*out*/,
                   bool *rt_key_changed /*out*/)
{
    H5B_t         *bt = NULL, *sibling = NULL;
    unsigned       bt_flags = H5AC__NO_FLAGS_SET;
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       idx = 0, lt = 0, rt; /* Final, left & right indices */
    int            cmp       = 1;       /* Key comparison value */
    H5B_ins_t      ret_value = H5B_INS_ERROR;

    FUNC_ENTER_PACKAGE

    assert(f);
    assert(H5_addr_defined(addr));
    assert(type);
    assert(type->decode);
    assert(type->cmp3);
    assert(lt_key && lt_key_changed);
    assert(udata);
    assert(rt_key && rt_key_changed);

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, H5B_INS_ERROR,
                    "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /*
     * Perform a binary search to locate the child which contains the thing
     * for which we're searching.
     */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to load B-tree node");

    rt = bt->nchildren;
    while (lt < rt && cmp) {
        idx = (lt + rt) / 2;
        if ((cmp = (type->cmp3)(H5B_NKEY(bt, shared, idx), udata, H5B_NKEY(bt, shared, idx + 1))) < 0)
            rt = idx;
        else
            lt = idx + 1;
    } /* end while */
    if (cmp)
        HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, H5B_INS_ERROR, "B-tree key not found");

    /*
     * Follow the link to the subtree or to the data node.  The return value
     * will be one of H5B_INS_ERROR, H5B_INS_NOOP, or H5B_INS_REMOVE.
     */
    assert(idx < bt->nchildren);
    if (bt->level > 0) {
        /* We're at an internal node -- call recursively */
        if ((int)(ret_value =
                      H5B__remove_helper(f, bt->child[idx], type, level + 1,
                                         H5B_NKEY(bt, shared, idx) /*out*/, lt_key_changed /*out*/, udata,
                                         H5B_NKEY(bt, shared, idx + 1) /*out*/, rt_key_changed /*out*/)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, H5B_INS_ERROR, "key not found in subtree");
    }
    else if (type->remove) {
        /*
         * We're at a leaf node but the leaf node points to an object that
         * has a removal method.  Pass the removal request to the pointed-to
         * object and let it decide how to progress.
         */
        if ((int)(ret_value = (type->remove)(f, bt->child[idx], H5B_NKEY(bt, shared, idx), lt_key_changed,
                                             udata, H5B_NKEY(bt, shared, idx + 1), rt_key_changed)) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, H5B_INS_ERROR, "key not found in leaf node");
    }
    else {
        /*
         * We're at a leaf node which points to an object that has no removal
         * method.  The best we can do is to leave the object alone but
         * remove the B-tree reference to the object.
         */
        *lt_key_changed = false;
        *rt_key_changed = false;
        ret_value       = H5B_INS_REMOVE;
    }

    /*
     * Update left and right key dirty bits if the subtree indicates that they
     * have changed.  If the subtree's left key changed and the subtree is the
     * left-most child of the current node then we must update the key in our
     * parent and indicate that it changed.  Similarly, if the right subtree
     * key changed and it's the right most key of this node we must update
     * our right key and indicate that it changed.
     */
    if (*lt_key_changed) {
        assert(type->critical_key == H5B_LEFT);
        bt_flags |= H5AC__DIRTIED_FLAG;

        if (idx > 0)
            /* Don't propagate change out of this B-tree node */
            *lt_key_changed = false;
        else
            H5MM_memcpy(lt_key, H5B_NKEY(bt, shared, idx), type->sizeof_nkey);
    } /* end if */
    if (*rt_key_changed) {
        assert(type->critical_key == H5B_RIGHT);
        bt_flags |= H5AC__DIRTIED_FLAG;
        if (idx + 1 < bt->nchildren)
            /* Don't propagate change out of this B-tree node */
            *rt_key_changed = false;
        else
            H5MM_memcpy(rt_key, H5B_NKEY(bt, shared, idx + 1), type->sizeof_nkey);
    } /* end if */

    /*
     * If the subtree returned H5B_INS_REMOVE then we should remove the
     * subtree entry from the current node.  There are four cases:
     */
    if (H5B_INS_REMOVE == ret_value) {
        /* Clients should not change keys when a node is removed.  This function
         * will handle it as appropriate, based on the value of bt->critical_key
         */
        assert(!(*lt_key_changed));
        assert(!(*rt_key_changed));

        if (1 == bt->nchildren) {
            /*
             * The subtree is the only child of this node.  Discard both
             * keys and the subtree pointer. Free this node (unless it's the
             * root node) and return H5B_INS_REMOVE.
             */
            /* Only delete the node if it is not the root node.  Note that this
             * "level" is the opposite of bt->level */
            if (level > 0) {
                /* Fix siblings, making sure that the keys remain consistent
                 * between siblings.  Overwrite the key that that is not
                 * "critical" for any child in its node to maintain this
                 * consistency (and avoid breaking key/child consistency) */
                if (H5_addr_defined(bt->left)) {
                    if (NULL == (sibling = (H5B_t *)H5AC_protect(f, H5AC_BT, bt->left, &cache_udata,
                                                                 H5AC__NO_FLAGS_SET)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR,
                                    "unable to load node from tree");

                    /* Copy right-most key from deleted node to right-most key
                     * in its left neighbor, but only if it is not the critical
                     * key for the right-most child of the left neighbor */
                    if (type->critical_key == H5B_LEFT)
                        H5MM_memcpy(H5B_NKEY(sibling, shared, sibling->nchildren), H5B_NKEY(bt, shared, 1),
                                    type->sizeof_nkey);

                    sibling->right = bt->right;

                    if (H5AC_unprotect(f, H5AC_BT, bt->left, sibling, H5AC__DIRTIED_FLAG) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR,
                                    "unable to release node from tree");
                    sibling = NULL; /* Make certain future references will be caught */
                }                   /* end if */
                if (H5_addr_defined(bt->right)) {
                    if (NULL == (sibling = (H5B_t *)H5AC_protect(f, H5AC_BT, bt->right, &cache_udata,
                                                                 H5AC__NO_FLAGS_SET)))
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR,
                                    "unable to unlink node from tree");

                    /* Copy left-most key from deleted node to left-most key in
                     * its right neighbor, but only if it is not the critical
                     * key for the left-most child of the right neighbor */
                    if (type->critical_key == H5B_RIGHT)
                        H5MM_memcpy(H5B_NKEY(sibling, shared, 0), H5B_NKEY(bt, shared, 0), type->sizeof_nkey);

                    sibling->left = bt->left;

                    if (H5AC_unprotect(f, H5AC_BT, bt->right, sibling, H5AC__DIRTIED_FLAG) < 0)
                        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR,
                                    "unable to release node from tree");
                    sibling = NULL; /* Make certain future references will be caught */
                }                   /* end if */

                /* Update bt struct */
                bt->left      = HADDR_UNDEF;
                bt->right     = HADDR_UNDEF;
                bt->nchildren = 0;

                /* Delete the node from disk (via the metadata cache) */
                bt_flags |= H5AC__DIRTIED_FLAG | H5AC__FREE_FILE_SPACE_FLAG;
                H5_CHECK_OVERFLOW(shared->sizeof_rnode, size_t, hsize_t);
                if (H5AC_unprotect(f, H5AC_BT, addr, bt, bt_flags | H5AC__DELETED_FLAG) < 0) {
                    bt       = NULL;
                    bt_flags = H5AC__NO_FLAGS_SET;
                    HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to free B-tree node");
                } /* end if */
                bt       = NULL;
                bt_flags = H5AC__NO_FLAGS_SET;
            }
            else {
                /* We removed the last child in the root node, set the level
                 * back to 0 (as well as nchildren) */
                bt->nchildren = 0;
                bt->level     = 0;
                bt_flags |= H5AC__DIRTIED_FLAG;
            } /* end else */
        }
        else if (0 == idx) {
            /*
             * The subtree is the left-most child of this node. We update the
             * key and child arrays and lt_key as appropriate, depending on the
             * status of bt->critical_key.  Return H5B_INS_NOOP.
             */
            if (type->critical_key == H5B_LEFT) {
                /* Slide all keys down 1, update lt_key */
                memmove(H5B_NKEY(bt, shared, 0), H5B_NKEY(bt, shared, 1), bt->nchildren * type->sizeof_nkey);
                H5MM_memcpy(lt_key, H5B_NKEY(bt, shared, 0), type->sizeof_nkey);
                *lt_key_changed = true;
            }
            else
                /* Slide all but the leftmost 2 keys down, leaving the leftmost
                 * key intact (the right key of the leftmost child is
                 * overwritten) */
                memmove(H5B_NKEY(bt, shared, 1), H5B_NKEY(bt, shared, 2),
                        (bt->nchildren - 1) * type->sizeof_nkey);

            memmove(bt->child, bt->child + 1, (bt->nchildren - 1) * sizeof(haddr_t));

            bt->nchildren -= 1;
            bt_flags |= H5AC__DIRTIED_FLAG;
            ret_value = H5B_INS_NOOP;
        }
        else if (idx + 1 == bt->nchildren) {
            /*
             * The subtree is the right-most child of this node. We update the
             * key and child arrays and rt_key as appropriate, depending on the
             * status of bt->critical_key.  Return H5B_INS_NOOP.
             */
            if (type->critical_key == H5B_LEFT)
                /* Slide the rightmost key down one, overwriting the left key of
                 * the deleted (rightmost) child */
                memmove(H5B_NKEY(bt, shared, bt->nchildren - 1), H5B_NKEY(bt, shared, bt->nchildren),
                        type->sizeof_nkey);
            else {
                /* Just update rt_key */
                H5MM_memcpy(rt_key, H5B_NKEY(bt, shared, bt->nchildren - 1), type->sizeof_nkey);
                *rt_key_changed = true;
            } /* end else */

            bt->nchildren -= 1;
            bt_flags |= H5AC__DIRTIED_FLAG;
            ret_value = H5B_INS_NOOP;
        }
        else {
            /*
             * There are subtrees out of this node to both the left and right of
             * the subtree being removed.  The subtree and its critical key are
             * removed from this node and all keys and nodes to the right are
             * shifted left by one place.  The subtree has already been freed.
             * Return H5B_INS_NOOP.
             */
            if (type->critical_key == H5B_LEFT)
                memmove(H5B_NKEY(bt, shared, idx), H5B_NKEY(bt, shared, idx + 1),
                        (bt->nchildren - idx) * type->sizeof_nkey);
            else
                memmove(H5B_NKEY(bt, shared, idx + 1), H5B_NKEY(bt, shared, idx + 2),
                        (bt->nchildren - 1 - idx) * type->sizeof_nkey);

            memmove(bt->child + idx, bt->child + idx + 1, (bt->nchildren - 1 - idx) * sizeof(haddr_t));

            bt->nchildren -= 1;
            bt_flags |= H5AC__DIRTIED_FLAG;
            ret_value = H5B_INS_NOOP;
        } /* end else */
    }
    else /* H5B_INS_REMOVE != ret_value */
        ret_value = H5B_INS_NOOP;

    /* Patch keys in neighboring trees if necessary */
    if (*lt_key_changed && H5_addr_defined(bt->left)) {
        assert(type->critical_key == H5B_LEFT);
        assert(level > 0);

        /* Update the rightmost key in the left sibling */
        if (NULL == (sibling = (H5B_t *)H5AC_protect(f, H5AC_BT, bt->left, &cache_udata, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to protect node");

        H5MM_memcpy(H5B_NKEY(sibling, shared, sibling->nchildren), H5B_NKEY(bt, shared, 0),
                    type->sizeof_nkey);

        if (H5AC_unprotect(f, H5AC_BT, bt->left, sibling, H5AC__DIRTIED_FLAG) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to release node from tree");
        sibling = NULL; /* Make certain future references will be caught */
    }                   /* end if */
    else if (*rt_key_changed && H5_addr_defined(bt->right)) {
        assert(type->critical_key == H5B_RIGHT);
        assert(level > 0);

        /* Update the lefttmost key in the right sibling */
        if (NULL ==
            (sibling = (H5B_t *)H5AC_protect(f, H5AC_BT, bt->right, &cache_udata, H5AC__NO_FLAGS_SET)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, H5B_INS_ERROR, "unable to protect node");

        H5MM_memcpy(H5B_NKEY(sibling, shared, 0), H5B_NKEY(bt, shared, bt->nchildren), type->sizeof_nkey);

        if (H5AC_unprotect(f, H5AC_BT, bt->right, sibling, H5AC__DIRTIED_FLAG) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to release node from tree");
        sibling = NULL; /* Make certain future references will be caught */
    }                   /* end else */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, bt_flags) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, H5B_INS_ERROR, "unable to release node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__remove_helper() */

/*-------------------------------------------------------------------------
 * Function:	H5B_remove
 *
 * Purpose:	Removes an item from a B-tree.
 *
 * Note:	The current version does not attempt to rebalance the tree.
 *              (Read the paper Yao & Lehman paper for details on why)
 *
 * Return:	Non-negative on success/Negative on failure (failure includes
 *		not being able to find the object which is to be removed).
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_remove(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata)
{
    /* These are defined this way to satisfy alignment constraints */
    uint64_t _lt_key[128], _rt_key[128];
    uint8_t *lt_key         = (uint8_t *)_lt_key; /*left key*/
    uint8_t *rt_key         = (uint8_t *)_rt_key; /*right key*/
    bool     lt_key_changed = false;              /*left key changed?*/
    bool     rt_key_changed = false;              /*right key changed?*/
    herr_t   ret_value      = SUCCEED;            /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(f);
    assert(type);
    assert(type->sizeof_nkey <= sizeof _lt_key);
    assert(H5_addr_defined(addr));

    /* The actual removal */
    if (H5B_INS_ERROR ==
        H5B__remove_helper(f, addr, type, 0, lt_key, &lt_key_changed, udata, rt_key, &rt_key_changed))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTINIT, FAIL, "unable to remove entry from B-tree");

#ifdef H5B_DEBUG
    H5B__assert(f, addr, type, udata);
#endif
done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_remove() */

/*-------------------------------------------------------------------------
 * Function:	H5B_delete
 *
 * Purpose:	Deletes an entire B-tree from the file, calling the 'remove'
 *              callbacks for each node.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_delete(H5F_t *f, const H5B_class_t *type, haddr_t addr, void *udata)
{
    H5B_t         *bt = NULL;           /* B-tree node being operated on */
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       u;                   /* Local index variable */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check args */
    assert(f);
    assert(type);
    assert(H5_addr_defined(addr));

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /* Lock this B-tree node into memory for now */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__NO_FLAGS_SET)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load B-tree node");

    /* Iterate over all children in tree, deleting them */
    if (bt->level > 0) {
        /* Iterate over all children in node, deleting them */
        for (u = 0; u < bt->nchildren; u++)
            if (H5B_delete(f, type, bt->child[u], udata) < 0)
                HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "unable to delete B-tree node");

    } /* end if */
    else {
        bool lt_key_changed, rt_key_changed; /* Whether key changed (unused here, just for callback) */

        /* Check for removal callback */
        if (type->remove) {
            /* Iterate over all entries in node, calling callback */
            for (u = 0; u < bt->nchildren; u++) {
                /* Call user's callback for each entry */
                if ((type->remove)(f, bt->child[u], H5B_NKEY(bt, shared, u), &lt_key_changed, udata,
                                   H5B_NKEY(bt, shared, u + 1), &rt_key_changed) < H5B_INS_NOOP)
                    HGOTO_ERROR(H5E_BTREE, H5E_NOTFOUND, FAIL, "can't remove B-tree node");
            } /* end for */
        }     /* end if */
    }         /* end else */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__DELETED_FLAG | H5AC__FREE_FILE_SPACE_FLAG) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node in cache");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_delete() */

/*-------------------------------------------------------------------------
 * Function:	H5B_shared_new
 *
 * Purpose:	Allocates & constructs a shared v1 B-tree struct for client.
 *
 * Return:	Success:	non-NULL pointer to struct allocated
 *		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
H5B_shared_t *
H5B_shared_new(const H5F_t *f, const H5B_class_t *type, size_t sizeof_rkey)
{
    H5B_shared_t *shared = NULL;    /* New shared B-tree struct */
    size_t        u;                /* Local index variable */
    H5B_shared_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /*
     * Check arguments.
     */
    assert(type);

    /* Allocate space for the shared structure */
    if (NULL == (shared = H5FL_CALLOC(H5B_shared_t)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "memory allocation failed for shared B-tree info");

    /* Set up the "global" information for this file's groups */
    shared->type        = type;
    shared->two_k       = 2 * H5F_KVALUE(f, type);
    shared->sizeof_addr = H5F_SIZEOF_ADDR(f);
    shared->sizeof_len  = H5F_SIZEOF_SIZE(f);
    shared->sizeof_rkey = sizeof_rkey;
    assert(shared->sizeof_rkey);
    shared->sizeof_keys  = (shared->two_k + 1) * type->sizeof_nkey;
    shared->sizeof_rnode = ((size_t)H5B_SIZEOF_HDR(f) +                 /*node header	*/
                            shared->two_k * H5F_SIZEOF_ADDR(f) +        /*child pointers */
                            (shared->two_k + 1) * shared->sizeof_rkey); /*keys		*/
    assert(shared->sizeof_rnode);

    /* Allocate and clear shared buffers */
    if (NULL == (shared->page = H5FL_BLK_MALLOC(page, shared->sizeof_rnode)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "memory allocation failed for B-tree page");
    memset(shared->page, 0, shared->sizeof_rnode);

    if (NULL == (shared->nkey = H5FL_SEQ_MALLOC(size_t, (size_t)(shared->two_k + 1))))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "memory allocation failed for B-tree native keys");

    /* Initialize the offsets into the native key buffer */
    for (u = 0; u < (shared->two_k + 1); u++)
        shared->nkey[u] = u * type->sizeof_nkey;

    /* Set return value */
    ret_value = shared;

done:
    if (NULL == ret_value)
        if (shared) {
            if (shared->page)
                shared->page = H5FL_BLK_FREE(page, shared->page);
            if (shared->nkey)
                shared->nkey = H5FL_SEQ_FREE(size_t, shared->nkey);
            shared = H5FL_FREE(H5B_shared_t, shared);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_shared_new() */

/*-------------------------------------------------------------------------
 * Function:	H5B_shared_free
 *
 * Purpose:	Free B-tree shared info
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_shared_free(void *_shared)
{
    H5B_shared_t *shared = (H5B_shared_t *)_shared;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    /* Free the raw B-tree node buffer */
    shared->page = H5FL_BLK_FREE(page, shared->page);

    /* Free the B-tree native key offsets buffer */
    shared->nkey = H5FL_SEQ_FREE(size_t, shared->nkey);

    /* Free the shared B-tree info */
    shared = H5FL_FREE(H5B_shared_t, shared);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5B_shared_free() */

/*-------------------------------------------------------------------------
 * Function:	H5B__copy
 *
 * Purpose:	Deep copies an existing H5B_t node.
 *
 * Return:	Success:	Pointer to H5B_t object.
 *
 * 		Failure:	NULL
 *
 *-------------------------------------------------------------------------
 */
static H5B_t *
H5B__copy(const H5B_t *old_bt)
{
    H5B_t        *new_node = NULL;
    H5B_shared_t *shared;           /* Pointer to shared B-tree info */
    H5B_t        *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(old_bt);
    shared = (H5B_shared_t *)H5UC_GET_OBJ(old_bt->rc_shared);
    assert(shared);

    /* Allocate memory for the new H5B_t object */
    if (NULL == (new_node = H5FL_MALLOC(H5B_t)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "memory allocation failed for B-tree root node");

    /* Copy the main structure */
    H5MM_memcpy(new_node, old_bt, sizeof(H5B_t));

    /* Reset cache info */
    memset(&new_node->cache_info, 0, sizeof(H5AC_info_t));

    if (NULL == (new_node->native = H5FL_BLK_MALLOC(native_block, shared->sizeof_keys)) ||
        NULL == (new_node->child = H5FL_SEQ_MALLOC(haddr_t, (size_t)shared->two_k)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTALLOC, NULL, "memory allocation failed for B-tree root node");

    /* Copy the other structures */
    H5MM_memcpy(new_node->native, old_bt->native, shared->sizeof_keys);
    H5MM_memcpy(new_node->child, old_bt->child, (sizeof(haddr_t) * shared->two_k));

    /* Increment the ref-count on the raw page */
    H5UC_INC(new_node->rc_shared);

    /* Set return value */
    ret_value = new_node;

done:
    if (NULL == ret_value) {
        if (new_node) {
            new_node->native = H5FL_BLK_FREE(native_block, new_node->native);
            new_node->child  = H5FL_SEQ_FREE(haddr_t, new_node->child);
            new_node         = H5FL_FREE(H5B_t, new_node);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__copy() */

/*-------------------------------------------------------------------------
 * Function:	H5B__get_info_helper
 *
 * Purpose:	Walks the B-tree nodes, getting information for all of them.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5B__get_info_helper(H5F_t *f, const H5B_class_t *type, haddr_t addr, const H5B_info_ud_t *info_udata)
{
    H5B_t         *bt = NULL;           /* Pointer to current B-tree node */
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       level;               /* Node level			     */
    size_t         sizeof_rnode;        /* Size of raw (disk) node	     */
    haddr_t        next_addr;           /* Address of next node to the right */
    haddr_t        left_child;          /* Address of left-most child in node */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(H5_addr_defined(addr));
    assert(info_udata);
    assert(info_udata->bt_info);
    assert(info_udata->udata);

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, info_udata->udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /* Get the raw node size for iteration */
    sizeof_rnode = shared->sizeof_rnode;

    /* Protect the initial/current node */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load B-tree node");

    /* Cache information from this node */
    left_child = bt->child[0];
    next_addr  = bt->right;
    level      = bt->level;

    /* Update B-tree info */
    info_udata->bt_info->size += sizeof_rnode;
    info_udata->bt_info->num_nodes++;

    /* Release current node */
    if (H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");
    bt = NULL;

    /*
     * Follow the right-sibling pointer from node to node until we've
     *      processed all nodes.
     */
    while (H5_addr_defined(next_addr)) {
        /* Protect the next node to the right */
        addr = next_addr;
        if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
            HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "B-tree node");

        /* Cache information from this node */
        next_addr = bt->right;

        /* Update B-tree info */
        info_udata->bt_info->size += sizeof_rnode;
        info_udata->bt_info->num_nodes++;

        /* Unprotect node */
        if (H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");
        bt = NULL;
    } /* end while */

    /* Check for another "row" of B-tree nodes to iterate over */
    if (level > 0) {
        /* Keep following the left-most child until we reach a leaf node. */
        if (H5B__get_info_helper(f, type, left_child, info_udata) < 0)
            HGOTO_ERROR(H5E_BTREE, H5E_CANTLIST, FAIL, "unable to list B-tree node");
    } /* end if */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__get_info_helper() */

/*-------------------------------------------------------------------------
 * Function:    H5B_get_info
 *
 * Purpose:     Return the amount of storage used for the btree.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_get_info(H5F_t *f, const H5B_class_t *type, haddr_t addr, H5B_info_t *bt_info, H5B_operator_t op,
             void *udata)
{
    H5B_info_ud_t info_udata;          /* User-data for B-tree size iteration */
    herr_t        ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);
    assert(bt_info);
    assert(H5_addr_defined(addr));
    assert(udata);

    /* Portably initialize B-tree info struct */
    memset(bt_info, 0, sizeof(*bt_info));

    /* Set up internal user-data for the B-tree 'get info' helper routine */
    info_udata.bt_info = bt_info;
    info_udata.udata   = udata;

    /* Iterate over the B-tree nodes */
    if (H5B__get_info_helper(f, type, addr, &info_udata) < 0)
        HGOTO_ERROR(H5E_BTREE, H5E_BADITER, FAIL, "B-tree iteration failed");

    /* Iterate over the B-tree records, making any "leaf" callbacks */
    /* (Only if operator defined) */
    if (op)
        if ((ret_value = H5B__iterate_helper(f, type, addr, op, udata)) < 0)
            HERROR(H5E_BTREE, H5E_BADITER, "B-tree iteration failed");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_get_info() */

/*-------------------------------------------------------------------------
 * Function:    H5B_valid
 *
 * Purpose:     Attempt to load a B-tree node.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
htri_t
H5B_valid(H5F_t *f, const H5B_class_t *type, haddr_t addr)
{
    H5B_t         *bt = NULL;           /* The B-tree */
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    htri_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(type);

    if (!H5_addr_defined(addr))
        HGOTO_ERROR(H5E_BTREE, H5E_BADVALUE, FAIL, "address is undefined");

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, NULL)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    assert(H5UC_GET_OBJ(rc_shared) != NULL);

    /*
     * Load the tree node.
     */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree node");

done:
    /* Release the node */
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_valid() */

/*-------------------------------------------------------------------------
 * Function:    H5B__node_dest
 *
 * Purpose:     Destroy/release a B-tree node
 *
 * Return:      Success:        SUCCEED
 *              Failure:        FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B__node_dest(H5B_t *bt)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* check arguments */
    assert(bt);
    assert(bt->rc_shared);

    bt->child  = H5FL_SEQ_FREE(haddr_t, bt->child);
    bt->native = H5FL_BLK_FREE(native_block, bt->native);
    H5UC_DEC(bt->rc_shared);
    bt = H5FL_FREE(H5B_t, bt);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5B__node_dest() */
