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
 * Created:         H5Bdbg.c
 *
 * Purpose:         Debugging routines for B-link tree package.
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
#include "H5private.h"   /* Generic Functions            */
#include "H5Bpkg.h"      /* B-link trees                 */
#include "H5Eprivate.h"  /* Error handling               */
#include "H5MMprivate.h" /* Memory management            */

/*-------------------------------------------------------------------------
 * Function:    H5B_debug
 *
 * Purpose:     Prints debugging info about a B-tree.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5B_class_t *type, void *udata)
{
    H5B_t         *bt = NULL;
    H5UC_t        *rc_shared;           /* Ref-counted shared info */
    H5B_shared_t  *shared;              /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata;         /* User-data for metadata cache callback */
    unsigned       u;                   /* Local index variable */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(type);

    /* Currently does not support SWMR access */
    assert(!(H5F_INTENT(f) & H5F_ACC_SWMR_WRITE));

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /*
     * Load the tree node.
     */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    if (NULL == (bt = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to load B-tree node");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "Tree type ID:",
            ((shared->type->id) == H5B_SNODE_ID
                 ? "H5B_SNODE_ID"
                 : ((shared->type->id) == H5B_CHUNK_ID ? "H5B_CHUNK_ID" : "Unknown!")));
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Size of node:", shared->sizeof_rnode);
    fprintf(stream, "%*s%-*s %zu\n", indent, "", fwidth, "Size of raw (disk) key:", shared->sizeof_rkey);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Dirty flag:", bt->cache_info.is_dirty ? "True" : "False");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Level:", bt->level);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Address of left sibling:", bt->left);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Address of right sibling:", bt->right);
    fprintf(stream, "%*s%-*s %u (%u)\n", indent, "", fwidth, "Number of children (max):", bt->nchildren,
            shared->two_k);

    /*
     * Print the child addresses
     */
    for (u = 0; u < bt->nchildren; u++) {
        fprintf(stream, "%*sChild %d...\n", indent, "", u);
        fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent + 3, "", MAX(3, fwidth) - 3,
                "Address:", bt->child[u]);

        /* If there is a key debugging routine, use it to display the left & right keys */
        if (type->debug_key) {
            /* Decode the 'left' key & print it */
            fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(3, fwidth) - 3, "Left Key:");
            assert(H5B_NKEY(bt, shared, u));
            (void)(type->debug_key)(stream, indent + 6, MAX(6, fwidth) - 6, H5B_NKEY(bt, shared, u), udata);

            /* Decode the 'right' key & print it */
            fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(3, fwidth) - 3, "Right Key:");
            assert(H5B_NKEY(bt, shared, u + 1));
            (void)(type->debug_key)(stream, indent + 6, MAX(6, fwidth) - 6, H5B_NKEY(bt, shared, u + 1),
                                    udata);
        } /* end if */
    }     /* end for */

done:
    if (bt && H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_CANTUNPROTECT, FAIL, "unable to release B-tree node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B_debug() */

/*-------------------------------------------------------------------------
 * Function:    H5B__assert
 *
 * Purpose:     Verifies that the tree is structured correctly.
 *
 * Return:      Success:    SUCCEED
 *
 *              Failure:    aborts if something is wrong.
 *
 *-------------------------------------------------------------------------
 */
#ifdef H5B_DEBUG
herr_t
H5B__assert(H5F_t *f, haddr_t addr, const H5B_class_t *type, void *udata)
{
    H5B_t         *bt = NULL;
    H5UC_t        *rc_shared;   /* Ref-counted shared info */
    H5B_shared_t  *shared;      /* Pointer to shared B-tree info */
    H5B_cache_ud_t cache_udata; /* User-data for metadata cache callback */
    int            ncell, cmp;
    static int     ncalls = 0;
    herr_t         status;
    herr_t         ret_value = SUCCEED; /* Return value */

    /* A queue of child data */
    struct child_t {
        haddr_t         addr;
        unsigned        level;
        struct child_t *next;
    } *head = NULL, *tail = NULL, *prev = NULL, *cur = NULL, *tmp = NULL;

    FUNC_ENTER_PACKAGE

    if (0 == ncalls++) {
        if (H5DEBUG(B))
            fprintf(H5DEBUG(B), "H5B: debugging B-trees (expensive)\n");
    } /* end if */

    /* Get shared info for B-tree */
    if (NULL == (rc_shared = (type->get_shared)(f, udata)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTGET, FAIL, "can't retrieve B-tree's shared ref. count object");
    shared = (H5B_shared_t *)H5UC_GET_OBJ(rc_shared);
    assert(shared);

    /* Initialize the queue */
    cache_udata.f         = f;
    cache_udata.type      = type;
    cache_udata.rc_shared = rc_shared;
    bt                    = (H5B_t *)H5AC_protect(f, H5AC_BT, addr, &cache_udata, H5AC__READ_ONLY_FLAG);
    assert(bt);
    shared = (H5B_shared_t *)H5UC_GET_OBJ(bt->rc_shared);
    assert(shared);
    cur = (struct child_t *)H5MM_calloc(sizeof(struct child_t));
    assert(cur);
    cur->addr  = addr;
    cur->level = bt->level;
    head = tail = cur;

    status = H5AC_unprotect(f, H5AC_BT, addr, bt, H5AC__NO_FLAGS_SET);
    assert(status >= 0);
    bt = NULL; /* Make certain future references will be caught */

    /*
     * Do a breadth-first search of the tree.  New nodes are added to the end
     * of the queue as the `cur' pointer is advanced toward the end.  We don't
     * remove any nodes from the queue because we need them in the uniqueness
     * test.
     */
    for (ncell = 0; cur; ncell++) {
        bt = (H5B_t *)H5AC_protect(f, H5AC_BT, cur->addr, &cache_udata, H5AC__READ_ONLY_FLAG);
        assert(bt);

        /* Check node header */
        assert(bt->level == cur->level);
        if (cur->next && cur->next->level == bt->level)
            assert(H5_addr_eq(bt->right, cur->next->addr));
        else
            assert(!H5_addr_defined(bt->right));
        if (prev && prev->level == bt->level)
            assert(H5_addr_eq(bt->left, prev->addr));
        else
            assert(!H5_addr_defined(bt->left));

        if (cur->level > 0) {
            unsigned u;

            for (u = 0; u < bt->nchildren; u++) {
                /*
                 * Check that child nodes haven't already been seen.  If they
                 * have then the tree has a cycle.
                 */
                for (tmp = head; tmp; tmp = tmp->next)
                    assert(H5_addr_ne(tmp->addr, bt->child[u]));

                /* Add the child node to the end of the queue */
                tmp = (struct child_t *)H5MM_calloc(sizeof(struct child_t));
                assert(tmp);
                tmp->addr  = bt->child[u];
                tmp->level = bt->level - 1;
                tail->next = tmp;
                tail       = tmp;

                /* Check that the keys are monotonically increasing */
                cmp = (type->cmp2)(H5B_NKEY(bt, shared, u), udata, H5B_NKEY(bt, shared, u + 1));
                assert(cmp < 0);
            } /* end for */
        }     /* end if */

        /* Release node */
        status = H5AC_unprotect(f, H5AC_BT, cur->addr, bt, H5AC__NO_FLAGS_SET);
        assert(status >= 0);
        bt = NULL; /* Make certain future references will be caught */

        /* Advance current location in queue */
        prev = cur;
        cur  = cur->next;
    } /* end for */

    /* Free all entries from queue */
    while (head) {
        tmp = head->next;
        H5MM_xfree(head);
        head = tmp;
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B__assert() */
#endif /* H5B_DEBUG */
