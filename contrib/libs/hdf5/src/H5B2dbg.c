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
 * Created:		H5B2dbg.c
 *
 * Purpose:		Dump debugging information about a v2 B-tree.
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5B2module.h" /* This source code file is part of the H5B2 module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5B2pkg.h"     /* v2 B-trees				*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5FLprivate.h" /* Free Lists                           */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/*-------------------------------------------------------------------------
 * Function:	H5B2__hdr_debug
 *
 * Purpose:	Prints debugging info about a B-tree header.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__hdr_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth,
                const H5B2_class_t H5_ATTR_NDEBUG_UNUSED *type, haddr_t H5_ATTR_NDEBUG_UNUSED obj_addr)
{
    H5B2_hdr_t *hdr = NULL;          /* B-tree header info */
    unsigned    u;                   /* Local index variable */
    char        temp_str[128];       /* Temporary string, for formatting */
    herr_t      ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(H5_addr_defined(obj_addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(type);

    /* Load the B-tree header  */
    if (NULL == (hdr = H5B2__hdr_protect(f, addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTLOAD, FAIL, "unable to load B-tree header");

    /* Set file pointer for this B-tree operation */
    hdr->f = f;

    /* Print opening message */
    fprintf(stream, "%*sv2 B-tree Header...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %s (%u)\n", indent, "", fwidth, "Tree type ID:", hdr->cls->name,
            (unsigned)hdr->cls->id);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Size of node:", (unsigned)hdr->node_size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Size of raw (disk) record:", (unsigned)hdr->rrec_size);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Dirty flag:", hdr->cache_info.is_dirty ? "True" : "False");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Depth:", hdr->depth);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE "\n", indent, "", fwidth,
            "Number of records in tree:", hdr->root.all_nrec);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Number of records in root node:", hdr->root.node_nrec);
    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Address of root node:", hdr->root.addr);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Split percent:", hdr->split_percent);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Merge percent:", hdr->merge_percent);

    /* Print relevant node info */
    fprintf(stream, "%*sNode Info: (max_nrec/split_nrec/merge_nrec)\n", indent, "");
    for (u = 0; u < (unsigned)(hdr->depth + 1); u++) {
        snprintf(temp_str, sizeof(temp_str), "Depth %u:", u);
        fprintf(stream, "%*s%-*s (%u/%u/%u)\n", indent + 3, "", MAX(0, fwidth - 3), temp_str,
                hdr->node_info[u].max_nrec, hdr->node_info[u].split_nrec, hdr->node_info[u].merge_nrec);
    } /* end for */

done:
    if (hdr && H5B2__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_PROTECT, FAIL, "unable to release v2 B-tree header");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__hdr_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__int_debug
 *
 * Purpose:	Prints debugging info about a B-tree internal node
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__int_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5B2_class_t *type,
                haddr_t hdr_addr, unsigned nrec, unsigned depth, haddr_t H5_ATTR_NDEBUG_UNUSED obj_addr)
{
    H5B2_hdr_t      *hdr      = NULL;     /* B-tree header */
    H5B2_internal_t *internal = NULL;     /* B-tree internal node */
    H5B2_node_ptr_t  node_ptr;            /* Fake node pointer for protect */
    unsigned         u;                   /* Local index variable */
    char             temp_str[128];       /* Temporary string, for formatting */
    herr_t           ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(type);
    assert(H5_addr_defined(hdr_addr));
    assert(H5_addr_defined(obj_addr));
    assert(nrec > 0);

    /* Load the B-tree header */
    if (NULL == (hdr = H5B2__hdr_protect(f, hdr_addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTLOAD, FAIL, "unable to load v2 B-tree header");

    /* Set file pointer for this B-tree operation */
    hdr->f = f;

    /*
     * Load the B-tree internal node
     */
    H5_CHECK_OVERFLOW(depth, unsigned, uint16_t);
    node_ptr.addr = addr;
    H5_CHECKED_ASSIGN(node_ptr.node_nrec, uint16_t, nrec, unsigned);
    if (NULL == (internal = H5B2__protect_internal(hdr, NULL, &node_ptr, (uint16_t)depth, false,
                                                   H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTLOAD, FAIL, "unable to load B-tree internal node");

    /* Print opening message */
    fprintf(stream, "%*sv2 B-tree Internal Node...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %s (%u)\n", indent, "", fwidth, "Tree type ID:", hdr->cls->name,
            (unsigned)hdr->cls->id);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Size of node:", (unsigned)hdr->node_size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Size of raw (disk) record:", (unsigned)hdr->rrec_size);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Dirty flag:", internal->cache_info.is_dirty ? "True" : "False");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of records in node:", internal->nrec);

    /* Print all node pointers and records */
    for (u = 0; u < internal->nrec; u++) {
        /* Print node pointer */
        snprintf(temp_str, sizeof(temp_str), "Node pointer #%u: (all/node/addr)", u);
        fprintf(stream, "%*s%-*s (%" PRIuHSIZE "/%u/%" PRIuHADDR ")\n", indent + 3, "", MAX(0, fwidth - 3),
                temp_str, internal->node_ptrs[u].all_nrec, internal->node_ptrs[u].node_nrec,
                internal->node_ptrs[u].addr);

        /* Print record */
        snprintf(temp_str, sizeof(temp_str), "Record #%u:", u);
        fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), temp_str);
        assert(H5B2_INT_NREC(internal, hdr, u));
        (void)(type->debug)(stream, indent + 6, MAX(0, fwidth - 6), H5B2_INT_NREC(internal, hdr, u),
                            hdr->cb_ctx);
    } /* end for */

    /* Print final node pointer */
    snprintf(temp_str, sizeof(temp_str), "Node pointer #%u: (all/node/addr)", u);
    fprintf(stream, "%*s%-*s (%" PRIuHSIZE "/%u/%" PRIuHADDR ")\n", indent + 3, "", MAX(0, fwidth - 3),
            temp_str, internal->node_ptrs[u].all_nrec, internal->node_ptrs[u].node_nrec,
            internal->node_ptrs[u].addr);

done:
    if (hdr && H5B2__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_PROTECT, FAIL, "unable to release v2 B-tree header");
    if (internal && H5AC_unprotect(f, H5AC_BT2_INT, addr, internal, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_PROTECT, FAIL, "unable to release B-tree internal node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__int_debug() */

/*-------------------------------------------------------------------------
 * Function:	H5B2__leaf_debug
 *
 * Purpose:	Prints debugging info about a B-tree leaf node
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5B2__leaf_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, const H5B2_class_t *type,
                 haddr_t hdr_addr, unsigned nrec, haddr_t H5_ATTR_NDEBUG_UNUSED obj_addr)
{
    H5B2_hdr_t     *hdr  = NULL;         /* B-tree header */
    H5B2_leaf_t    *leaf = NULL;         /* B-tree leaf node */
    H5B2_node_ptr_t node_ptr;            /* Fake node pointer for protect */
    unsigned        u;                   /* Local index variable */
    char            temp_str[128];       /* Temporary string, for formatting */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /*
     * Check arguments.
     */
    assert(f);
    assert(H5_addr_defined(addr));
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);
    assert(type);
    assert(H5_addr_defined(hdr_addr));
    assert(H5_addr_defined(obj_addr));
    assert(nrec > 0);

    /* Load the B-tree header */
    if (NULL == (hdr = H5B2__hdr_protect(f, hdr_addr, f, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect v2 B-tree header");

    /* Set file pointer for this B-tree operation */
    hdr->f = f;

    /*
     * Load the B-tree leaf node
     */
    H5_CHECK_OVERFLOW(nrec, unsigned, uint16_t);
    node_ptr.addr = addr;
    H5_CHECKED_ASSIGN(node_ptr.node_nrec, uint16_t, nrec, unsigned);
    if (NULL == (leaf = H5B2__protect_leaf(hdr, NULL, &node_ptr, false, H5AC__READ_ONLY_FLAG)))
        HGOTO_ERROR(H5E_BTREE, H5E_CANTPROTECT, FAIL, "unable to protect B-tree leaf node");

    /* Print opening message */
    fprintf(stream, "%*sv2 B-tree Leaf Node...\n", indent, "");

    /*
     * Print the values.
     */
    fprintf(stream, "%*s%-*s %s (%u)\n", indent, "", fwidth, "Tree type ID:", hdr->cls->name,
            (unsigned)hdr->cls->id);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Size of node:", (unsigned)hdr->node_size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Size of raw (disk) record:", (unsigned)hdr->rrec_size);
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Dirty flag:", leaf->cache_info.is_dirty ? "True" : "False");
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "Number of records in node:", leaf->nrec);

    /* Print all node pointers and records */
    for (u = 0; u < leaf->nrec; u++) {
        /* Print record */
        snprintf(temp_str, sizeof(temp_str), "Record #%u:", u);
        fprintf(stream, "%*s%-*s\n", indent + 3, "", MAX(0, fwidth - 3), temp_str);
        assert(H5B2_LEAF_NREC(leaf, hdr, u));
        (void)(type->debug)(stream, indent + 6, MAX(0, fwidth - 6), H5B2_LEAF_NREC(leaf, hdr, u),
                            hdr->cb_ctx);
    } /* end for */

done:
    if (hdr && H5B2__hdr_unprotect(hdr, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_PROTECT, FAIL, "unable to release B-tree header");
    if (leaf && H5AC_unprotect(f, H5AC_BT2_LEAF, addr, leaf, H5AC__NO_FLAGS_SET) < 0)
        HDONE_ERROR(H5E_BTREE, H5E_PROTECT, FAIL, "unable to release B-tree leaf node");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5B2__leaf_debug() */
