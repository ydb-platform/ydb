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

#include "H5Gmodule.h" /* This source code file is part of the H5G module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions			*/
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FLprivate.h" /* Free Lists                           */
#include "H5Gpkg.h"      /* Groups		  		*/
#include "H5HLprivate.h" /* Local Heaps				*/
#include "H5MMprivate.h" /* Memory management			*/

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
 * Function:    H5G__ent_decode_vec
 *
 * Purpose:     Same as H5G_ent_decode() except it does it for an array of
 *              symbol table entries.
 *
 * Return:      Success:        Non-negative, with *pp pointing to the first byte
 *                              after the last symbol.
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__ent_decode_vec(const H5F_t *f, const uint8_t **pp, const uint8_t *p_end, H5G_entry_t *ent, unsigned n)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(pp);
    assert(ent);

    /* decode entries */
    for (u = 0; u < n; u++) {
        if (*pp > p_end)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "ran off the end of the image buffer");
        if (H5G_ent_decode(f, pp, ent + u, p_end) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTDECODE, FAIL, "can't decode");
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__ent_decode_vec() */

/*-------------------------------------------------------------------------
 * Function:    H5G_ent_decode
 *
 * Purpose:     Decodes a symbol table entry pointed to by `*pp'.
 *
 * Return:      Success:        Non-negative with *pp pointing to the first byte
 *                              following the symbol table entry.
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_ent_decode(const H5F_t *f, const uint8_t **pp, H5G_entry_t *ent, const uint8_t *p_end)
{
    const uint8_t *p_ret = *pp;
    uint32_t       tmp;
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(f);
    assert(pp);
    assert(ent);

    if (H5_IS_BUFFER_OVERFLOW(*pp, ent->name_off, p_end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "image pointer is out of bounds");

    /* decode header */
    H5F_DECODE_LENGTH(f, *pp, ent->name_off);

    if (H5_IS_BUFFER_OVERFLOW(*pp, H5F_SIZEOF_ADDR(f) + sizeof(uint32_t), p_end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "image pointer is out of bounds");

    H5F_addr_decode(f, pp, &(ent->header));
    UINT32DECODE(*pp, tmp);
    *pp += 4; /*reserved*/

    if (H5_IS_BUFFER_OVERFLOW(*pp, 1, p_end))
        HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "image pointer is out of bounds");

    ent->type = (H5G_cache_type_t)tmp;

    /* decode scratch-pad */
    switch (ent->type) {
        case H5G_NOTHING_CACHED:
            break;

        case H5G_CACHED_STAB:
            assert(2 * H5F_SIZEOF_ADDR(f) <= H5G_SIZEOF_SCRATCH);
            if (H5_IS_BUFFER_OVERFLOW(*pp, H5F_SIZEOF_ADDR(f) * 2, p_end))
                HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "image pointer is out of bounds");
            H5F_addr_decode(f, pp, &(ent->cache.stab.btree_addr));
            H5F_addr_decode(f, pp, &(ent->cache.stab.heap_addr));
            break;

        case H5G_CACHED_SLINK:
            if (H5_IS_BUFFER_OVERFLOW(*pp, sizeof(uint32_t), p_end))
                HGOTO_ERROR(H5E_FILE, H5E_OVERFLOW, FAIL, "image pointer is out of bounds");
            UINT32DECODE(*pp, ent->cache.slink.lval_offset);
            break;

        case H5G_CACHED_ERROR:
        case H5G_NCACHED:
        default:
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unknown symbol table entry cache type");
    } /* end switch */

    *pp = p_ret + H5G_SIZEOF_ENTRY_FILE(f);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_ent_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5G__ent_encode_vec
 *
 * Purpose:     Same as H5G_ent_encode() except it does it for an array of
 *              symbol table entries.
 *
 * Return:      Success:        Non-negative, with *pp pointing to the first byte
 *                              after the last symbol.
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__ent_encode_vec(const H5F_t *f, uint8_t **pp, const H5G_entry_t *ent, unsigned n)
{
    unsigned u;                   /* Local index variable */
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(pp);
    assert(ent);

    /* encode entries */
    for (u = 0; u < n; u++)
        if (H5G_ent_encode(f, pp, ent + u) < 0)
            HGOTO_ERROR(H5E_SYM, H5E_CANTENCODE, FAIL, "can't encode");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5G__ent_encode_vec() */

/*-------------------------------------------------------------------------
 * Function:    H5G_ent_encode
 *
 * Purpose:     Encodes the specified symbol table entry into the buffer
 *              pointed to by *pp.
 *
 * Return:      Success:        Non-negative, with *pp pointing to the first byte
 *                              after the symbol table entry.
 *
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G_ent_encode(const H5F_t *f, uint8_t **pp, const H5G_entry_t *ent)
{
    uint8_t *p_ret     = *pp + H5G_SIZEOF_ENTRY_FILE(f);
    herr_t   ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check arguments */
    assert(f);
    assert(pp);

    /* Check for actual entry to encode */
    if (ent) {
        /* encode header */
        H5F_ENCODE_LENGTH(f, *pp, ent->name_off);
        H5F_addr_encode(f, pp, ent->header);
        UINT32ENCODE(*pp, ent->type);
        UINT32ENCODE(*pp, 0); /*reserved*/

        /* encode scratch-pad */
        switch (ent->type) {
            case H5G_NOTHING_CACHED:
                break;

            case H5G_CACHED_STAB:
                assert(2 * H5F_SIZEOF_ADDR(f) <= H5G_SIZEOF_SCRATCH);
                H5F_addr_encode(f, pp, ent->cache.stab.btree_addr);
                H5F_addr_encode(f, pp, ent->cache.stab.heap_addr);
                break;

            case H5G_CACHED_SLINK:
                UINT32ENCODE(*pp, ent->cache.slink.lval_offset);
                break;

            case H5G_CACHED_ERROR:
            case H5G_NCACHED:
            default:
                HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unknown symbol table entry cache type");
        } /* end switch */
    }     /* end if */
    else {
        H5F_ENCODE_LENGTH(f, *pp, 0);
        H5F_addr_encode(f, pp, HADDR_UNDEF);
        UINT32ENCODE(*pp, H5G_NOTHING_CACHED);
        UINT32ENCODE(*pp, 0); /*reserved*/
    }                         /* end else */

    /* fill with zero */
    if (*pp < p_ret)
        memset(*pp, 0, (size_t)(p_ret - *pp));
    *pp = p_ret;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G_ent_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5G__ent_copy
 *
 * Purpose:     Do a deep copy of symbol table entries
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 * Notes:       'depth' parameter determines how much of the group entry
 *              structure we want to copy.  The values are:
 *                  H5_COPY_SHALLOW - Copy all the fields from the source
 *                      to the destination, including the user path and
 *                      canonical path. (Destination "takes ownership" of
 *                      user and canonical paths)
 *                  H5_COPY_DEEP - Copy all the fields from the source to
 *                      the destination, deep copying the user and canonical
 *                      paths.
 *
 *-------------------------------------------------------------------------
 */
void
H5G__ent_copy(H5G_entry_t *dst, H5G_entry_t *src, H5_copy_depth_t depth)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(src);
    assert(dst);
    assert(depth == H5_COPY_SHALLOW || depth == H5_COPY_DEEP);

    /* Copy the top level information */
    H5MM_memcpy(dst, src, sizeof(H5G_entry_t));

    /* Deep copy the names */
    if (depth == H5_COPY_DEEP) {
        /* Nothing currently */
    }
    else if (depth == H5_COPY_SHALLOW) {
        H5G__ent_reset(src);
    } /* end if */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5G__ent_copy() */

/*-------------------------------------------------------------------------
 * Function:	H5G__ent_reset
 *
 * Purpose:	Reset a symbol table entry to an empty state
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
void
H5G__ent_reset(H5G_entry_t *ent)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Check arguments */
    assert(ent);

    /* Clear the symbol table entry to an empty state */
    memset(ent, 0, sizeof(H5G_entry_t));
    ent->header = HADDR_UNDEF;

    FUNC_LEAVE_NOAPI_VOID
} /* end H5G__ent_reset() */

/*-------------------------------------------------------------------------
 * Function:    H5G__ent_convert
 *
 * Purpose:     Convert a link to a symbol table entry
 *
 * Return:	Success:	Non-negative
 *		Failure:	Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__ent_convert(H5F_t *f, H5HL_t *heap, const char *name, const H5O_link_t *lnk, H5O_type_t obj_type,
                 const void *crt_info, H5G_entry_t *ent)
{
    size_t name_offset;         /* Offset of name in heap */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* check arguments */
    assert(f);
    assert(heap);
    assert(name);
    assert(lnk);

    /* Reset the new entry */
    H5G__ent_reset(ent);

    /* Add the new name to the heap */
    if (H5HL_insert(f, heap, strlen(name) + 1, name, &name_offset) < 0)
        HGOTO_ERROR(H5E_SYM, H5E_CANTINSERT, FAIL, "unable to insert symbol name into heap");
    ent->name_off = name_offset;

    /* Build correct information for symbol table entry based on link type */
    switch (lnk->type) {
        case H5L_TYPE_HARD:
            if (obj_type == H5O_TYPE_GROUP) {
                const H5G_obj_create_t *gcrt_info = (const H5G_obj_create_t *)crt_info;

                ent->type = gcrt_info->cache_type;
                if (ent->type != H5G_NOTHING_CACHED)
                    ent->cache = gcrt_info->cache;
#ifndef NDEBUG
                else {
                    /* Make sure there is no stab message in the target object
                     */
                    H5O_loc_t targ_oloc;   /* Location of link target */
                    htri_t    stab_exists; /* Whether the target symbol table exists */

                    /* Build target object location */
                    if (H5O_loc_reset(&targ_oloc) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "unable to initialize target location");
                    targ_oloc.file = f;
                    targ_oloc.addr = lnk->u.hard.addr;

                    /* Check if a symbol table message exists */
                    if ((stab_exists = H5O_msg_exists(&targ_oloc, H5O_STAB_ID)) < 0)
                        HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to check for STAB message");

                    assert(!stab_exists);
                } /* end else */
#endif            /* NDEBUG */
            }     /* end if */
            else if (obj_type == H5O_TYPE_UNKNOWN) {
                /* Try to retrieve symbol table information for caching */
                H5O_loc_t  targ_oloc;   /* Location of link target */
                H5O_t     *oh;          /* Link target object header */
                H5O_stab_t stab;        /* Link target symbol table */
                htri_t     stab_exists; /* Whether the target symbol table exists */

                /* Build target object location */
                if (H5O_loc_reset(&targ_oloc) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTRESET, FAIL, "unable to initialize target location");
                targ_oloc.file = f;
                targ_oloc.addr = lnk->u.hard.addr;

                /* Get the object header */
                if (NULL == (oh = H5O_protect(&targ_oloc, H5AC__READ_ONLY_FLAG, false)))
                    HGOTO_ERROR(H5E_SYM, H5E_CANTPROTECT, FAIL, "unable to protect target object header");

                /* Check if a symbol table message exists */
                if ((stab_exists = H5O_msg_exists_oh(oh, H5O_STAB_ID)) < 0) {
                    if (H5O_unprotect(&targ_oloc, oh, H5AC__NO_FLAGS_SET) < 0)
                        HERROR(H5E_SYM, H5E_CANTUNPROTECT, "unable to release object header");
                    HGOTO_ERROR(H5E_SYM, H5E_NOTFOUND, FAIL, "unable to check for STAB message");
                } /* end if */

                if (stab_exists) {
                    /* Read symbol table message */
                    if (NULL == H5O_msg_read_oh(f, oh, H5O_STAB_ID, &stab)) {
                        if (H5O_unprotect(&targ_oloc, oh, H5AC__NO_FLAGS_SET) < 0)
                            HERROR(H5E_SYM, H5E_CANTUNPROTECT, "unable to release object header");
                        HGOTO_ERROR(H5E_SYM, H5E_CANTGET, FAIL, "unable to read STAB message");
                    } /* end if */

                    /* Cache symbol table message */
                    ent->type                  = H5G_CACHED_STAB;
                    ent->cache.stab.btree_addr = stab.btree_addr;
                    ent->cache.stab.heap_addr  = stab.heap_addr;
                } /* end if */
                else
                    /* No symbol table message, don't cache anything */
                    ent->type = H5G_NOTHING_CACHED;

                if (H5O_unprotect(&targ_oloc, oh, H5AC__NO_FLAGS_SET) < 0)
                    HGOTO_ERROR(H5E_SYM, H5E_CANTUNPROTECT, FAIL, "unable to release object header");
            } /* end else */
            else
                ent->type = H5G_NOTHING_CACHED;

            ent->header = lnk->u.hard.addr;
            break;

        case H5L_TYPE_SOFT: {
            size_t lnk_offset; /* Offset to sym-link value	*/

            /* Insert link value into local heap */
            if (H5HL_insert(f, heap, strlen(lnk->u.soft.name) + 1, lnk->u.soft.name, &lnk_offset) < 0)
                HGOTO_ERROR(H5E_SYM, H5E_CANTINIT, FAIL, "unable to write link value to local heap");

            ent->type                    = H5G_CACHED_SLINK;
            ent->cache.slink.lval_offset = lnk_offset;
        } break;

        case H5L_TYPE_ERROR:
        case H5L_TYPE_EXTERNAL:
        case H5L_TYPE_MAX:
        default:
            HGOTO_ERROR(H5E_SYM, H5E_BADVALUE, FAIL, "unrecognized link type");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5G__ent_convert() */

/*-------------------------------------------------------------------------
 * Function:    H5G__ent_debug
 *
 * Purpose:     Prints debugging information about a symbol table entry.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5G__ent_debug(const H5G_entry_t *ent, FILE *stream, int indent, int fwidth, const H5HL_t *heap)
{
    const char *lval = NULL;
    int         nested_indent, nested_fwidth;

    FUNC_ENTER_PACKAGE_NOERR

    /* Calculate the indent & field width values for nested information */
    nested_indent = indent + 3;
    nested_fwidth = MAX(0, fwidth - 3);

    fprintf(stream, "%*s%-*s %lu\n", indent, "", fwidth,
            "Name offset into private heap:", (unsigned long)(ent->name_off));

    fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", indent, "", fwidth, "Object header address:", ent->header);

    fprintf(stream, "%*s%-*s ", indent, "", fwidth, "Cache info type:");
    switch (ent->type) {
        case H5G_NOTHING_CACHED:
            fprintf(stream, "Nothing Cached\n");
            break;

        case H5G_CACHED_STAB:
            fprintf(stream, "Symbol Table\n");

            fprintf(stream, "%*s%-*s\n", indent, "", fwidth, "Cached entry information:");
            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", nested_indent, "", nested_fwidth,
                    "B-tree address:", ent->cache.stab.btree_addr);

            fprintf(stream, "%*s%-*s %" PRIuHADDR "\n", nested_indent, "", nested_fwidth,
                    "Heap address:", ent->cache.stab.heap_addr);
            break;

        case H5G_CACHED_SLINK:
            fprintf(stream, "Symbolic Link\n");
            fprintf(stream, "%*s%-*s\n", indent, "", fwidth, "Cached information:");
            fprintf(stream, "%*s%-*s %lu\n", nested_indent, "", nested_fwidth,
                    "Link value offset:", (unsigned long)(ent->cache.slink.lval_offset));
            if (heap) {
                lval = (const char *)H5HL_offset_into(heap, ent->cache.slink.lval_offset);
                fprintf(stream, "%*s%-*s %s\n", nested_indent, "", nested_fwidth,
                        "Link value:", (lval == NULL) ? "" : lval);
            } /* end if */
            else
                fprintf(stream, "%*s%-*s\n", nested_indent, "", nested_fwidth,
                        "Warning: Invalid heap address given, name not displayed!");
            break;

        case H5G_CACHED_ERROR:
        case H5G_NCACHED:
        default:
            fprintf(stream, "*** Unknown symbol type %d\n", ent->type);
            break;
    } /* end switch */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5G__ent_debug() */
