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
 * Purpose:	File object debugging functions.
 */

#include "H5Fmodule.h" /* This source code file is part of the H5F module */
#define H5G_FRIEND     /*suppress error about including H5Gpkg   */

#include "H5private.h"  /* Generic Functions			*/
#include "H5Eprivate.h" /* Error handling		        */
#include "H5Fpkg.h"     /* File access				*/
#include "H5Gpkg.h"     /* Groups		  		*/
#include "H5Iprivate.h" /* ID Functions		                */
#include "H5Pprivate.h" /* Property lists			*/

/*-------------------------------------------------------------------------
 * Function:	H5F_debug
 *
 * Purpose:	Prints a file header to the specified stream.  Each line
 *		is indented and the field name occupies the specified width
 *		number of characters.
 *
 * Return:	Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5F_debug(H5F_t *f, FILE *stream, int indent, int fwidth)
{
    H5P_genplist_t *plist;               /* File creation property list */
    hsize_t         userblock_size;      /* Userblock size */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* check args */
    assert(f);
    assert(stream);
    assert(indent >= 0);
    assert(fwidth >= 0);

    /* Get property list */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(f->shared->fcpl_id)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Retrieve file creation properties */
    if (H5P_get(plist, H5F_CRT_USER_BLOCK_NAME, &userblock_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get userblock size");

    /* debug */
    fprintf(stream, "%*sFile Super Block...\n", indent, "");

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth, "File name (as opened):", H5F_OPEN_NAME(f));
    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "File name (after resolving symlinks):", H5F_ACTUAL_NAME(f));
    fprintf(stream, "%*s%-*s 0x%08x\n", indent, "", fwidth, "File access flags", f->shared->flags);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth, "File open reference count:", f->shared->nrefs);
    fprintf(stream, "%*s%-*s %" PRIuHADDR " (abs)\n", indent, "", fwidth,
            "Address of super block:", f->shared->sblock->base_addr);
    fprintf(stream, "%*s%-*s %" PRIuHSIZE " bytes\n", indent, "", fwidth,
            "Size of userblock:", userblock_size);

    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Superblock version number:", f->shared->sblock->super_vers);

    /* Hard-wired versions */
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Free list version number:", (unsigned)HDF5_FREESPACE_VERSION);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Root group symbol table entry version number:", (unsigned)HDF5_OBJECTDIR_VERSION);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Shared header version number:", (unsigned)HDF5_SHAREDHEADER_VERSION);

    fprintf(stream, "%*s%-*s %u bytes\n", indent, "", fwidth,
            "Size of file offsets (haddr_t type):", (unsigned)f->shared->sizeof_addr);
    fprintf(stream, "%*s%-*s %u bytes\n", indent, "", fwidth,
            "Size of file lengths (hsize_t type):", (unsigned)f->shared->sizeof_size);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Symbol table leaf node 1/2 rank:", f->shared->sblock->sym_leaf_k);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Symbol table internal node 1/2 rank:", f->shared->sblock->btree_k[H5B_SNODE_ID]);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Indexed storage internal node 1/2 rank:", f->shared->sblock->btree_k[H5B_CHUNK_ID]);
    fprintf(stream, "%*s%-*s 0x%02x\n", indent, "", fwidth,
            "File status flags:", (unsigned)(f->shared->sblock->status_flags));
    fprintf(stream, "%*s%-*s %" PRIuHADDR " (rel)\n", indent, "", fwidth,
            "Superblock extension address:", f->shared->sblock->ext_addr);
    fprintf(stream, "%*s%-*s %" PRIuHADDR " (rel)\n", indent, "", fwidth,
            "Shared object header message table address:", f->shared->sohm_addr);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Shared object header message version number:", (unsigned)f->shared->sohm_vers);
    fprintf(stream, "%*s%-*s %u\n", indent, "", fwidth,
            "Number of shared object header message indexes:", (unsigned)f->shared->sohm_nindexes);

    fprintf(stream, "%*s%-*s %" PRIuHADDR " (rel)\n", indent, "", fwidth,
            "Address of driver information block:", f->shared->sblock->driver_addr);

    fprintf(stream, "%*s%-*s %s\n", indent, "", fwidth,
            "Root group symbol table entry:", f->shared->root_grp ? "" : "(none)");
    if (f->shared->root_grp) {
        if (f->shared->sblock->root_ent) /* Use real root group symbol table entry */
            H5G__ent_debug(f->shared->sblock->root_ent, stream, indent + 3, MAX(0, fwidth - 3), NULL);
        else {
            H5O_loc_t  *root_oloc; /* Root object location */
            H5G_entry_t root_ent;  /* Constructed root symbol table entry */

            /* Reset the root group entry */
            H5G__ent_reset(&root_ent);

            /* Build up a simulated root group symbol table entry */
            root_oloc = H5G_oloc(f->shared->root_grp);
            assert(root_oloc);
            root_ent.type   = H5G_NOTHING_CACHED;
            root_ent.header = root_oloc->addr;

            /* Display root group symbol table entry info */
            H5G__ent_debug(&root_ent, stream, indent + 3, MAX(0, fwidth - 3), NULL);
        } /* end else */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5F_debug() */
