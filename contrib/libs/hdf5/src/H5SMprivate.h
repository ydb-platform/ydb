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
 * Purpose: This file contains private declarations for the H5SM
 *          shared object header messages module.
 */
#ifndef H5SMprivate_H
#define H5SMprivate_H

#include "H5Oprivate.h" /* Object headers			*/
#include "H5Pprivate.h" /* Property lists			*/

/**************************/
/* Library Private Macros */
/**************************/

/* Flags for the "defer_flags" argument to H5SM_try_share
 */
#define H5SM_DEFER 0x01u /* Don't actually write shared message to index, heap; just update shared info */
#define H5SM_WAS_DEFERRED                                                                                    \
    0x02u /* Message was previously updated by a call to H5SM_try_share with H5SM_DEFER */

/****************************/
/* Library Private Typedefs */
/****************************/

/* Forward references of package typedefs */
typedef struct H5SM_master_table_t H5SM_master_table_t;

/******************************/
/* Library Private Prototypes */
/******************************/

/* Generally useful shared message routines */
H5_DLL herr_t H5SM_init(H5F_t *f, H5P_genplist_t *fc_plist, const H5O_loc_t *ext_loc);
H5_DLL htri_t H5SM_can_share(H5F_t *f, H5SM_master_table_t *table, ssize_t *sohm_index_num, unsigned type_id,
                             const void *mesg);
H5_DLL htri_t H5SM_try_share(H5F_t *f, H5O_t *open_oh, unsigned defer_flags, unsigned type_id, void *mesg,
                             unsigned *mesg_flags);
H5_DLL herr_t H5SM_delete(H5F_t *f, H5O_t *open_oh, H5O_shared_t *sh_mesg);
H5_DLL herr_t H5SM_get_info(const H5O_loc_t *ext_loc, H5P_genplist_t *fc_plist);
H5_DLL htri_t H5SM_type_shared(H5F_t *f, unsigned type_id);
H5_DLL herr_t H5SM_get_fheap_addr(H5F_t *f, unsigned type_id, haddr_t *fheap_addr);
H5_DLL herr_t H5SM_reconstitute(H5O_shared_t *sh_mesg, H5F_t *f, unsigned msg_type_id,
                                H5O_fheap_id_t heap_id);
H5_DLL herr_t H5SM_get_refcount(H5F_t *f, unsigned type_id, const H5O_shared_t *sh_mesg, hsize_t *ref_count);
H5_DLL herr_t H5SM_ih_size(H5F_t *f, hsize_t *hdr_size, H5_ih_info_t *ih_info);

/* Debugging routines */
H5_DLL herr_t H5SM_table_debug(H5F_t *f, haddr_t table_addr, FILE *stream, int indent, int fwidth,
                               unsigned table_vers, unsigned num_indexes);
H5_DLL herr_t H5SM_list_debug(H5F_t *f, haddr_t list_addr, FILE *stream, int indent, int fwidth,
                              haddr_t table_addr);

#endif /*H5SMprivate_H*/
