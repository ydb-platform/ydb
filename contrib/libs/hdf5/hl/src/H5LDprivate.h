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

#ifndef H5LDprivate_H
#define H5LDprivate_H

/* High-level library internal header file */
#include "H5HLprivate2.h"
#include "H5LDpublic.h"

/* Store information for a field in <list_of_fields> for a compound data type */
/*
 *  Note: This data structure is used by both H5LD.c and hl/tools/h5watch
 *      This declaration is repeated in tools/lib/h5tools_str.c
 */
typedef struct H5LD_memb_t {
    size_t tot_offset;
    size_t last_tsize;
    hid_t  last_tid;
    char **names;
} H5LD_memb_t;

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Note that these two private routines are called by hl/tools/h5watch.
 * Have considered the following options:
 *   1) Repeat the coding in both H5LD.c and h5watch
 *   2) Make these public routines
 *   3) Break the rule "to avoid tools calling private routines in the library"
 * #1: not good for maintenance
 * #2: these two routines are too specific to be made as public routines
 * Decide to do #3 at this point of time after some discussion.
 */
H5_HLDLL void H5LD_clean_vector(H5LD_memb_t *listv[]);
H5_HLDLL int  H5LD_construct_vector(char *fields, H5LD_memb_t *listv[], hid_t par_tid);

#ifdef __cplusplus
}
#endif

#endif /* end H5LDprivate_H */
