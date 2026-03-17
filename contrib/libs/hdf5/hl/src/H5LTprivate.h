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

#ifndef H5LTprivate_H
#define H5LTprivate_H

/* High-level library internal header file */
#include "H5HLprivate2.h"

/* public LT prototypes			*/
#include "H5LTpublic.h"

H5_HLDLL herr_t H5LT_get_attribute_disk(hid_t obj_id, const char *attr_name, void *data);
H5_HLDLL herr_t H5LT_set_attribute_numerical(hid_t loc_id, const char *obj_name, const char *attr_name,
                                             size_t size, hid_t type_id, const void *data);
H5_HLDLL herr_t H5LT_set_attribute_string(hid_t dset_id, const char *name, const char *buf);
H5_HLDLL char  *H5LT_dtype_to_text(hid_t dtype, char *dt_str, H5LT_lang_t lang, size_t *slen,
                                   bool no_user_buf);
H5_HLDLL hid_t  H5LTyyparse(void);

#endif
