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
 * This file contains private information about the H5RS module
 */
#ifndef H5RSprivate_H
#define H5RSprivate_H

/**************************************/
/* Public headers needed by this file */
/**************************************/

/***************************************/
/* Private headers needed by this file */
/***************************************/
#include "H5private.h"

/************/
/* Typedefs */
/************/

/* Typedef for reference counted string (defined in H5RS.c) */
typedef struct H5RS_str_t H5RS_str_t;

/**********/
/* Macros */
/**********/

/********************/
/* Private routines */
/********************/
H5_DLL H5RS_str_t *H5RS_create(const char *s);
H5_DLL H5RS_str_t *H5RS_wrap(const char *s);
H5_DLL herr_t      H5RS_decr(H5RS_str_t *rs);
H5_DLL herr_t      H5RS_incr(H5RS_str_t *rs);
H5_DLL H5RS_str_t *H5RS_dup(H5RS_str_t *s);
H5_DLL herr_t      H5RS_asprintf_cat(H5RS_str_t *rs, const char *fmt, ...) H5_ATTR_FORMAT(printf, 2, 3);
H5_DLL herr_t      H5RS_acat(H5RS_str_t *rs, const char *s);
H5_DLL herr_t      H5RS_ancat(H5RS_str_t *rs, const char *s, size_t len);
H5_DLL herr_t      H5RS_aputc(H5RS_str_t *rs, int c);
H5_DLL int         H5RS_cmp(const H5RS_str_t *rs1, const H5RS_str_t *rs2);
H5_DLL size_t      H5RS_len(const H5RS_str_t *rs);
H5_DLL char       *H5RS_get_str(const H5RS_str_t *rs);
H5_DLL unsigned    H5RS_get_count(const H5RS_str_t *rs);

#endif /* H5RSprivate_H */
