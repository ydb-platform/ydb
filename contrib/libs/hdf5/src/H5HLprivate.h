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
 * Created:         H5HLprivate.h
 *
 * Purpose:         Private declarations for the H5HL (local heap) package.
 *
 *-------------------------------------------------------------------------
 */
#ifndef H5HLprivate_H
#define H5HLprivate_H

/* Private headers needed by this file. */
#include "H5private.h"   /* Generic Functions                */
#include "H5ACprivate.h" /* Metadata cache                   */
#include "H5Fprivate.h"  /* File access                      */

/*
 * Feature: Define H5HL_DEBUG on the compiler command line if you want to
 *          enable diagnostic messages from this layer.
 */
#ifdef NDEBUG
#undef H5HL_DEBUG
#endif

#define H5HL_ALIGN(X) ((((unsigned)X) + 7) & (unsigned)(~0x07)) /* align on 8-byte boundary   */

#define H5HL_SIZEOF_FREE(F)                                                                                  \
    H5HL_ALIGN(H5F_SIZEOF_SIZE(F) + /* ptr to next free block   */                                           \
               H5F_SIZEOF_SIZE(F))  /* size of this free block  */

/****************************/
/* Library Private Typedefs */
/****************************/

/* Typedef for local heap in memory (defined in H5HLpkg.h) */
typedef struct H5HL_t H5HL_t;

/*
 * Library prototypes
 */
H5_DLL herr_t  H5HL_create(H5F_t *f, size_t size_hint, haddr_t *addr /*out*/);
H5_DLL herr_t  H5HL_delete(H5F_t *f, haddr_t addr);
H5_DLL herr_t  H5HL_get_size(H5F_t *f, haddr_t addr, size_t *size);
H5_DLL herr_t  H5HL_heapsize(H5F_t *f, haddr_t addr, hsize_t *heap_size);
H5_DLL herr_t  H5HL_insert(H5F_t *f, H5HL_t *heap, size_t size, const void *buf, size_t *offset);
H5_DLL void   *H5HL_offset_into(const H5HL_t *heap, size_t offset);
H5_DLL H5HL_t *H5HL_protect(H5F_t *f, haddr_t addr, unsigned flags);
H5_DLL herr_t  H5HL_remove(H5F_t *f, H5HL_t *heap, size_t offset, size_t size);
H5_DLL herr_t  H5HL_unprotect(H5HL_t *heap);

/* Debugging routines for dumping file structures */
H5_DLL herr_t H5HL_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth);

#endif /* H5HLprivate_H */
