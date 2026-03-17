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
 * Purpose:     This file contains declarations which are visible only within
 *              the H5MF package.  Source files outside the H5MF package should
 *              include H5MFprivate.h instead.
 */
#if !(defined H5MF_FRIEND || defined H5MF_MODULE)
#error "Do not include this file outside the H5MF package!"
#endif

#ifndef H5MFpkg_H
#define H5MFpkg_H

/* Get package's private header */
#include "H5MFprivate.h"

/* Other private headers needed by this file */
#include "H5FSprivate.h" /* File free space                      */

/**************************/
/* Package Private Macros */
/**************************/

/* Define this to display information about file allocations */
/* #define H5MF_ALLOC_DEBUG */

/* Define this to display more information about file allocations */
/* #define H5MF_ALLOC_DEBUG_MORE */

/* Define this to display more information about block aggregator actions */
/* #define H5MF_AGGR_DEBUG */

/* Define this to dump free space tracker contents after they've been modified */
/* #define H5MF_ALLOC_DEBUG_DUMP */

/* Free-space section types for file */
/* (values stored in free space data structures in file) */
/* For non-paged aggregation: section is a range of actual bytes in file */
#define H5MF_FSPACE_SECT_SIMPLE 0
/* For paged aggregation: "small" meta/raw data section which is < fsp_size) */
#define H5MF_FSPACE_SECT_SMALL 1
/* For paged aggregation: "large" Section which is >= fsp_size) */
#define H5MF_FSPACE_SECT_LARGE 2

/* Get section class type based on size */
#define H5MF_SECT_CLASS_TYPE(F, S)                                                                           \
    ((H5F_PAGED_AGGR(F))                                                                                     \
         ? ((S >= (F)->shared->fs_page_size) ? H5MF_FSPACE_SECT_LARGE : H5MF_FSPACE_SECT_SMALL)              \
         : H5MF_FSPACE_SECT_SIMPLE)

/* Get section class cls */
#define H5MF_SECT_CLS_TYPE(F, S)                                                                             \
    ((H5F_PAGED_AGGR(F))                                                                                     \
         ? ((S >= (F)->shared->fs_page_size) ? H5MF_FSPACE_SECT_CLS_LARGE : H5MF_FSPACE_SECT_CLS_SMALL)      \
         : H5MF_FSPACE_SECT_CLS_SIMPLE)

/* Calculate the mis-aligned fragment */
#define H5MF_EOA_MISALIGN(F, E, A, FR)                                                                       \
    do {                                                                                                     \
        hsize_t m;                                                                                           \
                                                                                                             \
        if (H5_addr_gt((E), 0) && ((m) = ((E) + H5F_BASE_ADDR(F)) % (A)))                                    \
            (FR) = (A)-m;                                                                                    \
        else                                                                                                 \
            (FR) = 0;                                                                                        \
    } while (0)

/****************************/
/* Package Private Typedefs */
/****************************/

/* File free space section info */
typedef struct H5MF_free_section_t {
    H5FS_section_info_t sect_info; /* Free space section information (must be first in struct) */
#ifdef NOT_YET
    union {
        struct {
            H5HF_indirect_t *parent;    /* Indirect block parent for free section's direct block */
            unsigned         par_entry; /* Entry of free section's direct block in parent indirect block */
        } single;
        struct {
            struct H5HF_free_section_t *under;       /* Pointer to indirect block underlying row section */
            unsigned                    row;         /* Row for range of blocks */
            unsigned                    col;         /* Column for range of blocks */
            unsigned                    num_entries; /* Number of entries covered */

            /* Fields that aren't stored */
            bool checked_out; /* Flag to indicate that a row section is temporarily out of the free space
                                    manager */
        } row;
        struct {
            /* Holds either a pointer to an indirect block (if its "live") or
             *  the block offset of it's indirect block (if its "serialized")
             *  (This allows the indirect block that the section is within to
             *          be compared with other sections, whether it's serialized
             *          or not)
             */
            union {
                H5HF_indirect_t *iblock;     /* Indirect block for free section */
                hsize_t          iblock_off; /* Indirect block offset in "heap space" */
            } u;
            unsigned row;         /* Row for range of blocks */
            unsigned col;         /* Column for range of blocks */
            unsigned num_entries; /* Number of entries covered */

            /* Fields that aren't stored */
            struct H5HF_free_section_t *parent;    /* Pointer to "parent" indirect section */
            unsigned                    par_entry; /* Entry within parent indirect section */
            hsize_t                     span_size; /* Size of space tracked, in "heap space" */
            unsigned iblock_entries; /* Number of entries in indirect block where section is located */
            unsigned rc;             /* Reference count of outstanding row & child indirect sections */
            unsigned dir_nrows;      /* Number of direct rows in section */
            struct H5HF_free_section_t **dir_rows;    /* Array of pointers to outstanding row sections */
            unsigned                     indir_nents; /* Number of indirect entries in section */
            struct H5HF_free_section_t *
                *indir_ents; /* Array of pointers to outstanding child indirect sections */
        } indirect;
    } u;
#endif /* NOT_YET */
} H5MF_free_section_t;

/* Type of "container shrink" operation to perform */
typedef enum {
    H5MF_SHRINK_EOA,              /* Section should shrink the EOA value */
    H5MF_SHRINK_AGGR_ABSORB_SECT, /* Section should merge into the aggregator block */
    H5MF_SHRINK_SECT_ABSORB_AGGR  /* Aggregator block should merge into the section */
} H5MF_shrink_type_t;

/* User data for free space manager section callbacks */
typedef struct H5MF_sect_ud_t {
    /* Down */
    H5F_t     *f;                     /* Pointer to file to operate on */
    H5FD_mem_t alloc_type;            /* Type of memory being allocated */
    bool       allow_sect_absorb;     /* Whether sections are allowed to absorb a block aggregator */
    bool       allow_eoa_shrink_only; /* Whether shrinking eoa is allowed only for the section */

    /* Up */
    H5MF_shrink_type_t shrink; /* Type of shrink operation to perform */
    H5F_blk_aggr_t    *aggr;   /* Aggregator block to operate on */
} H5MF_sect_ud_t;

/* Information about the current free-space manager to use */
typedef struct H5MF_fs_t {
    H5F_fs_state_t *fs_state;
    haddr_t        *fs_addr;
    H5FS_t        **fs_man;
    hsize_t         align_thres; /* Threshold for alignment              */
    hsize_t         alignment;   /* Alignment                            */
} H5MF_fs_t;

/*****************************/
/* Package Private Variables */
/*****************************/

/* H5MF single section inherits serializable properties from H5FS_section_class_t */
H5_DLLVAR const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_SIMPLE[1];
H5_DLLVAR const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_SMALL[1];
H5_DLLVAR const H5FS_section_class_t H5MF_FSPACE_SECT_CLS_LARGE[1];

/******************************/
/* Package Private Prototypes */
/******************************/

/* Allocator routines */
H5_DLL herr_t H5MF__open_fstype(H5F_t *f, H5F_mem_page_t type);
H5_DLL herr_t H5MF__start_fstype(H5F_t *f, H5F_mem_page_t type);
H5_DLL htri_t H5MF__find_sect(H5F_t *f, H5FD_mem_t alloc_type, hsize_t size, H5FS_t *fspace, haddr_t *addr);
H5_DLL herr_t H5MF__add_sect(H5F_t *f, H5FD_mem_t alloc_type, H5FS_t *fspace, H5MF_free_section_t *node);
H5_DLL void   H5MF__alloc_to_fs_type(H5F_shared_t *f_sh, H5FD_mem_t alloc_type, hsize_t size,
                                     H5F_mem_page_t *fs_type);

/* 'simple/small/large' section routines */
H5_DLL H5MF_free_section_t *H5MF__sect_new(unsigned ctype, haddr_t sect_off, hsize_t sect_size);
H5_DLL herr_t               H5MF__sect_free(H5FS_section_info_t *sect);

/* Block aggregator routines */
H5_DLL htri_t H5MF__aggr_try_extend(H5F_t *f, H5F_blk_aggr_t *aggr, H5FD_mem_t type, haddr_t abs_blk_end,
                                    hsize_t extra_requested);
H5_DLL htri_t H5MF__aggrs_try_shrink_eoa(H5F_t *f);
H5_DLL htri_t H5MF__aggr_can_absorb(const H5F_t *f, const H5F_blk_aggr_t *aggr,
                                    const H5MF_free_section_t *sect, H5MF_shrink_type_t *shrink);
H5_DLL herr_t H5MF__aggr_absorb(const H5F_t *f, H5F_blk_aggr_t *aggr, H5MF_free_section_t *sect,
                                bool allow_sect_absorb);
H5_DLL herr_t H5MF__aggr_query(const H5F_t *f, const H5F_blk_aggr_t *aggr, haddr_t *addr, hsize_t *size);

/* Debugging routines */
#ifdef H5MF_ALLOC_DEBUG_DUMP
H5_DLL herr_t H5MF__sects_dump(H5F_t *f, FILE *stream);
#endif /* H5MF_ALLOC_DEBUG_DUMP */

/* Testing routines */
#ifdef H5MF_TESTING
#endif /* H5MF_TESTING */

#endif /* H5MFpkg_H */
