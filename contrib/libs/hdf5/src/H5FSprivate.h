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
 * Created:     H5FSprivate.h
 *
 * Purpose:     Private header for library accessible file free space routines.
 *
 *-------------------------------------------------------------------------
 */

#ifndef H5FSprivate_H
#define H5FSprivate_H

/* Private headers needed by this file */
#include "H5Fprivate.h"  /* File access				*/
#include "H5FLprivate.h" /* Free Lists                           */

/**************************/
/* Library Private Macros */
/**************************/

/* Flags for H5FS_section_class_t 'flags' field */
#define H5FS_CLS_GHOST_OBJ                                                                                   \
    0x01 /* Objects in this class shouldn't be                                                               \
          *      serialized to the file.                                                                     \
          */
#define H5FS_CLS_SEPAR_OBJ                                                                                   \
    0x02 /* Objects in this class shouldn't                                                                  \
          *      participate in merge operations.                                                            \
          */
#define H5FS_CLS_MERGE_SYM                                                                                   \
    0x04 /* Objects in this class only merge                                                                 \
          *      with other objects in this class.                                                           \
          */
#define H5FS_CLS_ADJUST_OK                                                                                   \
    0x08 /* Objects in this class can be merged                                                              \
          *      without requiring a can_adjust/adjust                                                       \
          *      callback pair.                                                                              \
          */

/* Flags for H5FS_add() */
#define H5FS_ADD_DESERIALIZING                                                                               \
    0x01 /* Free space is being deserialized                                                                 \
          */
#define H5FS_ADD_RETURNED_SPACE                                                                              \
    0x02 /* Section was previously allocated                                                                 \
          *      and is being returned to the                                                                \
          *      free space manager (usually                                                                 \
          *      as a result of freeing an                                                                   \
          *      object)                                                                                     \
          */
#define H5FS_ADD_SKIP_VALID                                                                                  \
    0x04 /* Don't check validity after adding                                                                \
          *      this section.  (state of the                                                                \
          *      managed sections is in flux)                                                                \
          */

#define H5FS_PAGE_END_NO_ADD                                                                                 \
    0x08 /* For "small" page fs:                                                                             \
          * Don't add section to free space:                                                                 \
          * 	when the section is at page end and                                                             \
          * 	when the section size is <= "small"                                                             \
          */

/* Flags for deserialize callback  */
#define H5FS_DESERIALIZE_NO_ADD                                                                              \
    0x01 /* Don't add section to free space                                                                  \
          *      manager after it's deserialized                                                             \
          *      (its only here for it's side-                                                               \
          *      effects).                                                                                   \
          */

/****************************/
/* Library Private Typedefs */
/****************************/

/* Free space info (forward decl - defined in H5FSpkg.h) */
typedef struct H5FS_t H5FS_t;

/* Forward declaration free space section info */
typedef struct H5FS_section_info_t H5FS_section_info_t;

/* Free space section class info */
typedef struct H5FS_section_class_t {
    /* Class variables */
    const unsigned type;        /* Type of free space section */
    size_t         serial_size; /* Size of serialized form of section */
    unsigned       flags;       /* Class flags */
    void          *cls_private; /* Class private information */

    /* Class methods */
    herr_t (*init_cls)(struct H5FS_section_class_t *,
                       void *);                        /* Routine to initialize class-specific settings */
    herr_t (*term_cls)(struct H5FS_section_class_t *); /* Routine to terminate class-specific settings */

    /* Object methods */
    herr_t (*add)(H5FS_section_info_t **, unsigned *,
                  void *); /* Routine called when section is about to be added to manager */
    herr_t (*serialize)(const struct H5FS_section_class_t *, const H5FS_section_info_t *,
                        uint8_t *); /* Routine to serialize a "live" section into a buffer */
    H5FS_section_info_t *(*deserialize)(
        const struct H5FS_section_class_t *, const uint8_t *, haddr_t, hsize_t,
        unsigned *); /* Routine to deserialize a buffer into a "live" section */
    htri_t (*can_merge)(const H5FS_section_info_t *, const H5FS_section_info_t *,
                        void *); /* Routine to determine if two nodes are mergeable */
    herr_t (*merge)(H5FS_section_info_t **, H5FS_section_info_t *, void *); /* Routine to merge two nodes */
    htri_t (*can_shrink)(const H5FS_section_info_t *,
                         void *);                     /* Routine to determine if node can shrink container */
    herr_t (*shrink)(H5FS_section_info_t **, void *); /* Routine to shrink container */
    herr_t (*free)(H5FS_section_info_t *);            /* Routine to free node */
    herr_t (*valid)(const struct H5FS_section_class_t *,
                    const H5FS_section_info_t *); /* Routine to check if a section is valid */
    H5FS_section_info_t *(*split)(H5FS_section_info_t *, hsize_t); /* Routine to create the split section */
    herr_t (*debug)(const H5FS_section_info_t *, FILE *, int,
                    int); /* Routine to dump debugging information about a section */
} H5FS_section_class_t;

/* State of section ("live" or "serialized") */
typedef enum H5FS_section_state_t {
    H5FS_SECT_LIVE,      /* Section has "live" memory references */
    H5FS_SECT_SERIALIZED /* Section is in "serialized" form */
} H5FS_section_state_t;

/* Free space section info */
struct H5FS_section_info_t {
    haddr_t              addr;  /* Offset of free space section in the address space */
    hsize_t              size;  /* Size of free space section */
    unsigned             type;  /* Type of free space section (i.e. class) */
    H5FS_section_state_t state; /* Whether the section is in "serialized" or "live" form */
};

/* Free space client IDs for identifying user of free space */
typedef enum H5FS_client_t {
    H5FS_CLIENT_FHEAP_ID = 0, /* Free space is used by fractal heap */
    H5FS_CLIENT_FILE_ID,      /* Free space is used by file */
    H5FS_NUM_CLIENT_ID        /* Number of free space client IDs (must be last)   */
} H5FS_client_t;

/* Free space creation parameters */
typedef struct H5FS_create_t {
    H5FS_client_t client;         /* Client's ID */
    unsigned      shrink_percent; /* Percent of "normal" serialized size to shrink serialized space at */
    unsigned      expand_percent; /* Percent of "normal" serialized size to expand serialized space at */
    unsigned      max_sect_addr;  /* Size of address space free sections are within (log2 of actual value) */
    hsize_t       max_sect_size;  /* Maximum size of section to track */
} H5FS_create_t;

/* Free space statistics info */
typedef struct H5FS_stat_t {
    hsize_t tot_space;         /* Total amount of space tracked              */
    hsize_t tot_sect_count;    /* Total # of sections tracked                */
    hsize_t serial_sect_count; /* # of serializable sections tracked         */
    hsize_t ghost_sect_count;  /* # of un-serializable sections tracked      */
    haddr_t addr;              /* Address of free space header on disk       */
    hsize_t hdr_size;          /* Size of the free-space header on disk      */
    haddr_t sect_addr;         /* Address of the section info in the file    */
    hsize_t alloc_sect_size;   /* Allocated size of the section info in the file */
    hsize_t sect_size;         /* Size of the section info in the file       */
} H5FS_stat_t;

/* Typedef for iteration operations */
typedef herr_t (*H5FS_operator_t)(H5FS_section_info_t *sect, void *operator_data /*in,out*/);

/*****************************/
/* Library-private Variables */
/*****************************/

/* Declare a free list to manage the H5FS_section_class_t sequence information */
H5FL_SEQ_EXTERN(H5FS_section_class_t);

/***************************************/
/* Library-private Function Prototypes */
/***************************************/

/* Free space manager routines */
H5_DLL H5FS_t *H5FS_create(H5F_t *f, haddr_t *fs_addr, const H5FS_create_t *fs_create, uint16_t nclasses,
                           const H5FS_section_class_t *classes[], void *cls_init_udata, hsize_t alignment,
                           hsize_t threshold);
H5_DLL H5FS_t *H5FS_open(H5F_t *f, haddr_t fs_addr, uint16_t nclasses, const H5FS_section_class_t *classes[],
                         void *cls_init_udata, hsize_t alignment, hsize_t threshold);
H5_DLL herr_t  H5FS_size(const H5FS_t *fspace, hsize_t *meta_size);
H5_DLL herr_t  H5FS_delete(H5F_t *f, haddr_t fs_addr);
H5_DLL herr_t  H5FS_close(H5F_t *f, H5FS_t *fspace);
H5_DLL herr_t  H5FS_alloc_hdr(H5F_t *f, H5FS_t *fspace, haddr_t *fs_addr);
H5_DLL herr_t  H5FS_alloc_sect(H5F_t *f, H5FS_t *fspace);
H5_DLL herr_t  H5FS_free(H5F_t *f, H5FS_t *fspace, bool free_file_space);

/* Free space section routines */
H5_DLL herr_t H5FS_sect_add(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *node, unsigned flags,
                            void *op_data);
H5_DLL htri_t H5FS_sect_try_merge(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect, unsigned flags,
                                  void *op_data);
H5_DLL htri_t H5FS_sect_try_extend(H5F_t *f, H5FS_t *fspace, haddr_t addr, hsize_t size,
                                   hsize_t extra_requested, unsigned flags, void *op_data);
H5_DLL herr_t H5FS_sect_remove(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *node);
H5_DLL htri_t H5FS_sect_find(H5F_t *f, H5FS_t *fspace, hsize_t request, H5FS_section_info_t **node);
H5_DLL herr_t H5FS_sect_iterate(H5F_t *f, H5FS_t *fspace, H5FS_operator_t op, void *op_data);
H5_DLL herr_t H5FS_sect_stats(const H5FS_t *fspace, hsize_t *tot_space, hsize_t *nsects);
H5_DLL herr_t H5FS_sect_change_class(H5F_t *f, H5FS_t *fspace, H5FS_section_info_t *sect, uint16_t new_class);
H5_DLL htri_t H5FS_sect_try_shrink_eoa(H5F_t *f, H5FS_t *fspace, void *op_data);

/* Statistics routine */
H5_DLL herr_t H5FS_stat_info(const H5F_t *f, const H5FS_t *frsp, H5FS_stat_t *stats);
H5_DLL herr_t H5FS_get_sect_count(const H5FS_t *frsp, hsize_t *tot_sect_count);

/* free space manager settling routines */
H5_DLL herr_t H5FS_vfd_alloc_hdr_and_section_info_if_needed(H5F_t *f, H5FS_t *fspace, haddr_t *fs_addr_ptr);

/* Debugging routines for dumping file structures */
H5_DLL herr_t H5FS_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth);
H5_DLL herr_t H5FS_sects_debug(H5F_t *f, haddr_t addr, FILE *stream, int indent, int fwidth, haddr_t fs_addr,
                               haddr_t client_addr);
H5_DLL herr_t H5FS_sect_debug(const H5FS_t *fspace, const H5FS_section_info_t *sect, FILE *stream, int indent,
                              int fwidth);

#endif /* H5FSprivate_H */
