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
 * This file contains public declarations for the H5FD (file drivers) developer
 *      support routines.
 */

#ifndef H5FDdevelop_H
#define H5FDdevelop_H

/* Include package's public header */
#include "H5FDpublic.h"

/*****************/
/* Public Macros */
/*****************/

/* H5FD_class_t struct version */
#define H5FD_CLASS_VERSION 0x01 /* File driver struct version */

/* Map "fractal heap" header blocks to 'ohdr' type file memory, since its
 * a fair amount of work to add a new kind of file memory and they are similar
 * enough to object headers and probably too minor to deserve their own type.
 *
 * Map "fractal heap" indirect blocks to 'ohdr' type file memory, since they
 * are similar to fractal heap header blocks.
 *
 * Map "fractal heap" direct blocks to 'lheap' type file memory, since they
 * will be replacing local heaps.
 *
 * Map "fractal heap" 'huge' objects to 'draw' type file memory, since they
 * represent large objects that are directly stored in the file.
 *
 *      -QAK
 */
#define H5FD_MEM_FHEAP_HDR      H5FD_MEM_OHDR
#define H5FD_MEM_FHEAP_IBLOCK   H5FD_MEM_OHDR
#define H5FD_MEM_FHEAP_DBLOCK   H5FD_MEM_LHEAP
#define H5FD_MEM_FHEAP_HUGE_OBJ H5FD_MEM_DRAW

/* Map "free space" header blocks to 'ohdr' type file memory, since its
 * a fair amount of work to add a new kind of file memory and they are similar
 * enough to object headers and probably too minor to deserve their own type.
 *
 * Map "free space" serialized sections to 'lheap' type file memory, since they
 * are similar enough to local heap info.
 *
 *      -QAK
 */
#define H5FD_MEM_FSPACE_HDR   H5FD_MEM_OHDR
#define H5FD_MEM_FSPACE_SINFO H5FD_MEM_LHEAP

/* Map "shared object header message" master table to 'ohdr' type file memory,
 * since its a fair amount of work to add a new kind of file memory and they are
 * similar enough to object headers and probably too minor to deserve their own
 * type.
 *
 * Map "shared object header message" indices to 'btree' type file memory,
 * since they are similar enough to B-tree nodes.
 *
 *      -QAK
 */
#define H5FD_MEM_SOHM_TABLE H5FD_MEM_OHDR
#define H5FD_MEM_SOHM_INDEX H5FD_MEM_BTREE

/* Map "extensible array" header blocks to 'ohdr' type file memory, since its
 * a fair amount of work to add a new kind of file memory and they are similar
 * enough to object headers and probably too minor to deserve their own type.
 *
 * Map "extensible array" index blocks to 'ohdr' type file memory, since they
 * are similar to extensible array header blocks.
 *
 * Map "extensible array" super blocks to 'btree' type file memory, since they
 * are similar enough to B-tree nodes.
 *
 * Map "extensible array" data blocks & pages to 'lheap' type file memory, since
 * they are similar enough to local heap info.
 *
 *      -QAK
 */
#define H5FD_MEM_EARRAY_HDR       H5FD_MEM_OHDR
#define H5FD_MEM_EARRAY_IBLOCK    H5FD_MEM_OHDR
#define H5FD_MEM_EARRAY_SBLOCK    H5FD_MEM_BTREE
#define H5FD_MEM_EARRAY_DBLOCK    H5FD_MEM_LHEAP
#define H5FD_MEM_EARRAY_DBLK_PAGE H5FD_MEM_LHEAP

/* Map "fixed array" header blocks to 'ohdr' type file memory, since its
 * a fair amount of work to add a new kind of file memory and they are similar
 * enough to object headers and probably too minor to deserve their own type.
 *
 * Map "fixed array" data blocks & pages to 'lheap' type file memory, since
 * they are similar enough to local heap info.
 *
 */
#define H5FD_MEM_FARRAY_HDR       H5FD_MEM_OHDR
#define H5FD_MEM_FARRAY_DBLOCK    H5FD_MEM_LHEAP
#define H5FD_MEM_FARRAY_DBLK_PAGE H5FD_MEM_LHEAP

/*
 * A free-list map which maps all types of allocation requests to a single
 * free list.  This is useful for drivers that don't really care about
 * keeping different requests segregated in the underlying file and which
 * want to make most efficient reuse of freed memory.  The use of the
 * H5FD_MEM_SUPER free list is arbitrary.
 */
#define H5FD_FLMAP_SINGLE                                                                                    \
    {                                                                                                        \
        H5FD_MEM_SUPER,     /*default*/                                                                      \
            H5FD_MEM_SUPER, /*super*/                                                                        \
            H5FD_MEM_SUPER, /*btree*/                                                                        \
            H5FD_MEM_SUPER, /*draw*/                                                                         \
            H5FD_MEM_SUPER, /*gheap*/                                                                        \
            H5FD_MEM_SUPER, /*lheap*/                                                                        \
            H5FD_MEM_SUPER  /*ohdr*/                                                                         \
    }

/*
 * A free-list map which segregates requests into `raw' or `meta' data
 * pools.
 */
#define H5FD_FLMAP_DICHOTOMY                                                                                 \
    {                                                                                                        \
        H5FD_MEM_SUPER,     /*default*/                                                                      \
            H5FD_MEM_SUPER, /*super*/                                                                        \
            H5FD_MEM_SUPER, /*btree*/                                                                        \
            H5FD_MEM_DRAW,  /*draw*/                                                                         \
            H5FD_MEM_DRAW,  /*gheap*/                                                                        \
            H5FD_MEM_SUPER, /*lheap*/                                                                        \
            H5FD_MEM_SUPER  /*ohdr*/                                                                         \
    }

/*
 * The default free list map which causes each request type to use it's own
 * free-list.
 */
#define H5FD_FLMAP_DEFAULT                                                                                   \
    {                                                                                                        \
        H5FD_MEM_DEFAULT,     /*default*/                                                                    \
            H5FD_MEM_DEFAULT, /*super*/                                                                      \
            H5FD_MEM_DEFAULT, /*btree*/                                                                      \
            H5FD_MEM_DEFAULT, /*draw*/                                                                       \
            H5FD_MEM_DEFAULT, /*gheap*/                                                                      \
            H5FD_MEM_DEFAULT, /*lheap*/                                                                      \
            H5FD_MEM_DEFAULT  /*ohdr*/                                                                       \
    }

/*******************/
/* Public Typedefs */
/*******************/

/* Forward declaration */
typedef struct H5FD_t H5FD_t;

/* Class information for each file driver */
typedef struct H5FD_class_t {
    unsigned           version; /**< File driver class struct version number */
    H5FD_class_value_t value;
    const char        *name;
    haddr_t            maxaddr;
    H5F_close_degree_t fc_degree;
    herr_t (*terminate)(void);
    hsize_t (*sb_size)(H5FD_t *file);
    herr_t (*sb_encode)(H5FD_t *file, char *name /*out*/, unsigned char *p /*out*/);
    herr_t (*sb_decode)(H5FD_t *f, const char *name, const unsigned char *p);
    size_t fapl_size;
    void *(*fapl_get)(H5FD_t *file);
    void *(*fapl_copy)(const void *fapl);
    herr_t (*fapl_free)(void *fapl);
    size_t dxpl_size;
    void *(*dxpl_copy)(const void *dxpl);
    herr_t (*dxpl_free)(void *dxpl);
    H5FD_t *(*open)(const char *name, unsigned flags, hid_t fapl, haddr_t maxaddr);
    herr_t (*close)(H5FD_t *file);
    int (*cmp)(const H5FD_t *f1, const H5FD_t *f2);
    herr_t (*query)(const H5FD_t *f1, unsigned long *flags);
    herr_t (*get_type_map)(const H5FD_t *file, H5FD_mem_t *type_map);
    haddr_t (*alloc)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
    herr_t (*free)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size);
    haddr_t (*get_eoa)(const H5FD_t *file, H5FD_mem_t type);
    herr_t (*set_eoa)(H5FD_t *file, H5FD_mem_t type, haddr_t addr);
    haddr_t (*get_eof)(const H5FD_t *file, H5FD_mem_t type);
    herr_t (*get_handle)(H5FD_t *file, hid_t fapl, void **file_handle);
    herr_t (*read)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, void *buffer);
    herr_t (*write)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl, haddr_t addr, size_t size, const void *buffer);
    herr_t (*read_vector)(H5FD_t *file, hid_t dxpl, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                          size_t sizes[], void *bufs[]);
    herr_t (*write_vector)(H5FD_t *file, hid_t dxpl, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                           size_t sizes[], const void *bufs[]);
    herr_t (*read_selection)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, size_t count, hid_t mem_spaces[],
                             hid_t file_spaces[], haddr_t offsets[], size_t element_sizes[],
                             void *bufs[] /*out*/);
    herr_t (*write_selection)(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, size_t count, hid_t mem_spaces[],
                              hid_t file_spaces[], haddr_t offsets[], size_t element_sizes[],
                              const void *bufs[] /*in*/);
    herr_t (*flush)(H5FD_t *file, hid_t dxpl_id, hbool_t closing);
    herr_t (*truncate)(H5FD_t *file, hid_t dxpl_id, hbool_t closing);
    herr_t (*lock)(H5FD_t *file, hbool_t rw);
    herr_t (*unlock)(H5FD_t *file);
    herr_t (*del)(const char *name, hid_t fapl);
    herr_t (*ctl)(H5FD_t *file, uint64_t op_code, uint64_t flags, const void *input, void **output);
    H5FD_mem_t fl_map[H5FD_MEM_NTYPES];
} H5FD_class_t;

/* A free list is a singly-linked list of address/size pairs. */
typedef struct H5FD_free_t {
    haddr_t             addr;
    hsize_t             size;
    struct H5FD_free_t *next;
} H5FD_free_t;

/*
 * The main datatype for each driver. Public fields common to all drivers
 * are declared here and the driver appends private fields in memory.
 */
struct H5FD_t {
    hid_t               driver_id;     /*driver ID for this file   */
    const H5FD_class_t *cls;           /*constant class info       */
    unsigned long       fileno;        /* File 'serial' number     */
    unsigned            access_flags;  /* File access flags (from create or open) */
    unsigned long       feature_flags; /* VFL Driver feature Flags */
    haddr_t             maxaddr;       /* For this file, overrides class */
    haddr_t             base_addr;     /* Base address for HDF5 data w/in file */

    /* Space allocation management fields */
    hsize_t threshold;  /* Threshold for alignment  */
    hsize_t alignment;  /* Allocation alignment     */
    hbool_t paged_aggr; /* Paged aggregation for file space is enabled or not */
};

/* VFD initialization function */
typedef hid_t (*H5FD_init_t)(void);

/********************/
/* Public Variables */
/********************/

/*********************/
/* Public Prototypes */
/*********************/

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t   H5FDperform_init(H5FD_init_t op);
H5_DLL hid_t   H5FDregister(const H5FD_class_t *cls);
H5_DLL htri_t  H5FDis_driver_registered_by_name(const char *driver_name);
H5_DLL htri_t  H5FDis_driver_registered_by_value(H5FD_class_value_t driver_value);
H5_DLL herr_t  H5FDunregister(hid_t driver_id);
H5_DLL H5FD_t *H5FDopen(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
H5_DLL herr_t  H5FDclose(H5FD_t *file);
H5_DLL int     H5FDcmp(const H5FD_t *f1, const H5FD_t *f2);
H5_DLL int     H5FDquery(const H5FD_t *f, unsigned long *flags);
H5_DLL haddr_t H5FDalloc(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
H5_DLL herr_t  H5FDfree(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size);
H5_DLL haddr_t H5FDget_eoa(H5FD_t *file, H5FD_mem_t type);
H5_DLL herr_t  H5FDset_eoa(H5FD_t *file, H5FD_mem_t type, haddr_t eoa);
H5_DLL haddr_t H5FDget_eof(H5FD_t *file, H5FD_mem_t type);
H5_DLL herr_t  H5FDget_vfd_handle(H5FD_t *file, hid_t fapl, void **file_handle);
H5_DLL herr_t  H5FDread(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                        void *buf /*out*/);
H5_DLL herr_t  H5FDwrite(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                         const void *buf);
H5_DLL herr_t  H5FDread_vector(H5FD_t *file, hid_t dxpl_id, uint32_t count, H5FD_mem_t types[],
                               haddr_t addrs[], size_t sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FDwrite_vector(H5FD_t *file, hid_t dxpl_id, uint32_t count, H5FD_mem_t types[],
                                haddr_t addrs[], size_t sizes[], const void *bufs[] /* in */);
H5_DLL herr_t  H5FDread_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                  hid_t mem_spaces[], hid_t file_spaces[], haddr_t offsets[],
                                  size_t element_sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FDwrite_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                   hid_t mem_spaces[], hid_t file_spaces[], haddr_t offsets[],
                                   size_t element_sizes[], const void *bufs[]);
H5_DLL herr_t  H5FDread_vector_from_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                              hid_t mem_spaces[], hid_t file_spaces[], haddr_t offsets[],
                                              size_t element_sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FDwrite_vector_from_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                               hid_t mem_spaces[], hid_t file_spaces[], haddr_t offsets[],
                                               size_t element_sizes[], const void *bufs[] /* in */);
H5_DLL herr_t  H5FDread_from_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                       hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                                       size_t element_sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FDwrite_from_selection(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, uint32_t count,
                                        hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                                        size_t element_sizes[], const void *bufs[] /* in */);
H5_DLL herr_t  H5FDflush(H5FD_t *file, hid_t dxpl_id, hbool_t closing);
H5_DLL herr_t  H5FDtruncate(H5FD_t *file, hid_t dxpl_id, hbool_t closing);
H5_DLL herr_t  H5FDlock(H5FD_t *file, hbool_t rw);
H5_DLL herr_t  H5FDunlock(H5FD_t *file);
H5_DLL herr_t  H5FDdelete(const char *name, hid_t fapl_id);
H5_DLL herr_t  H5FDctl(H5FD_t *file, uint64_t op_code, uint64_t flags, const void *input, void **output);

#ifdef __cplusplus
}
#endif

#endif /* H5FDdevelop_H */
