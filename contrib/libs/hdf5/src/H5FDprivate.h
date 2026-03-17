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

#ifndef H5FDprivate_H
#define H5FDprivate_H

/* Include package's public headers */
#include "H5FDpublic.h"
#include "H5FDdevelop.h"

/* Private headers needed by this file */
#include "H5Pprivate.h" /* Property lists            */
#include "H5Sprivate.h" /* Dataspaces                */

/*
 * The MPI drivers are needed because there are
 * places where we check for things that aren't handled by these drivers.
 */
#include "H5FDmpi.h" /* MPI-based file drivers        */

/**************************/
/* Library Private Macros */
/**************************/

/* Length of filename buffer */
#define H5FD_MAX_FILENAME_LEN 1024

#ifdef H5_HAVE_PARALLEL
/* ======== Temporary data transfer properties ======== */
/* Definitions for memory MPI type property */
#define H5FD_MPI_XFER_MEM_MPI_TYPE_NAME "H5FD_mpi_mem_mpi_type"
/* Definitions for file MPI type property */
#define H5FD_MPI_XFER_FILE_MPI_TYPE_NAME "H5FD_mpi_file_mpi_type"

#endif

/****************************/
/* Library Private Typedefs */
/****************************/

/* File operations */
typedef enum {
    OP_UNKNOWN = 0, /* Unknown last file operation */
    OP_READ    = 1, /* Last file I/O operation was a read */
    OP_WRITE   = 2  /* Last file I/O operation was a write */
} H5FD_file_op_t;

/* Define structure to hold initial file image and other relevant information */
typedef struct {
    void                       *buffer;
    size_t                      size;
    H5FD_file_image_callbacks_t callbacks;
} H5FD_file_image_info_t;

/* Define default file image info */
#define H5FD_DEFAULT_FILE_IMAGE_INFO                                                                         \
    {                                                                                                        \
        NULL,         /* file image buffer */                                                                \
            0,        /* buffer size */                                                                      \
        {             /* Callbacks */                                                                        \
            NULL,     /* image_malloc */                                                                     \
                NULL, /* image_memcpy */                                                                     \
                NULL, /* image_realloc */                                                                    \
                NULL, /* image_free */                                                                       \
                NULL, /* udata_copy */                                                                       \
                NULL, /* udata_free */                                                                       \
                NULL, /* udata */                                                                            \
        }                                                                                                    \
    }

#define SKIP_NO_CB        0x00u
#define SKIP_SELECTION_CB 0x01u
#define SKIP_VECTOR_CB    0x02u

/* Define structure to hold driver ID, info & configuration string for FAPLs */
typedef struct {
    hid_t       driver_id;         /* Driver's ID */
    const void *driver_info;       /* Driver info, for open callbacks */
    const char *driver_config_str; /* Driver configuration string */
} H5FD_driver_prop_t;

/* Which kind of VFD field to use for searching */
typedef enum H5FD_get_driver_kind_t {
    H5FD_GET_DRIVER_BY_NAME, /* Name field is set */
    H5FD_GET_DRIVER_BY_VALUE /* Value field is set */
} H5FD_get_driver_kind_t;

/* Forward declarations for prototype arguments */
struct H5S_t;

/*****************************/
/* Library Private Variables */
/*****************************/

/******************************/
/* Library Private Prototypes */
/******************************/

/* Forward declarations for prototype arguments */
struct H5F_t;
union H5PL_key_t;

H5_DLL int           H5FD_term_interface(void);
H5_DLL herr_t        H5FD_locate_signature(H5FD_t *file, haddr_t *sig_addr);
H5_DLL H5FD_class_t *H5FD_get_class(hid_t id);
H5_DLL hsize_t       H5FD_sb_size(H5FD_t *file);
H5_DLL herr_t        H5FD_sb_encode(H5FD_t *file, char *name /*out*/, uint8_t *buf);
H5_DLL herr_t        H5FD_sb_load(H5FD_t *file, const char *name, const uint8_t *buf);
H5_DLL void         *H5FD_fapl_get(H5FD_t *file);
H5_DLL herr_t        H5FD_free_driver_info(hid_t driver_id, const void *driver_info);
H5_DLL hid_t         H5FD_register(const void *cls, size_t size, bool app_ref);
H5_DLL hid_t         H5FD_register_driver_by_name(const char *name, bool app_ref);
H5_DLL hid_t         H5FD_register_driver_by_value(H5FD_class_value_t value, bool app_ref);
H5_DLL htri_t        H5FD_is_driver_registered_by_name(const char *driver_name, hid_t *registered_id);
H5_DLL htri_t  H5FD_is_driver_registered_by_value(H5FD_class_value_t driver_value, hid_t *registered_id);
H5_DLL hid_t   H5FD_get_driver_id_by_name(const char *name, bool is_api);
H5_DLL hid_t   H5FD_get_driver_id_by_value(H5FD_class_value_t value, bool is_api);
H5_DLL H5FD_t *H5FD_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
H5_DLL herr_t  H5FD_close(H5FD_t *file);
H5_DLL int     H5FD_cmp(const H5FD_t *f1, const H5FD_t *f2);
H5_DLL herr_t  H5FD_driver_query(const H5FD_class_t *driver, unsigned long *flags /*out*/);
H5_DLL herr_t  H5FD_check_plugin_load(const H5FD_class_t *cls, const union H5PL_key_t *key, bool *success);
H5_DLL haddr_t H5FD_alloc(H5FD_t *file, H5FD_mem_t type, struct H5F_t *f, hsize_t size, haddr_t *frag_addr,
                          hsize_t *frag_size);
H5_DLL herr_t  H5FD_free(H5FD_t *file, H5FD_mem_t type, struct H5F_t *f, haddr_t addr, hsize_t size);
H5_DLL htri_t  H5FD_try_extend(H5FD_t *file, H5FD_mem_t type, struct H5F_t *f, haddr_t blk_end,
                               hsize_t extra_requested);
H5_DLL haddr_t H5FD_get_eoa(const H5FD_t *file, H5FD_mem_t type);
H5_DLL herr_t  H5FD_set_eoa(H5FD_t *file, H5FD_mem_t type, haddr_t addr);
H5_DLL haddr_t H5FD_get_eof(const H5FD_t *file, H5FD_mem_t type);
H5_DLL haddr_t H5FD_get_maxaddr(const H5FD_t *file);
H5_DLL herr_t  H5FD_get_feature_flags(const H5FD_t *file, unsigned long *feature_flags);
H5_DLL herr_t  H5FD_set_feature_flags(H5FD_t *file, unsigned long feature_flags);
H5_DLL herr_t  H5FD_get_fs_type_map(const H5FD_t *file, H5FD_mem_t *type_map);
H5_DLL herr_t  H5FD_read(H5FD_t *file, H5FD_mem_t type, haddr_t addr, size_t size, void *buf /*out*/);
H5_DLL herr_t  H5FD_write(H5FD_t *file, H5FD_mem_t type, haddr_t addr, size_t size, const void *buf);
H5_DLL herr_t  H5FD_read_vector(H5FD_t *file, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                                size_t sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FD_write_vector(H5FD_t *file, uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                                 size_t sizes[], const void *bufs[] /* out */);
H5_DLL herr_t  H5FD_read_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count, struct H5S_t **mem_spaces,
                                   struct H5S_t **file_spaces, haddr_t offsets[], size_t element_sizes[],
                                   void *bufs[] /* out */);
H5_DLL herr_t  H5FD_write_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count, struct H5S_t **mem_spaces,
                                    struct H5S_t **file_spaces, haddr_t offsets[], size_t element_sizes[],
                                    const void *bufs[]);
H5_DLL herr_t  H5FD_read_selection_id(uint32_t skip_cb, H5FD_t *file, H5FD_mem_t type, uint32_t count,
                                      hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                                      size_t element_sizes[], void *bufs[] /* out */);
H5_DLL herr_t  H5FD_write_selection_id(uint32_t skip_cb, H5FD_t *file, H5FD_mem_t type, uint32_t count,
                                       hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                                       size_t element_sizes[], const void *bufs[]);
H5_DLL herr_t  H5FD_read_vector_from_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count,
                                               hid_t mem_space_ids[], hid_t file_space_ids[],
                                               haddr_t offsets[], size_t element_sizes[], void *bufs[]);

H5_DLL herr_t H5FD_write_vector_from_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count,
                                               hid_t mem_space_ids[], hid_t file_space_ids[],
                                               haddr_t offsets[], size_t element_sizes[], const void *bufs[]);

H5_DLL herr_t H5FD_read_from_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count, hid_t mem_space_ids[],
                                       hid_t file_space_ids[], haddr_t offsets[], size_t element_sizes[],
                                       void *bufs[]);

H5_DLL herr_t  H5FD_write_from_selection(H5FD_t *file, H5FD_mem_t type, uint32_t count, hid_t mem_space_ids[],
                                         hid_t file_space_ids[], haddr_t offsets[], size_t element_sizes[],
                                         const void *bufs[]);
H5_DLL herr_t  H5FD_flush(H5FD_t *file, bool closing);
H5_DLL herr_t  H5FD_truncate(H5FD_t *file, bool closing);
H5_DLL herr_t  H5FD_lock(H5FD_t *file, bool rw);
H5_DLL herr_t  H5FD_unlock(H5FD_t *file);
H5_DLL herr_t  H5FD_delete(const char *name, hid_t fapl_id);
H5_DLL herr_t  H5FD_ctl(H5FD_t *file, uint64_t op_code, uint64_t flags, const void *input, void **output);
H5_DLL herr_t  H5FD_get_fileno(const H5FD_t *file, unsigned long *filenum);
H5_DLL herr_t  H5FD_get_vfd_handle(H5FD_t *file, hid_t fapl, void **file_handle);
H5_DLL herr_t  H5FD_set_base_addr(H5FD_t *file, haddr_t base_addr);
H5_DLL haddr_t H5FD_get_base_addr(const H5FD_t *file);
H5_DLL herr_t  H5FD_set_paged_aggr(H5FD_t *file, bool paged);

H5_DLL herr_t H5FD_sort_vector_io_req(bool *vector_was_sorted, uint32_t count, H5FD_mem_t types[],
                                      haddr_t addrs[], size_t sizes[], H5_flexible_const_ptr_t bufs[],
                                      H5FD_mem_t **s_types_ptr, haddr_t **s_addrs_ptr, size_t **s_sizes_ptr,
                                      H5_flexible_const_ptr_t **s_bufs_ptr);

H5_DLL herr_t H5FD_sort_selection_io_req(bool *selection_was_sorted, size_t count, hid_t mem_space_ids[],
                                         hid_t file_space_ids[], haddr_t offsets[], size_t element_sizes[],
                                         H5_flexible_const_ptr_t bufs[], hid_t **s_mem_space_ids,
                                         hid_t **s_file_space_ids, haddr_t **s_offsets_ptr,
                                         size_t **s_element_sizes_ptr, H5_flexible_const_ptr_t **s_bufs_ptr);
H5_DLL herr_t H5FD_init(void);

/* Function prototypes for MPI based VFDs*/
#ifdef H5_HAVE_PARALLEL
/* General routines */
H5_DLL haddr_t H5FD_mpi_MPIOff_to_haddr(MPI_Offset mpi_off);
H5_DLL herr_t  H5FD_mpi_haddr_to_MPIOff(haddr_t addr, MPI_Offset *mpi_off /*out*/);
#ifdef NOT_YET
H5_DLL herr_t H5FD_mpio_wait_for_left_neighbor(H5FD_t *file);
H5_DLL herr_t H5FD_mpio_signal_right_neighbor(H5FD_t *file);
#endif /* NOT_YET */
H5_DLL herr_t H5FD_set_mpio_atomicity(H5FD_t *file, bool flag);
H5_DLL herr_t H5FD_get_mpio_atomicity(H5FD_t *file, bool *flag);

/* Driver specific methods */
H5_DLL int      H5FD_mpi_get_rank(H5FD_t *file);
H5_DLL int      H5FD_mpi_get_size(H5FD_t *file);
H5_DLL MPI_Comm H5FD_mpi_get_comm(H5FD_t *file);
H5_DLL herr_t   H5FD_mpi_get_file_sync_required(H5FD_t *file, bool *file_sync_required);
#endif /* H5_HAVE_PARALLEL */

#endif /* H5FDprivate_H */
