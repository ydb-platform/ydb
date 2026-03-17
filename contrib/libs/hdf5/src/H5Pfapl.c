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
 * Created:        H5Pfapl.c
 *
 * Purpose:        File access property list class routines
 *
 *-------------------------------------------------------------------------
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5ACprivate.h" /* Metadata cache                           */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5Fprivate.h"  /* Files                                    */
#include "H5FDprivate.h" /* File drivers                             */
#include "H5Iprivate.h"  /* IDs                                      */
#include "H5MMprivate.h" /* Memory Management                        */
#include "H5Ppkg.h"      /* Property lists                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */
#include "H5VMprivate.h" /* Vector Functions                         */

/* Includes needed to set default file driver */
#include "H5FDsec2.h" /* POSIX unbuffered I/O                     */
#include "H5FDcore.h"
#include "H5FDlog.h"
#include "H5FDfamily.h"
#include "H5FDmulti.h"
#include "H5FDstdio.h" /* Standard C buffered I/O                  */
#include "H5FDsplitter.h"
#ifdef H5_HAVE_PARALLEL
#include "H5FDmpio.h"
#endif
#ifdef H5_HAVE_DIRECT
#include "H5FDdirect.h"
#endif
#ifdef H5_HAVE_MIRROR_VFD
#include "H5FDmirror.h"
#endif
#ifdef H5_HAVE_LIBHDFS
#include "H5FDhdfs.h"
#endif
#ifdef H5_HAVE_ROS3_VFD
#include "H5FDros3.h"
#endif
#ifdef H5_HAVE_SUBFILING_VFD
#include "H5FDsubfiling.h"
#endif
#ifdef H5_HAVE_WINDOWS
#include "H5FDwindows.h" /* Win32 I/O                                */
#endif

/* Includes needed to set default VOL connector */
#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/* ========= File Access properties ============ */
/* Definitions for the initial metadata cache resize configuration */
#define H5F_ACS_META_CACHE_INIT_CONFIG_SIZE sizeof(H5AC_cache_config_t)
#define H5F_ACS_META_CACHE_INIT_CONFIG_DEF  H5AC__DEFAULT_CACHE_CONFIG
#define H5F_ACS_META_CACHE_INIT_CONFIG_ENC  H5P__facc_cache_config_enc
#define H5F_ACS_META_CACHE_INIT_CONFIG_DEC  H5P__facc_cache_config_dec
#define H5F_ACS_META_CACHE_INIT_CONFIG_CMP  H5P__facc_cache_config_cmp
/* Definitions for size of raw data chunk cache(slots) */
#define H5F_ACS_DATA_CACHE_NUM_SLOTS_SIZE sizeof(size_t)
#define H5F_ACS_DATA_CACHE_NUM_SLOTS_DEF  521
#define H5F_ACS_DATA_CACHE_NUM_SLOTS_ENC  H5P__encode_size_t
#define H5F_ACS_DATA_CACHE_NUM_SLOTS_DEC  H5P__decode_size_t
/* Definition for size of raw data chunk cache(bytes) */
#define H5F_ACS_DATA_CACHE_BYTE_SIZE_SIZE sizeof(size_t)
#define H5F_ACS_DATA_CACHE_BYTE_SIZE_DEF  (1024 * 1024)
#define H5F_ACS_DATA_CACHE_BYTE_SIZE_ENC  H5P__encode_size_t
#define H5F_ACS_DATA_CACHE_BYTE_SIZE_DEC  H5P__decode_size_t
/* Definition for preemption read chunks first */
#define H5F_ACS_PREEMPT_READ_CHUNKS_SIZE sizeof(double)
#define H5F_ACS_PREEMPT_READ_CHUNKS_DEF  0.75
#define H5F_ACS_PREEMPT_READ_CHUNKS_ENC  H5P__encode_double
#define H5F_ACS_PREEMPT_READ_CHUNKS_DEC  H5P__decode_double
/* Definition for threshold for alignment */
#define H5F_ACS_ALIGN_THRHD_SIZE sizeof(hsize_t)
#define H5F_ACS_ALIGN_THRHD_DEF  H5F_ALIGN_THRHD_DEF
#define H5F_ACS_ALIGN_THRHD_ENC  H5P__encode_hsize_t
#define H5F_ACS_ALIGN_THRHD_DEC  H5P__decode_hsize_t
/* Definition for alignment */
#define H5F_ACS_ALIGN_SIZE sizeof(hsize_t)
#define H5F_ACS_ALIGN_DEF  H5F_ALIGN_DEF
#define H5F_ACS_ALIGN_ENC  H5P__encode_hsize_t
#define H5F_ACS_ALIGN_DEC  H5P__decode_hsize_t
/* Definition for minimum metadata allocation block size (when
   aggregating metadata allocations. */
#define H5F_ACS_META_BLOCK_SIZE_SIZE sizeof(hsize_t)
#define H5F_ACS_META_BLOCK_SIZE_DEF  H5F_META_BLOCK_SIZE_DEF
#define H5F_ACS_META_BLOCK_SIZE_ENC  H5P__encode_hsize_t
#define H5F_ACS_META_BLOCK_SIZE_DEC  H5P__decode_hsize_t
/* Definition for maximum sieve buffer size (when data sieving
   is allowed by file driver */
#define H5F_ACS_SIEVE_BUF_SIZE_SIZE sizeof(size_t)
#define H5F_ACS_SIEVE_BUF_SIZE_DEF  (64 * 1024)
#define H5F_ACS_SIEVE_BUF_SIZE_ENC  H5P__encode_size_t
#define H5F_ACS_SIEVE_BUF_SIZE_DEC  H5P__decode_size_t
/* Definition for minimum "small data" allocation block size (when
   aggregating "small" raw data allocations. */
#define H5F_ACS_SDATA_BLOCK_SIZE_SIZE sizeof(hsize_t)
#define H5F_ACS_SDATA_BLOCK_SIZE_DEF  H5F_SDATA_BLOCK_SIZE_DEF
#define H5F_ACS_SDATA_BLOCK_SIZE_ENC  H5P__encode_hsize_t
#define H5F_ACS_SDATA_BLOCK_SIZE_DEC  H5P__decode_hsize_t
/* Definition for garbage-collect references */
#define H5F_ACS_GARBG_COLCT_REF_SIZE sizeof(unsigned)
#define H5F_ACS_GARBG_COLCT_REF_DEF  0
#define H5F_ACS_GARBG_COLCT_REF_ENC  H5P__encode_unsigned
#define H5F_ACS_GARBG_COLCT_REF_DEC  H5P__decode_unsigned
/* Definition for file driver ID & info */
#define H5F_ACS_FILE_DRV_SIZE sizeof(H5FD_driver_prop_t)
#define H5F_ACS_FILE_DRV_DEF                                                                                 \
    {                                                                                                        \
        H5_DEFAULT_VFD, NULL, NULL                                                                           \
    }
#define H5F_ACS_FILE_DRV_CRT   H5P__facc_file_driver_create
#define H5F_ACS_FILE_DRV_SET   H5P__facc_file_driver_set
#define H5F_ACS_FILE_DRV_GET   H5P__facc_file_driver_get
#define H5F_ACS_FILE_DRV_DEL   H5P__facc_file_driver_del
#define H5F_ACS_FILE_DRV_COPY  H5P__facc_file_driver_copy
#define H5F_ACS_FILE_DRV_CMP   H5P__facc_file_driver_cmp
#define H5F_ACS_FILE_DRV_CLOSE H5P__facc_file_driver_close
/* Definition for file close degree */
#define H5F_CLOSE_DEGREE_SIZE sizeof(H5F_close_degree_t)
#define H5F_CLOSE_DEGREE_DEF  H5F_CLOSE_DEFAULT
#define H5F_CLOSE_DEGREE_ENC  H5P__facc_fclose_degree_enc
#define H5F_CLOSE_DEGREE_DEC  H5P__facc_fclose_degree_dec
/* Definition for offset position in file for family file driver */
#define H5F_ACS_FAMILY_OFFSET_SIZE sizeof(hsize_t)
#define H5F_ACS_FAMILY_OFFSET_DEF  0
#define H5F_ACS_FAMILY_OFFSET_ENC  H5P__encode_hsize_t
#define H5F_ACS_FAMILY_OFFSET_DEC  H5P__decode_hsize_t
/* Definition for new member size of family driver. It's private
 * property only used by h5repart */
#define H5F_ACS_FAMILY_NEWSIZE_SIZE sizeof(hsize_t)
#define H5F_ACS_FAMILY_NEWSIZE_DEF  0
/* Definition for whether to convert family to a single-file driver.
 * It's a private property only used by h5repart.
 */
#define H5F_ACS_FAMILY_TO_SINGLE_SIZE sizeof(bool)
#define H5F_ACS_FAMILY_TO_SINGLE_DEF  false
/* Definition for data type in multi file driver */
#define H5F_ACS_MULTI_TYPE_SIZE sizeof(H5FD_mem_t)
#define H5F_ACS_MULTI_TYPE_DEF  H5FD_MEM_DEFAULT
#define H5F_ACS_MULTI_TYPE_ENC  H5P__facc_multi_type_enc
#define H5F_ACS_MULTI_TYPE_DEC  H5P__facc_multi_type_dec

/* Definition for "low" bound of library format versions */
#define H5F_ACS_LIBVER_LOW_BOUND_SIZE sizeof(H5F_libver_t)
#define H5F_ACS_LIBVER_LOW_BOUND_DEF  H5F_LIBVER_EARLIEST
#define H5F_ACS_LIBVER_LOW_BOUND_ENC  H5P__facc_libver_type_enc
#define H5F_ACS_LIBVER_LOW_BOUND_DEC  H5P__facc_libver_type_dec

/* Definition for "high" bound of library format versions */
#define H5F_ACS_LIBVER_HIGH_BOUND_SIZE sizeof(H5F_libver_t)
#define H5F_ACS_LIBVER_HIGH_BOUND_DEF  H5F_LIBVER_LATEST
#define H5F_ACS_LIBVER_HIGH_BOUND_ENC  H5P__facc_libver_type_enc
#define H5F_ACS_LIBVER_HIGH_BOUND_DEC  H5P__facc_libver_type_dec

/* Definition for whether to query the file descriptor from the core VFD
 * instead of the memory address.  (Private to library)
 */
#define H5F_ACS_WANT_POSIX_FD_SIZE sizeof(bool)
#define H5F_ACS_WANT_POSIX_FD_DEF  false
/* Definition for external file cache size */
#define H5F_ACS_EFC_SIZE_SIZE sizeof(unsigned)
#define H5F_ACS_EFC_SIZE_DEF  0
#define H5F_ACS_EFC_SIZE_ENC  H5P__encode_unsigned
#define H5F_ACS_EFC_SIZE_DEC  H5P__decode_unsigned
/* Definition of pointer to initial file image info */
#define H5F_ACS_FILE_IMAGE_INFO_SIZE  sizeof(H5FD_file_image_info_t)
#define H5F_ACS_FILE_IMAGE_INFO_DEF   H5FD_DEFAULT_FILE_IMAGE_INFO
#define H5F_ACS_FILE_IMAGE_INFO_SET   H5P__facc_file_image_info_set
#define H5F_ACS_FILE_IMAGE_INFO_GET   H5P__facc_file_image_info_get
#define H5F_ACS_FILE_IMAGE_INFO_DEL   H5P__facc_file_image_info_del
#define H5F_ACS_FILE_IMAGE_INFO_COPY  H5P__facc_file_image_info_copy
#define H5F_ACS_FILE_IMAGE_INFO_CMP   H5P__facc_file_image_info_cmp
#define H5F_ACS_FILE_IMAGE_INFO_CLOSE H5P__facc_file_image_info_close
/* Definition for # of metadata read attempts */
#define H5F_ACS_METADATA_READ_ATTEMPTS_SIZE sizeof(unsigned)
#define H5F_ACS_METADATA_READ_ATTEMPTS_DEF  0
#define H5F_ACS_METADATA_READ_ATTEMPTS_ENC  H5P__encode_unsigned
#define H5F_ACS_METADATA_READ_ATTEMPTS_DEC  H5P__decode_unsigned
/* Definition for object flush callback */
#define H5F_ACS_OBJECT_FLUSH_CB_SIZE sizeof(H5F_object_flush_t)
#define H5F_ACS_OBJECT_FLUSH_CB_DEF                                                                          \
    {                                                                                                        \
        NULL, NULL                                                                                           \
    }
/* Definition for status_flags in the superblock */
#define H5F_ACS_CLEAR_STATUS_FLAGS_SIZE sizeof(bool)
#define H5F_ACS_CLEAR_STATUS_FLAGS_DEF  false

/* Definition for dropping free-space to the floor when reading in the superblock */
#define H5F_ACS_NULL_FSM_ADDR_SIZE sizeof(bool)
#define H5F_ACS_NULL_FSM_ADDR_DEF  false
/* Definition for skipping EOF check when reading in the superblock */
#define H5F_ACS_SKIP_EOF_CHECK_SIZE sizeof(bool)
#define H5F_ACS_SKIP_EOF_CHECK_DEF  false

/* Definition for 'use metadata cache logging' flag */
#define H5F_ACS_USE_MDC_LOGGING_SIZE sizeof(bool)
#define H5F_ACS_USE_MDC_LOGGING_DEF  false
#define H5F_ACS_USE_MDC_LOGGING_ENC  H5P__encode_bool
#define H5F_ACS_USE_MDC_LOGGING_DEC  H5P__decode_bool
/* Definition for 'mdc log location' flag */
#define H5F_ACS_MDC_LOG_LOCATION_SIZE  sizeof(char *)
#define H5F_ACS_MDC_LOG_LOCATION_DEF   NULL /* default is no log location */
#define H5F_ACS_MDC_LOG_LOCATION_ENC   H5P__facc_mdc_log_location_enc
#define H5F_ACS_MDC_LOG_LOCATION_DEC   H5P__facc_mdc_log_location_dec
#define H5F_ACS_MDC_LOG_LOCATION_DEL   H5P__facc_mdc_log_location_del
#define H5F_ACS_MDC_LOG_LOCATION_COPY  H5P__facc_mdc_log_location_copy
#define H5F_ACS_MDC_LOG_LOCATION_CMP   H5P__facc_mdc_log_location_cmp
#define H5F_ACS_MDC_LOG_LOCATION_CLOSE H5P__facc_mdc_log_location_close
/* Definition for 'start metadata cache logging on access' flag */
#define H5F_ACS_START_MDC_LOG_ON_ACCESS_SIZE sizeof(bool)
#define H5F_ACS_START_MDC_LOG_ON_ACCESS_DEF  false
#define H5F_ACS_START_MDC_LOG_ON_ACCESS_ENC  H5P__encode_bool
#define H5F_ACS_START_MDC_LOG_ON_ACCESS_DEC  H5P__decode_bool
/* Definition for evict on close property */
#define H5F_ACS_EVICT_ON_CLOSE_FLAG_SIZE sizeof(bool)
#define H5F_ACS_EVICT_ON_CLOSE_FLAG_DEF  false
#define H5F_ACS_EVICT_ON_CLOSE_FLAG_ENC  H5P__encode_bool
#define H5F_ACS_EVICT_ON_CLOSE_FLAG_DEC  H5P__decode_bool
#ifdef H5_HAVE_PARALLEL
/* Definition of collective metadata read mode flag */
#define H5F_ACS_COLL_MD_READ_FLAG_SIZE sizeof(H5P_coll_md_read_flag_t)
#define H5F_ACS_COLL_MD_READ_FLAG_DEF  H5P_USER_FALSE
#define H5F_ACS_COLL_MD_READ_FLAG_ENC  H5P__encode_coll_md_read_flag_t
#define H5F_ACS_COLL_MD_READ_FLAG_DEC  H5P__decode_coll_md_read_flag_t
/* Definition of collective metadata write mode flag */
#define H5F_ACS_COLL_MD_WRITE_FLAG_SIZE sizeof(bool)
#define H5F_ACS_COLL_MD_WRITE_FLAG_DEF  false
#define H5F_ACS_COLL_MD_WRITE_FLAG_ENC  H5P__encode_bool
#define H5F_ACS_COLL_MD_WRITE_FLAG_DEC  H5P__decode_bool
/* Definition for the file's MPI communicator */
#define H5F_ACS_MPI_PARAMS_COMM_SIZE  sizeof(MPI_Comm)
#define H5F_ACS_MPI_PARAMS_COMM_DEF   MPI_COMM_NULL
#define H5F_ACS_MPI_PARAMS_COMM_SET   H5P__facc_mpi_comm_set
#define H5F_ACS_MPI_PARAMS_COMM_GET   H5P__facc_mpi_comm_get
#define H5F_ACS_MPI_PARAMS_COMM_DEL   H5P__facc_mpi_comm_del
#define H5F_ACS_MPI_PARAMS_COMM_COPY  H5P__facc_mpi_comm_copy
#define H5F_ACS_MPI_PARAMS_COMM_CMP   H5P__facc_mpi_comm_cmp
#define H5F_ACS_MPI_PARAMS_COMM_CLOSE H5P__facc_mpi_comm_close
/* Definition for the file's MPI info */
#define H5F_ACS_MPI_PARAMS_INFO_SIZE  sizeof(MPI_Info)
#define H5F_ACS_MPI_PARAMS_INFO_DEF   MPI_INFO_NULL
#define H5F_ACS_MPI_PARAMS_INFO_SET   H5P__facc_mpi_info_set
#define H5F_ACS_MPI_PARAMS_INFO_GET   H5P__facc_mpi_info_get
#define H5F_ACS_MPI_PARAMS_INFO_DEL   H5P__facc_mpi_info_del
#define H5F_ACS_MPI_PARAMS_INFO_COPY  H5P__facc_mpi_info_copy
#define H5F_ACS_MPI_PARAMS_INFO_CMP   H5P__facc_mpi_info_cmp
#define H5F_ACS_MPI_PARAMS_INFO_CLOSE H5P__facc_mpi_info_close
#endif /* H5_HAVE_PARALLEL */
/* Definitions for the initial metadata cache image configuration */
#define H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_SIZE sizeof(H5AC_cache_image_config_t)
#define H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_DEF  H5AC__DEFAULT_CACHE_IMAGE_CONFIG
#define H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_ENC  H5P__facc_cache_image_config_enc
#define H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_DEC  H5P__facc_cache_image_config_dec
#define H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_CMP  H5P__facc_cache_image_config_cmp
/* Definition for total size of page buffer(bytes) */
#define H5F_ACS_PAGE_BUFFER_SIZE_SIZE sizeof(size_t)
#define H5F_ACS_PAGE_BUFFER_SIZE_DEF  0
#define H5F_ACS_PAGE_BUFFER_SIZE_ENC  H5P__encode_size_t
#define H5F_ACS_PAGE_BUFFER_SIZE_DEC  H5P__decode_size_t
/* Definition for minimum metadata size of page buffer(bytes) */
#define H5F_ACS_PAGE_BUFFER_MIN_META_PERC_SIZE sizeof(unsigned)
#define H5F_ACS_PAGE_BUFFER_MIN_META_PERC_DEF  0
#define H5F_ACS_PAGE_BUFFER_MIN_META_PERC_ENC  H5P__encode_unsigned
#define H5F_ACS_PAGE_BUFFER_MIN_META_PERC_DEC  H5P__decode_unsigned
/* Definition for minimum raw data size of page buffer(bytes) */
#define H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_SIZE sizeof(unsigned)
#define H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_DEF  0
#define H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_ENC  H5P__encode_unsigned
#define H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_DEC  H5P__decode_unsigned
/* Definition for file VOL connector properties (ID, etc.) */
#define H5F_ACS_VOL_CONN_SIZE sizeof(H5VL_connector_prop_t)
#define H5F_ACS_VOL_CONN_DEF                                                                                 \
    {                                                                                                        \
        H5_DEFAULT_VOL, NULL                                                                                 \
    }
#define H5F_ACS_VOL_CONN_CRT   H5P__facc_vol_create
#define H5F_ACS_VOL_CONN_SET   H5P__facc_vol_set
#define H5F_ACS_VOL_CONN_GET   H5P__facc_vol_get
#define H5F_ACS_VOL_CONN_DEL   H5P__facc_vol_del
#define H5F_ACS_VOL_CONN_COPY  H5P__facc_vol_copy
#define H5F_ACS_VOL_CONN_CMP   H5P__facc_vol_cmp
#define H5F_ACS_VOL_CONN_CLOSE H5P__facc_vol_close
/* Definition for using file locking or not. The default is set
 * via the configure step.
 */
#define H5F_ACS_USE_FILE_LOCKING_SIZE sizeof(bool)
#if defined H5_USE_FILE_LOCKING && H5_USE_FILE_LOCKING
#define H5F_ACS_USE_FILE_LOCKING_DEF true
#else
#define H5F_ACS_USE_FILE_LOCKING_DEF false
#endif
#define H5F_ACS_USE_FILE_LOCKING_ENC H5P__encode_bool
#define H5F_ACS_USE_FILE_LOCKING_DEC H5P__decode_bool
/* Definition for whether we ignore file locking errors when we can
 * tell that file locking has been disabled on the file system.
 * The default is set via the configure step.
 */
#define H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_SIZE sizeof(bool)
#if defined H5_IGNORE_DISABLED_FILE_LOCKS && H5_IGNORE_DISABLED_FILE_LOCKS
#define H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_DEF true
#else
#define H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_DEF false
#endif
#define H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_ENC H5P__encode_bool
#define H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_DEC H5P__decode_bool

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Package Typedefs */
/********************/

/********************/
/* Local Prototypes */
/********************/

/* Property class callbacks */
static herr_t H5P__facc_reg_prop(H5P_genclass_t *pclass);

/* File driver ID & info property callbacks */
static herr_t H5P__facc_file_driver_create(const char *name, size_t size, void *value);
static herr_t H5P__facc_file_driver_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_driver_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_driver_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_driver_copy(const char *name, size_t size, void *value);
static int    H5P__facc_file_driver_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_file_driver_close(const char *name, size_t size, void *value);

/* File image info property callbacks */
static herr_t H5P__file_image_info_copy(void *value);
static herr_t H5P__file_image_info_free(void *value);
static herr_t H5P__facc_file_image_info_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_image_info_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_image_info_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_file_image_info_copy(const char *name, size_t size, void *value);
static int    H5P__facc_file_image_info_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_file_image_info_close(const char *name, size_t size, void *value);

/* encode & decode callbacks */
static herr_t H5P__facc_cache_config_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_cache_config_dec(const void **_pp, void *value);
static int    H5P__facc_cache_config_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_fclose_degree_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_fclose_degree_dec(const void **pp, void *value);
static herr_t H5P__facc_multi_type_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_multi_type_dec(const void **_pp, void *value);
static herr_t H5P__facc_libver_type_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_libver_type_dec(const void **_pp, void *value);

/* Metadata cache log location property callbacks */
static herr_t H5P__facc_mdc_log_location_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_mdc_log_location_dec(const void **_pp, void *value);
static herr_t H5P__facc_mdc_log_location_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mdc_log_location_copy(const char *name, size_t size, void *value);
static int    H5P__facc_mdc_log_location_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_mdc_log_location_close(const char *name, size_t size, void *value);

/* Metadata cache image property callbacks */
static int    H5P__facc_cache_image_config_cmp(const void *_config1, const void *_config2,
                                               size_t H5_ATTR_UNUSED size);
static herr_t H5P__facc_cache_image_config_enc(const void *value, void **_pp, size_t *size);
static herr_t H5P__facc_cache_image_config_dec(const void **_pp, void *_value);

/* VOL connector callbacks */
static herr_t H5P__facc_vol_create(const char *name, size_t size, void *value);
static herr_t H5P__facc_vol_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_vol_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_vol_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_vol_copy(const char *name, size_t size, void *value);
static int    H5P__facc_vol_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_vol_close(const char *name, size_t size, void *value);

#ifdef H5_HAVE_PARALLEL
/* MPI communicator callbacks */
static herr_t H5P__facc_mpi_comm_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_comm_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_comm_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_comm_copy(const char *name, size_t size, void *value);
static int    H5P__facc_mpi_comm_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_mpi_comm_close(const char *name, size_t size, void *value);

/* MPI info callbacks */
static herr_t H5P__facc_mpi_info_set(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_info_get(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_info_del(hid_t prop_id, const char *name, size_t size, void *value);
static herr_t H5P__facc_mpi_info_copy(const char *name, size_t size, void *value);
static int    H5P__facc_mpi_info_cmp(const void *value1, const void *value2, size_t size);
static herr_t H5P__facc_mpi_info_close(const char *name, size_t size, void *value);
#endif /* H5_HAVE_PARALLEL */

/* Internal routines */
static herr_t H5P__facc_set_def_driver_check_predefined(const char *driver_name, hid_t *driver_id);

/*********************/
/* Package Variables */
/*********************/

/* File access property list class library initialization object */
const H5P_libclass_t H5P_CLS_FACC[1] = {{
    "file access",        /* Class name for debugging     */
    H5P_TYPE_FILE_ACCESS, /* Class type                   */

    &H5P_CLS_ROOT_g,           /* Parent class                 */
    &H5P_CLS_FILE_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_FILE_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_FILE_ACCESS_ID_g, /* Pointer to default property list ID */
    H5P__facc_reg_prop,        /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Property value defaults */
static const H5AC_cache_config_t H5F_def_mdc_initCacheCfg_g =
    H5F_ACS_META_CACHE_INIT_CONFIG_DEF; /* Default metadata cache settings */
static const size_t H5F_def_rdcc_nslots_g =
    H5F_ACS_DATA_CACHE_NUM_SLOTS_DEF; /* Default raw data chunk cache # of slots */
static const size_t H5F_def_rdcc_nbytes_g =
    H5F_ACS_DATA_CACHE_BYTE_SIZE_DEF; /* Default raw data chunk cache # of bytes */
static const double H5F_def_rdcc_w0_g =
    H5F_ACS_PREEMPT_READ_CHUNKS_DEF; /* Default raw data chunk cache dirty ratio */
static const hsize_t H5F_def_threshold_g =
    H5F_ACS_ALIGN_THRHD_DEF;                                  /* Default allocation alignment threshold */
static const hsize_t H5F_def_alignment_g = H5F_ACS_ALIGN_DEF; /* Default allocation alignment value */
static const hsize_t H5F_def_meta_block_size_g =
    H5F_ACS_META_BLOCK_SIZE_DEF; /* Default metadata allocation block size */
static const size_t H5F_def_sieve_buf_size_g =
    H5F_ACS_SIEVE_BUF_SIZE_DEF; /* Default raw data I/O sieve buffer size */
static const hsize_t H5F_def_sdata_block_size_g =
    H5F_ACS_SDATA_BLOCK_SIZE_DEF; /* Default small data allocation block size */
static const unsigned H5F_def_gc_ref_g =
    H5F_ACS_GARBG_COLCT_REF_DEF; /* Default garbage collection for references setting */
static const H5F_close_degree_t H5F_def_close_degree_g = H5F_CLOSE_DEGREE_DEF; /* Default file close degree */
static const hsize_t H5F_def_family_offset_g = H5F_ACS_FAMILY_OFFSET_DEF; /* Default offset for family VFD */
static const hsize_t H5F_def_family_newsize_g =
    H5F_ACS_FAMILY_NEWSIZE_DEF; /* Default size of new files for family VFD */
static const bool H5F_def_family_to_single_g = H5F_ACS_FAMILY_TO_SINGLE_DEF; /* Default ?? for family VFD */
static const H5FD_mem_t H5F_def_mem_type_g =
    H5F_ACS_MULTI_TYPE_DEF; /* Default file space type for multi VFD */

static const H5F_libver_t H5F_def_libver_low_bound_g =
    H5F_ACS_LIBVER_LOW_BOUND_DEF; /* Default setting for "low" bound of format version */
static const H5F_libver_t H5F_def_libver_high_bound_g =
    H5F_ACS_LIBVER_HIGH_BOUND_DEF; /* Default setting for "high" bound of format version */

static const bool H5F_def_want_posix_fd_g =
    H5F_ACS_WANT_POSIX_FD_DEF; /* Default setting for retrieving 'handle' from core VFD */
static const unsigned H5F_def_efc_size_g = H5F_ACS_EFC_SIZE_DEF; /* Default external file cache size */
static const H5FD_file_image_info_t H5F_def_file_image_info_g =
    H5F_ACS_FILE_IMAGE_INFO_DEF; /* Default file image info and callbacks */
static const unsigned H5F_def_metadata_read_attempts_g =
    H5F_ACS_METADATA_READ_ATTEMPTS_DEF; /* Default setting for the # of metadata read attempts */
static const H5F_object_flush_t H5F_def_object_flush_cb_g =
    H5F_ACS_OBJECT_FLUSH_CB_DEF; /* Default setting for object flush callback */
static const bool H5F_def_clear_status_flags_g =
    H5F_ACS_CLEAR_STATUS_FLAGS_DEF; /* Default to clear the superblock status_flags */
static const bool H5F_def_skip_eof_check_g =
    H5F_ACS_SKIP_EOF_CHECK_DEF; /* Default setting for skipping EOF check */
static const bool H5F_def_null_fsm_addr_g =
    H5F_ACS_NULL_FSM_ADDR_DEF; /* Default setting for dropping free-space to the floor */

static const bool H5F_def_use_mdc_logging_g =
    H5F_ACS_USE_MDC_LOGGING_DEF; /* Default metadata cache logging flag */
static const char *H5F_def_mdc_log_location_g = H5F_ACS_MDC_LOG_LOCATION_DEF; /* Default mdc log location */
static const bool  H5F_def_start_mdc_log_on_access_g =
    H5F_ACS_START_MDC_LOG_ON_ACCESS_DEF; /* Default mdc log start on access flag */
static const bool H5F_def_evict_on_close_flag_g =
    H5F_ACS_EVICT_ON_CLOSE_FLAG_DEF; /* Default setting for evict on close property */
#ifdef H5_HAVE_PARALLEL
static const H5P_coll_md_read_flag_t H5F_def_coll_md_read_flag_g =
    H5F_ACS_COLL_MD_READ_FLAG_DEF; /* Default setting for the collective metedata read flag */
static const bool H5F_def_coll_md_write_flag_g =
    H5F_ACS_COLL_MD_WRITE_FLAG_DEF; /* Default setting for the collective metedata write flag */
static const MPI_Comm H5F_def_mpi_params_comm_g = H5F_ACS_MPI_PARAMS_COMM_DEF; /* Default MPI communicator */
static const MPI_Info H5F_def_mpi_params_info_g = H5F_ACS_MPI_PARAMS_INFO_DEF; /* Default MPI info struct */
#endif                                                                         /* H5_HAVE_PARALLEL */
static const H5AC_cache_image_config_t H5F_def_mdc_initCacheImageCfg_g =
    H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_DEF; /* Default metadata cache image settings */
static const size_t   H5F_def_page_buf_size_g = H5F_ACS_PAGE_BUFFER_SIZE_DEF; /* Default page buffer size */
static const unsigned H5F_def_page_buf_min_meta_perc_g =
    H5F_ACS_PAGE_BUFFER_MIN_META_PERC_DEF; /* Default page buffer minimum metadata size */
static const unsigned H5F_def_page_buf_min_raw_perc_g =
    H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_DEF; /* Default page buffer minimum raw data size */
static const bool H5F_def_use_file_locking_g =
    H5F_ACS_USE_FILE_LOCKING_DEF; /* Default use file locking flag */
static const bool H5F_def_ignore_disabled_file_locks_g =
    H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_DEF; /* Default ignore disabled file locks flag */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_reg_prop
 *
 * Purpose:     Register the file access property list class's properties
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_reg_prop(H5P_genclass_t *pclass)
{
    const H5FD_driver_prop_t def_driver_prop =
        H5F_ACS_FILE_DRV_DEF; /* Default VFL driver ID & info (initialized from a variable) */
    const H5VL_connector_prop_t def_vol_prop =
        H5F_ACS_VOL_CONN_DEF;   /* Default VOL connector ID & info (initialized from a variable) */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Register the initial metadata cache resize configuration */
    if (H5P__register_real(pclass, H5F_ACS_META_CACHE_INIT_CONFIG_NAME, H5F_ACS_META_CACHE_INIT_CONFIG_SIZE,
                           &H5F_def_mdc_initCacheCfg_g, NULL, NULL, NULL, H5F_ACS_META_CACHE_INIT_CONFIG_ENC,
                           H5F_ACS_META_CACHE_INIT_CONFIG_DEC, NULL, NULL, H5F_ACS_META_CACHE_INIT_CONFIG_CMP,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of raw data chunk cache (elements) */
    if (H5P__register_real(pclass, H5F_ACS_DATA_CACHE_NUM_SLOTS_NAME, H5F_ACS_DATA_CACHE_NUM_SLOTS_SIZE,
                           &H5F_def_rdcc_nslots_g, NULL, NULL, NULL, H5F_ACS_DATA_CACHE_NUM_SLOTS_ENC,
                           H5F_ACS_DATA_CACHE_NUM_SLOTS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of raw data chunk cache(bytes) */
    if (H5P__register_real(pclass, H5F_ACS_DATA_CACHE_BYTE_SIZE_NAME, H5F_ACS_DATA_CACHE_BYTE_SIZE_SIZE,
                           &H5F_def_rdcc_nbytes_g, NULL, NULL, NULL, H5F_ACS_DATA_CACHE_BYTE_SIZE_ENC,
                           H5F_ACS_DATA_CACHE_BYTE_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the preemption for reading chunks */
    if (H5P__register_real(pclass, H5F_ACS_PREEMPT_READ_CHUNKS_NAME, H5F_ACS_PREEMPT_READ_CHUNKS_SIZE,
                           &H5F_def_rdcc_w0_g, NULL, NULL, NULL, H5F_ACS_PREEMPT_READ_CHUNKS_ENC,
                           H5F_ACS_PREEMPT_READ_CHUNKS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the threshold for alignment */
    if (H5P__register_real(pclass, H5F_ACS_ALIGN_THRHD_NAME, H5F_ACS_ALIGN_THRHD_SIZE, &H5F_def_threshold_g,
                           NULL, NULL, NULL, H5F_ACS_ALIGN_THRHD_ENC, H5F_ACS_ALIGN_THRHD_DEC, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the alignment */
    if (H5P__register_real(pclass, H5F_ACS_ALIGN_NAME, H5F_ACS_ALIGN_SIZE, &H5F_def_alignment_g, NULL, NULL,
                           NULL, H5F_ACS_ALIGN_ENC, H5F_ACS_ALIGN_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the minimum metadata allocation block size */
    if (H5P__register_real(pclass, H5F_ACS_META_BLOCK_SIZE_NAME, H5F_ACS_META_BLOCK_SIZE_SIZE,
                           &H5F_def_meta_block_size_g, NULL, NULL, NULL, H5F_ACS_META_BLOCK_SIZE_ENC,
                           H5F_ACS_META_BLOCK_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the maximum sieve buffer size */
    if (H5P__register_real(pclass, H5F_ACS_SIEVE_BUF_SIZE_NAME, H5F_ACS_SIEVE_BUF_SIZE_SIZE,
                           &H5F_def_sieve_buf_size_g, NULL, NULL, NULL, H5F_ACS_SIEVE_BUF_SIZE_ENC,
                           H5F_ACS_SIEVE_BUF_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the minimum "small data" allocation block size */
    if (H5P__register_real(pclass, H5F_ACS_SDATA_BLOCK_SIZE_NAME, H5F_ACS_SDATA_BLOCK_SIZE_SIZE,
                           &H5F_def_sdata_block_size_g, NULL, NULL, NULL, H5F_ACS_SDATA_BLOCK_SIZE_ENC,
                           H5F_ACS_SDATA_BLOCK_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the garbage collection reference */
    if (H5P__register_real(pclass, H5F_ACS_GARBG_COLCT_REF_NAME, H5F_ACS_GARBG_COLCT_REF_SIZE,
                           &H5F_def_gc_ref_g, NULL, NULL, NULL, H5F_ACS_GARBG_COLCT_REF_ENC,
                           H5F_ACS_GARBG_COLCT_REF_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the file driver ID & info */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_FILE_DRV_NAME, H5F_ACS_FILE_DRV_SIZE, &def_driver_prop,
                           H5F_ACS_FILE_DRV_CRT, H5F_ACS_FILE_DRV_SET, H5F_ACS_FILE_DRV_GET, NULL, NULL,
                           H5F_ACS_FILE_DRV_DEL, H5F_ACS_FILE_DRV_COPY, H5F_ACS_FILE_DRV_CMP,
                           H5F_ACS_FILE_DRV_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the file close degree */
    if (H5P__register_real(pclass, H5F_ACS_CLOSE_DEGREE_NAME, H5F_CLOSE_DEGREE_SIZE, &H5F_def_close_degree_g,
                           NULL, NULL, NULL, H5F_CLOSE_DEGREE_ENC, H5F_CLOSE_DEGREE_DEC, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the offset of family driver info */
    if (H5P__register_real(pclass, H5F_ACS_FAMILY_OFFSET_NAME, H5F_ACS_FAMILY_OFFSET_SIZE,
                           &H5F_def_family_offset_g, NULL, NULL, NULL, H5F_ACS_FAMILY_OFFSET_ENC,
                           H5F_ACS_FAMILY_OFFSET_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of new family file size. It's used by h5repart only. */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_FAMILY_NEWSIZE_NAME, H5F_ACS_FAMILY_NEWSIZE_SIZE,
                           &H5F_def_family_newsize_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of whether convert family to a single-file driver. It's used by h5repart
     * only. */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_FAMILY_TO_SINGLE_NAME, H5F_ACS_FAMILY_TO_SINGLE_SIZE,
                           &H5F_def_family_to_single_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the data type of multi driver info */
    if (H5P__register_real(pclass, H5F_ACS_MULTI_TYPE_NAME, H5F_ACS_MULTI_TYPE_SIZE, &H5F_def_mem_type_g,
                           NULL, NULL, NULL, H5F_ACS_MULTI_TYPE_ENC, H5F_ACS_MULTI_TYPE_DEC, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the 'low' bound of library format versions */
    if (H5P__register_real(pclass, H5F_ACS_LIBVER_LOW_BOUND_NAME, H5F_ACS_LIBVER_LOW_BOUND_SIZE,
                           &H5F_def_libver_low_bound_g, NULL, NULL, NULL, H5F_ACS_LIBVER_LOW_BOUND_ENC,
                           H5F_ACS_LIBVER_LOW_BOUND_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the 'high' bound of library format versions */
    if (H5P__register_real(pclass, H5F_ACS_LIBVER_HIGH_BOUND_NAME, H5F_ACS_LIBVER_HIGH_BOUND_SIZE,
                           &H5F_def_libver_high_bound_g, NULL, NULL, NULL, H5F_ACS_LIBVER_HIGH_BOUND_ENC,
                           H5F_ACS_LIBVER_HIGH_BOUND_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of whether to retrieve the file descriptor from the core VFD */
    /* (used internally to the library only) */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_WANT_POSIX_FD_NAME, H5F_ACS_WANT_POSIX_FD_SIZE,
                           &H5F_def_want_posix_fd_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the external file cache size */
    if (H5P__register_real(pclass, H5F_ACS_EFC_SIZE_NAME, H5F_ACS_EFC_SIZE_SIZE, &H5F_def_efc_size_g, NULL,
                           NULL, NULL, H5F_ACS_EFC_SIZE_ENC, H5F_ACS_EFC_SIZE_DEC, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the initial file image info */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_FILE_IMAGE_INFO_NAME, H5F_ACS_FILE_IMAGE_INFO_SIZE,
                           &H5F_def_file_image_info_g, NULL, H5F_ACS_FILE_IMAGE_INFO_SET,
                           H5F_ACS_FILE_IMAGE_INFO_GET, NULL, NULL, H5F_ACS_FILE_IMAGE_INFO_DEL,
                           H5F_ACS_FILE_IMAGE_INFO_COPY, H5F_ACS_FILE_IMAGE_INFO_CMP,
                           H5F_ACS_FILE_IMAGE_INFO_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the # of read attempts */
    if (H5P__register_real(pclass, H5F_ACS_METADATA_READ_ATTEMPTS_NAME, H5F_ACS_METADATA_READ_ATTEMPTS_SIZE,
                           &H5F_def_metadata_read_attempts_g, NULL, NULL, NULL,
                           H5F_ACS_METADATA_READ_ATTEMPTS_ENC, H5F_ACS_METADATA_READ_ATTEMPTS_DEC, NULL, NULL,
                           NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register object flush callback */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_OBJECT_FLUSH_CB_NAME, H5F_ACS_OBJECT_FLUSH_CB_SIZE,
                           &H5F_def_object_flush_cb_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of whether to clear the superblock status_flags. It's used by h5clear
     * only. */
    if (H5P__register_real(pclass, H5F_ACS_CLEAR_STATUS_FLAGS_NAME, H5F_ACS_CLEAR_STATUS_FLAGS_SIZE,
                           &H5F_def_clear_status_flags_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of whether to skip EOF check. It's used by h5clear only. */
    if (H5P__register_real(pclass, H5F_ACS_SKIP_EOF_CHECK_NAME, H5F_ACS_SKIP_EOF_CHECK_SIZE,
                           &H5F_def_skip_eof_check_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the private property of whether to drop free-space to the floor. It's used by h5clear only. */
    if (H5P__register_real(pclass, H5F_ACS_NULL_FSM_ADDR_NAME, H5F_ACS_NULL_FSM_ADDR_SIZE,
                           &H5F_def_null_fsm_addr_g, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the metadata cache logging flag. */
    if (H5P__register_real(pclass, H5F_ACS_USE_MDC_LOGGING_NAME, H5F_ACS_USE_MDC_LOGGING_SIZE,
                           &H5F_def_use_mdc_logging_g, NULL, NULL, NULL, H5F_ACS_USE_MDC_LOGGING_ENC,
                           H5F_ACS_USE_MDC_LOGGING_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the metadata cache log location. */
    if (H5P__register_real(pclass, H5F_ACS_MDC_LOG_LOCATION_NAME, H5F_ACS_MDC_LOG_LOCATION_SIZE,
                           &H5F_def_mdc_log_location_g, NULL, NULL, NULL, H5F_ACS_MDC_LOG_LOCATION_ENC,
                           H5F_ACS_MDC_LOG_LOCATION_DEC, H5F_ACS_MDC_LOG_LOCATION_DEL,
                           H5F_ACS_MDC_LOG_LOCATION_COPY, H5F_ACS_MDC_LOG_LOCATION_CMP,
                           H5F_ACS_MDC_LOG_LOCATION_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the flag that indicates whether mdc logging starts on file access. */
    if (H5P__register_real(pclass, H5F_ACS_START_MDC_LOG_ON_ACCESS_NAME, H5F_ACS_START_MDC_LOG_ON_ACCESS_SIZE,
                           &H5F_def_start_mdc_log_on_access_g, NULL, NULL, NULL,
                           H5F_ACS_START_MDC_LOG_ON_ACCESS_ENC, H5F_ACS_START_MDC_LOG_ON_ACCESS_DEC, NULL,
                           NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the evict on close flag */
    if (H5P__register_real(pclass, H5F_ACS_EVICT_ON_CLOSE_FLAG_NAME, H5F_ACS_EVICT_ON_CLOSE_FLAG_SIZE,
                           &H5F_def_evict_on_close_flag_g, NULL, NULL, NULL, H5F_ACS_EVICT_ON_CLOSE_FLAG_ENC,
                           H5F_ACS_EVICT_ON_CLOSE_FLAG_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

#ifdef H5_HAVE_PARALLEL
    /* Register the metadata collective read flag */
    if (H5P__register_real(pclass, H5_COLL_MD_READ_FLAG_NAME, H5F_ACS_COLL_MD_READ_FLAG_SIZE,
                           &H5F_def_coll_md_read_flag_g, NULL, NULL, NULL, H5F_ACS_COLL_MD_READ_FLAG_ENC,
                           H5F_ACS_COLL_MD_READ_FLAG_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the metadata collective write flag */
    if (H5P__register_real(pclass, H5F_ACS_COLL_MD_WRITE_FLAG_NAME, H5F_ACS_COLL_MD_WRITE_FLAG_SIZE,
                           &H5F_def_coll_md_write_flag_g, NULL, NULL, NULL, H5F_ACS_COLL_MD_WRITE_FLAG_ENC,
                           H5F_ACS_COLL_MD_WRITE_FLAG_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the MPI communicator */
    if (H5P__register_real(pclass, H5F_ACS_MPI_PARAMS_COMM_NAME, H5F_ACS_MPI_PARAMS_COMM_SIZE,
                           &H5F_def_mpi_params_comm_g, NULL, H5F_ACS_MPI_PARAMS_COMM_SET,
                           H5F_ACS_MPI_PARAMS_COMM_GET, NULL, NULL, H5F_ACS_MPI_PARAMS_COMM_DEL,
                           H5F_ACS_MPI_PARAMS_COMM_COPY, H5F_ACS_MPI_PARAMS_COMM_CMP,
                           H5F_ACS_MPI_PARAMS_COMM_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the MPI info struct */
    if (H5P__register_real(pclass, H5F_ACS_MPI_PARAMS_INFO_NAME, H5F_ACS_MPI_PARAMS_INFO_SIZE,
                           &H5F_def_mpi_params_info_g, NULL, H5F_ACS_MPI_PARAMS_INFO_SET,
                           H5F_ACS_MPI_PARAMS_INFO_GET, NULL, NULL, H5F_ACS_MPI_PARAMS_INFO_DEL,
                           H5F_ACS_MPI_PARAMS_INFO_COPY, H5F_ACS_MPI_PARAMS_INFO_CMP,
                           H5F_ACS_MPI_PARAMS_INFO_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

#endif /* H5_HAVE_PARALLEL */

    /* Register the initial metadata cache image configuration */
    if (H5P__register_real(pclass, H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_NAME,
                           H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_SIZE, &H5F_def_mdc_initCacheImageCfg_g, NULL,
                           NULL, NULL, H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_ENC,
                           H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_DEC, NULL, NULL,
                           H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_CMP, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of the page buffer size */
    if (H5P__register_real(pclass, H5F_ACS_PAGE_BUFFER_SIZE_NAME, H5F_ACS_PAGE_BUFFER_SIZE_SIZE,
                           &H5F_def_page_buf_size_g, NULL, NULL, NULL, H5F_ACS_PAGE_BUFFER_SIZE_ENC,
                           H5F_ACS_PAGE_BUFFER_SIZE_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of the page buffer minimum metadata size */
    if (H5P__register_real(pclass, H5F_ACS_PAGE_BUFFER_MIN_META_PERC_NAME,
                           H5F_ACS_PAGE_BUFFER_MIN_META_PERC_SIZE, &H5F_def_page_buf_min_meta_perc_g, NULL,
                           NULL, NULL, H5F_ACS_PAGE_BUFFER_MIN_META_PERC_ENC,
                           H5F_ACS_PAGE_BUFFER_MIN_META_PERC_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the size of the page buffer minimum raw data size */
    if (H5P__register_real(pclass, H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_NAME,
                           H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_SIZE, &H5F_def_page_buf_min_raw_perc_g, NULL,
                           NULL, NULL, H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_ENC,
                           H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the file VOL connector ID & info */
    /* (Note: this property should not have an encode/decode callback -QAK) */
    if (H5P__register_real(pclass, H5F_ACS_VOL_CONN_NAME, H5F_ACS_VOL_CONN_SIZE, &def_vol_prop,
                           H5F_ACS_VOL_CONN_CRT, H5F_ACS_VOL_CONN_SET, H5F_ACS_VOL_CONN_GET, NULL, NULL,
                           H5F_ACS_VOL_CONN_DEL, H5F_ACS_VOL_CONN_COPY, H5F_ACS_VOL_CONN_CMP,
                           H5F_ACS_VOL_CONN_CLOSE) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the use file locking flag */
    if (H5P__register_real(pclass, H5F_ACS_USE_FILE_LOCKING_NAME, H5F_ACS_USE_FILE_LOCKING_SIZE,
                           &H5F_def_use_file_locking_g, NULL, NULL, NULL, H5F_ACS_USE_FILE_LOCKING_ENC,
                           H5F_ACS_USE_FILE_LOCKING_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

    /* Register the ignore disabled file locks flag */
    if (H5P__register_real(pclass, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME,
                           H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_SIZE, &H5F_def_ignore_disabled_file_locks_g,
                           NULL, NULL, NULL, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_ENC,
                           H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_DEC, NULL, NULL, NULL, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_reg_prop() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_set_def_driver
 *
 * Purpose:     Parses a string that contains the name of the default VFL
 *              driver for the default FAPL.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__facc_set_def_driver(void)
{
    const char *driver_env_var;
    bool        driver_ref_inc = false;
    hid_t       driver_id      = H5I_INVALID_HID; /* VFL driver ID */
    herr_t      ret_value      = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Check if VFL driver environment variable is set */
    driver_env_var = getenv(HDF5_DRIVER);

    /* Only parse VFL driver string if it's set */
    if (driver_env_var && *driver_env_var) {
        H5FD_driver_prop_t driver_prop;
        H5P_genplist_t    *def_fapl;     /* Default file access property list */
        H5P_genclass_t    *def_fapclass; /* Default file access property class */
        const char        *driver_config_env_var;
        htri_t             driver_is_registered;

        /* Check to see if the driver is already registered */
        if ((driver_is_registered = H5FD_is_driver_registered_by_name(driver_env_var, &driver_id)) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't check if VFL driver is already registered");
        if (driver_is_registered) {
            assert(driver_id >= 0);

            if (H5I_inc_ref(driver_id, true) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTINC, FAIL, "unable to increment ref count on VFD");
            driver_ref_inc = true;
        } /* end else-if */
        else {
            /* Check for VFL drivers that ship with the library */
            if (H5P__facc_set_def_driver_check_predefined(driver_env_var, &driver_id) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't check for predefined VFL driver name");
            else if (driver_id > 0) {
                if (H5I_inc_ref(driver_id, true) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTINC, FAIL, "can't increment VFL driver refcount");
                driver_ref_inc = true;
            }
            else {
                /* Register the VFL driver */
                if ((driver_id = H5FD_register_driver_by_name(driver_env_var, true)) < 0)
                    HGOTO_ERROR(H5E_VFL, H5E_CANTREGISTER, FAIL, "can't register VFL driver");
                driver_ref_inc = true;
            } /* end else */
        }     /* end else */

        /* Retrieve driver configuration string from environment variable, if set. */
        driver_config_env_var = getenv(HDF5_DRIVER_CONFIG);

        driver_prop.driver_id         = driver_id;
        driver_prop.driver_info       = NULL;
        driver_prop.driver_config_str = driver_config_env_var;

        /* Get default file access pclass */
        if (NULL == (def_fapclass = (H5P_genclass_t *)H5I_object(H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_VFL, H5E_BADID, FAIL,
                        "can't find object for default file access property class ID");

        /* Set new default VFL driver for default file access pclass */
        if (H5P__class_set(def_fapclass, H5F_ACS_FILE_DRV_NAME, &driver_prop) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL,
                        "can't set default VFL driver for default file access property list class");

        /* Get default file access plist */
        if (NULL == (def_fapl = (H5P_genplist_t *)H5I_object(H5P_FILE_ACCESS_DEFAULT)))
            HGOTO_ERROR(H5E_VFL, H5E_BADID, FAIL, "can't find object for default fapl ID");

        /* Set new default VFL driver for default FAPL */
        if (H5P_set_driver(def_fapl, driver_prop.driver_id, driver_prop.driver_info,
                           driver_prop.driver_config_str) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "can't set default VFL driver for default FAPL");
    }

done:
    /* Clean up on error */
    if (ret_value < 0) {
        if (driver_id >= 0 && driver_ref_inc && H5I_dec_app_ref(driver_id) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "unable to unregister VFL driver");
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_set_def_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_set_def_driver_check_predefined
 *
 * Purpose:     Checks a given driver name against a list of predefined
 *              names for VFL drivers that are internal to HDF5. If a name
 *              is matched, the ID for that driver is returned through
 *              `driver_id`. Otherwise, `driver_id` is set to
 *              H5I_INVALID_HID.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_set_def_driver_check_predefined(const char *driver_name, hid_t *driver_id)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(driver_name);
    assert(driver_id);

    if (!strcmp(driver_name, "sec2")) {
        if ((*driver_id = H5FD_SEC2) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize sec2 VFD");
    }
    else if (!strcmp(driver_name, "core") || !strcmp(driver_name, "core_paged")) {
        if ((*driver_id = H5FD_CORE) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize core VFD");
    }
    else if (!strcmp(driver_name, "log")) {
        if ((*driver_id = H5FD_LOG) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize log VFD");
    }
    else if (!strcmp(driver_name, "family")) {
        if ((*driver_id = H5FD_FAMILY) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize family VFD");
    }
    else if (!strcmp(driver_name, "multi") || !strcmp(driver_name, "split")) {
        if ((*driver_id = H5FD_MULTI) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize multi VFD");
    }
    else if (!strcmp(driver_name, "stdio")) {
        if ((*driver_id = H5FD_STDIO) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize stdio VFD");
    }
    else if (!strcmp(driver_name, "splitter")) {
        if ((*driver_id = H5FD_SPLITTER) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize splitter VFD");
    }
    else if (!strcmp(driver_name, "mpio")) {
#ifdef H5_HAVE_PARALLEL
        if ((*driver_id = H5FD_MPIO) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize MPI I/O VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "MPI-I/O VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "direct")) {
#ifdef H5_HAVE_DIRECT
        if ((*driver_id = H5FD_DIRECT) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize Direct I/O VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "Direct I/O VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "mirror")) {
#ifdef H5_HAVE_MIRROR_VFD
        if ((*driver_id = H5FD_MIRROR) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize mirror VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "Mirror VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "hdfs")) {
#ifdef H5_HAVE_LIBHDFS
        if ((*driver_id = H5FD_HDFS) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize HDFS VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "HDFS VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "ros3")) {
#ifdef H5_HAVE_ROS3_VFD
        if ((*driver_id = H5FD_ROS3) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize ROS3 VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "ROS3 VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "subfiling")) {
#ifdef H5_HAVE_SUBFILING_VFD
        if ((*driver_id = H5FD_SUBFILING) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize Subfiling VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "Subfiling VFD is not enabled");
#endif
    }
    else if (!strcmp(driver_name, "windows")) {
#ifdef H5_HAVE_WINDOWS
        if ((*driver_id = H5FD_WINDOWS) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, FAIL, "couldn't initialize Windows VFD");
#else
        HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "Windows VFD is not enabled");
#endif
    }
    else {
        *driver_id = H5I_INVALID_HID;
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_set_def_driver_check_predefined() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_alignment
 *
 * Purpose:    Sets the alignment properties of a file access property list
 *        so that any file object >= THRESHOLD bytes will be aligned on
 *        an address which is a multiple of ALIGNMENT.  The addresses
 *        are relative to the end of the user block; the alignment is
 *        calculated by subtracting the user block size from the
 *        absolute file address and then adjusting the address to be a
 *        multiple of ALIGNMENT.
 *
 *        Default values for THRESHOLD and ALIGNMENT are one, implying
 *        no alignment.  Generally the default values will result in
 *        the best performance for single-process access to the file.
 *        For MPI-IO and other parallel systems, choose an alignment
 *        which is a multiple of the disk block size.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_alignment(hid_t fapl_id, hsize_t threshold, hsize_t alignment)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ihh", fapl_id, threshold, alignment);

    /* Check args */
    if (alignment < 1)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "alignment must be positive");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_ALIGN_THRHD_NAME, &threshold) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set threshold");
    if (H5P_set(plist, H5F_ACS_ALIGN_NAME, &alignment) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set alignment");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_alignment
 *
 * Purpose:    Returns the current settings for alignment properties from a
 *        file access property list.  The THRESHOLD and/or ALIGNMENT
 *        pointers may be null pointers.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_alignment(hid_t fapl_id, hsize_t *threshold /*out*/, hsize_t *alignment /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, threshold, alignment);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (threshold)
        if (H5P_get(plist, H5F_ACS_ALIGN_THRHD_NAME, threshold) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get threshold");
    if (alignment)
        if (H5P_get(plist, H5F_ACS_ALIGN_NAME, alignment) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get alignment");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_alignment() */

/*-------------------------------------------------------------------------
 * Function:   H5P_set_driver
 *
 * Purpose:    Set the file driver (NEW_DRIVER_ID) for a file access
 *             property list (PLIST). A struct (NEW_DRIVER_INFO) or string
 *             (NEW_DRIVER_CONFIG_STR) containing the driver-specific
 *             properties can optionally be supplied. The driver properties
 *             (struct or string) will be copied into the property list and
 *             the reference count on the driver will be incremented,
 *             allowing the caller to close the driver ID but still use the
 *             property list.
 *
 * Note:       Only one of either NEW_DRIVER_INFO or NEW_DRIVER_CONFIG_STR
 *             should be specified, but not both.
 *
 * Return:     Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_set_driver(H5P_genplist_t *plist, hid_t new_driver_id, const void *new_driver_info,
               const char *new_driver_config_str)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* If VFD configuration information is supplied, ensure that either binary
     * configuration data or a configuration string is supplied, but not both.
     */
    assert(!new_driver_info || !new_driver_config_str);

    if (NULL == H5I_object_verify(new_driver_id, H5I_VFL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file driver ID");

    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5FD_driver_prop_t driver_prop; /* Property for driver ID, info & config. string */

        /* Prepare the driver property */
        driver_prop.driver_id         = new_driver_id;
        driver_prop.driver_info       = new_driver_info;
        driver_prop.driver_config_str = new_driver_config_str;

        /* Set the driver ID, info & config. string property */
        if (H5P_set(plist, H5F_ACS_FILE_DRV_NAME, &driver_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver ID & info");
    } /* end if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_driver
 *
 * Purpose:    Set the file driver (DRIVER_ID) for a file access
 *        property list (PLIST_ID) and supply an optional
 *        struct containing the driver-specific properties
 *        (DRIVER_INFO).  The driver properties will be copied into the
 *        property list and the reference count on the driver will be
 *        incremented, allowing the caller to close the driver ID but
 *        still use the property list.
 *
 * Return:    Success:    Non-negative
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_driver(hid_t plist_id, hid_t new_driver_id, const void *new_driver_info)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ii*x", plist_id, new_driver_id, new_driver_info);

    /* Check arguments */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (NULL == H5I_object_verify(new_driver_id, H5I_VFL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file driver ID");

    /* Set the driver */
    if (H5P_set_driver(plist, new_driver_id, new_driver_info, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5P_set_driver_by_name
 *
 * Purpose:     Set the file driver name (DRIVER_NAME) for a file access
 *              property list (PLIST) and supply an optional string
 *              containing the driver-specific properties (DRIVER_CONFIG).
 *              The driver properties string will be copied into the
 *              property list.
 *
 *              If the file driver specified by DRIVER_NAME is not
 *              currently registered, an attempt will be made to load the
 *              driver as a plugin.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_set_driver_by_name(H5P_genplist_t *plist, const char *driver_name, const char *driver_config,
                       bool app_ref)
{
    hid_t  new_driver_id = H5I_INVALID_HID;
    herr_t ret_value     = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(plist);
    assert(driver_name);

    /* Register the driver */
    if ((new_driver_id = H5FD_register_driver_by_name(driver_name, app_ref)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTREGISTER, FAIL, "unable to register VFD");

    /* Set the driver */
    if (H5P_set_driver(plist, new_driver_id, NULL, driver_config) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver info");

done:
    if (ret_value < 0) {
        if (new_driver_id >= 0 && H5I_dec_app_ref(new_driver_id) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "can't decrement count on VFD ID");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_driver_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_driver_by_name
 *
 * Purpose:     Set the file driver name (DRIVER_NAME) for a file access
 *              property list (PLIST_ID) and supply an optional string
 *              containing the driver-specific properties (DRIVER_CONFIG).
 *              The driver properties string will be copied into the
 *              property list.
 *
 *              If the file driver specified by DRIVER_NAME is not
 *              currently registered, an attempt will be made to load the
 *              driver as a plugin.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_driver_by_name(hid_t plist_id, const char *driver_name, const char *driver_config)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*s*s", plist_id, driver_name, driver_config);

    /* Check arguments */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (!driver_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "driver_name parameter cannot be NULL");
    if (!*driver_name)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "driver_name parameter cannot be an empty string");

    /* Set the driver */
    if (H5P_set_driver_by_name(plist, driver_name, driver_config, true) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_driver_by_name() */

/*-------------------------------------------------------------------------
 * Function:    H5P_set_driver_by_value
 *
 * Purpose:     Set the file driver value (DRIVER_VALUE) for a file access
 *              property list (PLIST) and supply an optional string
 *              containing the driver-specific properties (DRIVER_CONFIG).
 *              The driver properties string will be copied into the
 *              property list.
 *
 *              If the file driver specified by DRIVER_VALUE is not
 *              currently registered, an attempt will be made to load the
 *              driver as a plugin.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_set_driver_by_value(H5P_genplist_t *plist, H5FD_class_value_t driver_value, const char *driver_config,
                        bool app_ref)
{
    hid_t  new_driver_id = H5I_INVALID_HID;
    herr_t ret_value     = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    assert(plist);
    assert(driver_value >= 0);

    /* Register the driver */
    if ((new_driver_id = H5FD_register_driver_by_value(driver_value, app_ref)) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTREGISTER, FAIL, "unable to register VFD");

    /* Set the driver */
    if (H5P_set_driver(plist, new_driver_id, NULL, driver_config) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver info");

done:
    if (ret_value < 0) {
        if (new_driver_id >= 0 && H5I_dec_app_ref(new_driver_id) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "can't decrement count on VFD ID");
    }

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_driver_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_driver_by_value
 *
 * Purpose:     Set the file driver value (DRIVER_VALUE) for a file access
 *              property list (PLIST_ID) and supply an optional string
 *              containing the driver-specific properties (DRIVER_CONFIG).
 *              The driver properties string will be copied into the
 *              property list.
 *
 *              If the file driver specified by DRIVER_VALUE is not
 *              currently registered, an attempt will be made to load the
 *              driver as a plugin.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_driver_by_value(hid_t plist_id, H5FD_class_value_t driver_value, const char *driver_config)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iDV*s", plist_id, driver_value, driver_config);

    /* Check arguments */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (driver_value < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "negative VFD value is disallowed");

    /* Set the driver */
    if (H5P_set_driver_by_value(plist, driver_value, driver_config, true) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set driver info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_driver_by_value() */

/*-------------------------------------------------------------------------
 * Function:    H5P_peek_driver
 *
 * Purpose:    Return the ID of the low-level file driver.  PLIST_ID should
 *        be a file access property list.
 *
 * Return:    Success:    A low-level driver ID which is the same ID
 *                used when the driver was set for the property
 *                list. The driver ID is only valid as long as
 *                the file driver remains registered.
 *
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5P_peek_driver(H5P_genplist_t *plist)
{
    hid_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Get the current driver ID */
    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5FD_driver_prop_t driver_prop; /* Property for driver ID, info & configuration string */

        if (H5P_peek(plist, H5F_ACS_FILE_DRV_NAME, &driver_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get driver ID");
        ret_value = driver_prop.driver_id;
    } /* end if */
    else
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access property list");

    if (H5FD_VFD_DEFAULT == ret_value)
        ret_value = H5_DEFAULT_VFD;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_peek_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_driver
 *
 * Purpose:    Return the ID of the low-level file driver.  PLIST_ID should
 *        be a file access property list.
 *
 * Note:    The ID returned should not be closed.
 *
 * Return:    Success:    A low-level driver ID which is the same ID
 *                used when the driver was set for the property
 *                list. The driver ID is only valid as long as
 *                the file driver remains registered.
 *
 *        Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5Pget_driver(hid_t plist_id)
{
    H5P_genplist_t *plist;     /* Property list pointer */
    hid_t           ret_value; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE1("i", "i", plist_id);

    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Get the driver */
    if ((ret_value = H5P_peek_driver(plist)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get driver");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_driver() */

/*-------------------------------------------------------------------------
 * Function:    H5P_peek_driver_info
 *
 * Purpose:    Returns a pointer directly to the file driver-specific
 *        information of a file access.
 *
 * Return:    Success:    Ptr to *uncopied* driver specific data
 *                structure if any.
 *
 *        Failure:    NULL. Null is also returned if the driver has
 *                not registered any driver-specific properties
 *                although no error is pushed on the stack in
 *                this case.
 *
 *-------------------------------------------------------------------------
 */
const void *
H5P_peek_driver_info(H5P_genplist_t *plist)
{
    const void *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Get the current driver info */
    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5FD_driver_prop_t driver_prop; /* Property for driver ID, info & configuration string */

        if (H5P_peek(plist, H5F_ACS_FILE_DRV_NAME, &driver_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get driver info");
        ret_value = driver_prop.driver_info;
    } /* end if */
    else
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, NULL, "not a file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_peek_driver_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_driver_info
 *
 * Purpose:    Returns a pointer directly to the file driver-specific
 *        information of a file access.
 *
 * Return:    Success:    Ptr to *uncopied* driver specific data
 *                structure if any.
 *
 *        Failure:    NULL. Null is also returned if the driver has
 *                not registered any driver-specific properties
 *                although no error is pushed on the stack in
 *                this case.
 *
 *-------------------------------------------------------------------------
 */
const void *
H5Pget_driver_info(hid_t plist_id)
{
    H5P_genplist_t *plist     = NULL; /* Property list pointer            */
    const void     *ret_value = NULL; /* Return value                     */

    FUNC_ENTER_API(NULL)
    H5TRACE1("*x", "i", plist_id);

    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a property list");

    /* Get the driver info */
    if (NULL == (ret_value = (const void *)H5P_peek_driver_info(plist)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get driver info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_driver_info() */

/*-------------------------------------------------------------------------
 * Function:   H5P_peek_driver_config_str
 *
 * Purpose:    Returns a pointer directly to the file driver configuration
 *             string of a file access property list.
 *
 * Return:     Success: Ptr to *uncopied* driver configuration string, if
 *                      any.
 *
 *             Failure: NULL. NULL is also returned if the driver has not
 *                      been configured with a driver configuration string.
 *
 *-------------------------------------------------------------------------
 */
const char *
H5P_peek_driver_config_str(H5P_genplist_t *plist)
{
    const char *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Get the current driver configuration string */
    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5FD_driver_prop_t driver_prop; /* Property for driver ID, info & configuration string */

        if (H5P_peek(plist, H5F_ACS_FILE_DRV_NAME, &driver_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, NULL, "can't get driver configuration string");
        ret_value = driver_prop.driver_config_str;
    } /* end if */
    else
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, NULL, "not a file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_peek_driver_config_str() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_driver_config_str
 *
 * Purpose:     Retrieves a string representation of the configuration for
 *              the driver set on the given FAPL. The returned string can
 *              be used to configure the same driver in an identical way.
 *
 *              `config_buf` may be NULL, in which case the length of the
 *              driver configuration string is simply returned. The caller
 *              can then allocate a buffer of the appropriate size and call
 *              this routine again.
 *
 * Return:      Length of the driver configuration string on success (not
 *                  including the NUL terminator)
 *              Negative on failure
 *
 *-------------------------------------------------------------------------
 */
ssize_t
H5Pget_driver_config_str(hid_t fapl_id, char *config_buf, size_t buf_size)
{
    H5P_genplist_t *plist; /* Property list pointer */
    const char     *config_str = NULL;
    ssize_t         ret_value  = -1;

    FUNC_ENTER_API((-1))
    H5TRACE3("Zs", "i*sz", fapl_id, config_buf, buf_size);

    /* Check arguments */
    if (!config_buf && buf_size)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, (-1), "config_buf cannot be NULL if buf_size is non-zero");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, (-1), "can't find object for ID");

    /* Retrieve configuration string property */
    if ((config_str = H5P_peek_driver_config_str(plist))) {
        size_t config_str_len = strlen(config_str);

        if (config_buf) {
            strncpy(config_buf, config_str, buf_size);
            if (config_str_len >= buf_size)
                config_buf[buf_size - 1] = '\0';
        }

        ret_value = (ssize_t)config_str_len;
    }
    else
        ret_value = 0;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_driver_config_str() */

/*-------------------------------------------------------------------------
 * Function:    H5P__file_driver_copy
 *
 * Purpose:     Copy file driver ID & info.
 *
 * Note:        This is an "in-place" copy, since this routine gets called
 *              after the top-level copy has been performed and this routine
 *              finishes the "deep" part of the copy.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__file_driver_copy(void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (value) {
        H5FD_driver_prop_t *info = (H5FD_driver_prop_t *)value; /* Driver ID & info struct */

        /* Copy the driver & info, if there is one */
        if (info->driver_id > 0) {
            /* Increment the reference count on driver and copy driver info */
            if (H5I_inc_ref(info->driver_id, false) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINC, FAIL, "unable to increment ref count on VFL driver");

            /* Copy driver info, if it exists */
            if (info->driver_info) {
                H5FD_class_t *driver; /* Pointer to driver */
                void         *new_pl; /* Copy of driver info */

                /* Retrieve the driver for the ID */
                if (NULL == (driver = (H5FD_class_t *)H5I_object(info->driver_id)))
                    HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a driver ID");

                /* Allow the driver to copy or do it ourselves */
                if (driver->fapl_copy) {
                    if (NULL == (new_pl = (driver->fapl_copy)(info->driver_info)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "driver info copy failed");
                } /* end if */
                else if (driver->fapl_size > 0) {
                    if (NULL == (new_pl = H5MM_malloc(driver->fapl_size)))
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "driver info allocation failed");
                    H5MM_memcpy(new_pl, info->driver_info, driver->fapl_size);
                } /* end else-if */
                else
                    HGOTO_ERROR(H5E_PLIST, H5E_UNSUPPORTED, FAIL, "no way to copy driver info");

                /* Set the driver info for the copy */
                info->driver_info = new_pl;
            } /* end if */

            /* Copy driver configuration string, if it exists */
            if (info->driver_config_str) {
                char *new_config_str = NULL;

                if (NULL == (new_config_str = H5MM_strdup(info->driver_config_str)))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "driver configuration string copy failed");
                info->driver_config_str = new_config_str;
            } /* end if */
        }     /* end if */
    }         /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__file_driver_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5P__file_driver_free
 *
 * Purpose:     Free file driver ID & info.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__file_driver_free(void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (value) {
        H5FD_driver_prop_t *info = (H5FD_driver_prop_t *)value; /* Driver ID & info struct */

        if (info->driver_id > 0) {

            /* Free the driver info, if it exists */
            if (info->driver_info)
                if (H5FD_free_driver_info(info->driver_id, info->driver_info) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "driver info free request failed");

            /* Free the driver configuration string, if it exists */
            H5MM_xfree_const(info->driver_config_str);

            /* Decrement reference count for driver */
            if (H5I_dec_ref(info->driver_id) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTDEC, FAIL, "can't decrement reference count for driver ID");
        }
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__file_driver_free() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_create
 *
 * Purpose:     Create callback for the file driver ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_create(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make copy of file driver */
    if (H5P__file_driver_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_create() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_set
 *
 * Purpose:     Copies a file driver property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                          size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of file driver ID & info */
    if (H5P__file_driver_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_get
 *
 * Purpose:     Copies a file driver property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                          size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of file driver */
    if (H5P__file_driver_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_del
 *
 * Purpose:     Frees memory used to store the driver ID & info property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                          size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the file driver ID & info */
    if (H5P__file_driver_free(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_copy
 *
 * Purpose:     Copy callback for the file driver ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make copy of file driver */
    if (H5P__file_driver_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_file_driver_cmp
 *
 * Purpose:        Callback routine which is called whenever the file driver ID & info
 *                 property in the file access property list is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                      VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                      VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_file_driver_cmp(const void *_info1, const void *_info2, size_t H5_ATTR_UNUSED size)
{
    const H5FD_driver_prop_t *info1 =
                                 (const H5FD_driver_prop_t *)_info1, /* Create local aliases for values */
        *info2 = (const H5FD_driver_prop_t *)_info2;
    H5FD_class_t *cls1, *cls2;   /* Driver class for each property */
    int           cmp_value;     /* Value from comparison */
    herr_t        ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(info1);
    assert(info2);
    assert(size == sizeof(H5FD_driver_prop_t));

    /* Compare drivers */
    if (NULL == (cls1 = H5FD_get_class(info1->driver_id)))
        HGOTO_DONE(-1);
    if (NULL == (cls2 = H5FD_get_class(info2->driver_id)))
        HGOTO_DONE(1);
    if (cls1->name == NULL && cls2->name != NULL)
        HGOTO_DONE(-1);
    if (cls1->name != NULL && cls2->name == NULL)
        HGOTO_DONE(1);
    if (0 != (cmp_value = strcmp(cls1->name, cls2->name)))
        HGOTO_DONE(cmp_value);

    /* Compare driver infos */
    if (cls1->fapl_size < cls2->fapl_size)
        HGOTO_DONE(-1);
    if (cls1->fapl_size > cls2->fapl_size)
        HGOTO_DONE(1);
    assert(cls1->fapl_size == cls2->fapl_size);
    if (info1->driver_info == NULL && info2->driver_info != NULL)
        HGOTO_DONE(-1);
    if (info1->driver_info != NULL && info2->driver_info == NULL)
        HGOTO_DONE(1);
    if (info1->driver_info) {
        assert(cls1->fapl_size > 0);
        if (0 != (cmp_value = memcmp(info1->driver_info, info2->driver_info, cls1->fapl_size)))
            HGOTO_DONE(cmp_value);
    } /* end if */

    /* Compare driver configuration strings */
    if (info1->driver_config_str == NULL && info2->driver_config_str != NULL)
        HGOTO_DONE(-1);
    if (info1->driver_config_str != NULL && info2->driver_config_str == NULL)
        HGOTO_DONE(1);
    if (info1->driver_config_str) {
        if (0 != (cmp_value = strcmp(info1->driver_config_str, info2->driver_config_str)))
            HGOTO_DONE(cmp_value);
    }

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_driver_close
 *
 * Purpose:     Close callback for the file driver ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_driver_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the file driver */
    if (H5P__file_driver_free(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release file driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_driver_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_family_offset
 *
 * Purpose:     Set offset for family driver.  This file access property
 *              list will be passed to H5Fget_vfd_handle or H5FDget_vfd_handle
 *              to retrieve VFD file handle.
 *
 * Return:      Success:        Non-negative value.
 *              Failure:        Negative value.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_family_offset(hid_t fapl_id, hsize_t offset)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ih", fapl_id, offset);

    /* Get the plist structure */
    if (H5P_DEFAULT == fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't modify default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set value */
    if (H5P_set(plist, H5F_ACS_FAMILY_OFFSET_NAME, &offset) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set offset for family file");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_family_offset() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_family_offset
 *
 * Purpose:     Get offset for family driver.  This file access property
 *              list will be passed to H5Fget_vfd_handle or H5FDget_vfd_handle
 *              to retrieve VFD file handle.
 *
 * Return:      Success:        Non-negative value.
 *              Failure:        Negative value.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_family_offset(hid_t fapl_id, hsize_t *offset /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, offset);

    /* Get the plist structure */
    if (H5P_DEFAULT == fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't modify default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (offset) {
        if (H5P_get(plist, H5F_ACS_FAMILY_OFFSET_NAME, offset) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't set offset for family file");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_family_offset() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_multi_type
 *
 * Purpose:     Set data type for multi driver.  This file access property
 *              list will be passed to H5Fget_vfd_handle or H5FDget_vfd_handle
 *              to retrieve VFD file handle.
 *
 * Return:      Success:        Non-negative value.
 *              Failure:        Negative value.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_multi_type(hid_t fapl_id, H5FD_mem_t type)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iMt", fapl_id, type);

    /* Get the plist structure */
    if (H5P_DEFAULT == fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't modify default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set value */
    if (H5P_set(plist, H5F_ACS_MULTI_TYPE_NAME, &type) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set type for multi driver");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_multi_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_multi_type
 *
 * Purpose:     Get data type for multi driver.  This file access property
 *              list will be passed to H5Fget_vfd_handle or H5FDget_vfd_handle
 *              to retrieve VFD file handle.
 *
 * Return:      Success:        Non-negative value.
 *              Failure:        Negative value.
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_multi_type(hid_t fapl_id, H5FD_mem_t *type /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, type);

    /* Get the plist structure */
    if (H5P_DEFAULT == fapl_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't modify default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (type) {
        if (H5P_get(plist, H5F_ACS_MULTI_TYPE_NAME, type) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get type for multi driver");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_multi_type() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_cache
 *
 * Purpose:    Set the number of objects in the meta data cache and the
 *        maximum number of chunks and bytes in the raw data chunk
 *        cache.
 *
 *         The RDCC_W0 value should be between 0 and 1 inclusive and
 *        indicates how much chunks that have been fully read or fully
 *        written are favored for preemption.  A value of zero means
 *        fully read or written chunks are treated no differently than
 *        other chunks (the preemption is strictly LRU) while a value
 *        of one means fully read chunks are always preempted before
 *        other chunks.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_cache(hid_t plist_id, int H5_ATTR_UNUSED mdc_nelmts, size_t rdcc_nslots, size_t rdcc_nbytes,
             double rdcc_w0)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "iIszzd", plist_id, mdc_nelmts, rdcc_nslots, rdcc_nbytes, rdcc_w0);

    /* Check arguments */
    if (rdcc_w0 < 0.0 || rdcc_w0 > 1.0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "raw data cache w0 value must be between 0.0 and 1.0 inclusive");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set sizes */
    if (H5P_set(plist, H5F_ACS_DATA_CACHE_NUM_SLOTS_NAME, &rdcc_nslots) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache number of slots");
    if (H5P_set(plist, H5F_ACS_DATA_CACHE_BYTE_SIZE_NAME, &rdcc_nbytes) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set data cache byte size");
    if (H5P_set(plist, H5F_ACS_PREEMPT_READ_CHUNKS_NAME, &rdcc_w0) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set preempt read chunks");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_cache
 *
 * Purpose:    Retrieves the maximum possible number of elements in the meta
 *        data cache and the maximum possible number of elements and
 *        bytes and the RDCC_W0 value in the raw data chunk cache.  Any
 *        (or all) arguments may be null pointers in which case the
 *        corresponding datum is not returned.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_cache(hid_t plist_id, int *mdc_nelmts, size_t *rdcc_nslots /*out*/, size_t *rdcc_nbytes /*out*/,
             double *rdcc_w0 /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "i*Isxxx", plist_id, mdc_nelmts, rdcc_nslots, rdcc_nbytes, rdcc_w0);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get sizes */

    /* the mdc_nelmts FAPL entry no longer exists, so just return a constant */
    if (mdc_nelmts)
        *mdc_nelmts = 0;

    if (rdcc_nslots)
        if (H5P_get(plist, H5F_ACS_DATA_CACHE_NUM_SLOTS_NAME, rdcc_nslots) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache number of slots");
    if (rdcc_nbytes)
        if (H5P_get(plist, H5F_ACS_DATA_CACHE_BYTE_SIZE_NAME, rdcc_nbytes) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get data cache byte size");
    if (rdcc_w0)
        if (H5P_get(plist, H5F_ACS_PREEMPT_READ_CHUNKS_NAME, rdcc_w0) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get preempt read chunks");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_cache() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_mdc_image_config
 *
 * Purpose:    Set the initial metadata cache image configuration in the
 *        target FAPL.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_mdc_image_config(hid_t plist_id, H5AC_cache_image_config_t *config_ptr)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*CC", plist_id, config_ptr);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* validate the new configuration */
    if (H5AC_validate_cache_image_config(config_ptr) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid metadata cache image configuration");

    /* set the modified metadata cache image config */

    /* If we ever support multiple versions of H5AC_cache_image_config_t, we
     * will have to test the version and do translation here.
     */

    if (H5P_set(plist, H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_NAME, config_ptr) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set metadata cache image initial config");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_mdc_image_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_mdc_image_config
 *
 * Purpose:    Retrieve the metadata cache initial image configuration
 *        from the target FAPL.
 *
 *        Observe that the function will fail if config_ptr is
 *        NULL, or if config_ptr->version specifies an unknown
 *        version of H5AC_cache_image_config_t.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mdc_image_config(hid_t plist_id, H5AC_cache_image_config_t *config /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, config);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* validate the config ptr */
    if (config == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL config ptr on entry.");
    if (config->version != H5AC__CURR_CACHE_IMAGE_CONFIG_VERSION)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Unknown image config version.");

    /* If we ever support multiple versions of H5AC_cache_config_t, we
     * will have to get the canonical version here, and then translate
     * to the version of the structure supplied.
     */

    /* Get the current initial metadata cache resize configuration */
    if (H5P_get(plist, H5F_ACS_META_CACHE_INIT_IMAGE_CONFIG_NAME, config) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get metadata cache initial image config");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_mdc_image_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_mdc_config
 *
 * Purpose:    Set the initial metadata cache resize configuration in the
 *        target FAPL.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_mdc_config(hid_t plist_id, H5AC_cache_config_t *config_ptr)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*Cc", plist_id, config_ptr);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* validate the new configuration */
    if (H5AC_validate_config(config_ptr) < 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "invalid metadata cache configuration");

    /* set the modified config */

    /* If we ever support multiple versions of H5AC_cache_config_t, we
     * will have to test the version and do translation here.
     */

    if (H5P_set(plist, H5F_ACS_META_CACHE_INIT_CONFIG_NAME, config_ptr) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set metadata cache initial config");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_mdc_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_mdc_config
 *
 * Purpose:    Retrieve the metadata cache initial resize configuration
 *        from the target FAPL.
 *
 *        Observe that the function will fail if config is
 *        NULL, or if config_ptr->version specifies an unknown
 *        version of H5AC_cache_config_t.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mdc_config(hid_t plist_id, H5AC_cache_config_t *config /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, config);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* validate the config ptr */
    if (config == NULL)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL config ptr on entry.");
    if (config->version != H5AC__CURR_CACHE_CONFIG_VERSION)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Unknown config version.");

    /* If we ever support multiple versions of H5AC_cache_config_t, we
     * will have to get the canonical version here, and then translate
     * to the version of the structure supplied.
     */

    /* Get the current initial metadata cache resize configuration */
    if (H5P_get(plist, H5F_ACS_META_CACHE_INIT_CONFIG_NAME, config) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get metadata cache initial resize config");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_mdc_config() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_gc_references
 *
 * Purpose:    Sets the flag for garbage collecting references for the file.
 *        Dataset region references (and other reference types
 *        probably) use space in the file heap.  If garbage collection
 *        is on and the user passes in an uninitialized value in a
 *        reference structure, the heap might get corrupted.  When
 *        garbage collection is off however and the user re-uses a
 *        reference, the previous heap block will be orphaned and not
 *        returned to the free heap space.  When garbage collection is
 *        on, the user must initialize the reference structures to 0 or
 *        risk heap corruption.
 *
 *        Default value for garbage collecting references is off, just
 *        to be on the safe side.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_gc_references(hid_t plist_id, unsigned gc_ref)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", plist_id, gc_ref);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_GARBG_COLCT_REF_NAME, &gc_ref) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set garbage collect reference");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_gc_references
 *
 * Purpose:    Returns the current setting for the garbage collection
 *        references property from a file access property list.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_gc_references(hid_t plist_id, unsigned *gc_ref /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, gc_ref);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (gc_ref)
        if (H5P_get(plist, H5F_ACS_GARBG_COLCT_REF_NAME, gc_ref) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get garbage collect reference");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fclose_degree
 *
 * Purpose:     Sets the degree for the file close behavior.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fclose_degree(hid_t plist_id, H5F_close_degree_t degree)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iFd", plist_id, degree);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_CLOSE_DEGREE_NAME, &degree) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set file close degree");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_fclose_degree() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fclose_degree
 *
 * Purpose:     Returns the degree for the file close behavior.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fclose_degree(hid_t plist_id, H5F_close_degree_t *degree /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, degree);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (degree && H5P_get(plist, H5F_ACS_CLOSE_DEGREE_NAME, degree) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get file close degree");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fclose_degree() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_meta_block_size
 *
 * Purpose:    Sets the minimum size of metadata block allocations when
 *      the H5FD_FEAT_AGGREGATE_METADATA is set by a VFL driver.
 *      Each "raw" metadata block is allocated to be this size and then
 *      specific pieces of metadata (object headers, local heaps, B-trees, etc)
 *      are sub-allocated from this block.
 *
 *        The default value is set to 2048 (bytes), indicating that metadata
 *      will be attempted to be bunched together in (at least) 2K blocks in
 *      the file.  Setting the value to 0 with this API function will
 *      turn off the metadata aggregation, even if the VFL driver attempts to
 *      use that strategy.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_meta_block_size(hid_t plist_id, hsize_t size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ih", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_META_BLOCK_SIZE_NAME, &size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set meta data block size");

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_meta_block_size
 *
 * Purpose:    Returns the current settings for the metadata block allocation
 *      property from a file access property list.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_meta_block_size(hid_t plist_id, hsize_t *size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (size) {
        if (H5P_get(plist, H5F_ACS_META_BLOCK_SIZE_NAME, size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get meta data block size");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_sieve_buf_size
 *
 * Purpose:    Sets the maximum size of the data sieve buffer used for file
 *      drivers which are capable of using data sieving.  The data sieve
 *      buffer is used when performing I/O on datasets in the file.  Using a
 *      buffer which is large enough to hold several pieces of the dataset
 *      being read in for hyperslab selections boosts performance by quite a
 *      bit.
 *
 *        The default value is set to 64KB, indicating that file I/O for raw data
 *      reads and writes will occur in at least 64KB blocks.
 *      Setting the value to 0 with this API function will turn off the
 *      data sieving, even if the VFL driver attempts to use that strategy.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_sieve_buf_size(hid_t plist_id, size_t size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iz", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_SIEVE_BUF_SIZE_NAME, &size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set sieve buffer size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_sieve_buf_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_sieve_buf_size
 *
 * Purpose:    Returns the current settings for the data sieve buffer size
 *      property from a file access property list.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_sieve_buf_size(hid_t plist_id, size_t *size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (size)
        if (H5P_get(plist, H5F_ACS_SIEVE_BUF_SIZE_NAME, size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get sieve buffer size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_sieve_buf_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_small_data_block_size
 *
 * Purpose:    Sets the minimum size of "small" raw data block allocations
 *      when the H5FD_FEAT_AGGREGATE_SMALLDATA is set by a VFL driver.
 *      Each "small" raw data block is allocated to be this size and then
 *      pieces of raw data which are small enough to fit are sub-allocated from
 *      this block.
 *
 *    The default value is set to 2048 (bytes), indicating that raw data
 *      smaller than this value will be attempted to be bunched together in (at
 *      least) 2K blocks in the file.  Setting the value to 0 with this API
 *      function will turn off the "small" raw data aggregation, even if the
 *      VFL driver attempts to use that strategy.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_small_data_block_size(hid_t plist_id, hsize_t size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ih", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_SDATA_BLOCK_SIZE_NAME, &size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set 'small data' block size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_small_data_block_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_small_data_block_size
 *
 * Purpose:    Returns the current settings for the "small" raw data block
 *      allocation property from a file access property list.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_small_data_block_size(hid_t plist_id, hsize_t *size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (size) {
        if (H5P_get(plist, H5F_ACS_SDATA_BLOCK_SIZE_NAME, size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get 'small data' block size");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_small_data_block_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_libver_bounds
 *
 * Purpose:    Indicates which versions of the file format the library should
 *      use when creating objects.  LOW is the earliest version of the HDF5
 *      library that is guaranteed to be able to access the objects created
 *      (the format of some objects in an HDF5 file may not have changed between
 *      versions of the HDF5 library, possibly allowing earlier versions of the
 *      HDF5 library to access those objects) and HIGH is the latest version
 *      of the library required to access the objects created (later versions
 *      of the HDF5 library will also be able to access those objects).
 *
 *      LOW is used to require that objects use a more modern format and HIGH
 *      is used to restrict objects from using a more modern format.
 *
 *      The special values of H5F_FORMAT_EARLIEST and H5F_FORMAT_LATEST can be
 *      used in the following manner:  Setting LOW and HIGH to H5F_FORMAT_LATEST
 *      will produce files whose objects use the latest version of the file
 *      format available in the current HDF5 library for each object created.
 *      Setting LOW and HIGH to H5F_FORMAT_EARLIEST will produce files that that
 *      always require the use of the earliest version of the file format for
 *      each object created. [NOTE!  LOW=HIGH=H5F_FORMAT_EARLIEST is not
 *      implemented as of version 1.8.0 and setting LOW and HIGH to
 *      H5F_FORMAT_EARLIEST will produce an error currently].
 *
 *      Currently, the only two valid combinations for this routine are:
 *      LOW = H5F_FORMAT_EARLIEST and HIGH = H5F_FORMAT_LATEST (the default
 *      setting, which creates objects with the earliest version possible for
 *      each object, but no upper limit on the version allowed to be created if
 *      a newer version of an object's format is required to support a feature
 *      requested with an HDF5 library API routine), and LOW = H5F_FORMAT_LATEST
 *      and HIGH = H5F_FORMAT_LATEST (which is described above).
 *
 *      The LOW and HIGH values set with this routine at imposed with each
 *      HDF5 library API call that creates objects in the file.  API calls that
 *      would violate the LOW or HIGH format bound will fail.
 *
 *      Setting the LOW and HIGH values will not affect reading/writing existing
 *      objects, only the creation of new objects.
 *
 * Note: Eventually we want to add more values to the H5F_libver_t
 *      enumerated type that indicate library release values where the file
 *      format was changed (like "H5F_FORMAT_1_2_0" for the file format changes
 *      in the 1.2.x release branch and possibly even "H5F_FORMAT_1_4_2" for
 *      a change mid-way through the 1.4.x release branch, etc).
 *
 *      Adding more values will allow applications to make settings like the
 *      following:
 *          LOW = H5F_FORMAT_EARLIEST, HIGH = H5F_FORMAT_1_2_0 => Create objects
 *              with the earliest possible format and don't allow any objects
 *              to be created that require a library version greater than 1.2.x
 *              (This is the "make certain that <application> linked with v1.2.x
 *              of the library can read the file produced" use case)
 *
 *          LOW = H5F_FORMAT_1_4_2, HIGH = H5F_FORMAT_LATEST => create objects
 *              with at least the version of their format that the 1.4.2 library
 *              uses and allow any later version of the object's format
 *              necessary to represent features used.
 *              (This is the "make certain to take advantage of <new feature>
 *              in the file format" use case (maybe <new feature> is smaller
 *              or scales better than an earlier version, which would otherwise
 *              be used))
 *
 *         LOW = H5F_FORMAT_1_2_0, HIGH = H5F_FORMAT_1_6_0 => creates objects
 *              with at least the version of their format that the 1.2.x library
 *              uses and don't allow any objects to be created that require a
 *              library version greater than 1.6.x.
 *              (Not certain of a particular use case for these settings,
 *              although its probably just the logical combination of the
 *              previous two; it just falls out as possible/logical (if it turns
 *              out to be hard to implement in some way, we can always disallow
 *              it))
 *
 * Note #2:     We talked about whether to include enum values for only library
 *      versions where the format changed and decided it would be less confusing
 *      for application developers if we include enum values for _all_ library
 *      releases and then map down to the previous actual library release which
 *      had a format change.
 *
 * Note #3: The enumerated define for H5F_libver_t in 1.10 is:
 *      typedef enum H5F_libver_t {
 *          H5F_LIBVER_ERROR = -1,
 *          H5F_LIBVER_EARLIEST = 0,
 *          H5F_LIBVER_V18 = 1,
 *          H5F_LIBVER_V110 = 2,
 *          H5F_LIBVER_NBOUNDS
 *      } H5F_libver_t;
 *      #define H5F_LIBVER_LATEST       H5F_LIBVER_V110
 *
 *      The library supports five pairs of (low, high) combinations via H5Pset_libver_bounds():
 *      1) H5F_LIBVER_EARLIEST, H5F_LIBVER_V18
 *      2) H5F_LIBVER_EARLIEST, H5F_LIBVER_LATEST
 *      4) H5F_LIBVER_V18, H5F_LIBVER_V18
 *      4) H5F_LIBVER_V18, H5F_LIBVER_LATEST
 *      5) H5F_LIBVER_LATEST, H5F_LIBVER_LATEST
 *      See detailed description in the RFC: Setting Bounds for Object Creation in HDF5 1.10.0.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_libver_bounds(hid_t plist_id, H5F_libver_t low, H5F_libver_t high)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iFvFv", plist_id, low, high);

    /* Check args */
    if (low < 0 || low > H5F_LIBVER_LATEST)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "low bound is not valid");
    if (high < 0 || high > H5F_LIBVER_LATEST)
        HGOTO_ERROR(H5E_ARGS, H5E_BADRANGE, FAIL, "high bound is not valid");

    /* (earliest, earliest), (latest, earliest), (v18, earliest) are not valid combinations */
    if (high == H5F_LIBVER_EARLIEST)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid (low,high) combination of library version bound");

    /* (latest, v18) is not valid combination */
    if (high < low)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "Invalid (low,high) combination of library version bound");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_LIBVER_LOW_BOUND_NAME, &low) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set low bound for library format versions");
    if (H5P_set(plist, H5F_ACS_LIBVER_HIGH_BOUND_NAME, &high) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set high bound for library format versions");
done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_libver_bounds() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_libver_bounds
 *
 * Purpose:     Returns the current settings for the library version format bounds
 *              from a file access property list.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_libver_bounds(hid_t plist_id, H5F_libver_t *low /*out*/, H5F_libver_t *high /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, low, high);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (low)
        if (H5P_get(plist, H5F_ACS_LIBVER_LOW_BOUND_NAME, low) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get low bound for library format versions");

    if (high)
        if (H5P_get(plist, H5F_ACS_LIBVER_HIGH_BOUND_NAME, high) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get high bound for library format versions");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_libver_bounds() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_elink_file_cache_size
 *
 * Purpose:     Sets the number of files opened through external links
 *              from the file associated with this fapl to be held open
 *              in that file's external file cache.  When the maximum
 *              number of files is reached, the least recently used file
 *              is closed (unless it is opened from somewhere else).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_elink_file_cache_size(hid_t plist_id, unsigned efc_size)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", plist_id, efc_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set value */
    if (H5P_set(plist, H5F_ACS_EFC_SIZE_NAME, &efc_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set elink file cache size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_elink_file_cache_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_elink_file_cache_size
 *
 * Purpose:     Gets the number of files opened through external links
 *              from the file associated with this fapl to be held open
 *              in that file's external file cache.  When the maximum
 *              number of files is reached, the least recently used file
 *              is closed (unless it is opened from somewhere else).
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_elink_file_cache_size(hid_t plist_id, unsigned *efc_size /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, efc_size);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get value */
    if (efc_size)
        if (H5P_get(plist, H5F_ACS_EFC_SIZE_NAME, efc_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get elink file cache size");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_elink_file_cache_size() */

/*-------------------------------------------------------------------------
 * Function: H5Pset_file_image
 *
 * Purpose:     Sets the initial file image. Some file drivers can initialize
 *              the starting data in a file from a buffer.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_file_image(hid_t fapl_id, void *buf_ptr, size_t buf_len)
{
    H5P_genplist_t        *fapl;                /* Property list pointer */
    H5FD_file_image_info_t image_info;          /* File image info */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "i*xz", fapl_id, buf_ptr, buf_len);

    /* validate parameters */
    if (!(((buf_ptr == NULL) && (buf_len == 0)) || ((buf_ptr != NULL) && (buf_len > 0))))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "inconsistent buf_ptr and buf_len");

    /* Get the plist structure */
    if (NULL == (fapl = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get old image info */
    if (H5P_peek(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &image_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get old file image pointer");

    /* Release previous buffer, if it exists */
    if (image_info.buffer != NULL) {
        if (image_info.callbacks.image_free) {
            if (SUCCEED != image_info.callbacks.image_free(image_info.buffer,
                                                           H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET,
                                                           image_info.callbacks.udata))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "image_free callback failed");
        } /* end if */
        else
            H5MM_xfree(image_info.buffer);
    } /* end if */

    /* Update struct */
    if (buf_ptr) {
        /* Allocate memory */
        if (image_info.callbacks.image_malloc) {
            if (NULL == (image_info.buffer = image_info.callbacks.image_malloc(
                             buf_len, H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET, image_info.callbacks.udata)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "image malloc callback failed");
        } /* end if */
        else if (NULL == (image_info.buffer = H5MM_malloc(buf_len)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate memory block");

        /* Copy data */
        if (image_info.callbacks.image_memcpy) {
            if (image_info.buffer != image_info.callbacks.image_memcpy(image_info.buffer, buf_ptr, buf_len,
                                                                       H5FD_FILE_IMAGE_OP_PROPERTY_LIST_SET,
                                                                       image_info.callbacks.udata))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTCOPY, FAIL, "image_memcpy callback failed");
        } /* end if */
        else
            H5MM_memcpy(image_info.buffer, buf_ptr, buf_len);
    } /* end if */
    else
        image_info.buffer = NULL;

    image_info.size = buf_len;

    /* Set values */
    if (H5P_poke(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &image_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set file image info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_file_image() */

/*-------------------------------------------------------------------------
 * Function: H5Pget_file_image
 *
 * Purpose:     If the file image exists and buf is not NULL,
 *        allocate a buffer of the correct size, copy the image into
 *        the new buffer, and return the buffer to the caller in
 *        *buf.  Do this using the file image callbacks
 *        if defined.
 *
 *        NB: It is the responsibility of the caller to free the
 *        buffer whose address is returned in *buf.  Do
 *        this using free if the file image callbacks are not
 *        defined, or with whatever method is appropriate if
 *        the callbacks are defined.
 *
 *              If buf is not NULL, and no image exists, set
 *        *buf to NULL.
 *
 *        If buf_len is not NULL, set *buf_len equal
 *        to the length of the file image if it exists, and
 *        to 0 if it does not.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_file_image(hid_t fapl_id, void **buf /*out*/, size_t *buf_len /*out*/)
{
    H5P_genplist_t        *fapl;                /* Property list pointer */
    H5FD_file_image_info_t image_info;          /* File image info */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, buf, buf_len);

    /* Get the plist structure */
    if (NULL == (fapl = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (H5P_peek(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &image_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get file image info");

    /* verify file image field consistency */
    assert(((image_info.buffer != NULL) && (image_info.size > 0)) ||
           ((image_info.buffer == NULL) && (image_info.size == 0)));

    /* Set output size */
    if (buf_len != NULL)
        *buf_len = image_info.size;

    /* Duplicate the image if desired, using callbacks if available */
    if (buf != NULL) {
        void *copy_ptr = NULL; /* Copy of memory image */

        if (image_info.buffer != NULL) {
            /* Allocate memory */
            if (image_info.callbacks.image_malloc) {
                if (NULL ==
                    (copy_ptr = image_info.callbacks.image_malloc(
                         image_info.size, H5FD_FILE_IMAGE_OP_PROPERTY_LIST_GET, image_info.callbacks.udata)))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "image malloc callback failed");
            } /* end if */
            else if (NULL == (copy_ptr = H5MM_malloc(image_info.size)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL, "unable to allocate copy");

            /* Copy data */
            if (image_info.callbacks.image_memcpy) {
                if (copy_ptr != image_info.callbacks.image_memcpy(
                                    copy_ptr, image_info.buffer, image_info.size,
                                    H5FD_FILE_IMAGE_OP_PROPERTY_LIST_GET, image_info.callbacks.udata))
                    HGOTO_ERROR(H5E_RESOURCE, H5E_CANTCOPY, FAIL, "image_memcpy callback failed");
            } /* end if */
            else
                H5MM_memcpy(copy_ptr, image_info.buffer, image_info.size);
        } /* end if */

        *buf = copy_ptr;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_file_image */

/*-------------------------------------------------------------------------
 * Function: H5Pset_file_image_callbacks
 *
 * Purpose:     Sets the callbacks for file images. Some file drivers allow
 *              the use of user-defined callbacks for allocating, freeing and
 *              copying the drivers internal buffer, potentially allowing a
 *              clever user to do optimizations such as avoiding large mallocs
 *              and memcpys or to perform detailed logging.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_file_image_callbacks(hid_t fapl_id, H5FD_file_image_callbacks_t *callbacks_ptr)
{
    H5P_genplist_t        *fapl;                /* Property list pointer */
    H5FD_file_image_info_t info;                /* File image info */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*DI", fapl_id, callbacks_ptr);

    /* Get the plist structure */
    if (NULL == (fapl = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get old info */
    if (H5P_peek(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get old file image info");

    /* verify file image field consistency */
    assert(((info.buffer != NULL) && (info.size > 0)) || ((info.buffer == NULL) && (info.size == 0)));

    /* Make sure a file image hasn't already been set */
    if (info.buffer != NULL || info.size > 0)
        HGOTO_ERROR(
            H5E_PLIST, H5E_SETDISALLOWED, FAIL,
            "setting callbacks when an image is already set is forbidden. It could cause memory leaks.");

    /* verify that callbacks_ptr is not NULL */
    if (NULL == callbacks_ptr)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL callbacks_ptr");

    /* Make sure udata callbacks are going to be set if udata is going to be set */
    if (callbacks_ptr->udata)
        if (callbacks_ptr->udata_copy == NULL || callbacks_ptr->udata_free == NULL)
            HGOTO_ERROR(H5E_PLIST, H5E_SETDISALLOWED, FAIL, "udata callbacks must be set if udata is set");

    /* Release old udata if it exists */
    if (info.callbacks.udata != NULL) {
        assert(info.callbacks.udata_free);
        if (info.callbacks.udata_free(info.callbacks.udata) < 0)
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTFREE, FAIL, "udata_free callback failed");
    } /* end if */

    /* Update struct */
    info.callbacks = *callbacks_ptr;

    if (callbacks_ptr->udata) {
        assert(callbacks_ptr->udata_copy);
        assert(callbacks_ptr->udata_free);
        if ((info.callbacks.udata = callbacks_ptr->udata_copy(callbacks_ptr->udata)) == NULL)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't copy the supplied udata");
    } /* end if */

    /* Set values */
    if (H5P_poke(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set file image info");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_file_image_callbacks() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_file_image_callbacks
 *
 * Purpose:     Sets the callbacks for file images. Some file drivers allow
 *              the use of user-defined callbacks for allocating, freeing and
 *              copying the drivers internal buffer, potentially allowing a
 *              clever user to do optimizations such as avoiding large mallocs
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_file_image_callbacks(hid_t fapl_id, H5FD_file_image_callbacks_t *callbacks /*out*/)
{
    H5P_genplist_t        *fapl;                /* Property list pointer */
    H5FD_file_image_info_t info;                /* File image info */
    herr_t                 ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, callbacks);

    /* Get the plist structure */
    if (NULL == (fapl = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get old info */
    if (H5P_peek(fapl, H5F_ACS_FILE_IMAGE_INFO_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get file image info");

    /* verify file image field consistency */
    assert(((info.buffer != NULL) && (info.size > 0)) || ((info.buffer == NULL) && (info.size == 0)));

    /* verify that callbacks is not NULL */
    if (NULL == callbacks)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "NULL callbacks ptr");

    /* Transfer values to parameters */
    *callbacks = info.callbacks;

    /* Copy udata if it exists */
    if (info.callbacks.udata != NULL) {
        assert(info.callbacks.udata_copy);
        if ((callbacks->udata = info.callbacks.udata_copy(info.callbacks.udata)) == 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't copy udata");
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_file_image_callbacks() */

/*-------------------------------------------------------------------------
 * Function:    H5P__file_image_info_copy
 *
 * Purpose:     Copy file image info. The buffer
 *              and udata may need to be copied, possibly using their
 *              respective callbacks so the default copy won't work.
 *
 * Note:        This is an "in-place" copy, since this routine gets called
 *              after the top-level copy has been performed and this routine
 *        finishes the "deep" part of the copy.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__file_image_info_copy(void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (value) {
        H5FD_file_image_info_t *info; /* Image info struct */

        info = (H5FD_file_image_info_t *)value;

        /* verify file image field consistency */
        assert(((info->buffer != NULL) && (info->size > 0)) || ((info->buffer == NULL) && (info->size == 0)));

        if (info->buffer && info->size > 0) {
            void *old_buffer; /* Pointer to old image buffer */

            /* Store the old buffer */
            old_buffer = info->buffer;

            /* Allocate new buffer */
            if (info->callbacks.image_malloc) {
                if (NULL == (info->buffer = info->callbacks.image_malloc(
                                 info->size, H5FD_FILE_IMAGE_OP_PROPERTY_LIST_COPY, info->callbacks.udata)))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "image malloc callback failed");
            } /* end if */
            else {
                if (NULL == (info->buffer = H5MM_malloc(info->size)))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "unable to allocate memory block");
            } /* end else */

            /* Copy data to new buffer */
            if (info->callbacks.image_memcpy) {
                if (info->buffer != info->callbacks.image_memcpy(info->buffer, old_buffer, info->size,
                                                                 H5FD_FILE_IMAGE_OP_PROPERTY_LIST_COPY,
                                                                 info->callbacks.udata))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "image_memcpy callback failed");
            } /* end if */
            else
                H5MM_memcpy(info->buffer, old_buffer, info->size);
        } /* end if */

        /* Copy udata if it exists */
        if (info->callbacks.udata) {
            void *old_udata = info->callbacks.udata;

            if (NULL == info->callbacks.udata_copy)
                HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "udata_copy not defined");

            info->callbacks.udata = info->callbacks.udata_copy(old_udata);
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__file_image_info_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5P__file_image_info_free
 *
 * Purpose:     Free file image info.  The buffer and udata may need to be
 *        freed, possibly using their respective callbacks, so the
 *        default free won't work.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__file_image_info_free(void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    if (value) {
        H5FD_file_image_info_t *info; /* Image info struct */

        info = (H5FD_file_image_info_t *)value;

        /* Verify file image field consistency */
        assert(((info->buffer != NULL) && (info->size > 0)) || ((info->buffer == NULL) && (info->size == 0)));

        /* Free buffer */
        if (info->buffer != NULL && info->size > 0) {
            if (info->callbacks.image_free) {
                if ((*info->callbacks.image_free)(info->buffer, H5FD_FILE_IMAGE_OP_PROPERTY_LIST_CLOSE,
                                                  info->callbacks.udata) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "image_free callback failed");
            } /* end if */
            else
                H5MM_xfree(info->buffer);
        } /* end if */

        /* Free udata if it exists */
        if (info->callbacks.udata) {
            if (NULL == info->callbacks.udata_free)
                HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "udata_free not defined");
            if ((*info->callbacks.udata_free)(info->callbacks.udata) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "udata_free callback failed");
        } /* end if */
    }     /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__file_image_info_free() */

/*-------------------------------------------------------------------------
 * Function: H5P__facc_cache_image_config_cmp
 *
 * Purpose: Compare two cache image configurations.
 *
 * Return: positive if VALUE1 is greater than VALUE2, negative if VALUE2 is
 *        greater than VALUE1 and zero if VALUE1 and VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_cache_image_config_cmp(const void *_config1, const void *_config2, size_t H5_ATTR_UNUSED size)
{
    const H5AC_cache_image_config_t *config1 =
        (const H5AC_cache_image_config_t *)_config1; /* Create local aliases for values */
    const H5AC_cache_image_config_t *config2 =
        (const H5AC_cache_image_config_t *)_config2; /* Create local aliases for values */
    int ret_value = 0;                               /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for a property being set */
    if (config1 == NULL && config2 != NULL)
        HGOTO_DONE(-1);
    if (config1 != NULL && config2 == NULL)
        HGOTO_DONE(1);

    if (config1->version < config2->version)
        HGOTO_DONE(-1);
    if (config1->version > config2->version)
        HGOTO_DONE(1);

    if (config1->generate_image < config2->generate_image)
        HGOTO_DONE(-1);
    if (config1->generate_image > config2->generate_image)
        HGOTO_DONE(1);

    if (config1->save_resize_status < config2->save_resize_status)
        HGOTO_DONE(-1);
    if (config1->save_resize_status > config2->save_resize_status)
        HGOTO_DONE(1);

    if (config1->entry_ageout < config2->entry_ageout)
        HGOTO_DONE(-1);
    if (config1->entry_ageout > config2->entry_ageout)
        HGOTO_DONE(1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_cache_image_config_cmp() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_cache_image_config_enc
 *
 * Purpose:        Callback routine which is called whenever the default
 *                 cache image config property in the file creation
 *           property list is encoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_cache_image_config_enc(const void *value, void **_pp, size_t *size)
{
    const H5AC_cache_image_config_t *config =
        (const H5AC_cache_image_config_t *)value; /* Create local aliases for value */
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);

    if (NULL != *pp) {
        /* Encode type sizes (as a safety check) */
        *(*pp)++ = (uint8_t)sizeof(unsigned);

        INT32ENCODE(*pp, (int32_t)config->version);

        H5_ENCODE_UNSIGNED(*pp, config->generate_image);

        H5_ENCODE_UNSIGNED(*pp, config->save_resize_status);

        INT32ENCODE(*pp, (int32_t)config->entry_ageout);
    } /* end if */

    /* Compute encoded size of fixed-size values */
    *size += (1 + (2 * sizeof(unsigned)) + (2 * sizeof(int32_t)));

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_cache_image_config_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_cache_image_config_dec
 *
 * Purpose:        Callback routine which is called whenever the default
 *                 cache image config property in the file creation property
 *           list is  decoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_cache_image_config_dec(const void **_pp, void *_value)
{
    H5AC_cache_image_config_t *config = (H5AC_cache_image_config_t *)_value;
    const uint8_t            **pp     = (const uint8_t **)_pp;
    unsigned                   enc_size;
    herr_t                     ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(config);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Set property to default value */
    H5MM_memcpy(config, &H5F_def_mdc_initCacheImageCfg_g, sizeof(H5AC_cache_image_config_t));

    /* Decode type sizes */
    enc_size = *(*pp)++;
    if (enc_size != sizeof(unsigned))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "unsigned value can't be decoded");

    INT32DECODE(*pp, config->version);

    H5_DECODE_UNSIGNED(*pp, config->generate_image);

    H5_DECODE_UNSIGNED(*pp, config->save_resize_status);

    INT32DECODE(*pp, config->entry_ageout);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_cache_image_config_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_image_info_set
 *
 * Purpose:     Copies a file image property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_image_info_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                              size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of file image info */
    if (H5P__file_image_info_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file image info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_image_info_get
 *
 * Purpose:     Copies a file image property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_image_info_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                              size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of file image info */
    if (H5P__file_image_info_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file image info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_image_info_del
 *
 * Purpose:     Delete callback for the file image info property, called
 *              when the property is deleted from the plist. The buffer
 *              and udata may need to be freed, possibly using their
 *              respective callbacks so the default free won't work.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_image_info_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                              size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the file image info */
    if (H5P__file_image_info_free(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release file image info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_image_info_copy
 *
 * Purpose:     Copy callback for the file image info property. The buffer
 *              and udata may need to be copied, possibly using their
 *              respective callbacks so the default copy won't work.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_image_info_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make copy of file image info */
    if (H5P__file_image_info_copy(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy file image info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_file_image_info_cmp
 *
 * Purpose:        Callback routine which is called whenever the file image info
 *                 property in the file access property list is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                      VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                      VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_file_image_info_cmp(const void *_info1, const void *_info2, size_t H5_ATTR_UNUSED size)
{
    const H5FD_file_image_info_t *info1 = (const H5FD_file_image_info_t *)
                                     _info1, /* Create local aliases for values */
        *info2       = (const H5FD_file_image_info_t *)_info2;
    herr_t ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(info1);
    assert(info2);
    assert(size == sizeof(H5FD_file_image_info_t));

    /* Check for different buffer sizes */
    if (info1->size < info2->size)
        HGOTO_DONE(-1);
    if (info1->size > info2->size)
        HGOTO_DONE(1);

    /* Check for different callbacks */
    /* (Order in memory is fairly meaningless, so just check for equality) */
    if (info1->callbacks.image_malloc != info2->callbacks.image_malloc)
        HGOTO_DONE(1);
    if (info1->callbacks.image_memcpy != info2->callbacks.image_memcpy)
        HGOTO_DONE(-1);
    if (info1->callbacks.image_realloc != info2->callbacks.image_realloc)
        HGOTO_DONE(1);
    if (info1->callbacks.image_free != info2->callbacks.image_free)
        HGOTO_DONE(-1);
    if (info1->callbacks.udata_copy != info2->callbacks.udata_copy)
        HGOTO_DONE(1);
    if (info1->callbacks.udata_free != info2->callbacks.udata_free)
        HGOTO_DONE(-1);

    /* Check for different udata */
    /* (Don't know how big it is, so can't check contents) */
    if (info1->callbacks.udata < info2->callbacks.udata)
        HGOTO_DONE(-1);
    if (info1->callbacks.udata > info2->callbacks.udata)
        HGOTO_DONE(1);

    /* Check buffer contents (instead of buffer pointers) */
    if (info1->buffer != NULL && info2->buffer == NULL)
        HGOTO_DONE(-1);
    if (info1->buffer == NULL && info2->buffer != NULL)
        HGOTO_DONE(1);
    if (info1->buffer != NULL && info2->buffer != NULL)
        ret_value = memcmp(info1->buffer, info2->buffer, size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_file_image_info_close
 *
 * Purpose:     Close callback for the file image info property. The buffer
 *              and udata may need to be freed, possibly using their
 *              respective callbacks so the standard free won't work.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_file_image_info_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the file image info */
    if (H5P__file_image_info_free(value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release file image info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_file_image_info_close() */

/*-------------------------------------------------------------------------
 * Function: H5P__facc_cache_config_cmp
 *
 * Purpose: Compare two cache configurations.
 *
 * Return: positive if VALUE1 is greater than VALUE2, negative if VALUE2 is
 *        greater than VALUE1 and zero if VALUE1 and VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_cache_config_cmp(const void *_config1, const void *_config2, size_t H5_ATTR_UNUSED size)
{
    const H5AC_cache_config_t *config1 =
        (const H5AC_cache_config_t *)_config1; /* Create local aliases for values */
    const H5AC_cache_config_t *config2 =
        (const H5AC_cache_config_t *)_config2; /* Create local aliases for values */
    int ret_value = 0;                         /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Check for a property being set */
    if (config1 == NULL && config2 != NULL)
        HGOTO_DONE(-1);
    if (config1 != NULL && config2 == NULL)
        HGOTO_DONE(1);

    if (config1->version < config2->version)
        HGOTO_DONE(-1);
    if (config1->version > config2->version)
        HGOTO_DONE(1);

    if (config1->rpt_fcn_enabled < config2->rpt_fcn_enabled)
        HGOTO_DONE(-1);
    if (config1->rpt_fcn_enabled > config2->rpt_fcn_enabled)
        HGOTO_DONE(1);

    if (config1->evictions_enabled < config2->evictions_enabled)
        HGOTO_DONE(-1);
    if (config1->evictions_enabled > config2->evictions_enabled)
        HGOTO_DONE(1);

    if (config1->set_initial_size < config2->set_initial_size)
        HGOTO_DONE(-1);
    if (config1->set_initial_size > config2->set_initial_size)
        HGOTO_DONE(1);

    if (config1->initial_size < config2->initial_size)
        HGOTO_DONE(-1);
    if (config1->initial_size > config2->initial_size)
        HGOTO_DONE(1);

    if (config1->min_clean_fraction < config2->min_clean_fraction)
        HGOTO_DONE(-1);
    if (config1->min_clean_fraction > config2->min_clean_fraction)
        HGOTO_DONE(1);

    if (config1->max_size < config2->max_size)
        HGOTO_DONE(-1);
    if (config1->max_size > config2->max_size)
        HGOTO_DONE(1);

    if (config1->min_size < config2->min_size)
        HGOTO_DONE(-1);
    if (config1->min_size > config2->min_size)
        HGOTO_DONE(1);

    if (config1->epoch_length < config2->epoch_length)
        HGOTO_DONE(-1);
    if (config1->epoch_length > config2->epoch_length)
        HGOTO_DONE(1);

    if (config1->incr_mode < config2->incr_mode)
        HGOTO_DONE(-1);
    if (config1->incr_mode > config2->incr_mode)
        HGOTO_DONE(1);

    if (config1->lower_hr_threshold < config2->lower_hr_threshold)
        HGOTO_DONE(-1);
    if (config1->lower_hr_threshold > config2->lower_hr_threshold)
        HGOTO_DONE(1);

    if (config1->increment < config2->increment)
        HGOTO_DONE(-1);
    if (config1->increment > config2->increment)
        HGOTO_DONE(1);

    if (config1->apply_max_increment < config2->apply_max_increment)
        HGOTO_DONE(-1);
    if (config1->apply_max_increment > config2->apply_max_increment)
        HGOTO_DONE(1);

    if (config1->max_increment < config2->max_increment)
        HGOTO_DONE(-1);
    if (config1->max_increment > config2->max_increment)
        HGOTO_DONE(1);

    if (config1->flash_incr_mode < config2->flash_incr_mode)
        HGOTO_DONE(-1);
    if (config1->flash_incr_mode > config2->flash_incr_mode)
        HGOTO_DONE(1);

    if (config1->flash_multiple < config2->flash_multiple)
        HGOTO_DONE(-1);
    if (config1->flash_multiple > config2->flash_multiple)
        HGOTO_DONE(1);

    if (config1->flash_threshold < config2->flash_threshold)
        HGOTO_DONE(-1);
    if (config1->flash_threshold > config2->flash_threshold)
        HGOTO_DONE(1);

    if (config1->decr_mode < config2->decr_mode)
        HGOTO_DONE(-1);
    if (config1->decr_mode > config2->decr_mode)
        HGOTO_DONE(1);

    if (config1->upper_hr_threshold < config2->upper_hr_threshold)
        HGOTO_DONE(-1);
    if (config1->upper_hr_threshold > config2->upper_hr_threshold)
        HGOTO_DONE(1);

    if (config1->decrement < config2->decrement)
        HGOTO_DONE(-1);
    if (config1->decrement > config2->decrement)
        HGOTO_DONE(1);

    if (config1->apply_max_decrement < config2->apply_max_decrement)
        HGOTO_DONE(-1);
    if (config1->apply_max_decrement > config2->apply_max_decrement)
        HGOTO_DONE(1);

    if (config1->max_decrement < config2->max_decrement)
        HGOTO_DONE(-1);
    if (config1->max_decrement > config2->max_decrement)
        HGOTO_DONE(1);

    if (config1->epochs_before_eviction < config2->epochs_before_eviction)
        HGOTO_DONE(-1);
    if (config1->epochs_before_eviction > config2->epochs_before_eviction)
        HGOTO_DONE(1);

    if (config1->apply_empty_reserve < config2->apply_empty_reserve)
        HGOTO_DONE(-1);
    if (config1->apply_empty_reserve > config2->apply_empty_reserve)
        HGOTO_DONE(1);

    if (config1->empty_reserve < config2->empty_reserve)
        HGOTO_DONE(-1);
    if (config1->empty_reserve > config2->empty_reserve)
        HGOTO_DONE(1);

    if (config1->dirty_bytes_threshold < config2->dirty_bytes_threshold)
        HGOTO_DONE(-1);
    if (config1->dirty_bytes_threshold > config2->dirty_bytes_threshold)
        HGOTO_DONE(1);

    if (config1->metadata_write_strategy < config2->metadata_write_strategy)
        HGOTO_DONE(-1);
    if (config1->metadata_write_strategy > config2->metadata_write_strategy)
        HGOTO_DONE(1);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_cache_config_cmp() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_cache_config_enc
 *
 * Purpose:        Callback routine which is called whenever the default
 *                 cache config property in the file creation property list is
 *                 encoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_cache_config_enc(const void *value, void **_pp, size_t *size)
{
    const H5AC_cache_config_t *config =
        (const H5AC_cache_config_t *)value; /* Create local aliases for values */
    uint8_t **pp = (uint8_t **)_pp;
    unsigned  enc_size;  /* Size of encoded property */
    uint64_t  enc_value; /* Property to encode */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(value);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    if (NULL != *pp) {
        /* Encode type sizes (as a safety check) */
        *(*pp)++ = (uint8_t)sizeof(unsigned);
        *(*pp)++ = (uint8_t)sizeof(double);

        /* int */
        INT32ENCODE(*pp, (int32_t)config->version);

        H5_ENCODE_UNSIGNED(*pp, config->rpt_fcn_enabled);

        H5_ENCODE_UNSIGNED(*pp, config->open_trace_file);

        H5_ENCODE_UNSIGNED(*pp, config->close_trace_file);

        H5MM_memcpy(*pp, (const uint8_t *)(config->trace_file_name),
                    (size_t)(H5AC__MAX_TRACE_FILE_NAME_LEN + 1));
        *pp += H5AC__MAX_TRACE_FILE_NAME_LEN + 1;

        H5_ENCODE_UNSIGNED(*pp, config->evictions_enabled);

        H5_ENCODE_UNSIGNED(*pp, config->set_initial_size);

        enc_value = (uint64_t)config->initial_size;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        H5_ENCODE_DOUBLE(*pp, config->min_clean_fraction);

        enc_value = (uint64_t)config->max_size;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        enc_value = (uint64_t)config->min_size;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* long int */
        INT64ENCODE(*pp, (int64_t)config->epoch_length);

        /* enum */
        *(*pp)++ = (uint8_t)config->incr_mode;

        H5_ENCODE_DOUBLE(*pp, config->lower_hr_threshold);

        H5_ENCODE_DOUBLE(*pp, config->increment);

        H5_ENCODE_UNSIGNED(*pp, config->apply_max_increment);

        enc_value = (uint64_t)config->max_increment;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* enum */
        *(*pp)++ = (uint8_t)config->flash_incr_mode;

        H5_ENCODE_DOUBLE(*pp, config->flash_multiple);

        H5_ENCODE_DOUBLE(*pp, config->flash_threshold);

        /* enum */
        *(*pp)++ = (uint8_t)config->decr_mode;

        H5_ENCODE_DOUBLE(*pp, config->upper_hr_threshold);

        H5_ENCODE_DOUBLE(*pp, config->decrement);

        H5_ENCODE_UNSIGNED(*pp, config->apply_max_decrement);

        enc_value = (uint64_t)config->max_decrement;
        enc_size  = H5VM_limit_enc_size(enc_value);
        assert(enc_size < 256);
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* int */
        INT32ENCODE(*pp, (int32_t)config->epochs_before_eviction);

        H5_ENCODE_UNSIGNED(*pp, config->apply_empty_reserve);

        H5_ENCODE_DOUBLE(*pp, config->empty_reserve);

        /* unsigned */
        UINT32ENCODE(*pp, (uint32_t)config->dirty_bytes_threshold);

        /* int */
        INT32ENCODE(*pp, (int32_t)config->metadata_write_strategy);
    } /* end if */

    /* Compute encoded size of variably-encoded values */
    enc_value = (uint64_t)config->initial_size;
    *size += 1 + H5VM_limit_enc_size(enc_value);
    enc_value = (uint64_t)config->max_size;
    *size += 1 + H5VM_limit_enc_size(enc_value);
    enc_value = (uint64_t)config->min_size;
    *size += 1 + H5VM_limit_enc_size(enc_value);
    enc_value = (uint64_t)config->max_increment;
    *size += 1 + H5VM_limit_enc_size(enc_value);
    enc_value = (uint64_t)config->max_decrement;
    *size += 1 + H5VM_limit_enc_size(enc_value);

    /* Compute encoded size of fixed-size values */
    *size += (5 + (sizeof(unsigned) * 8) + (sizeof(double) * 8) + (sizeof(int32_t) * 4) + sizeof(int64_t) +
              H5AC__MAX_TRACE_FILE_NAME_LEN + 1);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_cache_config_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_cache_config_dec
 *
 * Purpose:        Callback routine which is called whenever the default
 *                 cache config property in the file creation property list is
 *                 decoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_cache_config_dec(const void **_pp, void *_value)
{
    H5AC_cache_config_t *config = (H5AC_cache_config_t *)_value;
    const uint8_t      **pp     = (const uint8_t **)_pp;
    unsigned             enc_size;
    uint64_t             enc_value;
    herr_t               ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(config);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Set property to default value */
    H5MM_memcpy(config, &H5F_def_mdc_initCacheCfg_g, sizeof(H5AC_cache_config_t));

    /* Decode type sizes */
    enc_size = *(*pp)++;
    if (enc_size != sizeof(unsigned))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "unsigned value can't be decoded");
    enc_size = *(*pp)++;
    if (enc_size != sizeof(double))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "double value can't be decoded");

    /* int */
    INT32DECODE(*pp, config->version);

    H5_DECODE_UNSIGNED(*pp, config->rpt_fcn_enabled);

    H5_DECODE_UNSIGNED(*pp, config->open_trace_file);

    H5_DECODE_UNSIGNED(*pp, config->close_trace_file);

    strcpy(config->trace_file_name, (const char *)(*pp));
    *pp += H5AC__MAX_TRACE_FILE_NAME_LEN + 1;

    H5_DECODE_UNSIGNED(*pp, config->evictions_enabled);

    H5_DECODE_UNSIGNED(*pp, config->set_initial_size);

    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    config->initial_size = (size_t)enc_value;

    H5_DECODE_DOUBLE(*pp, config->min_clean_fraction);

    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    config->max_size = (size_t)enc_value;

    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    config->min_size = (size_t)enc_value;

    /* long int */
    {
        int64_t temp;
        INT64DECODE(*pp, temp);
        config->epoch_length = (long int)temp;
    }
    /* enum */
    config->incr_mode = (enum H5C_cache_incr_mode) * (*pp)++;

    H5_DECODE_DOUBLE(*pp, config->lower_hr_threshold);

    H5_DECODE_DOUBLE(*pp, config->increment);

    H5_DECODE_UNSIGNED(*pp, config->apply_max_increment);

    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    config->max_increment = (size_t)enc_value;

    /* enum */
    config->flash_incr_mode = (enum H5C_cache_flash_incr_mode) * (*pp)++;

    H5_DECODE_DOUBLE(*pp, config->flash_multiple);

    H5_DECODE_DOUBLE(*pp, config->flash_threshold);

    /* enum */
    config->decr_mode = (enum H5C_cache_decr_mode) * (*pp)++;

    H5_DECODE_DOUBLE(*pp, config->upper_hr_threshold);

    H5_DECODE_DOUBLE(*pp, config->decrement);

    H5_DECODE_UNSIGNED(*pp, config->apply_max_decrement);

    enc_size = *(*pp)++;
    assert(enc_size < 256);
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    config->max_decrement = (size_t)enc_value;

    /* int */
    INT32DECODE(*pp, config->epochs_before_eviction);

    H5_DECODE_UNSIGNED(*pp, config->apply_empty_reserve);

    H5_DECODE_DOUBLE(*pp, config->empty_reserve);

    /* unsigned */
    UINT32DECODE(*pp, config->dirty_bytes_threshold);

    /* int */
    INT32DECODE(*pp, config->metadata_write_strategy);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_cache_config_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_fclose_degree_enc
 *
 * Purpose:        Callback routine which is called whenever the file close
 *                 degree property in the file access property list
 *                 is encoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_fclose_degree_enc(const void *value, void **_pp, size_t *size)
{
    const H5F_close_degree_t *fclose_degree =
        (const H5F_close_degree_t *)value; /* Create local alias for values */
    uint8_t **pp = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(fclose_degree);
    assert(size);

    if (NULL != *pp)
        /* Encode file close degree */
        *(*pp)++ = (uint8_t)*fclose_degree;

    /* Size of file close degree */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_fclose_degree_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_fclose_degree_dec
 *
 * Purpose:        Callback routine which is called whenever the file close
 *                 degree property in the file access property list
 *                 is decoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_fclose_degree_dec(const void **_pp, void *_value)
{
    H5F_close_degree_t *fclose_degree = (H5F_close_degree_t *)_value; /* File close degree */
    const uint8_t     **pp            = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(fclose_degree);

    /* Decode file close degree */
    *fclose_degree = (H5F_close_degree_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_fclose_degree_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_multi_type_enc
 *
 * Purpose:        Callback routine which is called whenever the multi VFD
 *                 memory type property in the file access property list
 *                 is encoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_multi_type_enc(const void *value, void **_pp, size_t *size)
{
    const H5FD_mem_t *type = (const H5FD_mem_t *)value; /* Create local alias for values */
    uint8_t         **pp   = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(type);
    assert(size);

    if (NULL != *pp)
        /* Encode file close degree */
        *(*pp)++ = (uint8_t)*type;

    /* Size of multi VFD memory type */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_multi_type_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_multi_type_dec
 *
 * Purpose:        Callback routine which is called whenever the multi VFD
 *                 memory type property in the file access property list
 *                 is decoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_multi_type_dec(const void **_pp, void *_value)
{
    H5FD_mem_t     *type = (H5FD_mem_t *)_value; /* File close degree */
    const uint8_t **pp   = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(type);

    /* Decode multi VFD memory type */
    *type = (H5FD_mem_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_multi_type_dec() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_libver_type_enc
 *
 * Purpose:        Callback routine which is called whenever the 'low' or
 *                 'high' bound of library format versions property in the
 *                 file access property list is encoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_libver_type_enc(const void *value, void **_pp, size_t *size)
{
    const H5F_libver_t *type = (const H5F_libver_t *)value; /* Create local alias for values */
    uint8_t           **pp   = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(type);
    assert(size);

    /* Encode */
    if (NULL != *pp)
        *(*pp)++ = (uint8_t)*type;

    /* Size */
    (*size)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_libver_type_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_libver_type_dec
 *
 * Purpose:        Callback routine which is called whenever the 'low' or
 *                 'high' bound of library format versions property in the
 *                 file access property list is decoded.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_libver_type_dec(const void **_pp, void *_value)
{
    H5F_libver_t   *type = (H5F_libver_t *)_value;
    const uint8_t **pp   = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(type);

    /* Decode */
    *type = (H5F_libver_t) * (*pp)++;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_libver_type_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_metadata_read_attempts
 *
 * Purpose:    Sets the # of read attempts in the file access property list
 *        when reading metadata with checksum.
 *        The # of read attempts set via this routine will only apply
 *        when opening a file with SWMR access.
 *        The # of read attempts set via this routine does not have
 *        any effect when opening a file with non-SWMR access; for this
 *        case, the # of read attempts will be always be 1.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_metadata_read_attempts(hid_t plist_id, unsigned attempts)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", plist_id, attempts);

    /* Cannot set the # of attempts to 0 */
    if (attempts == 0)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "number of metadatata read attempts must be greater than 0");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_METADATA_READ_ATTEMPTS_NAME, &attempts) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set # of metadata read attempts");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_metadata_read_attempts() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_metadata_read_attempts
 *
 * Purpose:    Returns the # of metadata read attempts set in the file access property list.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_metadata_read_attempts(hid_t plist_id, unsigned *attempts /*out*/)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, attempts);

    /* Get values */
    if (attempts) {
        H5P_genplist_t *plist; /* Property list pointer */

        /* Get the plist structure */
        if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

        /* Get the # of read attempts set */
        if (H5P_get(plist, H5F_ACS_METADATA_READ_ATTEMPTS_NAME, attempts) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get the number of metadata read attempts");

        /* If not set, return the default value */
        if (*attempts == H5F_ACS_METADATA_READ_ATTEMPTS_DEF) /* 0 */
            *attempts = H5F_METADATA_READ_ATTEMPTS;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_metadata_read_attempts() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_obj_flush_cb
 *
 * Purpose:    Sets the callback function to invoke and the user data when an
 *        object flush occurs in the file.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_object_flush_cb(hid_t plist_id, H5F_flush_cb_t func, void *udata)
{
    H5P_genplist_t    *plist; /* Property list pointer */
    H5F_object_flush_t flush_info;
    herr_t             ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iFF*x", plist_id, func, udata);

    /* Check if the callback function is NULL and the user data is non-NULL.
     * This is almost certainly an error as the user data will not be used. */
    if (!func && udata)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "callback is NULL while user data is not");

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Update property list */
    flush_info.func  = func;
    flush_info.udata = udata;

    /* Set values */
    if (H5P_set(plist, H5F_ACS_OBJECT_FLUSH_CB_NAME, &flush_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set object flush callback");

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_obj_flush_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_obj_flush_cb
 *
 * Purpose:    Retrieves the callback function and user data set in the
 *        property list for an object flush.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_object_flush_cb(hid_t plist_id, H5F_flush_cb_t *func /*out*/, void **udata /*out*/)
{
    H5P_genplist_t    *plist; /* Property list pointer */
    H5F_object_flush_t flush_info;
    herr_t             ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, func, udata);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Retrieve the callback function and user data */
    if (H5P_get(plist, H5F_ACS_OBJECT_FLUSH_CB_NAME, &flush_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get object flush callback");

    /* Assign return value */
    if (func)
        *func = flush_info.func;
    if (udata)
        *udata = flush_info.udata;

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_obj_flush_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_mdc_log_options
 *
 * Purpose:    Set metadata cache log options.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_mdc_log_options(hid_t plist_id, hbool_t is_enabled, const char *location, hbool_t start_on_access)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    char           *new_location;        /* Working location pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ib*sb", plist_id, is_enabled, location, start_on_access);

    /* Check arguments */
    if (H5P_DEFAULT == plist_id)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't modify default property list");
    if (!location)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "location cannot be NULL");

    /* Get the property list structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "plist_id is not a file access property list");

    /* Make a copy of the passed-in location */
    if (NULL == (new_location = H5MM_xstrdup(location)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy passed-in log location");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_USE_MDC_LOGGING_NAME, &is_enabled) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set is_enabled flag");
    if (H5P_set(plist, H5F_ACS_MDC_LOG_LOCATION_NAME, &new_location) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set log location");
    if (H5P_set(plist, H5F_ACS_START_MDC_LOG_ON_ACCESS_NAME, &start_on_access) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set start_on_access flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_mdc_log_options() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_mdc_log_options
 *
 * Purpose:    Get metadata cache log options.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mdc_log_options(hid_t plist_id, hbool_t *is_enabled /*out*/, char *location /*out*/,
                       size_t *location_size /*out*/, hbool_t *start_on_access /*out*/)
{
    H5P_genplist_t *plist;                  /* Property list pointer */
    char           *location_ptr = NULL;    /* Pointer to location string */
    herr_t          ret_value    = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE5("e", "ixxxx", plist_id, is_enabled, location, location_size, start_on_access);

    /* Get the property list structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "plist_id is not a file access property list");

    /* Get simple values */
    if (is_enabled)
        if (H5P_get(plist, H5F_ACS_USE_MDC_LOGGING_NAME, is_enabled) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get log location");
    if (start_on_access)
        if (H5P_get(plist, H5F_ACS_START_MDC_LOG_ON_ACCESS_NAME, start_on_access) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get start_on_access flag");

    /* Get the location */
    if (location || location_size)
        if (H5P_get(plist, H5F_ACS_MDC_LOG_LOCATION_NAME, &location_ptr) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get log location");

    /* Copy log location to output buffer */
    if (location_ptr && location)
        H5MM_memcpy(location, location_ptr, *location_size);

    /* Get location size, including terminating NULL */
    if (location_size) {
        if (location_ptr)
            *location_size = strlen(location_ptr) + 1;
        else
            *location_size = 0;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_mdc_log_options() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_mdc_log_location_enc
 *
 * Purpose:        Callback routine which is called whenever the metadata
 *                 cache log location property in the file access property
 *                 list is encoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mdc_log_location_enc(const void *value, void **_pp, size_t *size)
{
    const char *log_location = *(const char *const *)value;
    uint8_t   **pp           = (uint8_t **)_pp;
    size_t      len          = 0;
    uint64_t    enc_value;
    unsigned    enc_size;

    FUNC_ENTER_PACKAGE_NOERR

    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* calculate prefix length */
    if (NULL != log_location)
        len = strlen(log_location);

    enc_value = (uint64_t)len;
    enc_size  = H5VM_limit_enc_size(enc_value);
    assert(enc_size < 256);

    if (NULL != *pp) {
        /* encode the length of the prefix */
        *(*pp)++ = (uint8_t)enc_size;
        UINT64ENCODE_VAR(*pp, enc_value, enc_size);

        /* encode the prefix */
        if (NULL != log_location) {
            H5MM_memcpy(*(char **)pp, log_location, len);
            *pp += len;
        } /* end if */
    }     /* end if */

    *size += (1 + enc_size);
    if (NULL != log_location)
        *size += len;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_mdc_log_location_enc() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_mdc_log_location_dec
 *
 * Purpose:        Callback routine which is called whenever the metadata
 *                 cache log location property in the file access property
 *                 list is decoded.
 *
 * Return:         Success:     Non-negative
 *                 Failure:     Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mdc_log_location_dec(const void **_pp, void *_value)
{
    char          **log_location = (char **)_value;
    const uint8_t **pp           = (const uint8_t **)_pp;
    size_t          len;
    uint64_t        enc_value; /* Decoded property value */
    unsigned        enc_size;  /* Size of encoded property */
    herr_t          ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    assert(pp);
    assert(*pp);
    assert(log_location);
    HDcompile_assert(sizeof(size_t) <= sizeof(uint64_t));

    /* Decode the size */
    enc_size = *(*pp)++;
    assert(enc_size < 256);

    /* Decode the value */
    UINT64DECODE_VAR(*pp, enc_value, enc_size);
    len = enc_value;

    if (0 != len) {
        /* Make a copy of the user's prefix string */
        if (NULL == (*log_location = (char *)H5MM_malloc(len + 1)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTINIT, FAIL, "memory allocation failed for prefix");
        strncpy(*log_location, *(const char **)pp, len);
        (*log_location)[len] = '\0';

        *pp += len;
    } /* end if */
    else
        *log_location = NULL;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mdc_log_location_dec() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mdc_log_location_del
 *
 * Purpose:     Frees memory used to store the metadata cache log location.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mdc_log_location_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                               size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_mdc_log_location_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mdc_log_location_copy
 *
 * Purpose:     Creates a copy of the metadata cache log location string.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mdc_log_location_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    *(char **)value = H5MM_xstrdup(*(const char **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_mdc_log_location_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_mdc_log_location_cmp
 *
 * Purpose:        Callback routine which is called whenever the metadata
 *                 cache log location property in the file creation property
 *                 list is compared.
 *
 * Return:         zero if VALUE1 and VALUE2 are equal, non zero otherwise.
 *
 *-------------------------------------------------------------------------
 */
static H5_ATTR_PURE int
H5P__facc_mdc_log_location_cmp(const void *value1, const void *value2, size_t H5_ATTR_UNUSED size)
{
    const char *pref1     = *(const char *const *)value1;
    const char *pref2     = *(const char *const *)value2;
    int         ret_value = 0;

    FUNC_ENTER_PACKAGE_NOERR

    if (NULL == pref1 && NULL != pref2)
        HGOTO_DONE(1);
    if (NULL != pref1 && NULL == pref2)
        HGOTO_DONE(-1);
    if (NULL != pref1 && NULL != pref2)
        ret_value = strcmp(pref1, pref2);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mdc_log_location_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mdc_log_location_close
 *
 * Purpose:     Frees memory used to store the metadata cache log location
 *              string
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mdc_log_location_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(value);

    H5MM_xfree(*(void **)value);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__facc_mdc_log_location_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_evict_on_close
 *
 * Purpose:     Sets the evict_on_close property value.
 *
 *              When this property is set, closing an HDF5 object will
 *              cause the object's metadata cache entries to be flushed
 *              and evicted from the cache.
 *
 *              Currently only implemented for datasets.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_evict_on_close(hid_t fapl_id, hbool_t evict_on_close)
{
    H5P_genplist_t *plist;               /* property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", fapl_id, evict_on_close);

    /* Compare the property list's class against the other class */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not a file access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set value */
    if (H5P_set(plist, H5F_ACS_EVICT_ON_CLOSE_FLAG_NAME, &evict_on_close) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set evict on close property");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_evict_on_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_evict_on_close
 *
 * Purpose:     Gets the evict_on_close property value.
 *
 *              When this property is set, closing an HDF5 object will
 *              cause the object's metadata cache entries to be flushed
 *              and evicted from the cache.
 *
 *              Currently only implemented for datasets.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_evict_on_close(hid_t fapl_id, hbool_t *evict_on_close /*out*/)
{
    H5P_genplist_t *plist;               /* property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", fapl_id, evict_on_close);

    /* Compare the property list's class against the other class */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not an access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_get(plist, H5F_ACS_EVICT_ON_CLOSE_FLAG_NAME, evict_on_close) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get evict on close property");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_evict_on_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_file_locking
 *
 * Purpose:     Sets the file locking property values.
 *
 *              Overrides the default file locking flag setting that was
 *              set when the library was configured.
 *
 *              Can be overridden by the HDF5_USE_FILE_LOCKING environment
 *              variable.
 *
 *              File locking is used when creating/opening a file to prevent
 *              problematic file accesses.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_file_locking(hid_t fapl_id, hbool_t use_file_locking, hbool_t ignore_when_disabled)
{
    H5P_genplist_t *plist;               /* property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ibb", fapl_id, use_file_locking, ignore_when_disabled);

    /* Make sure this is a fapl */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not a file access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_USE_FILE_LOCKING_NAME, &use_file_locking) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set use file locking property");
    if (H5P_set(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, &ignore_when_disabled) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set ignore disabled file locks property");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_file_locking() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_file_locking
 *
 * Purpose:     Gets the file locking property values.
 *
 *              File locking is used when creating/opening a file to prevent
 *              problematic file accesses.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_file_locking(hid_t fapl_id, hbool_t *use_file_locking /*out*/, hbool_t *ignore_when_disabled /*out*/)
{
    H5P_genplist_t *plist;               /* property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, use_file_locking, ignore_when_disabled);

    /* Make sure this is a fapl */
    if (true != H5P_isa_class(fapl_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not an access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(fapl_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get values */
    if (H5P_get(plist, H5F_ACS_USE_FILE_LOCKING_NAME, use_file_locking) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get use file locking property");
    if (H5P_get(plist, H5F_ACS_IGNORE_DISABLED_FILE_LOCKS_NAME, ignore_when_disabled) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get ignore disabled file locks property");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_file_locking() */

#ifdef H5_HAVE_PARALLEL

/*-------------------------------------------------------------------------
 * Function:       H5P__encode_coll_md_read_flag_t
 *
 * Purpose:        Generic encoding callback routine for 'coll_md_read_flag' properties.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__encode_coll_md_read_flag_t(const void *value, void **_pp, size_t *size)
{
    const H5P_coll_md_read_flag_t *coll_md_read_flag = (const H5P_coll_md_read_flag_t *)value;
    uint8_t                      **pp                = (uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(coll_md_read_flag);
    assert(size);

    if (NULL != *pp) {
        /* Encode the value */
        H5MM_memcpy(*pp, coll_md_read_flag, sizeof(H5P_coll_md_read_flag_t));
        *pp += sizeof(H5P_coll_md_read_flag_t);
    } /* end if */

    /* Set size needed for encoding */
    *size += sizeof(H5P_coll_md_read_flag_t);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__encode_coll_md_read_flag_t() */

/*-------------------------------------------------------------------------
 * Function:       H5P__decode_coll_md_read_flag_t
 *
 * Purpose:        Generic decoding callback routine for 'coll_md_read_flag' properties.
 *
 * Return:       Success:    Non-negative
 *           Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P__decode_coll_md_read_flag_t(const void **_pp, void *_value)
{
    H5P_coll_md_read_flag_t *coll_md_read_flag = (H5P_coll_md_read_flag_t *)_value; /* File close degree */
    const uint8_t          **pp                = (const uint8_t **)_pp;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(pp);
    assert(*pp);
    assert(coll_md_read_flag);

    /* Decode file close degree */
    *coll_md_read_flag = (H5P_coll_md_read_flag_t) * (*pp);
    *pp += sizeof(H5P_coll_md_read_flag_t);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5P__decode_coll_md_read_flag_t() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_all_coll_metadata_ops
 *
 * Purpose:    Tell the library whether the metadata read operations will
 *             be done collectively (1) or not (0). Default is independent.
 *             With collective mode, the library will optimize access to
 *             metadata operations on the file.
 *
 * Note:    This routine accepts file access property lists, link
 *          access property lists, attribute access property lists,
 *          dataset access property lists, group access property lists
 *          and named datatype access property lists.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_all_coll_metadata_ops(hid_t plist_id, hbool_t is_collective)
{
    H5P_genplist_t         *plist;               /* Property list pointer */
    H5P_coll_md_read_flag_t coll_meta_read;      /* Property value */
    herr_t                  ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", plist_id, is_collective);

    /* Compare the property list's class against the other class */
    /* (Dataset, group, attribute, and named datatype access property lists
     *  are sub-classes of link access property lists -QAK)
     */
    if (true != H5P_isa_class(plist_id, H5P_LINK_ACCESS) && true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not an access plist");

    /* set property to either true if > 0, or false otherwise */
    if (is_collective)
        coll_meta_read = H5P_USER_TRUE;
    else
        coll_meta_read = H5P_USER_FALSE;

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5_COLL_MD_READ_FLAG_NAME, &coll_meta_read) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set collective metadata read flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_all_coll_metadata_ops() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_all_coll_metadata_ops
 *
 * Purpose:    Gets information about collective metadata read mode.
 *
 * Note:    This routine accepts file access property lists, link
 *          access property lists, attribute access property lists,
 *          dataset access property lists, group access property lists,
 *          and named datatype access property lists.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_all_coll_metadata_ops(hid_t plist_id, hbool_t *is_collective /*out*/)
{
    herr_t ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, is_collective);

    /* Compare the property list's class against the other class */
    /* (Dataset, group, attribute, and named datatype access property lists
     *  are sub-classes of link access property lists -QAK)
     */
    if (true != H5P_isa_class(plist_id, H5P_LINK_ACCESS) && true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not an access plist");

    /* Get value */
    if (is_collective) {
        H5P_coll_md_read_flag_t
                        internal_flag; /* property setting. we need to convert to either true or false */
        H5P_genplist_t *plist;         /* Property list pointer */

        /* Get the plist structure */
        if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
            HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

        if (H5P_get(plist, H5_COLL_MD_READ_FLAG_NAME, &internal_flag) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get core collective metadata read flag");

        if (internal_flag < 0)
            *is_collective = false;
        else
            *is_collective = (bool)internal_flag;
    } /* end if */

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pget_all_coll_metadata_ops */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_coll_metadata_write
 *
 * Purpose:    Tell the library whether the metadata write operations will
 *             be done collectively (1) or not (0). Default is independent.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_coll_metadata_write(hid_t plist_id, hbool_t is_collective)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ib", plist_id, is_collective);

    /* Compare the property list's class against the other class */
    if (true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not a file access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_COLL_MD_WRITE_FLAG_NAME, &is_collective) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set collective metadata write flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_coll_metadata_write() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_mpi_params
 *
 * Purpose:     Gets the MPI communicator and info stored in the fapl.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_mpi_params(hid_t plist_id, MPI_Comm *comm /*out*/, MPI_Info *info /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", plist_id, comm, info);

    /* Make sure that the property list is a fapl */
    if (true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not a file access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get the properties */
    if (H5P_get(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, comm) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get MPI communicator from plist");
    if (H5P_get(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get MPI info from plist");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_mpi_params() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_mpi_params
 *
 * Purpose:     Set the MPI communicator and info
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_mpi_params(hid_t plist_id, MPI_Comm comm, MPI_Info info)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iMcMi", plist_id, comm, info);

    /* Make sure the MPI communicator is valid */
    if (MPI_COMM_NULL == comm)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "not a valid argument");

    /* Make sure that the property list is a fapl */
    if (true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not a file access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Set values */
    if (H5P_set(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, &comm) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set MPI communicator");
    if (H5P_set(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set MPI info object");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_mpi_params() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_comm_set
 *
 * Purpose:     Copies an MPI comminicator property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_comm_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Comm *comm      = (MPI_Comm *)value;
    MPI_Comm  comm_tmp  = MPI_COMM_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI communicator */
    if (H5_mpi_comm_dup(*comm, &comm_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI communicator");

done:
    /* Copy the communicator to the in/out parameter */
    if (ret_value != SUCCEED)
        *comm = MPI_COMM_NULL;
    else
        *comm = comm_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_comm_get
 *
 * Purpose:     Copies an MPI comminicator property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_comm_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Comm *comm      = (MPI_Comm *)value;
    MPI_Comm  comm_tmp  = MPI_COMM_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI communicator */
    if (H5_mpi_comm_dup(*comm, &comm_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI communicator");

done:
    /* Copy the communicator to the out parameter */
    if (ret_value != SUCCEED)
        *comm = MPI_COMM_NULL;
    else
        *comm = comm_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_comm_del
 *
 * Purpose:     Frees an MPI communicator property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_comm_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Comm *comm      = (MPI_Comm *)value;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Free the MPI communicator */
    if (H5_mpi_comm_free(comm) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI communicator");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_comm_copy
 *
 * Purpose:     Copy callback for the MPI communicator property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_comm_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Comm *comm      = (MPI_Comm *)value;
    MPI_Comm  comm_tmp  = MPI_COMM_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI communicator */
    if (H5_mpi_comm_dup(*comm, &comm_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI communicator");

done:
    /* Copy the communicator to the in/out parameter */
    if (ret_value != SUCCEED)
        *comm = MPI_COMM_NULL;
    else
        *comm = comm_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_mpi_comm_cmp
 *
 * Purpose:        Callback routine which is called whenever the MPI
 *                 communicator property in the file access property list
 *                 is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                 VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                 VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_mpi_comm_cmp(const void *_comm1, const void *_comm2, size_t H5_ATTR_UNUSED size)
{
    const MPI_Comm *comm1     = (const MPI_Comm *)_comm1;
    const MPI_Comm *comm2     = (const MPI_Comm *)_comm2;
    int             ret_value = 0;

    FUNC_ENTER_PACKAGE

    /* Compare the MPI communicators */
    if (H5_mpi_comm_cmp(*comm1, *comm2, &ret_value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, 0, "unable to compare MPI communicator");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_comm_close
 *
 * Purpose:     Close callback for the MPI communicator property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_comm_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Comm *comm      = (MPI_Comm *)value;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Free the MPI communicator */
    if (H5_mpi_comm_free(comm) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI communicator");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_comm_close() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_info_set
 *
 * Purpose:     Copies an MPI info object property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_info_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Info *info      = (MPI_Info *)value;
    MPI_Info  info_tmp  = MPI_INFO_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI info object */
    if (H5_mpi_info_dup(*info, &info_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI info object");

done:
    /* Copy the info object to the in/out parameter */
    if (ret_value != SUCCEED)
        *info = MPI_INFO_NULL;
    else
        *info = info_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_info_get
 *
 * Purpose:     Copies an MPI comminicator property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_info_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Info *info      = (MPI_Info *)value;
    MPI_Info  info_tmp  = MPI_INFO_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI communicator */
    if (H5_mpi_info_dup(*info, &info_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI info object");

done:
    /* Copy the info object to the out parameter */
    if (ret_value != SUCCEED)
        *info = MPI_INFO_NULL;
    else
        *info = info_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_info_del
 *
 * Purpose:     Frees an MPI info object property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_info_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name,
                       size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Info *info      = (MPI_Info *)value;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Free the MPI info object */
    if (H5_mpi_info_free(info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI info object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_info_copy
 *
 * Purpose:     Copy callback for the MPI info object property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_info_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Info *info      = (MPI_Info *)value;
    MPI_Info  info_tmp  = MPI_INFO_NULL;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Make a copy of the MPI info object */
    if (H5_mpi_info_dup(*info, &info_tmp) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "unable to duplicate MPI info object");

done:
    /* Copy the info object to the in/out parameter */
    if (ret_value != SUCCEED)
        *info = MPI_INFO_NULL;
    else
        *info = info_tmp;

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_mpi_info_cmp
 *
 * Purpose:        Callback routine which is called whenever the MPI
 *                 info object property in the file access property list
 *                 is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                 VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                 VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_mpi_info_cmp(const void *_info1, const void *_info2, size_t H5_ATTR_UNUSED size)
{
    const MPI_Info *info1     = (const MPI_Info *)_info1;
    const MPI_Info *info2     = (const MPI_Info *)_info2;
    int             ret_value = 0;

    FUNC_ENTER_PACKAGE

    /* Compare the MPI info objects */
    if (H5_mpi_info_cmp(*info1, *info2, &ret_value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, 0, "unable to compare MPI info objects");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_mpi_info_close
 *
 * Purpose:     Close callback for the MPI info object property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_mpi_info_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    MPI_Info *info      = (MPI_Info *)value;
    herr_t    ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Free the MPI info object */
    if (H5_mpi_info_free(info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI info object");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_mpi_info_close() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_coll_metadata_write
 *
 * Purpose:    Gets information about collective metadata write mode.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_coll_metadata_write(hid_t plist_id, hbool_t *is_collective /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, is_collective);

    /* Compare the property list's class against the other class */
    if (true != H5P_isa_class(plist_id, H5P_FILE_ACCESS))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "property list is not an access plist");

    /* Get the plist structure */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (H5P_get(plist, H5F_ACS_COLL_MD_WRITE_FLAG_NAME, is_collective) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get collective metadata write flag");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_coll_metadata_write() */
#endif /* H5_HAVE_PARALLEL */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_page_buffer_size
 *
 * Purpose:     Set the maximum page buffering size. This has to be a
 *              multiple of the page allocation size which must be enabled;
 *              otherwise file create/open will fail.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_page_buffer_size(hid_t plist_id, size_t buf_size, unsigned min_meta_perc, unsigned min_raw_perc)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "izIuIu", plist_id, buf_size, min_meta_perc, min_raw_perc);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    if (min_meta_perc > 100)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "Minimum metadata fractions must be between 0 and 100 inclusive");
    if (min_raw_perc > 100)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "Minimum raw data fractions must be between 0 and 100 inclusive");

    if (min_meta_perc + min_raw_perc > 100)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL,
                    "Sum of minimum metadata and raw data fractions can't be bigger than 100");

    /* Set size */
    if (H5P_set(plist, H5F_ACS_PAGE_BUFFER_SIZE_NAME, &buf_size) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set page buffer size");
    if (H5P_set(plist, H5F_ACS_PAGE_BUFFER_MIN_META_PERC_NAME, &min_meta_perc) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set percentage of min metadata entries");
    if (H5P_set(plist, H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_NAME, &min_raw_perc) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set percentage of min raw data entries");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_page_buffer_size() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_page_buffer_size
 *
 * Purpose:    Retrieves the maximum page buffer size.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_page_buffer_size(hid_t plist_id, size_t *buf_size /*out*/, unsigned *min_meta_perc /*out*/,
                        unsigned *min_raw_perc /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE4("e", "ixxx", plist_id, buf_size, min_meta_perc, min_raw_perc);

    /* Get the plist structure */
    if (NULL == (plist = H5P_object_verify(plist_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, FAIL, "can't find object for ID");

    /* Get size */

    if (buf_size)
        if (H5P_get(plist, H5F_ACS_PAGE_BUFFER_SIZE_NAME, buf_size) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get page buffer size");
    if (min_meta_perc)
        if (H5P_get(plist, H5F_ACS_PAGE_BUFFER_MIN_META_PERC_NAME, min_meta_perc) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get page buffer minimum metadata percent");
    if (min_raw_perc)
        if (H5P_get(plist, H5F_ACS_PAGE_BUFFER_MIN_RAW_PERC_NAME, min_raw_perc) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get page buffer minimum raw data percent");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_page_buffer_size() */

/*-------------------------------------------------------------------------
 * Function:    H5P_set_vol
 *
 * Purpose:     Set the VOL connector for a file access property list
 *              (PLIST_ID).  The VOL properties will
 *              be copied into the property list and the reference count on
 *              the VOL will be incremented.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_set_vol(H5P_genplist_t *plist, hid_t vol_id, const void *vol_info)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    if (NULL == H5I_object_verify(vol_id, H5I_VOL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a VOL connector ID");

    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5VL_connector_prop_t vol_prop; /* Property for VOL ID & info */

        /* Prepare the VOL connector property */
        vol_prop.connector_id   = vol_id;
        vol_prop.connector_info = vol_info;

        /* Set the connector ID & info property */
        if (H5P_set(plist, H5F_ACS_VOL_CONN_NAME, &vol_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set VOL connector ID & info");
    } /* end if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_vol() */

/*-------------------------------------------------------------------------
 * Function:    H5P_reset_vol_class
 *
 * Purpose:     Change the VOL connector for a file access property class.
 *
 * Note:	The VOL property will be copied into the property list and
 *		the reference count on the previous VOL will _NOT_ be decremented.
 *		The reference count on the new VOL will _NOT_ be incremented.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_reset_vol_class(const H5P_genclass_t *pclass, const H5VL_connector_prop_t *vol_prop)
{
    H5VL_connector_prop_t old_vol_prop;        /* Previous VOL connector property */
    herr_t                ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Get the connector ID & info property */
    if (H5P__class_get(pclass, H5F_ACS_VOL_CONN_NAME, &old_vol_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL connector ID & info");

    /* Set the new connector ID & info property */
    if (H5P__class_set(pclass, H5F_ACS_VOL_CONN_NAME, vol_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set VOL connector ID & info");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_set_vol_class() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_vol
 *
 * Purpose:     Set the file VOL connector (VOL_ID) for a file access
 *              property list (PLIST_ID)
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_vol(hid_t plist_id, hid_t new_vol_id, const void *new_vol_info)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ii*x", plist_id, new_vol_id, new_vol_info);

    /* Check arguments */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (NULL == H5I_object_verify(new_vol_id, H5I_VOL))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file VOL ID");

    /* Set the VOL */
    if (H5P_set_vol(plist, new_vol_id, new_vol_info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set VOL");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_vol() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_vol_id
 *
 * Purpose:     Returns the ID of the current VOL connector.
 *              This ID should be closed with H5VLclose().
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_vol_id(hid_t plist_id, hid_t *vol_id /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, vol_id);

    if (H5P_DEFAULT == plist_id)
        plist_id = H5P_FILE_ACCESS_DEFAULT;

    /* Get property list for ID */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Get the current VOL ID */
    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        H5VL_connector_prop_t connector_prop; /* Property for VOL connector ID & info */

        /* Get the connector property */
        if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL connector info");

        /* Increment the VOL ID's ref count */
        if (H5I_inc_ref(connector_prop.connector_id, true) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINC, FAIL, "unable to increment ref count on VOL connector ID");

        /* Set the connector ID to return */
        *vol_id = connector_prop.connector_id;
    } /* end if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_vol_id() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_vol_info
 *
 * Purpose:     Returns a copy of the VOL info for a connector.
 *              This information should be freed with H5VLfree_connector_info.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_vol_info(hid_t plist_id, void **vol_info /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", plist_id, vol_info);

    /* Get property list for ID */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

    /* Get the current VOL info */
    if (true == H5P_isa_class(plist->plist_id, H5P_FILE_ACCESS)) {
        void                 *new_connector_info = NULL; /* Copy of connector info */
        H5VL_connector_prop_t connector_prop;            /* Property for VOL connector ID & info */

        /* Get the connector property */
        if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL connector property");

        /* Copy connector info, if it exists */
        if (connector_prop.connector_info) {
            H5VL_class_t *connector; /* Pointer to connector */

            /* Retrieve the connector for the ID */
            if (NULL == (connector = (H5VL_class_t *)H5I_object(connector_prop.connector_id)))
                HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a VOL connector ID");

            /* Allocate and copy connector info */
            if (H5VL_copy_connector_info(connector, &new_connector_info, connector_prop.connector_info) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "connector info copy failed");
        } /* end if */

        /* Set the connector info */
        *vol_info = new_connector_info;
    } /* end if */
    else
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_vol_info() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_vol_cap_flags
 *
 * Purpose:     Queries the current VOL connector information for a FAPL to
 *              retrieve the capability flags for the VOL connector stack, as will
 *              be used by a file open or create operation that uses this FAPL.
 *
 *              Current capability flags are:
 *                  H5VL_CAP_FLAG_THREADSAFE   - Connector is threadsafe
 *                  H5VL_CAP_FLAG_ASYNC        - Connector performs operations asynchronously
 *                  H5VL_CAP_FLAG_NATIVE_FILES - Connector produces native file format
 *
 * Note:        This routine supports the use of the HDF5_VOL_CONNECTOR environment
 *              environment variable to override the VOL connector set programmatically
 *              for the FAPL (with H5Pset_vol).
 *
 * Note:        The H5VL_CAP_FLAG_ASYNC flag can be checked to see if asynchronous
 *              operations are supported by the VOL connector stack.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_vol_cap_flags(hid_t plist_id, uint64_t *cap_flags)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "i*UL", plist_id, cap_flags);

    /* Get the 'cap_flags' from the connector */
    if (cap_flags) {
        if (H5P_DEFAULT == plist_id)
            plist_id = H5P_FILE_ACCESS_DEFAULT;

        if (true == H5P_isa_class(plist_id, H5P_FILE_ACCESS)) {
            H5P_genplist_t       *plist;          /* Property list pointer */
            H5VL_connector_prop_t connector_prop; /* Property for VOL connector ID & info */

            /* Get property list for ID */
            if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
                HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");

            /* Get the connector property */
            if (H5P_peek(plist, H5F_ACS_VOL_CONN_NAME, &connector_prop) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL connector property");

            /* Query the capability flags */
            if (H5VL_get_cap_flags(&connector_prop, cap_flags) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get VOL connector capability flags");
        }
        else
            HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    }

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_vol_cap_flags() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_create
 *
 connectorose:     Create callback for the VOL connector ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_create(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make copy of the VOL connector */
    if (H5VL_conn_copy((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_create() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_set
 *
 * Purpose:     Copies a VOL connector property when it's set for a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_set(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                  void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of VOL connector ID & info */
    if (H5VL_conn_copy((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_set() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_get
 *
 * Purpose:     Copies a VOL connector property when it's retrieved from a property list
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_get(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                  void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(value);

    /* Make copy of VOL connector */
    if (H5VL_conn_copy((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_get() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_del
 *
 * Purpose:     Frees memory used to store the VOL connector ID & info property
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_del(hid_t H5_ATTR_UNUSED prop_id, const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size,
                  void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the VOL connector ID & info */
    if (H5VL_conn_free((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_del() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_copy
 *
 * Purpose:     Copy callback for the VOL connector ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_copy(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Make copy of VOL connector */
    if (H5VL_conn_copy((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_copy() */

/*-------------------------------------------------------------------------
 * Function:       H5P__facc_vol_cmp
 *
 * Purpose:        Callback routine which is called whenever the VOL connector
 *                 ID & info property in the file access property list
 *                 is compared.
 *
 * Return:         positive if VALUE1 is greater than VALUE2, negative if
 *                 VALUE2 is greater than VALUE1 and zero if VALUE1 and
 *                 VALUE2 are equal.
 *
 *-------------------------------------------------------------------------
 */
static int
H5P__facc_vol_cmp(const void *_info1, const void *_info2, size_t H5_ATTR_UNUSED size)
{
    const H5VL_connector_prop_t *info1 =
        (const H5VL_connector_prop_t *)_info1; /* Create local aliases for values */
    const H5VL_connector_prop_t *info2 = (const H5VL_connector_prop_t *)_info2;
    H5VL_class_t                *cls1, *cls2;   /* connector class for each property */
    int                          cmp_value = 0; /* Value from comparison */
    herr_t H5_ATTR_NDEBUG_UNUSED status;        /* Status from info comparison */
    int                          ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(info1);
    assert(info2);
    assert(size == sizeof(H5VL_connector_prop_t));

    /* Compare connectors */
    if (NULL == (cls1 = (H5VL_class_t *)H5I_object(info1->connector_id)))
        HGOTO_DONE(-1);
    if (NULL == (cls2 = (H5VL_class_t *)H5I_object(info2->connector_id)))
        HGOTO_DONE(1);
    status = H5VL_cmp_connector_cls(&cmp_value, cls1, cls2);
    assert(status >= 0);
    if (cmp_value != 0)
        HGOTO_DONE(cmp_value);

    /* At this point, we should be able to assume that we are dealing with
     * the same connector class struct (or a copies of the same class struct)
     */

    /* Use one of the classes (cls1) info comparison routines to compare the
     * info objects
     */
    assert(cls1->info_cls.cmp == cls2->info_cls.cmp);
    status = H5VL_cmp_connector_info(cls1, &cmp_value, info1->connector_info, info2->connector_info);
    assert(status >= 0);

    /* Set return value */
    ret_value = cmp_value;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_cmp() */

/*-------------------------------------------------------------------------
 * Function:    H5P__facc_vol_close
 *
 * Purpose:     Close callback for the VOL connector ID & info property.
 *
 * Return:      Success:        Non-negative
 *              Failure:        Negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__facc_vol_close(const char H5_ATTR_UNUSED *name, size_t H5_ATTR_UNUSED size, void *value)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Free the VOL connector */
    if (H5VL_conn_free((H5VL_connector_prop_t *)value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "can't release VOL connector");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__facc_vol_close() */
