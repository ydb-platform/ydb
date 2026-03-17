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
 * Purpose:     Connector/container introspection callbacks for the native VOL connector
 *
 */

/****************/
/* Module Setup */
/****************/

/***********/
/* Headers */
/***********/
#include "H5private.h"   /* Generic Functions                        */
#include "H5Eprivate.h"  /* Error handling                           */
#include "H5VLprivate.h" /* Virtual Object Layer                     */

#include "H5VLnative_private.h" /* Native VOL connector                     */

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/********************/
/* Local Prototypes */
/********************/

/*********************/
/* Package Variables */
/*********************/

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Note: H5VL__native_introspect_get_conn_cls and H5VL__native_introspect_get_cap_flags
 *      are in src/H5VLnative.c so that they can work with the statically declared
 *      class struct.
 */

/*---------------------------------------------------------------------------
 * Function:    H5VL__native_introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Returns:     SUCCEED (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
herr_t
H5VL__native_introspect_opt_query(void H5_ATTR_UNUSED *obj, H5VL_subclass_t subcls, int opt_type,
                                  uint64_t *flags)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(flags);

    /* The native VOL connector supports all optional operations */
    *flags = H5VL_OPT_QUERY_SUPPORTED;

    /* Set appropriate flags for each operation in each subclass */
    switch (subcls) {
        case H5VL_SUBCLS_NONE:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional 'none' operation");

        case H5VL_SUBCLS_INFO:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional info operation");

        case H5VL_SUBCLS_WRAP:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional wrapper operation");

        case H5VL_SUBCLS_ATTR:
            switch (opt_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                case H5VL_NATIVE_ATTR_ITERATE_OLD:
                    /* Don't allow asynchronous execution, due to iterator callbacks */
                    *flags |= H5VL_OPT_QUERY_NO_ASYNC;
                    break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional attribute operation");
                    break;
            } /* end switch */
            break;

        case H5VL_SUBCLS_DATASET:
            switch (opt_type) {
                case H5VL_NATIVE_DATASET_FORMAT_CONVERT:
                    *flags |= H5VL_OPT_QUERY_MODIFY_METADATA;
                    break;

                case H5VL_NATIVE_DATASET_GET_CHUNK_INDEX_TYPE:
                case H5VL_NATIVE_DATASET_GET_CHUNK_STORAGE_SIZE:
                case H5VL_NATIVE_DATASET_GET_NUM_CHUNKS:
                case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_IDX:
                case H5VL_NATIVE_DATASET_GET_CHUNK_INFO_BY_COORD:
                case H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE:
                case H5VL_NATIVE_DATASET_GET_OFFSET:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    break;

                case H5VL_NATIVE_DATASET_CHUNK_READ:
                    *flags |= H5VL_OPT_QUERY_READ_DATA;
                    break;

                case H5VL_NATIVE_DATASET_CHUNK_WRITE:
                    *flags |= H5VL_OPT_QUERY_WRITE_DATA;
                    break;

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional dataset operation");
                    break;
            } /* end switch */
            break;

        case H5VL_SUBCLS_DATATYPE:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional datatype operation");

        case H5VL_SUBCLS_FILE:
            switch (opt_type) {
                case H5VL_NATIVE_FILE_CLEAR_ELINK_CACHE:
                case H5VL_NATIVE_FILE_RESET_MDC_HIT_RATE:
                case H5VL_NATIVE_FILE_SET_MDC_CONFIG:
                    *flags |= H5VL_OPT_QUERY_MODIFY_METADATA;
                    break;

                case H5VL_NATIVE_FILE_GET_FILE_IMAGE:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    *flags |= H5VL_OPT_QUERY_READ_DATA;
                    break;

                case H5VL_NATIVE_FILE_GET_FREE_SECTIONS:
                case H5VL_NATIVE_FILE_GET_FREE_SPACE:
                case H5VL_NATIVE_FILE_GET_INFO:
                case H5VL_NATIVE_FILE_GET_MDC_CONF:
                case H5VL_NATIVE_FILE_GET_MDC_HR:
                case H5VL_NATIVE_FILE_GET_MDC_SIZE:
                case H5VL_NATIVE_FILE_GET_SIZE:
                case H5VL_NATIVE_FILE_GET_VFD_HANDLE:
                case H5VL_NATIVE_FILE_GET_METADATA_READ_RETRY_INFO:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    break;

                case H5VL_NATIVE_FILE_START_SWMR_WRITE:
                    *flags |= H5VL_OPT_QUERY_MODIFY_METADATA;
                    *flags |= H5VL_OPT_QUERY_WRITE_DATA;
                    *flags |= H5VL_OPT_QUERY_NO_ASYNC;
                    break;

                case H5VL_NATIVE_FILE_START_MDC_LOGGING:
                case H5VL_NATIVE_FILE_STOP_MDC_LOGGING:
                case H5VL_NATIVE_FILE_GET_MDC_LOGGING_STATUS:
                case H5VL_NATIVE_FILE_FORMAT_CONVERT:
                case H5VL_NATIVE_FILE_RESET_PAGE_BUFFERING_STATS:
                case H5VL_NATIVE_FILE_GET_PAGE_BUFFERING_STATS:
                case H5VL_NATIVE_FILE_GET_MDC_IMAGE_INFO:
                case H5VL_NATIVE_FILE_GET_EOA:
                case H5VL_NATIVE_FILE_INCR_FILESIZE:
                case H5VL_NATIVE_FILE_SET_LIBVER_BOUNDS:
                case H5VL_NATIVE_FILE_GET_MIN_DSET_OHDR_FLAG:
                case H5VL_NATIVE_FILE_SET_MIN_DSET_OHDR_FLAG:
#ifdef H5_HAVE_PARALLEL
                case H5VL_NATIVE_FILE_GET_MPI_ATOMICITY:
                case H5VL_NATIVE_FILE_SET_MPI_ATOMICITY:
#endif /* H5_HAVE_PARALLEL */
                case H5VL_NATIVE_FILE_POST_OPEN:
                    break;

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional file operation");
                    break;
            } /* end switch */
            break;

        case H5VL_SUBCLS_GROUP:
            switch (opt_type) {
#ifndef H5_NO_DEPRECATED_SYMBOLS
                case H5VL_NATIVE_GROUP_ITERATE_OLD:
                    /* Don't allow asynchronous execution, due to iterator callbacks */
                    *flags |= H5VL_OPT_QUERY_NO_ASYNC;
                    break;

                case H5VL_NATIVE_GROUP_GET_OBJINFO:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    break;
#endif /* H5_NO_DEPRECATED_SYMBOLS */

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional group operation");
                    break;
            } /* end switch */
            break;

        case H5VL_SUBCLS_LINK:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional link operation");

        case H5VL_SUBCLS_OBJECT:
            switch (opt_type) {
                case H5VL_NATIVE_OBJECT_GET_COMMENT:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    break;

                case H5VL_NATIVE_OBJECT_SET_COMMENT:
                    *flags |= H5VL_OPT_QUERY_MODIFY_METADATA;
                    break;

                case H5VL_NATIVE_OBJECT_DISABLE_MDC_FLUSHES:
                case H5VL_NATIVE_OBJECT_ENABLE_MDC_FLUSHES:
                case H5VL_NATIVE_OBJECT_ARE_MDC_FLUSHES_DISABLED:
                    break;

                case H5VL_NATIVE_OBJECT_GET_NATIVE_INFO:
                    *flags |= H5VL_OPT_QUERY_QUERY_METADATA;
                    break;

                default:
                    HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional object operation");
                    break;
            } /* end switch */
            break;

        case H5VL_SUBCLS_REQUEST:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional request operation");

        case H5VL_SUBCLS_BLOB:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional blob operation");

        case H5VL_SUBCLS_TOKEN:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown optional token operation");

        default:
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "unknown H5VL subclass");
    } /* end switch */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5VL__native_introspect_opt_query() */
