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
 * Purpose:     This is the MPI-2 I/O driver.
 */

#include "H5FDdrvr_module.h" /* This source code file is part of the H5FD driver module */

#include "H5private.h"   /* Generic Functions                    */
#include "H5CXprivate.h" /* API Contexts                         */
#include "H5Dprivate.h"  /* Dataset functions                    */
#include "H5Eprivate.h"  /* Error handling                       */
#include "H5Fprivate.h"  /* File access                          */
#include "H5FDprivate.h" /* File drivers                         */
#include "H5FDmpi.h"     /* MPI-based file drivers               */
#include "H5Iprivate.h"  /* IDs                                  */
#include "H5MMprivate.h" /* Memory management                    */
#include "H5Pprivate.h"  /* Property lists                       */

#ifdef H5_HAVE_PARALLEL

/*
 * The driver identification number, initialized at runtime if H5_HAVE_PARALLEL
 * is defined. This allows applications to still have the H5FD_MPIO
 * "constants" in their source code.
 */
static hid_t H5FD_MPIO_g = 0;

/* Whether to allow collective I/O operations */
/* (Can be changed by setting "HDF5_MPI_OPT_TYPES" environment variable to '0' or '1') */
hbool_t H5FD_mpi_opt_types_g = true;

/* Whether the driver initialized MPI on its own */
static bool H5FD_mpi_self_initialized = false;

/*
 * The view is set to this value
 */
static char H5FD_mpi_native_g[] = "native";

/*
 * The description of a file belonging to this driver.
 * The EOF value is only used just after the file is opened in order for the
 * library to determine whether the file is empty, truncated, or okay. The MPIO
 * driver doesn't bother to keep it updated since it's an expensive operation.
 */
typedef struct H5FD_mpio_t {
    H5FD_t   pub;                    /* Public stuff, must be first                  */
    MPI_File f;                      /* MPIO file handle                             */
    MPI_Comm comm;                   /* MPI Communicator                             */
    MPI_Info info;                   /* MPI info object                              */
    int      mpi_rank;               /* This process's rank                          */
    int      mpi_size;               /* Total number of processes                    */
    haddr_t  eof;                    /* End-of-file marker                           */
    haddr_t  eoa;                    /* End-of-address marker                        */
    haddr_t  last_eoa;               /* Last known end-of-address marker             */
    haddr_t  local_eof;              /* Local end-of-file address for each process   */
    bool     mpi_file_sync_required; /* Whether the ROMIO driver requires MPI_File_sync after write */
} H5FD_mpio_t;

/* Private Prototypes */

/* Callbacks */
static herr_t  H5FD__mpio_term(void);
static H5FD_t *H5FD__mpio_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD__mpio_close(H5FD_t *_file);
static herr_t  H5FD__mpio_query(const H5FD_t *_f1, unsigned long *flags);
static haddr_t H5FD__mpio_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__mpio_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD__mpio_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD__mpio_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static herr_t  H5FD__mpio_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                               void *buf);
static herr_t  H5FD__mpio_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                const void *buf);
static herr_t  H5FD__mpio_read_vector(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, uint32_t count,
                                      H5FD_mem_t types[], haddr_t addrs[], size_t sizes[], void *bufs[]);
static herr_t  H5FD__mpio_write_vector(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, uint32_t count,
                                       H5FD_mem_t types[], haddr_t addrs[], size_t sizes[],
                                       const void *bufs[]);

static herr_t H5FD__mpio_read_selection(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id,
                                        size_t count, hid_t mem_space_ids[], hid_t file_space_ids[],
                                        haddr_t offsets[], size_t element_sizes[], void *bufs[]);

static herr_t H5FD__mpio_write_selection(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id,
                                         size_t count, hid_t mem_space_ids[], hid_t file_space_ids[],
                                         haddr_t offsets[], size_t element_sizes[], const void *bufs[]);

static herr_t H5FD__mpio_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t H5FD__mpio_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t H5FD__mpio_delete(const char *filename, hid_t fapl_id);
static herr_t H5FD__mpio_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input,
                             void **output);

/* Other functions */
static herr_t H5FD__mpio_vector_build_types(uint32_t count, H5FD_mem_t types[], haddr_t addrs[],
                                            size_t sizes[], H5_flexible_const_ptr_t bufs[],
                                            haddr_t *s_addrs[], size_t *s_sizes[],
                                            H5_flexible_const_ptr_t *s_bufs[], bool *vector_was_sorted,
                                            MPI_Offset *mpi_off, H5_flexible_const_ptr_t *mpi_bufs_base,
                                            int *size_i, MPI_Datatype *buf_type, bool *buf_type_created,
                                            MPI_Datatype *file_type, bool *file_type_created, char *unused);

static herr_t H5FD__selection_build_types(bool io_op_write, size_t num_pieces, H5_flexible_const_ptr_t mbb,
                                          H5S_t **file_spaces, H5S_t **mem_spaces, haddr_t offsets[],
                                          H5_flexible_const_ptr_t bufs[], size_t src_element_sizes[],
                                          size_t dst_element_sizes[], MPI_Datatype *final_ftype,
                                          bool *final_ftype_is_derived, MPI_Datatype *final_mtype,
                                          bool *final_mtype_is_derived);

/* The MPIO file driver information */
static const H5FD_class_t H5FD_mpio_g = {
    H5FD_CLASS_VERSION,         /* struct version        */
    H5_VFD_MPIO,                /* value                 */
    "mpio",                     /* name                  */
    HADDR_MAX,                  /* maxaddr               */
    H5F_CLOSE_SEMI,             /* fc_degree             */
    H5FD__mpio_term,            /* terminate             */
    NULL,                       /* sb_size               */
    NULL,                       /* sb_encode             */
    NULL,                       /* sb_decode             */
    0,                          /* fapl_size             */
    NULL,                       /* fapl_get              */
    NULL,                       /* fapl_copy             */
    NULL,                       /* fapl_free             */
    0,                          /* dxpl_size             */
    NULL,                       /* dxpl_copy             */
    NULL,                       /* dxpl_free             */
    H5FD__mpio_open,            /* open                  */
    H5FD__mpio_close,           /* close                 */
    NULL,                       /* cmp                   */
    H5FD__mpio_query,           /* query                 */
    NULL,                       /* get_type_map          */
    NULL,                       /* alloc                 */
    NULL,                       /* free                  */
    H5FD__mpio_get_eoa,         /* get_eoa               */
    H5FD__mpio_set_eoa,         /* set_eoa               */
    H5FD__mpio_get_eof,         /* get_eof               */
    H5FD__mpio_get_handle,      /* get_handle            */
    H5FD__mpio_read,            /* read                  */
    H5FD__mpio_write,           /* write                 */
    H5FD__mpio_read_vector,     /* read_vector           */
    H5FD__mpio_write_vector,    /* write_vector          */
    H5FD__mpio_read_selection,  /* read_selection     */
    H5FD__mpio_write_selection, /* write_selection    */
    H5FD__mpio_flush,           /* flush                 */
    H5FD__mpio_truncate,        /* truncate              */
    NULL,                       /* lock                  */
    NULL,                       /* unlock                */
    H5FD__mpio_delete,          /* del                   */
    H5FD__mpio_ctl,             /* ctl                   */
    H5FD_FLMAP_DICHOTOMY        /* fl_map                */
};

#ifdef H5FDmpio_DEBUG
/* Flags to control debug actions in the MPI-IO VFD.
 * (Meant to be indexed by characters)
 *
 * These flags can be set with either (or both) the environment variable
 *      "H5FD_mpio_Debug" set to a string containing one or more characters
 *      (flags) or by setting them as a string value for the
 *      "H5F_mpio_debug_key" MPI Info key.
 *
 * Supported characters in 'H5FD_mpio_Debug' string:
 *      't' trace function entry and exit
 *      'r' show read offset and size
 *      'w' show write offset and size
 *      '0'-'9' only show output from a single MPI rank (ranks 0-9 supported)
 */
static int H5FD_mpio_debug_flags_s[256];
static int H5FD_mpio_debug_rank_s = -1;

/* Indicate if this rank should output tracing info */
#define H5FD_MPIO_TRACE_THIS_RANK(file)                                                                      \
    (H5FD_mpio_debug_rank_s < 0 || H5FD_mpio_debug_rank_s == (file)->mpi_rank)
#endif

#ifdef H5FDmpio_DEBUG

/*---------------------------------------------------------------------------
 * Function:    H5FD__mpio_parse_debug_str
 *
 * Purpose:     Parse a string for debugging flags
 *
 * Returns:     N/A
 *
 *---------------------------------------------------------------------------
 */
static void
H5FD__mpio_parse_debug_str(const char *s)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(s);

    /* Set debug mask */
    while (*s) {
        if ((int)(*s) >= (int)'0' && (int)(*s) <= (int)'9')
            H5FD_mpio_debug_rank_s = ((int)*s) - (int)'0';
        else
            H5FD_mpio_debug_flags_s[(int)*s]++;
        s++;
    } /* end while */

    FUNC_LEAVE_NOAPI_VOID
} /* end H5FD__mpio_parse_debug_str() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__mem_t_to_str
 *
 * Purpose:     Returns a string representing the enum value in an H5FD_mem_t
 *              enum
 *
 * Returns:     H5FD_mem_t enum value string
 *
 *---------------------------------------------------------------------------
 */
static const char *
H5FD__mem_t_to_str(H5FD_mem_t mem_type)
{
    switch (mem_type) {
        case H5FD_MEM_NOLIST:
            return "H5FD_MEM_NOLIST";
        case H5FD_MEM_DEFAULT:
            return "H5FD_MEM_DEFAULT";
        case H5FD_MEM_SUPER:
            return "H5FD_MEM_SUPER";
        case H5FD_MEM_BTREE:
            return "H5FD_MEM_BTREE";
        case H5FD_MEM_DRAW:
            return "H5FD_MEM_DRAW";
        case H5FD_MEM_GHEAP:
            return "H5FD_MEM_GHEAP";
        case H5FD_MEM_LHEAP:
            return "H5FD_MEM_LHEAP";
        case H5FD_MEM_OHDR:
            return "H5FD_MEM_OHDR";
        case H5FD_MEM_NTYPES:
            return "H5FD_MEM_NTYPES";
        default:
            return "(Unknown)";
    }
}
#endif /* H5FDmpio_DEBUG */

/*-------------------------------------------------------------------------
 * Function:    H5FD_mpio_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the mpio driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_mpio_init(void)
{
    static int H5FD_mpio_Debug_inited = 0;
    char      *env                    = NULL;
    hid_t      ret_value              = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    /* Register the MPI-IO VFD, if it isn't already */
    if (H5I_VFL != H5I_get_type(H5FD_MPIO_g)) {
        H5FD_MPIO_g = H5FD_register((const H5FD_class_t *)&H5FD_mpio_g, sizeof(H5FD_class_t), false);

        /* Check if MPI driver has been loaded dynamically */
        env = getenv(HDF5_DRIVER);
        if (env && !strcmp(env, "mpio")) {
            int mpi_initialized = 0;

            /* Initialize MPI if not already initialized */
            if (MPI_SUCCESS != MPI_Initialized(&mpi_initialized))
                HGOTO_ERROR(H5E_VFL, H5E_UNINITIALIZED, H5I_INVALID_HID, "can't check if MPI is initialized");
            if (!mpi_initialized) {
                if (MPI_SUCCESS != MPI_Init(NULL, NULL))
                    HGOTO_ERROR(H5E_VFL, H5E_CANTINIT, H5I_INVALID_HID, "can't initialize MPI");
                H5FD_mpi_self_initialized = true;
            }
        }
    }

    if (!H5FD_mpio_Debug_inited) {
        const char *s; /* String for environment variables */

        /* Allow MPI buf-and-file-type optimizations? */
        s = getenv("HDF5_MPI_OPT_TYPES");
        if (s && isdigit(*s))
            H5FD_mpi_opt_types_g = (0 == strtol(s, NULL, 0)) ? false : true;

#ifdef H5FDmpio_DEBUG
        /* Clear the flag buffer */
        memset(H5FD_mpio_debug_flags_s, 0, sizeof(H5FD_mpio_debug_flags_s));

        /* Retrieve MPI-IO debugging environment variable */
        s = getenv("H5FD_mpio_Debug");
        if (s)
            H5FD__mpio_parse_debug_str(s);
#endif /* H5FDmpio_DEBUG */

        H5FD_mpio_Debug_inited++;
    } /* end if */

    /* Set return value */
    ret_value = H5FD_MPIO_g;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_mpio_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD__mpio_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_term(void)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Terminate MPI if the driver initialized it */
    if (H5FD_mpi_self_initialized) {
        int mpi_finalized = 0;

        MPI_Finalized(&mpi_finalized);
        if (!mpi_finalized)
            MPI_Finalize();

        H5FD_mpi_self_initialized = false;
    }

    /* Reset VFL ID */
    H5FD_MPIO_g = 0;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mpio_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_mpio
 *
 * Purpose:     Store the user supplied MPIO communicator comm and info in
 *              the file access property list FAPL_ID which can then be used
 *              to create and/or open the file.  This function is available
 *              only in the parallel HDF5 library and is not collective.
 *
 *              comm is the MPI communicator to be used for file open as
 *              defined in MPI_FILE_OPEN of MPI-2. This function makes a
 *              duplicate of comm. Any modification to comm after this function
 *              call returns has no effect on the access property list.
 *
 *              info is the MPI Info object to be used for file open as
 *              defined in MPI_FILE_OPEN of MPI-2. This function makes a
 *              duplicate of info. Any modification to info after this
 *              function call returns has no effect on the access property
 *              list.
 *
 *              If fapl_id has previously set comm and info values, they
 *              will be replaced and the old communicator and Info object
 *              are freed.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_mpio(hid_t fapl_id, MPI_Comm comm, MPI_Info info)
{
    H5P_genplist_t *plist; /* Property list pointer */
    herr_t          ret_value;

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "iMcMi", fapl_id, comm, info);

    /* Check arguments */
    if (fapl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access list");
    if (MPI_COMM_NULL == comm)
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "MPI_COMM_NULL is not a valid communicator");

    /* Set the MPI communicator and info object */
    if (H5P_set(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, &comm) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set MPI communicator");
    if (H5P_set(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, &info) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "can't set MPI info object");

    /* duplication is done during driver setting. */
    ret_value = H5P_set_driver(plist, H5FD_MPIO, NULL, NULL);

done:
    FUNC_LEAVE_API(ret_value)
} /* H5Pset_fapl_mpio() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_mpio
 *
 * Purpose:     If the file access property list is set to the H5FD_MPIO
 *              driver then this function returns duplicates of the MPI
 *              communicator and Info object stored through the comm and
 *              info pointers.  It is the responsibility of the application
 *              to free the returned communicator and Info object.
 *
 * Return:      Success:    Non-negative with the communicator and
 *                          Info object returned through the comm and
 *                          info arguments if non-null. Since they are
 *                          duplicates of the stored objects, future
 *                          modifications to the access property list do
 *                          not affect them and it is the responsibility
 *                          of the application to free them.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_mpio(hid_t fapl_id, MPI_Comm *comm /*out*/, MPI_Info *info /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE3("e", "ixx", fapl_id, comm, info);

    /* Set comm and info in case we have problems */
    if (comm)
        *comm = MPI_COMM_NULL;
    if (info)
        *info = MPI_INFO_NULL;

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a file access list");
    if (H5FD_MPIO != H5P_peek_driver(plist))
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "VFL driver is not MPI-I/O");

    /* Get the MPI communicator and info object */
    if (comm)
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, comm) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get MPI communicator");
    if (info)
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, info) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTGET, FAIL, "can't get MPI info object");

done:
    /* Clean up anything duplicated on errors. The free calls will set
     * the output values to MPI_COMM|INFO_NULL.
     */
    if (ret_value != SUCCEED) {
        if (comm)
            if (H5_mpi_comm_free(comm) < 0)
                HDONE_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI communicator");
        if (info)
            if (H5_mpi_info_free(info) < 0)
                HDONE_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "unable to free MPI info object");
    }

    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_fapl_mpio() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_mpio
 *
 * Purpose:     Set the data transfer property list DXPL_ID to use transfer
 *              mode XFER_MODE. The property list can then be used to control
 *              the I/O transfer mode during data I/O operations. The valid
 *              transfer modes are:
 *
 *              H5FD_MPIO_INDEPENDENT:
 *                  Use independent I/O access (the default).
 *
 *              H5FD_MPIO_COLLECTIVE:
 *                  Use collective I/O access.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_mpio(hid_t dxpl_id, H5FD_mpio_xfer_t xfer_mode)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDt", dxpl_id, xfer_mode);

    /* Check arguments */
    if (dxpl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");
    if (H5FD_MPIO_INDEPENDENT != xfer_mode && H5FD_MPIO_COLLECTIVE != xfer_mode)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "incorrect xfer_mode");

    /* Set the transfer mode */
    if (H5P_set(plist, H5D_XFER_IO_XFER_MODE_NAME, &xfer_mode) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dxpl_mpio() */

/*-------------------------------------------------------------------------
 * Function:    H5Pget_dxpl_mpio
 *
 * Purpose:     Queries the transfer mode current set in the data transfer
 *              property list DXPL_ID. This is not collective.
 *
 * Return:      Success:    Non-negative, with the transfer mode returned
 *                          through the XFER_MODE argument if it is
 *                          non-null.
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_dxpl_mpio(hid_t dxpl_id, H5FD_mpio_xfer_t *xfer_mode /*out*/)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "ix", dxpl_id, xfer_mode);

    /* Check arguments */
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Get the transfer mode */
    if (xfer_mode)
        if (H5P_get(plist, H5D_XFER_IO_XFER_MODE_NAME, xfer_mode) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to get value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pget_dxpl_mpio() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_mpio_collective_opt
 *
 * Purpose:     Set the data transfer property list DXPL_ID to use transfer
 *              mode OPT_MODE during I/O. This allows the application to
 *              specify collective I/O at the HDF5 interface level (with
 *              the H5Pset_dxpl_mpio routine), while controlling whether
 *              the actual I/O is performed collectively (e.g., via
 *              MPI_File_write_at_all) or independently (e.g., via
 *              MPI_File_write_at).
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_mpio_collective_opt(hid_t dxpl_id, H5FD_mpio_collective_opt_t opt_mode)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDc", dxpl_id, opt_mode);

    /* Check arguments */
    if (dxpl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the transfer mode */
    if (H5P_set(plist, H5D_XFER_MPIO_COLLECTIVE_OPT_NAME, &opt_mode) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dxpl_mpio_collective_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_mpio_chunk_opt
 *
 * Purpose:     To set a flag to choose linked chunk I/O or multi-chunk I/O
 *              without involving decision-making inside HDF5
 *
 * Note:        The library will do linked chunk I/O or multi-chunk I/O without
 *              involving communications for decision-making process.
 *              The library won't behave as it asks for only when we find
 *              that the low-level MPI-IO package doesn't support this.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_mpio_chunk_opt(hid_t dxpl_id, H5FD_mpio_chunk_opt_t opt_mode)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iDh", dxpl_id, opt_mode);

    /* Check arguments */
    if (dxpl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the transfer mode */
    if (H5P_set(plist, H5D_XFER_MPIO_CHUNK_OPT_HARD_NAME, &opt_mode) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dxpl_mpio_chunk_opt() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_mpio_chunk_opt_num
 *
 * Purpose:     To set a threshold for doing linked chunk IO
 *
 * Note:        If the number is greater than the threshold set by the user,
 *              the library will do linked chunk I/O; otherwise, I/O will be
 *              done for every chunk.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_mpio_chunk_opt_num(hid_t dxpl_id, unsigned num_chunk_per_proc)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", dxpl_id, num_chunk_per_proc);

    /* Check arguments */
    if (dxpl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the transfer mode */
    if (H5P_set(plist, H5D_XFER_MPIO_CHUNK_OPT_NUM_NAME, &num_chunk_per_proc) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dxpl_mpio_chunk_opt_num() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_dxpl_mpio_chunk_opt_ratio
 *
 * Purpose:     To set a threshold for doing collective I/O for each chunk
 *
 * Note:        The library will calculate the percentage of the number of
 *              process holding selections at each chunk. If that percentage
 *              of number of process in the individual chunk is greater than
 *              the threshold set by the user, the library will do collective
 *              chunk I/O for this chunk; otherwise, independent I/O will be
 *              done for this chunk.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_dxpl_mpio_chunk_opt_ratio(hid_t dxpl_id, unsigned percent_num_proc_per_chunk)
{
    H5P_genplist_t *plist;               /* Property list pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_API(FAIL)
    H5TRACE2("e", "iIu", dxpl_id, percent_num_proc_per_chunk);

    /* Check arguments */
    if (dxpl_id == H5P_DEFAULT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "can't set values in default property list");
    if (NULL == (plist = H5P_object_verify(dxpl_id, H5P_DATASET_XFER)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL, "not a dxpl");

    /* Set the transfer mode */
    if (H5P_set(plist, H5D_XFER_MPIO_CHUNK_OPT_RATIO_NAME, &percent_num_proc_per_chunk) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to set value");

done:
    FUNC_LEAVE_API(ret_value)
} /* end H5Pset_dxpl_mpio_chunk_opt_ratio() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_set_mpio_atomicity
 *
 * Purpose:     Sets the atomicity mode
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_set_mpio_atomicity(H5FD_t *_file, bool flag)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* set atomicity value */
    if (MPI_SUCCESS != (mpi_code = MPI_File_set_atomicity(file->f, (int)(flag != false))))
        HMPI_GOTO_ERROR(FAIL, "MPI_File_set_atomicity", mpi_code)

done:
#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_set_mpio_atomicity() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_get_mpio_atomicity
 *
 * Purpose:     Returns the atomicity mode
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5FD_get_mpio_atomicity(H5FD_t *_file, bool *flag)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    int          temp_flag;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI_NOINIT

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Get atomicity value */
    if (MPI_SUCCESS != (mpi_code = MPI_File_get_atomicity(file->f, &temp_flag)))
        HMPI_GOTO_ERROR(FAIL, "MPI_File_get_atomicity", mpi_code)

    if (0 != temp_flag)
        *flag = true;
    else
        *flag = false;

done:
#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD_get_mpio_atomicity() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_open
 *
 * Purpose:     Opens a file with name NAME.  The FLAGS are a bit field with
 *              purpose similar to the second argument of open(2) and which
 *              are defined in H5Fpublic.h. The file access property list
 *              FAPL_ID contains the properties driver properties and MAXADDR
 *              is the largest address which this file will be expected to
 *              access.  This is collective.
 *
 * Return:      Success:    A new file pointer
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD__mpio_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t H5_ATTR_UNUSED maxaddr)
{
    H5FD_mpio_t    *file = NULL;          /* VFD File struct for new file */
    H5P_genplist_t *plist;                /* Property list pointer */
    MPI_Comm        comm = MPI_COMM_NULL; /* MPI Communicator, from plist */
    MPI_Info        info = MPI_INFO_NULL; /* MPI Info, from plist */
    MPI_Info        info_used;            /* MPI Info returned from MPI_File_open */
    MPI_File        fh;                   /* MPI file handle */
    bool            file_opened = false;  /* Flag to indicate that the file was successfully opened */
    int             mpi_amode;            /* MPI file access flags */
    int             mpi_rank = INT_MAX;   /* MPI rank of this process */
    int             mpi_size;             /* Total number of MPI processes */
    MPI_Offset      file_size;            /* File size (of existing files) */
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = false;
#endif
    int     mpi_code;         /* MPI return code */
    H5FD_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Get a pointer to the fapl */
    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, NULL, "not a file access property list");

    if (H5FD_mpi_self_initialized) {
        comm = MPI_COMM_WORLD;
    }
    else {
        /* Get the MPI communicator and info object from the property list */
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, &comm) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get MPI communicator");
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, &info) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "can't get MPI info object");
    }

    /* Get the MPI rank of this process and the total number of processes */
    if (MPI_SUCCESS != (mpi_code = MPI_Comm_rank(comm, &mpi_rank)))
        HMPI_GOTO_ERROR(NULL, "MPI_Comm_rank failed", mpi_code)
    if (MPI_SUCCESS != (mpi_code = MPI_Comm_size(comm, &mpi_size)))
        HMPI_GOTO_ERROR(NULL, "MPI_Comm_size failed", mpi_code)

#ifdef H5FDmpio_DEBUG
    H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] &&
                              (H5FD_mpio_debug_rank_s < 0 || H5FD_mpio_debug_rank_s == mpi_rank));
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering - name = \"%s\", flags = 0x%x, fapl_id = %d, maxaddr = %lu\n",
                __func__, mpi_rank, name, flags, (int)fapl_id, (unsigned long)maxaddr);
#endif

    /* Convert HDF5 flags to MPI-IO flags */
    /* Some combinations are illegal; let MPI-IO figure it out */
    mpi_amode = (flags & H5F_ACC_RDWR) ? MPI_MODE_RDWR : MPI_MODE_RDONLY;
    if (flags & H5F_ACC_CREAT)
        mpi_amode |= MPI_MODE_CREATE;
    if (flags & H5F_ACC_EXCL)
        mpi_amode |= MPI_MODE_EXCL;

#ifdef H5FDmpio_DEBUG
    /* Check for debug commands in the info parameter */
    if (MPI_INFO_NULL != info) {
        char debug_str[128];
        int  flag;

        MPI_Info_get(info, H5F_MPIO_DEBUG_KEY, sizeof(debug_str) - 1, debug_str, &flag);
        if (flag)
            H5FD__mpio_parse_debug_str(debug_str);
    } /* end if */
#endif

    if (MPI_SUCCESS != (mpi_code = MPI_File_open(comm, name, mpi_amode, info, &fh)))
        HMPI_GOTO_ERROR(NULL, "MPI_File_open failed", mpi_code)
    file_opened = true;

    /* Get the MPI-IO hints that actually used by MPI-IO underneath. */
    if (MPI_SUCCESS != (mpi_code = MPI_File_get_info(fh, &info_used)))
        HMPI_GOTO_ERROR(NULL, "MPI_File_get_info failed", mpi_code)

    /* Copy hints in info_used into info. Note hints in info_used supersede
     * info. There may be some hints set and used by HDF5 only, but not
     * recognizable by MPI-IO. We need to keep them, as MPI_File_get_info()
     * will remove any hints unrecognized by MPI-IO library underneath.
     */
    if (info_used != MPI_INFO_NULL) {
        int i, nkeys;

        if (info == MPI_INFO_NULL) /* reuse info created from MPI_File_get_info() */
            info = info_used;
        else {
            /* retrieve the number of hints */
            if (MPI_SUCCESS != (mpi_code = MPI_Info_get_nkeys(info_used, &nkeys)))
                HMPI_GOTO_ERROR(NULL, "MPI_Info_get_nkeys failed", mpi_code)

            /* copy over each hint */
            for (i = 0; i < nkeys; i++) {
                char key[MPI_MAX_INFO_KEY], value[MPI_MAX_INFO_VAL];
                int  valuelen, flag;

                /* retrieve the nth hint */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get_nthkey(info_used, i, key)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Info_get_nkeys failed", mpi_code)
                /* retrieve the key of nth hint */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get_valuelen(info_used, key, &valuelen, &flag)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Info_get_valuelen failed", mpi_code)
                /* retrieve the value of nth hint */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_get(info_used, key, valuelen + 1, value, &flag)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Info_get failed", mpi_code)

                /* copy the hint into info */
                if (MPI_SUCCESS != (mpi_code = MPI_Info_set(info, key, value)))
                    HMPI_GOTO_ERROR(NULL, "MPI_Info_set failed", mpi_code)
            }

            /* Free info_used allocated in the call to MPI_File_get_info() */
            if (MPI_SUCCESS != (mpi_code = MPI_Info_free(&info_used)))
                HMPI_GOTO_ERROR(NULL, "MPI_Info_free failed", mpi_code)
        }
        /* Add info to the file access property list */
        if (H5P_set(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, &info) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTSET, NULL, "can't set MPI info object");
    }

    /* Build the return value and initialize it */
    if (NULL == (file = (H5FD_mpio_t *)H5MM_calloc(sizeof(H5FD_mpio_t))))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
    file->f        = fh;
    file->comm     = comm;
    file->info     = info;
    file->mpi_rank = mpi_rank;
    file->mpi_size = mpi_size;

    /* Retrieve the flag indicating whether MPI_File_sync is needed after each write */
    if (H5_mpio_get_file_sync_required(fh, &file->mpi_file_sync_required) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, NULL, "unable to get mpi_file_sync_required hint");

    /* Only processor p0 will get the file size and broadcast it. */
    if (mpi_rank == 0) {
        /* If MPI_File_get_size fails, broadcast file size as -1 to signal error */
        if (MPI_SUCCESS != (mpi_code = MPI_File_get_size(fh, &file_size)))
            file_size = (MPI_Offset)-1;
    }

    /* Broadcast file size */
    if (MPI_SUCCESS != (mpi_code = MPI_Bcast(&file_size, (int)sizeof(MPI_Offset), MPI_BYTE, 0, comm)))
        HMPI_GOTO_ERROR(NULL, "MPI_Bcast failed", mpi_code)

    if (file_size < 0)
        HMPI_GOTO_ERROR(NULL, "MPI_File_get_size failed", mpi_code)

    /* Determine if the file should be truncated */
    if (file_size && (flags & H5F_ACC_TRUNC)) {
        /* Truncate the file */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_size(fh, (MPI_Offset)0)))
            HMPI_GOTO_ERROR(NULL, "MPI_File_set_size failed", mpi_code)

        /* Don't let any proc return until all have truncated the file. */
        if (MPI_SUCCESS != (mpi_code = MPI_Barrier(comm)))
            HMPI_GOTO_ERROR(NULL, "MPI_Barrier failed", mpi_code)

        /* File is zero size now */
        file_size = 0;
    } /* end if */

    /* Set the size of the file (from library's perspective) */
    file->eof       = H5FD_mpi_MPIOff_to_haddr(file_size);
    file->local_eof = file->eof;

    /* Set return value */
    ret_value = (H5FD_t *)file;

done:
    if (ret_value == NULL) {
        if (file_opened)
            MPI_File_close(&fh);
        if (H5_mpi_comm_free(&comm) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTFREE, NULL, "unable to free MPI communicator");
        if (H5_mpi_info_free(&info) < 0)
            HDONE_ERROR(H5E_VFL, H5E_CANTFREE, NULL, "unable to free MPI info object");
        if (file)
            H5MM_xfree(file);
    } /* end if */

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_open() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_close
 *
 * Purpose:     Closes a file.  This is collective.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_close(H5FD_t *_file)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    int  mpi_rank               = file->mpi_rank;
#endif
    int    mpi_code;            /* MPI return code */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    /* MPI_File_close sets argument to MPI_FILE_NULL */
    if (MPI_SUCCESS != (mpi_code = MPI_File_close(&(file->f))))
        HMPI_GOTO_ERROR(FAIL, "MPI_File_close failed", mpi_code)

    /* Clean up other stuff */
    H5_mpi_comm_free(&file->comm);
    H5_mpi_info_free(&file->info);
    H5MM_xfree(file);

done:
#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_close() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_query
 *
 * Purpose:     Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_query(const H5FD_t H5_ATTR_UNUSED *_file, unsigned long *flags /* out */)
{
    FUNC_ENTER_PACKAGE_NOERR

    /* Set the VFL feature flags that this driver supports */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_AGGREGATE_METADATA;  /* OK to aggregate metadata allocations  */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */
        *flags |= H5FD_FEAT_HAS_MPI; /* This driver uses MPI                                             */
        *flags |= H5FD_FEAT_DEFAULT_VFD_COMPATIBLE; /* VFD creates a file which can be opened with the default
                                                       VFD */
    }                                               /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mpio_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_get_eoa
 *
 * Purpose:     Gets the end-of-address marker for the file. The EOA marker
 *              is the first address past the last byte allocated in the
 *              format address space.
 *
 * Return:      Success:    The end-of-address marker
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__mpio_get_eoa(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_mpio_t *file = (const H5FD_mpio_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    FUNC_LEAVE_NOAPI(file->eoa)
} /* end H5FD__mpio_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_set_eoa
 *
 * Purpose:     Set the end-of-address marker for the file. This function is
 *              called shortly after an existing HDF5 file is opened in order
 *              to tell the driver where the end of the HDF5 data is located.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_set_eoa(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, haddr_t addr)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    file->eoa = addr;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* end H5FD__mpio_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_get_eof
 *
 * Purpose:     Gets the end-of-file marker for the file. The EOF marker
 *              is the real size of the file.
 *
 *              The MPIO driver doesn't bother keeping this field updated
 *              since that's a relatively expensive operation. Fortunately
 *              the library only needs the EOF just after the file is opened
 *              in order to determine whether the file is empty, truncated,
 *              or okay.  Therefore, any MPIO I/O function will set its value
 *              to HADDR_UNDEF which is the error return value of this
 *              function.
 *
 *              Keeping the EOF updated (during write calls) is expensive
 *              because any process may extend the physical end of the
 *              file. -QAK
 *
 * Return:      Success:    The end-of-file marker
 *              Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD__mpio_get_eof(const H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type)
{
    const H5FD_mpio_t *file = (const H5FD_mpio_t *)_file;

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    FUNC_LEAVE_NOAPI(file->eof)
} /* end H5FD__mpio_get_eof() */

/*-------------------------------------------------------------------------
 * Function:       H5FD__mpio_get_handle
 *
 * Purpose:        Returns the file handle of MPIO file driver.
 *
 * Returns:        SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_get_handle(H5FD_t *_file, hid_t H5_ATTR_UNUSED fapl, void **file_handle)
{
    H5FD_mpio_t *file      = (H5FD_mpio_t *)_file;
    herr_t       ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    if (!file_handle)
        HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "file handle not valid");

    *file_handle = &(file->f);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_get_handle() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_read
 *
 * Purpose:     Reads SIZE bytes of data from FILE beginning at address ADDR
 *              into buffer BUF according to data transfer properties in
 *              DXPL_ID using potentially complex file and buffer types to
 *              effect the transfer.
 *
 *              Reading past the end of the MPI file returns zeros instead of
 *              failing.  MPI is able to coalesce requests from different
 *              processes (collective or independent).
 *
 * Return:      Success:    SUCCEED. Result is stored in caller-supplied
 *                          buffer BUF.
 *
 *              Failure:    FAIL. Contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_read(H5FD_t *_file, H5FD_mem_t H5_ATTR_UNUSED type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr,
                size_t size, void *buf /*out*/)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    MPI_Offset   mpi_off;
    MPI_Status   mpi_stat;            /* Status from I/O operation */
    MPI_Datatype buf_type = MPI_BYTE; /* MPI description of the selection in memory */
    int          size_i;              /* Integer copy of 'size' to read */
#if H5_CHECK_MPI_VERSION(3, 0)
    MPI_Count bytes_read = 0; /* Number of bytes read in */
    MPI_Count type_size;      /* MPI datatype used for I/O's size */
    MPI_Count io_size;        /* Actual number of bytes requested */
    MPI_Count n;
#else
    int bytes_read = 0; /* Number of bytes read in */
    int type_size;      /* MPI datatype used for I/O's size */
    int io_size;        /* Actual number of bytes requested */
    int n;
#endif
    bool use_view_this_time = false;
    bool derived_type       = false;
    bool rank0_bcast        = false; /* If read-with-rank0-and-bcast flag was used */
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_r_flag = (H5FD_mpio_debug_flags_s[(int)'r'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert(buf);

    /* Portably initialize MPI status variable */
    memset(&mpi_stat, 0, sizeof(MPI_Status));

    /* some numeric conversions */
    if (H5FD_mpi_haddr_to_MPIOff(addr, &mpi_off /*out*/) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");
    size_i = (int)size;

    /* Only look for MPI views for raw data transfers */
    if (type == H5FD_MEM_DRAW) {
        H5FD_mpio_xfer_t xfer_mode; /* I/O transfer mode */

        /* Get the transfer mode from the API context */
        if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

        /*
         * Set up for a fancy xfer using complex types, or single byte block. We
         * wouldn't need to rely on the use_view field if MPI semantics allowed
         * us to test that btype=ftype=MPI_BYTE (or even MPI_TYPE_NULL, which
         * could mean "use MPI_BYTE" by convention).
         */
        if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
            MPI_Datatype file_type;

            /* Remember that views are used */
            use_view_this_time = true;

            /* Prepare for a full-blown xfer using btype, ftype, and displacement */
            if (H5CX_get_mpi_coll_datatypes(&buf_type, &file_type) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O datatypes");

            /*
             * Set the file view when we are using MPI derived types
             */
            if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, file_type,
                                                             H5FD_mpi_native_g, file->info)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)

            /* When using types, use the address as the displacement for
             * MPI_File_set_view and reset the address for the read to zero
             */
            mpi_off = 0;
        } /* end if */
    }     /* end if */

    /* Read the data. */
    if (use_view_this_time) {
        H5FD_mpio_collective_opt_t coll_opt_mode;

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stderr, "%s: (%d) using MPIO collective mode\n", __func__, file->mpi_rank);
#endif
        /* Get the collective_opt property to check whether the application wants to do IO individually. */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stderr, "%s: (%d) doing MPI collective IO\n", __func__, file->mpi_rank);
#endif
            /* Check whether we should read from rank 0 and broadcast to other ranks */
            if (H5CX_get_mpio_rank0_bcast()) {
#ifdef H5FDmpio_DEBUG
                if (H5FD_mpio_debug_r_flag)
                    fprintf(stderr, "%s: (%d) doing read-rank0-and-MPI_Bcast\n", __func__, file->mpi_rank);
#endif
                /* Indicate path we've taken */
                rank0_bcast = true;

                /* Read on rank 0 Bcast to other ranks */
                if (file->mpi_rank == 0) {
                    /* If MPI_File_read_at fails, push an error, but continue
                     * to participate in following MPI_Bcast */
                    if (MPI_SUCCESS !=
                        (mpi_code = MPI_File_read_at(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
                        HMPI_DONE_ERROR(FAIL, "MPI_File_read_at failed", mpi_code)
                }

                if (MPI_SUCCESS != (mpi_code = MPI_Bcast(buf, size_i, buf_type, 0, file->comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_code)
            } /* end if */
            else
                /* Perform collective read operation */
                if (MPI_SUCCESS !=
                    (mpi_code = MPI_File_read_at_all(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at_all failed", mpi_code)
        } /* end if */
        else {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif

            /* Perform independent read operation */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_read_at(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at failed", mpi_code)
        } /* end else */

        /*
         * Reset the file view when we used MPI derived types
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)
    } /* end if */
    else {
        if (size != (hsize_t)size_i) {
            /* If HERE, then we need to work around the integer size limit
             * of 2GB. The input size_t size variable cannot fit into an integer,
             * but we can get around that limitation by creating a different datatype
             * and then setting the integer size (or element count) to 1 when using
             * the derived_type.
             */

            if (H5_mpio_create_large_type(size, 0, MPI_BYTE, &buf_type) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");

            derived_type = true;
            size_i       = 1;
        }

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif

        /* Perform independent read operation */
        if (MPI_SUCCESS != (mpi_code = MPI_File_read_at(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at failed", mpi_code)
    } /* end else */

    /* Only retrieve bytes read if this rank _actually_ participated in I/O */
    if (!rank0_bcast || (rank0_bcast && file->mpi_rank == 0)) {
        /* How many bytes were actually read? */
#if H5_CHECK_MPI_VERSION(3, 0)
        if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, buf_type, &bytes_read))) {
#else
        if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_read))) {
#endif
            if (rank0_bcast && file->mpi_rank == 0) {
                /* If MPI_Get_elements(_x) fails for a rank 0 bcast strategy,
                 * push an error, but continue to participate in the following
                 * MPI_Bcast.
                 */
                bytes_read = -1;
                HMPI_DONE_ERROR(FAIL, "MPI_Get_elements failed for rank 0", mpi_code)
            }
            else
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code)
        }
    } /* end if */

    /* If the rank0-bcast feature was used, broadcast the # of bytes read to
     * other ranks, which didn't perform any I/O.
     */
    /* NOTE: This could be optimized further to be combined with the broadcast
     *          of the data.  (QAK - 2019/1/2)
     */
    if (rank0_bcast)
#if H5_CHECK_MPI_VERSION(3, 0)
        if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_COUNT, 0, file->comm))
#else
        if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_INT, 0, file->comm))
#endif
            HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", 0)

            /* Get the type's size */
#if H5_CHECK_MPI_VERSION(3, 0)
    if (MPI_SUCCESS != (mpi_code = MPI_Type_size_x(buf_type, &type_size)))
#else
    if (MPI_SUCCESS != (mpi_code = MPI_Type_size(buf_type, &type_size)))
#endif
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_size failed", mpi_code)

    /* Compute the actual number of bytes requested */
    io_size = type_size * size_i;

    /* Check for read failure */
    if (bytes_read < 0 || bytes_read > io_size)
        HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed");

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_r_flag)
        fprintf(stderr, "%s: (%d) mpi_off = %ld  bytes_read = %lld  type = %s\n", __func__, file->mpi_rank,
                (long)mpi_off, (long long)bytes_read, H5FD__mem_t_to_str(type));
#endif

    /*
     * This gives us zeroes beyond end of physical MPI file.
     */
    if ((n = (io_size - bytes_read)) > 0)
        memset((char *)buf + bytes_read, 0, (size_t)n);

done:
    if (derived_type)
        MPI_Type_free(&buf_type);

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_write
 *
 * Purpose:     Writes SIZE bytes of data to FILE beginning at address ADDR
 *              from buffer BUF according to data transfer properties in
 *              DXPL_ID using potentially complex file and buffer types to
 *              effect the transfer.
 *
 *              MPI is able to coalesce requests from different processes
 *              (collective and independent).
 *
 * Return:      Success:    SUCCEED. USE_TYPES and OLD_USE_TYPES in the
 *                          access params are altered.
 *              Failure:    FAIL. USE_TYPES and OLD_USE_TYPES in the
 *                          access params may be altered.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_write(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, haddr_t addr, size_t size,
                 const void *buf)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    MPI_Offset   mpi_off;
    MPI_Status   mpi_stat;            /* Status from I/O operation */
    MPI_Datatype buf_type = MPI_BYTE; /* MPI description of the selection in memory */
#if H5_CHECK_MPI_VERSION(3, 0)
    MPI_Count bytes_written;
    MPI_Count type_size; /* MPI datatype used for I/O's size */
    MPI_Count io_size;   /* Actual number of bytes requested */
#else
    int bytes_written;
    int type_size; /* MPI datatype used for I/O's size */
    int io_size;   /* Actual number of bytes requested */
#endif
    int              size_i;
    bool             use_view_this_time = false;
    bool             derived_type       = false;
    H5FD_mpio_xfer_t xfer_mode; /* I/O transfer mode */
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_w_flag = (H5FD_mpio_debug_flags_s[(int)'w'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert(buf);

    /* Verify that no data is written when between MPI_Barrier()s during file flush */
    assert(!H5CX_get_mpi_file_flushing());

    /* Portably initialize MPI status variable */
    memset(&mpi_stat, 0, sizeof(MPI_Status));

    /* some numeric conversions */
    if (H5FD_mpi_haddr_to_MPIOff(addr, &mpi_off) < 0)
        HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");
    size_i = (int)size;

    /* Get the transfer mode from the API context */
    if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

    /*
     * Set up for a fancy xfer using complex types, or single byte block. We
     * wouldn't need to rely on the use_view field if MPI semantics allowed
     * us to test that btype=ftype=MPI_BYTE (or even MPI_TYPE_NULL, which
     * could mean "use MPI_BYTE" by convention).
     */
    if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
        MPI_Datatype file_type;

        /* Remember that views are used */
        use_view_this_time = true;

        /* Prepare for a full-blown xfer using btype, ftype, and disp */
        if (H5CX_get_mpi_coll_datatypes(&buf_type, &file_type) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O datatypes");

        /*
         * Set the file view when we are using MPI derived types
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, file_type,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)

        /* When using types, use the address as the displacement for
         * MPI_File_set_view and reset the address for the read to zero
         */
        mpi_off = 0;
    } /* end if */

    /* Write the data. */
    if (use_view_this_time) {
        H5FD_mpio_collective_opt_t coll_opt_mode;

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stderr, "%s: (%d) using MPIO collective mode\n", __func__, file->mpi_rank);
#endif

        /* Get the collective_opt property to check whether the application wants to do IO individually. */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stderr, "%s: (%d) doing MPI collective IO\n", __func__, file->mpi_rank);
#endif
            /* Perform collective write operation */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_write_at_all(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at_all failed", mpi_code)

            /* Do MPI_File_sync when needed by underlying ROMIO driver */
            if (file->mpi_file_sync_required) {
                if (MPI_SUCCESS != (mpi_code = MPI_File_sync(file->f)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_File_sync failed", mpi_code)
            }
        } /* end if */
        else {
            if (type != H5FD_MEM_DRAW)
                HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, FAIL,
                            "Metadata Coll opt property should be collective at this point");

#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif
            /* Perform independent write operation */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_write_at(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at failed", mpi_code)
        } /* end else */

        /* Reset the file view when we used MPI derived types */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)
    } /* end if */
    else {
        if (size != (hsize_t)size_i) {
            /* If HERE, then we need to work around the integer size limit
             * of 2GB. The input size_t size variable cannot fit into an integer,
             * but we can get around that limitation by creating a different datatype
             * and then setting the integer size (or element count) to 1 when using
             * the derived_type.
             */

            if (H5_mpio_create_large_type(size, 0, MPI_BYTE, &buf_type) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");

            derived_type = true;
            size_i       = 1;
        }

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif

        /* Perform independent write operation */
        if (MPI_SUCCESS != (mpi_code = MPI_File_write_at(file->f, mpi_off, buf, size_i, buf_type, &mpi_stat)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at failed", mpi_code)
    } /* end else */

    /* How many bytes were actually written? */
#if H5_CHECK_MPI_VERSION(3, 0)
    if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, buf_type, &bytes_written)))
#else
    if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_written)))
#endif
        HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code)

        /* Get the type's size */
#if H5_CHECK_MPI_VERSION(3, 0)
    if (MPI_SUCCESS != (mpi_code = MPI_Type_size_x(buf_type, &type_size)))
#else
    if (MPI_SUCCESS != (mpi_code = MPI_Type_size(buf_type, &type_size)))
#endif
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_size failed", mpi_code)

    /* Compute the actual number of bytes requested */
    io_size = type_size * size_i;

    /* Check for write failure */
    if (bytes_written != io_size || bytes_written < 0)
        HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_w_flag)
        fprintf(stderr, "%s: (%d) mpi_off = %ld  bytes_written = %lld  type = %s\n", __func__, file->mpi_rank,
                (long)mpi_off, (long long)bytes_written, H5FD__mem_t_to_str(type));
#endif

    /* Each process will keep track of its perceived EOF value locally, and
     * ultimately we will reduce this value to the maximum amongst all
     * processes, but until then keep the actual eof at HADDR_UNDEF just in
     * case something bad happens before that point. (rather have a value
     * we know is wrong sitting around rather than one that could only
     * potentially be wrong.) */
    file->eof = HADDR_UNDEF;

    if (bytes_written && (((haddr_t)bytes_written + addr) > file->local_eof))
        file->local_eof = addr + (haddr_t)bytes_written;

done:
    if (derived_type)
        MPI_Type_free(&buf_type);

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving: ret_value = %d\n", __func__, file->mpi_rank, ret_value);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_vector_build_types
 *
 * Purpose:     Build MPI datatypes and calculate offset, base buffer, and
 *              size for MPIO vector I/O.  Spun off from common code in
 *              H5FD__mpio_vector_read() and H5FD__mpio_vector_write().
 *
 * Return:      Success:    SUCCEED.
 *              Failure:    FAIL.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_vector_build_types(uint32_t count, H5FD_mem_t types[], haddr_t addrs[], size_t sizes[],
                              H5_flexible_const_ptr_t bufs[], haddr_t *s_addrs[], size_t *s_sizes[],
                              H5_flexible_const_ptr_t *s_bufs[], bool *vector_was_sorted, MPI_Offset *mpi_off,
                              H5_flexible_const_ptr_t *mpi_bufs_base, int *size_i, MPI_Datatype *buf_type,
                              bool *buf_type_created, MPI_Datatype *file_type, bool *file_type_created,
                              char *unused)
{
    hsize_t       bigio_count; /* Transition point to create derived type */
    bool          fixed_size = false;
    size_t        size;
    H5FD_mem_t   *s_types           = NULL;
    int          *mpi_block_lengths = NULL;
    MPI_Aint      mpi_bufs_base_Aint;
    MPI_Aint     *mpi_bufs          = NULL;
    MPI_Aint     *mpi_displacements = NULL;
    MPI_Datatype *sub_types         = NULL;
    uint8_t      *sub_types_created = NULL;
    int           i;
    int           j;
    int           mpi_code; /* MPI return code */
    herr_t        ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    assert(s_sizes);
    assert(s_bufs);
    assert(vector_was_sorted);
    assert(*vector_was_sorted);
    assert(mpi_off);
    assert(mpi_bufs_base);
    assert(size_i);
    assert(buf_type);
    assert(buf_type_created);
    assert(!*buf_type_created);
    assert(file_type);
    assert(file_type_created);
    assert(!*file_type_created);
    assert(unused);

    /* Get bio I/O transition point (may be lower than 2G for testing) */
    bigio_count = H5_mpi_get_bigio_count();

    if (count == 1) {
        /* Single block.  Just use a series of MPI_BYTEs for the file view.
         */
        *size_i        = (int)sizes[0];
        *buf_type      = MPI_BYTE;
        *file_type     = MPI_BYTE;
        *mpi_bufs_base = bufs[0];

        /* Setup s_addrs, s_sizes and s_bufs (needed for incomplete read filling code and eof
         * calculation code) */
        *s_addrs = addrs;
        *s_sizes = sizes;
        *s_bufs  = bufs;

        /* some numeric conversions */
        if (H5FD_mpi_haddr_to_MPIOff(addrs[0], mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI offset");

        /* Check for size overflow */
        if (sizes[0] > bigio_count) {
            /* We need to work around the integer size limit of 2GB. The input size_t size
             * variable cannot fit into an integer, but we can get around that limitation by
             * creating a different datatype and then setting the integer size (or element
             * count) to 1 when using the derived_type. */

            if (H5_mpio_create_large_type(sizes[0], 0, MPI_BYTE, buf_type) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");
            *buf_type_created = true;

            if (H5_mpio_create_large_type(sizes[0], 0, MPI_BYTE, file_type) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");
            *file_type_created = true;

            *size_i = 1;
        }
    }
    else if (count > 0) { /* create MPI derived types describing the vector write */

        /* sort the vector I/O request into increasing address order if required
         *
         * If the vector is already sorted, the base addresses of types, addrs, sizes,
         * and bufs will be returned in s_types, s_addrs, s_sizes, and s_bufs respectively.
         *
         * If the vector was not already sorted, new, sorted versions of types, addrs, sizes, and bufs
         * are allocated, populated, and returned in s_types, s_addrs, s_sizes, and s_bufs respectively.
         * In this case, this function must free the memory allocated for the sorted vectors.
         */
        if (H5FD_sort_vector_io_req(vector_was_sorted, count, types, addrs, sizes, bufs, &s_types, s_addrs,
                                    s_sizes, s_bufs) < 0)
            HGOTO_ERROR(H5E_ARGS, H5E_BADVALUE, FAIL, "can't sort vector I/O request");

        if ((NULL == (mpi_block_lengths = (int *)malloc((size_t)count * sizeof(int)))) ||
            (NULL == (mpi_displacements = (MPI_Aint *)malloc((size_t)count * sizeof(MPI_Aint)))) ||
            (NULL == (mpi_bufs = (MPI_Aint *)malloc((size_t)count * sizeof(MPI_Aint))))) {

            HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't alloc mpi block lengths / displacement");
        }

        /* when we setup mpi_bufs[] below, all addresses are offsets from
         * mpi_bufs_base.
         *
         * Since these offsets must all be positive, we must scan through
         * s_bufs[] to find the smallest value, and choose that for
         * mpi_bufs_base.
         */

        j = 0; /* guess at the index of the smallest value of s_bufs[] */

        for (i = 1; i < (int)count; i++) {

            if ((*s_bufs)[i].cvp < (*s_bufs)[j].cvp) {

                j = i;
            }
        }

        *mpi_bufs_base = (*s_bufs)[j];

        if (MPI_SUCCESS != (mpi_code = MPI_Get_address(mpi_bufs_base->cvp, &mpi_bufs_base_Aint)))

            HMPI_GOTO_ERROR(FAIL, "MPI_Get_address for s_bufs[] to mpi_bufs_base failed", mpi_code)

        *size_i = 1;

        fixed_size = false;

        /* load the mpi_block_lengths and mpi_displacements arrays */
        for (i = 0; i < (int)count; i++) {
            /* Determine size of this vector element */
            if (!fixed_size) {
                if ((*s_sizes)[i] == 0) {
                    assert(vector_was_sorted);
                    fixed_size = true;
                    size       = sizes[i - 1];
                }
                else {
                    size = (*s_sizes)[i];
                }
            }

            /* Add to block lengths and displacements arrays */
            mpi_block_lengths[i] = (int)size;
            mpi_displacements[i] = (MPI_Aint)(*s_addrs)[i];

            /* convert s_bufs[i] to MPI_Aint... */
            if (MPI_SUCCESS != (mpi_code = MPI_Get_address((*s_bufs)[i].cvp, &(mpi_bufs[i]))))
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_address for s_bufs[] - mpi_bufs_base failed", mpi_code)

                /*... and then subtract mpi_bufs_base_Aint from it. */
#if ((MPI_VERSION > 3) || ((MPI_VERSION == 3) && (MPI_SUBVERSION >= 1)))
            mpi_bufs[i] = MPI_Aint_diff(mpi_bufs[i], mpi_bufs_base_Aint);
#else
            mpi_bufs[i] = mpi_bufs[i] - mpi_bufs_base_Aint;
#endif

            /* Check for size overflow */
            if (size > bigio_count) {
                /* We need to work around the integer size limit of 2GB. The input size_t size
                 * variable cannot fit into an integer, but we can get around that limitation by
                 * creating a different datatype and then setting the integer size (or element
                 * count) to 1 when using the derived_type. */

                /* Allocate arrays to keep track of types and whether they were created, if
                 * necessary */
                if (!sub_types) {
                    assert(!sub_types_created);

                    if (NULL == (sub_types = malloc((size_t)count * sizeof(MPI_Datatype))))
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't alloc sub types array");
                    if (NULL == (sub_types_created = (uint8_t *)calloc((size_t)count, 1))) {
                        H5MM_free(sub_types);
                        sub_types = NULL;
                        HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL, "can't alloc sub types created array");
                    }

                    /* Initialize sub_types to all MPI_BYTE */
                    for (j = 0; j < (int)count; j++)
                        sub_types[j] = MPI_BYTE;
                }
                assert(sub_types_created);

                /* Create type for large block */
                if (H5_mpio_create_large_type(size, 0, MPI_BYTE, &sub_types[i]) < 0)
                    HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");
                sub_types_created[i] = true;

                /* Only one of these large types for this vector element */
                mpi_block_lengths[i] = 1;
            }
            else
                assert(size == (size_t)mpi_block_lengths[i]);
        }

        /* create the memory MPI derived types */
        if (sub_types) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct((int)count, mpi_block_lengths, mpi_bufs,
                                                                  sub_types, buf_type)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct for buf_type failed", mpi_code)
        }
        else if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hindexed((int)count, mpi_block_lengths, mpi_bufs,
                                                                     MPI_BYTE, buf_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed for buf_type failed", mpi_code)

        *buf_type_created = true;

        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(buf_type)))

            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit for buf_type failed", mpi_code)

        /* create the file MPI derived type */
        if (sub_types) {
            if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct((int)count, mpi_block_lengths,
                                                                  mpi_displacements, sub_types, file_type)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct for file_type failed", mpi_code)
        }
        else if (MPI_SUCCESS != (mpi_code = MPI_Type_create_hindexed((int)count, mpi_block_lengths,
                                                                     mpi_displacements, MPI_BYTE, file_type)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_hindexed for file_type failed", mpi_code)

        *file_type_created = true;

        if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(file_type)))

            HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit for file_type failed", mpi_code)

        /* Free up memory used to build types */
        assert(mpi_block_lengths);
        free(mpi_block_lengths);
        mpi_block_lengths = NULL;

        assert(mpi_displacements);
        free(mpi_displacements);
        mpi_displacements = NULL;

        assert(mpi_bufs);
        free(mpi_bufs);
        mpi_bufs = NULL;

        if (sub_types) {
            assert(sub_types);

            for (i = 0; i < (int)count; i++)
                if (sub_types_created[i])
                    MPI_Type_free(&sub_types[i]);

            free(sub_types);
            sub_types = NULL;
            free(sub_types_created);
            sub_types_created = NULL;
        }

        /* some numeric conversions */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");
    }
    else {
        /* setup for null participation in the collective operation. */
        *buf_type  = MPI_BYTE;
        *file_type = MPI_BYTE;

        /* Set non-NULL pointer for I/O operation */
        mpi_bufs_base->vp = unused;

        /* MPI count to read */
        *size_i = 0;

        /* some numeric conversions */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");
    }

done:
    /* free sorted vectors if they exist */
    if (!vector_was_sorted)
        if (s_types) {
            free(s_types);
            s_types = NULL;
        }

    /* Clean up on error */
    if (ret_value < 0) {
        if (mpi_block_lengths) {
            free(mpi_block_lengths);
            mpi_block_lengths = NULL;
        }

        if (mpi_displacements) {
            free(mpi_displacements);
            mpi_displacements = NULL;
        }

        if (mpi_bufs) {
            free(mpi_bufs);
            mpi_bufs = NULL;
        }

        if (sub_types) {
            assert(sub_types_created);

            for (i = 0; i < (int)count; i++)
                if (sub_types_created[i])
                    MPI_Type_free(&sub_types[i]);

            free(sub_types);
            sub_types = NULL;
            free(sub_types_created);
            sub_types_created = NULL;
        }
    }

    /* Make sure we cleaned up */
    assert(!mpi_block_lengths);
    assert(!mpi_displacements);
    assert(!mpi_bufs);
    assert(!sub_types);
    assert(!sub_types_created);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_vector_build_types() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_read_vector()
 *
 * Purpose:     The behavior of this function depends on the value of
 *              the io_xfer_mode obtained from the context.
 *
 *              If it is H5FD_MPIO_COLLECTIVE, this is a collective
 *              operation, which allows us to use MPI_File_set_view, and
 *              then perform the entire vector read in a single MPI call.
 *
 *              Do this (if count is positive), by constructing memory
 *              and file derived types from the supplied vector, using
 *              file type to set the file view, and then reading the
 *              the memory type from file.  Note that this read is
 *              either independent or collective depending on the
 *              value of mpio_coll_opt -- again obtained from the context.
 *
 *              If count is zero, participate in the collective read
 *              (if so configured) with an empty read.
 *
 *              Finally, set the file view back to its default state.
 *
 *              In contrast, if io_xfer_mode is H5FD_MPIO_INDEPENDENT,
 *              this call is independent, and thus we cannot use
 *              MPI_File_set_view().
 *
 *              In this case, simply walk the vector, and issue an
 *              independent read for each entry.
 *
 * Return:      Success:    SUCCEED.
 *              Failure:    FAIL.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_read_vector(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, uint32_t count, H5FD_mem_t types[],
                       haddr_t addrs[], size_t sizes[], void *bufs[])
{
    H5FD_mpio_t               *file              = (H5FD_mpio_t *)_file;
    bool                       vector_was_sorted = true;
    haddr_t                   *s_addrs           = NULL;
    size_t                    *s_sizes           = NULL;
    void                     **s_bufs            = NULL;
    char                       unused            = 0; /* Unused, except for non-NULL pointer value */
    void                      *mpi_bufs_base     = NULL;
    MPI_Datatype               buf_type          = MPI_BYTE; /* MPI description of the selection in memory */
    bool                       buf_type_created  = false;
    MPI_Datatype               file_type         = MPI_BYTE; /* MPI description of the selection in file */
    bool                       file_type_created = false;
    int                        i;
    int                        mpi_code; /* MPI return code */
    MPI_Offset                 mpi_off = 0;
    MPI_Status                 mpi_stat;      /* Status from I/O operation */
    H5FD_mpio_xfer_t           xfer_mode;     /* I/O transfer mode */
    H5FD_mpio_collective_opt_t coll_opt_mode; /* whether we are doing collective or independent I/O */
    int                        size_i;
#if MPI_VERSION >= 3
    MPI_Count bytes_read = 0; /* Number of bytes read in */
    MPI_Count type_size;      /* MPI datatype used for I/O's size */
    MPI_Count io_size;        /* Actual number of bytes requested */
    MPI_Count n;
#else
    int bytes_read = 0; /* Number of bytes read in */
    int type_size;      /* MPI datatype used for I/O's size */
    int io_size;        /* Actual number of bytes requested */
    int n;
#endif
    bool rank0_bcast = false; /* If read-with-rank0-and-bcast flag was used */
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_r_flag = (H5FD_mpio_debug_flags_s[(int)'r'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert((types) || (count == 0));
    assert((addrs) || (count == 0));
    assert((sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /* verify that the first elements of the sizes and types arrays are
     * valid.
     */
    assert((count == 0) || (sizes[0] != 0));
    assert((count == 0) || (types[0] != H5FD_MEM_NOLIST));

    /* Get the transfer mode from the API context
     *
     * This flag is set to H5FD_MPIO_COLLECTIVE if the API call is
     * collective, and to H5FD_MPIO_INDEPENDENT if it is not.
     *
     * While this doesn't mean that we are actually about to do a collective
     * read, it does mean that all ranks are here, so we can use MPI_File_set_view().
     */
    if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

    if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
        /* Build MPI types, etc. */
        if (H5FD__mpio_vector_build_types(count, types, addrs, sizes, (H5_flexible_const_ptr_t *)bufs,
                                          &s_addrs, &s_sizes, (H5_flexible_const_ptr_t **)&s_bufs,
                                          &vector_was_sorted, &mpi_off,
                                          (H5_flexible_const_ptr_t *)&mpi_bufs_base, &size_i, &buf_type,
                                          &buf_type_created, &file_type, &file_type_created, &unused) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't build MPI datatypes for I/O");

        /* free sorted addrs vector if it exists */
        if (!vector_was_sorted)
            if (s_addrs) {
                free(s_addrs);
                s_addrs = NULL;
            }

        /* Portably initialize MPI status variable */
        memset(&mpi_stat, 0, sizeof(mpi_stat));

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stdout, "%s: mpi_off = %ld  size_i = %d\n", __func__, (long)mpi_off, size_i);
#endif

        /* Setup the file view. */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, file_type,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)

        /* Reset mpi_off to 0 since the view now starts at the data offset */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, &mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");

        /* Get the collective_opt property to check whether the application wants to do IO individually.
         */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)

            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

            /* Read the data. */
#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stdout, "%s: using MPIO collective mode\n", __func__);
#endif
        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stdout, "%s: doing MPI collective IO\n", __func__);
#endif
            /* Check whether we should read from rank 0 and broadcast to other ranks */
            if (H5CX_get_mpio_rank0_bcast()) {
#ifdef H5FDmpio_DEBUG
                if (H5FD_mpio_debug_r_flag)
                    fprintf(stdout, "%s: doing read-rank0-and-MPI_Bcast\n", __func__);
#endif
                /* Indicate path we've taken */
                rank0_bcast = true;

                /* Read on rank 0 Bcast to other ranks */
                if (file->mpi_rank == 0)
                    if (MPI_SUCCESS != (mpi_code = MPI_File_read_at(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                    buf_type, &mpi_stat)))
                        HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at_all failed", mpi_code)
                if (MPI_SUCCESS != (mpi_code = MPI_Bcast(mpi_bufs_base, size_i, buf_type, 0, file->comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_code)
            } /* end if */
            else if (MPI_SUCCESS != (mpi_code = MPI_File_read_at_all(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                     buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at_all failed", mpi_code)
        } /* end if */
        else if (size_i > 0) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stdout, "%s: doing MPI independent IO\n", __func__);
#endif

            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_read_at(file->f, mpi_off, mpi_bufs_base, size_i, buf_type, &mpi_stat)))

                HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at failed", mpi_code)

        } /* end else */

        /* Reset the file view  */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)

        /* Only retrieve bytes read if this rank _actually_ participated in I/O */
        if (!rank0_bcast || (rank0_bcast && file->mpi_rank == 0)) {
            /* How many bytes were actually read? */
#if MPI_VERSION >= 3
            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, buf_type, &bytes_read)))
#else
            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_read)))
#endif
                HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code)
        } /* end if */

        /* If the rank0-bcast feature was used, broadcast the # of bytes read to
         * other ranks, which didn't perform any I/O.
         */
        /* NOTE: This could be optimized further to be combined with the broadcast
         *          of the data.  (QAK - 2019/1/2)
         *       Or have rank 0 clear the unread parts of the buffer prior to
         *          the bcast.  (NAF - 2021/9/15)
         */
        if (rank0_bcast)
#if MPI_VERSION >= 3
            if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_COUNT, 0, file->comm))
#else
            if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_INT, 0, file->comm))
#endif
                HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", 0)

                /* Get the type's size */
#if MPI_VERSION >= 3
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size_x(buf_type, &type_size)))
#else
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size(buf_type, &type_size)))
#endif
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_size failed", mpi_code)

        /* Compute the actual number of bytes requested */
        io_size = type_size * size_i;

        /* Check for read failure */
        if (bytes_read < 0 || bytes_read > io_size)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed");

        /* Check for incomplete read */
        n = io_size - bytes_read;
        if (n > 0) {
            i = (int)count - 1;

            /* Iterate over sorted array in reverse, filling in zeroes to
             * sections of the buffers that were not read to */
            do {
                assert(i >= 0);

#if MPI_VERSION >= 3
                io_size    = MIN(n, (MPI_Count)s_sizes[i]);
                bytes_read = (MPI_Count)s_sizes[i] - io_size;
#else
                io_size    = MIN(n, (int)s_sizes[i]);
                bytes_read = (int)s_sizes[i] - io_size;
#endif
                assert(bytes_read >= 0);

                memset((char *)s_bufs[i] + bytes_read, 0, (size_t)io_size);

                n -= io_size;
                i--;
            } while (n > 0);
        }
    }
    else if (count > 0) {
        haddr_t max_addr   = HADDR_MAX;
        bool    fixed_size = false;
        size_t  size;

        /* The read is part of an independent operation. As a result,
         * we can't use MPI_File_set_view() (since it it a collective operation),
         * and thus we can't use the above code to construct the MPI datatypes.
         * In the future, we could write code to detect when a contiguous slab
         * in the file selection spans multiple vector elements and construct a
         * memory datatype to match this larger block in the file, but for now
         * just read in each element of the vector in a separate
         * MPI_File_read_at() call.
         *
         * We could also just detect the case when the entire file selection is
         * contiguous, which would allow us to use
         * H5FD__mpio_vector_build_types() to construct the memory datatype.
         */

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stdout, "%s: doing MPI independent IO\n", __func__);
#endif

        /* Loop over vector elements */
        for (i = 0; i < (int)count; i++) {
            /* Convert address to mpi offset */
            if (H5FD_mpi_haddr_to_MPIOff(addrs[i], &mpi_off) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");

            /* Calculate I/O size */
            if (!fixed_size) {
                if (sizes[i] == 0) {
                    fixed_size = true;
                    size       = sizes[i - 1];
                }
                else {
                    size = sizes[i];
                }
            }
            size_i = (int)size;

            if (size != (size_t)size_i) {
                /* If HERE, then we need to work around the integer size limit
                 * of 2GB. The input size_t size variable cannot fit into an integer,
                 * but we can get around that limitation by creating a different datatype
                 * and then setting the integer size (or element count) to 1 when using
                 * the derived_type.
                 */

                if (H5_mpio_create_large_type(size, 0, MPI_BYTE, &buf_type) < 0)
                    HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");

                buf_type_created = true;
                size_i           = 1;
            }

            /* Check if we actually need to do I/O */
            if (addrs[i] < max_addr) {
                /* Portably initialize MPI status variable */
                memset(&mpi_stat, 0, sizeof(mpi_stat));

                /* Issue read */
                if (MPI_SUCCESS !=
                    (mpi_code = MPI_File_read_at(file->f, mpi_off, bufs[i], size_i, buf_type, &mpi_stat)))

                    HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at failed", mpi_code)

                    /* How many bytes were actually read? */
#if MPI_VERSION >= 3
                if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, MPI_BYTE, &bytes_read)))
#else
                if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_read)))
#endif
                    HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code)

                    /* Compute the actual number of bytes requested */
#if MPI_VERSION >= 3
                io_size = (MPI_Count)size;
#else
                io_size = (int)size;
#endif

                /* Check for read failure */
                if (bytes_read < 0 || bytes_read > io_size)
                    HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed");

                /*
                 * If we didn't read the entire I/O, fill in zeroes beyond end of
                 * the physical MPI file and don't issue any more reads at higher
                 * addresses.
                 */
                if ((n = (io_size - bytes_read)) > 0) {
                    memset((char *)bufs[i] + bytes_read, 0, (size_t)n);
                    max_addr = addrs[i] + (haddr_t)bytes_read;
                }
            }
            else {
                /* Read is past the max address, fill in zeroes */
                memset((char *)bufs[i], 0, size);
            }
        }
    }

done:
    if (buf_type_created) {
        MPI_Type_free(&buf_type);
    }

    if (file_type_created) {
        MPI_Type_free(&file_type);
    }

    /* free sorted vectors if they exist */
    if (!vector_was_sorted) {
        if (s_addrs) {
            free(s_addrs);
            s_addrs = NULL;
        }
        if (s_sizes) {
            free(s_sizes);
            s_sizes = NULL;
        }
        if (s_bufs) {
            free(s_bufs);
            s_bufs = NULL;
        }
    }

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stdout, "%s: Leaving, proc %d: ret_value = %d\n", __func__, file->mpi_rank, ret_value);
#endif

    FUNC_LEAVE_NOAPI(ret_value)

} /* end H5FD__mpio_read_vector() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_write_vector
 *
 * Purpose:     The behavior of this function depends on the value of
 *              the io_xfer_mode obtained from the context.
 *
 *              If it is H5FD_MPIO_COLLECTIVE, this is a collective
 *              operation, which allows us to use MPI_File_set_view, and
 *              then perform the entire vector write in a single MPI call.
 *
 *              Do this (if count is positive), by constructing memory
 *              and file derived types from the supplied vector, using
 *              file type to set the file view, and then writing the
 *              the memory type to file.  Note that this write is
 *              either independent or collective depending on the
 *              value of mpio_coll_opt -- again obtained from the context.
 *
 *              If count is zero, participate in the collective write
 *              (if so configured) with an empty write.
 *
 *              Finally, set the file view back to its default state.
 *
 *              In contrast, if io_xfer_mode is H5FD_MPIO_INDEPENDENT,
 *              this call is independent, and thus we cannot use
 *              MPI_File_set_view().
 *
 *              In this case, simply walk the vector, and issue an
 *              independent write for each entry.
 *
 * Return:      Success:    SUCCEED.
 *              Failure:    FAIL.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_write_vector(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, uint32_t count, H5FD_mem_t types[],
                        haddr_t addrs[], size_t sizes[], const void *bufs[])
{
    H5FD_mpio_t               *file              = (H5FD_mpio_t *)_file;
    bool                       vector_was_sorted = true;
    haddr_t                   *s_addrs           = NULL;
    size_t                    *s_sizes           = NULL;
    const void               **s_bufs            = NULL;
    char                       unused            = 0; /* Unused, except for non-NULL pointer value */
    const void                *mpi_bufs_base     = NULL;
    MPI_Datatype               buf_type          = MPI_BYTE; /* MPI description of the selection in memory */
    bool                       buf_type_created  = false;
    MPI_Datatype               file_type         = MPI_BYTE; /* MPI description of the selection in file */
    bool                       file_type_created = false;
    int                        i;
    int                        mpi_code; /* MPI return code */
    MPI_Offset                 mpi_off = 0;
    MPI_Status                 mpi_stat;      /* Status from I/O operation */
    H5FD_mpio_xfer_t           xfer_mode;     /* I/O transfer mode */
    H5FD_mpio_collective_opt_t coll_opt_mode; /* whether we are doing collective or independent I/O */
    int                        size_i;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_w_flag = (H5FD_mpio_debug_flags_s[(int)'w'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    haddr_t max_addr  = 0;
    herr_t  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert((types) || (count == 0));
    assert((addrs) || (count == 0));
    assert((sizes) || (count == 0));
    assert((bufs) || (count == 0));

    /* verify that the first elements of the sizes and types arrays are
     * valid.
     */
    assert((count == 0) || (sizes[0] != 0));
    assert((count == 0) || (types[0] != H5FD_MEM_NOLIST));

    /* Verify that no data is written when between MPI_Barrier()s during file flush */

    assert(!H5CX_get_mpi_file_flushing());

    /* Get the transfer mode from the API context
     *
     * This flag is set to H5FD_MPIO_COLLECTIVE if the API call is
     * collective, and to H5FD_MPIO_INDEPENDENT if it is not.
     *
     * While this doesn't mean that we are actually about to do a collective
     * write, it does mean that all ranks are here, so we can use MPI_File_set_view().
     */
    if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

    if (xfer_mode == H5FD_MPIO_COLLECTIVE) {
        /* Build MPI types, etc. */
        if (H5FD__mpio_vector_build_types(count, types, addrs, sizes, (H5_flexible_const_ptr_t *)bufs,
                                          &s_addrs, &s_sizes, (H5_flexible_const_ptr_t **)&s_bufs,
                                          &vector_was_sorted, &mpi_off,
                                          (H5_flexible_const_ptr_t *)&mpi_bufs_base, &size_i, &buf_type,
                                          &buf_type_created, &file_type, &file_type_created, &unused) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't build MPI datatypes for I/O");

        /* Compute max address written to */
        if (count > 0)
            max_addr = s_addrs[count - 1] + (haddr_t)(s_sizes[count - 1]);

        /* free sorted vectors if they exist */
        if (!vector_was_sorted) {
            if (s_addrs) {
                free(s_addrs);
                s_addrs = NULL;
            }
            if (s_sizes) {
                free(s_sizes);
                s_sizes = NULL;
            }
            if (s_bufs) {
                free(s_bufs);
                s_bufs = NULL;
            }
        }

        /* Portably initialize MPI status variable */
        memset(&mpi_stat, 0, sizeof(MPI_Status));

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stdout, "%s: mpi_off = %ld  size_i = %d\n", __func__, (long)mpi_off, size_i);
#endif

        /* Setup the file view. */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, file_type,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)

        /* Reset mpi_off to 0 since the view now starts at the data offset */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, &mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");

        /* Get the collective_opt property to check whether the application wants to do IO individually.
         */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

            /* Write the data. */
#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stdout, "%s: using MPIO collective mode\n", __func__);
#endif

        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stdout, "%s: doing MPI collective IO\n", __func__);
#endif

            if (MPI_SUCCESS != (mpi_code = MPI_File_write_at_all(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                 buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at_all failed", mpi_code)

            /* Do MPI_File_sync when needed by underlying ROMIO driver */
            if (file->mpi_file_sync_required) {
                if (MPI_SUCCESS != (mpi_code = MPI_File_sync(file->f)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_File_sync failed", mpi_code)
            }
        } /* end if */
        else if (size_i > 0) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stdout, "%s: doing MPI independent IO\n", __func__);
#endif

            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_write_at(file->f, mpi_off, mpi_bufs_base, size_i, buf_type, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at failed", mpi_code)
        } /* end else */

        /* Reset the file view  */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code)
    }
    else if (count > 0) {
        bool   fixed_size = false;
        size_t size;

        /* The read is part of an independent operation. As a result,
         * we can't use MPI_File_set_view() (since it it a collective operation),
         * and thus we can't use the above code to construct the MPI datatypes.
         * In the future, we could write code to detect when a contiguous slab
         * in the file selection spans multiple vector elements and construct a
         * memory datatype to match this larger block in the file, but for now
         * just read in each element of the vector in a separate
         * MPI_File_read_at() call.
         *
         * We could also just detect the case when the entire file selection is
         * contiguous, which would allow us to use
         * H5FD__mpio_vector_build_types() to construct the memory datatype.
         */

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stdout, "%s: doing MPI independent IO\n", __func__);
#endif

        /* Loop over vector elements */
        for (i = 0; i < (int)count; i++) {
            /* Convert address to mpi offset */
            if (H5FD_mpi_haddr_to_MPIOff(addrs[i], &mpi_off) < 0)
                HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");

            /* Calculate I/O size */
            if (!fixed_size) {
                if (sizes[i] == 0) {
                    fixed_size = true;
                    size       = sizes[i - 1];
                }
                else {
                    size = sizes[i];
                }
            }
            size_i = (int)size;

            if (size != (size_t)size_i) {
                /* If HERE, then we need to work around the integer size limit
                 * of 2GB. The input size_t size variable cannot fit into an integer,
                 * but we can get around that limitation by creating a different datatype
                 * and then setting the integer size (or element count) to 1 when using
                 * the derived_type.
                 */

                if (H5_mpio_create_large_type(size, 0, MPI_BYTE, &buf_type) < 0)
                    HGOTO_ERROR(H5E_INTERNAL, H5E_CANTGET, FAIL, "can't create MPI-I/O datatype");

                buf_type_created = true;
                size_i           = 1;
            }

            /* Perform write */
            if (MPI_SUCCESS !=
                (mpi_code = MPI_File_write_at(file->f, mpi_off, bufs[i], size_i, buf_type, &mpi_stat)))

                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at failed", mpi_code)

            /* Check if this is the highest address written to so far */
            if (addrs[i] + size > max_addr)
                max_addr = addrs[i] + size;
        }
    }

    /* Each process will keep track of its perceived EOF value locally, and
     * ultimately we will reduce this value to the maximum amongst all
     * processes, but until then keep the actual eof at HADDR_UNDEF just in
     * case something bad happens before that point. (rather have a value
     * we know is wrong sitting around rather than one that could only
     * potentially be wrong.)
     */
    file->eof = HADDR_UNDEF;

    /* check to see if the local eof has been extended, and update if so */
    if (max_addr > file->local_eof)
        file->local_eof = max_addr;

done:
    if (buf_type_created)
        MPI_Type_free(&buf_type);

    if (file_type_created)
        MPI_Type_free(&file_type);

    /* Cleanup on error */
    if (ret_value < 0 && !vector_was_sorted) {
        if (s_addrs) {
            free(s_addrs);
            s_addrs = NULL;
        }
        if (s_sizes) {
            free(s_sizes);
            s_sizes = NULL;
        }
        if (s_bufs) {
            free(s_bufs);
            s_bufs = NULL;
        }
    }

    /* Make sure we cleaned up */
    assert(vector_was_sorted || !s_addrs);
    assert(vector_was_sorted || !s_sizes);
    assert(vector_was_sorted || !s_bufs);

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stdout, "%s: Leaving, proc %d: ret_value = %d\n", __func__, file->mpi_rank, ret_value);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_write_vector() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__selection_build_types
 *
 * Purpose:     Build MPI derived datatype for each piece and then
 *              build MPI final derived datatype for file and memory.
 *
 *              Note: This is derived from H5D__link_piece_collective_io() in
 *              src/H5Dmpio.c.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__selection_build_types(bool io_op_write, size_t num_pieces, H5_flexible_const_ptr_t mbb,
                            H5S_t **file_spaces, H5S_t **mem_spaces, haddr_t offsets[],
                            H5_flexible_const_ptr_t bufs[], size_t src_element_sizes[],
                            size_t dst_element_sizes[], MPI_Datatype *final_ftype,
                            bool *final_ftype_is_derived, MPI_Datatype *final_mtype,
                            bool *final_mtype_is_derived)
{

    MPI_Datatype *piece_mtype           = NULL;
    MPI_Datatype *piece_ftype           = NULL;
    MPI_Aint     *piece_file_disp_array = NULL;
    MPI_Aint     *piece_mem_disp_array  = NULL;
    bool *piece_mft_is_derived_array = NULL; /* Flags to indicate each piece's MPI file datatype is derived */
    ;
    bool *piece_mmt_is_derived_array =
        NULL;                          /* Flags to indicate each piece's MPI memory datatype is derived */
    int *piece_mpi_file_counts = NULL; /* Count of MPI file datatype for each piece */
    int *piece_mpi_mem_counts  = NULL; /* Count of MPI memory datatype for each piece */

    haddr_t base_file_addr;
    size_t  i;        /* Local index variable */
    int     mpi_code; /* MPI return code */

    bool                    extend_src_sizes = false;
    bool                    extend_dst_sizes = false;
    bool                    extend_bufs      = false;
    H5_flexible_const_ptr_t buf;
    size_t                  src_element_size, dst_element_size;

    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

    /* Allocate information for num_pieces */
    if (NULL == (piece_mtype = (MPI_Datatype *)H5MM_malloc(num_pieces * sizeof(MPI_Datatype))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece memory datatype buffer");
    if (NULL == (piece_ftype = (MPI_Datatype *)H5MM_malloc(num_pieces * sizeof(MPI_Datatype))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece file datatype buffer");
    if (NULL == (piece_file_disp_array = (MPI_Aint *)H5MM_malloc(num_pieces * sizeof(MPI_Aint))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece file displacement buffer");
    if (NULL == (piece_mem_disp_array = (MPI_Aint *)H5MM_calloc(num_pieces * sizeof(MPI_Aint))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece memory displacement buffer");
    if (NULL == (piece_mpi_mem_counts = (int *)H5MM_calloc(num_pieces * sizeof(int))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece memory counts buffer");
    if (NULL == (piece_mpi_file_counts = (int *)H5MM_calloc(num_pieces * sizeof(int))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL, "couldn't allocate piece file counts buffer");
    if (NULL == (piece_mmt_is_derived_array = (bool *)H5MM_calloc(num_pieces * sizeof(bool))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                    "couldn't allocate piece memory is derived datatype flags buffer");
    if (NULL == (piece_mft_is_derived_array = (bool *)H5MM_calloc(num_pieces * sizeof(bool))))
        HGOTO_ERROR(H5E_DATASET, H5E_CANTALLOC, FAIL,
                    "couldn't allocate piece file is derived datatype flags buffer");

    /* save lowest file address */
    base_file_addr = offsets[0];

    /* Obtain MPI derived datatype from all individual pieces */
    /* Iterate over selected pieces for this process */
    for (i = 0; i < num_pieces; i++) {
        hsize_t *permute_map = NULL; /* array that holds the mapping from the old,
                                            out-of-order displacements to the in-order
                                            displacements of the MPI datatypes of the
                                            point selection of the file space */
        bool is_permuted = false;

        if (!extend_src_sizes) {
            if (src_element_sizes[i] == 0) {
                extend_src_sizes = true;
                src_element_size = src_element_sizes[i - 1];
            }
            else
                src_element_size = src_element_sizes[i];
        }

        if (!extend_dst_sizes) {
            if (dst_element_sizes[i] == 0) {
                extend_dst_sizes = true;
                dst_element_size = dst_element_sizes[i - 1];
            }
            else
                dst_element_size = src_element_sizes[i];
        }

        if (!extend_bufs) {
            if (bufs[i].cvp == NULL) {
                extend_bufs = true;
                buf         = bufs[i - 1];
            }
            else
                buf = bufs[i];
        }

        /* Obtain disk and memory MPI derived datatype */
        /* NOTE: The permute_map array can be allocated within H5S_mpio_space_type
         *       and will be fed into the next call to H5S_mpio_space_type
         *       where it will be freed.
         */
        if (H5S_mpio_space_type(file_spaces[i], src_element_size, &piece_ftype[i], /* OUT: datatype created */
                                &piece_mpi_file_counts[i],                         /* OUT */
                                &(piece_mft_is_derived_array[i]),                  /* OUT */
                                true,                                              /* this is a file space,
                                                                                      so permute the
                                                                                      datatype if the point
                                                                                      selections are out of
                                                                                      order */
                                &permute_map, /* OUT: a map to indicate the
                                                 permutation of points
                                                 selected in case they
                                                 are out of order */
                                &is_permuted /* OUT */) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI file type");

        /* Sanity check */
        if (is_permuted)
            assert(permute_map);

        if (H5S_mpio_space_type(mem_spaces[i], dst_element_size, &piece_mtype[i], &piece_mpi_mem_counts[i],
                                &(piece_mmt_is_derived_array[i]), false, /* this is a memory
                                                                            space, so if the file
                                                                            space is not
                                                                            permuted, there is no
                                                                            need to permute the
                                                                            datatype if the point
                                                                            selections are out of
                                                                            order*/
                                &permute_map,                            /* IN: the permutation map
                                                                            generated by the
                                                                            file_space selection
                                                                            and applied to the
                                                                            memory selection */
                                &is_permuted /* IN */) < 0)
            HGOTO_ERROR(H5E_DATASPACE, H5E_BADTYPE, FAIL, "couldn't create MPI buf type");

        /* Sanity check */
        if (is_permuted)
            assert(!permute_map);

        /* Piece address relative to the first piece addr
         * Assign piece address to MPI displacement
         * (assume MPI_Aint big enough to hold it) */
        piece_file_disp_array[i] = (MPI_Aint)offsets[i] - (MPI_Aint)base_file_addr;

        if (io_op_write) {
            piece_mem_disp_array[i] = (MPI_Aint)buf.cvp - (MPI_Aint)mbb.cvp;
        }
        else {
            piece_mem_disp_array[i] = (MPI_Aint)buf.vp - (MPI_Aint)mbb.vp;
        }
    } /* end for */

    /* Create final MPI derived datatype for the file */
    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct((int)num_pieces, piece_mpi_file_counts,
                                                          piece_file_disp_array, piece_ftype, final_ftype)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code);

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(final_ftype)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code);
    *final_ftype_is_derived = true;

    /* Create final MPI derived datatype for memory */
    if (MPI_SUCCESS != (mpi_code = MPI_Type_create_struct((int)num_pieces, piece_mpi_mem_counts,
                                                          piece_mem_disp_array, piece_mtype, final_mtype)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_create_struct failed", mpi_code);

    if (MPI_SUCCESS != (mpi_code = MPI_Type_commit(final_mtype)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Type_commit failed", mpi_code);
    *final_mtype_is_derived = true;

    /* Free the file & memory MPI datatypes for each piece */
    for (i = 0; i < num_pieces; i++) {
        if (piece_mmt_is_derived_array[i])
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(piece_mtype + i)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);

        if (piece_mft_is_derived_array[i])
            if (MPI_SUCCESS != (mpi_code = MPI_Type_free(piece_ftype + i)))
                HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);
    } /* end for */

done:

    /* Release resources */
    if (piece_mtype)
        H5MM_xfree(piece_mtype);
    if (piece_ftype)
        H5MM_xfree(piece_ftype);
    if (piece_file_disp_array)
        H5MM_xfree(piece_file_disp_array);
    if (piece_mem_disp_array)
        H5MM_xfree(piece_mem_disp_array);
    if (piece_mpi_mem_counts)
        H5MM_xfree(piece_mpi_mem_counts);
    if (piece_mpi_file_counts)
        H5MM_xfree(piece_mpi_file_counts);
    if (piece_mmt_is_derived_array)
        H5MM_xfree(piece_mmt_is_derived_array);
    if (piece_mft_is_derived_array)
        H5MM_xfree(piece_mft_is_derived_array);

    FUNC_LEAVE_NOAPI(ret_value);

} /* H5FD__selection_build_types() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_read_selection
 *
 * Purpose:     The behavior of this function depends on the value of
 *              the transfer mode obtained from the context.
 *
 *              If the transfer mode is H5FD_MPIO_COLLECTIVE:
 *              --sort the selections
 *              --set mpi_bufs_base
 *              --build the MPI derived types
 *              --perform MPI_File_set_view()
 *              --perform MPI_File_read_at_all() or MPI_File_read_at()
 *                depending on whether this is a H5FD_MPIO_COLLECTIVE_IO
 *
 *              If this is not H5FD_MPIO_COLLECTIVE:
 *              --undo possible base address addition in internal routines
 *              --call H5FD_read_vector_from_selection() to perform vector
 *                or scalar writes for the selections
 *
 * Return:      Success:    SUCCEED.
 *              Failure:    FAIL.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_read_selection(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, size_t count,
                          hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                          size_t element_sizes[], void *bufs[] /* out */)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    MPI_Offset   mpi_off;
    MPI_Status   mpi_stat; /* Status from I/O operation */
    int          size_i;   /* Integer copy of 'size' to read */

    H5FD_mpio_xfer_t           xfer_mode; /* I/O transfer mode */
    H5FD_mpio_collective_opt_t coll_opt_mode;

    MPI_Datatype final_mtype; /* Final memory MPI datatype for all pieces with selection */
    bool         final_mtype_is_derived = false;

    MPI_Datatype final_ftype; /* Final file MPI datatype for all pieces with selection */
    bool         final_ftype_is_derived = false;

    hid_t                   *s_mem_space_ids      = NULL;
    hid_t                   *s_file_space_ids     = NULL;
    haddr_t                 *s_offsets            = NULL;
    size_t                  *s_element_sizes      = NULL;
    H5_flexible_const_ptr_t *s_bufs               = NULL;
    bool                     selection_was_sorted = true;

    uint32_t i, j;
    H5S_t  **s_mem_spaces  = NULL;
    H5S_t  **s_file_spaces = NULL;
    haddr_t  tmp_offset    = 0;
    void    *mpi_bufs_base = NULL;
    char     unused        = 0; /* Unused, except for non-NULL pointer value */

#if H5_CHECK_MPI_VERSION(3, 0)
    MPI_Count bytes_read = 0; /* Number of bytes read in */
    MPI_Count type_size;      /* MPI datatype used for I/O's size */
    MPI_Count io_size;        /* Actual number of bytes requested */
    MPI_Count n;
#else
    int bytes_read = 0; /* Number of bytes read in */
    int type_size;      /* MPI datatype used for I/O's size */
    int io_size;        /* Actual number of bytes requested */
    int n;
#endif
    bool rank0_bcast = false; /* If read-with-rank0-and-bcast flag was used */
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_r_flag = (H5FD_mpio_debug_flags_s[(int)'r'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int                     mpi_code; /* MPI return code */
    H5_flexible_const_ptr_t mbb;
    herr_t                  ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert((count == 0) || (mem_space_ids));
    assert((count == 0) || (file_space_ids));
    assert((count == 0) || (offsets));
    assert((count == 0) || (element_sizes));
    assert((count == 0) || (bufs));

    /* Verify that the first elements of the element_sizes and bufs arrays are
     * valid. */
    assert((count == 0) || (element_sizes[0] != 0));
    assert((count == 0) || (bufs[0] != NULL));

    /* Portably initialize MPI status variable */
    memset(&mpi_stat, 0, sizeof(MPI_Status));

    /* Get the transfer mode from the API context */
    if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

    /*
     * Set up for a fancy xfer using complex types, or single byte block. We
     * wouldn't need to rely on the use_view field if MPI semantics allowed
     * us to test that btype=ftype=MPI_BYTE (or even MPI_TYPE_NULL, which
     * could mean "use MPI_BYTE" by convention).
     */
    if (xfer_mode == H5FD_MPIO_COLLECTIVE) {

        if (count) {
            if (H5FD_sort_selection_io_req(&selection_was_sorted, count, mem_space_ids, file_space_ids,
                                           offsets, element_sizes, (H5_flexible_const_ptr_t *)bufs,
                                           &s_mem_space_ids, &s_file_space_ids, &s_offsets, &s_element_sizes,
                                           &s_bufs) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't sort selection I/O request");

            tmp_offset = s_offsets[0];

            if (NULL == (s_file_spaces = H5MM_malloc(count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for file space list");
            if (NULL == (s_mem_spaces = H5MM_malloc(count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for memory space list");

            for (i = 0; i < count; i++) {
                if (NULL == (s_mem_spaces[i] = (H5S_t *)H5I_object_verify(s_mem_space_ids[i], H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, H5I_INVALID_HID,
                                "can't retrieve memory dataspace from ID");
                if (NULL ==
                    (s_file_spaces[i] = (H5S_t *)H5I_object_verify(s_file_space_ids[i], H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, H5I_INVALID_HID,
                                "can't retrieve file dataspace from ID");
            }

            /* when we setup mpi_bufs[] below, all addresses are offsets from
             * mpi_bufs_base.
             *
             * Since these offsets must all be positive, we must scan through
             * s_bufs[] to find the smallest value, and choose that for
             * mpi_bufs_base.
             */

            j = 0; /* guess at the index of the smallest value of s_bufs[] */
            if ((count > 1) && (s_bufs[1].vp != NULL)) {
                for (i = 1; i < count; i++)
                    if (s_bufs[i].vp < s_bufs[j].vp)
                        j = i;
            }

            mpi_bufs_base = s_bufs[j].vp;
            mbb.vp        = mpi_bufs_base;

            if (H5FD__selection_build_types(false, count, mbb, s_file_spaces, s_mem_spaces, s_offsets, s_bufs,
                                            s_element_sizes, s_element_sizes, &final_ftype,
                                            &final_ftype_is_derived, &final_mtype,
                                            &final_mtype_is_derived) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "couldn't build type for MPI-IO");

            /* We have a single, complicated MPI datatype for both memory & file */
            size_i = 1;
        }
        else {

            /* No chunks selected for this process */
            size_i = 0;

            mpi_bufs_base = &unused;

            /* Set the MPI datatype */
            final_ftype = MPI_BYTE;
            final_mtype = MPI_BYTE;
        }

        /* some numeric conversions */
        if (H5FD_mpi_haddr_to_MPIOff(tmp_offset, &mpi_off /*out*/) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");

        /*
         * Set the file view when we are using MPI derived types
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, final_ftype,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code);

        /* When using types, use the address as the displacement for
         * MPI_File_set_view and reset the address for the read to zero
         */
        /* Reset mpi_off to 0 since the view now starts at the data offset */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, &mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stderr, "%s: (%d) using MPIO collective mode\n", __func__, file->mpi_rank);
#endif
        /* Get the collective_opt property to check whether the application wants to do IO individually. */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stderr, "%s: (%d) doing MPI collective IO\n", __func__, file->mpi_rank);
#endif
            /* Check whether we should read from rank 0 and broadcast to other ranks */
            if (H5CX_get_mpio_rank0_bcast()) {
#ifdef H5FDmpio_DEBUG
                if (H5FD_mpio_debug_r_flag)
                    fprintf(stderr, "%s: (%d) doing read-rank0-and-MPI_Bcast\n", __func__, file->mpi_rank);
#endif
                /* Indicate path we've taken */
                rank0_bcast = true;

                /* Read on rank 0 Bcast to other ranks */
                if (file->mpi_rank == 0) {
                    /* If MPI_File_read_at fails, push an error, but continue
                     * to participate in following MPI_Bcast */
                    if (MPI_SUCCESS != (mpi_code = MPI_File_read_at(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                    final_mtype, &mpi_stat)))
                        HMPI_DONE_ERROR(FAIL, "MPI_File_read_at failed", mpi_code);
                }

                if (MPI_SUCCESS != (mpi_code = MPI_Bcast(mpi_bufs_base, size_i, final_mtype, 0, file->comm)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_code);
            } /* end if */
            else
                /* Perform collective read operation */
                if (MPI_SUCCESS != (mpi_code = MPI_File_read_at_all(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                    final_mtype, &mpi_stat)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at_all failed", mpi_code);
        } /* end if */
        else {
#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_r_flag)
                fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif

            /* Perform independent read operation */
            if (MPI_SUCCESS != (mpi_code = MPI_File_read_at(file->f, mpi_off, mpi_bufs_base, size_i,
                                                            final_mtype, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_read_at failed", mpi_code);
        } /* end else */

        /*
         * Reset the file view when we used MPI derived types
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code);

        /* Only retrieve bytes read if this rank _actually_ participated in I/O */
        if (!rank0_bcast || (rank0_bcast && file->mpi_rank == 0)) {
            /* How many bytes were actually read? */
#if H5_CHECK_MPI_VERSION(3, 0)
            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, final_mtype, &bytes_read))) {
#else
            if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_read))) {
#endif
                if (rank0_bcast && file->mpi_rank == 0) {
                    /* If MPI_Get_elements(_x) fails for a rank 0 bcast strategy,
                     * push an error, but continue to participate in the following
                     * MPI_Bcast.
                     */
                    bytes_read = -1;
                    HMPI_DONE_ERROR(FAIL, "MPI_Get_elements failed", mpi_code);
                }
                else
                    HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code);
            }
        } /* end if */

        /* If the rank0-bcast feature was used, broadcast the # of bytes read to
         * other ranks, which didn't perform any I/O.
         */
        /* NOTE: This could be optimized further to be combined with the broadcast
         *          of the data.  (QAK - 2019/1/2)
         */
        if (rank0_bcast)
#if H5_CHECK_MPI_VERSION(3, 0)
            if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_COUNT, 0, file->comm))
#else
            if (MPI_SUCCESS != MPI_Bcast(&bytes_read, 1, MPI_INT, 0, file->comm))
#endif
                HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", 0);

                /* Get the type's size */
#if H5_CHECK_MPI_VERSION(3, 0)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size_x(final_mtype, &type_size)))
#else
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size(final_mtype, &type_size)))
#endif
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_size failed", mpi_code);

        /* Compute the actual number of bytes requested */
        io_size = type_size * size_i;

        /* Check for read failure */
        if (bytes_read < 0 || bytes_read > io_size)
            HGOTO_ERROR(H5E_IO, H5E_READERROR, FAIL, "file read failed");

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stderr, "%s: (%d) mpi_off = %ld  bytes_read = %lld  type = %s\n", __func__,
                    file->mpi_rank, (long)mpi_off, (long long)bytes_read, H5FD__mem_t_to_str(type));
#endif

        /*
         * This gives us zeroes beyond end of physical MPI file.
         */
        if ((n = (io_size - bytes_read)) > 0)
            memset((char *)bufs[0] + bytes_read, 0, (size_t)n);

    } /* end if */
    else {
#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_r_flag)
            fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif
        if (_file->base_addr > 0) {
            /* Undo base address addition in internal routines before passing down to the mpio driver */
            for (i = 0; i < count; i++) {
                assert(offsets[i] >= _file->base_addr);
                offsets[i] -= _file->base_addr;
            }
        }

        if (H5FD_read_from_selection(_file, type, (uint32_t)count, mem_space_ids, file_space_ids, offsets,
                                     element_sizes, bufs) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_READERROR, FAIL, "read vector from selection failed");
    }

done:
    /* Free the MPI buf and file types, if they were derived */
    if (final_mtype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&final_mtype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);
    if (final_ftype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&final_ftype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);

    /* Cleanup dataspace arrays */
    if (s_mem_spaces)
        s_mem_spaces = H5MM_xfree(s_mem_spaces);
    if (s_file_spaces)
        s_file_spaces = H5MM_xfree(s_file_spaces);

    if (!selection_was_sorted) {
        free(s_mem_space_ids);
        s_mem_space_ids = NULL;
        free(s_file_space_ids);
        s_file_space_ids = NULL;
        free(s_offsets);
        s_offsets = NULL;
        free(s_element_sizes);
        s_element_sizes = NULL;
        free(s_bufs);
        s_bufs = NULL;
    }

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)

} /* end H5FD__mpio_read_selection() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_write_selection
 *
 * Purpose:     The behavior of this function depends on the value of
 *              the transfer mode obtained from the context.
 *
 *              If the transfer mode is H5FD_MPIO_COLLECTIVE:
 *              --sort the selections
 *              --set mpi_bufs_base
 *              --build the MPI derived types
 *              --perform MPI_File_set_view()
 *              --perform MPI_File_write_at_all() or MPI_File_write_at()
 *                depending on whether this is a H5FD_MPIO_COLLECTIVE_IO
 *              --calculate and set the file's eof for the bytes written
 *
 *              If this is not H5FD_MPIO_COLLECTIVE:
 *              --undo possible base address addition in internal routines
 *              --call H5FD_write_vector_from_selection() to perform vector
 *                or scalar writes for the selections
 *
 * Return:      Success:    SUCCEED.
 *              Failure:    FAIL.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_write_selection(H5FD_t *_file, H5FD_mem_t type, hid_t H5_ATTR_UNUSED dxpl_id, size_t count,
                           hid_t mem_space_ids[], hid_t file_space_ids[], haddr_t offsets[],
                           size_t element_sizes[], const void *bufs[])
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
    MPI_Offset   mpi_off;
    MPI_Offset   save_mpi_off; /* Use at the end of the routine for setting local_eof */
    MPI_Status   mpi_stat;     /* Status from I/O operation */

    int                        size_i;
    H5FD_mpio_xfer_t           xfer_mode; /* I/O transfer mode */
    H5FD_mpio_collective_opt_t coll_opt_mode;

    MPI_Datatype final_mtype; /* Final memory MPI datatype for all pieces with selection */
    bool         final_mtype_is_derived = false;

    MPI_Datatype final_ftype; /* Final file MPI datatype for all pieces with selection */
    bool         final_ftype_is_derived = false;

    hid_t                   *s_mem_space_ids      = NULL;
    hid_t                   *s_file_space_ids     = NULL;
    haddr_t                 *s_offsets            = NULL;
    size_t                  *s_element_sizes      = NULL;
    H5_flexible_const_ptr_t *s_bufs               = NULL;
    bool                     selection_was_sorted = true;
    const void              *mpi_bufs_base        = NULL;

    uint32_t                i, j;
    H5S_t                 **s_mem_spaces  = NULL;
    H5S_t                 **s_file_spaces = NULL;
    haddr_t                 tmp_offset    = 0;
    char                    unused        = 0; /* Unused, except for non-NULL pointer value */
    H5_flexible_const_ptr_t mbb;

#if H5_CHECK_MPI_VERSION(3, 0)
    MPI_Count bytes_written;
    MPI_Count type_size; /* MPI datatype used for I/O's size */
    MPI_Count io_size;   /* Actual number of bytes requested */
#else
    int bytes_written;
    int type_size; /* MPI datatype used for I/O's size */
    int io_size;   /* Actual number of bytes requested */
#endif

#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
    bool H5FD_mpio_debug_w_flag = (H5FD_mpio_debug_flags_s[(int)'w'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* MPI return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);
    assert((count == 0) || (mem_space_ids));
    assert((count == 0) || (file_space_ids));
    assert((count == 0) || (offsets));
    assert((count == 0) || (element_sizes));
    assert((count == 0) || (bufs));

    /* Verify that the first elements of the element_sizes and bufs arrays are
     * valid. */
    assert((count == 0) || (element_sizes[0] != 0));
    assert((count == 0) || (bufs[0] != NULL));

    /* Verify that no data is written when between MPI_Barrier()s during file flush */
    assert(!H5CX_get_mpi_file_flushing());

    /* Portably initialize MPI status variable */
    memset(&mpi_stat, 0, sizeof(MPI_Status));

    /* Get the transfer mode from the API context */
    if (H5CX_get_io_xfer_mode(&xfer_mode) < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O transfer mode");

    if (xfer_mode == H5FD_MPIO_COLLECTIVE) {

        if (count) {
            if (H5FD_sort_selection_io_req(&selection_was_sorted, count, mem_space_ids, file_space_ids,
                                           offsets, element_sizes, (H5_flexible_const_ptr_t *)bufs,
                                           &s_mem_space_ids, &s_file_space_ids, &s_offsets, &s_element_sizes,
                                           &s_bufs) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_BADVALUE, FAIL, "can't sort selection I/O request");

            tmp_offset = s_offsets[0];

            if (NULL == (s_file_spaces = H5MM_malloc(count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for file space list");
            if (NULL == (s_mem_spaces = H5MM_malloc(count * sizeof(H5S_t *))))
                HGOTO_ERROR(H5E_RESOURCE, H5E_CANTALLOC, FAIL,
                            "memory allocation failed for memory space list");

            for (i = 0; i < count; i++) {
                if (NULL ==
                    (s_file_spaces[i] = (H5S_t *)H5I_object_verify(s_file_space_ids[i], H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, H5I_INVALID_HID,
                                "can't retrieve file dataspace from ID");
                if (NULL == (s_mem_spaces[i] = (H5S_t *)H5I_object_verify(s_mem_space_ids[i], H5I_DATASPACE)))
                    HGOTO_ERROR(H5E_VFL, H5E_BADTYPE, H5I_INVALID_HID,
                                "can't retrieve memory dataspace from ID");
            }

            /* when we setup mpi_bufs[] below, all addresses are offsets from
             * mpi_bufs_base.
             *
             * Since these offsets must all be positive, we must scan through
             * s_bufs[] to find the smallest value, and choose that for
             * mpi_bufs_base.
             */

            j = 0; /* guess at the index of the smallest value of s_bufs[] */
            if ((count > 1) && (s_bufs[1].cvp != NULL)) {
                for (i = 1; i < count; i++)
                    if (s_bufs[i].cvp < s_bufs[j].cvp)
                        j = i;
            }

            mpi_bufs_base = s_bufs[j].cvp;
            mbb.cvp       = mpi_bufs_base;

            if (H5FD__selection_build_types(true, count, mbb, s_file_spaces, s_mem_spaces, s_offsets, s_bufs,
                                            s_element_sizes, s_element_sizes, &final_ftype,
                                            &final_ftype_is_derived, &final_mtype,
                                            &final_mtype_is_derived) < 0)
                HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "couldn't build type for MPI-IO");

            /* We have a single, complicated MPI datatype for both memory & file */
            size_i = 1;
        }
        else {

            /* No chunks selected for this process */
            size_i = 0;

            mpi_bufs_base = &unused;

            /* Set the MPI datatype */
            final_ftype = MPI_BYTE;
            final_mtype = MPI_BYTE;
        }

        /* some numeric conversions */
        if (H5FD_mpi_haddr_to_MPIOff(tmp_offset, &mpi_off /*out*/) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't convert from haddr to MPI off");

        /* To be used at the end of the routine for setting local_eof */
        save_mpi_off = mpi_off;

        /*
         * Set the file view when we are using MPI derived types
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, mpi_off, MPI_BYTE, final_ftype,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code);

        /* Reset mpi_off to 0 since the view now starts at the data offset */
        if (H5FD_mpi_haddr_to_MPIOff((haddr_t)0, &mpi_off) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "can't set MPI off to 0");

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stderr, "%s: (%d) using MPIO collective mode\n", __func__, file->mpi_rank);
#endif

        /* Get the collective_opt property to check whether the application wants to do IO individually. */
        if (H5CX_get_mpio_coll_opt(&coll_opt_mode) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI-I/O collective_op property");

        if (coll_opt_mode == H5FD_MPIO_COLLECTIVE_IO) {

#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stderr, "%s: (%d) doing MPI collective IO\n", __func__, file->mpi_rank);
#endif

            /* Perform collective write operation */
            if (MPI_SUCCESS != (mpi_code = MPI_File_write_at_all(file->f, mpi_off, mpi_bufs_base, size_i,
                                                                 final_mtype, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at_all failed", mpi_code);

            /* Do MPI_File_sync when needed by underlying ROMIO driver */
            if (file->mpi_file_sync_required) {
                if (MPI_SUCCESS != (mpi_code = MPI_File_sync(file->f)))
                    HMPI_GOTO_ERROR(FAIL, "MPI_File_sync failed", mpi_code);
            }
        }
        else {

#ifdef H5FDmpio_DEBUG
            if (H5FD_mpio_debug_w_flag)
                fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif
            /* Perform independent write operation */
            if (MPI_SUCCESS != (mpi_code = MPI_File_write_at(file->f, mpi_off, mpi_bufs_base, size_i,
                                                             final_mtype, &mpi_stat)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_write_at failed", mpi_code);
        } /* end else */

        /* Reset the file view when we used MPI derived types */
        if (MPI_SUCCESS != (mpi_code = MPI_File_set_view(file->f, (MPI_Offset)0, MPI_BYTE, MPI_BYTE,
                                                         H5FD_mpi_native_g, file->info)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_set_view failed", mpi_code);

            /* How many bytes were actually written */
#if H5_CHECK_MPI_VERSION(3, 0)
        if (MPI_SUCCESS != (mpi_code = MPI_Get_elements_x(&mpi_stat, final_mtype, &bytes_written)))
#else
        if (MPI_SUCCESS != (mpi_code = MPI_Get_elements(&mpi_stat, MPI_BYTE, &bytes_written)))
#endif
            HMPI_GOTO_ERROR(FAIL, "MPI_Get_elements failed", mpi_code);

            /* Get the type's size */
#if H5_CHECK_MPI_VERSION(3, 0)
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size_x(final_mtype, &type_size)))
#else
        if (MPI_SUCCESS != (mpi_code = MPI_Type_size(final_mtype, &type_size)))
#endif
            HMPI_GOTO_ERROR(FAIL, "MPI_Type_size failed", mpi_code);

        /* Compute the actual number of bytes requested */
        io_size = type_size * size_i;

        /* Check for write failure */
        if (bytes_written != io_size || bytes_written < 0)
            HGOTO_ERROR(H5E_IO, H5E_WRITEERROR, FAIL, "file write failed");

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stderr, "%s: (%d) mpi_off = %ld  bytes_written = %lld  type = %s\n", __func__,
                    file->mpi_rank, (long)mpi_off, (long long)bytes_written, H5FD__mem_t_to_str(type));
#endif

        /* Each process will keep track of its perceived EOF value locally, and
         * ultimately we will reduce this value to the maximum amongst all
         * processes, but until then keep the actual eof at HADDR_UNDEF just in
         * case something bad happens before that point. (rather have a value
         * we know is wrong sitting around rather than one that could only
         * potentially be wrong.) */
        file->eof = HADDR_UNDEF;

        if (bytes_written && (((haddr_t)bytes_written + (haddr_t)save_mpi_off) > file->local_eof))
            file->local_eof = (haddr_t)save_mpi_off + (haddr_t)bytes_written;
    }
    else { /* Not H5FD_MPIO_COLLECTIVE */

#ifdef H5FDmpio_DEBUG
        if (H5FD_mpio_debug_w_flag)
            fprintf(stderr, "%s: (%d) doing MPI independent IO\n", __func__, file->mpi_rank);
#endif
        if (_file->base_addr > 0) {
            /* Undo base address addition in internal routines before passing down to the mpio driver */
            for (i = 0; i < count; i++) {
                assert(offsets[i] >= _file->base_addr);
                offsets[i] -= _file->base_addr;
            }
        }

        if (H5FD_write_from_selection(_file, type, (uint32_t)count, mem_space_ids, file_space_ids, offsets,
                                      element_sizes, bufs) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_WRITEERROR, FAIL, "write vector from selection failed");
    }

done:
    /* Free the MPI buf and file types, if they were derived */
    if (final_mtype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&final_mtype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);
    if (final_ftype_is_derived && MPI_SUCCESS != (mpi_code = MPI_Type_free(&final_ftype)))
        HMPI_DONE_ERROR(FAIL, "MPI_Type_free failed", mpi_code);

    /* Cleanup dataspace arrays */
    if (s_mem_spaces)
        s_mem_spaces = H5MM_xfree(s_mem_spaces);
    if (s_file_spaces)
        s_file_spaces = H5MM_xfree(s_file_spaces);

    if (!selection_was_sorted) {
        free(s_mem_space_ids);
        s_mem_space_ids = NULL;
        free(s_file_space_ids);
        s_file_space_ids = NULL;
        free(s_offsets);
        s_offsets = NULL;
        free(s_element_sizes);
        s_element_sizes = NULL;
        free(s_bufs);
        s_bufs = NULL;
    }

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving: ret_value = %d\n", __func__, file->mpi_rank, ret_value);
#endif

    FUNC_LEAVE_NOAPI(ret_value)

} /* end H5FD__mpio_write_selection() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_flush
 *
 * Purpose:     Makes sure that all data is on disk.  This is collective.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_flush(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool closing)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    int    mpi_code; /* mpi return code */
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    /* Only sync the file if we are not going to immediately close it */
    if (!closing)
        if (MPI_SUCCESS != (mpi_code = MPI_File_sync(file->f)))
            HMPI_GOTO_ERROR(FAIL, "MPI_File_sync failed", mpi_code)

done:
#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_flush() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_truncate
 *
 * Purpose:     Make certain the file's size matches it's allocated size
 *
 *              This is a little sticky in the mpio case, as it is not
 *              easy for us to track the current EOF by extracting it from
 *              write calls, since other ranks could have written to the
 *              file beyond the local EOF.
 *
 *              Instead, we first check to see if the EOA has changed since
 *              the last call to this function.  If it has, we call
 *              MPI_File_get_size() to determine the current EOF, and
 *              only call MPI_File_set_size() if this value disagrees
 *              with the current EOA.
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_truncate(H5FD_t *_file, hid_t H5_ATTR_UNUSED dxpl_id, bool H5_ATTR_UNUSED closing)
{
    H5FD_mpio_t *file = (H5FD_mpio_t *)_file;
#ifdef H5FDmpio_DEBUG
    bool H5FD_mpio_debug_t_flag = (H5FD_mpio_debug_flags_s[(int)'t'] && H5FD_MPIO_TRACE_THIS_RANK(file));
#endif
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_PACKAGE

#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Entering\n", __func__, file->mpi_rank);
#endif

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    if (!H5_addr_eq(file->eoa, file->last_eoa)) {
        int        mpi_code; /* mpi return code */
        MPI_Offset size;
        MPI_Offset needed_eof;

        /* In principle, it is possible for the size returned by the
         * call to MPI_File_get_size() to depend on whether writes from
         * all processes have completed at the time process 0 makes the
         * call.
         *
         * In practice, most (all?) truncate calls will come after a barrier
         * and with no intervening writes to the file (with the possible
         * exception of sueprblock / superblock extension message updates).
         *
         * Check the "MPI file closing" flag in the API context to determine
         * if we can skip the barrier.
         */
        if (!H5CX_get_mpi_file_flushing())
            if (MPI_SUCCESS != (mpi_code = MPI_Barrier(file->comm)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_code)

        /* Only processor p0 will get the filesize and broadcast it. */
        if (0 == file->mpi_rank) {
            /* If MPI_File_get_size fails, broadcast file size as -1 to signal error */
            if (MPI_SUCCESS != (mpi_code = MPI_File_get_size(file->f, &size)))
                size = (MPI_Offset)-1;
        }

        /* Broadcast file size */
        if (MPI_SUCCESS != (mpi_code = MPI_Bcast(&size, (int)sizeof(MPI_Offset), MPI_BYTE, 0, file->comm)))
            HMPI_GOTO_ERROR(FAIL, "MPI_Bcast failed", mpi_code)

        if (size < 0)
            HMPI_GOTO_ERROR(FAIL, "MPI_File_get_size failed", mpi_code)

        if (H5FD_mpi_haddr_to_MPIOff(file->eoa, &needed_eof) < 0)
            HGOTO_ERROR(H5E_INTERNAL, H5E_BADRANGE, FAIL, "cannot convert from haddr_t to MPI_Offset");

        /* EOA != EOF.  Set EOF to EOA */
        if (size != needed_eof) {
            /* Extend the file's size */
            if (MPI_SUCCESS != (mpi_code = MPI_File_set_size(file->f, needed_eof)))
                HMPI_GOTO_ERROR(FAIL, "MPI_File_set_size failed", mpi_code)

            /* In general, we must wait until all processes have finished
             * the truncate before any process can continue, since it is
             * possible that a process would write at the end of the
             * file, and this write would be discarded by the truncate.
             *
             * While this is an issue for a user initiated flush, it may
             * not be an issue at file close.  If so, we may be able to
             * optimize out the following barrier in that case.
             */
            if (MPI_SUCCESS != (mpi_code = MPI_Barrier(file->comm)))
                HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_code)
        } /* end if */

        /* Update the 'last' eoa value */
        file->last_eoa = file->eoa;
    } /* end if */

done:
#ifdef H5FDmpio_DEBUG
    if (H5FD_mpio_debug_t_flag)
        fprintf(stderr, "%s: (%d) Leaving\n", __func__, file->mpi_rank);
#endif

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_delete(const char *filename, hid_t fapl_id)
{
    H5P_genplist_t *plist; /* Property list pointer */
    MPI_Comm        comm     = MPI_COMM_NULL;
    MPI_Info        info     = MPI_INFO_NULL;
    int             mpi_rank = INT_MAX;
    int             mpi_code;            /* MPI return code */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(filename);

    if (NULL == (plist = H5P_object_verify(fapl_id, H5P_FILE_ACCESS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a file access property list");
    assert(H5FD_MPIO == H5P_peek_driver(plist));

    if (H5FD_mpi_self_initialized) {
        comm = MPI_COMM_WORLD;
    }
    else {
        /* Get the MPI communicator and info from the fapl */
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_INFO_NAME, &info) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI info object");
        if (H5P_get(plist, H5F_ACS_MPI_PARAMS_COMM_NAME, &comm) < 0)
            HGOTO_ERROR(H5E_VFL, H5E_CANTGET, FAIL, "can't get MPI communicator");
    }

    /* Get the MPI rank of this process */
    if (MPI_SUCCESS != (mpi_code = MPI_Comm_rank(comm, &mpi_rank)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Comm_rank failed", mpi_code)

    /* Set up a barrier */
    if (MPI_SUCCESS != (mpi_code = MPI_Barrier(comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_code)

    /* Delete the file */
    if (mpi_rank == 0) {
        /* If MPI_File_delete fails, push an error but
         * still participate in the following MPI_Barrier
         */
        if (MPI_SUCCESS != (mpi_code = MPI_File_delete(filename, info)))
            HMPI_DONE_ERROR(FAIL, "MPI_File_delete failed", mpi_code)
    }

    /* Set up a barrier (don't want processes to run ahead of the delete) */
    if (MPI_SUCCESS != (mpi_code = MPI_Barrier(comm)))
        HMPI_GOTO_ERROR(FAIL, "MPI_Barrier failed", mpi_code)

done:
    /* Free duplicated MPI Communicator and Info objects */
    if (H5_mpi_comm_free(&comm) < 0)
        HDONE_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "unable to free MPI communicator");
    if (H5_mpi_info_free(&info) < 0)
        HDONE_ERROR(H5E_VFL, H5E_CANTFREE, FAIL, "unable to free MPI info object");

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5FD__mpio_delete() */

/*-------------------------------------------------------------------------
 * Function:    H5FD__mpio_ctl
 *
 * Purpose:     MPIO version of the ctl callback.
 *
 *              The desired operation is specified by the op_code
 *              parameter.
 *
 *              The flags parameter controls management of op_codes that
 *              are unknown to the callback
 *
 *              The input and output parameters allow op_code specific
 *              input and output
 *
 *              At present, the supported op codes are:
 *
 *                  H5FD_CTL_GET_MPI_COMMUNICATOR_OPCODE
 *                  H5FD_CTL_GET_MPI_RANK_OPCODE
 *                  H5FD_CTL_GET_MPI_SIZE_OPCODE
 *                  H5FD_CTL_GET_MPI_FILE_SYNC_OPCODE
 *
 *              Note that these opcodes must be supported by all VFDs that
 *              support MPI.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD__mpio_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void H5_ATTR_UNUSED *input,
               void **output)
{
    H5FD_mpio_t *file      = (H5FD_mpio_t *)_file;
    herr_t       ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity checks */
    assert(file);
    assert(H5FD_MPIO == file->pub.driver_id);

    switch (op_code) {

        case H5FD_CTL_GET_MPI_COMMUNICATOR_OPCODE:
            assert(output);
            assert(*output);
            **((MPI_Comm **)output) = file->comm;
            break;

        case H5FD_CTL_GET_MPI_RANK_OPCODE:
            assert(output);
            assert(*output);
            **((int **)output) = file->mpi_rank;
            break;

        case H5FD_CTL_GET_MPI_SIZE_OPCODE:
            assert(output);
            assert(*output);
            **((int **)output) = file->mpi_size;
            break;

        case H5FD_CTL_GET_MPI_FILE_SYNC_OPCODE:
            assert(output);
            assert(*output);
            **((bool **)output) = file->mpi_file_sync_required;
            break;

        default: /* unknown op code */
            if (flags & H5FD_CTL_FAIL_IF_UNKNOWN_FLAG) {

                HGOTO_ERROR(H5E_VFL, H5E_FCNTL, FAIL, "unknown op_code and fail if unknown");
            }
            break;
    }

done:

    FUNC_LEAVE_NOAPI(ret_value)

} /* end H5FD__mpio_ctl() */
#endif /* H5_HAVE_PARALLEL */
