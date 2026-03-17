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
 * Purpose:    Implements a file driver which dispatches I/O requests to
 *        other file drivers depending on the purpose of the address
 *        region being accessed. For instance, all meta-data could be
 *        place in one file while all raw data goes to some other file.
 *        This also serves as an example of coding a complex file driver,
 *        therefore, it should not use any non-public definitions.
 */
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "hdf5.h"

#ifndef false
#define false 0
#endif
#ifndef true
#define true 1
#endif

/* Windows doesn't like some POSIX names and redefines them with an
 * underscore
 */
#ifdef _WIN32
#define my_strdup _strdup
#else
#define my_strdup strdup
#endif

/* Macros for enabling/disabling particular GCC / clang warnings
 *
 * These are duplicated in H5private.h. If you make changes here, be sure to
 * update those as well.
 *
 * (see the following web-sites for more info:
 *      http://www.dbp-consulting.com/tutorials/SuppressingGCCWarnings.html
 *      http://gcc.gnu.org/onlinedocs/gcc/Diagnostic-Pragmas.html#Diagnostic-Pragmas
 */
#define H5_MULTI_DIAG_JOINSTR(x, y) x y
#define H5_MULTI_DIAG_DO_PRAGMA(x)  _Pragma(#x)
#define H5_MULTI_DIAG_PRAGMA(x)     H5_MULTI_DIAG_DO_PRAGMA(GCC diagnostic x)

#define H5_MULTI_DIAG_OFF(x)                                                                                 \
    H5_MULTI_DIAG_PRAGMA(push) H5_MULTI_DIAG_PRAGMA(ignored H5_MULTI_DIAG_JOINSTR("-W", x))
#define H5_MULTI_DIAG_ON(x) H5_MULTI_DIAG_PRAGMA(pop)

/* Macros for enabling/disabling particular GCC / clang warnings.
 * These macros should be used for warnings supported by both gcc and clang.
 */
#if (((__GNUC__ * 100) + __GNUC_MINOR__) >= 406) || defined(__clang__)
#define H5_MULTI_GCC_CLANG_DIAG_OFF(x) H5_MULTI_DIAG_OFF(x)
#define H5_MULTI_GCC_CLANG_DIAG_ON(x)  H5_MULTI_DIAG_ON(x)
#else
#define H5_MULTI_GCC_CLANG_DIAG_OFF(x)
#define H5_MULTI_GCC_CLANG_DIAG_ON(x)
#endif

/* Loop through all mapped files */
#define UNIQUE_MEMBERS_CORE(MAP, ITER, SEEN, LOOPVAR)                                                        \
    {                                                                                                        \
        H5FD_mem_t ITER, LOOPVAR;                                                                            \
        unsigned   SEEN[H5FD_MEM_NTYPES];                                                                    \
                                                                                                             \
        memset(SEEN, 0, sizeof SEEN);                                                                        \
        for (ITER = H5FD_MEM_SUPER; ITER < H5FD_MEM_NTYPES; ITER = (H5FD_mem_t)(ITER + 1)) {                 \
            LOOPVAR = MAP[ITER];                                                                             \
            if (H5FD_MEM_DEFAULT == LOOPVAR)                                                                 \
                LOOPVAR = ITER;                                                                              \
            assert(LOOPVAR > 0 && LOOPVAR < H5FD_MEM_NTYPES);                                                \
            if (SEEN[LOOPVAR]++)                                                                             \
                continue;

/* Need two front-ends, since they are nested sometimes */
#define UNIQUE_MEMBERS(MAP, LOOPVAR)  UNIQUE_MEMBERS_CORE(MAP, _unmapped, _seen, LOOPVAR)
#define UNIQUE_MEMBERS2(MAP, LOOPVAR) UNIQUE_MEMBERS_CORE(MAP, _unmapped2, _seen2, LOOPVAR)

#define ALL_MEMBERS(LOOPVAR)                                                                                 \
    {                                                                                                        \
        H5FD_mem_t LOOPVAR;                                                                                  \
        for (LOOPVAR = H5FD_MEM_DEFAULT; LOOPVAR < H5FD_MEM_NTYPES; LOOPVAR = (H5FD_mem_t)(LOOPVAR + 1)) {

#define END_MEMBERS                                                                                          \
    }                                                                                                        \
    }

#define H5FD_MULT_MAX_FILE_NAME_LEN 1024

/* The driver identification number, initialized at runtime */
static hid_t H5FD_MULTI_g = 0;

/* Driver-specific file access properties */
typedef struct H5FD_multi_fapl_t {
    H5FD_mem_t memb_map[H5FD_MEM_NTYPES];  /*memory usage map              */
    hid_t      memb_fapl[H5FD_MEM_NTYPES]; /*member access properties      */
    char      *memb_name[H5FD_MEM_NTYPES]; /*name generators               */
    haddr_t    memb_addr[H5FD_MEM_NTYPES]; /*starting addr per member      */
    bool       relax;                      /*less stringent error checking */
} H5FD_multi_fapl_t;

/*
 * The description of a file belonging to this driver. The file access
 * properties and member names do not have to be copied into this struct
 * since they will be held open by the file access property list which is
 * copied into the parent file struct in H5F_open().
 */
typedef struct H5FD_multi_t {
    H5FD_t            pub;                        /*public stuff, must be first               */
    H5FD_multi_fapl_t fa;                         /*driver-specific file access properties    */
    haddr_t           memb_next[H5FD_MEM_NTYPES]; /*addr of next member                       */
    H5FD_t           *memb[H5FD_MEM_NTYPES];      /*member pointers                           */
    haddr_t           memb_eoa[H5FD_MEM_NTYPES];  /*EOA for individual files,
                                                   *end of allocated addresses.  v1.6 library
                                                   *have the EOA for the entire file. But it's
                                                   *meaningless for MULTI file.  We replaced it
                                                   *with the EOAs for individual files        */
    unsigned flags;                               /*file open flags saved for debugging       */
    char    *name;                                /*name passed to H5Fopen or H5Fcreate       */
} H5FD_multi_t;

/* Driver specific data transfer properties */
typedef struct H5FD_multi_dxpl_t {
    hid_t memb_dxpl[H5FD_MEM_NTYPES]; /*member data xfer properties*/
} H5FD_multi_dxpl_t;

/* Private functions */
static herr_t H5FD_split_populate_config(const char *meta_ext, hid_t meta_plist_id, const char *raw_ext,
                                         hid_t raw_plist_id, bool relax, H5FD_multi_fapl_t *fa_out);
static herr_t H5FD_multi_populate_config(const H5FD_mem_t *memb_map, const hid_t *memb_fapl,
                                         const char *const *memb_name, const haddr_t *memb_addr, bool relax,
                                         H5FD_multi_fapl_t *fa_out);
static int    compute_next(H5FD_multi_t *file);
static int    open_members(H5FD_multi_t *file);

/* Callback prototypes */
static herr_t  H5FD_multi_term(void);
static hsize_t H5FD_multi_sb_size(H5FD_t *file);
static herr_t  H5FD_multi_sb_encode(H5FD_t *file, char *name /*out*/, unsigned char *buf /*out*/);
static herr_t  H5FD_multi_sb_decode(H5FD_t *file, const char *name, const unsigned char *buf);
static void   *H5FD_multi_fapl_get(H5FD_t *file);
static void   *H5FD_multi_fapl_copy(const void *_old_fa);
static herr_t  H5FD_multi_fapl_free(void *_fa);
static H5FD_t *H5FD_multi_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t  H5FD_multi_close(H5FD_t *_file);
static int     H5FD_multi_cmp(const H5FD_t *_f1, const H5FD_t *_f2);
static herr_t  H5FD_multi_query(const H5FD_t *_f1, unsigned long *flags);
static herr_t  H5FD_multi_get_type_map(const H5FD_t *file, H5FD_mem_t *type_map);
static haddr_t H5FD_multi_get_eoa(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD_multi_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t eoa);
static haddr_t H5FD_multi_get_eof(const H5FD_t *_file, H5FD_mem_t type);
static herr_t  H5FD_multi_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle);
static haddr_t H5FD_multi_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size);
static herr_t  H5FD_multi_free(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size);
static herr_t  H5FD_multi_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                               void *_buf /*out*/);
static herr_t  H5FD_multi_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                                const void *_buf);
static herr_t  H5FD_multi_flush(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD_multi_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing);
static herr_t  H5FD_multi_lock(H5FD_t *_file, bool rw);
static herr_t  H5FD_multi_unlock(H5FD_t *_file);
static herr_t  H5FD_multi_delete(const char *filename, hid_t fapl_id);
static herr_t  H5FD_multi_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input,
                              void **output);

/* The class struct */
static const H5FD_class_t H5FD_multi_g = {
    H5FD_CLASS_VERSION,        /* struct version    */
    H5_VFD_MULTI,              /* value             */
    "multi",                   /* name              */
    HADDR_MAX,                 /* maxaddr           */
    H5F_CLOSE_WEAK,            /* fc_degree         */
    H5FD_multi_term,           /* terminate         */
    H5FD_multi_sb_size,        /* sb_size           */
    H5FD_multi_sb_encode,      /* sb_encode         */
    H5FD_multi_sb_decode,      /* sb_decode         */
    sizeof(H5FD_multi_fapl_t), /* fapl_size         */
    H5FD_multi_fapl_get,       /* fapl_get          */
    H5FD_multi_fapl_copy,      /* fapl_copy         */
    H5FD_multi_fapl_free,      /* fapl_free         */
    0,                         /* dxpl_size         */
    NULL,                      /* dxpl_copy         */
    NULL,                      /* dxpl_free         */
    H5FD_multi_open,           /* open              */
    H5FD_multi_close,          /* close             */
    H5FD_multi_cmp,            /* cmp               */
    H5FD_multi_query,          /* query             */
    H5FD_multi_get_type_map,   /* get_type_map      */
    H5FD_multi_alloc,          /* alloc             */
    H5FD_multi_free,           /* free              */
    H5FD_multi_get_eoa,        /* get_eoa           */
    H5FD_multi_set_eoa,        /* set_eoa           */
    H5FD_multi_get_eof,        /* get_eof           */
    H5FD_multi_get_handle,     /* get_handle        */
    H5FD_multi_read,           /* read              */
    H5FD_multi_write,          /* write             */
    NULL,                      /*read_vector        */
    NULL,                      /*write_vector       */
    NULL,                      /* read_selection    */
    NULL,                      /* write_selection   */
    H5FD_multi_flush,          /* flush             */
    H5FD_multi_truncate,       /* truncate          */
    H5FD_multi_lock,           /* lock              */
    H5FD_multi_unlock,         /* unlock            */
    H5FD_multi_delete,         /* del               */
    H5FD_multi_ctl,            /* ctl               */
    H5FD_FLMAP_DEFAULT         /* fl_map            */
};

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_init
 *
 * Purpose:     Initialize this driver by registering the driver with the
 *              library.
 *
 * Return:      Success:    The driver ID for the multi driver
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5FD_multi_init(void)
{
    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (H5I_VFL != H5Iget_type(H5FD_MULTI_g))
        H5FD_MULTI_g = H5FDregister(&H5FD_multi_g);

    return H5FD_MULTI_g;
} /* end H5FD_multi_init() */

/*---------------------------------------------------------------------------
 * Function:    H5FD_multi_term
 *
 * Purpose:     Shut down the VFD
 *
 * Returns:     Non-negative on success or negative on failure
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_term(void)
{
    /* Reset VFL ID */
    H5FD_MULTI_g = 0;

    return 0;
} /* end H5FD_multi_term() */

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_split
 *
 * Purpose:    Compatibility function. Makes the multi driver act like the
 *        old split driver which stored meta data in one file and raw
 *        data in another file.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_split(hid_t fapl, const char *meta_ext, hid_t meta_plist_id, const char *raw_ext,
                  hid_t raw_plist_id)
{
    H5FD_multi_fapl_t  fa;
    static const char *func = "H5Pset_fapl_split"; /* Function Name for error reporting */

    /*NO TRACE*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (H5FD_split_populate_config(meta_ext, meta_plist_id, raw_ext, raw_plist_id, true, &fa) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET, "can't setup split driver configuration",
                    -1);

    return H5Pset_driver(fapl, H5FD_MULTI, &fa);
}

/*-------------------------------------------------------------------------
 * Function:    H5Pset_fapl_multi
 *
 * Purpose:    Sets the file access property list FAPL_ID to use the multi
 *        driver. The MEMB_MAP array maps memory usage types to other
 *        memory usage types and is the mechanism which allows the
 *        caller to specify how many files are created. The array
 *        contains H5FD_MEM_NTYPES entries which are either the value
 *        H5FD_MEM_DEFAULT or a memory usage type and the number of
 *        unique values determines the number of files which are
 *        opened.  For each memory usage type which will be associated
 *        with a file the MEMB_FAPL array should have a property list
 *        and the MEMB_NAME array should be a name generator (a
 *        printf-style format with a %s which will be replaced with the
 *        name passed to H5FDopen(), usually from H5Fcreate() or
 *        H5Fopen()).
 *
 *        If RELAX is set then opening an existing file for read-only
 *        access will not fail if some file members are missing.  This
 *        allows a file to be accessed in a limited sense if just the
 *        meta data is available.
 *
 * Defaults:    Default values for each of the optional arguments are:
 *
 *        memb_map:    The default member map has the value
 *                H5FD_MEM_DEFAULT for each element.
 *
 *        memb_fapl:    The value H5P_DEFAULT for each element.
 *
 *        memb_name:    The string `%s-X.h5' where `X' is one of the
 *                letters `s' (H5FD_MEM_SUPER),
 *                `b' (H5FD_MEM_BTREE), `r' (H5FD_MEM_DRAW),
 *                 `g' (H5FD_MEM_GHEAP), 'l' (H5FD_MEM_LHEAP),
 *                 `o' (H5FD_MEM_OHDR).
 *
 *        memb_addr:    The value HADDR_UNDEF for each element.
 *
 *
 * Example:    To set up a multi file access property list which partitions
 *        data into meta and raw files each being 1/2 of the address
 *        space one would say:
 *
 *            H5FD_mem_t mt, memb_map[H5FD_MEM_NTYPES];
 *            hid_t memb_fapl[H5FD_MEM_NTYPES];
 *            const char *memb[H5FD_MEM_NTYPES];
 *            haddr_t memb_addr[H5FD_MEM_NTYPES];
 *
 *            // The mapping...
 *            for (mt=0; mt<H5FD_MEM_NTYPES; mt++) {
 *                memb_map[mt] = H5FD_MEM_SUPER;
 *            }
 *            memb_map[H5FD_MEM_DRAW] = H5FD_MEM_DRAW;
 *
 *            // Member information
 *            memb_fapl[H5FD_MEM_SUPER] = H5P_DEFAULT;
 *            memb_name[H5FD_MEM_SUPER] = "%s.meta";
 *            memb_addr[H5FD_MEM_SUPER] = 0;
 *
 *            memb_fapl[H5FD_MEM_DRAW] = H5P_DEFAULT;
 *            memb_name[H5FD_MEM_DRAW] = "%s.raw";
 *            memb_addr[H5FD_MEM_DRAW] = HADDR_MAX/2;
 *
 *            hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
 *            H5Pset_fapl_multi(fapl, memb_map, memb_fapl,
 *                              memb_name, memb_addr, true);
 *
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pset_fapl_multi(hid_t fapl_id, const H5FD_mem_t *memb_map, const hid_t *memb_fapl,
                  const char *const *memb_name, const haddr_t *memb_addr, hbool_t relax)
{
    H5FD_multi_fapl_t  fa;
    static const char *func = "H5FDset_fapl_multi"; /* Function Name for error reporting */

    /*NO TRACE*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check arguments and supply default values */
    if (H5I_GENPROP_LST != H5Iget_type(fapl_id) || true != H5Pisa_class(fapl_id, H5P_FILE_ACCESS))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_PLIST, H5E_BADVALUE, "not an access list", -1);
    if (H5FD_multi_populate_config(memb_map, memb_fapl, memb_name, memb_addr, relax, &fa) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET, "can't setup driver configuration", -1);

    return H5Pset_driver(fapl_id, H5FD_MULTI, &fa);
}

/*-------------------------------------------------------------------------
 * Function:    H5Pget_fapl_multi
 *
 * Purpose:    Returns information about the multi file access property
 *        list though the function arguments which are the same as for
 *        H5Pset_fapl_multi() above.
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5Pget_fapl_multi(hid_t fapl_id, H5FD_mem_t *memb_map /*out*/, hid_t *memb_fapl /*out*/,
                  char **memb_name /*out*/, haddr_t *memb_addr /*out*/, hbool_t *relax)
{
    const H5FD_multi_fapl_t *fa;
    H5FD_multi_fapl_t        default_fa;
    H5FD_mem_t               mt;
    static const char       *func = "H5FDget_fapl_multi"; /* Function Name for error reporting */

    /*NO TRACE*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (H5I_GENPROP_LST != H5Iget_type(fapl_id) || true != H5Pisa_class(fapl_id, H5P_FILE_ACCESS))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_PLIST, H5E_BADTYPE, "not an access list", -1);
    if (H5FD_MULTI != H5Pget_driver(fapl_id))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_PLIST, H5E_BADVALUE, "incorrect VFL driver", -1);
    H5E_BEGIN_TRY
    {
        fa = (const H5FD_multi_fapl_t *)H5Pget_driver_info(fapl_id);
    }
    H5E_END_TRY
    if (!fa || (H5P_FILE_ACCESS_DEFAULT == fapl_id)) {
        if (H5FD_multi_populate_config(NULL, NULL, NULL, NULL, true, &default_fa) < 0)
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTSET, "can't setup default driver configuration",
                        -1);
        fa = &default_fa;
    }

    if (memb_map)
        memcpy(memb_map, fa->memb_map, H5FD_MEM_NTYPES * sizeof(H5FD_mem_t));
    if (memb_fapl) {
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
            if (fa->memb_fapl[mt] >= 0)
                memb_fapl[mt] = H5Pcopy(fa->memb_fapl[mt]);
            else
                memb_fapl[mt] = fa->memb_fapl[mt]; /*default or bad ID*/
        }
    }
    if (memb_name) {
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
            if (fa->memb_name[mt])
                memb_name[mt] = my_strdup(fa->memb_name[mt]);
            else
                memb_name[mt] = NULL;
        }
    }
    if (memb_addr)
        memcpy(memb_addr, fa->memb_addr, H5FD_MEM_NTYPES * sizeof(haddr_t));
    if (relax)
        *relax = fa->relax;

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_split_populate_config
 *
 * Purpose:    Populates a H5FD_multi_fapl_t structure with the provided
 *             split driver values, supplying defaults where values are not
 *             provided.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_split_populate_config(const char *meta_ext, hid_t meta_plist_id, const char *raw_ext, hid_t raw_plist_id,
                           bool relax, H5FD_multi_fapl_t *fa_out)
{
    static const char *func = "H5FD_split_populate_config"; /* Function Name for error reporting */
    static char
        meta_name_g[H5FD_MULT_MAX_FILE_NAME_LEN]; /* Static scratch buffer to store metadata member name */
    static char
        raw_name_g[H5FD_MULT_MAX_FILE_NAME_LEN]; /* Static scratch buffer to store raw data member name */
    const char *_memb_name[H5FD_MEM_NTYPES];
    H5FD_mem_t  _memb_map[H5FD_MEM_NTYPES];
    hid_t       _memb_fapl[H5FD_MEM_NTYPES];
    haddr_t     _memb_addr[H5FD_MEM_NTYPES];
    herr_t      ret_value = 0;

    assert(fa_out);

    /* Initialize */
    ALL_MEMBERS (mt) {
        /* Treat global heap as raw data, not metadata */
        _memb_map[mt]  = ((mt == H5FD_MEM_DRAW || mt == H5FD_MEM_GHEAP) ? H5FD_MEM_DRAW : H5FD_MEM_SUPER);
        _memb_fapl[mt] = H5P_DEFAULT;
        _memb_name[mt] = NULL;
        _memb_addr[mt] = HADDR_UNDEF;
    }
    END_MEMBERS

    /* The file access properties */
    _memb_fapl[H5FD_MEM_SUPER] = meta_plist_id;
    _memb_fapl[H5FD_MEM_DRAW]  = raw_plist_id;

    /* The names */
    /* process meta filename */
    if (meta_ext) {
        if (strstr(meta_ext, "%s")) {
            /* Note: this doesn't accommodate for when the '%s' in the user's
             *  string is at a position >sizeof(meta_name) - QK & JK - 2013/01/17
             */
            strncpy(meta_name_g, meta_ext, sizeof(meta_name_g));
            meta_name_g[sizeof(meta_name_g) - 1] = '\0';
        }
        else
            snprintf(meta_name_g, sizeof(meta_name_g), "%%s%s", meta_ext);
    }
    else {
        strncpy(meta_name_g, "%s.meta", sizeof(meta_name_g));
        meta_name_g[sizeof(meta_name_g) - 1] = '\0';
    }
    _memb_name[H5FD_MEM_SUPER] = meta_name_g;

    /* process raw filename */
    if (raw_ext) {
        if (strstr(raw_ext, "%s")) {
            /* Note: this doesn't accommodate for when the '%s' in the user's
             *  string is at a position >sizeof(raw_name) - QK & JK - 2013/01/17
             */
            strncpy(raw_name_g, raw_ext, sizeof(raw_name_g));
            raw_name_g[sizeof(raw_name_g) - 1] = '\0';
        }
        else
            snprintf(raw_name_g, sizeof(raw_name_g), "%%s%s", raw_ext);
    }
    else {
        strncpy(raw_name_g, "%s.raw", sizeof(raw_name_g));
        raw_name_g[sizeof(raw_name_g) - 1] = '\0';
    }
    _memb_name[H5FD_MEM_DRAW] = raw_name_g;

    /* The sizes */
    _memb_addr[H5FD_MEM_SUPER] = 0;
    _memb_addr[H5FD_MEM_DRAW]  = HADDR_MAX / 2;

    ALL_MEMBERS (mt) {
        /* Map usage type */
        H5FD_mem_t mmt = _memb_map[mt];
        if (mmt < 0 || mmt >= H5FD_MEM_NTYPES)
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADRANGE, "file resource type out of range", -1);

        /*
         * All members of MEMB_FAPL must be either defaults or actual file
         * access property lists.
         */
        if (H5P_DEFAULT != _memb_fapl[mmt] && true != H5Pisa_class(_memb_fapl[mmt], H5P_FILE_ACCESS))
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "file resource type incorrect", -1);

        /* All names must be defined */
        if (!_memb_name[mmt] || !_memb_name[mmt][0])
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "file resource type not set", -1);
    }
    END_MEMBERS

    /*
     * Initialize driver specific information. No need to copy it into the FA
     * struct since all members will be copied by H5Pset_driver().
     */
    memset(fa_out, 0, sizeof(H5FD_multi_fapl_t));
    memcpy(fa_out->memb_map, _memb_map, H5FD_MEM_NTYPES * sizeof(H5FD_mem_t));
    memcpy(fa_out->memb_fapl, _memb_fapl, H5FD_MEM_NTYPES * sizeof(hid_t));
    memcpy(fa_out->memb_name, _memb_name, H5FD_MEM_NTYPES * sizeof(char *));
    memcpy(fa_out->memb_addr, _memb_addr, H5FD_MEM_NTYPES * sizeof(haddr_t));
    fa_out->relax = relax;

    /* Patch up H5P_DEFAULT property lists for members */
    ALL_MEMBERS (mt) {
        if (fa_out->memb_fapl[mt] == H5P_DEFAULT) {
            fa_out->memb_fapl[mt] = H5Pcreate(H5P_FILE_ACCESS);
            if (H5Pset_fapl_sec2(fa_out->memb_fapl[mt]) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET,
                            "can't set sec2 driver on member FAPL", -1);
        }
    }
    END_MEMBERS

    return ret_value;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_populate_config
 *
 * Purpose:    Populates a H5FD_multi_fapl_t structure with the provided
 *             values, supplying defaults where values are not provided.
 *
 * Return:    Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_populate_config(const H5FD_mem_t *memb_map, const hid_t *memb_fapl, const char *const *memb_name,
                           const haddr_t *memb_addr, bool relax, H5FD_multi_fapl_t *fa_out)
{
    static const char *func    = "H5FD_multi_populate_config"; /* Function Name for error reporting */
    static const char *letters = "Xsbrglo";
    static char        _memb_name_g[H5FD_MEM_NTYPES][16]; /* Static scratch buffer to store member names */
    H5FD_mem_t         mt, mmt;
    H5FD_mem_t         _memb_map[H5FD_MEM_NTYPES];
    hid_t              _memb_fapl[H5FD_MEM_NTYPES];
    const char        *_memb_name_ptrs[H5FD_MEM_NTYPES];
    haddr_t            _memb_addr[H5FD_MEM_NTYPES];
    herr_t             ret_value = 0;

    assert(fa_out);

    if (!memb_map) {
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1))
            _memb_map[mt] = H5FD_MEM_DEFAULT;
        memb_map = _memb_map;
    }
    if (!memb_fapl) {
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
            _memb_fapl[mt] = H5Pcreate(H5P_FILE_ACCESS);
            if (H5Pset_fapl_sec2(_memb_fapl[mt]) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET,
                            "can't set sec2 driver on member FAPL", -1);
        }
        memb_fapl = _memb_fapl;
    }
    if (!memb_name) {
        assert(strlen(letters) == H5FD_MEM_NTYPES);
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
            snprintf(_memb_name_g[mt], 16, "%%s-%c.h5", letters[mt]);
            _memb_name_ptrs[mt] = _memb_name_g[mt];
        }
        memb_name = _memb_name_ptrs;
    }
    if (!memb_addr) {
        for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1))
            _memb_addr[mt] = (hsize_t)(mt ? (mt - 1) : 0) * (HADDR_MAX / (H5FD_MEM_NTYPES - 1));
        memb_addr = _memb_addr;
    }

    for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        /* Map usage type */
        mmt = memb_map[mt];
        if (mmt < 0 || mmt >= H5FD_MEM_NTYPES)
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADRANGE, "file resource type out of range", -1);
        if (H5FD_MEM_DEFAULT == mmt)
            mmt = mt;

        /*
         * All members of MEMB_FAPL must be either defaults or actual file
         * access property lists.
         */
        if (H5P_DEFAULT != memb_fapl[mmt] && true != H5Pisa_class(memb_fapl[mmt], H5P_FILE_ACCESS))
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "file resource type incorrect", -1);

        /* All names must be defined */
        if (!memb_name[mmt] || !memb_name[mmt][0])
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "file resource type not set", -1);
    }

    /*
     * Initialize driver specific information. No need to copy it into the FA
     * struct since all members will be copied by H5Pset_driver().
     */
    memset(fa_out, 0, sizeof(H5FD_multi_fapl_t));
    memcpy(fa_out->memb_map, memb_map, H5FD_MEM_NTYPES * sizeof(H5FD_mem_t));
    memcpy(fa_out->memb_fapl, memb_fapl, H5FD_MEM_NTYPES * sizeof(hid_t));
    memcpy(fa_out->memb_name, memb_name, H5FD_MEM_NTYPES * sizeof(char *));
    memcpy(fa_out->memb_addr, memb_addr, H5FD_MEM_NTYPES * sizeof(haddr_t));
    fa_out->relax = relax;

    /* Patch up H5P_DEFAULT property lists for members */
    for (mt = H5FD_MEM_DEFAULT; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        if (fa_out->memb_fapl[mt] == H5P_DEFAULT) {
            fa_out->memb_fapl[mt] = H5Pcreate(H5P_FILE_ACCESS);
            if (H5Pset_fapl_sec2(fa_out->memb_fapl[mt]) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET,
                            "can't set sec2 driver on member FAPL", -1);
        }
    }

    return ret_value;
} /* end H5FD_multi_populate_config() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_sb_size
 *
 * Purpose:    Returns the size of the private information to be stored in
 *             the superblock.
 *
 * Return:    Success:    The super block driver data size.
 *
 *            Failure:    never fails
 *
 *-------------------------------------------------------------------------
 */
static hsize_t
H5FD_multi_sb_size(H5FD_t *_file)
{
    H5FD_multi_t *file   = (H5FD_multi_t *)_file;
    unsigned      nseen  = 0;
    hsize_t       nbytes = 8; /*size of header*/

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* How many unique files? */
    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        nseen++;
    }
    END_MEMBERS

    /* Addresses and EOA markers */
    nbytes += nseen * 2 * 8;

    /* Name templates */
    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        size_t n = strlen(file->fa.memb_name[mt]) + 1;
        nbytes += (n + 7) & ~((size_t)0x0007);
    }
    END_MEMBERS

    return nbytes;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_sb_encode
 *
 * Purpose:    Encode driver information for the superblock. The NAME
 *        argument is a nine-byte buffer which will be initialized with
 *        an eight-character name/version number and null termination.
 *
 *        The encoding is a six-byte member mapping followed two bytes
 *        which are unused. For each unique file in usage-type order
 *        encode all the starting addresses as unsigned 64-bit integers,
 *        then all the EOA values as unsigned 64-bit integers, then all
 *        the template names as null terminated strings which are
 *        multiples of 8 characters.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_sb_encode(H5FD_t *_file, char *name /*out*/, unsigned char *buf /*out*/)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    haddr_t            memb_eoa;
    unsigned char     *p;
    size_t             nseen;
    size_t             i;
    H5FD_mem_t         m;
    static const char *func = "H5FD_multi_sb_encode"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Name and version number */
    strncpy(name, "NCSAmult", (size_t)9);
    name[8] = '\0';

    assert(7 == H5FD_MEM_NTYPES);

    for (m = H5FD_MEM_SUPER; m < H5FD_MEM_NTYPES; m = (H5FD_mem_t)(m + 1)) {
        buf[m - 1] = (unsigned char)file->fa.memb_map[m];
    }
    buf[6] = 0;
    buf[7] = 0;

    /*
     * Copy the starting addresses and EOA values into the buffer in order of
     * usage type but only for types which map to something unique.
     */

    /* Encode all starting addresses and EOA values */
    nseen = 0;
    p     = buf + 8;
    assert(sizeof(haddr_t) <= 8);
    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        memcpy(p, &(file->fa.memb_addr[mt]), sizeof(haddr_t));
        p += sizeof(haddr_t);
        memb_eoa = H5FDget_eoa(file->memb[mt], mt);
        memcpy(p, &memb_eoa, sizeof(haddr_t));
        p += sizeof(haddr_t);
        nseen++;
    }
    END_MEMBERS
    if (H5Tconvert(H5T_NATIVE_HADDR, H5T_STD_U64LE, nseen * 2, buf + 8, NULL, H5P_DEFAULT) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_DATATYPE, H5E_CANTCONVERT, "can't convert superblock info", -1);

    /* Encode all name templates */
    p = buf + 8 + nseen * 2 * 8;
    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        size_t n = strlen(file->fa.memb_name[mt]) + 1;
        strcpy((char *)p, file->fa.memb_name[mt]);
        p += n;
        for (i = n; i % 8; i++)
            *p++ = '\0';
    }
    END_MEMBERS

    return 0;
} /* end H5FD_multi_sb_encode() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_sb_decode
 *
 * Purpose:    Decodes the superblock information for this driver. The NAME
 *        argument is the eight-character (plus null termination) name
 *        stored in the file.
 *
 *        The FILE argument is updated according to the information in
 *        the superblock. This may mean that some member files are
 *        closed and others are opened.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_sb_decode(H5FD_t *_file, const char *name, const unsigned char *buf)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    char               x[2 * H5FD_MEM_NTYPES * 8];
    H5FD_mem_t         map[H5FD_MEM_NTYPES];
    int                i;
    size_t             nseen       = 0;
    bool               map_changed = false;
    bool               in_use[H5FD_MEM_NTYPES];
    const char        *memb_name[H5FD_MEM_NTYPES];
    haddr_t            memb_addr[H5FD_MEM_NTYPES];
    haddr_t            memb_eoa[H5FD_MEM_NTYPES];
    haddr_t           *ap;
    static const char *func = "H5FD_multi_sb_decode"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Make sure the name/version number is correct */
    if (strcmp(name, "NCSAmult") != 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_BADVALUE, "invalid multi superblock", -1);

    /* Set default values */
    ALL_MEMBERS (mt) {
        memb_addr[mt] = HADDR_UNDEF;
        memb_eoa[mt]  = HADDR_UNDEF;
        memb_name[mt] = NULL;
    }
    END_MEMBERS

    /*
     * Read the map and count the unique members.
     */
    memset(map, 0, sizeof map);

    for (i = 0; i < 6; i++) {
        map[i + 1] = (H5FD_mem_t)buf[i];
        if (file->fa.memb_map[i + 1] != map[i + 1])
            map_changed = true;
    }

    UNIQUE_MEMBERS (map, mt) {
        nseen++;
    }
    END_MEMBERS
    buf += 8;

    /* Decode Address and EOA values */
    assert(sizeof(haddr_t) <= 8);
    memcpy(x, buf, (nseen * 2 * 8));
    buf += nseen * 2 * 8;
    if (H5Tconvert(H5T_STD_U64LE, H5T_NATIVE_HADDR, nseen * 2, x, NULL, H5P_DEFAULT) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_DATATYPE, H5E_CANTCONVERT, "can't convert superblock info", -1);
    ap = (haddr_t *)((void *)x); /* Extra (void *) cast to quiet "cast to create alignment" warning -
                                    2019/07/05, QAK */
    UNIQUE_MEMBERS (map, mt) {
        memb_addr[_unmapped] = *ap++;
        memb_eoa[_unmapped]  = *ap++;
    }
    END_MEMBERS

    /* Decode name templates */
    UNIQUE_MEMBERS (map, mt) {
        size_t n             = strlen((const char *)buf) + 1;
        memb_name[_unmapped] = (const char *)buf;
        buf += (n + 7) & ~((unsigned)0x0007);
    }
    END_MEMBERS

    /*
     * Use the mapping saved in the superblock in preference to the one
     * already set for the file. Since we may have opened files which are no
     * longer needed we should close all those files. We'll open the new
     * files at the end.
     */
    if (map_changed) {
        /* Commit map */
        ALL_MEMBERS (mt) {
            file->fa.memb_map[mt] = map[mt];
        }
        END_MEMBERS

        /* Close files which are unused now */
        memset(in_use, 0, sizeof in_use);
        UNIQUE_MEMBERS (map, mt) {
            in_use[mt] = true;
        }
        END_MEMBERS
        ALL_MEMBERS (mt) {
            if (!in_use[mt] && file->memb[mt]) {
                (void)H5FDclose(file->memb[mt]);
                file->memb[mt] = NULL;
            }
            file->fa.memb_map[mt] = map[mt];
        }
        END_MEMBERS
    }

    /* Commit member starting addresses and name templates */
    ALL_MEMBERS (mt) {
        file->fa.memb_addr[mt] = memb_addr[mt];
        if (memb_name[mt]) {
            if (file->fa.memb_name[mt])
                free(file->fa.memb_name[mt]);
            file->fa.memb_name[mt] = my_strdup(memb_name[mt]);
        }
    }
    END_MEMBERS
    if (compute_next(file) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "compute_next() failed", -1);

    /* Open all necessary files */
    if (open_members(file) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "open_members() failed", -1);

    /* Set the EOA marker for all open files */
    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        if (file->memb[mt])
            if (H5FDset_eoa(file->memb[mt], mt, memb_eoa[mt]) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_CANTSET, "set_eoa() failed", -1);

        /* Save the individual EOAs in one place for later comparison (in H5FD_multi_set_eoa)
         */
        file->memb_eoa[mt] = memb_eoa[mt];
    }
    END_MEMBERS

    return 0;
} /* end H5FD_multi_sb_decode() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_fapl_get
 *
 * Purpose:    Returns a file access property list which indicates how the
 *        specified file is being accessed. The return list could be
 *        used to access another file the same way.
 *
 * Return:    Success:    Ptr to new file access property list with all
 *                members copied from the file struct.
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD_multi_fapl_get(H5FD_t *_file)
{
    H5FD_multi_t *file = (H5FD_multi_t *)_file;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    return H5FD_multi_fapl_copy(&(file->fa));
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_fapl_copy
 *
 * Purpose:    Copies the multi-specific file access properties.
 *
 * Return:    Success:    Ptr to a new property list
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5FD_multi_fapl_copy(const void *_old_fa)
{
    const H5FD_multi_fapl_t *old_fa  = (const H5FD_multi_fapl_t *)_old_fa;
    H5FD_multi_fapl_t       *new_fa  = (H5FD_multi_fapl_t *)calloc(1, sizeof(H5FD_multi_fapl_t));
    int                      nerrors = 0;
    static const char       *func    = "H5FD_multi_fapl_copy"; /* Function Name for error reporting */

    assert(new_fa);

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    memcpy(new_fa, old_fa, sizeof(H5FD_multi_fapl_t));
    ALL_MEMBERS (mt) {
        if (old_fa->memb_fapl[mt] >= 0) {
            if (H5Iinc_ref(old_fa->memb_fapl[mt]) < 0) {
                nerrors++;
                break;
            }
            new_fa->memb_fapl[mt] = old_fa->memb_fapl[mt];
        }
        if (old_fa->memb_name[mt]) {
            new_fa->memb_name[mt] = my_strdup(old_fa->memb_name[mt]);
            if (NULL == new_fa->memb_name[mt]) {
                nerrors++;
                break;
            }
        }
    }
    END_MEMBERS

    if (nerrors) {
        ALL_MEMBERS (mt) {
            if (new_fa->memb_fapl[mt] >= 0)
                (void)H5Idec_ref(new_fa->memb_fapl[mt]);
            if (new_fa->memb_name[mt])
                free(new_fa->memb_name[mt]);
        }
        END_MEMBERS
        free(new_fa);
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "can't release object on error", NULL);
    }
    return new_fa;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_fapl_free
 *
 * Purpose:    Frees the multi-specific file access properties.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_fapl_free(void *_fa)
{
    H5FD_multi_fapl_t *fa   = (H5FD_multi_fapl_t *)_fa;
    static const char *func = "H5FD_multi_fapl_free"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    ALL_MEMBERS (mt) {
        if (fa->memb_fapl[mt] >= 0)
            if (H5Idec_ref(fa->memb_fapl[mt]) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTCLOSEOBJ, "can't close property list", -1);
        if (fa->memb_name[mt])
            free(fa->memb_name[mt]);
    }
    END_MEMBERS
    free(fa);

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_open
 *
 * Purpose:    Creates and/or opens a multi HDF5 file.
 *
 * Return:    Success:    A pointer to a new file data structure. The
 *                public fields will be initialized by the
 *                caller, which is always H5FD_open().
 *
 *            Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5FD_t *
H5FD_multi_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr)
{
    H5FD_multi_t            *file       = NULL;
    hid_t                    close_fapl = -1;
    const H5FD_multi_fapl_t *fa;
    H5FD_mem_t               m;
    static const char       *func = "H5FD_multi_open"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Check arguments */
    if (!name || !*name)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADVALUE, "invalid file name", NULL);
    if (0 == maxaddr || HADDR_UNDEF == maxaddr)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_ARGS, H5E_BADRANGE, "bogus maxaddr", NULL);

    /*
     * Initialize the file from the file access properties, using default
     * values if necessary.  Make sure to use CALLOC here because the code
     * in H5FD_multi_set_eoa depends on the proper initialization of memb_eoa
     * in H5FD_multi_t.
     */
    if (NULL == (file = (H5FD_multi_t *)calloc((size_t)1, sizeof(H5FD_multi_t))))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_RESOURCE, H5E_NOSPACE, "memory allocation failed", NULL);
    H5E_BEGIN_TRY
    {
        fa = (const H5FD_multi_fapl_t *)H5Pget_driver_info(fapl_id);
    }
    H5E_END_TRY
    if (!fa || (H5P_FILE_ACCESS_DEFAULT == fapl_id) || (H5FD_MULTI != H5Pget_driver(fapl_id))) {
        char *env = getenv(HDF5_DRIVER);

        close_fapl = fapl_id = H5Pcreate(H5P_FILE_ACCESS);
        if (env && !strcmp(env, "split")) {
            if (H5Pset_fapl_split(fapl_id, NULL, H5P_DEFAULT, NULL, H5P_DEFAULT) < 0)
                H5Epush_goto(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTSET, "can't set property value", error);
        }
        else {
            if (H5Pset_fapl_multi(fapl_id, NULL, NULL, NULL, NULL, true) < 0)
                H5Epush_goto(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTSET, "can't set property value", error);
        }

        fa = (const H5FD_multi_fapl_t *)H5Pget_driver_info(fapl_id);
    }
    assert(fa);
    ALL_MEMBERS (mt) {
        file->fa.memb_map[mt]  = fa->memb_map[mt];
        file->fa.memb_addr[mt] = fa->memb_addr[mt];
        if (fa->memb_fapl[mt] >= 0)
            H5Iinc_ref(fa->memb_fapl[mt]);
        file->fa.memb_fapl[mt] = fa->memb_fapl[mt];
        if (fa->memb_name[mt])
            file->fa.memb_name[mt] = my_strdup(fa->memb_name[mt]);
        else
            file->fa.memb_name[mt] = NULL;
    }
    END_MEMBERS
    file->fa.relax = fa->relax;
    file->flags    = flags;
    file->name     = my_strdup(name);
    if (close_fapl >= 0)
        if (H5Pclose(close_fapl) < 0)
            H5Epush_goto(func, H5E_ERR_CLS, H5E_FILE, H5E_CANTCLOSEOBJ, "can't close property list", error);

    /* Compute derived properties and open member files */
    if (compute_next(file) < 0)
        H5Epush_goto(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "compute_next() failed", error);
    if (open_members(file) < 0)
        H5Epush_goto(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "open_members() failed", error);

    /* We must have opened at least the superblock file */
    if (H5FD_MEM_DEFAULT == (m = file->fa.memb_map[H5FD_MEM_SUPER]))
        m = H5FD_MEM_SUPER;
    if (NULL == file->memb[m])
        goto error;

    return (H5FD_t *)file;

error:
    /* Cleanup and fail */
    if (file) {
        ALL_MEMBERS (mt) {
            if (file->memb[mt])
                (void)H5FDclose(file->memb[mt]);
            if (file->fa.memb_fapl[mt] >= 0)
                (void)H5Idec_ref(file->fa.memb_fapl[mt]);
            if (file->fa.memb_name[mt])
                free(file->fa.memb_name[mt]);
        }
        END_MEMBERS
        if (file->name)
            free(file->name);
        free(file);
    }
    return NULL;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_close
 *
 * Purpose:    Closes a multi file.
 *
 * Return:    Success:    Non-negative
 *
 *            Failure:    Negative with as many members closed as
 *                possible. The only subsequent operation
 *                permitted on the file is a close operation.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_close(H5FD_t *_file)
{
    H5FD_multi_t      *file    = (H5FD_multi_t *)_file;
    int                nerrors = 0;
    static const char *func    = "H5FD_multi_close"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Close as many members as possible */
    ALL_MEMBERS (mt) {
        if (file->memb[mt]) {
            if (H5FDclose(file->memb[mt]) < 0) {
                nerrors++;
            }
            else {
                file->memb[mt] = NULL;
            }
        }
    }
    END_MEMBERS
    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "error closing member files", -1);

    /* Clean up other stuff */
    ALL_MEMBERS (mt) {
        if (file->fa.memb_fapl[mt] >= 0)
            (void)H5Idec_ref(file->fa.memb_fapl[mt]);
        if (file->fa.memb_name[mt])
            free(file->fa.memb_name[mt]);
    }
    END_MEMBERS

    free(file->name);
    free(file);
    return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_cmp
 *
 * Purpose:    Compares two file families to see if they are the same. It
 *        does this by comparing the first common member of the two
 *        families.  If the families have no members in common then the
 *        file with the earliest member is smaller than the other file.
 *        We abort if neither file has any members.
 *
 * Return:    Success:    like strcmp()
 *
 *            Failure:    never fails (arguments were checked by th caller).
 *
 *-------------------------------------------------------------------------
 */
static int
H5FD_multi_cmp(const H5FD_t *_f1, const H5FD_t *_f2)
{
    const H5FD_multi_t *f1     = (const H5FD_multi_t *)_f1;
    const H5FD_multi_t *f2     = (const H5FD_multi_t *)_f2;
    H5FD_mem_t          out_mt = H5FD_MEM_DEFAULT;
    int                 cmp    = 0;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    ALL_MEMBERS (mt) {
        out_mt = mt;
        if (f1->memb[mt] && f2->memb[mt])
            break;
        if (!cmp) {
            if (f1->memb[mt])
                cmp = -1;
            else if (f2->memb[mt])
                cmp = 1;
        }
    }
    END_MEMBERS
    assert(cmp || out_mt < H5FD_MEM_NTYPES);
    if (out_mt >= H5FD_MEM_NTYPES)
        return cmp;

    return H5FDcmp(f1->memb[out_mt], f2->memb[out_mt]);
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_query
 *
 * Purpose:    Set the flags that this VFL driver is capable of supporting.
 *              (listed in H5FDpublic.h)
 *
 * Return:    Success:    non-negative
 *
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_query(const H5FD_t *_f, unsigned long *flags /* out */)
{
    /* Shut compiler up */
    (void)_f;

    /* Set the VFL feature flags that this driver supports */
    if (flags) {
        *flags = 0;
        *flags |= H5FD_FEAT_DATA_SIEVE; /* OK to perform data sieving for faster raw data reads & writes */
        *flags |= H5FD_FEAT_AGGREGATE_SMALLDATA; /* OK to aggregate "small" raw data allocations */
        *flags |= H5FD_FEAT_USE_ALLOC_SIZE;      /* OK just pass the allocation size to the alloc callback */
        *flags |= H5FD_FEAT_PAGED_AGGR;          /* OK special file space mapping for paged aggregation */
    }                                            /* end if */

    return (0);
} /* end H5FD_multi_query() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_get_type_map
 *
 * Purpose:    Retrieve the memory type mapping for this file
 *
 * Return:    Success:    non-negative
 *            Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_get_type_map(const H5FD_t *_file, H5FD_mem_t *type_map)
{
    const H5FD_multi_t *file = (const H5FD_multi_t *)_file;

    /* Copy file's free space type mapping */
    memcpy(type_map, file->fa.memb_map, sizeof(file->fa.memb_map));

    return (0);
} /* end H5FD_multi_get_type_map() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_get_eoa
 *
 * Purpose:    Returns the end-of-address marker for the file. The EOA
 *        marker is the first address past the last byte allocated in
 *        the format address space.
 *
 * Return:    Success:    The end-of-address-marker
 *
 *            Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_multi_get_eoa(const H5FD_t *_file, H5FD_mem_t type)
{
    const H5FD_multi_t *file = (const H5FD_multi_t *)_file;
    haddr_t             eoa  = 0;
    static const char  *func = "H5FD_multi_get_eoa"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* The library used to have EOA for the whole file.  But it's
     * taken out because it makes little sense for MULTI files.
     * However, the library sometimes queries it through H5F_get_eoa.
     * Here the code finds the biggest EOA for individual file if
     * the query is for TYPE == H5FD_MEM_DEFAULT.
     */
    if (H5FD_MEM_DEFAULT == type) {
        UNIQUE_MEMBERS (file->fa.memb_map, mt) {
            haddr_t memb_eoa;

            if (file->memb[mt]) {
                /* Retrieve EOA */
                H5E_BEGIN_TRY
                {
                    memb_eoa = H5FDget_eoa(file->memb[mt], mt);
                }
                H5E_END_TRY

                if (HADDR_UNDEF == memb_eoa)
                    H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "member file has unknown eoa",
                                HADDR_UNDEF);
                if (memb_eoa > 0)
                    memb_eoa += file->fa.memb_addr[mt];
            }
            else if (file->fa.relax) {
                /*
                 * The member is not open yet (maybe it doesn't exist). Make the
                 * best guess about the end-of-file.
                 */
                memb_eoa = file->memb_next[mt];
                assert(HADDR_UNDEF != memb_eoa);
            }
            else {
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "bad eoa", HADDR_UNDEF);
            }

            if (memb_eoa > eoa)
                eoa = memb_eoa;
        }
        END_MEMBERS
    }
    else {
        H5FD_mem_t mmt = file->fa.memb_map[type];

        if (H5FD_MEM_DEFAULT == mmt)
            mmt = type;

        if (file->memb[mmt]) {
            H5E_BEGIN_TRY
            {
                eoa = H5FDget_eoa(file->memb[mmt], mmt);
            }
            H5E_END_TRY

            if (HADDR_UNDEF == eoa)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "member file has unknown eoa",
                            HADDR_UNDEF);
            if (eoa > 0)
                eoa += file->fa.memb_addr[mmt];
        }
        else if (file->fa.relax) {
            /*
             * The member is not open yet (maybe it doesn't exist). Make the
             * best guess about the end-of-file.
             */
            eoa = file->memb_next[mmt];
            assert(HADDR_UNDEF != eoa);
        }
        else {
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "bad eoa", HADDR_UNDEF);
        }
    }

    return eoa;
} /* end H5FD_multi_get_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_set_eoa
 *
 * Purpose:    Set the end-of-address marker for the file by savig the new
 *        EOA value in the file struct. Also set the EOA marker for the
 *        subfile in which the new EOA value falls. We don't set the
 *        EOA values of any other subfiles.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t eoa)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    H5FD_mem_t         mmt;
    herr_t             status;
    static const char *func = "H5FD_multi_set_eoa"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    mmt = file->fa.memb_map[type];
    if (H5FD_MEM_DEFAULT == mmt) {
        if (H5FD_MEM_DEFAULT == type)
            mmt = H5FD_MEM_SUPER;
        else
            mmt = type;
    } /* end if */

    /* Handle backward compatibility in a quick and simple way.  v1.6 library
     * had EOA for the entire virtual file.  But it wasn't meaningful.  So v1.8
     * library doesn't have it anymore.  It saves the EOA for the metadata file,
     * instead.  Here we try to figure out whether the EOA is from a v1.6 file
     * by comparing its value.  If it is a big value, we assume it's from v1.6
     * and simply discard it. This is the normal case when the metadata file
     * has the smallest starting address.  If the metadata file has the biggest
     * address, the EOAs of v1.6 and v1.8 files are the same.  It won't cause
     * any trouble.  (Please see Issue 2598 in Jira) SLU - 2011/6/21
     */
    if (H5FD_MEM_SUPER == mmt && file->memb_eoa[H5FD_MEM_SUPER] > 0 &&
        eoa > (file->memb_next[H5FD_MEM_SUPER] / 2))
        return 0;

    assert(eoa >= file->fa.memb_addr[mmt]);
    assert(eoa < file->memb_next[mmt]);

    H5E_BEGIN_TRY
    {
        status = H5FDset_eoa(file->memb[mmt], mmt, (eoa - file->fa.memb_addr[mmt]));
    }
    H5E_END_TRY
    if (status < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_FILE, H5E_BADVALUE, "member H5FDset_eoa failed", -1);

    return 0;
} /* end H5FD_multi_set_eoa() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_get_eof
 *
 * Purpose:    Returns the end-of-file marker, which is the greater of
 *        either the total multi size or the current EOA marker.
 *
 * Return:    Success:    End of file address, the first address past
 *                the end of the multi of files or the current
 *                EOA, whichever is larger.
 *
 *            Failure:          HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_multi_get_eof(const H5FD_t *_file, H5FD_mem_t type)
{
    const H5FD_multi_t *file = (const H5FD_multi_t *)_file;
    haddr_t             eof  = 0;
    static const char  *func = "H5FD_multi_get_eof"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    if (H5FD_MEM_DEFAULT == type) {
        UNIQUE_MEMBERS (file->fa.memb_map, mt) {
            haddr_t tmp_eof;

            if (file->memb[mt]) {
                /* Retrieve EOF */
                H5E_BEGIN_TRY
                {
                    tmp_eof = H5FDget_eof(file->memb[mt], type);
                }
                H5E_END_TRY

                if (HADDR_UNDEF == tmp_eof)
                    H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "member file has unknown eof",
                                HADDR_UNDEF);
                if (tmp_eof > 0)
                    tmp_eof += file->fa.memb_addr[mt];
            }
            else if (file->fa.relax) {
                /*
                 * The member is not open yet (maybe it doesn't exist). Make the
                 * best guess about the end-of-file.
                 */
                tmp_eof = file->memb_next[mt];
                assert(HADDR_UNDEF != tmp_eof);
            }
            else {
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "bad eof", HADDR_UNDEF);
            }
            if (tmp_eof > eof)
                eof = tmp_eof;
        }
        END_MEMBERS
    }
    else {
        H5FD_mem_t mmt = file->fa.memb_map[type];

        if (H5FD_MEM_DEFAULT == mmt)
            mmt = type;

        if (file->memb[mmt]) {
            /* Retrieve EOF */
            H5E_BEGIN_TRY
            {
                eof = H5FDget_eof(file->memb[mmt], mmt);
            }
            H5E_END_TRY

            if (HADDR_UNDEF == eof)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "member file has unknown eof",
                            HADDR_UNDEF);
            if (eof > 0)
                eof += file->fa.memb_addr[mmt];
        }
        else if (file->fa.relax) {
            /*
             * The member is not open yet (maybe it doesn't exist). Make the
             * best guess about the end-of-file.
             */
            eof = file->memb_next[mmt];
            assert(HADDR_UNDEF != eof);
        }
        else {
            H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "bad eof", HADDR_UNDEF);
        }
    }
    return eof;
}

/*-------------------------------------------------------------------------
 * Function:       H5FD_multi_get_handle
 *
 * Purpose:        Returns the file handle of MULTI file driver.
 *
 * Returns:        Non-negative if succeed or negative if fails.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_get_handle(H5FD_t *_file, hid_t fapl, void **file_handle)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    H5FD_mem_t         type, mmt;
    static const char *func = "H5FD_multi_get_handle"; /* Function Name for error reporting */

    /* Get data type for multi driver */
    if (H5Pget_multi_type(fapl, &type) < 0)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "can't get data type for multi driver",
                    -1);
    if (type < H5FD_MEM_DEFAULT || type >= H5FD_MEM_NTYPES)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "data type is out of range", -1);
    mmt = file->fa.memb_map[type];
    if (H5FD_MEM_DEFAULT == mmt)
        mmt = type;

    return (H5FDget_vfd_handle(file->memb[mmt], fapl, file_handle));
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_alloc
 *
 * Purpose:    Allocate file memory.
 *
 * Return:    Success:    Address of new memory
 *
 *            Failure:    HADDR_UNDEF
 *
 *-------------------------------------------------------------------------
 */
static haddr_t
H5FD_multi_alloc(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, hsize_t size)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    H5FD_mem_t         mmt;
    haddr_t            addr;
    static const char *func = "H5FD_multi_alloc"; /* Function Name for error reporting */

    mmt = file->fa.memb_map[type];
    if (H5FD_MEM_DEFAULT == mmt)
        mmt = type;

    /* XXX: NEED to work on this again */
    if (file->pub.paged_aggr) {
        ALL_MEMBERS (mt) {
            if (file->memb[mt])
                file->memb[mt]->paged_aggr = file->pub.paged_aggr;
        }
        END_MEMBERS
    }

    if (HADDR_UNDEF == (addr = H5FDalloc(file->memb[mmt], mmt, dxpl_id, size)))
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "member file can't alloc", HADDR_UNDEF);
    addr += file->fa.memb_addr[mmt];

    /*#ifdef TMP
        if ( addr + size > file->eoa ) {

        if ( H5FD_multi_set_eoa(_file, addr + size) < 0 ) {

                H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, \
                "can't set eoa", HADDR_UNDEF);
        }
        }
    #else
        if ( addr + size > file->eoa )
        file->eoa = addr + size;
    #endif */

    return addr;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_free
 *
 * Purpose:    Frees memory
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_free(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, hsize_t size)
{
    H5FD_multi_t *file = (H5FD_multi_t *)_file;
    H5FD_mem_t    mmt;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    mmt = file->fa.memb_map[type];
    if (H5FD_MEM_DEFAULT == mmt)
        mmt = type;

    assert(addr >= file->fa.memb_addr[mmt]);
    assert(addr + size <= file->memb_next[mmt]);
    return H5FDfree(file->memb[mmt], mmt, dxpl_id, addr - file->fa.memb_addr[mmt], size);
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_read
 *
 * Purpose:    Reads SIZE bytes of data from FILE beginning at address ADDR
 *        into buffer BUF according to data transfer properties in
 *        DXPL_ID.
 *
 * Return:    Success:    Zero. Result is stored in caller-supplied
 *                buffer BUF.
 *
 *            Failure:    -1, contents of buffer BUF are undefined.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, void *_buf /*out*/)
{
    H5FD_multi_t *file = (H5FD_multi_t *)_file;
    H5FD_mem_t    mt, mmt, hi = H5FD_MEM_DEFAULT;
    haddr_t       start_addr = 0;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Find the file to which this address belongs */
    for (mt = H5FD_MEM_SUPER; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        mmt = file->fa.memb_map[mt];
        if (H5FD_MEM_DEFAULT == mmt)
            mmt = mt;
        assert(mmt > 0 && mmt < H5FD_MEM_NTYPES);

        if (file->fa.memb_addr[mmt] > addr)
            continue;
        if (file->fa.memb_addr[mmt] >= start_addr) {
            start_addr = file->fa.memb_addr[mmt];
            hi         = mmt;
        } /* end if */
    }     /* end for */
    assert(hi > 0);

    /* Read from that member */
    return H5FDread(file->memb[hi], type, dxpl_id, addr - start_addr, size, _buf);
} /* end H5FD_multi_read() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_write
 *
 * Purpose:    Writes SIZE bytes of data to FILE beginning at address ADDR
 *        from buffer BUF according to data transfer properties in
 *        DXPL_ID.
 *
 * Return:    Success:    Zero
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, const void *_buf)
{
    H5FD_multi_t *file = (H5FD_multi_t *)_file;
    H5FD_mem_t    mt, mmt, hi = H5FD_MEM_DEFAULT;
    haddr_t       start_addr = 0;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Find the file to which this address belongs */
    for (mt = H5FD_MEM_SUPER; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        mmt = file->fa.memb_map[mt];
        if (H5FD_MEM_DEFAULT == mmt)
            mmt = mt;
        assert(mmt > 0 && mmt < H5FD_MEM_NTYPES);

        if (file->fa.memb_addr[mmt] > addr)
            continue;
        if (file->fa.memb_addr[mmt] >= start_addr) {
            start_addr = file->fa.memb_addr[mmt];
            hi         = mmt;
        } /* end if */
    }     /* end for */
    assert(hi > 0);

    /* Write to that member */
    return H5FDwrite(file->memb[hi], type, dxpl_id, addr - start_addr, size, _buf);
} /* end H5FD_multi_write() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_flush
 *
 * Purpose:    Flushes all multi members.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1, as many files flushed as possible.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_flush(H5FD_t *_file, hid_t dxpl_id, bool closing)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    H5FD_mem_t         mt;
    int                nerrors = 0;
    static const char *func    = "H5FD_multi_flush"; /* Function Name for error reporting */

#if 0
    H5FD_mem_t        mmt;

    /* Debugging stuff... */
    fprintf(stderr, "multifile access information:\n");

    /* print the map */
    fprintf(stderr, "    map=");
    for (mt=1; mt<H5FD_MEM_NTYPES; mt++) {
    mmt = file->memb_map[mt];
    if (H5FD_MEM_DEFAULT==mmt) mmt = mt;
    fprintf(stderr, "%s%d", 1==mt?"":",", (int)mmt);
    }
    fprintf(stderr, "\n");

    /* print info about each file */
    fprintf(stderr, "      File             Starting            Allocated                 Next Member\n");
    fprintf(stderr, "    Number              Address                 Size              Address Name\n");
    fprintf(stderr, "    ------ -------------------- -------------------- -------------------- ------------------------------\n");

    for (mt=1; mt<H5FD_MEM_NTYPES; mt++) {
    if (HADDR_UNDEF!=file->memb_addr[mt]) {
        haddr_t eoa = H5FDget_eoa(file->memb[mt], mt);
        fprintf(stderr, "    %6d %20llu %20llu %20llu %s\n",
            (int)mt, (unsigned long long)(file->memb_addr[mt]),
            (unsigned long long)eoa,
            (unsigned long long)(file->memb_next[mt]),
            file->memb_name[mt]);
    }
    }
#endif

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Flush each file */
    for (mt = H5FD_MEM_SUPER; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        if (file->memb[mt]) {
            H5E_BEGIN_TRY
            {
                if (H5FDflush(file->memb[mt], dxpl_id, closing) < 0)
                    nerrors++;
            }
            H5E_END_TRY
        }
    }
    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "error flushing member files", -1);

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_truncate
 *
 * Purpose:    Truncates all multi members.
 *
 * Return:    Success:    0
 *            Failure:    -1, as many files truncated as possible.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_truncate(H5FD_t *_file, hid_t dxpl_id, bool closing)
{
    H5FD_multi_t      *file = (H5FD_multi_t *)_file;
    H5FD_mem_t         mt;
    int                nerrors = 0;
    static const char *func    = "H5FD_multi_truncate"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Truncate each file */
    for (mt = H5FD_MEM_SUPER; mt < H5FD_MEM_NTYPES; mt = (H5FD_mem_t)(mt + 1)) {
        if (file->memb[mt]) {
            H5E_BEGIN_TRY
            {
                if (H5FDtruncate(file->memb[mt], dxpl_id, closing) < 0)
                    nerrors++;
            }
            H5E_END_TRY
        }
    }
    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "error truncating member files", -1);

    return 0;
} /* end H5FD_multi_truncate() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_lock
 *
 * Purpose:    Place a lock on all multi members.
 *        When there is error in locking a member file, it will not
 *        proceed further and will try to remove the locks  of those
 *        member files that are locked before error is encountered.
 *
 * Return:    Success:    0
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_lock(H5FD_t *_file, bool rw)
{
    H5FD_multi_t      *file    = (H5FD_multi_t *)_file;
    int                nerrors = 0;
    H5FD_mem_t         out_mt  = H5FD_MEM_DEFAULT;
    static const char *func    = "H5FD_multi_unlock"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Lock all member files */
    ALL_MEMBERS (mt) {
        out_mt = mt;
        if (file->memb[mt]) {
            H5E_BEGIN_TRY
            {
                if (H5FDlock(file->memb[mt], rw) < 0) {
                    nerrors++;
                    break;
                } /* end if */
            }
            H5E_END_TRY
        } /* end if */
    }
    END_MEMBERS

    /* Try to unlock the member files that are locked before error is encountered */
    if (nerrors) {
        H5FD_mem_t k;

        for (k = H5FD_MEM_DEFAULT; k < out_mt; k = (H5FD_mem_t)(k + 1)) {
            H5E_BEGIN_TRY
            {
                if (H5FDunlock(file->memb[k]) < 0)
                    nerrors++;
            }
            H5E_END_TRY
        } /* end for */
    }     /* end if */

    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTLOCKFILE, "error locking member files", -1);
    return 0;

} /* H5FD_multi_lock() */

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_unlock
 *
 * Purpose:    Remove the lock on all multi members.
 *        It will try to unlock all member files but will record error
 *        encountered.
 *
 * Return:    Success:    0
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_unlock(H5FD_t *_file)
{
    H5FD_multi_t      *file    = (H5FD_multi_t *)_file;
    int                nerrors = 0;
    static const char *func    = "H5FD_multi_unlock"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    ALL_MEMBERS (mt) {
        if (file->memb[mt])
            if (H5FDunlock(file->memb[mt]) < 0)
                nerrors++;
    }
    END_MEMBERS

    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTUNLOCKFILE, "error unlocking member files", -1);

    return 0;
} /* H5FD_multi_unlock() */

/*-------------------------------------------------------------------------
 * Function:    compute_next
 *
 * Purpose:    Compute the memb_next[] values of the file based on the
 *        file's member map and the member starting addresses.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static int
compute_next(H5FD_multi_t *file)
{
    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    ALL_MEMBERS (mt) {
        file->memb_next[mt] = HADDR_UNDEF;
    }
    END_MEMBERS

    UNIQUE_MEMBERS (file->fa.memb_map, mt1) {
        UNIQUE_MEMBERS2(file->fa.memb_map, mt2)
        {
            if (file->fa.memb_addr[mt1] < file->fa.memb_addr[mt2] &&
                (HADDR_UNDEF == file->memb_next[mt1] || file->memb_next[mt1] > file->fa.memb_addr[mt2])) {
                file->memb_next[mt1] = file->fa.memb_addr[mt2];
            }
        }
        END_MEMBERS
        if (HADDR_UNDEF == file->memb_next[mt1]) {
            file->memb_next[mt1] = HADDR_MAX; /*last member*/
        }
    }
    END_MEMBERS

    return 0;
}

/*-------------------------------------------------------------------------
 * Function:    open_members
 *
 * Purpose:    Opens all members which are not opened yet.
 *
 * Return:    Success:    0
 *
 *            Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
/* Disable warning for "format not a string literal" here
 *
 *      This pragma only needs to surround the snprintf() call with
 *      tmp in the code below, but early (4.4.7, at least) gcc only
 *      allows diagnostic pragmas to be toggled outside of functions.
 */
H5_MULTI_GCC_CLANG_DIAG_OFF("format-nonliteral")
static int
open_members(H5FD_multi_t *file)
{
    char               tmp[H5FD_MULT_MAX_FILE_NAME_LEN];
    int                nerrors = 0;
    int                nchars;
    static const char *func = "(H5FD_multi)open_members"; /* Function Name for error reporting */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    UNIQUE_MEMBERS (file->fa.memb_map, mt) {
        if (file->memb[mt])
            continue; /*already open*/
        assert(file->fa.memb_name[mt]);

        nchars = snprintf(tmp, sizeof(tmp), file->fa.memb_name[mt], file->name);
        if (nchars < 0 || nchars >= (int)sizeof(tmp))
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_BADVALUE,
                        "filename is too long and would be truncated", -1);

        H5E_BEGIN_TRY
        {
            file->memb[mt] = H5FDopen(tmp, file->flags, file->fa.memb_fapl[mt], HADDR_UNDEF);
        }
        H5E_END_TRY
        if (!file->memb[mt]) {
            if (!file->fa.relax || (file->flags & H5F_ACC_RDWR))
                nerrors++;
        }
    }
    END_MEMBERS
    if (nerrors)
        H5Epush_ret(func, H5E_ERR_CLS, H5E_INTERNAL, H5E_BADVALUE, "error opening member files", -1);

    return 0;
}
H5_MULTI_GCC_CLANG_DIAG_ON("format-nonliteral")

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_delete
 *
 * Purpose:     Delete a file
 *
 * Return:      SUCCEED/FAIL
 *
 *-------------------------------------------------------------------------
 */
H5_MULTI_GCC_CLANG_DIAG_OFF("format-nonliteral")
static herr_t
H5FD_multi_delete(const char *filename, hid_t fapl_id)
{
    char                     full_filename[H5FD_MULT_MAX_FILE_NAME_LEN];
    int                      nchars;
    const H5FD_multi_fapl_t *fa;
    H5FD_multi_fapl_t        default_fa;
    static const char       *func = "H5FD_multi_delete"; /* Function Name for error reporting    */

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    assert(filename);

    /* Get the driver info */
    H5E_BEGIN_TRY
    {
        fa = (const H5FD_multi_fapl_t *)H5Pget_driver_info(fapl_id);
    }
    H5E_END_TRY
    if (!fa) {
        char *env = getenv(HDF5_DRIVER);

        if (env && !strcmp(env, "split")) {
            if (H5FD_split_populate_config(NULL, H5P_DEFAULT, NULL, H5P_DEFAULT, true, &default_fa) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTSET, "can't setup driver configuration", -1);
        }
        else {
            if (H5FD_multi_populate_config(NULL, NULL, NULL, NULL, true, &default_fa) < 0)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_CANTSET, "can't setup driver configuration", -1);
        }

        fa = &default_fa;
    }
    assert(fa);

    /* Delete each member file using the underlying fapl */
    UNIQUE_MEMBERS (fa->memb_map, mt) {
        assert(fa->memb_name[mt]);
        assert(fa->memb_fapl[mt] >= 0);

        nchars = snprintf(full_filename, sizeof(full_filename), fa->memb_name[mt], filename);
        if (nchars < 0 || nchars >= (int)sizeof(full_filename))
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_BADVALUE,
                        "filename is too long and would be truncated", -1);

        if (H5FDdelete(full_filename, fa->memb_fapl[mt]) < 0)
            H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_BADVALUE, "error deleting member files", -1);
    }
    END_MEMBERS

    return 0;
} /* end H5FD_multi_delete() */
H5_MULTI_GCC_CLANG_DIAG_ON("format-nonliteral")

/*-------------------------------------------------------------------------
 * Function:    H5FD_multi_ctl
 *
 * Purpose:     Multi VFD version of the ctl callback.
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
 *              At present, this VFD supports no op codes of its own.
 *
 * Return:      Non-negative on success/Negative on failure
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5FD_multi_ctl(H5FD_t *_file, uint64_t op_code, uint64_t flags, const void *input, void **output)
{
    H5FD_multi_t      *file      = (H5FD_multi_t *)_file;
    static const char *func      = "H5FD_multi_ctl"; /* Function Name for error reporting */
    herr_t             ret_value = 0;

    /* Silence compiler */
    (void)file;
    (void)input;
    (void)output;

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    switch (op_code) {
        /* Unknown op code */
        default:
            if (flags & H5FD_CTL_FAIL_IF_UNKNOWN_FLAG)
                H5Epush_ret(func, H5E_ERR_CLS, H5E_VFL, H5E_FCNTL,
                            "VFD ctl request failed (unknown op code and fail if unknown flag is set)", -1);

            break;
    }

    return ret_value;
} /* end H5FD_multi_ctl() */

#ifdef H5private_H
/*
 * This is not related to the functionality of the driver code.
 * It is added here to trigger warning if HDF5 private definitions are included
 * by mistake.  The code should use only HDF5 public API and definitions.
 */
#error "Do not use HDF5 private definitions"
#endif
