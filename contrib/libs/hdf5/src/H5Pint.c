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
 * Purpose:	Generic Property Functions
 */

/****************/
/* Module Setup */
/****************/

#include "H5Pmodule.h" /* This source code file is part of the H5P module */

/***********/
/* Headers */
/***********/
#include "H5private.h" /* Generic Functions			*/
#ifdef H5_HAVE_PARALLEL
#include "H5ACprivate.h" /* Metadata cache                       */
#endif                   /* H5_HAVE_PARALLEL */
#include "H5Eprivate.h"  /* Error handling		  	*/
#include "H5Fprivate.h"  /* File access				*/
#include "H5FLprivate.h" /* Free lists                           */
#include "H5Iprivate.h"  /* IDs			  		*/
#include "H5MMprivate.h" /* Memory management			*/
#include "H5Ppkg.h"      /* Property lists		  	*/

/****************/
/* Local Macros */
/****************/

/******************/
/* Local Typedefs */
/******************/

/* Typedef for checking for duplicate class names in parent class */
typedef struct {
    const H5P_genclass_t *parent;    /* Pointer to parent class */
    const char           *name;      /* Pointer to name to check */
    H5P_genclass_t       *new_class; /* Pointer to class during path traversal */
} H5P_check_class_t;

/* Typedef for property list iterator callback */
typedef struct {
    H5P_iterate_int_t     cb_func;      /* Iterator callback */
    void                 *udata;        /* Iterator callback pointer */
    const H5P_genplist_t *plist;        /* Property list pointer */
    H5SL_t               *seen;         /* Skip list to hold names of properties already seen */
    int                  *curr_idx_ptr; /* Pointer to current iteration index */
    int                   prev_idx;     /* Previous iteration index */
} H5P_iter_plist_ud_t;

/* Typedef for property list class iterator callback */
typedef struct {
    H5P_iterate_int_t cb_func;      /* Iterator callback */
    void             *udata;        /* Iterator callback pointer */
    int              *curr_idx_ptr; /* Pointer to current iteration index */
    int               prev_idx;     /* Previous iteration index */
} H5P_iter_pclass_ud_t;

/* Typedef for property list comparison callback */
typedef struct {
    const H5P_genplist_t *plist2;    /* Pointer to second property list */
    int                   cmp_value; /* Value from property comparison */
} H5P_plist_cmp_ud_t;

/* Typedef for property list set/poke callbacks */
typedef struct {
    const void *value; /* Pointer to value to set */
} H5P_prop_set_ud_t;

/* Typedef for property list get/peek callbacks */
typedef struct {
    void *value; /* Pointer for retrieved value */
} H5P_prop_get_ud_t;

/* Typedef for H5P__do_prop() callbacks */
typedef herr_t (*H5P_do_plist_op_t)(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop,
                                    void *udata);
typedef herr_t (*H5P_do_pclass_op_t)(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop,
                                     void *udata);

/********************/
/* Local Prototypes */
/********************/

/* Infrastructure routines */
static herr_t H5P__close_class_cb(void *space, void **request);
static herr_t H5P__close_list_cb(void *space, void **request);

/* General helper routines */
static H5P_genplist_t *H5P__create(H5P_genclass_t *pclass);
static H5P_genprop_t  *H5P__create_prop(const char *name, size_t size, H5P_prop_within_t type,
                                        const void *value, H5P_prp_create_func_t prp_create,
                                        H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get,
                                        H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
                                        H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy,
                                        H5P_prp_compare_func_t prp_cmp, H5P_prp_close_func_t prp_close);
static H5P_genprop_t  *H5P__dup_prop(H5P_genprop_t *oprop, H5P_prop_within_t type);
static herr_t          H5P__free_prop(H5P_genprop_t *prop);
static int             H5P__cmp_prop(const H5P_genprop_t *prop1, const H5P_genprop_t *prop2);
static herr_t          H5P__do_prop(H5P_genplist_t *plist, const char *name, H5P_do_plist_op_t plist_op,
                                    H5P_do_pclass_op_t pclass_op, void *udata);
static int             H5P__open_class_path_cb(void *_obj, hid_t H5_ATTR_UNUSED id, void *_key);
static H5P_genprop_t  *H5P__find_prop_pclass(H5P_genclass_t *pclass, const char *name);
static herr_t          H5P__free_prop_cb(void *item, void H5_ATTR_UNUSED *key, void *op_data);
static herr_t H5P__free_del_name_cb(void *item, void H5_ATTR_UNUSED *key, void H5_ATTR_UNUSED *op_data);

/*********************/
/* Package Variables */
/*********************/

/*
 * Predefined property list classes. These are initialized at runtime by
 * H5P_init() in this source file.
 */
hid_t           H5P_CLS_ROOT_ID_g = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_ROOT_g    = NULL;

hid_t           H5P_CLS_ATTRIBUTE_ACCESS_ID_g = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_ATTRIBUTE_ACCESS_g    = NULL;
hid_t           H5P_CLS_ATTRIBUTE_CREATE_ID_g = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_ATTRIBUTE_CREATE_g    = NULL;
hid_t           H5P_CLS_DATASET_ACCESS_ID_g   = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_DATASET_ACCESS_g      = NULL;
hid_t           H5P_CLS_DATASET_CREATE_ID_g   = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_DATASET_CREATE_g      = NULL;
hid_t           H5P_CLS_DATASET_XFER_ID_g     = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_DATASET_XFER_g        = NULL;
hid_t           H5P_CLS_DATATYPE_ACCESS_ID_g  = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_DATATYPE_ACCESS_g     = NULL;
hid_t           H5P_CLS_DATATYPE_CREATE_ID_g  = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_DATATYPE_CREATE_g     = NULL;
hid_t           H5P_CLS_FILE_ACCESS_ID_g      = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_FILE_ACCESS_g         = NULL;
hid_t           H5P_CLS_FILE_CREATE_ID_g      = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_FILE_CREATE_g         = NULL;
hid_t           H5P_CLS_FILE_MOUNT_ID_g       = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_FILE_MOUNT_g          = NULL;
hid_t           H5P_CLS_GROUP_ACCESS_ID_g     = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_GROUP_ACCESS_g        = NULL;
hid_t           H5P_CLS_GROUP_CREATE_ID_g     = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_GROUP_CREATE_g        = NULL;
hid_t           H5P_CLS_LINK_ACCESS_ID_g      = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_LINK_ACCESS_g         = NULL;
hid_t           H5P_CLS_LINK_CREATE_ID_g      = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_LINK_CREATE_g         = NULL;
hid_t           H5P_CLS_MAP_ACCESS_ID_g       = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_MAP_ACCESS_g          = NULL;
hid_t           H5P_CLS_MAP_CREATE_ID_g       = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_MAP_CREATE_g          = NULL;
hid_t           H5P_CLS_OBJECT_COPY_ID_g      = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_OBJECT_COPY_g         = NULL;
hid_t           H5P_CLS_OBJECT_CREATE_ID_g    = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_OBJECT_CREATE_g       = NULL;
hid_t           H5P_CLS_REFERENCE_ACCESS_ID_g = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_REFERENCE_ACCESS_g    = NULL;
hid_t           H5P_CLS_STRING_CREATE_ID_g    = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_STRING_CREATE_g       = NULL;
hid_t           H5P_CLS_VOL_INITIALIZE_ID_g   = H5I_INVALID_HID;
H5P_genclass_t *H5P_CLS_VOL_INITIALIZE_g      = NULL;

/*
 * Predefined property lists for each predefined class. These are initialized
 * at runtime by H5P_init() in this source file.
 */
hid_t H5P_LST_ATTRIBUTE_ACCESS_ID_g = H5I_INVALID_HID;
hid_t H5P_LST_ATTRIBUTE_CREATE_ID_g = H5I_INVALID_HID;
hid_t H5P_LST_DATASET_ACCESS_ID_g   = H5I_INVALID_HID;
hid_t H5P_LST_DATASET_CREATE_ID_g   = H5I_INVALID_HID;
hid_t H5P_LST_DATASET_XFER_ID_g     = H5I_INVALID_HID;
hid_t H5P_LST_DATATYPE_ACCESS_ID_g  = H5I_INVALID_HID;
hid_t H5P_LST_DATATYPE_CREATE_ID_g  = H5I_INVALID_HID;
hid_t H5P_LST_FILE_ACCESS_ID_g      = H5I_INVALID_HID;
hid_t H5P_LST_FILE_CREATE_ID_g      = H5I_INVALID_HID;
hid_t H5P_LST_FILE_MOUNT_ID_g       = H5I_INVALID_HID;
hid_t H5P_LST_GROUP_ACCESS_ID_g     = H5I_INVALID_HID;
hid_t H5P_LST_GROUP_CREATE_ID_g     = H5I_INVALID_HID;
hid_t H5P_LST_LINK_ACCESS_ID_g      = H5I_INVALID_HID;
hid_t H5P_LST_LINK_CREATE_ID_g      = H5I_INVALID_HID;
hid_t H5P_LST_MAP_ACCESS_ID_g       = H5I_INVALID_HID;
hid_t H5P_LST_MAP_CREATE_ID_g       = H5I_INVALID_HID;
hid_t H5P_LST_OBJECT_COPY_ID_g      = H5I_INVALID_HID;
hid_t H5P_LST_REFERENCE_ACCESS_ID_g = H5I_INVALID_HID;
hid_t H5P_LST_VOL_INITIALIZE_ID_g   = H5I_INVALID_HID;

/* Root property list class library initialization object */
const H5P_libclass_t H5P_CLS_ROOT[1] = {{
    "root",        /* Class name for debugging     */
    H5P_TYPE_ROOT, /* Class type                   */

    NULL,               /* Parent class                 */
    &H5P_CLS_ROOT_g,    /* Pointer to class             */
    &H5P_CLS_ROOT_ID_g, /* Pointer to class ID          */
    NULL,               /* Pointer to default property list ID */
    NULL,               /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* Attribute access property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_AACC[1] = {{
    "attribute access",        /* Class name for debugging     */
    H5P_TYPE_ATTRIBUTE_ACCESS, /* Class type                   */

    &H5P_CLS_LINK_ACCESS_g,         /* Parent class                 */
    &H5P_CLS_ATTRIBUTE_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_ATTRIBUTE_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_ATTRIBUTE_ACCESS_ID_g, /* Pointer to default property list ID */
    NULL,                           /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* Group access property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_GACC[1] = {{
    "group access",        /* Class name for debugging     */
    H5P_TYPE_GROUP_ACCESS, /* Class type                   */

    &H5P_CLS_LINK_ACCESS_g,     /* Parent class                 */
    &H5P_CLS_GROUP_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_GROUP_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_GROUP_ACCESS_ID_g, /* Pointer to default property list ID */
    NULL,                       /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* Datatype creation property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_TCRT[1] = {{
    "datatype create",        /* Class name for debugging     */
    H5P_TYPE_DATATYPE_CREATE, /* Class type                   */

    &H5P_CLS_OBJECT_CREATE_g,      /* Parent class                 */
    &H5P_CLS_DATATYPE_CREATE_g,    /* Pointer to class             */
    &H5P_CLS_DATATYPE_CREATE_ID_g, /* Pointer to class ID          */
    &H5P_LST_DATATYPE_CREATE_ID_g, /* Pointer to default property list ID */
    NULL,                          /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* Datatype access property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_TACC[1] = {{
    "datatype access",        /* Class name for debugging     */
    H5P_TYPE_DATATYPE_ACCESS, /* Class type                   */

    &H5P_CLS_LINK_ACCESS_g,        /* Parent class                 */
    &H5P_CLS_DATATYPE_ACCESS_g,    /* Pointer to class             */
    &H5P_CLS_DATATYPE_ACCESS_ID_g, /* Pointer to class ID          */
    &H5P_LST_DATATYPE_ACCESS_ID_g, /* Pointer to default property list ID */
    NULL,                          /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* VOL initialization property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_VINI[1] = {{
    "VOL initialization",    /* Class name for debugging     */
    H5P_TYPE_VOL_INITIALIZE, /* Class type                   */

    &H5P_CLS_ROOT_g,              /* Parent class                 */
    &H5P_CLS_VOL_INITIALIZE_g,    /* Pointer to class             */
    &H5P_CLS_VOL_INITIALIZE_ID_g, /* Pointer to class ID          */
    &H5P_LST_VOL_INITIALIZE_ID_g, /* Pointer to default property list ID */
    NULL,                         /* Default property registration routine */

    NULL, /* Class creation callback      */
    NULL, /* Class creation callback info */
    NULL, /* Class copy callback          */
    NULL, /* Class copy callback info     */
    NULL, /* Class close callback         */
    NULL  /* Class close callback info    */
}};

/* Reference access property list class library initialization object */
/* (move to proper source code file when used for real) */
const H5P_libclass_t H5P_CLS_RACC[1] = {{
    "reference access",        /* Class name for debugging     */
    H5P_TYPE_REFERENCE_ACCESS, /* Class type                   */

    &H5P_CLS_FILE_ACCESS_g,         /* Parent class                         */
    &H5P_CLS_REFERENCE_ACCESS_g,    /* Pointer to class                     */
    &H5P_CLS_REFERENCE_ACCESS_ID_g, /* Pointer to class ID                  */
    &H5P_LST_REFERENCE_ACCESS_ID_g, /* Pointer to default property list ID  */
    NULL,                           /* Default property registration routine*/

    NULL, /* Class creation callback              */
    NULL, /* Class creation callback info         */
    NULL, /* Class copy callback                  */
    NULL, /* Class copy callback info             */
    NULL, /* Class close callback                 */
    NULL  /* Class close callback info            */
}};

/* Library property list classes defined in other code modules */
/* (And not present in src/H5Pprivate.h) */
H5_DLLVAR const H5P_libclass_t H5P_CLS_OCRT[1];   /* Object creation */
H5_DLLVAR const H5P_libclass_t H5P_CLS_STRCRT[1]; /* String create */
H5_DLLVAR const H5P_libclass_t H5P_CLS_GCRT[1];   /* Group create */
H5_DLLVAR const H5P_libclass_t H5P_CLS_FCRT[1];   /* File creation */
H5_DLLVAR const H5P_libclass_t H5P_CLS_DCRT[1];   /* Dataset creation */
H5_DLLVAR const H5P_libclass_t H5P_CLS_MCRT[1];   /* Map creation */
H5_DLLVAR const H5P_libclass_t H5P_CLS_DXFR[1];   /* Data transfer */
H5_DLLVAR const H5P_libclass_t H5P_CLS_FMNT[1];   /* File mount */
H5_DLLVAR const H5P_libclass_t H5P_CLS_ACRT[1];   /* Attribute creation */

/*****************************/
/* Library Private Variables */
/*****************************/

/*******************/
/* Local Variables */
/*******************/

/* Track the revision count of a class, to make comparisons faster */
static unsigned H5P_next_rev = 0;
#define H5P_GET_NEXT_REV (H5P_next_rev++)

/* List of all property list classes in the library */
/* (order here is not important, they will be initialized in the proper
 *      order according to their parent class dependencies)
 */
static H5P_libclass_t const *const init_class[] = {
    H5P_CLS_ROOT,   /* Root */
    H5P_CLS_OCRT,   /* Object create */
    H5P_CLS_STRCRT, /* String create */
    H5P_CLS_LACC,   /* Link access */
    H5P_CLS_GCRT,   /* Group create */
    H5P_CLS_OCPY,   /* Object copy */
    H5P_CLS_GACC,   /* Group access */
    H5P_CLS_FCRT,   /* File creation */
    H5P_CLS_FACC,   /* File access */
    H5P_CLS_DCRT,   /* Dataset creation */
    H5P_CLS_DACC,   /* Dataset access */
    H5P_CLS_DXFR,   /* Data transfer */
    H5P_CLS_FMNT,   /* File mount */
    H5P_CLS_TCRT,   /* Datatype creation */
    H5P_CLS_TACC,   /* Datatype access */
    H5P_CLS_MCRT,   /* Map creation */
    H5P_CLS_MACC,   /* Map access */
    H5P_CLS_ACRT,   /* Attribute creation */
    H5P_CLS_AACC,   /* Attribute access */
    H5P_CLS_LCRT,   /* Link creation */
    H5P_CLS_VINI,   /* VOL initialization */
    H5P_CLS_RACC    /* Reference access */
};

/* Declare a free list to manage the H5P_genclass_t struct */
H5FL_DEFINE_STATIC(H5P_genclass_t);

/* Declare a free list to manage the H5P_genprop_t struct */
H5FL_DEFINE_STATIC(H5P_genprop_t);

/* Declare a free list to manage the H5P_genplist_t struct */
H5FL_DEFINE_STATIC(H5P_genplist_t);

/* Generic Property Class ID class */
static const H5I_class_t H5I_GENPROPCLS_CLS[1] = {{
    H5I_GENPROP_CLS,                /* ID class value */
    0,                              /* Class flags */
    0,                              /* # of reserved IDs for class */
    (H5I_free_t)H5P__close_class_cb /* Callback routine for closing objects of this class */
}};

/* Generic Property List ID class */
static const H5I_class_t H5I_GENPROPLST_CLS[1] = {{
    H5I_GENPROP_LST,               /* ID class value */
    0,                             /* Class flags */
    0,                             /* # of reserved IDs for class */
    (H5I_free_t)H5P__close_list_cb /* Callback routine for closing objects of this class */
}};

/*-------------------------------------------------------------------------
 * Function:    H5P_init_phase1
 *
 * Purpose:     Initialize the interface from some other layer. This should
 *              be followed with a call to H5P_init_phase2 after the H5P
 *              interface is completely setup.
 *
 * Return:      Success:    non-negative
 *              Failure:    negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_init_phase1(void)
{
    size_t tot_init = 0; /* Total # of classes initialized */
    size_t pass_init;    /* # of classes initialized in each pass */
    size_t u;
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    HDcompile_assert(H5P_TYPE_REFERENCE_ACCESS == (H5P_TYPE_MAX_TYPE - 1));

    /*
     * Initialize the Generic Property class & object groups.
     */
    if (H5I_register_type(H5I_GENPROPCLS_CLS) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTINIT, FAIL, "unable to initialize ID group");
    if (H5I_register_type(H5I_GENPROPLST_CLS) < 0)
        HGOTO_ERROR(H5E_ID, H5E_CANTINIT, FAIL, "unable to initialize ID group");

    /* Repeatedly pass over the list of property list classes for the library,
     * initializing each class if its parent class is initialized, until no
     * more progress is made.
     */
    tot_init = 0;
    do {
        /* Reset pass initialization counter */
        pass_init = 0;

        /* Make a pass over all the library's property list classes */
        for (u = 0; u < NELMTS(init_class); u++) {
            H5P_libclass_t const *lib_class = init_class[u]; /* Current class to operate on */

            /* Check if the current class hasn't been initialized and can be now */
            assert(lib_class->class_id);
            if (*lib_class->class_id == (-1) &&
                (lib_class->par_pclass == NULL || *lib_class->par_pclass != NULL)) {
                /* Sanity check - only the root class is not allowed to have a parent class */
                assert(lib_class->par_pclass || lib_class == H5P_CLS_ROOT);

                /* Allocate the new class */
                if (NULL == (*lib_class->pclass = H5P__create_class(
                                 lib_class->par_pclass ? *lib_class->par_pclass : NULL, lib_class->name,
                                 lib_class->type, lib_class->create_func, lib_class->create_data,
                                 lib_class->copy_func, lib_class->copy_data, lib_class->close_func,
                                 lib_class->close_data)))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "class initialization failed");

                /* Call routine to register properties for class */
                if (lib_class->reg_prop_func && (*lib_class->reg_prop_func)(*lib_class->pclass) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "can't register properties");

                /* Register the new class */
                if ((*lib_class->class_id = H5I_register(H5I_GENPROP_CLS, *lib_class->pclass, false)) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "can't register property list class");

                /* Only register the default property list if it hasn't been created yet */
                if (lib_class->def_plist_id && *lib_class->def_plist_id == (-1)) {
                    /* Register the default property list for the new class*/
                    if ((*lib_class->def_plist_id = H5P_create_id(*lib_class->pclass, false)) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL,
                                    "can't register default property list for class");
                } /* end if */

                /* Increment class initialization counters */
                pass_init++;
                tot_init++;
            } /* end if */
        }     /* end for */
    } while (pass_init > 0);

    /* Verify that all classes were initialized */
    assert(tot_init == NELMTS(init_class));

done:
    if (ret_value < 0 && tot_init > 0) {
        /* First uninitialize all default property lists */
        H5I_clear_type(H5I_GENPROP_LST, false, false);

        /* Then uninitialize any initialized libclass */
        for (u = 0; u < NELMTS(init_class); u++) {
            H5P_libclass_t const *lib_class = init_class[u]; /* Current class to operate on */

            assert(lib_class->class_id);
            if (*lib_class->class_id >= 0) {
                /* Close the class ID */
                if (H5I_dec_ref(*lib_class->class_id) < 0)
                    HDONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to close property list class ID");
            }
            else if (lib_class->pclass && *lib_class->pclass) {
                /* Close a half-initialized pclass */
                if (H5P__close_class(*lib_class->pclass) < 0)
                    HDONE_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to close property list class");
            }
        }
    }

    FUNC_LEAVE_NOAPI(ret_value)
}

/*-------------------------------------------------------------------------
 * Function:    H5P_init_phase2
 *
 * Purpose:     Finish initializing the interface from some other package.
 *
 * Note:        This is broken out as a separate routine so that the
 *              library's default VFL driver can be chosen and initialized
 *              after the entire H5P interface has been initialized.
 *
 * Return:      Success:    Non-negative
 *              Failure:    Negative
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5P_init_phase2(void)
{
    herr_t ret_value = SUCCEED;

    FUNC_ENTER_NOAPI(FAIL)

    /* Set up the default VFL driver */
    if (H5P__facc_set_def_driver() < 0)
        HGOTO_ERROR(H5E_VFL, H5E_CANTSET, FAIL, "unable to set default VFL driver");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P_init_phase2() */

/*--------------------------------------------------------------------------
 NAME
    H5P_term_package
 PURPOSE
    Terminate various H5P objects
 USAGE
    void H5P_term_package()
 RETURNS
    Non-negative on success/Negative on failure
 DESCRIPTION
    Release the ID group and any other resources allocated.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
     Can't report errors...
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5P_term_package(void)
{
    int n = 0;

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    int64_t nlist, nclass;

    /* Destroy HDF5 library property classes & lists */

    /* Check if there are any open property list classes or lists */
    nclass = H5I_nmembers(H5I_GENPROP_CLS);
    nlist  = H5I_nmembers(H5I_GENPROP_LST);

    /* If there are any open classes or groups, attempt to get rid of them. */
    if ((nclass + nlist) > 0) {
        /* Clear the lists */
        if (nlist > 0) {
            (void)H5I_clear_type(H5I_GENPROP_LST, false, false);

            /* Reset the default property lists, if they've been closed */
            if (H5I_nmembers(H5I_GENPROP_LST) == 0) {
                H5P_LST_ATTRIBUTE_ACCESS_ID_g = H5I_INVALID_HID;
                H5P_LST_ATTRIBUTE_CREATE_ID_g = H5I_INVALID_HID;
                H5P_LST_DATASET_ACCESS_ID_g   = H5I_INVALID_HID;
                H5P_LST_DATASET_CREATE_ID_g   = H5I_INVALID_HID;
                H5P_LST_DATASET_XFER_ID_g     = H5I_INVALID_HID;
                H5P_LST_DATATYPE_ACCESS_ID_g  = H5I_INVALID_HID;
                H5P_LST_DATATYPE_CREATE_ID_g  = H5I_INVALID_HID;
                H5P_LST_FILE_ACCESS_ID_g      = H5I_INVALID_HID;
                H5P_LST_FILE_CREATE_ID_g      = H5I_INVALID_HID;
                H5P_LST_FILE_MOUNT_ID_g       = H5I_INVALID_HID;
                H5P_LST_GROUP_ACCESS_ID_g     = H5I_INVALID_HID;
                H5P_LST_GROUP_CREATE_ID_g     = H5I_INVALID_HID;
                H5P_LST_LINK_ACCESS_ID_g      = H5I_INVALID_HID;
                H5P_LST_LINK_CREATE_ID_g      = H5I_INVALID_HID;
                H5P_LST_MAP_ACCESS_ID_g       = H5I_INVALID_HID;
                H5P_LST_MAP_CREATE_ID_g       = H5I_INVALID_HID;
                H5P_LST_OBJECT_COPY_ID_g      = H5I_INVALID_HID;
                H5P_LST_REFERENCE_ACCESS_ID_g = H5I_INVALID_HID;
                H5P_LST_VOL_INITIALIZE_ID_g   = H5I_INVALID_HID;
            }
        }

        /* Only attempt to close the classes after all the lists are closed */
        if (nlist == 0 && nclass > 0) {
            (void)H5I_clear_type(H5I_GENPROP_CLS, false, false);

            /* Reset the default property classes and IDs if they've been closed */
            if (H5I_nmembers(H5I_GENPROP_CLS) == 0) {
                H5P_CLS_ROOT_g = NULL;

                H5P_CLS_ATTRIBUTE_ACCESS_g = NULL;
                H5P_CLS_ATTRIBUTE_CREATE_g = NULL;
                H5P_CLS_DATASET_ACCESS_g   = NULL;
                H5P_CLS_DATASET_CREATE_g   = NULL;
                H5P_CLS_DATASET_XFER_g     = NULL;
                H5P_CLS_DATATYPE_ACCESS_g  = NULL;
                H5P_CLS_DATATYPE_CREATE_g  = NULL;
                H5P_CLS_FILE_ACCESS_g      = NULL;
                H5P_CLS_FILE_CREATE_g      = NULL;
                H5P_CLS_FILE_MOUNT_g       = NULL;
                H5P_CLS_GROUP_ACCESS_g     = NULL;
                H5P_CLS_GROUP_CREATE_g     = NULL;
                H5P_CLS_LINK_ACCESS_g      = NULL;
                H5P_CLS_LINK_CREATE_g      = NULL;
                H5P_CLS_MAP_ACCESS_g       = NULL;
                H5P_CLS_MAP_CREATE_g       = NULL;
                H5P_CLS_OBJECT_COPY_g      = NULL;
                H5P_CLS_OBJECT_CREATE_g    = NULL;
                H5P_CLS_REFERENCE_ACCESS_g = NULL;
                H5P_CLS_STRING_CREATE_g    = NULL;
                H5P_CLS_VOL_INITIALIZE_g   = NULL;

                H5P_CLS_ROOT_ID_g = H5I_INVALID_HID;

                H5P_CLS_ATTRIBUTE_ACCESS_ID_g = H5I_INVALID_HID;
                H5P_CLS_ATTRIBUTE_CREATE_ID_g = H5I_INVALID_HID;
                H5P_CLS_DATASET_ACCESS_ID_g   = H5I_INVALID_HID;
                H5P_CLS_DATASET_CREATE_ID_g   = H5I_INVALID_HID;
                H5P_CLS_DATASET_XFER_ID_g     = H5I_INVALID_HID;
                H5P_CLS_DATATYPE_ACCESS_ID_g  = H5I_INVALID_HID;
                H5P_CLS_DATATYPE_CREATE_ID_g  = H5I_INVALID_HID;
                H5P_CLS_FILE_ACCESS_ID_g      = H5I_INVALID_HID;
                H5P_CLS_FILE_CREATE_ID_g      = H5I_INVALID_HID;
                H5P_CLS_FILE_MOUNT_ID_g       = H5I_INVALID_HID;
                H5P_CLS_GROUP_ACCESS_ID_g     = H5I_INVALID_HID;
                H5P_CLS_GROUP_CREATE_ID_g     = H5I_INVALID_HID;
                H5P_CLS_LINK_ACCESS_ID_g      = H5I_INVALID_HID;
                H5P_CLS_LINK_CREATE_ID_g      = H5I_INVALID_HID;
                H5P_CLS_MAP_ACCESS_ID_g       = H5I_INVALID_HID;
                H5P_CLS_MAP_CREATE_ID_g       = H5I_INVALID_HID;
                H5P_CLS_OBJECT_COPY_ID_g      = H5I_INVALID_HID;
                H5P_CLS_OBJECT_CREATE_ID_g    = H5I_INVALID_HID;
                H5P_CLS_REFERENCE_ACCESS_ID_g = H5I_INVALID_HID;
                H5P_CLS_STRING_CREATE_ID_g    = H5I_INVALID_HID;
                H5P_CLS_VOL_INITIALIZE_ID_g   = H5I_INVALID_HID;
            }
        }

        n++; /*H5I*/
    }
    else {
        /* Destroy the property list and class id groups */
        n += (H5I_dec_type_ref(H5I_GENPROP_LST) > 0);
        n += (H5I_dec_type_ref(H5I_GENPROP_CLS) > 0);
    } /* end else */

    FUNC_LEAVE_NOAPI(n)
} /* end H5P_term_package() */

/*-------------------------------------------------------------------------
 * Function:    H5P__close_class_cb
 *
 * Purpose:     Called when the ref count reaches zero on a property class's ID
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__close_class_cb(void *_pclass, void H5_ATTR_UNUSED **request)
{
    H5P_genclass_t *pclass    = (H5P_genclass_t *)_pclass; /* Property list class to close */
    herr_t          ret_value = SUCCEED;                   /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pclass);

    /* Close the property list class object */
    if (H5P__close_class(pclass) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to close property list class");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__close_class_cb() */

/*-------------------------------------------------------------------------
 * Function:    H5P__close_list_cb
 *
 * Purpose:     Called when the ref count reaches zero on a property list's ID
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5P__close_list_cb(void *_plist, void H5_ATTR_UNUSED **request)
{
    H5P_genplist_t *plist     = (H5P_genplist_t *)_plist; /* Property list to close */
    herr_t          ret_value = SUCCEED;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);

    /* Close the property list object */
    if (H5P_close(plist) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CLOSEERROR, FAIL, "unable to close property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__close_list_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__do_prop_cb1
 PURPOSE
    Internal routine to call a property list callback routine and update
    the property list accordingly.
 USAGE
    herr_t H5P__do_prop_cb1(slist,prop,cb)
        H5SL_t *slist;          IN/OUT: Skip list to hold changed properties
        H5P_genprop_t *prop;    IN: Property to call callback for
        H5P_prp_cb1_t *cb;      IN: Callback routine to call
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Calls the callback routine passed in.  If the callback routine changes
    the property value, then the property is duplicated and added to skip list.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__do_prop_cb1(H5SL_t *slist, H5P_genprop_t *prop, H5P_prp_cb1_t cb)
{
    void          *tmp_value = NULL;    /* Temporary value buffer */
    H5P_genprop_t *pcopy     = NULL;    /* Copy of property to insert into skip list */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(slist);
    assert(prop);
    assert(prop->cmp);
    assert(cb);

    /* Allocate space for a temporary copy of the property value */
    if (NULL == (tmp_value = H5MM_malloc(prop->size)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed for temporary property value");
    H5MM_memcpy(tmp_value, prop->value, prop->size);

    /* Call "type 1" callback ('create', 'copy' or 'close') */
    if (cb(prop->name, prop->size, tmp_value) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "Property callback failed");

    /* Make a copy of the class's property */
    if (NULL == (pcopy = H5P__dup_prop(prop, H5P_PROP_WITHIN_LIST)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");

    /* Copy the changed value into the new property */
    H5MM_memcpy(pcopy->value, tmp_value, prop->size);

    /* Insert the changed property into the property list */
    if (H5P__add_prop(slist, pcopy) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into skip list");

done:
    /* Release the temporary value buffer */
    if (tmp_value)
        H5MM_xfree(tmp_value);

    /* Cleanup on failure */
    if (ret_value < 0)
        if (pcopy)
            H5P__free_prop(pcopy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__do_prop_cb1() */

/*--------------------------------------------------------------------------
 NAME
    H5P__copy_pclass
 PURPOSE
    Internal routine to copy a generic property class
 USAGE
    hid_t H5P__copy_pclass(pclass)
        H5P_genclass_t *pclass;      IN: Property class to copy
 RETURNS
    Success: valid property class ID on success (non-negative)
    Failure: negative
 DESCRIPTION
    Copy a property class and return the ID.  This routine does not make
    any callbacks.  (They are only make when operating on property lists).

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genclass_t *
H5P__copy_pclass(H5P_genclass_t *pclass)
{
    H5P_genclass_t *new_pclass = NULL; /* Property list class copied */
    H5P_genprop_t  *pcopy;             /* Copy of property to insert into class */
    H5P_genclass_t *ret_value = NULL;  /* return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);

    /*
     * Create new property class object
     */

    /* Create the new property list class */
    if (NULL == (new_pclass = H5P__create_class(pclass->parent, pclass->name, pclass->type,
                                                pclass->create_func, pclass->create_data, pclass->copy_func,
                                                pclass->copy_data, pclass->close_func, pclass->close_data)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, NULL, "unable to create property list class");

    /* Copy the properties registered for this class */
    if (pclass->nprops > 0) {
        H5SL_node_t *curr_node; /* Current node in skip list */

        /* Walk through the properties in the old class */
        curr_node = H5SL_first(pclass->props);
        while (curr_node != NULL) {
            /* Make a copy of the class's property */
            if (NULL == (pcopy = H5P__dup_prop((H5P_genprop_t *)H5SL_item(curr_node), H5P_PROP_WITHIN_CLASS)))
                HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, NULL, "Can't copy property");

            /* Insert the initialized property into the property list */
            if (H5P__add_prop(new_pclass->props, pcopy) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, NULL, "Can't insert property into class");

            /* Increment property count for class */
            new_pclass->nprops++;

            /* Get the next property node in the list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end if */

    /* Set the return value */
    ret_value = new_pclass;

done:
    if (NULL == ret_value && new_pclass)
        H5P__close_class(new_pclass);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__copy_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P_copy_plist
 PURPOSE
    Internal routine to copy a generic property list
 USAGE
        hid_t H5P_copy_plist(old_plist_id)
            hid_t old_plist_id;             IN: Property list ID to copy
 RETURNS
    Success: valid property list ID on success (non-negative)
    Failure: H5I_INVALID_HID
 DESCRIPTION
    Copy a property list and return the ID.  This routine calls the
    class 'copy' callback after any property 'copy' callbacks are called
    (assuming all property 'copy' callbacks return successfully).

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5P_copy_plist(const H5P_genplist_t *old_plist, bool app_ref)
{
    H5P_genclass_t *tclass;           /* Temporary class pointer */
    H5P_genplist_t *new_plist = NULL; /* New property list generated from copy */
    H5P_genprop_t  *tmp;              /* Temporary pointer to properties */
    H5P_genprop_t  *new_prop;         /* New property created for copy */
    hid_t           new_plist_id;     /* Property list ID of new list created */
    H5SL_node_t    *curr_node;        /* Current node in skip list */
    H5SL_t         *seen = NULL;      /* Skip list containing properties already seen */
    size_t          nseen;            /* Number of items 'seen' */
    bool            has_parent_class; /* Flag to indicate that this property list's class has a parent */
    hid_t           ret_value = H5I_INVALID_HID; /* return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    assert(old_plist);

    /*
     * Create new property list object
     */

    /* Allocate room for the property list */
    if (NULL == (new_plist = H5FL_CALLOC(H5P_genplist_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5I_INVALID_HID, "memory allocation failed");

    /* Set class state */
    new_plist->pclass     = old_plist->pclass;
    new_plist->nprops     = 0;     /* Initially the plist has the same number of properties as the class */
    new_plist->class_init = false; /* Initially, wait until the class callback finishes to set */

    /* Initialize the skip list to hold the changed properties */
    if ((new_plist->props = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, H5I_INVALID_HID,
                    "can't create skip list for changed properties");

    /* Create the skip list for deleted properties */
    if ((new_plist->del = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, H5I_INVALID_HID,
                    "can't create skip list for deleted properties");

    /* Create the skip list to hold names of properties already seen
     * (This prevents a property in the class hierarchy from having it's
     * 'create' callback called, if a property in the class hierarchy has
     * already been seen)
     */
    if ((seen = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, H5I_INVALID_HID, "can't create skip list for seen properties");
    nseen = 0;

    /* Cycle through the deleted properties & copy them into the new list's deleted section */
    if (H5SL_count(old_plist->del) > 0) {
        curr_node = H5SL_first(old_plist->del);
        while (curr_node) {
            char *new_name; /* Pointer to new name */

            /* Duplicate string for insertion into new deleted property skip list */
            if ((new_name = H5MM_xstrdup((char *)H5SL_item(curr_node))) == NULL)
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, H5I_INVALID_HID, "memory allocation failed");

            /* Insert property name into deleted list */
            if (H5SL_insert(new_plist->del, new_name, new_name) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5I_INVALID_HID,
                            "can't insert property into deleted skip list");

            /* Add property name to "seen" list */
            if (H5SL_insert(seen, new_name, new_name) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5I_INVALID_HID,
                            "can't insert property into seen skip list");
            nseen++;

            /* Get the next property node in the skip list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end if */

    /* Cycle through the properties and copy them also */
    if (H5SL_count(old_plist->props) > 0) {
        curr_node = H5SL_first(old_plist->props);
        while (curr_node) {
            /* Get a pointer to the node's property */
            tmp = (H5P_genprop_t *)H5SL_item(curr_node);

            /* Make a copy of the list's property */
            if (NULL == (new_prop = H5P__dup_prop(tmp, H5P_PROP_WITHIN_LIST)))
                HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, H5I_INVALID_HID, "Can't copy property");

            /* Call property copy callback, if it exists */
            if (new_prop->copy) {
                if ((new_prop->copy)(new_prop->name, new_prop->size, new_prop->value) < 0) {
                    H5P__free_prop(new_prop);
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, H5I_INVALID_HID, "Can't copy property");
                } /* end if */
            }     /* end if */

            /* Insert the initialized property into the property list */
            if (H5P__add_prop(new_plist->props, new_prop) < 0) {
                H5P__free_prop(new_prop);
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5I_INVALID_HID, "Can't insert property into list");
            } /* end if */

            /* Add property name to "seen" list */
            if (H5SL_insert(seen, new_prop->name, new_prop->name) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5I_INVALID_HID,
                            "can't insert property into seen skip list");
            nseen++;

            /* Increment the number of properties in list */
            new_plist->nprops++;

            /* Get the next property node in the skip list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end if */

    /*
     * Check for copying class properties (up through list of parent classes also),
     * initialize each with default value & make property 'copy' callback.
     */
    tclass           = old_plist->pclass;
    has_parent_class = (bool)(tclass != NULL && tclass->parent != NULL && tclass->parent->nprops > 0);
    while (tclass != NULL) {
        if (tclass->nprops > 0) {
            /* Walk through the properties in the old class */
            curr_node = H5SL_first(tclass->props);
            while (curr_node != NULL) {
                /* Get pointer to property from node */
                tmp = (H5P_genprop_t *)H5SL_item(curr_node);

                /* Only "copy" properties we haven't seen before */
                if (nseen == 0 || H5SL_search(seen, tmp->name) == NULL) {
                    /* Call property copy callback, if it exists */
                    if (tmp->copy) {
                        /* Call the callback & insert changed value into skip list (if necessary) */
                        if (H5P__do_prop_cb1(new_plist->props, tmp, tmp->copy) < 0)
                            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, H5I_INVALID_HID, "Can't create property");
                    } /* end if */

                    /* Add property name to "seen" list, if we have other classes to work on */
                    if (has_parent_class) {
                        if (H5SL_insert(seen, tmp->name, tmp->name) < 0)
                            HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5I_INVALID_HID,
                                        "can't insert property into seen skip list");
                        nseen++;
                    } /* end if */

                    /* Increment the number of properties in list */
                    new_plist->nprops++;
                } /* end if */

                /* Get the next property node in the skip list */
                curr_node = H5SL_next(curr_node);
            } /* end while */
        }     /* end if */

        /* Go up to parent class */
        tclass = tclass->parent;
    } /* end while */

    /* Increment the number of property lists derived from class */
    if (H5P__access_class(new_plist->pclass, H5P_MOD_INC_LST) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, H5I_INVALID_HID, "Can't increment class ref count");

    /* Get an ID for the property list */
    if ((new_plist_id = H5I_register(H5I_GENPROP_LST, new_plist, app_ref)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register property list");

    /* Save the property list ID in the property list struct, for use in the property class's 'close' callback
     */
    new_plist->plist_id = new_plist_id;

    /* Call the class callback (if it exists) now that we have the property list ID
     * (up through chain of parent classes also)
     */
    tclass = new_plist->pclass;
    while (NULL != tclass) {
        if (NULL != tclass->copy_func) {
            if ((tclass->copy_func)(new_plist_id, old_plist->plist_id, old_plist->pclass->copy_data) < 0) {
                /* Delete ID, ignore return value */
                H5I_remove(new_plist_id);
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, H5I_INVALID_HID, "Can't initialize property");
            } /* end if */
        }     /* end if */

        /* Go up to parent class */
        tclass = tclass->parent;
    } /* end while */

    /* Set the class initialization flag */
    new_plist->class_init = true;

    /* Set the return value */
    ret_value = new_plist_id;

done:
    /* Release the list of 'seen' properties */
    if (seen != NULL)
        H5SL_close(seen);

    if (H5I_INVALID_HID == ret_value && new_plist)
        H5P_close(new_plist);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_copy_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__dup_prop
 PURPOSE
    Internal routine to duplicate a property
 USAGE
    H5P_genprop_t *H5P__dup_prop(oprop)
        H5P_genprop_t *oprop;   IN: Pointer to property to copy
        H5P_prop_within_t type; IN: Type of object the property will be inserted into
 RETURNS
    Returns a pointer to the newly created duplicate of a property on success,
        NULL on failure.
 DESCRIPTION
    Allocates memory and copies property information into a new property object.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5P_genprop_t *
H5P__dup_prop(H5P_genprop_t *oprop, H5P_prop_within_t type)
{
    H5P_genprop_t *prop      = NULL; /* Pointer to new property copied */
    H5P_genprop_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(oprop);
    assert(type != H5P_PROP_WITHIN_UNKNOWN);

    /* Allocate the new property */
    if (NULL == (prop = H5FL_MALLOC(H5P_genprop_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Copy basic property information */
    H5MM_memcpy(prop, oprop, sizeof(H5P_genprop_t));

    /* Check if we should duplicate the name or share it */

    /* Duplicating property for a class */
    if (type == H5P_PROP_WITHIN_CLASS) {
        assert(oprop->type == H5P_PROP_WITHIN_CLASS);
        assert(oprop->shared_name == false);

        /* Duplicate name */
        prop->name = H5MM_xstrdup(oprop->name);
    } /* end if */
    /* Duplicating property for a list */
    else {
        /* Check if we are duplicating a property from a list or a class */

        /* Duplicating a property from a list */
        if (oprop->type == H5P_PROP_WITHIN_LIST) {
            /* If the old property's name wasn't shared, we have to copy it here also */
            if (!oprop->shared_name)
                prop->name = H5MM_xstrdup(oprop->name);
        } /* end if */
        /* Duplicating a property from a class */
        else {
            assert(oprop->type == H5P_PROP_WITHIN_CLASS);
            assert(oprop->shared_name == false);

            /* Share the name */
            prop->shared_name = true;

            /* Set the type */
            prop->type = type;
        } /* end else */
    }     /* end else */

    /* Duplicate current value, if it exists */
    if (oprop->value != NULL) {
        assert(prop->size > 0);
        if (NULL == (prop->value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
        H5MM_memcpy(prop->value, oprop->value, prop->size);
    } /* end if */

    /* Set return value */
    ret_value = prop;

done:
    /* Free any resources allocated */
    if (ret_value == NULL) {
        if (prop != NULL) {
            if (prop->name != NULL)
                H5MM_xfree(prop->name);
            if (prop->value != NULL)
                H5MM_xfree(prop->value);
            prop = H5FL_FREE(H5P_genprop_t, prop);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__dup_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__create_prop
 PURPOSE
    Internal routine to create a new property
 USAGE
    H5P_genprop_t *H5P__create_prop(name,size,type,value,prp_create,prp_set,
                                   prp_get,prp_delete,prp_close, prp_encode, prp_decode)
        const char *name;       IN: Name of property to register
        size_t size;            IN: Size of property in bytes
        H5P_prop_within_t type; IN: Type of object the property will be inserted into
        void *value;            IN: Pointer to buffer containing value for property
        H5P_prp_create_func_t prp_create;   IN: Function pointer to property
                                    creation callback
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_encode_func_t prp_encode; IN: Function pointer to property encode
        H5P_prp_decode_func_t prp_decode; IN: Function pointer to property decode
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_compare_func_t prp_cmp; IN: Function pointer to property compare callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns a pointer to the newly created property on success,
        NULL on failure.
 DESCRIPTION
    Allocates memory and copies property information into a new property object.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5P_genprop_t *
H5P__create_prop(const char *name, size_t size, H5P_prop_within_t type, const void *value,
                 H5P_prp_create_func_t prp_create, H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get,
                 H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
                 H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy,
                 H5P_prp_compare_func_t prp_cmp, H5P_prp_close_func_t prp_close)
{
    H5P_genprop_t *prop      = NULL; /* Pointer to new property copied */
    H5P_genprop_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(name);
    assert((size > 0 && value != NULL) || (size == 0));
    assert(type != H5P_PROP_WITHIN_UNKNOWN);

    /* Allocate the new property */
    if (NULL == (prop = H5FL_MALLOC(H5P_genprop_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set the property initial values */
    prop->name        = H5MM_xstrdup(name); /* Duplicate name */
    prop->shared_name = false;
    prop->size        = size;
    prop->type        = type;

    /* Duplicate value, if it exists */
    if (value != NULL) {
        if (NULL == (prop->value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");
        H5MM_memcpy(prop->value, value, prop->size);
    } /* end if */
    else
        prop->value = NULL;

    /* Set the function pointers */
    prop->create = prp_create;
    prop->set    = prp_set;
    prop->get    = prp_get;
    prop->encode = prp_encode;
    prop->decode = prp_decode;
    prop->del    = prp_delete;
    prop->copy   = prp_copy;
    /* Use custom comparison routine if available, otherwise default to memcmp() */
    if (prp_cmp != NULL)
        prop->cmp = prp_cmp;
    else
        prop->cmp = &memcmp;
    prop->close = prp_close;

    /* Set return value */
    ret_value = prop;

done:
    /* Free any resources allocated */
    if (ret_value == NULL) {
        if (prop != NULL) {
            if (prop->name != NULL)
                H5MM_xfree(prop->name);
            if (prop->value != NULL)
                H5MM_xfree(prop->value);
            prop = H5FL_FREE(H5P_genprop_t, prop);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__create_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__add_prop
 PURPOSE
    Internal routine to insert a property into a property skip list
 USAGE
    herr_t H5P__add_prop(slist, prop)
        H5SL_t *slist;          IN/OUT: Pointer to skip list of properties
        H5P_genprop_t *prop;    IN: Pointer to property to insert
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Inserts a property into a skip list of properties.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__add_prop(H5SL_t *slist, H5P_genprop_t *prop)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(slist);
    assert(prop);
    assert(prop->type != H5P_PROP_WITHIN_UNKNOWN);

    /* Insert property into skip list */
    if (H5SL_insert(slist, prop, prop->name) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__add_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__find_prop_plist
 PURPOSE
    Internal routine to check for a property in a property list's skip list
 USAGE
    H5P_genprop_t *H5P_find_prop(plist, name)
        const H5P_genplist_t *plist;  IN: Pointer to property list to check
        const char *name;       IN: Name of property to check for
 RETURNS
    Returns pointer to property on success, NULL on failure.
 DESCRIPTION
    Checks for a property in a property list's skip list of properties.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genprop_t *
H5P__find_prop_plist(const H5P_genplist_t *plist, const char *name)
{
    H5P_genprop_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(plist);
    assert(name);

    /* Check if the property has been deleted from list */
    if (H5SL_search(plist->del, name) != NULL) {
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "property deleted from skip list");
    } /* end if */
    else {
        /* Get the property data from the skip list */
        if (NULL == (ret_value = (H5P_genprop_t *)H5SL_search(plist->props, name))) {
            H5P_genclass_t *tclass; /* Temporary class pointer */

            /* Couldn't find property in list itself, start searching through class info */
            tclass = plist->pclass;
            while (tclass != NULL) {
                /* Find the property in the class */
                if (NULL != (ret_value = (H5P_genprop_t *)H5SL_search(tclass->props, name)))
                    /* Got pointer to property - leave now */
                    break;

                /* Go up to parent class */
                tclass = tclass->parent;
            } /* end while */

            /* Check if we haven't found the property */
            if (ret_value == NULL)
                HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "can't find property in skip list");
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__find_prop_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__find_prop_pclass
 PURPOSE
    Internal routine to check for a property in a class skip list
 USAGE
    H5P_genprop_t *H5P__find_prop_class(pclass, name)
        H5P_genclass *pclass;   IN: Pointer generic property class to check
        const char *name;       IN: Name of property to check for
 RETURNS
    Returns pointer to property on success, NULL on failure.
 DESCRIPTION
    Checks for a property in a class's skip list of properties.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5P_genprop_t *
H5P__find_prop_pclass(H5P_genclass_t *pclass, const char *name)
{
    H5P_genprop_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);
    assert(name);

    /* Get the property from the skip list */
    if (NULL == (ret_value = (H5P_genprop_t *)H5SL_search(pclass->props, name)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "can't find property in skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__find_prop_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__free_prop
 PURPOSE
    Internal routine to destroy a property node
 USAGE
    herr_t H5P__free_prop(prop)
        H5P_genprop_t *prop;    IN: Pointer to property to destroy
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Releases all the memory for a property list.  Does _not_ call the
    properties 'close' callback, that should already have been done.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__free_prop(H5P_genprop_t *prop)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(prop);

    /* Release the property value if it exists */
    if (prop->value)
        H5MM_xfree(prop->value);

    /* Only free the name if we own it */
    if (!prop->shared_name)
        H5MM_xfree(prop->name);

    prop = H5FL_FREE(H5P_genprop_t, prop);

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5P__free_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__free_prop_cb
 PURPOSE
    Internal routine to properties from a property skip list
 USAGE
    herr_t H5P__free_prop_cb(item, key, op_data)
        void *item;             IN/OUT: Pointer to property
        void *key;              IN/OUT: Pointer to property key
        void *_make_cb;         IN: Whether to make property callbacks or not
 RETURNS
    Returns zero on success, negative on failure.
 DESCRIPTION
        Calls the property 'close' callback for a property & frees property
    info.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__free_prop_cb(void *item, void H5_ATTR_UNUSED *key, void *op_data)
{
    H5P_genprop_t *tprop   = (H5P_genprop_t *)item; /* Temporary pointer to property */
    bool           make_cb = *(bool *)op_data;      /* Whether to make property 'close' callback */

    FUNC_ENTER_PACKAGE_NOERR

    assert(tprop);

    /* Call the close callback and ignore the return value, there's nothing we can do about it */
    if (make_cb && tprop->close != NULL)
        (tprop->close)(tprop->name, tprop->size, tprop->value);

    /* Free the property, ignoring return value, nothing we can do */
    H5P__free_prop(tprop);

    FUNC_LEAVE_NOAPI(0)
} /* H5P__free_prop_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__free_del_name_cb
 PURPOSE
    Internal routine to free 'deleted' property name
 USAGE
    herr_t H5P__free_del_name_cb(item, key, op_data)
        void *item;             IN/OUT: Pointer to deleted name
        void *key;              IN/OUT: Pointer to key
        void *op_data;          IN: Operator callback data (unused)
 RETURNS
    Returns zero on success, negative on failure.
 DESCRIPTION
    Frees the deleted property name
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__free_del_name_cb(void *item, void H5_ATTR_UNUSED *key, void H5_ATTR_UNUSED *op_data)
{
    char *del_name = (char *)item; /* Temporary pointer to deleted name */

    FUNC_ENTER_PACKAGE_NOERR

    assert(del_name);

    /* Free the name */
    H5MM_xfree(del_name);

    FUNC_LEAVE_NOAPI(0)
} /* H5P__free_del_name_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__access_class
 PURPOSE
    Internal routine to increment or decrement list & class dependencies on a
        property list class
 USAGE
    herr_t H5P__access_class(pclass,mod)
        H5P_genclass_t *pclass;     IN: Pointer to class to modify
        H5P_class_mod_t mod;        IN: Type of modification to class
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Increment/Decrement the class or list dependencies for a given class.
    This routine is the final arbiter on decisions about actually releasing a
    class in memory, such action is only taken when the reference counts for
    both dependent classes & lists reach zero.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__access_class(H5P_genclass_t *pclass, H5P_class_mod_t mod)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(pclass);
    assert(mod > H5P_MOD_ERR && mod < H5P_MOD_MAX);

    switch (mod) {
        case H5P_MOD_INC_CLS: /* Increment the dependent class count*/
            pclass->classes++;
            break;

        case H5P_MOD_DEC_CLS: /* Decrement the dependent class count*/
            pclass->classes--;
            break;

        case H5P_MOD_INC_LST: /* Increment the dependent list count*/
            pclass->plists++;
            break;

        case H5P_MOD_DEC_LST: /* Decrement the dependent list count*/
            pclass->plists--;
            break;

        case H5P_MOD_INC_REF: /* Increment the ID reference count*/
            /* Reset the deleted flag if incrementing the reference count */
            if (pclass->deleted)
                pclass->deleted = false;
            pclass->ref_count++;
            break;

        case H5P_MOD_DEC_REF: /* Decrement the ID reference count*/
            pclass->ref_count--;

            /* Mark the class object as deleted if reference count drops to zero */
            if (pclass->ref_count == 0)
                pclass->deleted = true;
            break;

        case H5P_MOD_ERR:
        case H5P_MOD_MAX:
        default:
            assert(0 && "Invalid H5P class modification");
    } /* end switch */

    /* Check if we can release the class information now */
    if (pclass->deleted && pclass->plists == 0 && pclass->classes == 0) {
        H5P_genclass_t *par_class = pclass->parent; /* Pointer to class's parent */

        assert(pclass->name);
        H5MM_xfree(pclass->name);

        /* Free the class properties without making callbacks */
        if (pclass->props) {
            bool make_cb = false;

            H5SL_destroy(pclass->props, H5P__free_prop_cb, &make_cb);
        } /* end if */

        pclass = H5FL_FREE(H5P_genclass_t, pclass);

        /* Reduce the number of dependent classes on parent class also */
        if (par_class != NULL)
            H5P__access_class(par_class, H5P_MOD_DEC_CLS);
    } /* end if */

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5P__access_class() */

/*--------------------------------------------------------------------------
 NAME
    H5P__open_class_path_cb
 PURPOSE
    Internal callback routine to check for duplicated names in parent class.
 USAGE
    int H5P__open_class_path_cb(obj, id, key)
        H5P_genclass_t *obj;    IN: Pointer to class
        hid_t id;               IN: ID of object being looked at
        const void *key;        IN: Pointer to information used to compare
                                    classes.
 RETURNS
    Returns >0 on match, 0 on no match and <0 on failure.
 DESCRIPTION
    Checks whether a property list class has the same parent and name as a
    new class being created.  This is a callback routine for H5I_search()
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__open_class_path_cb(void *_obj, hid_t H5_ATTR_UNUSED id, void *_key)
{
    H5P_genclass_t    *obj       = (H5P_genclass_t *)_obj;    /* Pointer to the class for this ID */
    H5P_check_class_t *key       = (H5P_check_class_t *)_key; /* Pointer to key information for comparison */
    int                ret_value = 0;                         /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(obj);
    assert(H5I_GENPROP_CLS == H5I_get_type(id));
    assert(key);

    /* Check if the class object has the same parent as the new class */
    if (obj->parent == key->parent) {
        /* Check if they have the same name */
        if (strcmp(obj->name, key->name) == 0) {
            key->new_class = obj;
            ret_value      = 1; /* Indicate a match */
        }                       /* end if */
    }                           /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__open_class_path_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__create_class
 PURPOSE
    Internal routine to create a new property list class.
 USAGE
    H5P_genclass_t H5P__create_class(par_class, name, type,
                cls_create, create_data, cls_close, close_data)
        H5P_genclass_t *par_class;  IN: Pointer to parent class
        const char *name;       IN: Name of class we are creating
        H5P_plist_type_t type;  IN: Type of class we are creating
        H5P_cls_create_func_t;  IN: The callback function to call when each
                                    property list in this class is created.
        void *create_data;      IN: Pointer to user data to pass along to class
                                    creation callback.
        H5P_cls_copy_func_t;    IN: The callback function to call when each
                                    property list in this class is copied.
        void *copy_data;        IN: Pointer to user data to pass along to class
                                    copy callback.
        H5P_cls_close_func_t;   IN: The callback function to call when each
                                    property list in this class is closed.
        void *close_data;       IN: Pointer to user data to pass along to class
                                    close callback.
 RETURNS
    Returns a pointer to the newly created property list class on success,
        NULL on failure.
 DESCRIPTION
    Allocates memory and attaches a class to the property list class hierarchy.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genclass_t *
H5P__create_class(H5P_genclass_t *par_class, const char *name, H5P_plist_type_t type,
                  H5P_cls_create_func_t cls_create, void *create_data, H5P_cls_copy_func_t cls_copy,
                  void *copy_data, H5P_cls_close_func_t cls_close, void *close_data)
{
    H5P_genclass_t *pclass    = NULL; /* Property list class created */
    H5P_genclass_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(name);
    /* Allow internal classes to break some rules */
    /* (This allows the root of the tree to be created with this routine -QAK) */
    if (type == H5P_TYPE_USER)
        assert(par_class);

    /* Allocate room for the class */
    if (NULL == (pclass = H5FL_CALLOC(H5P_genclass_t)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, NULL, "property list class allocation failed");

    /* Set class state */
    pclass->parent = par_class;
    if (NULL == (pclass->name = H5MM_xstrdup(name)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, NULL, "property list class name allocation failed");
    pclass->type      = type;
    pclass->nprops    = 0;                /* Classes are created without properties initially */
    pclass->plists    = 0;                /* No properties lists of this class yet */
    pclass->classes   = 0;                /* No classes derived from this class yet */
    pclass->ref_count = 1;                /* This is the first reference to the new class */
    pclass->deleted   = false;            /* Not deleted yet... :-) */
    pclass->revision  = H5P_GET_NEXT_REV; /* Get a revision number for the class */

    /* Create the skip list for properties */
    if (NULL == (pclass->props = H5SL_create(H5SL_TYPE_STR, NULL)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, NULL, "can't create skip list for properties");

    /* Set callback functions and pass-along data */
    pclass->create_func = cls_create;
    pclass->create_data = create_data;
    pclass->copy_func   = cls_copy;
    pclass->copy_data   = copy_data;
    pclass->close_func  = cls_close;
    pclass->close_data  = close_data;

    /* Increment parent class's derived class value */
    if (par_class != NULL) {
        if (H5P__access_class(par_class, H5P_MOD_INC_CLS) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, NULL, "Can't increment parent class ref count");
    } /* end if */

    /* Set return value */
    ret_value = pclass;

done:
    /* Free any resources allocated */
    if (ret_value == NULL)
        if (pclass) {
            if (pclass->name)
                H5MM_xfree(pclass->name);
            if (pclass->props) {
                bool make_cb = false;

                H5SL_destroy(pclass->props, H5P__free_prop_cb, &make_cb);
            } /* end if */
            pclass = H5FL_FREE(H5P_genclass_t, pclass);
        } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__create_class() */

/*--------------------------------------------------------------------------
 NAME
    H5P__create
 PURPOSE
    Internal routine to create a new property list of a property list class.
 USAGE
    H5P_genplist_t *H5P__create(class)
        H5P_genclass_t *class;  IN: Property list class create list from
 RETURNS
    Returns a pointer to the newly created property list on success,
        NULL on failure.
 DESCRIPTION
        Creates a property list of a given class.  If a 'create' callback
    exists for the property list class, it is called before the
    property list is passed back to the user.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        If this routine is called from a library routine other than
    H5P_c, the calling routine is responsible for getting an ID for
    the property list and calling the class 'create' callback (if one exists)
    and also setting the "class_init" flag.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static H5P_genplist_t *
H5P__create(H5P_genclass_t *pclass)
{
    H5P_genclass_t *tclass;           /* Temporary class pointer */
    H5P_genplist_t *plist = NULL;     /* New property list created */
    H5P_genprop_t  *tmp;              /* Temporary pointer to parent class properties */
    H5SL_t         *seen      = NULL; /* Skip list to hold names of properties already seen */
    H5P_genplist_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);

    /*
     * Create new property list object
     */

    /* Allocate room for the property list */
    if (NULL == (plist = H5FL_CALLOC(H5P_genplist_t)))
        HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed");

    /* Set class state */
    plist->pclass     = pclass;
    plist->nprops     = 0;     /* Initially the plist has the same number of properties as the class */
    plist->class_init = false; /* Initially, wait until the class callback finishes to set */

    /* Create the skip list for changed properties */
    if ((plist->props = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, NULL, "can't create skip list for changed properties");

    /* Create the skip list for deleted properties */
    if ((plist->del = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, NULL, "can't create skip list for deleted properties");

    /* Create the skip list to hold names of properties already seen
     * (This prevents a property in the class hierarchy from having it's
     * 'create' callback called, if a property in the class hierarchy has
     * already been seen)
     */
    if ((seen = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, NULL, "can't create skip list for seen properties");

    /*
     * Check if we should copy class properties (up through list of parent classes also),
     * initialize each with default value & make property 'create' callback.
     */
    tclass = pclass;
    while (tclass != NULL) {
        if (tclass->nprops > 0) {
            H5SL_node_t *curr_node; /* Current node in skip list */

            /* Walk through the properties in the old class */
            curr_node = H5SL_first(tclass->props);
            while (curr_node != NULL) {
                /* Get pointer to property from node */
                tmp = (H5P_genprop_t *)H5SL_item(curr_node);

                /* Only "create" properties we haven't seen before */
                if (H5SL_search(seen, tmp->name) == NULL) {
                    /* Call property creation callback, if it exists */
                    if (tmp->create) {
                        /* Call the callback & insert changed value into skip list (if necessary) */
                        if (H5P__do_prop_cb1(plist->props, tmp, tmp->create) < 0)
                            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, NULL, "Can't create property");
                    } /* end if */

                    /* Add property name to "seen" list */
                    if (H5SL_insert(seen, tmp->name, tmp->name) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, NULL,
                                    "can't insert property into seen skip list");

                    /* Increment the number of properties in list */
                    plist->nprops++;
                } /* end if */

                /* Get the next property node in the skip list */
                curr_node = H5SL_next(curr_node);
            } /* end while */
        }     /* end if */

        /* Go up to parent class */
        tclass = tclass->parent;
    } /* end while */

    /* Increment the number of property lists derived from class */
    if (H5P__access_class(plist->pclass, H5P_MOD_INC_LST) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, NULL, "Can't increment class ref count");

    /* Set return value */
    ret_value = plist;

done:
    /* Release the skip list of 'seen' properties */
    if (seen != NULL)
        H5SL_close(seen);

    /* Release resources allocated on failure */
    if (ret_value == NULL) {
        if (plist != NULL) {
            /* Close & free any changed properties */
            if (plist->props) {
                unsigned make_cb = 1;

                H5SL_destroy(plist->props, H5P__free_prop_cb, &make_cb);
            } /* end if */

            /* Close the deleted property skip list */
            if (plist->del)
                H5SL_close(plist->del);

            /* Release the property list itself */
            plist = H5FL_FREE(H5P_genplist_t, plist);
        } /* end if */
    }     /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__create() */

/*--------------------------------------------------------------------------
 NAME
    H5P_create_id
 PURPOSE
    Internal routine to create a new property list of a property list class.
 USAGE
    hid_t H5P_create_id(pclass)
        H5P_genclass_t *pclass;       IN: Property list class create list from
 RETURNS
    Returns a valid property list ID on success, H5I_INVALID_HID on failure.
 DESCRIPTION
        Creates a property list of a given class.  If a 'create' callback
    exists for the property list class, it is called before the
    property list is passed back to the user.  If 'create' callbacks exist for
    any individual properties in the property list, they are called before the
    class 'create' callback.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
hid_t
H5P_create_id(H5P_genclass_t *pclass, bool app_ref)
{
    H5P_genclass_t *tclass;                      /* Temporary class pointer */
    H5P_genplist_t *plist     = NULL;            /* Property list created */
    hid_t           plist_id  = FAIL;            /* Property list ID */
    hid_t           ret_value = H5I_INVALID_HID; /* return value */

    FUNC_ENTER_NOAPI(H5I_INVALID_HID)

    assert(pclass);

    /* Create the new property list */
    if ((plist = H5P__create(pclass)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create property list");

    /* Get an ID for the property list */
    if ((plist_id = H5I_register(H5I_GENPROP_LST, plist, app_ref)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, H5I_INVALID_HID, "unable to register property list");

    /* Save the property list ID in the property list struct, for use in the property class's 'close' callback
     */
    plist->plist_id = plist_id;

    /* Call the class callback (if it exists) now that we have the property list ID
     * (up through chain of parent classes also)
     */
    tclass = plist->pclass;
    while (NULL != tclass) {
        if (NULL != tclass->create_func) {
            if ((tclass->create_func)(plist_id, tclass->create_data) < 0) {
                /* Delete ID, ignore return value */
                H5I_remove(plist_id);
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, H5I_INVALID_HID, "Can't initialize property");
            } /* end if */
        }     /* end if */

        /* Go up to parent class */
        tclass = tclass->parent;
    } /* end while */

    /* Set the class initialization flag */
    plist->class_init = true;

    /* Set the return value */
    ret_value = plist_id;

done:
    if (H5I_INVALID_HID == ret_value && plist)
        H5P_close(plist);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_create_id() */

/*--------------------------------------------------------------------------
 NAME
    H5P__register_real
 PURPOSE
    Internal routine to register a new property in a property list class.
 USAGE
    herr_t H5P__register_real(class, name, size, default, prp_create, prp_set,
                             prp_get, prp_close, prp_encode, prp_decode)
        H5P_genclass_t *class;  IN: Property list class to modify
        const char *name;       IN: Name of property to register
        size_t size;            IN: Size of property in bytes
        void *def_value;        IN: Pointer to buffer containing default value
                                    for property in newly created property lists
        H5P_prp_create_func_t prp_create;   IN: Function pointer to property
                                    creation callback
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_encode_func_t prp_encode; IN: Function pointer to property encode
        H5P_prp_decode_func_t prp_decode; IN: Function pointer to property decode
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_compare_func_t prp_cmp; IN: Function pointer to property compare callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Registers a new property with a property list class.  The property will
    exist in all property list objects of that class after this routine is
    finished.  The name of the property must not already exist.  The default
    property value must be provided and all new property lists created with this
    property will have the property value set to the default provided.  Any of
    the callback routines may be set to NULL if they are not needed.

        Zero-sized properties are allowed and do not store any data in the
    property list.  These may be used as flags to indicate the presence or
    absence of a particular piece of information.  The 'default' pointer for a
    zero-sized property may be set to NULL.  The property 'create' & 'close'
    callbacks are called for zero-sized properties, but the 'set' and 'get'
    callbacks are never called.

        The 'create' callback is called when a new property list with this
    property is being created.  H5P_prp_create_func_t is defined as:
        typedef herr_t (*H5P_prp_create_func_t)(hid_t prop_id, const char *name,
                size_t size, void *initial_value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being created.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *initial_value; IN/OUT: The initial value for the property being created.
                                (The 'default' value passed to H5Pregister2)
    The 'create' routine may modify the value to be set and those changes will
    be stored as the initial value of the property.  If the 'create' routine
    returns a negative value, the new property value is not copied into the
    property and the property list creation routine returns an error value.

        The 'set' callback is called before a new value is copied into the
    property.  H5P_prp_set_func_t is defined as:
        typedef herr_t (*H5P_prp_set_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being modified.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *new_value;    IN/OUT: The value being set for the property.
    The 'set' routine may modify the value to be set and those changes will be
    stored as the value of the property.  If the 'set' routine returns a
    negative value, the new property value is not copied into the property and
    the property list set routine returns an error value.

        The 'get' callback is called before a value is retrieved from the
    property.  H5P_prp_get_func_t is defined as:
        typedef herr_t (*H5P_prp_get_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being queried.
        const char *name;   IN: The name of the property being queried.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value being retrieved for the property.
    The 'get' routine may modify the value to be retrieved and those changes
    will be returned to the calling function.  If the 'get' routine returns a
    negative value, the property value is returned and the property list get
    routine returns an error value.

        The 'delete' callback is called when a property is deleted from a
    property list.  H5P_prp_del_func_t is defined as:
        typedef herr_t (*H5P_prp_del_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list the property is deleted from.
        const char *name;   IN: The name of the property being deleted.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value of the property being deleted.
    The 'delete' routine may modify the value passed in, but the value is not
    used by the library when the 'delete' routine returns.  If the
    'delete' routine returns a negative value, the property list deletion
    routine returns an error value but the property is still deleted.

        The 'copy' callback is called when a property list with this
    property is copied.  H5P_prp_copy_func_t is defined as:
        typedef herr_t (*H5P_prp_copy_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being copied.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being copied.
    The 'copy' routine may modify the value to be copied and those changes will be
    stored as the value of the property.  If the 'copy' routine returns a
    negative value, the new property value is not copied into the property and
    the property list copy routine returns an error value.

        The 'compare' callback is called when a property list with this
    property is compared to another property list.  H5P_prp_compare_func_t is
    defined as:
        typedef int (*H5P_prp_compare_func_t)( void *value1, void *value2,
            size_t size);
    where the parameters to the callback function are:
        const void *value1; IN: The value of the first property being compared.
        const void *value2; IN: The value of the second property being compared.
        size_t size;        IN: The size of the property value
    The 'compare' routine may not modify the values to be compared.  The
    'compare' routine should return a positive value if VALUE1 is greater than
    VALUE2, a negative value if VALUE2 is greater than VALUE1 and zero if VALUE1
    and VALUE2 are equal.

        The 'close' callback is called when a property list with this
    property is being destroyed.  H5P_prp_close_func_t is defined as:
        typedef herr_t (*H5P_prp_close_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being closed.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being closed.
    The 'close' routine may modify the value passed in, but the value is not
    used by the library when the 'close' routine returns.  If the
    'close' routine returns a negative value, the property list close
    routine returns an error value but the property list is still closed.

        The 'encode' callback is called when a property list with this
    property is being encoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to encode.
        size_t *size;       IN/OUT: The size of the buffer to encode the property.
        void *value;        IN: The value of the property being encoded.
        void *plist;        IN: The property list structure.
        uint8_t **buf;      OUT: The buffer that holds the encoded property;
    The 'encode' routine returns the size needed to encode the property value
    if the buffer passed in is NULL or the size is zero. Otherwise it encodes
    the property value into binary in buf.

        The 'decode' callback is called when a property list with this
    property is being decoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to decode.
        size_t *size;       IN: H5_ATTR_UNUSED
        void *value;        IN: H5_ATTR_UNUSED
        void *plist;        IN: The property list structure.
        uint8_t **buf;      IN: The buffer that holds the binary encoded property;
    The 'decode' routine decodes the binary buffer passed in and transforms it into
    corresponding property values that are set in the property list passed in.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback function may be useful to range check the value being
    set for the property or may perform some transformation/translation of the
    value set.  The 'get' callback would then [probably] reverse the
    transformation, etc.  A single 'get' or 'set' callback could handle
    multiple properties by performing different actions based on the property
    name or other properties in the property list.

        I would like to say "the property list is not closed" when a 'close'
    routine fails, but I don't think that's possible due to other properties in
    the list being successfully closed & removed from the property list.  I
    suppose that it would be possible to just remove the properties which have
    successful 'close' callbacks, but I'm not happy with the ramifications
    of a mangled, un-closable property list hanging around...  Any comments? -QAK

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__register_real(H5P_genclass_t *pclass, const char *name, size_t size, const void *def_value,
                   H5P_prp_create_func_t prp_create, H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get,
                   H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
                   H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy,
                   H5P_prp_compare_func_t prp_cmp, H5P_prp_close_func_t prp_close)
{
    H5P_genprop_t *new_prop  = NULL;    /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);
    assert(0 == pclass->plists);
    assert(0 == pclass->classes);
    assert(name);
    assert((size > 0 && def_value != NULL) || (size == 0));

    /* Check for duplicate named properties */
    if (NULL != H5SL_search(pclass->props, name))
        HGOTO_ERROR(H5E_PLIST, H5E_EXISTS, FAIL, "property already exists");

    /* Create property object from parameters */
    if (NULL == (new_prop = H5P__create_prop(name, size, H5P_PROP_WITHIN_CLASS, def_value, prp_create,
                                             prp_set, prp_get, prp_encode, prp_decode, prp_delete, prp_copy,
                                             prp_cmp, prp_close)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "Can't create property");

    /* Insert property into property list class */
    if (H5P__add_prop(pclass->props, new_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into class");

    /* Increment property count for class */
    pclass->nprops++;

    /* Update the revision for the class */
    pclass->revision = H5P_GET_NEXT_REV;

done:
    if (ret_value < 0)
        if (new_prop && H5P__free_prop(new_prop) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "unable to close property");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__register_real() */

/*--------------------------------------------------------------------------
 NAME
    H5P__register
 PURPOSE
    Internal routine to register a new property in a property list class.
 USAGE
    herr_t H5P__register(class, name, size, default, prp_create, prp_set, prp_get, prp_close)
        H5P_genclass_t **class; IN: Property list class to modify
        const char *name;       IN: Name of property to register
        size_t size;            IN: Size of property in bytes
        void *def_value;        IN: Pointer to buffer containing default value
                                    for property in newly created property lists
        H5P_prp_create_func_t prp_create;   IN: Function pointer to property
                                    creation callback
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_encode_func_t prp_encode; IN: Function pointer to property encode
        H5P_prp_decode_func_t prp_decode; IN: Function pointer to property decode
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_compare_func_t prp_cmp; IN: Function pointer to property compare callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Registers a new property with a property list class.  The property will
    exist in all property list objects of that class after this routine is
    finished.  The name of the property must not already exist.  The default
    property value must be provided and all new property lists created with this
    property will have the property value set to the default provided.  Any of
    the callback routines may be set to NULL if they are not needed.

        Zero-sized properties are allowed and do not store any data in the
    property list.  These may be used as flags to indicate the presence or
    absence of a particular piece of information.  The 'default' pointer for a
    zero-sized property may be set to NULL.  The property 'create' & 'close'
    callbacks are called for zero-sized properties, but the 'set' and 'get'
    callbacks are never called.

        The 'create' callback is called when a new property list with this
    property is being created.  H5P_prp_create_func_t is defined as:
        typedef herr_t (*H5P_prp_create_func_t)(hid_t prop_id, const char *name,
                size_t size, void *initial_value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being created.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *initial_value; IN/OUT: The initial value for the property being created.
                                (The 'default' value passed to H5Pregister2)
    The 'create' routine may modify the value to be set and those changes will
    be stored as the initial value of the property.  If the 'create' routine
    returns a negative value, the new property value is not copied into the
    property and the property list creation routine returns an error value.

        The 'set' callback is called before a new value is copied into the
    property.  H5P_prp_set_func_t is defined as:
        typedef herr_t (*H5P_prp_set_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being modified.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *new_value;    IN/OUT: The value being set for the property.
    The 'set' routine may modify the value to be set and those changes will be
    stored as the value of the property.  If the 'set' routine returns a
    negative value, the new property value is not copied into the property and
    the property list set routine returns an error value.

        The 'get' callback is called before a value is retrieved from the
    property.  H5P_prp_get_func_t is defined as:
        typedef herr_t (*H5P_prp_get_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being queried.
        const char *name;   IN: The name of the property being queried.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value being retrieved for the property.
    The 'get' routine may modify the value to be retrieved and those changes
    will be returned to the calling function.  If the 'get' routine returns a
    negative value, the property value is returned and the property list get
    routine returns an error value.

        The 'encode' callback is called when a property list with this
    property is being encoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to encode.
        size_t *size;       IN/OUT: The size of the buffer to encode the property.
        void *value;        IN: The value of the property being encoded.
        void *plist;        IN: The property list structure.
        uint8_t **buf;      OUT: The buffer that holds the encoded property;
    The 'encode' routine returns the size needed to encode the property value
    if the buffer passed in is NULL or the size is zero. Otherwise it encodes
    the property value into binary in buf.

        The 'decode' callback is called when a property list with this
    property is being decoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to decode.
        size_t *size;       IN: H5_ATTR_UNUSED
        void *value;        IN: H5_ATTR_UNUSED
        void *plist;        IN: The property list structure.
        uint8_t **buf;      IN: The buffer that holds the binary encoded property;
    The 'decode' routine decodes the binary buffer passed in and transforms it into
    corresponding property values that are set in the property list passed in.

        The 'delete' callback is called when a property is deleted from a
    property list.  H5P_prp_del_func_t is defined as:
        typedef herr_t (*H5P_prp_del_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list the property is deleted from.
        const char *name;   IN: The name of the property being deleted.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value of the property being deleted.
    The 'delete' routine may modify the value passed in, but the value is not
    used by the library when the 'delete' routine returns.  If the
    'delete' routine returns a negative value, the property list deletion
    routine returns an error value but the property is still deleted.

        The 'copy' callback is called when a property list with this
    property is copied.  H5P_prp_copy_func_t is defined as:
        typedef herr_t (*H5P_prp_copy_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being copied.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being copied.
    The 'copy' routine may modify the value to be copied and those changes will be
    stored as the value of the property.  If the 'copy' routine returns a
    negative value, the new property value is not copied into the property and
    the property list copy routine returns an error value.

        The 'compare' callback is called when a property list with this
    property is compared to another property list.  H5P_prp_compare_func_t is
    defined as:
        typedef int (*H5P_prp_compare_func_t)( void *value1, void *value2,
            size_t size);
    where the parameters to the callback function are:
        const void *value1; IN: The value of the first property being compared.
        const void *value2; IN: The value of the second property being compared.
        size_t size;        IN: The size of the property value
    The 'compare' routine may not modify the values to be compared.  The
    'compare' routine should return a positive value if VALUE1 is greater than
    VALUE2, a negative value if VALUE2 is greater than VALUE1 and zero if VALUE1
    and VALUE2 are equal.

        The 'close' callback is called when a property list with this
    property is being destroyed.  H5P_prp_close_func_t is defined as:
        typedef herr_t (*H5P_prp_close_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being closed.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being closed.
    The 'close' routine may modify the value passed in, but the value is not
    used by the library when the 'close' routine returns.  If the
    'close' routine returns a negative value, the property list close
    routine returns an error value but the property list is still closed.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback function may be useful to range check the value being
    set for the property or may perform some transformation/translation of the
    value set.  The 'get' callback would then [probably] reverse the
    transformation, etc.  A single 'get' or 'set' callback could handle
    multiple properties by performing different actions based on the property
    name or other properties in the property list.

        I would like to say "the property list is not closed" when a 'close'
    routine fails, but I don't think that's possible due to other properties in
    the list being successfully closed & removed from the property list.  I
    suppose that it would be possible to just remove the properties which have
    successful 'close' callbacks, but I'm not happy with the ramifications
    of a mangled, un-closable property list hanging around...  Any comments? -QAK

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__register(H5P_genclass_t **ppclass, const char *name, size_t size, const void *def_value,
              H5P_prp_create_func_t prp_create, H5P_prp_set_func_t prp_set, H5P_prp_get_func_t prp_get,
              H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
              H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy, H5P_prp_compare_func_t prp_cmp,
              H5P_prp_close_func_t prp_close)
{
    H5P_genclass_t *pclass    = *ppclass; /* Pointer to class to modify */
    H5P_genclass_t *new_class = NULL;     /* New class pointer */
    herr_t          ret_value = SUCCEED;  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(ppclass);
    assert(pclass);

    /* Check if class needs to be split because property lists or classes have
     *  been created since the last modification was made to the class.
     */
    if (pclass->plists > 0 || pclass->classes > 0) {
        if (NULL == (new_class = H5P__create_class(
                         pclass->parent, pclass->name, pclass->type, pclass->create_func, pclass->create_data,
                         pclass->copy_func, pclass->copy_data, pclass->close_func, pclass->close_data)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "can't copy class");

        /* Walk through the skip list of the old class and copy properties */
        if (pclass->nprops > 0) {
            H5SL_node_t *curr_node; /* Current node in skip list */

            /* Walk through the properties in the old class */
            curr_node = H5SL_first(pclass->props);
            while (curr_node != NULL) {
                H5P_genprop_t *pcopy; /* Property copy */

                /* Make a copy of the class's property */
                if (NULL ==
                    (pcopy = H5P__dup_prop((H5P_genprop_t *)H5SL_item(curr_node), H5P_PROP_WITHIN_CLASS)))
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");

                /* Insert the initialized property into the property class */
                if (H5P__add_prop(new_class->props, pcopy) < 0)
                    HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into class");

                /* Increment property count for class */
                new_class->nprops++;

                /* Get the next property node in the skip list */
                curr_node = H5SL_next(curr_node);
            } /* end while */
        }     /* end if */

        /* Use the new class instead of the old one */
        pclass = new_class;
    } /* end if */

    /* Really register the property in the class */
    if (H5P__register_real(pclass, name, size, def_value, prp_create, prp_set, prp_get, prp_encode,
                           prp_decode, prp_delete, prp_copy, prp_cmp, prp_close) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "can't register property");

    /* Update pointer to pointer to class, if a new one was generated */
    if (new_class)
        *ppclass = pclass;

done:
    if (ret_value < 0)
        if (new_class && H5P__close_class(new_class) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "unable to close new property class");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__register() */

/*--------------------------------------------------------------------------
 NAME
    H5P_insert
 PURPOSE
    Internal routine to insert a new property in a property list.
 USAGE
    herr_t H5P_insert(plist, name, size, value, prp_set, prp_get, prp_close,
                      prp_encode, prp_decode)
        H5P_genplist_t *plist;  IN: Property list to add property to
        const char *name;       IN: Name of property to add
        size_t size;            IN: Size of property in bytes
        void *value;            IN: Pointer to the value for the property
        H5P_prp_set_func_t prp_set; IN: Function pointer to property set callback
        H5P_prp_get_func_t prp_get; IN: Function pointer to property get callback
        H5P_prp_encode_func_t prp_encode; IN: Function pointer to property encode
        H5P_prp_decode_func_t prp_decode; IN: Function pointer to property decode
        H5P_prp_delete_func_t prp_delete; IN: Function pointer to property delete callback
        H5P_prp_copy_func_t prp_copy; IN: Function pointer to property copy callback
        H5P_prp_compare_func_t prp_cmp; IN: Function pointer to property compare callback
        H5P_prp_close_func_t prp_close; IN: Function pointer to property close
                                    callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Inserts a temporary property into a property list.  The property will
    exist only in this property list object.  The name of the property must not
    already exist.  The value must be provided unless the property is zero-
    sized.  Any of the callback routines may be set to NULL if they are not
    needed.

        Zero-sized properties are allowed and do not store any data in the
    property list.  These may be used as flags to indicate the presence or
    absence of a particular piece of information.  The 'value' pointer for a
    zero-sized property may be set to NULL.  The property 'close' callback is
    called for zero-sized properties, but the 'set' and 'get' callbacks are
    never called.

        The 'set' callback is called before a new value is copied into the
    property.  H5P_prp_set_func_t is defined as:
        typedef herr_t (*H5P_prp_set_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being modified.
        const char *name;   IN: The name of the property being modified.
        size_t size;        IN: The size of the property value
        void *new_value;    IN/OUT: The value being set for the property.
    The 'set' routine may modify the value to be set and those changes will be
    stored as the value of the property.  If the 'set' routine returns a
    negative value, the new property value is not copied into the property and
    the property list set routine returns an error value.

        The 'get' callback is called before a value is retrieved from the
    property.  H5P_prp_get_func_t is defined as:
        typedef herr_t (*H5P_prp_get_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list being queried.
        const char *name;   IN: The name of the property being queried.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value being retrieved for the property.
    The 'get' routine may modify the value to be retrieved and those changes
    will be returned to the calling function.  If the 'get' routine returns a
    negative value, the property value is returned and the property list get
    routine returns an error value.

        The 'encode' callback is called when a property list with this
    property is being encoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to encode.
        size_t *size;       IN/OUT: The size of the buffer to encode the property.
        void *value;        IN: The value of the property being encoded.
        void *plist;        IN: The property list structure.
        uint8_t **buf;      OUT: The buffer that holds the encoded property;
    The 'encode' routine returns the size needed to encode the property value
    if the buffer passed in is NULL or the size is zero. Otherwise it encodes
    the property value into binary in buf.

        The 'decode' callback is called when a property list with this
    property is being decoded.  H5P_prp_encode_func_t is defined as:
        typedef herr_t (*H5P_prp_encode_func_t)(void *f, size_t *size,
        void *value, void *plist, uint8_t **buf);
    where the parameters to the callback function are:
        void *f;            IN: A fake file structure used to decode.
        size_t *size;       IN: H5_ATTR_UNUSED
        void *value;        IN: H5_ATTR_UNUSED
        void *plist;        IN: The property list structure.
        uint8_t **buf;      IN: The buffer that holds the binary encoded property;
    The 'decode' routine decodes the binary buffer passed in and transforms it into
    corresponding property values that are set in the property list passed in.

        The 'delete' callback is called when a property is deleted from a
    property list.  H5P_prp_del_func_t is defined as:
        typedef herr_t (*H5P_prp_del_func_t)(hid_t prop_id, const char *name,
            size_t size, void *value);
    where the parameters to the callback function are:
        hid_t prop_id;      IN: The ID of the property list the property is deleted from.
        const char *name;   IN: The name of the property being deleted.
        size_t size;        IN: The size of the property value
        void *value;        IN/OUT: The value of the property being deleted.
    The 'delete' routine may modify the value passed in, but the value is not
    used by the library when the 'delete' routine returns.  If the
    'delete' routine returns a negative value, the property list deletion
    routine returns an error value but the property is still deleted.

        The 'copy' callback is called when a property list with this
    property is copied.  H5P_prp_copy_func_t is defined as:
        typedef herr_t (*H5P_prp_copy_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being copied.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being copied.
    The 'copy' routine may modify the value to be copied and those changes will be
    stored as the value of the property.  If the 'copy' routine returns a
    negative value, the new property value is not copied into the property and
    the property list copy routine returns an error value.

        The 'compare' callback is called when a property list with this
    property is compared to another property list.  H5P_prp_compare_func_t is
    defined as:
        typedef int (*H5P_prp_compare_func_t)( void *value1, void *value2,
            size_t size);
    where the parameters to the callback function are:
        const void *value1; IN: The value of the first property being compared.
        const void *value2; IN: The value of the second property being compared.
        size_t size;        IN: The size of the property value
    The 'compare' routine may not modify the values to be compared.  The
    'compare' routine should return a positive value if VALUE1 is greater than
    VALUE2, a negative value if VALUE2 is greater than VALUE1 and zero if VALUE1
    and VALUE2 are equal.

        The 'close' callback is called when a property list with this
    property is being destroyed.  H5P_prp_close_func_t is defined as:
        typedef herr_t (*H5P_prp_close_func_t)(const char *name, size_t size,
            void *value);
    where the parameters to the callback function are:
        const char *name;   IN: The name of the property being closed.
        size_t size;        IN: The size of the property value
        void *value;        IN: The value of the property being closed.
    The 'close' routine may modify the value passed in, but the value is not
    used by the library when the 'close' routine returns.  If the
    'close' routine returns a negative value, the property list close
    routine returns an error value but the property list is still closed.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback function may be useful to range check the value being
    set for the property or may perform some transformation/translation of the
    value set.  The 'get' callback would then [probably] reverse the
    transformation, etc.  A single 'get' or 'set' callback could handle
    multiple properties by performing different actions based on the property
    name or other properties in the property list.

        There is no 'create' callback routine for temporary property list
    objects, the initial value is assumed to have any necessary setup already
    performed on it.

        I would like to say "the property list is not closed" when a 'close'
    routine fails, but I don't think that's possible due to other properties in
    the list being successfully closed & removed from the property list.  I
    suppose that it would be possible to just remove the properties which have
    successful 'close' callbacks, but I'm not happy with the ramifications
    of a mangled, un-closable property list hanging around...  Any comments? -QAK

 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_insert(H5P_genplist_t *plist, const char *name, size_t size, void *value, H5P_prp_set_func_t prp_set,
           H5P_prp_get_func_t prp_get, H5P_prp_encode_func_t prp_encode, H5P_prp_decode_func_t prp_decode,
           H5P_prp_delete_func_t prp_delete, H5P_prp_copy_func_t prp_copy, H5P_prp_compare_func_t prp_cmp,
           H5P_prp_close_func_t prp_close)
{
    H5P_genprop_t *new_prop  = NULL;    /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    assert(plist);
    assert(name);
    assert((size > 0 && value != NULL) || (size == 0));

    /* Check for duplicate named properties */
    if (NULL != H5SL_search(plist->props, name))
        HGOTO_ERROR(H5E_PLIST, H5E_EXISTS, FAIL, "property already exists");

    /* Check if the property has been deleted */
    if (NULL != H5SL_search(plist->del, name)) {
        char *temp_name = NULL;

        /* Remove the property name from the deleted property skip list */
        if (NULL == (temp_name = (char *)H5SL_remove(plist->del, name)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "can't remove property from deleted skip list");

        /* free the name of the removed property */
        H5MM_xfree(temp_name);
    } /* end if */
    else {
        H5P_genclass_t *tclass; /* Temporary class pointer */

        /* Check if the property is already in the class hierarchy */
        tclass = plist->pclass;
        while (tclass) {
            if (tclass->nprops > 0) {
                /* Find the property in the class */
                if (NULL != H5SL_search(tclass->props, name))
                    HGOTO_ERROR(H5E_PLIST, H5E_EXISTS, FAIL, "property already exists");
            } /* end if */

            /* Go up to parent class */
            tclass = tclass->parent;
        } /* end while */
    }     /* end else */

    /* Ok to add to property list */

    /* Create property object from parameters */
    if (NULL ==
        (new_prop = H5P__create_prop(name, size, H5P_PROP_WITHIN_LIST, value, NULL, prp_set, prp_get,
                                     prp_encode, prp_decode, prp_delete, prp_copy, prp_cmp, prp_close)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "Can't create property");

    /* Insert property into property list class */
    if (H5P__add_prop(plist->props, new_prop) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into class");

    /* Increment property count for class */
    plist->nprops++;

done:
    if (ret_value < 0)
        if (new_prop && H5P__free_prop(new_prop) < 0)
            HDONE_ERROR(H5E_PLIST, H5E_CANTRELEASE, FAIL, "unable to close property");

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_insert() */

/*--------------------------------------------------------------------------
 NAME
    H5P__do_prop
 PURPOSE
    Internal routine to perform an operation on a property in a property list
 USAGE
    herr_t H5P__do_prop(plist, name, cb, udata)
        H5P_genplist_t *plist;  IN: Property list to find property in
        const char *name;       IN: Name of property to set
        H5P_do_plist_op_t plist_op;  IN: Pointer to the callback to invoke when the
                                    property is found in the property list
        H5P_do_pclass_op_t pclass_op; IN: Pointer to the callback to invoke when the
                                    property is found in the property class
        void *udata;            IN: Pointer to the user data for the callback
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Finds a property in a property list and calls the callback with it.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__do_prop(H5P_genplist_t *plist, const char *name, H5P_do_plist_op_t plist_op,
             H5P_do_pclass_op_t pclass_op, void *udata)
{
    H5P_genclass_t *tclass;              /* Temporary class pointer */
    H5P_genprop_t  *prop;                /* Temporary property pointer */
    herr_t          ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(plist_op);
    assert(pclass_op);

    /* Check if the property has been deleted */
    if (NULL != H5SL_search(plist->del, name))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

    /* Find property in changed list */
    if (NULL != (prop = (H5P_genprop_t *)H5SL_search(plist->props, name))) {
        /* Call the 'found in property list' callback */
        if ((*plist_op)(plist, name, prop, udata) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on property");
    } /* end if */
    else {
        /*
         * Check if we should set class properties (up through list of parent classes also),
         * & make property 'set' callback.
         */
        tclass = plist->pclass;
        while (NULL != tclass) {
            if (tclass->nprops > 0) {
                /* Find the property in the class */
                if (NULL != (prop = (H5P_genprop_t *)H5SL_search(tclass->props, name))) {
                    /* Call the 'found in class' callback */
                    if ((*pclass_op)(plist, name, prop, udata) < 0)
                        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on property");

                    /* Leave */
                    break;
                } /* end if */
            }     /* end if */

            /* Go up to parent class */
            tclass = tclass->parent;
        } /* end while */

        /* If we get this far, then it wasn't in the list of changed properties,
         * nor in the properties in the class hierarchy, indicate an error
         */
        if (NULL == tclass)
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "can't find property in skip list");
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__do_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__poke_plist_cb
 PURPOSE
    Internal callback for H5P__do_prop, to overwrite a property's value in a property list.
 USAGE
    herr_t H5P__poke_plist_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to overwrite property in
        const char *name;       IN: Name of property to overwrite
        H5P_genprop_t *prop;    IN: Property to overwrite
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Overwrite a value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property list.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__poke_plist_cb(H5P_genplist_t H5_ATTR_NDEBUG_UNUSED *plist, const char H5_ATTR_NDEBUG_UNUSED *name,
                   H5P_genprop_t *prop, void *_udata)
{
    H5P_prop_set_ud_t *udata     = (H5P_prop_set_ud_t *)_udata; /* User data for callback */
    herr_t             ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Overwrite value in property */
    H5MM_memcpy(prop->value, udata->value, prop->size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__poke_plist_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__poke_pclass_cb
 PURPOSE
    Internal callback for H5P__do_prop, to overwrite a property's value in a property list.
 USAGE
    herr_t H5P__poke_pclass_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to overwrite property in
        const char *name;       IN: Name of property to overwrite
        H5P_genprop_t *prop;    IN: Property to overwrite
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Overwrite a value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property class.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__poke_pclass_cb(H5P_genplist_t *plist, const char H5_ATTR_NDEBUG_UNUSED *name, H5P_genprop_t *prop,
                    void *_udata)
{
    H5P_prop_set_ud_t *udata     = (H5P_prop_set_ud_t *)_udata; /* User data for callback */
    H5P_genprop_t     *pcopy     = NULL;    /* Copy of property to insert into skip list */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);
    assert(prop->cmp);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Make a copy of the class's property */
    if (NULL == (pcopy = H5P__dup_prop(prop, H5P_PROP_WITHIN_LIST)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");

    H5MM_memcpy(pcopy->value, udata->value, pcopy->size);

    /* Insert the changed property into the property list */
    if (H5P__add_prop(plist->props, pcopy) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert changed property into skip list");

done:
    /* Cleanup on failure */
    if (ret_value < 0)
        if (pcopy)
            H5P__free_prop(pcopy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__poke_pclass_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P_poke
 PURPOSE
    Internal routine to overwrite a property's value in a property list.
 USAGE
    herr_t H5P_poke(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to find property in
        const char *name;       IN: Name of property to overwrite
        void *value;            IN: Pointer to the value for the property
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Overwrites a property in a property list (i.e. a "shallow" copy over
    the property value).  The property name must exist or this routine will
    fail.  If there is a setget' callback routine registered for this property,
    it is _NOT_ called.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        This routine may not be called for zero-sized properties and will
    return an error in that case.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_poke(H5P_genplist_t *plist, const char *name, const void *value)
{
    H5P_prop_set_ud_t udata;               /* User data for callback */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(value);

    /* Find the property and set the value */
    udata.value = value;
    if (H5P__do_prop(plist, name, H5P__poke_plist_cb, H5P__poke_pclass_cb, &udata) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on plist to overwrite value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_poke() */

/*--------------------------------------------------------------------------
 NAME
    H5P__set_plist_cb
 PURPOSE
    Internal callback for H5P__do_prop, to set a property's value in a property list.
 USAGE
    herr_t H5P__set_plist_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to set property in
        const char *name;       IN: Name of property to set
        H5P_genprop_t *prop;    IN: Property to set
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Sets a new value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property list.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__set_plist_cb(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop, void *_udata)
{
    H5P_prop_set_ud_t *udata     = (H5P_prop_set_ud_t *)_udata; /* User data for callback */
    void              *tmp_value = NULL;                        /* Temporary value for property */
    const void        *prp_value = NULL;                        /* Property value */
    herr_t             ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Make a copy of the value and pass to 'set' callback */
    if (NULL != prop->set) {
        /* Make a copy of the current value, in case the callback fails */
        if (NULL == (tmp_value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed temporary property value");
        H5MM_memcpy(tmp_value, udata->value, prop->size);

        /* Call user's callback */
        if ((*(prop->set))(plist->plist_id, name, prop->size, tmp_value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set property value");

        /* Set the pointer for copying */
        prp_value = tmp_value;
    } /* end if */
    /* No 'set' callback, just copy value */
    else
        prp_value = udata->value;

    /* Free any previous value for the property */
    if (NULL != prop->del) {
        /* Call user's 'delete' callback */
        if ((*(prop->del))(plist->plist_id, name, prop->size, prop->value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "can't release property value");
    } /* end if */

    /* Copy new [possibly unchanged] value into property value */
    H5MM_memcpy(prop->value, prp_value, prop->size);

done:
    /* Free the temporary value buffer */
    if (tmp_value != NULL)
        H5MM_xfree(tmp_value);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__set_plist_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__set_pclass_cb
 PURPOSE
    Internal callback for H5P__do_prop, to set a property's value in a property list.
 USAGE
    herr_t H5P__set_pclass_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to set property in
        const char *name;       IN: Name of property to set
        H5P_genprop_t *prop;    IN: Property to set
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Sets a new value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property class.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__set_pclass_cb(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop, void *_udata)
{
    H5P_prop_set_ud_t *udata     = (H5P_prop_set_ud_t *)_udata; /* User data for callback */
    H5P_genprop_t     *pcopy     = NULL;    /* Copy of property to insert into skip list */
    void              *tmp_value = NULL;    /* Temporary value for property */
    const void        *prp_value = NULL;    /* Property value */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);
    assert(prop->cmp);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Make a copy of the value and pass to 'set' callback */
    if (NULL != prop->set) {
        /* Make a copy of the current value, in case the callback fails */
        if (NULL == (tmp_value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed temporary property value");
        H5MM_memcpy(tmp_value, udata->value, prop->size);

        /* Call user's callback */
        if ((*(prop->set))(plist->plist_id, name, prop->size, tmp_value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set property value");

        /* Set the pointer for copying */
        prp_value = tmp_value;
    } /* end if */
    /* No 'set' callback, just copy value */
    else
        prp_value = udata->value;

    /* Make a copy of the class's property */
    if (NULL == (pcopy = H5P__dup_prop(prop, H5P_PROP_WITHIN_LIST)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");

    H5MM_memcpy(pcopy->value, prp_value, pcopy->size);

    /* Insert the changed property into the property list */
    if (H5P__add_prop(plist->props, pcopy) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert changed property into skip list");

done:
    /* Free the temporary value buffer */
    if (tmp_value != NULL)
        H5MM_xfree(tmp_value);

    /* Cleanup on failure */
    if (ret_value < 0)
        if (pcopy)
            H5P__free_prop(pcopy);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__set_pclass_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P_set
 PURPOSE
    Internal routine to set a property's value in a property list.
 USAGE
    herr_t H5P_set(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to find property in
        const char *name;       IN: Name of property to set
        const void *value;      IN: Pointer to the value for the property
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Sets a new value for a property in a property list.  The property name
    must exist or this routine will fail.  If there is a 'set' callback routine
    registered for this property, the 'value' will be passed to that routine and
    any changes to the 'value' will be used when setting the property value.
    The information pointed at by the 'value' pointer (possibly modified by the
    'set' callback) is copied into the property list value and may be changed
    by the application making the H5Pset call without affecting the property
    value.

        If the 'set' callback routine returns an error, the property value will
    not be modified.  This routine may not be called for zero-sized properties
    and will return an error in that case.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_set(H5P_genplist_t *plist, const char *name, const void *value)
{
    H5P_prop_set_ud_t udata;               /* User data for callback */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(value);

    /* Find the property and set the value */
    udata.value = value;
    if (H5P__do_prop(plist, name, H5P__set_plist_cb, H5P__set_pclass_cb, &udata) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on plist to set value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_set() */

/*--------------------------------------------------------------------------
 NAME
    H5P__class_get
 PURPOSE
    Internal routine to get a property's value from a property class.
 USAGE
    herr_t H5P__class_get(pclass, name, value)
        const H5P_genclass_t *pclass; IN: Property class to find property in
        const char *name;       IN: Name of property to get
        void *value;            IN: Pointer to the value for the property
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Gets the current value for a property in a property class.  The property
    name must exist or this routine will fail.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'get' callback routine registered for this property will _NOT_ be
    called, this routine is designed for internal library use only!

        This routine may not be called for zero-sized properties and will
    return an error in that case.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__class_get(const H5P_genclass_t *pclass, const char *name, void *value)
{
    H5P_genprop_t *prop;                /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pclass);
    assert(name);
    assert(value);

    /* Find property in list */
    if (NULL == (prop = (H5P_genprop_t *)H5SL_search(pclass->props, name)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Copy the property value */
    H5MM_memcpy(value, prop->value, prop->size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__class_get() */

/*--------------------------------------------------------------------------
 NAME
    H5P__class_set
 PURPOSE
    Internal routine to set a property's value in a property class.
 USAGE
    herr_t H5P__class_set(pclass, name, value)
        const H5P_genclass_t *pclass; IN: Property class to find property in
        const char *name;       IN: Name of property to set
        const void *value;      IN: Pointer to the value for the property
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Sets a new value for a property in a property class.  The property name
    must exist or this routine will fail.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The 'set' callback routine registered for this property will _NOT_ be
    called, this routine is designed for internal library use only!

        This routine may not be called for zero-sized properties and will
    return an error in that case.

        The previous value is overwritten, not released in any way.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__class_set(const H5P_genclass_t *pclass, const char *name, const void *value)
{
    H5P_genprop_t *prop;                /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(pclass);
    assert(name);
    assert(value);

    /* Find property in list */
    if (NULL == (prop = (H5P_genprop_t *)H5SL_search(pclass->props, name)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Copy the property value */
    H5MM_memcpy(prop->value, value, prop->size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__class_set() */

/*--------------------------------------------------------------------------
 NAME
    H5P_exist_plist
 PURPOSE
    Internal routine to query the existence of a property in a property list.
 USAGE
    htri_t H5P_exist_plist(plist, name)
        const H5P_genplist_t *plist;  IN: Property list to check
        const char *name;       IN: Name of property to check for
 RETURNS
    Success: Positive if the property exists in the property list, zero
            if the property does not exist.
    Failure: negative value
 DESCRIPTION
        This routine checks if a property exists within a property list.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5P_exist_plist(const H5P_genplist_t *plist, const char *name)
{
    htri_t ret_value = FAIL; /* return value */

    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(plist);
    assert(name);

    /* Check for property in deleted property list */
    if (H5SL_search(plist->del, name) != NULL)
        ret_value = false;
    else {
        /* Check for property in changed property list */
        if (H5SL_search(plist->props, name) != NULL)
            ret_value = true;
        else {
            H5P_genclass_t *tclass; /* Temporary class pointer */

            tclass = plist->pclass;
            while (tclass != NULL) {
                if (H5SL_search(tclass->props, name) != NULL)
                    HGOTO_DONE(true);

                /* Go up to parent class */
                tclass = tclass->parent;
            } /* end while */

            /* If we've reached here, we couldn't find the property */
            ret_value = false;
        } /* end else */
    }     /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_exist_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__exist_pclass
 PURPOSE
    Internal routine to query the existence of a property in a property class.
 USAGE
    herr_t H5P__exist_pclass(pclass, name)
        H5P_genclass_t *pclass;  IN: Property class to check
        const char *name;       IN: Name of property to check for
 RETURNS
    Success: Positive if the property exists in the property class, zero
            if the property does not exist.
    Failure: negative value
 DESCRIPTION
        This routine checks if a property exists within a property class.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5P__exist_pclass(H5P_genclass_t *pclass, const char *name)
{
    htri_t ret_value = FAIL; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(pclass);
    assert(name);

    /* Check for property in property list */
    if (H5SL_search(pclass->props, name) != NULL)
        ret_value = true;
    else {
        H5P_genclass_t *tclass; /* Temporary class pointer */

        tclass = pclass->parent;
        while (tclass != NULL) {
            if (H5SL_search(tclass->props, name) != NULL)
                HGOTO_DONE(true);

            /* Go up to parent class */
            tclass = tclass->parent;
        } /* end while */

        /* If we've reached here, we couldn't find the property */
        ret_value = false;
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__exist_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_size_plist
 PURPOSE
    Internal routine to query the size of a property in a property list.
 USAGE
    herr_t H5P__get_size_plist(plist, name)
        const H5P_genplist_t *plist;  IN: Property list to check
        const char *name;       IN: Name of property to query
        size_t *size;           OUT: Size of property
 RETURNS
    Success: non-negative value
    Failure: negative value
 DESCRIPTION
        This routine retrieves the size of a property's value in bytes.  Zero-
    sized properties are allowed and return a value of 0.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__get_size_plist(const H5P_genplist_t *plist, const char *name, size_t *size)
{
    H5P_genprop_t *prop;                /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    assert(plist);
    assert(name);
    assert(size);

    /* Find property */
    if (NULL == (prop = H5P__find_prop_plist(plist, name)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

    /* Get property size */
    *size = prop->size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_size_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_size_pclass
 PURPOSE
    Internal routine to query the size of a property in a property class.
 USAGE
    herr_t H5P__get_size_pclass(pclass, name)
        H5P_genclass_t *pclass; IN: Property class to check
        const char *name;       IN: Name of property to query
        size_t *size;           OUT: Size of property
 RETURNS
    Success: non-negative value
    Failure: negative value
 DESCRIPTION
        This routine retrieves the size of a property's value in bytes.  Zero-
    sized properties are allowed and return a value of 0.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__get_size_pclass(H5P_genclass_t *pclass, const char *name, size_t *size)
{
    H5P_genprop_t *prop;                /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);
    assert(name);
    assert(size);

    /* Find property */
    if ((prop = H5P__find_prop_pclass(pclass, name)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

    /* Get property size */
    *size = prop->size;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_size_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_nprops_plist
 PURPOSE
    Internal routine to query the number of properties in a property list
 USAGE
    herr_t H5P__get_nprops_plist(plist, nprops)
        H5P_genplist_t *plist;  IN: Property list to check
        size_t *nprops;         OUT: Number of properties in the property list
 RETURNS
    Success: non-negative value
    Failure: negative value
 DESCRIPTION
        This routine retrieves the number of a properties in a property list.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__get_nprops_plist(const H5P_genplist_t *plist, size_t *nprops)
{
    FUNC_ENTER_PACKAGE_NOERR

    assert(plist);
    assert(nprops);

    /* Get property size */
    *nprops = plist->nprops;

    FUNC_LEAVE_NOAPI(SUCCEED)
} /* H5P__get_nprops_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P_get_nprops_pclass
 PURPOSE
    Internal routine to query the number of properties in a property class
 USAGE
    herr_t H5P_get_nprops_pclass(pclass, nprops)
        H5P_genclass_t *pclass;  IN: Property class to check
        size_t *nprops;         OUT: Number of properties in the property list
        bool recurse;        IN: Include properties in parent class(es) also
 RETURNS
    Success: non-negative value (can't fail)
    Failure: negative value
 DESCRIPTION
    This routine retrieves the number of a properties in a property class.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_get_nprops_pclass(const H5P_genclass_t *pclass, size_t *nprops, bool recurse)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(pclass);
    assert(nprops);

    /* Get number of properties */
    *nprops = pclass->nprops;

    /* Check if the class is derived, and walk up the chain, if so */
    if (recurse)
        while (pclass->parent != NULL) {
            pclass = pclass->parent;
            *nprops += pclass->nprops;
        } /* end while */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_get_nprops_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__cmp_prop
 PURPOSE
    Internal routine to compare two generic properties
 USAGE
    int H5P__cmp_prop(prop1, prop2)
        H5P_genprop_t *prop1;    IN: 1st property to compare
        H5P_genprop_t *prop1;    IN: 2nd property to compare
 RETURNS
    Success: negative if prop1 "less" than prop2, positive if prop1 "greater"
        than prop2, zero if prop1 is "equal" to prop2
    Failure: can't fail
 DESCRIPTION
        This function compares two generic properties together to see if
    they are the same property.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__cmp_prop(const H5P_genprop_t *prop1, const H5P_genprop_t *prop2)
{
    int cmp_value;     /* Value from comparison */
    int ret_value = 0; /* return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(prop1);
    assert(prop2);

    /* Check the name */
    if ((cmp_value = strcmp(prop1->name, prop2->name)) != 0)
        HGOTO_DONE(cmp_value);

    /* Check the size of properties */
    if (prop1->size < prop2->size)
        HGOTO_DONE(-1);
    if (prop1->size > prop2->size)
        HGOTO_DONE(1);

    /* Check if they both have the same 'create' callback */
    if (prop1->create == NULL && prop2->create != NULL)
        HGOTO_DONE(-1);
    if (prop1->create != NULL && prop2->create == NULL)
        HGOTO_DONE(1);
    if (prop1->create != prop2->create)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'set' callback */
    if (prop1->set == NULL && prop2->set != NULL)
        HGOTO_DONE(-1);
    if (prop1->set != NULL && prop2->set == NULL)
        HGOTO_DONE(1);
    if (prop1->set != prop2->set)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'get' callback */
    if (prop1->get == NULL && prop2->get != NULL)
        HGOTO_DONE(-1);
    if (prop1->get != NULL && prop2->get == NULL)
        HGOTO_DONE(1);
    if (prop1->get != prop2->get)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'encode' callback */
    if (prop1->encode == NULL && prop2->encode != NULL)
        HGOTO_DONE(-1);
    if (prop1->encode != NULL && prop2->encode == NULL)
        HGOTO_DONE(1);
    if (prop1->encode != prop2->encode)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'decode' callback */
    if (prop1->decode == NULL && prop2->decode != NULL)
        HGOTO_DONE(-1);
    if (prop1->decode != NULL && prop2->decode == NULL)
        HGOTO_DONE(1);
    if (prop1->decode != prop2->decode)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'delete' callback */
    if (prop1->del == NULL && prop2->del != NULL)
        HGOTO_DONE(-1);
    if (prop1->del != NULL && prop2->del == NULL)
        HGOTO_DONE(1);
    if (prop1->del != prop2->del)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'copy' callback */
    if (prop1->copy == NULL && prop2->copy != NULL)
        HGOTO_DONE(-1);
    if (prop1->copy != NULL && prop2->copy == NULL)
        HGOTO_DONE(1);
    if (prop1->copy != prop2->copy)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'compare' callback */
    if (prop1->cmp == NULL && prop2->cmp != NULL)
        HGOTO_DONE(-1);
    if (prop1->cmp != NULL && prop2->cmp == NULL)
        HGOTO_DONE(1);
    if (prop1->cmp != prop2->cmp)
        HGOTO_DONE(-1);

    /* Check if they both have the same 'close' callback */
    if (prop1->close == NULL && prop2->close != NULL)
        HGOTO_DONE(-1);
    if (prop1->close != NULL && prop2->close == NULL)
        HGOTO_DONE(1);
    if (prop1->close != prop2->close)
        HGOTO_DONE(-1);

    /* Check if they both have values allocated (or not allocated) */
    if (prop1->value == NULL && prop2->value != NULL)
        HGOTO_DONE(-1);
    if (prop1->value != NULL && prop2->value == NULL)
        HGOTO_DONE(1);
    if (prop1->value != NULL) {
        /* Call comparison routine */
        if ((cmp_value = prop1->cmp(prop1->value, prop2->value, prop1->size)) != 0)
            HGOTO_DONE(cmp_value);
    } /* end if */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__cmp_prop() */

/*--------------------------------------------------------------------------
 NAME
    H5P__cmp_class
 PURPOSE
    Internal routine to compare two generic property classes
 USAGE
    int H5P__cmp_class(pclass1, pclass2)
        H5P_genclass_t *pclass1;    IN: 1st property class to compare
        H5P_genclass_t *pclass2;    IN: 2nd property class to compare
 RETURNS
    Success: negative if class1 "less" than class2, positive if class1 "greater"
        than class2, zero if class1 is "equal" to class2
    Failure: can't fail
 DESCRIPTION
        This function compares two generic property classes together to see if
    they are the same class.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5P__cmp_class(const H5P_genclass_t *pclass1, const H5P_genclass_t *pclass2)
{
    H5SL_node_t *tnode1, *tnode2; /* Temporary pointer to property nodes */
    int          cmp_value;       /* Value from comparison */
    int          ret_value = 0;   /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(pclass1);
    assert(pclass2);

    /* Use the revision number to quickly check for identical classes */
    if (pclass1->revision == pclass2->revision)
        HGOTO_DONE(0);

    /* Check the name */
    if ((cmp_value = strcmp(pclass1->name, pclass2->name)) != 0)
        HGOTO_DONE(cmp_value);

    /* Check the number of properties */
    if (pclass1->nprops < pclass2->nprops)
        HGOTO_DONE(-1);
    if (pclass1->nprops > pclass2->nprops)
        HGOTO_DONE(1);

    /* Check the number of property lists created from the class */
    if (pclass1->plists < pclass2->plists)
        HGOTO_DONE(-1);
    if (pclass1->plists > pclass2->plists)
        HGOTO_DONE(1);

    /* Check the number of classes derived from the class */
    if (pclass1->classes < pclass2->classes)
        HGOTO_DONE(-1);
    if (pclass1->classes > pclass2->classes)
        HGOTO_DONE(1);

    /* Check the number of ID references open on the class */
    if (pclass1->ref_count < pclass2->ref_count)
        HGOTO_DONE(-1);
    if (pclass1->ref_count > pclass2->ref_count)
        HGOTO_DONE(1);

    /* Check the property list types */
    if (pclass1->type < pclass2->type)
        HGOTO_DONE(-1);
    if (pclass1->type > pclass2->type)
        HGOTO_DONE(1);

    /* Check whether they are deleted or not */
    if (pclass1->deleted < pclass2->deleted)
        HGOTO_DONE(-1);
    if (pclass1->deleted > pclass2->deleted)
        HGOTO_DONE(1);

    /* Check whether they have creation callback functions & data */
    if (pclass1->create_func == NULL && pclass2->create_func != NULL)
        HGOTO_DONE(-1);
    if (pclass1->create_func != NULL && pclass2->create_func == NULL)
        HGOTO_DONE(1);
    if (pclass1->create_func != pclass2->create_func)
        HGOTO_DONE(-1);
    if (pclass1->create_data < pclass2->create_data)
        HGOTO_DONE(-1);
    if (pclass1->create_data > pclass2->create_data)
        HGOTO_DONE(1);

    /* Check whether they have close callback functions & data */
    if (pclass1->close_func == NULL && pclass2->close_func != NULL)
        HGOTO_DONE(-1);
    if (pclass1->close_func != NULL && pclass2->close_func == NULL)
        HGOTO_DONE(1);
    if (pclass1->close_func != pclass2->close_func)
        HGOTO_DONE(-1);
    if (pclass1->close_data < pclass2->close_data)
        HGOTO_DONE(-1);
    if (pclass1->close_data > pclass2->close_data)
        HGOTO_DONE(1);

    /* Cycle through the properties and compare them also */
    tnode1 = H5SL_first(pclass1->props);
    tnode2 = H5SL_first(pclass2->props);
    while (tnode1 || tnode2) {
        H5P_genprop_t *prop1, *prop2; /* Property for node */

        /* Check if they both have properties in this skip list node */
        if (tnode1 == NULL && tnode2 != NULL)
            HGOTO_DONE(-1);
        if (tnode1 != NULL && tnode2 == NULL)
            HGOTO_DONE(1);

        /* Compare the two properties */
        prop1 = (H5P_genprop_t *)H5SL_item(tnode1);
        prop2 = (H5P_genprop_t *)H5SL_item(tnode2);
        if ((cmp_value = H5P__cmp_prop(prop1, prop2)) != 0)
            HGOTO_DONE(cmp_value);

        /* Advance the pointers */
        tnode1 = H5SL_next(tnode1);
        tnode2 = H5SL_next(tnode2);
    } /* end while */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__cmp_class() */

/*--------------------------------------------------------------------------
 NAME
    H5P__cmp_plist_cb
 PURPOSE
    Internal callback routine when iterating over properties in property list
    to compare them for equality
 USAGE
    int H5P__cmp_plist_cb(prop, udata)
        H5P_genprop_t *prop;        IN: Pointer to the property
        void *udata;                IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns whether to continue (H5_ITER_CONT) or stop (H5_ITER_STOP)
            iterating over the property lists.
    Failure: Negative value (H5_ITER_ERROR)
 DESCRIPTION
    This routine compares a property from one property list (the one being
    iterated over, to a property from the second property list (which is
    looked up).  Iteration is stopped if the comparison is non-equal.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__cmp_plist_cb(H5P_genprop_t *prop, void *_udata)
{
    H5P_plist_cmp_ud_t *udata = (H5P_plist_cmp_ud_t *)_udata; /* Pointer to user data */
    htri_t              prop2_exist; /* Whether the property exists in the second property list */
    int                 ret_value = H5_ITER_CONT; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(prop);
    assert(udata);

    /* Check if the property exists in the second property list */
    if ((prop2_exist = H5P_exist_plist(udata->plist2, prop->name)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, H5_ITER_ERROR, "can't lookup existence of property?");
    if (prop2_exist) {
        const H5P_genprop_t *prop2; /* Pointer to property in second plist */

        /* Look up same property in second property list */
        if (NULL == (prop2 = H5P__find_prop_plist(udata->plist2, prop->name)))
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, H5_ITER_ERROR, "property doesn't exist");

        /* Compare the two properties */
        if ((udata->cmp_value = H5P__cmp_prop(prop, prop2)) != 0)
            HGOTO_DONE(H5_ITER_STOP);
    } /* end if */
    else {
        /* Property exists in first list, but not second */
        udata->cmp_value = 1;
        HGOTO_DONE(H5_ITER_STOP);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__cmp_plist_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__cmp_plist
 PURPOSE
    Internal routine to compare two generic property lists
 USAGE
    herr_t H5P__cmp_plist(plist1, plist2, cmp_ret)
        H5P_genplist_t *plist1;    IN: 1st property list to compare
        H5P_genplist_t *plist2;    IN: 2nd property list to compare
        int *cmp_ret;              OUT: Comparison value for two property lists
                                        Negative if list1 "less" than list2,
                                        positive if list1 "greater" than list2,
                                        zero if list1 is "equal" to list2
 RETURNS
    Success: non-negative value
    Failure: negative value
 DESCRIPTION
        This function compares two generic property lists together to see if
    they are equal.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__cmp_plist(const H5P_genplist_t *plist1, const H5P_genplist_t *plist2, int *cmp_ret)
{
    H5P_plist_cmp_ud_t udata;               /* User data for callback */
    int                idx       = 0;       /* Index of property to begin with */
    herr_t             ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(plist1);
    assert(plist2);
    assert(cmp_ret);

    /* Check the number of properties */
    if (plist1->nprops < plist2->nprops) {
        *cmp_ret = -1;
        HGOTO_DONE(SUCCEED);
    } /* end if */
    if (plist1->nprops > plist2->nprops) {
        *cmp_ret = 1;
        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Check whether they've been initialized */
    if (plist1->class_init < plist2->class_init) {
        *cmp_ret = -1;
        HGOTO_DONE(SUCCEED);
    } /* end if */
    if (plist1->class_init > plist2->class_init) {
        *cmp_ret = 1;
        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Set up iterator callback info */
    udata.cmp_value = 0;
    udata.plist2    = plist2;

    /* Iterate over properties in first property list */
    if ((ret_value = H5P__iterate_plist(plist1, true, &idx, H5P__cmp_plist_cb, &udata)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "unable to iterate over list");
    if (ret_value != 0) {
        *cmp_ret = udata.cmp_value;
        HGOTO_DONE(SUCCEED);
    } /* end if */

    /* Check the parent classes */
    if ((*cmp_ret = H5P__cmp_class(plist1->pclass, plist2->pclass)) != 0)
        HGOTO_DONE(SUCCEED);

    /* Property lists must be equal, set comparison value to 0 */
    *cmp_ret = 0;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__cmp_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P_class_isa
 PURPOSE
    Internal routine to query whether a property class is the same as another
    class.
 USAGE
    htri_t H5P_class_isa(pclass1, pclass2)
        H5P_genclass_t *pclass1;   IN: Property class to check
        H5P_genclass_t *pclass2;   IN: Property class to compare with
 RETURNS
    Success: true (1) or false (0)
    Failure: negative value
 DESCRIPTION
    This routine queries whether a property class is the same as another class,
    and walks up the hierarchy of derived classes, checking if the first class
    is derived from the second class also.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5P_class_isa(const H5P_genclass_t *pclass1, const H5P_genclass_t *pclass2)
{
    htri_t ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(pclass1);
    assert(pclass2);

    /* Compare property classes */
    if (H5P__cmp_class(pclass1, pclass2) == 0) {
        HGOTO_DONE(true);
    }
    else {
        /* Check if the class is derived, and walk up the chain, if so */
        if (pclass1->parent != NULL)
            ret_value = H5P_class_isa(pclass1->parent, pclass2);
        else
            HGOTO_DONE(false);
    } /* end else */

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_class_isa() */

/*--------------------------------------------------------------------------
 NAME
    H5P_isa_class
 PURPOSE
    Internal routine to query whether a property list is a certain class
 USAGE
    hid_t H5P_isa_class(plist_id, pclass_id)
        hid_t plist_id;         IN: Property list to query
        hid_t pclass_id;        IN: Property class to query
 RETURNS
    Success: true (1) or false (0)
    Failure: negative
 DESCRIPTION
    This routine queries whether a property list is a member of the property
    list class.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This function is special in that it is an internal library function, but
    accepts hid_t's as parameters.  Since it is used in basically the same way
    as the H5I functions, this should be OK.  Don't make more library functions
    which accept hid_t's without thorough discussion. -QAK
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
htri_t
H5P_isa_class(hid_t plist_id, hid_t pclass_id)
{
    H5P_genplist_t *plist;            /* Property list to query */
    H5P_genclass_t *pclass;           /* Property list class */
    htri_t          ret_value = FAIL; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Check arguments. */
    if (NULL == (plist = (H5P_genplist_t *)H5I_object_verify(plist_id, H5I_GENPROP_LST)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property list");
    if (NULL == (pclass = (H5P_genclass_t *)H5I_object_verify(pclass_id, H5I_GENPROP_CLS)))
        HGOTO_ERROR(H5E_ARGS, H5E_BADTYPE, FAIL, "not a property class");

    /* Compare the property list's class against the other class */
    if ((ret_value = H5P_class_isa(plist->pclass, pclass)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTREGISTER, FAIL, "unable to compare property list classes");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_isa_class() */

/*--------------------------------------------------------------------------
 NAME
    H5P_object_verify
 PURPOSE
    Internal routine to query whether a property list is a certain class and
        retrieve the property list object associated with it.
 USAGE
    void *H5P_object_verify(plist_id, pclass_id)
        hid_t plist_id;         IN: Property list to query
        hid_t pclass_id;        IN: Property class to query
 RETURNS
    Success: valid pointer to a property list object
    Failure: NULL
 DESCRIPTION
    This routine queries whether a property list is member of a certain class
    and retrieves the property list object associated with it.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    This function is special in that it is an internal library function, but
    accepts hid_t's as parameters.  Since it is used in basically the same way
    as the H5I functions, this should be OK.  Don't make more library functions
    which accept hid_t's without thorough discussion. -QAK

    This function is similar (in spirit) to H5I_object_verify()
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genplist_t *
H5P_object_verify(hid_t plist_id, hid_t pclass_id)
{
    H5P_genplist_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI(NULL)

    /* Compare the property list's class against the other class */
    if (H5P_isa_class(plist_id, pclass_id) != true)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOMPARE, NULL, "property list is not a member of the class");

    /* Get the plist structure */
    if (NULL == (ret_value = (H5P_genplist_t *)H5I_object(plist_id)))
        HGOTO_ERROR(H5E_ID, H5E_BADID, NULL, "can't find object for ID");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_object_verify() */

/*--------------------------------------------------------------------------
 NAME
    H5P__iterate_plist_cb
 PURPOSE
    Internal callback routine when iterating over properties in property list
 USAGE
    int H5P__iterate_plist_cb(item, key, udata)
        void *item;                 IN: Pointer to the property
        void *key;                  IN: Pointer to the property's name
        void *udata;            IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns the return value of the last call to ITER_FUNC
 DESCRIPTION
    This routine calls the actual callback routine for the property in the
property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__iterate_plist_cb(void *_item, void *_key, void *_udata)
{
    H5P_genprop_t       *item      = (H5P_genprop_t *)_item;        /* Pointer to the property */
    char                *key       = (char *)_key;                  /* Pointer to the property's name */
    H5P_iter_plist_ud_t *udata     = (H5P_iter_plist_ud_t *)_udata; /* Pointer to user data */
    int                  ret_value = H5_ITER_CONT;                  /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(item);
    assert(key);

    /* Check if we've found the correctly indexed property */
    if (*udata->curr_idx_ptr >= udata->prev_idx) {
        /* Call the callback function */
        ret_value = (*udata->cb_func)(item, udata->udata);
        if (ret_value != 0)
            HGOTO_DONE(ret_value);
    } /* end if */

    /* Increment the current index */
    (*udata->curr_idx_ptr)++;

    /* Add property name to 'seen' list */
    if (H5SL_insert(udata->seen, key, key) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, H5_ITER_ERROR, "can't insert property into 'seen' skip list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__iterate_plist_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__iterate_plist_pclass_cb
 PURPOSE
    Internal callback routine when iterating over properties in property class
 USAGE
    int H5P__iterate_plist_pclass_cb(item, key, udata)
        void *item;                 IN: Pointer to the property
        void *key;                  IN: Pointer to the property's name
        void *udata;            IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns the return value of the last call to ITER_FUNC
 DESCRIPTION
    This routine verifies that the property hasn't already been seen or was
deleted, and then chains to the property list callback.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__iterate_plist_pclass_cb(void *_item, void *_key, void *_udata)
{
    H5P_genprop_t       *item      = (H5P_genprop_t *)_item;        /* Pointer to the property */
    char                *key       = (char *)_key;                  /* Pointer to the property's name */
    H5P_iter_plist_ud_t *udata     = (H5P_iter_plist_ud_t *)_udata; /* Pointer to user data */
    int                  ret_value = H5_ITER_CONT;                  /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(item);
    assert(key);

    /* Only call iterator callback for properties we haven't seen
     * before and that haven't been deleted.
     */
    if (NULL == H5SL_search(udata->seen, key) && NULL == H5SL_search(udata->plist->del, key))
        ret_value = H5P__iterate_plist_cb(item, key, udata);

    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__iterate_plist_pclass_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__iterate_plist
 PURPOSE
    Internal routine to iterate over the properties in a property list
 USAGE
    int H5P__iterate_plist(plist, iter_all_prop, idx, cb_func, iter_data)
        const H5P_genplist_t *plist; IN: Property list to iterate over
        bool iter_all_prop;      IN: Whether to iterate over all properties
                                        (true), or just non-default (i.e. changed)
                                        properties (false).
        int *idx;                   IN/OUT: Index of the property to begin with
        H5P_iterate_t cb_func;    IN: Function pointer to function to be
                                        called with each property iterated over.
        void *iter_data;            IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns the return value of the last call to ITER_FUNC if it was
                non-zero, or zero if all properties have been processed.
    Failure: negative value
 DESCRIPTION
    This routine iterates over the properties in the property object specified
with PLIST_ID.  For each property in the object, the ITER_DATA and some
additional information, specified below, are passed to the ITER_FUNC function.
The iteration begins with the IDX property in the object and the next element
to be processed by the operator is returned in IDX.  If IDX is NULL, then the
iterator starts at the first property; since no stopping point is returned in
this case, the iterator cannot be restarted if one of the calls to its operator
returns non-zero.

The prototype for H5P_iterate_t is:
    typedef herr_t (*H5P_iterate_t)(hid_t id, const char *name, void *iter_data);
The operation receives the property list or class identifier for the object
being iterated over, ID, the name of the current property within the object,
NAME, and the pointer to the operator data passed in to H5Piterate, ITER_DATA.

The return values from an operator are:
    Zero causes the iterator to continue, returning zero when all properties
        have been processed.
    Positive causes the iterator to immediately return that positive value,
        indicating short-circuit success. The iterator can be restarted at the
        index of the next property.
    Negative causes the iterator to immediately return that value, indicating
        failure. The iterator can be restarted at the index of the next
        property.

H5Piterate assumes that the properties in the object identified by ID remains
unchanged through the iteration.  If the membership changes during the
iteration, the function's behavior is undefined.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5P__iterate_plist(const H5P_genplist_t *plist, bool iter_all_prop, int *idx, H5P_iterate_int_t cb_func,
                   void *udata)
{
    H5P_genclass_t     *tclass;           /* Temporary class pointer */
    H5P_iter_plist_ud_t udata_int;        /* User data for skip list iterator */
    H5SL_t             *seen      = NULL; /* Skip list to hold names of properties already seen */
    int                 curr_idx  = 0;    /* Current iteration index */
    int                 ret_value = 0;    /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(idx);
    assert(cb_func);

    /* Create the skip list to hold names of properties already seen */
    if (NULL == (seen = H5SL_create(H5SL_TYPE_STR, NULL)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "can't create skip list for seen properties");

    /* Set up iterator callback info */
    udata_int.plist        = plist;
    udata_int.cb_func      = cb_func;
    udata_int.udata        = udata;
    udata_int.seen         = seen;
    udata_int.curr_idx_ptr = &curr_idx;
    udata_int.prev_idx     = *idx;

    /* Iterate over properties in property list proper */
    /* (Will be only the non-default (i.e. changed) properties) */
    ret_value = H5SL_iterate(plist->props, H5P__iterate_plist_cb, &udata_int);
    if (ret_value != 0)
        HGOTO_DONE(ret_value);

    /* Check for iterating over all properties, or just non-default ones */
    if (iter_all_prop) {
        /* Walk up the class hierarchy */
        tclass = plist->pclass;
        while (tclass != NULL) {
            /* Iterate over properties in property list class */
            ret_value = H5SL_iterate(tclass->props, H5P__iterate_plist_pclass_cb, &udata_int);
            if (ret_value != 0)
                HGOTO_DONE(ret_value);

            /* Go up to parent class */
            tclass = tclass->parent;
        } /* end while */
    }     /* end if */

done:
    /* Set the index we stopped at */
    *idx = curr_idx;

    /* Release the skip list of 'seen' properties */
    if (seen != NULL)
        H5SL_close(seen);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__iterate_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__iterate_pclass_cb
 PURPOSE
    Internal callback routine when iterating over properties in property list
    class
 USAGE
    int H5P__iterate_pclass_cb(item, key, udata)
        void *item;                 IN: Pointer to the property
        void *key;                  IN: Pointer to the property's name
        void *udata;            IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns the return value of the last call to ITER_FUNC
 DESCRIPTION
    This routine calls the actual callback routine for the property in the
property list class.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static int
H5P__iterate_pclass_cb(void *_item, void H5_ATTR_NDEBUG_UNUSED *_key, void *_udata)
{
    H5P_genprop_t        *item      = (H5P_genprop_t *)_item;         /* Pointer to the property */
    H5P_iter_pclass_ud_t *udata     = (H5P_iter_pclass_ud_t *)_udata; /* Pointer to user data */
    int                   ret_value = 0;                              /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(item);
    assert((char *)_key);

    /* Check if we've found the correctly indexed property */
    if (*udata->curr_idx_ptr >= udata->prev_idx) {
        /* Call the callback function */
        ret_value = (*udata->cb_func)(item, udata->udata);
        if (ret_value != 0)
            HGOTO_DONE(ret_value);
    } /* end if */

    /* Increment the current index */
    (*udata->curr_idx_ptr)++;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__iterate_pclass_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__iterate_pclass
 PURPOSE
    Internal routine to iterate over the properties in a property class
 USAGE
    herr_t H5P__iterate_pclass(pclass, idx, cb_func, iter_data)
        const H5P_genpclass_t *pclass; IN: Property list class to iterate over
        int *idx;                   IN/OUT: Index of the property to begin with
        H5P_iterate_t cb_func;    IN: Function pointer to function to be
                                        called with each property iterated over.
        void *iter_data;            IN/OUT: Pointer to iteration data from user
 RETURNS
    Success: Returns the return value of the last call to ITER_FUNC if it was
                non-zero, or zero if all properties have been processed.
    Failure: negative value
 DESCRIPTION
    This routine iterates over the properties in the property object specified
with PCLASS_ID.  For each property in the object, the ITER_DATA and some
additional information, specified below, are passed to the ITER_FUNC function.
The iteration begins with the IDX property in the object and the next element
to be processed by the operator is returned in IDX.  If IDX is NULL, then the
iterator starts at the first property; since no stopping point is returned in
this case, the iterator cannot be restarted if one of the calls to its operator
returns non-zero.

The prototype for H5P_iterate_t is:
    typedef herr_t (*H5P_iterate_t)(hid_t id, const char *name, void *iter_data);
The operation receives the property list or class identifier for the object
being iterated over, ID, the name of the current property within the object,
NAME, and the pointer to the operator data passed in to H5Piterate, ITER_DATA.

The return values from an operator are:
    Zero causes the iterator to continue, returning zero when all properties
        have been processed.
    Positive causes the iterator to immediately return that positive value,
        indicating short-circuit success. The iterator can be restarted at the
        index of the next property.
    Negative causes the iterator to immediately return that value, indicating
        failure. The iterator can be restarted at the index of the next
        property.

H5Piterate assumes that the properties in the object identified by ID remains
unchanged through the iteration.  If the membership changes during the
iteration, the function's behavior is undefined.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
int
H5P__iterate_pclass(const H5P_genclass_t *pclass, int *idx, H5P_iterate_int_t cb_func, void *udata)
{
    H5P_iter_pclass_ud_t udata_int;     /* User data for skip list iterator */
    int                  curr_idx  = 0; /* Current iteration index */
    int                  ret_value = 0; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    /* Sanity check */
    assert(pclass);
    assert(idx);
    assert(cb_func);

    /* Set up iterator callback info */
    udata_int.cb_func      = cb_func;
    udata_int.udata        = udata;
    udata_int.curr_idx_ptr = &curr_idx;
    udata_int.prev_idx     = *idx;

    /* Iterate over properties in property list class proper */
    ret_value = H5SL_iterate(pclass->props, H5P__iterate_pclass_cb, &udata_int);
    if (ret_value != 0)
        HGOTO_DONE(ret_value);

done:
    /* Set the index we stopped at */
    *idx = curr_idx;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__iterate_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__peek_cb
 PURPOSE
    Internal callback for H5P__do_prop, to peek at a property's value in a property list.
 USAGE
    herr_t H5P__peek_plist_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to peek property in
        const char *name;       IN: Name of property to peek
        H5P_genprop_t *prop;    IN: Property to peek
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Peeks at a new value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property list and when it's found
        for the property class.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__peek_cb(H5P_genplist_t H5_ATTR_NDEBUG_UNUSED *plist, const char H5_ATTR_NDEBUG_UNUSED *name,
             H5P_genprop_t *prop, void *_udata)
{
    H5P_prop_get_ud_t *udata     = (H5P_prop_get_ud_t *)_udata; /* User data for callback */
    herr_t             ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Make a (shallow) copy of the value */
    H5MM_memcpy(udata->value, prop->value, prop->size);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__peek_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P_peek
 PURPOSE
    Internal routine to look at the value of a property in a property list.
 USAGE
    herr_t H5P_peek(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to check
        const char *name;       IN: Name of property to query
        void *value;            OUT: Pointer to the buffer for the property value
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Retrieves a "shallow" copy of the value for a property in a property
    list.  The property name must exist or this routine will fail.  If there
    is a 'get' callback routine registered for this property, it is _NOT_
    called.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        This routine may not be called for zero-sized properties and will
    return an error in that case.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_peek(H5P_genplist_t *plist, const char *name, void *value)
{
    H5P_prop_get_ud_t udata;               /* User data for callback */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(value);

    /* Find the property and peek at the value */
    udata.value = value;
    if (H5P__do_prop(plist, name, H5P__peek_cb, H5P__peek_cb, &udata) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on plist to peek at value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_peek() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_cb
 PURPOSE
    Internal callback for H5P__do_prop, to get a property's value in a property list.
 USAGE
    herr_t H5P__get_plist_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to get property in
        const char *name;       IN: Name of property to get
        H5P_genprop_t *prop;    IN: Property to get
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Gets a new value for a property in a property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
    Called when the property is found in the property list and when it's found
        for the property class.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__get_cb(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop, void *_udata)
{
    H5P_prop_get_ud_t *udata     = (H5P_prop_get_ud_t *)_udata; /* User data for callback */
    void              *tmp_value = NULL;                        /* Temporary value for property */
    herr_t             ret_value = SUCCEED;                     /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Check for property size >0 */
    if (0 == prop->size)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, FAIL, "property has zero size");

    /* Call the 'get' callback, if there is one */
    if (NULL != prop->get) {
        /* Make a copy of the current value, in case the callback fails */
        if (NULL == (tmp_value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed temporary property value");
        H5MM_memcpy(tmp_value, prop->value, prop->size);

        /* Call user's callback */
        if ((*(prop->get))(plist->plist_id, name, prop->size, tmp_value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't set property value");

        /* Copy new [possibly unchanged] value into return value */
        H5MM_memcpy(udata->value, tmp_value, prop->size);
    } /* end if */
    /* No 'get' callback, just copy value */
    else
        H5MM_memcpy(udata->value, prop->value, prop->size);

done:
    /* Free the temporary value buffer */
    if (tmp_value)
        H5MM_xfree(tmp_value);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P_get
 PURPOSE
    Internal routine to query the value of a property in a property list.
 USAGE
    herr_t H5P_get(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to check
        const char *name;       IN: Name of property to query
        void *value;            OUT: Pointer to the buffer for the property value
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Retrieves a copy of the value for a property in a property list.  The
    property name must exist or this routine will fail.  If there is a
    'get' callback routine registered for this property, the copy of the
    value of the property will first be passed to that routine and any changes
    to the copy of the value will be used when returning the property value
    from this routine.
        If the 'get' callback routine returns an error, 'value' will not be
    modified and this routine will return an error.  This routine may not be
    called for zero-sized properties.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_get(H5P_genplist_t *plist, const char *name, void *value)
{
    H5P_prop_get_ud_t udata;               /* User data for callback */
    herr_t            ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(value);

    /* Find the property and get the value */
    udata.value = value;
    if (H5P__do_prop(plist, name, H5P__get_cb, H5P__get_cb, &udata) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on plist to get value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_get() */

/*--------------------------------------------------------------------------
 NAME
    H5P__del_plist_cb
 PURPOSE
    Internal callback for H5P__do_prop, to remove a property's value in a property list.
 USAGE
    herr_t H5P__del_plist_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to remove property from
        const char *name;       IN: Name of property to remove
        H5P_genprop_t *prop;    IN: Property to remove
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Remove a property in a property list.  Called when the
    property is found in the property list.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__del_plist_cb(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop, void H5_ATTR_UNUSED *_udata)
{
    char  *del_name  = NULL;    /* Pointer to deleted name */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Pass value to 'close' callback, if it exists */
    if (NULL != prop->del) {
        /* Call user's callback */
        if ((*(prop->del))(plist->plist_id, name, prop->size, prop->value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTFREE, FAIL, "can't release property value");
    } /* end if */

    /* Duplicate string for insertion into new deleted property skip list */
    if (NULL == (del_name = H5MM_xstrdup(name)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* Insert property name into deleted list */
    if (H5SL_insert(plist->del, del_name, del_name) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into deleted skip list");

    /* Remove the property from the skip list */
    if (NULL == H5SL_remove(plist->props, prop->name))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "can't remove property from skip list");

    /* Free the property, ignoring return value, nothing we can do */
    H5P__free_prop(prop);

    /* Decrement the number of properties in list */
    plist->nprops--;

done:
    /* Error cleanup */
    if (ret_value < 0)
        if (del_name)
            H5MM_xfree(del_name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__del_plist_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P__del_pclass_cb
 PURPOSE
    Internal callback for H5P__do_prop, to remove a property's value in a property list.
 USAGE
    herr_t H5P__del_pclass_cb(plist, name, value)
        H5P_genplist_t *plist;  IN: Property list to remove property from
        const char *name;       IN: Name of property to remove
        H5P_genprop_t *prop;    IN: Property to remove
        void *udata;            IN: User data for operation
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Remove a property in a property list.  Called when the
    property is found in the property class.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
static herr_t
H5P__del_pclass_cb(H5P_genplist_t *plist, const char *name, H5P_genprop_t *prop, void H5_ATTR_UNUSED *_udata)
{
    char  *del_name  = NULL;    /* Pointer to deleted name */
    void  *tmp_value = NULL;    /* Temporary value for property */
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(plist);
    assert(name);
    assert(prop);

    /* Pass value to 'del' callback, if it exists */
    if (NULL != prop->del) {
        /* Allocate space for a temporary copy of the property value */
        if (NULL == (tmp_value = H5MM_malloc(prop->size)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL,
                        "memory allocation failed for temporary property value");
        H5MM_memcpy(tmp_value, prop->value, prop->size);

        /* Call user's callback */
        if ((*(prop->del))(plist->plist_id, name, prop->size, tmp_value) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "can't close property value");
    } /* end if */

    /* Duplicate string for insertion into new deleted property skip list */
    if (NULL == (del_name = H5MM_xstrdup(name)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTALLOC, FAIL, "memory allocation failed");

    /* Insert property name into deleted list */
    if (H5SL_insert(plist->del, del_name, del_name) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into deleted skip list");

    /* Decrement the number of properties in list */
    plist->nprops--;

done:
    /* Free the temporary value buffer */
    if (tmp_value)
        H5MM_xfree(tmp_value);

    /* Error cleanup */
    if (ret_value < 0)
        if (del_name)
            H5MM_xfree(del_name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__del_pclass_cb() */

/*--------------------------------------------------------------------------
 NAME
    H5P_remove
 PURPOSE
    Internal routine to remove a property from a property list.
 USAGE
    herr_t H5P_remove(plist, name)
        H5P_genplist_t *plist;  IN: Property list to modify
        const char *name;       IN: Name of property to remove
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Removes a property from a property list.  Both properties which were
    in existence when the property list was created (i.e. properties registered
    with H5Pregister2) and properties added to the list after it was created
    (i.e. added with H5Pinsert2) may be removed from a property list.
    Properties do not need to be removed a property list before the list itself
    is closed, they will be released automatically when H5Pclose is called.
    The 'close' callback for this property is called before the property is
    release, if the callback exists.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_remove(H5P_genplist_t *plist, const char *name)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI(FAIL)

    /* Sanity check */
    assert(plist);
    assert(name);

    /* Find the property and get the value */
    if (H5P__do_prop(plist, name, H5P__del_plist_cb, H5P__del_pclass_cb, NULL) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTOPERATE, FAIL, "can't operate on plist to remove value");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_remove() */

/*--------------------------------------------------------------------------
 NAME
    H5P__copy_prop_plist
 PURPOSE
    Internal routine to copy a property from one list to another
 USAGE
    herr_t H5P__copy_prop_plist(dst_plist, src_plist, name)
        hid_t dst_id;               IN: ID of destination property list or class
        hid_t src_id;               IN: ID of source property list or class
        const char *name;           IN: Name of property to copy
 RETURNS
    Success: non-negative value.
    Failure: negative value.
 DESCRIPTION
    Copies a property from one property list to another.

    If a property is copied from one list to another, the property will be
    first deleted from the destination list (generating a call to the 'close'
    callback for the property, if one exists) and then the property is copied
    from the source list to the destination list (generating a call to the
    'copy' callback for the property, if one exists).

    If the property does not exist in the destination list, this call is
    equivalent to calling H5Pinsert2 and the 'create' callback will be called
    (if such a callback exists for the property).

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__copy_prop_plist(hid_t dst_id, hid_t src_id, const char *name)
{
    H5P_genplist_t *dst_plist;           /* Pointer to destination property list */
    H5P_genplist_t *src_plist;           /* Pointer to source property list */
    H5P_genprop_t  *prop;                /* Temporary property pointer */
    H5P_genprop_t  *new_prop  = NULL;    /* Pointer to new property */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    assert(name);

    /* Get the objects to operate on */
    if (NULL == (src_plist = (H5P_genplist_t *)H5I_object(src_id)) ||
        NULL == (dst_plist = (H5P_genplist_t *)H5I_object(dst_id)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property object doesn't exist");

    /* If the property exists in the destination already */
    if (NULL != H5P__find_prop_plist(dst_plist, name)) {
        /* Delete the property from the destination list, calling the 'close' callback if necessary */
        if (H5P_remove(dst_plist, name) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "unable to remove property");

        /* Get the pointer to the source property */
        prop = H5P__find_prop_plist(src_plist, name);

        /* Make a copy of the source property */
        if ((new_prop = H5P__dup_prop(prop, H5P_PROP_WITHIN_LIST)) == NULL)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");

        /* Call property copy callback, if it exists */
        if (new_prop->copy) {
            if ((new_prop->copy)(new_prop->name, new_prop->size, new_prop->value) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, FAIL, "Can't copy property");
        } /* end if */

        /* Insert the initialized property into the property list */
        if (H5P__add_prop(dst_plist->props, new_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into list");

        /* Increment the number of properties in list */
        dst_plist->nprops++;
    } /* end if */
    /* If not, get the information required to do an H5Pinsert2 with the property into the destination list */
    else {
        /* Get the pointer to the source property */
        if (NULL == (prop = H5P__find_prop_plist(src_plist, name)))
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "property doesn't exist");

        /* Create property object from parameters */
        if (NULL ==
            (new_prop = H5P__create_prop(prop->name, prop->size, H5P_PROP_WITHIN_LIST, prop->value,
                                         prop->create, prop->set, prop->get, prop->encode, prop->decode,
                                         prop->del, prop->copy, prop->cmp, prop->close)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "Can't create property");

        /* Call property creation callback, if it exists */
        if (new_prop->create) {
            if ((new_prop->create)(new_prop->name, new_prop->size, new_prop->value) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "Can't initialize property");
        } /* end if */

        /* Insert property into property list class */
        if (H5P__add_prop(dst_plist->props, new_prop) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "Can't insert property into class");

        /* Increment property count for class */
        dst_plist->nprops++;
    } /* end else */

done:
    /* Cleanup, if necessary */
    if (ret_value < 0) {
        if (new_prop != NULL)
            H5P__free_prop(new_prop);
    } /* end if */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__copy_prop_plist() */

/*--------------------------------------------------------------------------
 NAME
    H5P__copy_prop_pclass
 PURPOSE
    Internal routine to copy a property from one class to another
 USAGE
    herr_t H5P__copy_prop_pclass(dst_pclass, src_pclass, name)
        H5P_genclass_t	*dst_pclass;    IN: Pointer to destination class
        H5P_genclass_t	*src_pclass;    IN: Pointer to source class
        const char *name;               IN: Name of property to copy
 RETURNS
    Success: non-negative value.
    Failure: negative value.
 DESCRIPTION
    Copies a property from one property class to another.

    If a property is copied from one class to another, all the property
    information will be first deleted from the destination class and then the
    property information will be copied from the source class into the
    destination class.

    If the property does not exist in the destination class or list, this call
    is equivalent to calling H5Pregister2.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__copy_prop_pclass(hid_t dst_id, hid_t src_id, const char *name)
{
    H5P_genclass_t *src_pclass;          /* Source property class, containing property to copy */
    H5P_genclass_t *dst_pclass;          /* Destination property class */
    H5P_genclass_t *orig_dst_pclass;     /* Original destination property class */
    H5P_genprop_t  *prop;                /* Temporary property pointer */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_PACKAGE

    /* Sanity check */
    assert(name);

    /* Get property list classes */
    if (NULL == (src_pclass = (H5P_genclass_t *)H5I_object(src_id)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "source property class object doesn't exist");
    if (NULL == (dst_pclass = (H5P_genclass_t *)H5I_object(dst_id)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "destination property class object doesn't exist");

    /* Get the property from the source */
    if (NULL == (prop = H5P__find_prop_pclass(src_pclass, name)))
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "unable to locate property");

    /* If the property exists in the destination already */
    if (H5P__exist_pclass(dst_pclass, name)) {
        /* Delete the old property from the destination class */
        if (H5P__unregister(dst_pclass, name) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "unable to remove property");
    } /* end if */

    /* Register the property into the destination */
    orig_dst_pclass = dst_pclass;
    if (H5P__register(&dst_pclass, name, prop->size, prop->value, prop->create, prop->set, prop->get,
                      prop->encode, prop->decode, prop->del, prop->copy, prop->cmp, prop->close) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "unable to remove property");

    /* Check if the property class changed and needs to be substituted in the ID */
    if (dst_pclass != orig_dst_pclass) {
        H5P_genclass_t *old_dst_pclass; /* Old destination property class */

        /* Substitute the new destination property class in the ID */
        if (NULL == (old_dst_pclass = (H5P_genclass_t *)H5I_subst(dst_id, dst_pclass)))
            HGOTO_ERROR(H5E_PLIST, H5E_CANTSET, FAIL, "unable to substitute property class in ID");
        assert(old_dst_pclass == orig_dst_pclass);

        /* Close the previous class */
        if (H5P__close_class(old_dst_pclass) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_CANTCLOSEOBJ, FAIL,
                        "unable to close original property class after substitution");
    } /* end if */

done:
    /* Cleanup, if necessary */

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__copy_prop_pclass() */

/*--------------------------------------------------------------------------
 NAME
    H5P__unregister
 PURPOSE
    Internal routine to remove a property from a property list class.
 USAGE
    herr_t H5P__unregister(pclass, name)
        H5P_genclass_t *pclass; IN: Property list class to modify
        const char *name;       IN: Name of property to remove
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Removes a property from a property list class.  Future property lists
    created of that class will not contain this property.  Existing property
    lists containing this property are not affected.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__unregister(H5P_genclass_t *pclass, const char *name)
{
    H5P_genprop_t *prop;                /* Temporary property pointer */
    herr_t         ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);
    assert(name);

    /* Get the property node from the skip list */
    if ((prop = (H5P_genprop_t *)H5SL_search(pclass->props, name)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "can't find property in skip list");

    /* Remove the property from the skip list */
    if (H5SL_remove(pclass->props, prop->name) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTDELETE, FAIL, "can't remove property from skip list");

    /* Free the property, ignoring return value, nothing we can do */
    H5P__free_prop(prop);

    /* Decrement the number of registered properties in class */
    pclass->nprops--;

    /* Update the revision for the class */
    pclass->revision = H5P_GET_NEXT_REV;

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__unregister() */

/*--------------------------------------------------------------------------
 NAME
    H5P_close
 PURPOSE
    Internal routine to close a property list.
 USAGE
    herr_t H5P_close(plist)
        H5P_genplist_t *plist;  IN: Property list to close
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
        Closes a property list.  If a 'close' callback exists for the property
    list class, it is called before the property list is destroyed.  If 'close'
    callbacks exist for any individual properties in the property list, they are
    called after the class 'close' callback.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
        The property list class 'close' callback routine is not called from
    here, it must have been checked for and called properly prior to this routine
    being called.
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P_close(H5P_genplist_t *plist)
{
    H5P_genclass_t *tclass;              /* Temporary class pointer */
    H5SL_t         *seen = NULL;         /* Skip list to hold names of properties already seen */
    size_t          nseen;               /* Number of items 'seen' */
    bool            has_parent_class;    /* Flag to indicate that this property list's class has a parent */
    size_t          ndel;                /* Number of items deleted */
    H5SL_node_t    *curr_node;           /* Current node in skip list */
    H5P_genprop_t  *tmp;                 /* Temporary pointer to properties */
    unsigned        make_cb   = 0;       /* Operator data for property free callback */
    herr_t          ret_value = SUCCEED; /* return value */

    FUNC_ENTER_NOAPI_NOINIT

    assert(plist);

    /* Make call to property list class close callback, if needed
     * (up through chain of parent classes also)
     */
    if (plist->class_init) {
        tclass = plist->pclass;
        while (NULL != tclass) {
            if (NULL != tclass->close_func) {
                /* Call user's "close" callback function, ignoring return value */
                (tclass->close_func)(plist->plist_id, tclass->close_data);
            } /* end if */

            /* Go up to parent class */
            tclass = tclass->parent;
        } /* end while */
    }     /* end if */

    /* Create the skip list to hold names of properties already seen
     * (This prevents a property in the class hierarchy from having it's
     * 'close' callback called, if a property in the class hierarchy has
     * already been seen)
     */
    if ((seen = H5SL_create(H5SL_TYPE_STR, NULL)) == NULL)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, FAIL, "can't create skip list for seen properties");
    nseen = 0;

    /* Walk through the changed properties in the list */
    if (H5SL_count(plist->props) > 0) {
        curr_node = H5SL_first(plist->props);
        while (curr_node != NULL) {
            /* Get pointer to property from node */
            tmp = (H5P_genprop_t *)H5SL_item(curr_node);

            /* Call property close callback, if it exists */
            if (tmp->close) {
                /* Call the 'close' callback */
                (tmp->close)(tmp->name, tmp->size, tmp->value);
            } /* end if */

            /* Add property name to "seen" list */
            if (H5SL_insert(seen, tmp->name, tmp->name) < 0)
                HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL, "can't insert property into seen skip list");
            nseen++;

            /* Get the next property node in the skip list */
            curr_node = H5SL_next(curr_node);
        } /* end while */
    }     /* end if */

    /* Determine number of deleted items from property list */
    ndel = H5SL_count(plist->del);

    /*
     * Check if we should remove class properties (up through list of parent classes also),
     * initialize each with default value & make property 'remove' callback.
     */
    tclass           = plist->pclass;
    has_parent_class = (bool)(tclass != NULL && tclass->parent != NULL && tclass->parent->nprops > 0);
    while (tclass != NULL) {
        if (tclass->nprops > 0) {
            /* Walk through the properties in the class */
            curr_node = H5SL_first(tclass->props);
            while (curr_node != NULL) {
                /* Get pointer to property from node */
                tmp = (H5P_genprop_t *)H5SL_item(curr_node);

                /* Only "delete" properties we haven't seen before
                 * and that haven't already been deleted
                 */
                if ((nseen == 0 || H5SL_search(seen, tmp->name) == NULL) &&
                    (ndel == 0 || H5SL_search(plist->del, tmp->name) == NULL)) {

                    /* Call property close callback, if it exists */
                    if (tmp->close) {
                        void *tmp_value; /* Temporary value buffer */

                        /* Allocate space for a temporary copy of the property value */
                        if (NULL == (tmp_value = H5MM_malloc(tmp->size)))
                            HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, FAIL,
                                        "memory allocation failed for temporary property value");
                        H5MM_memcpy(tmp_value, tmp->value, tmp->size);

                        /* Call the 'close' callback */
                        (tmp->close)(tmp->name, tmp->size, tmp_value);

                        /* Release the temporary value buffer */
                        H5MM_xfree(tmp_value);
                    } /* end if */

                    /* Add property name to "seen" list, if we have other classes to work on */
                    if (has_parent_class) {
                        if (H5SL_insert(seen, tmp->name, tmp->name) < 0)
                            HGOTO_ERROR(H5E_PLIST, H5E_CANTINSERT, FAIL,
                                        "can't insert property into seen skip list");
                        nseen++;
                    } /* end if */
                }     /* end if */

                /* Get the next property node in the skip list */
                curr_node = H5SL_next(curr_node);
            } /* end while */
        }     /* end if */

        /* Go up to parent class */
        tclass = tclass->parent;
    } /* end while */

    /* Decrement class's dependent property list value! */
    if (H5P__access_class(plist->pclass, H5P_MOD_DEC_LST) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTINIT, FAIL, "Can't decrement class ref count");

    /* Free the list of 'seen' properties */
    H5SL_close(seen);
    seen = NULL;

    /* Free the list of deleted property names */
    H5SL_destroy(plist->del, H5P__free_del_name_cb, NULL);

    /* Free the properties */
    H5SL_destroy(plist->props, H5P__free_prop_cb, &make_cb);

    /* Destroy property list object */
    plist = H5FL_FREE(H5P_genplist_t, plist);

done:
    /* Release the skip list of 'seen' properties */
    if (seen != NULL)
        H5SL_close(seen);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_close() */

/*--------------------------------------------------------------------------
 NAME
    H5P_get_class_name
 PURPOSE
    Internal routine to query the name of a generic property list class
 USAGE
    char *H5P_get_class_name(pclass)
        H5P_genclass_t *pclass;    IN: Property list class to check
 RETURNS
    Success: Pointer to a malloc'ed string containing the class name
    Failure: NULL
 DESCRIPTION
        This routine retrieves the name of a generic property list class.
    The pointer to the name must be free'd by the user for successful calls.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
char *
H5P_get_class_name(H5P_genclass_t *pclass)
{
    char *ret_value = NULL; /* Return value */

    FUNC_ENTER_NOAPI_NOERR

    assert(pclass);

    /* Get class name */
    ret_value = H5MM_xstrdup(pclass->name);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P_get_class_name() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_class_path
 PURPOSE
    Internal routine to query the full path of a generic property list class
 USAGE
    char *H5P__get_class_name(pclass)
        H5P_genclass_t *pclass;    IN: Property list class to check
 RETURNS
    Success: Pointer to a malloc'ed string containing the full path of class
    Failure: NULL
 DESCRIPTION
        This routine retrieves the full path name of a generic property list
    class, starting with the root of the class hierarchy.
    The pointer to the name must be free'd by the user for successful calls.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
char *
H5P__get_class_path(H5P_genclass_t *pclass)
{
    char *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(pclass);

    /* Recursively build the full path */
    if (pclass->parent != NULL) {
        char *par_path; /* Parent class's full path */

        /* Get the parent class's path */
        par_path = H5P__get_class_path(pclass->parent);
        if (par_path != NULL) {
            size_t ret_str_len;

            /* Allocate enough space for the parent class's path, plus the '/'
             * separator, this class's name and the string terminator
             */
            ret_str_len = strlen(par_path) + strlen(pclass->name) + 1 +
                          3; /* Extra "+3" to quiet GCC warning - 2019/07/05, QAK */
            if (NULL == (ret_value = (char *)H5MM_malloc(ret_str_len)))
                HGOTO_ERROR(H5E_RESOURCE, H5E_NOSPACE, NULL, "memory allocation failed for class name");

            /* Build the full path for this class */
            snprintf(ret_value, ret_str_len, "%s/%s", par_path, pclass->name);

            /* Free the parent class's path */
            H5MM_xfree(par_path);
        } /* end if */
        else
            ret_value = H5MM_xstrdup(pclass->name);
    } /* end if */
    else
        ret_value = H5MM_xstrdup(pclass->name);

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_class_path() */

/*--------------------------------------------------------------------------
 NAME
    H5P__open_class_path
 PURPOSE
    Internal routine to open [a copy of] a class with its full path name
 USAGE
    H5P_genclass_t *H5P__open_class_path(path)
        const char *path;       IN: Full path name of class to open [copy of]
 RETURNS
    Success: Pointer to a generic property class object
    Failure: NULL
 DESCRIPTION
    This routine opens [a copy] of the class indicated by the full path.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genclass_t *
H5P__open_class_path(const char *path)
{
    char             *tmp_path = NULL;  /* Temporary copy of the path */
    char             *curr_name;        /* Pointer to current component of path name */
    char             *delimit;          /* Pointer to path delimiter during traversal */
    H5P_genclass_t   *curr_class;       /* Pointer to class during path traversal */
    H5P_check_class_t check_info;       /* Structure to hold the information for checking duplicate names */
    H5P_genclass_t   *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE

    assert(path);

    /* Duplicate the path to use */
    tmp_path = H5MM_xstrdup(path);
    assert(tmp_path);

    /* Find the generic property class with this full path */
    curr_name  = tmp_path;
    curr_class = NULL;
    while (NULL != (delimit = strchr(curr_name, '/'))) {
        /* Change the delimiter to terminate the string */
        *delimit = '\0';

        /* Set up the search structure */
        check_info.parent    = curr_class;
        check_info.name      = curr_name;
        check_info.new_class = NULL;

        /* Find the class with this name & parent by iterating over the open classes */
        if (H5I_iterate(H5I_GENPROP_CLS, H5P__open_class_path_cb, &check_info, false) < 0)
            HGOTO_ERROR(H5E_PLIST, H5E_BADITER, NULL, "can't iterate over classes");
        else if (NULL == check_info.new_class)
            HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "can't locate class");

        /* Advance the pointer in the path to the start of the next component */
        curr_class = check_info.new_class;
        curr_name  = delimit + 1;
    } /* end while */

    /* Should be pointing to the last component in the path name now... */

    /* Set up the search structure */
    check_info.parent    = curr_class;
    check_info.name      = curr_name;
    check_info.new_class = NULL;

    /* Find the class with this name & parent by iterating over the open classes */
    if (H5I_iterate(H5I_GENPROP_CLS, H5P__open_class_path_cb, &check_info, false) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_BADITER, NULL, "can't iterate over classes");
    else if (NULL == check_info.new_class)
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, NULL, "can't locate class");

    /* Copy it */
    if (NULL == (ret_value = H5P__copy_pclass(check_info.new_class)))
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCOPY, NULL, "can't copy property class");

done:
    /* Free the duplicated path */
    H5MM_xfree(tmp_path);

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__open_class_path() */

/*--------------------------------------------------------------------------
 NAME
    H5P__get_class_parent
 PURPOSE
    Internal routine to query the parent class of a generic property class
 USAGE
    H5P_genclass_t *H5P__get_class_parent(pclass)
        H5P_genclass_t *pclass;    IN: Property class to check
 RETURNS
    Success: Pointer to the parent class of a property class
    Failure: NULL
 DESCRIPTION
    This routine retrieves a pointer to the parent class for a property class.

 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
H5P_genclass_t *
H5P__get_class_parent(const H5P_genclass_t *pclass)
{
    H5P_genclass_t *ret_value = NULL; /* Return value */

    FUNC_ENTER_PACKAGE_NOERR

    assert(pclass);

    /* Get property size */
    ret_value = pclass->parent;

    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__get_class_parent() */

/*--------------------------------------------------------------------------
 NAME
    H5P__close_class
 PURPOSE
    Internal routine to close a property list class.
 USAGE
    herr_t H5P__close_class(class)
        H5P_genclass_t *class;  IN: Property list class to close
 RETURNS
    Returns non-negative on success, negative on failure.
 DESCRIPTION
    Releases memory and de-attach a class from the property list class hierarchy.
 GLOBAL VARIABLES
 COMMENTS, BUGS, ASSUMPTIONS
 EXAMPLES
 REVISION LOG
--------------------------------------------------------------------------*/
herr_t
H5P__close_class(H5P_genclass_t *pclass)
{
    herr_t ret_value = SUCCEED; /* Return value */

    FUNC_ENTER_NOAPI_NOINIT

    assert(pclass);

    /* Decrement the reference count & check if the object should go away */
    if (H5P__access_class(pclass, H5P_MOD_DEC_REF) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_NOTFOUND, FAIL, "can't decrement ID ref count");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* H5P__close_class() */

/*-------------------------------------------------------------------------
 * Function:       H5P__new_plist_of_type
 *
 * Purpose:        Create a new property list, of a given type
 *
 * Return:	   Success:	ID of new property list
 *		   Failure:	H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5P__new_plist_of_type(H5P_plist_type_t type)
{
    H5P_genclass_t *pclass;                      /* Class of property list to create */
    hid_t           class_id;                    /* ID of class to create */
    hid_t           ret_value = H5I_INVALID_HID; /* Return value */

    FUNC_ENTER_PACKAGE

    /* Sanity checks */
    HDcompile_assert(H5P_TYPE_REFERENCE_ACCESS == (H5P_TYPE_MAX_TYPE - 1));
    assert(type >= H5P_TYPE_USER && type <= H5P_TYPE_REFERENCE_ACCESS);

    /* Check arguments */
    if (type == H5P_TYPE_USER)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, H5I_INVALID_HID, "can't create user property list");
    if (type == H5P_TYPE_ROOT)
        HGOTO_ERROR(H5E_PLIST, H5E_BADVALUE, H5I_INVALID_HID,
                    "shouldn't be creating root class property list");

    /* Instantiate a property list of the proper type */
    switch (type) {
        case H5P_TYPE_OBJECT_CREATE:
            class_id = H5P_CLS_OBJECT_CREATE_ID_g;
            break;

        case H5P_TYPE_FILE_CREATE:
            class_id = H5P_CLS_FILE_CREATE_ID_g;
            break;

        case H5P_TYPE_FILE_ACCESS:
            class_id = H5P_CLS_FILE_ACCESS_ID_g;
            break;

        case H5P_TYPE_DATASET_CREATE:
            class_id = H5P_CLS_DATASET_CREATE_ID_g;
            break;

        case H5P_TYPE_DATASET_ACCESS:
            class_id = H5P_CLS_DATASET_ACCESS_ID_g;
            break;

        case H5P_TYPE_DATASET_XFER:
            class_id = H5P_CLS_DATASET_XFER_ID_g;
            break;

        case H5P_TYPE_FILE_MOUNT:
            class_id = H5P_CLS_FILE_MOUNT_ID_g;
            break;

        case H5P_TYPE_GROUP_CREATE:
            class_id = H5P_CLS_GROUP_CREATE_ID_g;
            break;

        case H5P_TYPE_GROUP_ACCESS:
            class_id = H5P_CLS_GROUP_ACCESS_ID_g;
            break;

        case H5P_TYPE_DATATYPE_CREATE:
            class_id = H5P_CLS_DATATYPE_CREATE_ID_g;
            break;

        case H5P_TYPE_DATATYPE_ACCESS:
            class_id = H5P_CLS_DATATYPE_ACCESS_ID_g;
            break;

        case H5P_TYPE_MAP_CREATE:
            class_id = H5P_CLS_MAP_CREATE_ID_g;
            break;

        case H5P_TYPE_MAP_ACCESS:
            class_id = H5P_CLS_MAP_ACCESS_ID_g;
            break;

        case H5P_TYPE_STRING_CREATE:
            class_id = H5P_CLS_STRING_CREATE_ID_g;
            break;

        case H5P_TYPE_ATTRIBUTE_CREATE:
            class_id = H5P_CLS_ATTRIBUTE_CREATE_ID_g;
            break;

        case H5P_TYPE_ATTRIBUTE_ACCESS:
            class_id = H5P_CLS_ATTRIBUTE_ACCESS_ID_g;
            break;

        case H5P_TYPE_OBJECT_COPY:
            class_id = H5P_CLS_OBJECT_COPY_ID_g;
            break;

        case H5P_TYPE_LINK_CREATE:
            class_id = H5P_CLS_LINK_CREATE_ID_g;
            break;

        case H5P_TYPE_LINK_ACCESS:
            class_id = H5P_CLS_LINK_ACCESS_ID_g;
            break;

        case H5P_TYPE_VOL_INITIALIZE:
            class_id = H5P_CLS_VOL_INITIALIZE_ID_g;
            break;

        case H5P_TYPE_REFERENCE_ACCESS:
            class_id = H5P_CLS_REFERENCE_ACCESS_ID_g;
            break;

        case H5P_TYPE_USER: /* shut compiler warnings up */
        case H5P_TYPE_ROOT:
        case H5P_TYPE_MAX_TYPE:
        default:
            HGOTO_ERROR(H5E_PLIST, H5E_BADRANGE, FAIL, "invalid property list type: %u\n", (unsigned)type);
    } /* end switch */

    /* Get the class object */
    if (NULL == (pclass = (H5P_genclass_t *)H5I_object(class_id)))
        HGOTO_ERROR(H5E_PLIST, H5E_BADTYPE, H5I_INVALID_HID, "not a property class");

    /* Create the new property list */
    if ((ret_value = H5P_create_id(pclass, true)) < 0)
        HGOTO_ERROR(H5E_PLIST, H5E_CANTCREATE, H5I_INVALID_HID, "unable to create property list");

done:
    FUNC_LEAVE_NOAPI(ret_value)
} /* end H5P__new_plist_of_type() */

/*-------------------------------------------------------------------------
 * Function:	H5P_get_plist_id
 *
 * Purpose:	Quick and dirty routine to retrieve property list ID from
 *		property list structure.
 *          (Mainly added to stop non-file routines from poking about in the
 *          H5P_genplist_t data structure)
 *
 * Return:      Success:        Non-negative ID of property list.
 *              Failure:        H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5P_get_plist_id(const H5P_genplist_t *plist)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(plist);

    FUNC_LEAVE_NOAPI(plist->plist_id)
} /* end H5P_get_plist_id() */

/*-------------------------------------------------------------------------
 * Function:	H5P_get_class
 *
 * Purpose:	Quick and dirty routine to retrieve property list class from
 *		property list structure.
 *          (Mainly added to stop non-file routines from poking about in the
 *          H5P_genplist_t data structure)
 *
 * Return:      Success:        Non-NULL class of property list.
 *              Failure:        NULL
 *
 *-------------------------------------------------------------------------
 */
H5P_genclass_t *
H5P_get_class(const H5P_genplist_t *plist)
{
    /* Use FUNC_ENTER_NOAPI_NOINIT_NOERR here to avoid performance issues */
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    assert(plist);

    FUNC_LEAVE_NOAPI(plist->pclass)
} /* end H5P_get_class() */

/*-------------------------------------------------------------------------
 * Function:       H5P_ignore_cmp
 *
 * Purpose:        Callback routine to ignore comparing property values.
 *
 * Return:         zero
 *
 *-------------------------------------------------------------------------
 */
int
H5P_ignore_cmp(const void H5_ATTR_UNUSED *val1, const void H5_ATTR_UNUSED *val2, size_t H5_ATTR_UNUSED size)
{
    FUNC_ENTER_NOAPI_NOINIT_NOERR

    FUNC_LEAVE_NOAPI(0)
} /* end H5P_ignore_cmp() */
