/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2012 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/class/opal_pointer_array.h"

#include "ompi/constants.h"
#include "ompi/op/op.h"
#include "ompi/mca/op/base/base.h"
#include "ompi/datatype/ompi_datatype_internal.h"


/*
 * Table for Fortran <-> C op handle conversion
 */
opal_pointer_array_t *ompi_op_f_to_c_table = {0};


/*
 * Create intrinsic op
 */
static int add_intrinsic(ompi_op_t *op, int fort_handle, int flags,
                         const char *name);


/*
 * Class information
 */
static void ompi_op_construct(ompi_op_t *eh);
static void ompi_op_destruct(ompi_op_t *eh);


/*
 * Class instance
 */
OBJ_CLASS_INSTANCE(ompi_op_t, opal_object_t,
                   ompi_op_construct, ompi_op_destruct);


/*
 * Intrinsic MPI_Op objects (_addr flavors are for F03 bindings)
 */
ompi_predefined_op_t ompi_mpi_op_null = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_null_addr = &ompi_mpi_op_null;
ompi_predefined_op_t ompi_mpi_op_max = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_max_addr = &ompi_mpi_op_max;
ompi_predefined_op_t ompi_mpi_op_min = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_min_addr = &ompi_mpi_op_min;
ompi_predefined_op_t ompi_mpi_op_sum = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_sum_addr = &ompi_mpi_op_sum;
ompi_predefined_op_t ompi_mpi_op_prod = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_prod_addr = &ompi_mpi_op_prod;
ompi_predefined_op_t ompi_mpi_op_land = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_land_addr = &ompi_mpi_op_land;
ompi_predefined_op_t ompi_mpi_op_band = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_band_addr = &ompi_mpi_op_band;
ompi_predefined_op_t ompi_mpi_op_lor = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_lor_addr = &ompi_mpi_op_lor;
ompi_predefined_op_t ompi_mpi_op_bor = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_bor_addr = &ompi_mpi_op_bor;
ompi_predefined_op_t ompi_mpi_op_lxor = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_lxor_addr = &ompi_mpi_op_lxor;
ompi_predefined_op_t ompi_mpi_op_bxor = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_bxor_addr = &ompi_mpi_op_bxor;
ompi_predefined_op_t ompi_mpi_op_maxloc = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_maxloc_addr = &ompi_mpi_op_maxloc;
ompi_predefined_op_t ompi_mpi_op_minloc = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_minloc_addr = &ompi_mpi_op_minloc;
ompi_predefined_op_t ompi_mpi_op_replace = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_replace_addr = &ompi_mpi_op_replace;
ompi_predefined_op_t ompi_mpi_op_no_op = {{{0}}};
ompi_predefined_op_t *ompi_mpi_op_no_op_addr = &ompi_mpi_op_no_op;

/*
 * Map from ddt->id to position in op function pointer array
 */
int ompi_op_ddt_map[OMPI_DATATYPE_MAX_PREDEFINED] = {0};

/* Get the c complex operator associated with a fortran complex type */
#define FORTRAN_COMPLEX_OP_TYPE_X(type) OMPI_OP_BASE_TYPE_ ## type
/* Preprocessor hack to ensure type gets expanded correctly */
#define FORTRAN_COMPLEX_OP_TYPE(type) FORTRAN_COMPLEX_OP_TYPE_X(type)

#define FLAGS_NO_FLOAT \
    (OMPI_OP_FLAGS_INTRINSIC | OMPI_OP_FLAGS_ASSOC | OMPI_OP_FLAGS_COMMUTE)
#define FLAGS \
    (OMPI_OP_FLAGS_INTRINSIC | OMPI_OP_FLAGS_ASSOC | \
     OMPI_OP_FLAGS_FLOAT_ASSOC | OMPI_OP_FLAGS_COMMUTE)

/*
 * Initialize OMPI op infrastructure
 */
int ompi_op_init(void)
{
    int i;

  /* initialize ompi_op_f_to_c_table */

    ompi_op_f_to_c_table = OBJ_NEW(opal_pointer_array_t);
    if (NULL == ompi_op_f_to_c_table){
        return OMPI_ERROR;
    }

    /* Fill in the ddt.id->op_position map */

    for (i = 0; i < OMPI_DATATYPE_MAX_PREDEFINED; ++i) {
        ompi_op_ddt_map[i] = -1;
    }

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_INT8_T] = OMPI_OP_BASE_TYPE_INT8_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_UINT8_T] = OMPI_OP_BASE_TYPE_UINT8_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_INT16_T] = OMPI_OP_BASE_TYPE_INT16_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_UINT16_T] = OMPI_OP_BASE_TYPE_UINT16_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_INT32_T] = OMPI_OP_BASE_TYPE_INT32_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_UINT32_T] = OMPI_OP_BASE_TYPE_UINT32_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_INT64_T] = OMPI_OP_BASE_TYPE_INT64_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_UINT64_T] = OMPI_OP_BASE_TYPE_UINT64_T;

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_FLOAT] = OMPI_OP_BASE_TYPE_FLOAT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_DOUBLE] = OMPI_OP_BASE_TYPE_DOUBLE;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_LONG_DOUBLE] = OMPI_OP_BASE_TYPE_LONG_DOUBLE;

#if OMPI_HAVE_FORTRAN_COMPLEX8
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COMPLEX8] = FORTRAN_COMPLEX_OP_TYPE(OMPI_KIND_FORTRAN_COMPLEX8);
#endif

#if OMPI_HAVE_FORTRAN_COMPLEX16
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COMPLEX16] = FORTRAN_COMPLEX_OP_TYPE(OMPI_KIND_FORTRAN_COMPLEX16);
#endif

    /* Only enable reductions on COMPLEX32 if the REAL*16 matches the equivalent C type */
#if OMPI_REAL16_MATCHES_C && OMPI_HAVE_FORTRAN_COMPLEX32
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COMPLEX32] = FORTRAN_COMPLEX_OP_TYPE(OMPI_KIND_FORTRAN_COMPLEX32);
#endif

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_WCHAR] = OMPI_OP_BASE_TYPE_WCHAR;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_BOOL] = OMPI_OP_BASE_TYPE_BOOL;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_LOGICAL] = OMPI_OP_BASE_TYPE_LOGICAL;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_CHARACTER] = OMPI_OP_BASE_TYPE_UINT8_T;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_INTEGER] = OMPI_OP_BASE_TYPE_INTEGER;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_REAL] = OMPI_OP_BASE_TYPE_REAL;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_DOUBLE_PRECISION] = OMPI_OP_BASE_TYPE_DOUBLE_PRECISION;

#if OMPI_HAVE_FORTRAN_COMPLEX
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COMPLEX] = FORTRAN_COMPLEX_OP_TYPE(OMPI_KIND_FORTRAN_COMPLEX);
#endif

#if OMPI_HAVE_FORTRAN_DOUBLE_COMPLEX
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_DOUBLE_COMPLEX] = FORTRAN_COMPLEX_OP_TYPE(OMPI_KIND_FORTRAN_DOUBLE_COMPLEX);
#endif

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_LONG_DOUBLE_COMPLEX] = OMPI_OP_BASE_TYPE_C_LONG_DOUBLE_COMPLEX;

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2INT] = OMPI_OP_BASE_TYPE_2INT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2INTEGER] = OMPI_OP_BASE_TYPE_2INTEGER;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2REAL] = OMPI_OP_BASE_TYPE_2REAL;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2DBLPREC] = OMPI_OP_BASE_TYPE_2DOUBLE_PRECISION;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2COMPLEX] = -1;         /* Not defined */
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_2DOUBLE_COMPLEX] = -1;  /* Not defined */

    ompi_op_ddt_map[OMPI_DATATYPE_MPI_FLOAT_INT] = OMPI_OP_BASE_TYPE_FLOAT_INT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_DOUBLE_INT] = OMPI_OP_BASE_TYPE_DOUBLE_INT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_LONG_DOUBLE_INT] = OMPI_OP_BASE_TYPE_LONG_DOUBLE_INT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_LONG_INT] = OMPI_OP_BASE_TYPE_LONG_INT;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_SHORT_INT] = OMPI_OP_BASE_TYPE_SHORT_INT;

#if SIZEOF_PTRDIFF_T == 4
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_AINT] = OMPI_OP_BASE_TYPE_INT32_T;
#elif SIZEOF_PTRDIFF_T == 8
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_AINT] = OMPI_OP_BASE_TYPE_INT64_T;
#else
#warning Unsupported definition for MPI_AINT
#endif
#if OMPI_MPI_OFFSET_SIZE == 4
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_OFFSET] = OMPI_OP_BASE_TYPE_UINT32_T;
#elif OMPI_MPI_OFFSET_SIZE == 8
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_OFFSET] = OMPI_OP_BASE_TYPE_UINT64_T;
#else
#warning Unsupported definition for MPI_OFFSET
#endif
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_C_BOOL] = OMPI_OP_BASE_TYPE_BOOL;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_C_COMPLEX] = OMPI_OP_BASE_TYPE_C_FLOAT_COMPLEX;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_C_FLOAT_COMPLEX] = OMPI_OP_BASE_TYPE_C_FLOAT_COMPLEX;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_C_DOUBLE_COMPLEX] = OMPI_OP_BASE_TYPE_C_DOUBLE_COMPLEX;
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_C_LONG_DOUBLE_COMPLEX] = OMPI_OP_BASE_TYPE_C_LONG_DOUBLE_COMPLEX;

    /* MPI 3.0 datatypes */
#if OMPI_MPI_COUNT_SIZE == 4
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COUNT] = OMPI_OP_BASE_TYPE_INT32_T;
#elif OMPI_MPI_COUNT_SIZE == 8
    ompi_op_ddt_map[OMPI_DATATYPE_MPI_COUNT] = OMPI_OP_BASE_TYPE_INT64_T;
#else
#warning Unsupported definition for MPI_COUNT
#endif

    /* Create the intrinsic ops */

    if (OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_null.op, OMPI_OP_BASE_FORTRAN_NULL,
                      FLAGS, "MPI_NULL") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_max.op, OMPI_OP_BASE_FORTRAN_MAX,
                      FLAGS, "MPI_MAX") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_min.op, OMPI_OP_BASE_FORTRAN_MIN,
                      FLAGS, "MPI_MIN") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_sum.op, OMPI_OP_BASE_FORTRAN_SUM,
                      FLAGS_NO_FLOAT, "MPI_SUM") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_prod.op, OMPI_OP_BASE_FORTRAN_PROD,
                      FLAGS_NO_FLOAT, "MPI_PROD") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_land.op, OMPI_OP_BASE_FORTRAN_LAND,
                      FLAGS, "MPI_LAND") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_band.op, OMPI_OP_BASE_FORTRAN_BAND,
                      FLAGS, "MPI_BAND") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_lor.op, OMPI_OP_BASE_FORTRAN_LOR,
                      FLAGS, "MPI_LOR") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_bor.op, OMPI_OP_BASE_FORTRAN_BOR,
                      FLAGS, "MPI_BOR") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_lxor.op, OMPI_OP_BASE_FORTRAN_LXOR,
                      FLAGS, "MPI_LXOR") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_bxor.op, OMPI_OP_BASE_FORTRAN_BXOR,
                      FLAGS, "MPI_BXOR") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_maxloc.op, OMPI_OP_BASE_FORTRAN_MAXLOC,
                      FLAGS, "MPI_MAXLOC") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_minloc.op, OMPI_OP_BASE_FORTRAN_MINLOC,
                      FLAGS, "MPI_MINLOC") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_replace.op, OMPI_OP_BASE_FORTRAN_REPLACE,
                      FLAGS, "MPI_REPLACE") ||
        OMPI_SUCCESS !=
        add_intrinsic(&ompi_mpi_op_no_op.op, OMPI_OP_BASE_FORTRAN_NO_OP,
                      FLAGS, "MPI_NO_OP")) {
        return OMPI_ERROR;
    }else{
/* This code is placed back here to support
 * HCOL allreduce at the moment. It is a part of bgate repository only. This conflict with OMPI v1.7
 * is to be resolved some other way.
 * */
        ompi_mpi_op_null.op.op_type = OMPI_OP_NULL;
        ompi_mpi_op_max.op.op_type = OMPI_OP_MAX;
        ompi_mpi_op_min.op.op_type = OMPI_OP_MIN;
        ompi_mpi_op_sum.op.op_type = OMPI_OP_SUM;
        ompi_mpi_op_prod.op.op_type = OMPI_OP_PROD;
        ompi_mpi_op_land.op.op_type = OMPI_OP_LAND;
        ompi_mpi_op_band.op.op_type = OMPI_OP_BAND;
        ompi_mpi_op_lor.op.op_type = OMPI_OP_LOR;
        ompi_mpi_op_bor.op.op_type = OMPI_OP_BOR;
        ompi_mpi_op_lxor.op.op_type = OMPI_OP_LXOR;
        ompi_mpi_op_bxor.op.op_type = OMPI_OP_BXOR;
        ompi_mpi_op_maxloc.op.op_type = OMPI_OP_MAXLOC;
        ompi_mpi_op_minloc.op.op_type = OMPI_OP_MINLOC;
        ompi_mpi_op_replace.op.op_type = OMPI_OP_REPLACE;
    }

    /* All done */
    return OMPI_SUCCESS;
}


/*
 * Clean up the op resources
 */
int ompi_op_finalize(void)
{
    /* clean up the intrinsic ops */
    OBJ_DESTRUCT(&ompi_mpi_op_no_op);
    OBJ_DESTRUCT(&ompi_mpi_op_replace);
    OBJ_DESTRUCT(&ompi_mpi_op_minloc);
    OBJ_DESTRUCT(&ompi_mpi_op_maxloc);
    OBJ_DESTRUCT(&ompi_mpi_op_bxor);
    OBJ_DESTRUCT(&ompi_mpi_op_lxor);
    OBJ_DESTRUCT(&ompi_mpi_op_bor);
    OBJ_DESTRUCT(&ompi_mpi_op_lor);
    OBJ_DESTRUCT(&ompi_mpi_op_band);
    OBJ_DESTRUCT(&ompi_mpi_op_land);
    OBJ_DESTRUCT(&ompi_mpi_op_prod);
    OBJ_DESTRUCT(&ompi_mpi_op_sum);
    OBJ_DESTRUCT(&ompi_mpi_op_min);
    OBJ_DESTRUCT(&ompi_mpi_op_max);
    OBJ_DESTRUCT(&ompi_mpi_op_null);

    /* Remove op F2C table */

    OBJ_RELEASE(ompi_op_f_to_c_table);

    /* All done */

    return OMPI_SUCCESS;
}


/*
 * Create a new MPI_Op
 */
ompi_op_t *ompi_op_create_user(bool commute,
                               ompi_op_fortran_handler_fn_t func)
{
    ompi_op_t *new_op;

    /* Create a new object and ensure that it's valid */
    new_op = OBJ_NEW(ompi_op_t);
    if (NULL == new_op) {
        goto error;
    }

    if (0 > new_op->o_f_to_c_index) {
        OBJ_RELEASE(new_op);
        new_op = NULL;
        goto error;
    }

    /*
     * The new object is valid -- initialize it.  If this is being
     *  created from fortran, the fortran MPI API wrapper function
     *  will override the o_flags field directly.  We cast the
     *  function pointer type to the fortran type arbitrarily -- it
     *  only has to be a function pointer in order to store properly,
     *  it doesn't matter what type it is (we'll cast it to the Right
     *  type when we *use* it).
     */
    new_op->o_flags = OMPI_OP_FLAGS_ASSOC;
    if (commute) {
        new_op->o_flags |= OMPI_OP_FLAGS_COMMUTE;
    }

    strncpy(new_op->o_name, "USER OP", sizeof(new_op->o_name) - 1);
    new_op->o_name[sizeof(new_op->o_name) - 1] = '\0';

    /* Set the user-defined callback function.  The "fort_fn" member
       is part of a union, so it doesn't matter if this is a C or
       Fortan callback; we'll call the right flavor (per o_flags) at
       invocation time. */
    new_op->o_func.fort_fn = func;

error:
    /* All done */
    return new_op;
}


/*
 * See lengthy comment in mpi/cxx/intercepts.cc for how the C++ MPI::Op
 * callbacks work.
 */
void ompi_op_set_cxx_callback(ompi_op_t *op, MPI_User_function *fn)
{
    op->o_flags |= OMPI_OP_FLAGS_CXX_FUNC;
    /* The OMPI C++ intercept was previously stored in
       op->o_func.fort_fn by ompi_op_create_user().  So save that in
       cxx.intercept_fn and put the user's fn in cxx.user_fn. */
    op->o_func.cxx_data.intercept_fn =
        (ompi_op_cxx_handler_fn_t *) op->o_func.fort_fn;
    op->o_func.cxx_data.user_fn = fn;
}


/*
 * See lengthy comment in mpi/cxx/intercepts.cc for how the C++ MPI::Op
 * callbacks work.
 */
void ompi_op_set_java_callback(ompi_op_t *op, void *jnienv,
                               void *object, int baseType)
{
    op->o_flags |= OMPI_OP_FLAGS_JAVA_FUNC;
    /* The OMPI Java intercept was previously stored in
       op->o_func.fort_fn by ompi_op_create_user().  So save that in
       cxx.intercept_fn and put the user's fn in cxx.user_fn. */
    op->o_func.java_data.intercept_fn =
        (ompi_op_java_handler_fn_t *) op->o_func.fort_fn;
    op->o_func.java_data.jnienv = jnienv;
    op->o_func.java_data.object = object;
    op->o_func.java_data.baseType = baseType;
}


/**************************************************************************
 *
 * Static functions
 *
 **************************************************************************/

static int add_intrinsic(ompi_op_t *op, int fort_handle, int flags,
                         const char *name)
{
    /* Add the op to the table */
    OBJ_CONSTRUCT(op, ompi_op_t);
    if (op->o_f_to_c_index != fort_handle) {
        return OMPI_ERROR;
    }

    /* Set the members */
    op->o_flags = flags;
    strncpy(op->o_name, name, sizeof(op->o_name) - 1);
    op->o_name[sizeof(op->o_name) - 1] = '\0';

    /* Perform the selection on this op to fill in the function
       pointers (except for NULL and REPLACE, which don't get
       components) */
    if (OMPI_OP_BASE_FORTRAN_NULL != op->o_f_to_c_index &&
        OMPI_OP_BASE_FORTRAN_REPLACE != op->o_f_to_c_index &&
        OMPI_OP_BASE_FORTRAN_NO_OP != op->o_f_to_c_index) {
        return ompi_op_base_op_select(op);
    } else {
        return OMPI_SUCCESS;
    }
}


/*
 * Op constructor
 */
static void ompi_op_construct(ompi_op_t *new_op)
{
    int i;

    /* Provide a default of a high value. Useful for non-predefined ops. */
    new_op->op_type = OMPI_OP_NUM_OF_TYPES;
    new_op->o_flags = 0;
    new_op->o_name[0] = '\0';

    /* assign entry in fortran <-> c translation array */

    new_op->o_f_to_c_index =
        opal_pointer_array_add(ompi_op_f_to_c_table, new_op);

    /* Set everything to NULL so that we can intelligently free
       non-NULL's in the destructor */
    for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
        new_op->o_func.intrinsic.fns[i] = NULL;
        new_op->o_func.intrinsic.modules[i] = NULL;
        new_op->o_3buff_intrinsic.fns[i] = NULL;
        new_op->o_3buff_intrinsic.modules[i] = NULL;
    }
}


/*
 * Op destructor
 */
static void ompi_op_destruct(ompi_op_t *op)
{
    int i;

    /* reset the ompi_op_f_to_c_table entry - make sure that the
       entry is in the table */

    if (NULL != opal_pointer_array_get_item(ompi_op_f_to_c_table,
                                            op->o_f_to_c_index)) {
        opal_pointer_array_set_item(ompi_op_f_to_c_table,
                                    op->o_f_to_c_index, NULL);
    }

    for (i = 0; i < OMPI_OP_BASE_TYPE_MAX; ++i) {
        op->o_func.intrinsic.fns[i] = NULL;
        if( NULL != op->o_func.intrinsic.modules[i] ) {
            OBJ_RELEASE(op->o_func.intrinsic.modules[i]);
            op->o_func.intrinsic.modules[i] = NULL;
        }
        op->o_3buff_intrinsic.fns[i] = NULL;
        if( NULL != op->o_3buff_intrinsic.modules[i] ) {
            OBJ_RELEASE(op->o_3buff_intrinsic.modules[i]);
            op->o_3buff_intrinsic.modules[i] = NULL;
        }
    }
}
