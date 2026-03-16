/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      UT-Battelle, LLC
 * Copyright (c) 2008-2017 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2019      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Public interface for the MPI_Op handle.
 */

#ifndef OMPI_OP_H
#define OMPI_OP_H

#include "ompi_config.h"

#include <stdio.h>

#include "mpi.h"

#include "opal/class/opal_object.h"

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mpi/fortran/base/fint_2_int.h"
#include "ompi/mca/op/op.h"

BEGIN_C_DECLS

/**
 * Typedef for C op functions for user-defined MPI_Ops.
 *
 * We don't use MPI_User_function because this would create a
 * confusing dependency loop between this file and mpi.h.  So this is
 * repeated code, but it's better this way (and this typedef will
 * never change, so there's not much of a maintenance worry).
 */
typedef void (ompi_op_c_handler_fn_t)(void *, void *, int *,
                                      struct ompi_datatype_t **);

/**
 * Typedef for fortran user-defined MPI_Ops.
 */
typedef void (ompi_op_fortran_handler_fn_t)(void *, void *,
                                            MPI_Fint *, MPI_Fint *);

/**
 * Typedef for C++ op functions intercept (used for user-defined
 * MPI::Ops).
 *
 * See the lengthy explanation for why this is different than the C
 * intercept in ompi/mpi/cxx/intercepts.cc in the
 * ompi_mpi_cxx_op_intercept() function.
 */
typedef void (ompi_op_cxx_handler_fn_t)(void *, void *, int *,
                                        struct ompi_datatype_t **,
                                        MPI_User_function * op);

/**
 * Typedef for Java op functions intercept (used for user-defined
 * MPI.Ops).
 *
 * See the lengthy explanation for why this is different than the C
 * intercept in ompi/mpi/cxx/intercepts.cc in the
 * ompi_mpi_cxx_op_intercept() function.
 */
typedef void (ompi_op_java_handler_fn_t)(void *, void *, int *,
                                         struct ompi_datatype_t **,
                                         int baseType,
                                         void *jnienv, void *object);

/*
 * Flags for MPI_Op
 */
/** Set if the MPI_Op is a built-in operation */
#define OMPI_OP_FLAGS_INTRINSIC    0x0001
/** Set if the callback function is in Fortran */
#define OMPI_OP_FLAGS_FORTRAN_FUNC 0x0002
/** Set if the callback function is in C++ */
#define OMPI_OP_FLAGS_CXX_FUNC     0x0004
/** Set if the callback function is in Java */
#define OMPI_OP_FLAGS_JAVA_FUNC    0x0008
/** Set if the callback function is associative (MAX and SUM will both
    have ASSOC set -- in fact, it will only *not* be set if we
    implement some extensions to MPI, because MPI says that all
    MPI_Op's should be associative, so this flag is really here for
    future expansion) */
#define OMPI_OP_FLAGS_ASSOC        0x0010
/** Set if the callback function is associative for floating point
    operands (e.g., MPI_SUM will have ASSOC set, but will *not* have
    FLOAT_ASSOC set)  */
#define OMPI_OP_FLAGS_FLOAT_ASSOC  0x0020
/** Set if the callback function is communative */
#define OMPI_OP_FLAGS_COMMUTE      0x0040




/*
 * Basic operation type for predefined types.
 */
enum ompi_op_type {
    OMPI_OP_NULL,
    OMPI_OP_MAX,
    OMPI_OP_MIN,
    OMPI_OP_SUM,
    OMPI_OP_PROD,
    OMPI_OP_LAND,
    OMPI_OP_BAND,
    OMPI_OP_LOR,
    OMPI_OP_BOR,
    OMPI_OP_LXOR,
    OMPI_OP_BXOR,
    OMPI_OP_MAXLOC,
    OMPI_OP_MINLOC,
    OMPI_OP_REPLACE,
    OMPI_OP_NUM_OF_TYPES
};
/**
 * Back-end type of MPI_Op
 */
struct ompi_op_t {
    /** Parent class, for reference counting */
    opal_object_t super;

    /** Name, for debugging purposes */
    char o_name[MPI_MAX_OBJECT_NAME];

    enum ompi_op_type op_type;

    /** Flags about the op */
    uint32_t o_flags;

    /** Index in Fortran <-> C translation array */
    int o_f_to_c_index;

    /** Union holding (2-buffer functions):
        1. Function pointers for all supported datatypes when this op
           is intrinsic
        2. Function pointers for when this op is user-defined (only
           need one function pointer for this; we call it for *all*
           datatypes, even intrinsics)
     */
    union {
        /** Function/module pointers for intrinsic ops */
        ompi_op_base_op_fns_t intrinsic;
        /** C handler function pointer */
        ompi_op_c_handler_fn_t *c_fn;
        /** Fortran handler function pointer */
        ompi_op_fortran_handler_fn_t *fort_fn;
        /** C++ intercept function data -- see lengthy comment in
            ompi/mpi/cxx/intercepts.cc::ompi_mpi_cxx_op_intercept() for
            an explanation */
        struct {
            /* The user's function (it's the wrong type, but that's ok) */
            ompi_op_c_handler_fn_t *user_fn;
            /* The OMPI C++ callback/intercept function */
            ompi_op_cxx_handler_fn_t *intercept_fn;
        } cxx_data;
        struct {
            /* The OMPI C++ callback/intercept function */
            ompi_op_java_handler_fn_t *intercept_fn;
            /* The Java run time environment */
            void *jnienv, *object;
            int baseType;
        } java_data;
    } o_func;

    /** 3-buffer functions, which is only for intrinsic ops.  No need
        for the C/C++/Fortran user-defined functions. */
    ompi_op_base_op_3buff_fns_t o_3buff_intrinsic;
};

/**
 * Convenience typedef
 */
typedef struct ompi_op_t ompi_op_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_op_t);

/**
 * Padded struct to maintain back compatibiltiy.
 * See ompi/communicator/communicator.h comments with struct ompi_communicator_t
 * for full explanation why we chose the following padding construct for predefines.
 */
#define PREDEFINED_OP_PAD 2048

struct ompi_predefined_op_t {
    struct ompi_op_t op;
    char padding[PREDEFINED_OP_PAD - sizeof(ompi_op_t)];
};

typedef struct ompi_predefined_op_t ompi_predefined_op_t;

/**
 * Array to map ddt->id values to the corresponding position in the op
 * function array.
 *
 * NOTE: It is possible to have an implementation without this map.
 * There are basically 3 choices for implementing "how to find the
 * right position in the op array based on the datatype":
 *
 * 1. Use the exact same ordering as ddt->id in the op map.  This is
 * nice in that it's always a direct lookup via one memory
 * de-reference.  But it makes a sparse op array, and it's at least
 * somewhat wasteful.  It also chains the ddt and op implementations
 * together.  If the ddt ever changes its ordering, op is screwed.  It
 * seemed safer from a maintenance point of view not to do it that
 * way.
 *
 * 2. Re-arrange the ddt ID values so that all the reducable types are
 * at the beginning.  This means that we can have a dense array here
 * in op, but then we have the same problem as number one -- and so
 * this didn't seem like a good idea from a maintenance point of view.
 *
 * 3. Create a mapping between the ddt->id values and the position in
 * the op array.  This allows a nice dense op array, and if we make
 * the map based on symbolic values, then if ddt ever changes its
 * ordering, it won't matter to op.  This seemed like the safest thing
 * to do from a maintenance perspective, and since it only costs one
 * extra lookup, and that lookup is way cheaper than the function call
 * to invoke the reduction operation, it seemed like the best idea.
 */
OMPI_DECLSPEC extern int ompi_op_ddt_map[OMPI_DATATYPE_MAX_PREDEFINED];

/**
 * Global variable for MPI_OP_NULL (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_null;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_null_addr;

/**
 * Global variable for MPI_MAX (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_max;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_max_addr;

/**
 * Global variable for MPI_MIN (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_min;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_min_addr;

/**
 * Global variable for MPI_SUM (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_sum;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_sum_addr;

/**
 * Global variable for MPI_PROD (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_prod;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_prod_addr;

/**
 * Global variable for MPI_LAND (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_land;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_land_addr;

/**
 * Global variable for MPI_BAND (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_band;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_band_addr;

/**
 * Global variable for MPI_LOR (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_lor;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_lor_addr;

/**
 * Global variable for MPI_BOR (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_bor;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_bor_addr;

/**
 * Global variable for MPI_LXOR (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_lxor;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_lxor_addr;

/**
 * Global variable for MPI_BXOR (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_bxor;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_bxor_addr;

/**
 * Global variable for MPI_MAXLOC (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_maxloc;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_maxloc_addr;

/**
 * Global variable for MPI_MINLOC (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_minloc;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_minloc_addr;

/**
 * Global variable for MPI_REPLACE (_addr flavor is for F03 bindings)
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_replace;
OMPI_DECLSPEC extern ompi_predefined_op_t *ompi_mpi_op_replace_addr;

/**
 * Global variable for MPI_NO_OP
 */
OMPI_DECLSPEC extern ompi_predefined_op_t ompi_mpi_op_no_op;


/**
 * Table for Fortran <-> C op handle conversion
 */
extern struct opal_pointer_array_t *ompi_op_f_to_c_table;

/**
 * Initialize the op interface.
 *
 * @returns OMPI_SUCCESS Upon success
 * @returns OMPI_ERROR Otherwise
 *
 * Invoked from ompi_mpi_init(); sets up the op interface, creates
 * the predefined MPI operations, and creates the corresopnding F2C
 * translation table.
 */
int ompi_op_init(void);

/**
 * Finalize the op interface.
 *
 * @returns OMPI_SUCCESS Always
 *
 * Invokes from ompi_mpi_finalize(); tears down the op interface, and
 * destroys the F2C translation table.
 */
int ompi_op_finalize(void);

/**
 * Create a ompi_op_t with a user-defined callback (vs. creating an
 * intrinsic ompi_op_t).
 *
 * @param commute Boolean indicating whether the operation is
 *        communative or not
 * @param func Function pointer of the error handler
 *
 * @returns op Pointer to the ompi_op_t that will be
 *   created and returned
 *
 * This function is called as the back-end of all the MPI_OP_CREATE
 * function.  It creates a new ompi_op_t object, initializes it to the
 * correct object type, and sets the callback function on it.
 *
 * The type of the function pointer is (arbitrarily) the fortran
 * function handler type.  Since this function has to accept 2
 * different function pointer types (lest we have 2 different
 * functions to create errhandlers), the fortran one was picked
 * arbitrarily.  Note that (void*) is not sufficient because at
 * least theoretically, a sizeof(void*) may not necessarily be the
 * same as sizeof(void(*)).
 *
 * NOTE: It *always* sets the "fortran" flag to false.  The Fortran
 * wrapper for MPI_OP_CREATE is expected to reset this flag to true
 * manually.
 */
ompi_op_t *ompi_op_create_user(bool commute,
                               ompi_op_fortran_handler_fn_t func);

/**
 * Mark an MPI_Op as holding a C++ callback function, and cache
 * that function in the MPI_Op.  See a lenghty comment in
 * ompi/mpi/cxx/op.c::ompi_mpi_cxx_op_intercept() for a full
 * expalantion.
 */
OMPI_DECLSPEC void ompi_op_set_cxx_callback(ompi_op_t * op,
                                            MPI_User_function * fn);

/**
 * Similar to ompi_op_set_cxx_callback(), mark an MPI_Op as holding a
 * Java calback function, and cache that function in the MPI_Op.
 */
OMPI_DECLSPEC void ompi_op_set_java_callback(ompi_op_t *op,  void *jnienv,
                                             void *object, int baseType);

/**
 * Check to see if an op is intrinsic.
 *
 * @param op The op to check
 *
 * @returns true If the op is intrinsic
 * @returns false If the op is not intrinsic
 *
 * Self-explanitory.  This is needed in a few top-level MPI functions;
 * this function is provided to hide the internal structure field
 * names.
 */
static inline bool ompi_op_is_intrinsic(ompi_op_t * op)
{
    return (bool) (0 != (op->o_flags & OMPI_OP_FLAGS_INTRINSIC));
}


/**
 * Check to see if an op is communative or not
 *
 * @param op The op to check
 *
 * @returns true If the op is communative
 * @returns false If the op is not communative
 *
 * Self-explanitory.  This is needed in a few top-level MPI functions;
 * this function is provided to hide the internal structure field
 * names.
 */
static inline bool ompi_op_is_commute(ompi_op_t * op)
{
    return (bool) (0 != (op->o_flags & OMPI_OP_FLAGS_COMMUTE));
}

/**
 * Check to see if an op is floating point associative or not
 *
 * @param op The op to check
 *
 * @returns true If the op is floating point associative
 * @returns false If the op is not floating point associative
 *
 * Self-explanitory.  This is needed in a few top-level MPI functions;
 * this function is provided to hide the internal structure field
 * names.
 */
static inline bool ompi_op_is_float_assoc(ompi_op_t * op)
{
    return (bool) (0 != (op->o_flags & OMPI_OP_FLAGS_FLOAT_ASSOC));
}


/**
 * Check to see if an op is valid on a given datatype
 *
 * @param op The op to check
 * @param ddt The datatype to check
 *
 * @returns true If the op is valid on that datatype
 * @returns false If the op is not valid on that datatype
 *
 * Self-explanitory.  This is needed in a few top-level MPI functions;
 * this function is provided to hide the internal structure field
 * names.
 */
static inline bool ompi_op_is_valid(ompi_op_t * op, ompi_datatype_t * ddt,
                                    char **msg, const char *func)
{
    /* Check:
       - non-intrinsic ddt's cannot be invoked on intrinsic op's
       - if intrinsic ddt invoked on intrinsic op:
       - ensure the datatype is defined in the op map
       - ensure we have a function pointer for that combination
     */

    if (ompi_op_is_intrinsic(op)) {
        if (ompi_datatype_is_predefined(ddt)) {
            /* Intrinsic ddt on intrinsic op */
            if (-1 == ompi_op_ddt_map[ddt->id] ||
                NULL == op->o_func.intrinsic.fns[ompi_op_ddt_map[ddt->id]]) {
                (void) asprintf(msg,
                                "%s: the reduction operation %s is not defined on the %s datatype",
                                func, op->o_name, ddt->name);
                return false;
            }
        } else {
            /* Non-intrinsic ddt on intrinsic op */
            if ('\0' != ddt->name[0]) {
                (void) asprintf(msg,
                                "%s: the reduction operation %s is not defined for non-intrinsic datatypes (attempted with datatype named \"%s\")",
                                func, op->o_name, ddt->name);
            } else {
                (void) asprintf(msg,
                                "%s: the reduction operation %s is not defined for non-intrinsic datatypes",
                                func, op->o_name);
            }
            return false;
        }
    }

    /* All other cases ok */
    return true;
}


/**
 * Perform a reduction operation.
 *
 * @param op The operation (IN)
 * @param source Source (input) buffer (IN)
 * @param target Target (output) buffer (IN/OUT)
 * @param count Number of elements (IN)
 * @param dtype MPI datatype (IN)
 *
 * @returns void As with MPI user-defined reduction functions, there
 * is no return code from this function.
 *
 * Perform a reduction operation with count elements of type dtype in
 * the buffers source and target.  The target buffer obtains the
 * result (i.e., the original values in the target buffer are reduced
 * with the values in the source buffer and the result is stored in
 * the target buffer).
 *
 * This function figures out which reduction operation function to
 * invoke and whether to invoke it with C- or Fortran-style invocation
 * methods.  If the op is intrinsic and has the operation defined for
 * dtype, the appropriate back-end function will be invoked.
 * Otherwise, the op is assumed to be a user op and the first function
 * pointer in the op array will be used.
 *
 * NOTE: This function assumes that a correct combination will be
 * given to it; it makes no provision for errors (in the name of
 * optimization).  If you give it an intrinsic op with a datatype that
 * is not defined to have that operation, it is likely to seg fault.
 */
static inline void ompi_op_reduce(ompi_op_t * op, void *source,
                                  void *target, int count,
                                  ompi_datatype_t * dtype)
{
    MPI_Fint f_dtype, f_count;

    /*
     * Call the reduction function.  Two dimensions: a) if both the op
     * and the datatype are intrinsic, we have a series of predefined
     * functions for each datatype (that are *only* in C -- not
     * Fortran or C++!), or b) the op is user-defined, and therefore
     * we have to check whether to invoke the callback with the C,
     * C++, or Fortran callback signature (see lengthy description of
     * the C++ callback in ompi/mpi/cxx/intercepts.cc).
     *
     * NOTE: We *assume* the following:
     *
     * 1. If the op is intrinsic, the op is pre-defined
     * 2. That we will get a valid result back from the
     * ompi_op_ddt_map[] (and not -1).
     *
     * Failures in these assumptions should have been caught by the
     * upper layer (i.e., they should never have called this
     * function).  If either of these assumptions are wrong, it's
     * likely that the MPI API function parameter checking is turned
     * off, then it's an erroneous program and it's the user's fault.
     * :-)
     */

    /* For intrinsics, we also pass the corresponding op module */
    if (0 != (op->o_flags & OMPI_OP_FLAGS_INTRINSIC)) {
        int dtype_id;
        if (!ompi_datatype_is_predefined(dtype)) {
            ompi_datatype_t *dt = ompi_datatype_get_single_predefined_type_from_args(dtype);
            dtype_id = ompi_op_ddt_map[dt->id];
        } else {
            dtype_id = ompi_op_ddt_map[dtype->id];
        }
        op->o_func.intrinsic.fns[dtype_id](source, target,
                                           &count, &dtype,
                                           op->o_func.intrinsic.modules[dtype_id]);
        return;
    }

    /* User-defined function */
    if (0 != (op->o_flags & OMPI_OP_FLAGS_FORTRAN_FUNC)) {
        f_dtype = OMPI_INT_2_FINT(dtype->d_f_to_c_index);
        f_count = OMPI_INT_2_FINT(count);
        op->o_func.fort_fn(source, target, &f_count, &f_dtype);
        return;
    } else if (0 != (op->o_flags & OMPI_OP_FLAGS_CXX_FUNC)) {
        op->o_func.cxx_data.intercept_fn(source, target, &count, &dtype,
                                         op->o_func.cxx_data.user_fn);
        return;
    } else if (0 != (op->o_flags & OMPI_OP_FLAGS_JAVA_FUNC)) {
        op->o_func.java_data.intercept_fn(source, target, &count, &dtype,
                                          op->o_func.java_data.baseType,
                                          op->o_func.java_data.jnienv,
                                          op->o_func.java_data.object);
        return;
    }
    op->o_func.c_fn(source, target, &count, &dtype);
    return;
}

static inline void ompi_3buff_op_user (ompi_op_t *op, void * restrict source1, void * restrict source2,
                                       void * restrict result, int count, struct ompi_datatype_t *dtype)
{
    ompi_datatype_copy_content_same_ddt (dtype, count, result, source1);
    op->o_func.c_fn (source2, result, &count, &dtype);
}

/**
 * Perform a reduction operation.
 *
 * @param op The operation (IN)
 * @param source Source1 (input) buffer (IN)
 * @param source Source2 (input) buffer (IN)
 * @param target Target (output) buffer (IN/OUT)
 * @param count Number of elements (IN)
 * @param dtype MPI datatype (IN)
 *
 * @returns void As with MPI user-defined reduction functions, there
 * is no return code from this function.
 *
 * Perform a reduction operation with count elements of type dtype in
 * the buffers source and target.  The target buffer obtains the
 * result (i.e., the original values in the target buffer are reduced
 * with the values in the source buffer and the result is stored in
 * the target buffer).
 *
 * This function will *only* be invoked on intrinsic MPI_Ops.
 *
 * Otherwise, this function is the same as ompi_op_reduce.
 */
static inline void ompi_3buff_op_reduce(ompi_op_t * op, void *source1,
                                        void *source2, void *target,
                                        int count, ompi_datatype_t * dtype)
{
    void *restrict src1;
    void *restrict src2;
    void *restrict tgt;
    src1 = source1;
    src2 = source2;
    tgt = target;

    if (OPAL_LIKELY(ompi_op_is_intrinsic (op))) {
        op->o_3buff_intrinsic.fns[ompi_op_ddt_map[dtype->id]](src1, src2,
                                                              tgt, &count,
                                                              &dtype,
                                                              op->o_3buff_intrinsic.modules[ompi_op_ddt_map[dtype->id]]);
    } else {
        ompi_3buff_op_user (op, src1, src2, tgt, count, dtype);
    }
}

END_C_DECLS

#endif /* OMPI_OP_H */
