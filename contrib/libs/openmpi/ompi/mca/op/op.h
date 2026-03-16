/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2007-2008 UT-Battelle, LLC
 * Copyright (c) 2007-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2013-2015 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * MPI_Op back-end operation framework.  This framework allows
 * component-izing the back-end operations of MPI_Op in order to use
 * specialized hardware (e.g., mathematical accelerators).  In short:
 * each MPI_Op contains a table of function pointers; one for
 * implementing the operation on each predefined datatype.
 *
 * The MPI interface provides error checking and error handler
 * invocation, but the op components provide all other functionality.
 *
 * Component selection is done on a per-MPI_Op basis when each MPI_Op
 * is created.  All MPI_Ops go through the selection process, even
 * user-defined MPI_Ops -- although it is expected that most (all?)
 * op components will only be able to handle the predefined MPI_Ops.
 *
 * The general sequence of usage for the op framework is:
 *
 * 1. ompi_op_base_open() is invoked during MPI_INIT to find/open all
 * op components.
 *
 * 2. ompi_op_base_find_available() is invoked during MPI_INIT to call
 * each successfully opened op component's opc_init_query() function.
 * All op components that return OMPI_SUCCESS are kept; all others are
 * closed and removed from the process.
 *
 * 3. ompi_op_base_op_select() is invoked during MPI_INIT for each
 * predefined MPI_Op (e.g., MPI_SUM).  This function will call each
 * available op component's opc_op_query() function to see if this
 * component wants to provide a module for one or more of the function
 * pointers on this MPI_Op.  Priorities are used to rank returned
 * modules; the module with the highest priority has its function
 * pointers set in the MPI_Op function table.
 *
 * Note that a module may only have *some* non-NULL function pointers
 * (i.e., for the functions that it can support).  For example, some
 * modules may only support operations on single-precision floating
 * point datatypes.  These modules would provide function pointers for
 * these datatypes and NULL for all the rest.  The op framework will
 * mix-n-match function pointers between modules to obtain a full set
 * of non-NULL function pointers for a given MPI_Op (note that the op
 * base provides a complete set of functions for the MPI_Op, usually a
 * simple C loop around the operation, such as "+=" -- so even if
 * there is no specialized op component available, there will *always*
 * be a full set of MPI_Op function pointers).  The op framework will
 * OBJ_RETAIN an op module once for each function pointer where it is
 * used on a given MPI_Op.
 *
 * Note that this scheme can result in up to N different modules being
 * used for a single MPI_Op, one per needed datatype function.
 *
 * 5. Finally, during MPI_FINALIZE, ompi_op_base_close() is invoked to
 * close all available op components.
 */

#ifndef MCA_OP_H
#define MCA_OP_H

#include "ompi_config.h"

#include "opal/class/opal_object.h"
#include "ompi/mca/mca.h"

/*
 * This file includes some basic struct declarations (but not
 * definitions) just so that we can avoid including files like op/op.h
 * and datatype/datatype.h, which would create #include file loops.
 */
#include "ompi/types.h"

BEGIN_C_DECLS

/**
 * Corresponding to the types that we can reduce over.  See
 * MPI-1:4.9.2, p114-115 and
 * MPI-2:4.15, p76-77
 */
enum {
    /** C integer: int8_t */
    OMPI_OP_BASE_TYPE_INT8_T,
    /** C integer: uint8_t */
    OMPI_OP_BASE_TYPE_UINT8_T,
    /** C integer: int16_t */
    OMPI_OP_BASE_TYPE_INT16_T,
    /** C integer: uint16_t */
    OMPI_OP_BASE_TYPE_UINT16_T,
    /** C integer: int32_t */
    OMPI_OP_BASE_TYPE_INT32_T,
    /** C integer: uint32_t */
    OMPI_OP_BASE_TYPE_UINT32_T,
    /** C integer: int64_t */
    OMPI_OP_BASE_TYPE_INT64_T,
    /** C integer: uint64_t */
    OMPI_OP_BASE_TYPE_UINT64_T,

    /** Fortran integer */
    OMPI_OP_BASE_TYPE_INTEGER,
    /** Fortran integer*1 */
    OMPI_OP_BASE_TYPE_INTEGER1,
    /** Fortran integer*2 */
    OMPI_OP_BASE_TYPE_INTEGER2,
    /** Fortran integer*4 */
    OMPI_OP_BASE_TYPE_INTEGER4,
    /** Fortran integer*8 */
    OMPI_OP_BASE_TYPE_INTEGER8,
    /** Fortran integer*16 */
    OMPI_OP_BASE_TYPE_INTEGER16,

    /** Floating point: float */
    OMPI_OP_BASE_TYPE_FLOAT,
    /** Floating point: double */
    OMPI_OP_BASE_TYPE_DOUBLE,
    /** Floating point: real */
    OMPI_OP_BASE_TYPE_REAL,
    /** Floating point: real*2 */
    OMPI_OP_BASE_TYPE_REAL2,
    /** Floating point: real*4 */
    OMPI_OP_BASE_TYPE_REAL4,
    /** Floating point: real*8 */
    OMPI_OP_BASE_TYPE_REAL8,
    /** Floating point: real*16 */
    OMPI_OP_BASE_TYPE_REAL16,
    /** Floating point: double precision */
    OMPI_OP_BASE_TYPE_DOUBLE_PRECISION,
    /** Floating point: long double */
    OMPI_OP_BASE_TYPE_LONG_DOUBLE,

    /** Logical */
    OMPI_OP_BASE_TYPE_LOGICAL,
    /** Bool */
    OMPI_OP_BASE_TYPE_BOOL,

    /** Complex */
    /* float complex */
    OMPI_OP_BASE_TYPE_C_FLOAT_COMPLEX,
    /* double complex */
    OMPI_OP_BASE_TYPE_C_DOUBLE_COMPLEX,
    /* long double complex */
    OMPI_OP_BASE_TYPE_C_LONG_DOUBLE_COMPLEX,

    /** Byte */
    OMPI_OP_BASE_TYPE_BYTE,

    /** 2 location Fortran: 2 real */
    OMPI_OP_BASE_TYPE_2REAL,
    /** 2 location Fortran: 2 double precision */
    OMPI_OP_BASE_TYPE_2DOUBLE_PRECISION,
    /** 2 location Fortran: 2 integer */
    OMPI_OP_BASE_TYPE_2INTEGER,

    /** 2 location C: float int */
    OMPI_OP_BASE_TYPE_FLOAT_INT,
    /** 2 location C: double int */
    OMPI_OP_BASE_TYPE_DOUBLE_INT,
    /** 2 location C: long int */
    OMPI_OP_BASE_TYPE_LONG_INT,
    /** 2 location C: int int */
    OMPI_OP_BASE_TYPE_2INT,
    /** 2 location C: short int */
    OMPI_OP_BASE_TYPE_SHORT_INT,
    /** 2 location C: long double int */
    OMPI_OP_BASE_TYPE_LONG_DOUBLE_INT,

    /** 2 location C: wchar_t */
    OMPI_OP_BASE_TYPE_WCHAR,

    /** Maximum type */
    OMPI_OP_BASE_TYPE_MAX
};


/**
 * Fortran handles; must be [manually set to be] equivalent to the
 * values in mpif.h.
 */
enum {
    /** Corresponds to Fortran MPI_OP_NULL */
    OMPI_OP_BASE_FORTRAN_NULL = 0,
    /** Corresponds to Fortran MPI_MAX */
    OMPI_OP_BASE_FORTRAN_MAX,
    /** Corresponds to Fortran MPI_MIN */
    OMPI_OP_BASE_FORTRAN_MIN,
    /** Corresponds to Fortran MPI_SUM */
    OMPI_OP_BASE_FORTRAN_SUM,
    /** Corresponds to Fortran MPI_PROD */
    OMPI_OP_BASE_FORTRAN_PROD,
    /** Corresponds to Fortran MPI_LAND */
    OMPI_OP_BASE_FORTRAN_LAND,
    /** Corresponds to Fortran MPI_BAND */
    OMPI_OP_BASE_FORTRAN_BAND,
    /** Corresponds to Fortran MPI_LOR */
    OMPI_OP_BASE_FORTRAN_LOR,
    /** Corresponds to Fortran MPI_BOR */
    OMPI_OP_BASE_FORTRAN_BOR,
    /** Corresponds to Fortran MPI_LXOR */
    OMPI_OP_BASE_FORTRAN_LXOR,
    /** Corresponds to Fortran MPI_BXOR */
    OMPI_OP_BASE_FORTRAN_BXOR,
    /** Corresponds to Fortran MPI_MAXLOC */
    OMPI_OP_BASE_FORTRAN_MAXLOC,
    /** Corresponds to Fortran MPI_MINLOC */
    OMPI_OP_BASE_FORTRAN_MINLOC,
    /** Corresponds to Fortran MPI_REPLACE */
    OMPI_OP_BASE_FORTRAN_REPLACE,
    /** Corresponds to Fortran MPI_NO_OP */
    OMPI_OP_BASE_FORTRAN_NO_OP,

    /** Maximum value */
    OMPI_OP_BASE_FORTRAN_OP_MAX
};

/**
 * Pre-declare this so that we can pass it as an argument to the
 * typedef'ed functions.
 */
struct ompi_op_base_module_1_0_0_t;

typedef struct ompi_op_base_module_1_0_0_t ompi_op_base_module_t;

/**
 * Typedef for 2-buffer op functions.
 *
 * We don't use MPI_User_function because this would create a
 * confusing dependency loop between this file and mpi.h.  So this is
 * repeated code, but it's better this way (and this typedef will
 * never change, so there's not much of a maintenance worry).
 */
typedef void (*ompi_op_base_handler_fn_1_0_0_t)(void *, void *, int *,
                                                struct ompi_datatype_t **,
                                                struct ompi_op_base_module_1_0_0_t *);

typedef ompi_op_base_handler_fn_1_0_0_t ompi_op_base_handler_fn_t;

/*
 * Typedef for 3-buffer (two input and one output) op functions.
 */
typedef void (*ompi_op_base_3buff_handler_fn_1_0_0_t)(void *,
                                                      void *,
                                                      void *, int *,
                                                      struct ompi_datatype_t **,
                                                      struct ompi_op_base_module_1_0_0_t *);

typedef ompi_op_base_3buff_handler_fn_1_0_0_t ompi_op_base_3buff_handler_fn_t;

/**
 * Op component initialization
 *
 * Initialize the given op component.  This function should initialize
 * any component-level. data.  It will be called exactly once during
 * MPI_INIT.
 *
 * @note The component framework is not lazily opened, so attempts
 * should be made to minimze the amount of memory allocated during
 * this function.
 *
 * @param[in] enable_progress_threads True if the component needs to
 *                                support progress threads
 * @param[in] enable_mpi_threads  True if the component needs to
 *                                support MPI_THREAD_MULTIPLE
 *
 * @retval OMPI_SUCCESS Component successfully initialized
 * @retval OMPI_ERROR   An unspecified error occurred
 */
typedef int (*ompi_op_base_component_init_query_fn_t)
     (bool enable_progress_threads, bool enable_mpi_threads);


/**
 * Query whether a component is available for a specific MPI_Op.
 *
 * If the component is available, an object should be allocated and
 * returned (with refcount at 1).  The module will not be used for
 * reduction operations until module_enable() is called on the module,
 * but may be destroyed (via OBJ_RELEASE) either before or after
 * module_enable() is called.  If the module needs to release
 * resources obtained during query(), it should do so in the module
 * destructor.
 *
 * A component may provide NULL to this function to indicate it does
 * not wish to run or return an error during module_enable().
 *
 * @param[in] op          The MPI_Op being created
 * @param[out] priority   Priority setting for component on
 *                        this op
 *
 * @returns An initialized module structure if the component can
 * provide a module with the requested functionality or NULL if the
 * component should not be used on the given communicator.
 */
typedef struct ompi_op_base_module_1_0_0_t *
  (*ompi_op_base_component_op_query_1_0_0_fn_t)
    (struct ompi_op_t *op, int *priority);

/**
 * Op component interface.
 *
 * Component interface for the op framework.  A public instance of
 * this structure, called mca_op_[component_name]_component, must
 * exist in any op component.
 */
typedef struct ompi_op_base_component_1_0_0_t {
    /** Base component description */
    mca_base_component_t opc_version;
    /** Base component data block */
    mca_base_component_data_t opc_data;

    /** Component initialization function */
    ompi_op_base_component_init_query_fn_t opc_init_query;
    /** Query whether component is useable for given op */
    ompi_op_base_component_op_query_1_0_0_fn_t opc_op_query;
} ompi_op_base_component_1_0_0_t;


/** Per guidence in mca.h, use the unversioned struct name if you just
    want to always keep up with the most recent version of the
    interace. */
typedef struct ompi_op_base_component_1_0_0_t ompi_op_base_component_t;

/**
 * Module initialization function.  Should return OPAL_SUCCESS if
 * everything goes ok.  This function can be NULL in the module struct
 * if the module doesn't need to do anything between the component
 * query function and being invoked for MPI_Op operations.
 */
typedef int (*ompi_op_base_module_enable_1_0_0_fn_t)
    (struct ompi_op_base_module_1_0_0_t *module,
     struct ompi_op_t *op);

/**
 * Module struct
 */
typedef struct ompi_op_base_module_1_0_0_t {
    /** Op modules all inherit from opal_object */
    opal_object_t super;

    /** Enable function called when an op module is (possibly) going
        to be used for the given MPI_Op */
    ompi_op_base_module_enable_1_0_0_fn_t opm_enable;

    /** Just for reference -- a pointer to the MPI_Op that this module
        is being used for */
    struct ompi_op_t *opm_op;

    /** Function pointers for all the different datatypes to be used
        with the MPI_Op that this module is used with */
    ompi_op_base_handler_fn_1_0_0_t opm_fns[OMPI_OP_BASE_TYPE_MAX];
    ompi_op_base_3buff_handler_fn_1_0_0_t opm_3buff_fns[OMPI_OP_BASE_TYPE_MAX];
} ompi_op_base_module_1_0_0_t;

/**
 * Declare the module as a class, unversioned
 */
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_op_base_module_t);

/**
 * Declare the module as a class, unversioned
 */
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_op_base_module_1_0_0_t);

/**
 * Struct that is used in op.h to hold all the function pointers and
 * pointers to the corresopnding modules (so that we can properly
 * RETAIN/RELEASE them)
 */
typedef struct ompi_op_base_op_fns_1_0_0_t {
    ompi_op_base_handler_fn_1_0_0_t fns[OMPI_OP_BASE_TYPE_MAX];
    ompi_op_base_module_t *modules[OMPI_OP_BASE_TYPE_MAX];
} ompi_op_base_op_fns_1_0_0_t;

typedef ompi_op_base_op_fns_1_0_0_t ompi_op_base_op_fns_t;

/**
 * Struct that is used in op.h to hold all the function pointers and
 * pointers to the corresopnding modules (so that we can properly
 * RETAIN/RELEASE them)
 */
typedef struct ompi_op_base_op_3buff_fns_1_0_0_t {
    ompi_op_base_3buff_handler_fn_1_0_0_t fns[OMPI_OP_BASE_TYPE_MAX];
    ompi_op_base_module_t *modules[OMPI_OP_BASE_TYPE_MAX];
} ompi_op_base_op_3buff_fns_1_0_0_t;

typedef ompi_op_base_op_3buff_fns_1_0_0_t ompi_op_base_op_3buff_fns_t;

/*
 * Macro for use in modules that are of type op v2.0.0
 */
#define OMPI_OP_BASE_VERSION_1_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("op", 1, 0, 0)

END_C_DECLS

#endif /* OMPI_MCA_OP_H */
