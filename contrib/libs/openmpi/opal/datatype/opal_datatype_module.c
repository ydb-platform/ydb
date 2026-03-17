/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2018 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
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

#include "opal_config.h"

#include <stddef.h>

#include "opal/util/arch.h"
#include "opal/util/output.h"
#include "opal/datatype/opal_datatype_internal.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/datatype/opal_convertor_internal.h"
#include "opal/mca/base/mca_base_var.h"

/* by default the debuging is turned off */
int opal_datatype_dfd = -1;
bool opal_unpack_debug = false;
bool opal_pack_debug = false;
bool opal_position_debug = false;
bool opal_copy_debug = false;
int opal_ddt_verbose = -1;  /* Has the datatype verbose it's own output stream */

extern int opal_cuda_verbose;

/* Using this macro implies that at this point _all_ informations needed
 * to fill up the datatype are known.
 * We fill all the static information, the pointer to desc.desc is setup
 * into an array, which is initialized at runtime.
 * Everything is constant.
 */
OPAL_DECLSPEC const opal_datatype_t opal_datatype_empty =       OPAL_DATATYPE_INITIALIZER_EMPTY(0);

OPAL_DECLSPEC const opal_datatype_t opal_datatype_loop =        OPAL_DATATYPE_INITIALIZER_LOOP(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_end_loop =    OPAL_DATATYPE_INITIALIZER_END_LOOP(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_lb =          OPAL_DATATYPE_INITIALIZER_LB(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_ub =          OPAL_DATATYPE_INITIALIZER_UB(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_int1 =        OPAL_DATATYPE_INITIALIZER_INT1(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_int2 =        OPAL_DATATYPE_INITIALIZER_INT2(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_int4 =        OPAL_DATATYPE_INITIALIZER_INT4(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_int8 =        OPAL_DATATYPE_INITIALIZER_INT8(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_int16 =       OPAL_DATATYPE_INITIALIZER_INT16(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_uint1 =       OPAL_DATATYPE_INITIALIZER_UINT1(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_uint2 =       OPAL_DATATYPE_INITIALIZER_UINT2(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_uint4 =       OPAL_DATATYPE_INITIALIZER_UINT4(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_uint8 =       OPAL_DATATYPE_INITIALIZER_UINT8(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_uint16 =      OPAL_DATATYPE_INITIALIZER_UINT16(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float2 =      OPAL_DATATYPE_INITIALIZER_FLOAT2(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float4 =      OPAL_DATATYPE_INITIALIZER_FLOAT4(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float8 =      OPAL_DATATYPE_INITIALIZER_FLOAT8(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float12 =     OPAL_DATATYPE_INITIALIZER_FLOAT12(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float16 =     OPAL_DATATYPE_INITIALIZER_FLOAT16(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_float_complex = OPAL_DATATYPE_INITIALIZER_FLOAT_COMPLEX(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_double_complex = OPAL_DATATYPE_INITIALIZER_DOUBLE_COMPLEX(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_long_double_complex = OPAL_DATATYPE_INITIALIZER_LONG_DOUBLE_COMPLEX(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_bool =        OPAL_DATATYPE_INITIALIZER_BOOL(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_wchar =       OPAL_DATATYPE_INITIALIZER_WCHAR(0);
OPAL_DECLSPEC const opal_datatype_t opal_datatype_unavailable = OPAL_DATATYPE_INITIALIZER_UNAVAILABLE_NAMED(UNAVAILABLE, 0);

OPAL_DECLSPEC dt_elem_desc_t opal_datatype_predefined_elem_desc[2 * OPAL_DATATYPE_MAX_PREDEFINED] = {{{{0}}}};

/*
 * NOTE: The order of this array *MUST* match the order in opal_datatype_basicDatatypes
 * (use of designated initializers should relax this restrictions some)
 */
OPAL_DECLSPEC const size_t opal_datatype_local_sizes[OPAL_DATATYPE_MAX_PREDEFINED] =
{
    [OPAL_DATATYPE_INT1] = sizeof(int8_t),
    [OPAL_DATATYPE_INT2] = sizeof(int16_t),
    [OPAL_DATATYPE_INT4] = sizeof(int32_t),
    [OPAL_DATATYPE_INT8] = sizeof(int64_t),
    [OPAL_DATATYPE_INT16] = 16,    /* sizeof (int128_t) */
    [OPAL_DATATYPE_UINT1] = sizeof(uint8_t),
    [OPAL_DATATYPE_UINT2] = sizeof(uint16_t),
    [OPAL_DATATYPE_UINT4] = sizeof(uint32_t),
    [OPAL_DATATYPE_UINT8] = sizeof(uint64_t),
    [OPAL_DATATYPE_UINT16] = 16,    /* sizeof (uint128_t) */
    [OPAL_DATATYPE_FLOAT2] = 2,     /* sizeof (float2) */
    [OPAL_DATATYPE_FLOAT4] = 4,     /* sizeof (float4) */
    [OPAL_DATATYPE_FLOAT8] = 8,     /* sizeof (float8) */
    [OPAL_DATATYPE_FLOAT12] = 12,   /* sizeof (float12) */
    [OPAL_DATATYPE_FLOAT16] = 16,   /* sizeof (float16) */
    [OPAL_DATATYPE_FLOAT_COMPLEX] = sizeof(float _Complex),
    [OPAL_DATATYPE_DOUBLE_COMPLEX] = sizeof(double _Complex),
    [OPAL_DATATYPE_LONG_DOUBLE_COMPLEX] = sizeof(long double _Complex),
    [OPAL_DATATYPE_BOOL] = sizeof (_Bool),
    [OPAL_DATATYPE_WCHAR] = sizeof (wchar_t),
};

/*
 * NOTE: The order of this array *MUST* match what is listed in datatype.h
 * (use of designated initializers should relax this restrictions some)
 */
OPAL_DECLSPEC const opal_datatype_t* opal_datatype_basicDatatypes[OPAL_DATATYPE_MAX_PREDEFINED] = {
    [OPAL_DATATYPE_LOOP] = &opal_datatype_loop,
    [OPAL_DATATYPE_END_LOOP] = &opal_datatype_end_loop,
    [OPAL_DATATYPE_LB] = &opal_datatype_lb,
    [OPAL_DATATYPE_UB] = &opal_datatype_ub,
    [OPAL_DATATYPE_INT1] = &opal_datatype_int1,
    [OPAL_DATATYPE_INT2] = &opal_datatype_int2,
    [OPAL_DATATYPE_INT4] = &opal_datatype_int4,
    [OPAL_DATATYPE_INT8] = &opal_datatype_int8,
    [OPAL_DATATYPE_INT16] = &opal_datatype_int16,       /* Yes, double-machine word integers are available */
    [OPAL_DATATYPE_UINT1] = &opal_datatype_uint1,
    [OPAL_DATATYPE_UINT2] = &opal_datatype_uint2,
    [OPAL_DATATYPE_UINT4] = &opal_datatype_uint4,
    [OPAL_DATATYPE_UINT8] = &opal_datatype_uint8,
    [OPAL_DATATYPE_UINT16] = &opal_datatype_uint16,      /* Yes, double-machine word integers are available */
    [OPAL_DATATYPE_FLOAT2] = &opal_datatype_float2,
    [OPAL_DATATYPE_FLOAT4] = &opal_datatype_float4,
    [OPAL_DATATYPE_FLOAT8] = &opal_datatype_float8,
    [OPAL_DATATYPE_FLOAT12] = &opal_datatype_float12,
    [OPAL_DATATYPE_FLOAT16] = &opal_datatype_float16,
    [OPAL_DATATYPE_FLOAT_COMPLEX] = &opal_datatype_float_complex,
    [OPAL_DATATYPE_DOUBLE_COMPLEX] = &opal_datatype_double_complex,
    [OPAL_DATATYPE_LONG_DOUBLE_COMPLEX] = &opal_datatype_long_double_complex,
    [OPAL_DATATYPE_BOOL] = &opal_datatype_bool,
    [OPAL_DATATYPE_WCHAR] = &opal_datatype_wchar,
    [OPAL_DATATYPE_UNAVAILABLE] = &opal_datatype_unavailable,
};


int opal_datatype_register_params(void)
{
#if OPAL_ENABLE_DEBUG
    int ret;

    ret = mca_base_var_register ("opal", "mpi", NULL, "ddt_unpack_debug",
				 "Whether to output debugging information in the ddt unpack functions (nonzero = enabled)",
				 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
				 MCA_BASE_VAR_SCOPE_LOCAL, &opal_unpack_debug);
    if (0 > ret) {
	return ret;
    }

    ret = mca_base_var_register ("opal", "mpi", NULL, "ddt_pack_debug",
				 "Whether to output debugging information in the ddt pack functions (nonzero = enabled)",
				 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
				 MCA_BASE_VAR_SCOPE_LOCAL, &opal_pack_debug);
    if (0 > ret) {
	return ret;
    }

    ret = mca_base_var_register ("opal", "mpi", NULL, "ddt_position_debug",
				 "Non zero lead to output generated by the datatype position functions",
				 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
				 MCA_BASE_VAR_SCOPE_LOCAL, &opal_position_debug);
    if (0 > ret) {
	return ret;
    }

    ret = mca_base_var_register ("opal", "mpi", NULL, "ddt_copy_debug",
				 "Whether to output debugging information in the ddt copy functions (nonzero = enabled)",
				 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE, OPAL_INFO_LVL_3,
				 MCA_BASE_VAR_SCOPE_LOCAL, &opal_copy_debug);
    if (0 > ret) {
	return ret;
    }

    ret = mca_base_var_register ("opal", "opal", NULL, "ddt_verbose",
                                 "Set level of opal datatype verbosity",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                 OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_LOCAL,
                                 &opal_ddt_verbose);
    if (0 > ret) {
        return ret;
    }
#if OPAL_CUDA_SUPPORT
    /* Set different levels of verbosity in the cuda related code. */
    ret = mca_base_var_register ("opal", "opal", NULL, "cuda_verbose",
                                 "Set level of opal cuda verbosity",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, MCA_BASE_VAR_FLAG_SETTABLE,
                                 OPAL_INFO_LVL_8, MCA_BASE_VAR_SCOPE_LOCAL,
                                 &opal_cuda_verbose);
    if (0 > ret) {
	return ret;
    }
#endif

#endif /* OPAL_ENABLE_DEBUG */

    return OPAL_SUCCESS;
}


int32_t opal_datatype_init( void )
{
    const opal_datatype_t* datatype;
    int32_t i;

    /**
     * Force he initialization of the opal_datatype_t class. This will allow us to
     * call OBJ_DESTRUCT without going too deep in the initialization process.
     */
    opal_class_initialize(OBJ_CLASS(opal_datatype_t));
    for( i = OPAL_DATATYPE_FIRST_TYPE; i < OPAL_DATATYPE_MAX_PREDEFINED; i++ ) {
        datatype = opal_datatype_basicDatatypes[i];

        /* All of the predefined OPAL types don't have any GAPS! */
        datatype->desc.desc[0].elem.common.flags = OPAL_DATATYPE_FLAG_PREDEFINED |
                                                   OPAL_DATATYPE_FLAG_DATA |
                                                   OPAL_DATATYPE_FLAG_CONTIGUOUS |
                                                   OPAL_DATATYPE_FLAG_NO_GAPS;
        datatype->desc.desc[0].elem.common.type  = i;
        /* datatype->desc.desc[0].elem.blocklen XXX not set at the moment, it will be needed later */
        datatype->desc.desc[0].elem.count        = 1;
        datatype->desc.desc[0].elem.disp         = 0;
        datatype->desc.desc[0].elem.extent       = datatype->size;

        datatype->desc.desc[1].end_loop.common.flags    = 0;
        datatype->desc.desc[1].end_loop.common.type     = OPAL_DATATYPE_END_LOOP;
        datatype->desc.desc[1].end_loop.items           = 1;
        datatype->desc.desc[1].end_loop.first_elem_disp = datatype->desc.desc[0].elem.disp;
        datatype->desc.desc[1].end_loop.size            = datatype->size;
    }

    /* Enable a private output stream for datatype */
    if( opal_ddt_verbose > 0 ) {
        opal_datatype_dfd = opal_output_open(NULL);
        opal_output_set_verbosity(opal_datatype_dfd, opal_ddt_verbose);
    }

    return OPAL_SUCCESS;
}


int32_t opal_datatype_finalize( void )
{
    /* As the synonyms are just copies of the internal data we should not free them.
     * Anyway they are over the limit of OPAL_DATATYPE_MAX_PREDEFINED so they will never get freed.
     */

    /* As they are statically allocated they cannot be released. But we
     * can call OBJ_DESTRUCT, just to free all internally allocated ressources.
     */
#if defined(VERBOSE)
    if( opal_datatype_dfd != -1 )
        opal_output_close( opal_datatype_dfd );
    opal_datatype_dfd = -1;
#endif /* VERBOSE */

    /* clear all master convertors */
    opal_convertor_destroy_masters();

    return OPAL_SUCCESS;
}

#if OPAL_ENABLE_DEBUG
/*
 * Set a breakpoint to this function in your favorite debugger
 * to make it stop on all pack and unpack errors.
 */
int opal_datatype_safeguard_pointer_debug_breakpoint( const void* actual_ptr, int length,
                                                      const void* initial_ptr,
                                                      const opal_datatype_t* pData,
                                                      int count )
{
    return 0;
}
#endif  /* OPAL_ENABLE_DEBUG */
