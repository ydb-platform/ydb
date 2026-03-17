/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include "opal_config.h"

#include "opal/class/opal_object.h"
#include "opal/dss/dss.h"
#include "opal/util/output.h"
#include "opal/util/malloc.h"
#include "opal/util/net.h"
#include "opal/util/proc.h"
#include "opal/util/keyval_parse.h"
#include "opal/util/show_help.h"
#include "opal/memoryhooks/memory.h"
#include "opal/mca/base/base.h"
#include "opal/runtime/opal.h"
#include "opal/constants.h"
#include "opal/datatype/opal_datatype.h"
#include "opal/mca/if/base/base.h"
#include "opal/mca/installdirs/base/base.h"
#include "opal/mca/memchecker/base/base.h"
#include "opal/mca/memcpy/base/base.h"
#include "opal/mca/backtrace/base/base.h"
#include "opal/mca/reachable/base/base.h"
#include "opal/mca/timer/base/base.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/event/base/base.h"
#include "opal/runtime/opal_progress.h"
#include "opal/mca/shmem/base/base.h"
#if OPAL_ENABLE_FT_CR    == 1
#include "opal/mca/compress/base/base.h"
#endif

#include "opal/runtime/opal_cr.h"
#include "opal/mca/crs/base/base.h"
#include "opal/threads/tsd.h"

extern int opal_initialized;
extern int opal_util_initialized;
extern bool opal_init_called;

int
opal_finalize_util(void)
{
    if( --opal_util_initialized != 0 ) {
        if( opal_util_initialized < 0 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    /* close interfaces code. */
    (void) mca_base_framework_close(&opal_if_base_framework);

    (void) mca_base_framework_close(&opal_event_base_framework);

    /* Clear out all the registered MCA params */
    opal_deregister_params();
    mca_base_var_finalize();

    opal_net_finalize();

    /* keyval lex-based parser */
    opal_util_keyval_parse_finalize();

    (void) mca_base_framework_close(&opal_installdirs_base_framework);

    mca_base_close();

    /* finalize the memory allocator */
    opal_malloc_finalize();

    /* finalize the show_help system */
    opal_show_help_finalize();

    /* finalize the output system.  This has to come *after* the
       malloc code, as the malloc code needs to call into this, but
       the malloc code turning off doesn't affect opal_output that
       much */
    opal_output_finalize();

    /* close the dss */
    opal_dss_close();

    opal_datatype_finalize();

    /* finalize the class/object system */
    opal_class_finalize();

    free (opal_process_info.nodename);
    opal_process_info.nodename = NULL;

    return OPAL_SUCCESS;
}


int
opal_finalize(void)
{
    if( --opal_initialized != 0 ) {
        if( opal_initialized < 0 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    opal_progress_finalize();

    /* close the checkpoint and restart service */
    opal_cr_finalize();

#if OPAL_ENABLE_FT_CR    == 1
    (void) mca_base_framework_close(&opal_compress_base_framework);
#endif

    (void) mca_base_framework_close(&opal_reachable_base_framework);

    (void) mca_base_framework_close(&opal_event_base_framework);

    /* close high resolution timers */
    (void) mca_base_framework_close(&opal_timer_base_framework);

    (void) mca_base_framework_close(&opal_backtrace_base_framework);
    (void) mca_base_framework_close(&opal_memchecker_base_framework);

    /* close the memcpy framework */
    (void) mca_base_framework_close(&opal_memcpy_base_framework);

    /* finalize the memory manager / tracker */
    opal_mem_hooks_finalize();

    /* close the hwloc framework */
    (void) mca_base_framework_close(&opal_hwloc_base_framework);

    /* close the shmem framework */
    (void) mca_base_framework_close(&opal_shmem_base_framework);

    /* cleanup the main thread specific stuff */
    opal_tsd_keys_destruct();

    /* finalize util code */
    opal_finalize_util();

    return OPAL_SUCCESS;
}


void opal_finalize_test(void)
{
    /* Clear out all the registered MCA params */
    mca_base_var_finalize();

    (void) mca_base_framework_close(&opal_installdirs_base_framework);

    /* finalize the mca */
    mca_base_close();

    /* finalize the show_help system */
    opal_show_help_finalize();

    /* finalize the output system.  This has to come *after* the
       malloc code, as the malloc code needs to call into this, but
       the malloc code turning off doesn't affect opal_output that
       much */
    opal_output_finalize();

    /* close the dss */
    opal_dss_close();

    /* finalize the class/object system */
    opal_class_finalize();
}
