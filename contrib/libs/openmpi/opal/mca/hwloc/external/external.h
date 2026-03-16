/*
 * Copyright (c) 2011-2019 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2016-2019 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * When this component is used, this file is included in the rest of
 * the OPAL/ORTE/OMPI code base via opal/mca/hwloc/hwloc-internal.h.  As such,
 * this header represents the public interface to this static component.
 */

#ifndef MCA_OPAL_HWLOC_EXTERNAL_H
#define MCA_OPAL_HWLOC_EXTERNAL_H

BEGIN_C_DECLS

#include <opal_config.h>

/* Top-level configure will always configure the embedded hwloc
 * component, even if we already know that we'll be using an external
 * hwloc (because of complicated reasons). A side-effect of this is
 * that the embedded hwloc will AC_DEFINE HWLOC_VERSION (and friends)
 * in opal_config.h. If the external hwloc defines a different value
 * of HWLOC_VERSION (etc.), we'll get zillions of warnings about the
 * two HWLOC_VERSION values not matching.  Hence, we undefined all of
 * them here (so that the external <hwloc.h> can define them to
 * whatever it wants). */

#undef HWLOC_VERSION
#undef HWLOC_VERSION_MAJOR
#undef HWLOC_VERSION_MINOR
#undef HWLOC_VERSION_RELEASE
#undef HWLOC_VERSION_GREEK

#include MCA_hwloc_external_header

/* If the including file requested it, also include the hwloc verbs
   helper file.  We can't just always include this file (even if we
   know we have <infiniband/verbs.h>) because there are some inline
   functions in that file that invoke ibv_* functions.  Some linkers
   (e.g., Solaris Studio Compilers) will instantiate those static
   inline functions even if we don't use them, and therefore we need
   to be able to resolve the ibv_* symbols at link time.

   Since -libverbs is only specified in places where we use other
   ibv_* functions (e.g., the OpenFabrics-based BTLs), that means that
   linking random executables can/will fail (e.g., orterun).
 */
#if defined(OPAL_HWLOC_WANT_VERBS_HELPER) && OPAL_HWLOC_WANT_VERBS_HELPER
#    if defined(HAVE_INFINIBAND_VERBS_H)
#        error #include MCA_hwloc_external_openfabrics_header
#    else
#        error Tried to include hwloc verbs helper file, but hwloc was compiled with no OpenFabrics support
#    endif
#endif

#if defined(OPAL_HWLOC_WANT_SHMEM) && OPAL_HWLOC_WANT_SHMEM
#    if HWLOC_API_VERSION >= 0x20000
#        include MCA_hwloc_external_shmem_header
#    endif
/* Do nothing in the 1.x case because the caller doesn't know HWLOC_API_VERSION when it sets OPAL_HWLOC_WANT_SHMEM.
 * Calls to hwloc/shmem.h are protected by HWLOC_API_VERSION >= 0x20000 in the actual code.
 */
#endif

#if HWLOC_API_VERSION < 0x00010b00
#define HWLOC_OBJ_NUMANODE HWLOC_OBJ_NODE
#define HWLOC_OBJ_PACKAGE HWLOC_OBJ_SOCKET
#endif

END_C_DECLS

#endif /* MCA_OPAL_HWLOC_EXTERNAL_H */
