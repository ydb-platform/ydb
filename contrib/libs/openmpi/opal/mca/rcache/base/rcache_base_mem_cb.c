/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 */
#include "opal_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/util/show_help.h"
#include "opal/util/proc.h"
#include "opal/runtime/opal_params.h"

#include "opal/mca/rcache/base/rcache_base_mem_cb.h"
#include "opal/mca/rcache/base/base.h"

#include "opal/mca/mca.h"
#include "opal/memoryhooks/memory.h"

static char msg[512];


/*
 *  memory hook callback, called when memory is free'd out from under
 *  us.  Be wary of the from_alloc flag -- if you're called with
 *  from_alloc==true, then you cannot call malloc (or any of its
 *  friends)!
 */
void mca_rcache_base_mem_cb (void* base, size_t size, void* cbdata, bool from_alloc)
{
    mca_rcache_base_selected_module_t* current;
    int rc;

    /* Only do anything meaningful if the OPAL layer is up and running
       and size != 0 */
    if ((from_alloc && (!opal_initialized)) || size == 0) {
        return;
    }

    OPAL_LIST_FOREACH(current, &mca_rcache_base_modules, mca_rcache_base_selected_module_t) {
        if (current->rcache_module->rcache_invalidate_range != NULL) {
            rc = current->rcache_module->rcache_invalidate_range (current->rcache_module,
                                                                  base, size);
            if (rc != OPAL_SUCCESS) {
                if (from_alloc) {
                    int len;
                    len = snprintf(msg, sizeof(msg), "[%s:%05d] Attempt to free memory that is still in "
                                   "use by an ongoing MPI communication (buffer %p, size %lu).  MPI job "
                                   "will now abort.\n", opal_proc_local_get()->proc_hostname,
                                   getpid(), base, (unsigned long) size);
                    msg[sizeof(msg) - 1] = '\0';
                    write(2, msg, len);
                } else {
                    opal_show_help("help-rcache-base.txt",
                                   "cannot deregister in-use memory", true,
                                   current->rcache_component->rcache_version.mca_component_name,
                                   opal_proc_local_get()->proc_hostname,
                                   base, (unsigned long) size);
                }

                /* We're in a callback from somewhere; we can't do
                   anything meaningful to pass an error back up.  :-(
                   So just exit.  Call _exit() so that we don't try to
                   call anything on the way out -- just exit!
                   (remember that we're in a callback, and state may
                   be very undefined at this point...) */
                _exit(1);
            }
        }
    }
}
