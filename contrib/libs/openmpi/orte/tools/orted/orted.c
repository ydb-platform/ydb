/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "orte_config.h"

#include <stdlib.h>
#include <limits.h>
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#include <stdio.h>
#include <errno.h>

#include "orte/orted/orted.h"

int main(int argc, char *argv[])
{
    /* Allow the PLM starters to pass us a umask to use, if required.
       Most starters by default can do something sane with the umask,
       but some (like TM) do not pass on the umask but instead inherit
       it form the root level process starter.  This has to happen
       before opal_init and everything else so that the couple of
       places that stash a umask end up with the correct value.  Only
       do it here (and not in orte_daemon) mainly to make it clear
       that this should only happen when starting an orted for the
       first time.  All startes I'm aware of that don't require an
       orted are smart enough to pass on a reasonable umask, so they
       wouldn't need this functionality anyway. */
    char *umask_str = getenv("ORTE_DAEMON_UMASK_VALUE");
    if (NULL != umask_str) {
        char *endptr;
        long mask = strtol(umask_str, &endptr, 8);
        if ((! (0 == mask && (EINVAL == errno || ERANGE == errno))) &&
            (*endptr == '\0')) {
            umask(mask);
        }
    }

    return orte_daemon(argc, argv);
}
