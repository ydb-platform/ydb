/*
 * Copyright 2015 The py-lmdb authors, all rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * OpenLDAP is a registered trademark of the OpenLDAP Foundation.
 *
 * Individual files and/or contributed packages may be copyright by
 * other parties and/or subject to additional restrictions.
 *
 * This work also contains materials derived from public sources.
 *
 * Additional information about OpenLDAP can be obtained at
 * <http://www.openldap.org/>.
 */

#ifndef LMDB_PRELOAD_H
#define LMDB_PRELOAD_H

#include <stdlib.h>

/**
 * Touch a byte from every page in `x`, causing any read faults necessary for
 * copying the value to occur. This should be called with the GIL released, in
 * order to dramatically decrease the chances of a page fault being taken with
 * the GIL held.
 *
 * We do this since PyMalloc cannot be invoked with the GIL released, and we
 * cannot know the size of the MDB result value before dropping the GIL. This
 * seems the simplest and cheapest compromise to ensuring multithreaded Python
 * apps don't hard stall when dealing with a database larger than RAM.
 */
static void preload(int rc, void *x, size_t size) {
    if(rc == 0) {
        volatile char j;
        size_t i;
        for(i = 0; i < size; i += 4096) {
            j = ((volatile char *)x)[i];
        }
        (void) j; /* -Wunused-variable */
    }
}

#endif /* !LMDB_PRELOAD_H */
