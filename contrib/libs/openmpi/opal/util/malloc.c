/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdlib.h>

#include "opal/util/malloc.h"
#include "opal/util/output.h"


/*
 * Undefine "malloc" and "free"
 */

#if defined(malloc)
#undef malloc
#endif
#if defined(calloc)
#undef calloc
#endif
#if defined(free)
#undef free
#endif
#if defined(realloc)
#undef realloc
#endif

/*
 * Public variables
 */
int opal_malloc_debug_level = OPAL_MALLOC_DEBUG_LEVEL;
int opal_malloc_output = -1;


/*
 * Private variables
 */
static opal_output_stream_t malloc_stream;


/*
 * Initialize the malloc debug interface
 */
void opal_malloc_init(void)
{
#if OPAL_ENABLE_DEBUG
    OBJ_CONSTRUCT(&malloc_stream, opal_output_stream_t);
    malloc_stream.lds_is_debugging = true;
    malloc_stream.lds_verbose_level = 5;
    malloc_stream.lds_prefix = "malloc debug: ";
    malloc_stream.lds_want_stderr = true;
    opal_malloc_output = opal_output_open(&malloc_stream);
#endif  /* OPAL_ENABLE_DEBUG */
}


/*
 * Finalize the malloc debug interface
 */
void opal_malloc_finalize(void)
{
    if (-1 != opal_malloc_output) {
        opal_output_close(opal_malloc_output);
        opal_malloc_output = -1;
        OBJ_DESTRUCT(&malloc_stream);
    }
}


/*
 * Debug version of malloc
 */
void *opal_malloc(size_t size, const char *file, int line)
{
    void *addr;
#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 1) {
        if (size <= 0) {
            opal_output(opal_malloc_output, "Request for %ld bytes (%s, %d)",
                        (long) size, file, line);
        }
    }
#endif /* OPAL_ENABLE_DEBUG */

    addr = malloc(size);

#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 0) {
        if (NULL == addr) {
            opal_output(opal_malloc_output,
                        "Request for %ld bytes failed (%s, %d)",
                        (long) size, file, line);
        }
    }
#endif  /* OPAL_ENABLE_DEBUG */
    return addr;
}


/*
 * Debug version of calloc
 */
void *opal_calloc(size_t nmembers, size_t size, const char *file, int line)
{
    void *addr;
#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 1) {
        if (size <= 0) {
            opal_output(opal_malloc_output,
                        "Request for %ld zeroed elements of size %ld (%s, %d)",
                        (long) nmembers, (long) size, file, line);
        }
    }
#endif  /* OPAL_ENABLE_DEBUG */
    addr = calloc(nmembers, size);
#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 0) {
        if (NULL == addr) {
            opal_output(opal_malloc_output,
                        "Request for %ld zeroed elements of size %ld failed (%s, %d)",
                        (long) nmembers, (long) size, file, line);
        }
    }
#endif  /* OPAL_ENABLE_DEBUG */
    return addr;
}


/*
 * Debug version of realloc
 */
void *opal_realloc(void *ptr, size_t size, const char *file, int line)
{
    void *addr;
#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 1) {
        if (size <= 0) {
            if (NULL == ptr) {
                opal_output(opal_malloc_output,
                            "Realloc NULL for %ld bytes (%s, %d)",
                            (long) size, file, line);
            } else {
                opal_output(opal_malloc_output, "Realloc %p for %ld bytes (%s, %d)",
                            ptr, (long) size, file, line);
            }
        }
    }
#endif  /* OPAL_ENABLE_DEBUG */
    addr = realloc(ptr, size);
#if OPAL_ENABLE_DEBUG
    if (opal_malloc_debug_level > 0) {
        if (NULL == addr) {
            opal_output(opal_malloc_output,
                        "Realloc %p for %ld bytes failed (%s, %d)",
                        ptr, (long) size, file, line);
        }
    }
#endif  /* OPAL_ENABLE_DEBUG */
    return addr;
}


/*
 * Debug version of free
 */
void opal_free(void *addr, const char *file, int line)
{
    free(addr);
}

void opal_malloc_debug(int level)
{
#if OPAL_ENABLE_DEBUG
    opal_malloc_debug_level = level;
#endif  /* OPAL_ENABLE_DEBUG */
}
