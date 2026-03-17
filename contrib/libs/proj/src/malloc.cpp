/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Memory management for proj.4.
 *           This version includes an implementation of generic destructors,
 *           for memory deallocation for the large majority of PJ-objects
 *           that do not allocate anything else than the PJ-object itself,
 *           and its associated opaque object - i.e. no additional malloc'ed
 *           memory inside the opaque object.
 *
 * Author:   Gerald I. Evenden (Original proj.4 author),
 *           Frank Warmerdam   (2000)  pj_malloc?
 *           Thomas Knudsen    (2016) - freeup/dealloc parts
 *
 ******************************************************************************
 * Copyright (c) 2000, Frank Warmerdam
 * Copyright (c) 2016, Thomas Knudsen / SDFE
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *****************************************************************************/
#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

/* allocate and deallocate memory */
/* These routines are used so that applications can readily replace
** projection system memory allocation/deallocation call with custom
** application procedures.  */

#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#include <new>

#include "proj/internal/io_internal.hpp"

#include "filemanager.hpp"
#include "grids.hpp"
#include "proj.h"
#include "proj_internal.h"

using namespace NS_PROJ;

/**********************************************************************/
char *pj_strdup(const char *str)
/**********************************************************************/
{
    size_t len = strlen(str) + 1;
    char *dup = static_cast<char *>(malloc(len));
    if (dup)
        memcpy(dup, str, len);
    return dup;
}

/*****************************************************************************/
void *free_params(PJ_CONTEXT *ctx, paralist *start, int errlev) {
    /*****************************************************************************
        Companion to pj_default_destructor (below). Deallocates a linked list
        of "+proj=xxx" initialization parameters.

        Also called from pj_init_ctx when encountering errors before the PJ
        proper is allocated.
    ******************************************************************************/
    paralist *t, *n;
    for (t = start; t; t = n) {
        n = t->next;
        free(t);
    }
    proj_context_errno_set(ctx, errlev);
    return (void *)nullptr;
}

/************************************************************************/
/*                         proj_destroy()                               */
/*                                                                      */
/*      This is the application callable entry point for destroying     */
/*      a projection definition.  It does work generic to all           */
/*      projection types, and then calls the projection specific        */
/*      free function, P->destructor(), to do local work.               */
/*      In most cases P->destructor()==pj_default_destructor.           */
/************************************************************************/

PJ *proj_destroy(PJ *P) {
    if (nullptr == P || !P->destructor)
        return nullptr;
    /* free projection parameters - all the hard work is done by */
    /* pj_default_destructor, which is supposed */
    /* to be called as the last step of the local destructor     */
    /* pointed to by P->destructor. In most cases,               */
    /* pj_default_destructor actually *is* what is pointed to    */
    P->destructor(P, proj_errno(P));
    return nullptr;
}

/*****************************************************************************/
// cppcheck-suppress uninitMemberVar
PJconsts::PJconsts() : destructor(pj_default_destructor) {}
/*****************************************************************************/

/*****************************************************************************/
/*              void PJconsts::copyStateFrom(const PJconsts& other)          */
/*****************************************************************************/

void PJconsts::copyStateFrom(const PJconsts &other) {
    over = other.over;
    errorIfBestTransformationNotAvailable =
        other.errorIfBestTransformationNotAvailable;
    warnIfBestTransformationNotAvailable =
        other.warnIfBestTransformationNotAvailable;
    skipNonInstantiable = other.skipNonInstantiable;
}

/*****************************************************************************/
PJ *pj_new() {
    /*****************************************************************************/
    return new (std::nothrow) PJ();
}

/*****************************************************************************/
PJ *pj_default_destructor(PJ *P, int errlev) { /* Destructor */
    /*****************************************************************************
        Does memory deallocation for "plain" PJ objects, i.e. that vast majority
        of PJs where the opaque object does not contain any additionally
        allocated memory below the P->opaque level.
    ******************************************************************************/

    /* Even if P==0, we set the errlev on pj_error and the default context   */
    /* Note that both, in the multithreaded case, may then contain undefined */
    /* values. This is expected behavior. For MT have one ctx per thread    */
    if (0 != errlev)
        proj_context_errno_set(pj_get_ctx(P), errlev);

    if (nullptr == P)
        return nullptr;

    free(P->def_size);
    free(P->def_shape);
    free(P->def_spherification);
    free(P->def_ellps);

    delete static_cast<ListOfHGrids *>(P->hgrids_legacy);
    delete static_cast<ListOfVGrids *>(P->vgrids_legacy);

    /* We used to call free( P->catalog ), but this will leak */
    /* memory. The safe way to clear catalog and grid is to call */
    /* pj_gc_unloadall(pj_get_default_ctx()); and freeate_grids(); */
    /* TODO: we should probably have a public pj_cleanup() method to do all */
    /* that */

    /* free the interface to Charles Karney's geodesic library */
    free(P->geod);

    /* free parameter list elements */
    free_params(pj_get_ctx(P), P->params, errlev);
    free(P->def_full);

    /* free the cs2cs emulation elements */
    proj_destroy(P->axisswap);
    proj_destroy(P->helmert);
    proj_destroy(P->cart);
    proj_destroy(P->cart_wgs84);
    proj_destroy(P->hgridshift);
    proj_destroy(P->vgridshift);

    proj_destroy(P->cached_op_for_proj_factors);

    free(static_cast<struct pj_opaque *>(P->opaque));
    delete P;
    return nullptr;
}

/*****************************************************************************/
void proj_cleanup() {
    /*****************************************************************************/

    // Close the database context of the default PJ_CONTEXT
    auto ctx = pj_get_default_ctx();
    ctx->iniFileLoaded = false;
    auto cpp_context = ctx->cpp_context;
    if (cpp_context) {
        cpp_context->closeDb();
    }

    pj_clear_initcache();
    FileManager::clearMemoryCache();
    pj_clear_hgridshift_knowngrids_cache();
    pj_clear_vgridshift_knowngrids_cache();
    pj_clear_gridshift_knowngrids_cache();
    pj_clear_sqlite_cache();
}
