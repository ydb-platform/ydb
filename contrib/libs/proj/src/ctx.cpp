/******************************************************************************
 * Project:  PROJ.4
 * Purpose:  Implementation of the PJ_CONTEXT thread context object.
 * Author:   Frank Warmerdam, warmerdam@pobox.com
 *
 ******************************************************************************
 * Copyright (c) 2010, Frank Warmerdam
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

#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <new>

#include "filemanager.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"
#include "proj_experimental.h"
#include "proj_internal.h"

/************************************************************************/
/*                             pj_get_ctx()                             */
/************************************************************************/

PJ_CONTEXT *pj_get_ctx(PJ *pj)

{
    if (nullptr == pj)
        return pj_get_default_ctx();
    if (nullptr == pj->ctx)
        return pj_get_default_ctx();
    return pj->ctx;
}

/************************************************************************/
/*                        proj_assign_context()                         */
/************************************************************************/

/** \brief Re-assign a context to a PJ* object.
 *
 * This may be useful if the PJ* has been created with a context that is
 * thread-specific, and is later used in another thread. In that case,
 * the user may want to assign another thread-specific context to the
 * object.
 */
void proj_assign_context(PJ *pj, PJ_CONTEXT *ctx) {
    if (pj == nullptr)
        return;
    pj->ctx = ctx;
    if (pj->reassign_context) {
        pj->reassign_context(pj, ctx);
    }
    for (const auto &alt : pj->alternativeCoordinateOperations) {
        proj_assign_context(alt.pj, ctx);
    }
}

/************************************************************************/
/*                          createDefault()                             */
/************************************************************************/

pj_ctx pj_ctx::createDefault() {
    pj_ctx ctx;
    ctx.debug_level = PJ_LOG_ERROR;
    ctx.logger = pj_stderr_logger;
    NS_PROJ::FileManager::fillDefaultNetworkInterface(&ctx);

    const char *projDebug = getenv("PROJ_DEBUG");
    if (projDebug != nullptr) {
        if (NS_PROJ::internal::ci_equal(projDebug, "ON")) {
            ctx.debug_level = PJ_LOG_DEBUG;
        } else if (NS_PROJ::internal::ci_equal(projDebug, "OFF")) {
            ctx.debug_level = PJ_LOG_ERROR;
        } else if (projDebug[0] == '-' ||
                   (projDebug[0] >= '0' && projDebug[0] <= '9')) {
            const int debugLevel = atoi(projDebug);
            // Negative debug levels mean that we first start logging when errno
            // is set Cf
            // https://github.com/OSGeo/PROJ/commit/1c1d04b45d76366f54e104f9346879fd48bfde8e
            // This isn't documented for now. Not totally sure we really want
            // that...
            if (debugLevel >= -PJ_LOG_TRACE)
                ctx.debug_level = debugLevel;
            else
                ctx.debug_level = PJ_LOG_TRACE;
        } else {
            fprintf(stderr, "Invalid value for PROJ_DEBUG: %s\n", projDebug);
        }
    }

    return ctx;
}

/**************************************************************************/
/*                           get_cpp_context()                            */
/**************************************************************************/

projCppContext *pj_ctx::get_cpp_context() {
    if (cpp_context == nullptr) {
        cpp_context = new projCppContext(this);
    }
    return cpp_context;
}

/************************************************************************/
/*                           set_search_paths()                         */
/************************************************************************/

void pj_ctx::set_search_paths(const std::vector<std::string> &search_paths_in) {
    lookupedFiles.clear();
    search_paths = search_paths_in;
    delete[] c_compat_paths;
    c_compat_paths = nullptr;
    if (!search_paths.empty()) {
        c_compat_paths = new const char *[search_paths.size()];
        for (size_t i = 0; i < search_paths.size(); ++i) {
            c_compat_paths[i] = search_paths[i].c_str();
        }
    }
}

/**************************************************************************/
/*                           set_ca_bundle_path()                         */
/**************************************************************************/

void pj_ctx::set_ca_bundle_path(const std::string &ca_bundle_path_in) {
    ca_bundle_path = ca_bundle_path_in;
}

/************************************************************************/
/*                  pj_ctx(const pj_ctx& other)                         */
/************************************************************************/

pj_ctx::pj_ctx(const pj_ctx &other)
    : lastFullErrorMessage(std::string()), last_errno(0),
      debug_level(other.debug_level),
      errorIfBestTransformationNotAvailableDefault(
          other.errorIfBestTransformationNotAvailableDefault),
      warnIfBestTransformationNotAvailableDefault(
          other.warnIfBestTransformationNotAvailableDefault),
      logger(other.logger), logger_app_data(other.logger_app_data),
      cpp_context(other.cpp_context ? other.cpp_context->clone(this) : nullptr),
      use_proj4_init_rules(other.use_proj4_init_rules),
      forceOver(other.forceOver), epsg_file_exists(other.epsg_file_exists),
      env_var_proj_data(other.env_var_proj_data),
      file_finder(other.file_finder),
      file_finder_user_data(other.file_finder_user_data),
      defer_grid_opening(false),
      custom_sqlite3_vfs_name(other.custom_sqlite3_vfs_name),
      user_writable_directory(other.user_writable_directory),
      // BEGIN ini file settings
      iniFileLoaded(other.iniFileLoaded), endpoint(other.endpoint),
      networking(other.networking), ca_bundle_path(other.ca_bundle_path),
      native_ca(other.native_ca), gridChunkCache(other.gridChunkCache),
      defaultTmercAlgo(other.defaultTmercAlgo),
      // END ini file settings
      projStringParserCreateFromPROJStringRecursionCounter(0),
      pipelineInitRecursiongCounter(0) {
    set_search_paths(other.search_paths);
}

/************************************************************************/
/*                         pj_get_default_ctx()                         */
/************************************************************************/

PJ_CONTEXT *pj_get_default_ctx()

{
    // C++11 rules guarantee a thread-safe instantiation.
    static pj_ctx default_context(pj_ctx::createDefault());
    return &default_context;
}

/************************************************************************/
/*                            ~pj_ctx()                              */
/************************************************************************/

pj_ctx::~pj_ctx() {
    delete[] c_compat_paths;
    proj_context_delete_cpp_context(cpp_context);
}

/************************************************************************/
/*                            proj_context_clone()                      */
/*           Create a new context based on a custom context             */
/************************************************************************/

PJ_CONTEXT *proj_context_clone(PJ_CONTEXT *ctx) {
    if (nullptr == ctx)
        return proj_context_create();

    return new (std::nothrow) pj_ctx(*ctx);
}

/*****************************************************************************/
int proj_errno(const PJ *P) {
    /******************************************************************************
        Read an error level from the context of a PJ.
    ******************************************************************************/
    return proj_context_errno(pj_get_ctx((PJ *)P));
}

/*****************************************************************************/
int proj_context_errno(PJ_CONTEXT *ctx) {
    /******************************************************************************
        Read an error directly from a context, without going through a PJ
        belonging to that context.
    ******************************************************************************/
    if (nullptr == ctx)
        ctx = pj_get_default_ctx();
    return ctx->last_errno;
}

/*****************************************************************************/
int proj_errno_set(const PJ *P, int err) {
    /******************************************************************************
        Set context-errno, bubble it up to the thread local errno, return err
    ******************************************************************************/
    /* Use proj_errno_reset to explicitly clear the error status */
    if (0 == err)
        return 0;

    /* For P==0 err goes to the default context */
    proj_context_errno_set(pj_get_ctx((PJ *)P), err);
    errno = err;

    return err;
}

/*****************************************************************************/
int proj_errno_restore(const PJ *P, int err) {
    /******************************************************************************
        Use proj_errno_restore when the current function succeeds, but the
        error flag was set on entry, and stored/reset using proj_errno_reset
        in order to monitor for new errors.

        See usage example under proj_errno_reset ()
    ******************************************************************************/
    if (0 == err)
        return 0;
    proj_errno_set(P, err);
    return 0;
}

/*****************************************************************************/
int proj_errno_reset(const PJ *P) {
    /******************************************************************************
        Clears errno in the context and thread local levels
        through the low level pj_ctx interface.

        Returns the previous value of the errno, for convenient reset/restore
        operations:

        int foo (PJ *P) {
            // errno may be set on entry, but we need to reset it to be able to
            // check for errors from "do_something_with_P(P)"
            int last_errno = proj_errno_reset (P);

            // local failure
            if (0==P)
                return proj_errno_set (P, 42);

            // call to function that may fail
            do_something_with_P (P);

            // failure in do_something_with_P? - keep latest error status
            if (proj_errno(P))
                return proj_errno (P);

            // success - restore previous error status, return 0
            return proj_errno_restore (P, last_errno);
        }
    ******************************************************************************/
    int last_errno;
    last_errno = proj_errno(P);

    proj_context_errno_set(pj_get_ctx((PJ *)P), 0);
    errno = 0;
    return last_errno;
}

/* Create a new context based on the default context */
PJ_CONTEXT *proj_context_create(void) {
    return new (std::nothrow) pj_ctx(*pj_get_default_ctx());
}

PJ_CONTEXT *proj_context_destroy(PJ_CONTEXT *ctx) {
    if (nullptr == ctx)
        return nullptr;

    /* Trying to free the default context is a no-op (since it is statically
     * allocated) */
    if (pj_get_default_ctx() == ctx)
        return nullptr;

    delete ctx;
    return nullptr;
}

/************************************************************************/
/*                  proj_context_use_proj4_init_rules()                 */
/************************************************************************/

void proj_context_use_proj4_init_rules(PJ_CONTEXT *ctx, int enable) {
    if (ctx == nullptr) {
        ctx = pj_get_default_ctx();
    }
    ctx->use_proj4_init_rules = enable;
}

/************************************************************************/
/*                              EQUAL()                                 */
/************************************************************************/

static int EQUAL(const char *a, const char *b) {
#ifdef _MSC_VER
    return _stricmp(a, b) == 0;
#else
    return strcasecmp(a, b) == 0;
#endif
}

/************************************************************************/
/*                  proj_context_get_use_proj4_init_rules()             */
/************************************************************************/

int proj_context_get_use_proj4_init_rules(PJ_CONTEXT *ctx,
                                          int from_legacy_code_path) {
    const char *val = getenv("PROJ_USE_PROJ4_INIT_RULES");

    if (ctx == nullptr) {
        ctx = pj_get_default_ctx();
    }

    if (val) {
        if (EQUAL(val, "yes") || EQUAL(val, "on") || EQUAL(val, "true")) {
            return TRUE;
        }
        if (EQUAL(val, "no") || EQUAL(val, "off") || EQUAL(val, "false")) {
            return FALSE;
        }
        pj_log(ctx, PJ_LOG_ERROR,
               "Invalid value for PROJ_USE_PROJ4_INIT_RULES");
    }

    if (ctx->use_proj4_init_rules >= 0) {
        return ctx->use_proj4_init_rules;
    }
    return from_legacy_code_path;
}
