/******************************************************************************
 * Project:  PROJ
 * Purpose:  proj_info() and proj_pj_info()
 *
 * Author:   Thomas Knudsen,  thokn@sdfe.dk,  2016-06-09/2016-11-06
 *
 ******************************************************************************
 * Copyright (c) 2016, 2017 Thomas Knudsen/SDFE
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

#define FROM_PROJ_CPP

#include "proj.h"
#include "proj_internal.h"

#include "filemanager.hpp"

/*****************************************************************************/
static char *path_append(char *buf, const char *app, size_t *buf_size) {
    /******************************************************************************
        Helper for proj_info() below. Append app to buf, separated by a
        semicolon. Also handle allocation of longer buffer if needed.

        Returns buffer and adjusts *buf_size through provided pointer arg.
    ******************************************************************************/
    char *p;
    size_t len, applen = 0, buflen = 0;
#ifdef _WIN32
    const char *delim = ";";
#else
    const char *delim = ":";
#endif

    /* Nothing to do? */
    if (nullptr == app)
        return buf;
    applen = strlen(app);
    if (0 == applen)
        return buf;

    /* Start checking whether buf is long enough */
    if (nullptr != buf)
        buflen = strlen(buf);
    len = buflen + applen + strlen(delim) + 1;

    /* "pj_realloc", so to speak */
    if (*buf_size < len) {
        p = static_cast<char *>(calloc(2 * len, sizeof(char)));
        if (nullptr == p) {
            free(buf);
            return nullptr;
        }
        *buf_size = 2 * len;
        if (buf != nullptr)
            strcpy(p, buf);
        free(buf);
        buf = p;
    }
    assert(buf);

    /* Only append a delimiter if something's already there */
    if (0 != buflen)
        strcat(buf, delim);
    strcat(buf, app);
    return buf;
}

static const char *empty = {""};
static char version[64] = {""};
static PJ_INFO info = {0, 0, 0, nullptr, nullptr, nullptr, nullptr, 0};

/*****************************************************************************/
PJ_INFO proj_info(void) {
    /******************************************************************************
        Basic info about the current instance of the PROJ.4 library.

        Returns PJ_INFO struct.
    ******************************************************************************/
    size_t buf_size = 0;
    char *buf = nullptr;

    pj_acquire_lock();

    info.major = PROJ_VERSION_MAJOR;
    info.minor = PROJ_VERSION_MINOR;
    info.patch = PROJ_VERSION_PATCH;

    /* A normal version string is xx.yy.zz which is 8 characters
    long and there is room for 64 bytes in the version string. */
    snprintf(version, sizeof(version), "%d.%d.%d", info.major, info.minor,
             info.patch);

    info.version = version;
    info.release = pj_get_release();

    /* build search path string */
    auto ctx = pj_get_default_ctx();
    if (ctx->search_paths.empty()) {
        const auto searchpaths = pj_get_default_searchpaths(ctx);
        for (const auto &path : searchpaths) {
            buf = path_append(buf, path.c_str(), &buf_size);
        }
    } else {
        for (const auto &path : ctx->search_paths) {
            buf = path_append(buf, path.c_str(), &buf_size);
        }
    }

    if (info.searchpath != empty)
        free(const_cast<char *>(info.searchpath));
    info.searchpath = buf ? buf : empty;

    info.paths = ctx->c_compat_paths;
    info.path_count = static_cast<int>(ctx->search_paths.size());

    pj_release_lock();
    return info;
}

/*****************************************************************************/
PJ_PROJ_INFO proj_pj_info(PJ *P) {
    /******************************************************************************
        Basic info about a particular instance of a projection object.

        Returns PJ_PROJ_INFO struct.
    ******************************************************************************/
    PJ_PROJ_INFO pjinfo;
    char *def;

    memset(&pjinfo, 0, sizeof(PJ_PROJ_INFO));

    pjinfo.accuracy = -1.0;

    if (nullptr == P)
        return pjinfo;

    /* coordinate operation description */
    if (!P->alternativeCoordinateOperations.empty()) {
        if (P->iCurCoordOp >= 0) {
            P = P->alternativeCoordinateOperations[P->iCurCoordOp].pj;
        } else {
            PJ *candidateOp = nullptr;
            // If there's just a single coordinate operation which is
            // instantiable, use it.
            for (const auto &op : P->alternativeCoordinateOperations) {
                if (op.isInstantiable()) {
                    if (candidateOp == nullptr) {
                        candidateOp = op.pj;
                    } else {
                        candidateOp = nullptr;
                        break;
                    }
                }
            }
            if (candidateOp) {
                P = candidateOp;
            } else {
                pjinfo.id = "unknown";
                pjinfo.description = "unavailable until proj_trans is called";
                pjinfo.definition = "unavailable until proj_trans is called";
                return pjinfo;
            }
        }
    }

    /* projection id */
    if (pj_param(P->ctx, P->params, "tproj").i)
        pjinfo.id = pj_param(P->ctx, P->params, "sproj").s;

    pjinfo.description = P->descr;
    if (P->iso_obj) {
        auto identifiedObj =
            dynamic_cast<NS_PROJ::common::IdentifiedObject *>(P->iso_obj.get());
        // cppcheck-suppress knownConditionTrueFalse
        if (identifiedObj) {
            pjinfo.description = identifiedObj->nameStr().c_str();
        }
    }

    // accuracy
    if (P->iso_obj) {
        auto conv = dynamic_cast<const NS_PROJ::operation::Conversion *>(
            P->iso_obj.get());
        // cppcheck-suppress knownConditionTrueFalse
        if (conv) {
            pjinfo.accuracy = 0.0;
        } else {
            auto op =
                dynamic_cast<const NS_PROJ::operation::CoordinateOperation *>(
                    P->iso_obj.get());
            // cppcheck-suppress knownConditionTrueFalse
            if (op) {
                const auto &accuracies = op->coordinateOperationAccuracies();
                if (!accuracies.empty()) {
                    try {
                        pjinfo.accuracy = std::stod(accuracies[0]->value());
                    } catch (const std::exception &) {
                    }
                }
            }
        }
    }

    /* projection definition */
    if (P->def_full)
        def = P->def_full;
    else
        def = pj_get_def(P, 0); /* pj_get_def takes a non-const PJ pointer */
    if (nullptr == def)
        pjinfo.definition = empty;
    else
        pjinfo.definition = pj_shrink(def);
    /* Make proj_destroy clean this up eventually */
    P->def_full = def;

    pjinfo.has_inverse = pj_has_inverse(P);
    return pjinfo;
}
