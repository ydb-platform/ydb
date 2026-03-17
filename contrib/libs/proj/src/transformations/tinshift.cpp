/******************************************************************************
 * Project:  PROJ
 * Purpose:  Functionality related to triangulation transformation
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2020, Even Rouault, <even.rouault at spatialys.com>
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

#define PROJ_COMPILATION

#include "tinshift.hpp"
#include "filemanager.hpp"
#include "proj_internal.h"

PROJ_HEAD(tinshift, "Triangulation based transformation");

using namespace TINSHIFT_NAMESPACE;

namespace {

struct tinshiftData {
    std::unique_ptr<Evaluator> evaluator{};

    tinshiftData() = default;

    tinshiftData(const tinshiftData &) = delete;
    tinshiftData &operator=(const tinshiftData &) = delete;
};

} // namespace

static PJ *pj_tinshift_destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    auto Q = static_cast<struct tinshiftData *>(P->opaque);
    delete Q;
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

static void tinshift_forward_4d(PJ_COORD &coo, PJ *P) {
    auto *Q = (struct tinshiftData *)P->opaque;

    if (!Q->evaluator->forward(coo.xyz.x, coo.xyz.y, coo.xyz.z, coo.xyz.x,
                               coo.xyz.y, coo.xyz.z)) {
        coo = proj_coord_error();
    }
}

static void tinshift_reverse_4d(PJ_COORD &coo, PJ *P) {
    auto *Q = (struct tinshiftData *)P->opaque;

    if (!Q->evaluator->inverse(coo.xyz.x, coo.xyz.y, coo.xyz.z, coo.xyz.x,
                               coo.xyz.y, coo.xyz.z)) {
        coo = proj_coord_error();
    }
}

PJ *PJ_TRANSFORMATION(tinshift, 1) {

    const char *filename = pj_param(P->ctx, P->params, "sfile").s;
    if (!filename) {
        proj_log_error(P, _("+file= should be specified."));
        return pj_tinshift_destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    auto file = NS_PROJ::FileManager::open_resource_file(P->ctx, filename);
    if (nullptr == file) {
        proj_log_error(P, _("Cannot open %s"), filename);
        return pj_tinshift_destructor(
            P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }
    file->seek(0, SEEK_END);
    unsigned long long size = file->tell();
    // Arbitrary threshold to avoid ingesting an arbitrarily large JSON file,
    // that could be a denial of service risk. 100 MB should be sufficiently
    // large for any valid use !
    if (size > 100 * 1024 * 1024) {
        proj_log_error(P, _("File %s too large"), filename);
        return pj_tinshift_destructor(
            P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }
    file->seek(0);
    std::string jsonStr;
    try {
        jsonStr.resize(static_cast<size_t>(size));
    } catch (const std::bad_alloc &) {
        proj_log_error(P, _("Cannot read %s. Not enough memory"), filename);
        return pj_tinshift_destructor(P, PROJ_ERR_OTHER);
    }
    if (file->read(&jsonStr[0], jsonStr.size()) != jsonStr.size()) {
        proj_log_error(P, _("Cannot read %s"), filename);
        return pj_tinshift_destructor(
            P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }

    auto Q = new tinshiftData();
    P->opaque = (void *)Q;
    P->destructor = pj_tinshift_destructor;

    try {
        Q->evaluator.reset(new Evaluator(TINShiftFile::parse(jsonStr)));
    } catch (const std::exception &e) {
        proj_log_error(P, _("invalid model: %s"), e.what());
        return pj_tinshift_destructor(
            P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }

    P->fwd4d = tinshift_forward_4d;
    P->inv4d = tinshift_reverse_4d;
    P->left = PJ_IO_UNITS_WHATEVER;
    P->right = PJ_IO_UNITS_WHATEVER;

    return P;
}
