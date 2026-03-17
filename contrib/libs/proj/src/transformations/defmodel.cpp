/******************************************************************************
 * Project:  PROJ
 * Purpose:  Functionality related to deformation model
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

#include "defmodel.hpp"
#include "filemanager.hpp"
#include "grids.hpp"
#include "proj_internal.h"

#include <assert.h>

#include <map>
#include <memory>
#include <utility>

PROJ_HEAD(defmodel, "Deformation model");

using namespace DeformationModel;

namespace {

struct Grid : public GridPrototype {
    PJ_CONTEXT *ctx;
    const NS_PROJ::GenericShiftGrid *realGrid;
    mutable bool checkedHorizontal = false;
    mutable bool checkedVertical = false;
    mutable int sampleX = 0;
    mutable int sampleY = 1;
    mutable int sampleZ = 2;

    Grid(PJ_CONTEXT *ctxIn, const NS_PROJ::GenericShiftGrid *realGridIn)
        : ctx(ctxIn), realGrid(realGridIn) {
        minx = realGridIn->extentAndRes().west;
        miny = realGridIn->extentAndRes().south;
        resx = realGridIn->extentAndRes().resX;
        resy = realGridIn->extentAndRes().resY;
        width = realGridIn->width();
        height = realGridIn->height();
    }

    bool checkHorizontal(const std::string &expectedUnit) const {
        if (!checkedHorizontal) {
            const auto samplesPerPixel = realGrid->samplesPerPixel();
            if (samplesPerPixel < 2) {
                pj_log(ctx, PJ_LOG_ERROR, "grid %s has not enough samples",
                       realGrid->name().c_str());
                return false;
            }
            bool foundDescX = false;
            bool foundDescY = false;
            bool foundDesc = false;
            for (int i = 0; i < samplesPerPixel; i++) {
                const auto desc = realGrid->description(i);
                if (desc == "east_offset") {
                    sampleX = i;
                    foundDescX = true;
                } else if (desc == "north_offset") {
                    sampleY = i;
                    foundDescY = true;
                }
                if (!desc.empty()) {
                    foundDesc = true;
                }
            }
            if (foundDesc && (!foundDescX || !foundDescY)) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "grid %s : Found band description, "
                       "but not the ones expected",
                       realGrid->name().c_str());
                return false;
            }
            const auto unit = realGrid->unit(sampleX);
            if (!unit.empty() && unit != expectedUnit) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "grid %s : Only unit=%s "
                       "currently handled for this mode",
                       realGrid->name().c_str(), expectedUnit.c_str());
                return false;
            }
            checkedHorizontal = true;
        }
        return true;
    }

    bool getLongLatOffset(int ix, int iy, double &longOffsetRadian,
                          double &latOffsetRadian) const {
        if (!checkHorizontal(STR_DEGREE)) {
            return false;
        }
        float longOffsetDeg;
        float latOffsetDeg;
        if (!realGrid->valueAt(ix, iy, sampleX, longOffsetDeg) ||
            !realGrid->valueAt(ix, iy, sampleY, latOffsetDeg)) {
            return false;
        }
        longOffsetRadian = longOffsetDeg * DEG_TO_RAD;
        latOffsetRadian = latOffsetDeg * DEG_TO_RAD;
        return true;
    }

    bool getZOffset(int ix, int iy, double &zOffset) const {
        if (!checkedVertical) {
            const auto samplesPerPixel = realGrid->samplesPerPixel();
            if (samplesPerPixel == 1) {
                sampleZ = 0;
            } else if (samplesPerPixel < 3) {
                pj_log(ctx, PJ_LOG_ERROR, "grid %s has not enough samples",
                       realGrid->name().c_str());
                return false;
            }
            bool foundDesc = false;
            bool foundDescZ = false;
            for (int i = 0; i < samplesPerPixel; i++) {
                const auto desc = realGrid->description(i);
                if (desc == "vertical_offset") {
                    sampleZ = i;
                    foundDescZ = true;
                }
                if (!desc.empty()) {
                    foundDesc = true;
                }
            }
            if (foundDesc && !foundDescZ) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "grid %s : Found band description, "
                       "but not the ones expected",
                       realGrid->name().c_str());
                return false;
            }
            const auto unit = realGrid->unit(sampleZ);
            if (!unit.empty() && unit != STR_METRE) {
                pj_log(ctx, PJ_LOG_ERROR,
                       "grid %s : Only unit=metre currently "
                       "handled for this mode",
                       realGrid->name().c_str());
                return false;
            }
            checkedVertical = true;
        }
        float zOffsetFloat = 0.0f;
        const bool ret = realGrid->valueAt(ix, iy, sampleZ, zOffsetFloat);
        zOffset = zOffsetFloat;
        return ret;
    }

    bool getEastingNorthingOffset(int ix, int iy, double &eastingOffset,
                                  double &northingOffset) const {
        if (!checkHorizontal(STR_METRE)) {
            return false;
        }
        float eastingOffsetFloat = 0.0f;
        float northingOffsetFloat = 0.0f;
        const bool ret =
            realGrid->valueAt(ix, iy, sampleX, eastingOffsetFloat) &&
            realGrid->valueAt(ix, iy, sampleY, northingOffsetFloat);
        eastingOffset = eastingOffsetFloat;
        northingOffset = northingOffsetFloat;
        return ret;
    }

    bool getLongLatZOffset(int ix, int iy, double &longOffsetRadian,
                           double &latOffsetRadian, double &zOffset) const {
        return getLongLatOffset(ix, iy, longOffsetRadian, latOffsetRadian) &&
               getZOffset(ix, iy, zOffset);
    }

    bool getEastingNorthingZOffset(int ix, int iy, double &eastingOffset,
                                   double &northingOffset,
                                   double &zOffset) const {
        return getEastingNorthingOffset(ix, iy, eastingOffset,
                                        northingOffset) &&
               getZOffset(ix, iy, zOffset);
    }

#ifdef DEBUG_DEFMODEL
    std::string name() const { return realGrid->name(); }
#endif

  private:
    Grid(const Grid &) = delete;
    Grid &operator=(const Grid &) = delete;
};

struct GridSet : public GridSetPrototype<Grid> {

    PJ_CONTEXT *ctx;
    std::unique_ptr<NS_PROJ::GenericShiftGridSet> realGridSet;
    std::map<const NS_PROJ::GenericShiftGrid *, std::unique_ptr<Grid>>
        mapGrids{};

    GridSet(PJ_CONTEXT *ctxIn,
            std::unique_ptr<NS_PROJ::GenericShiftGridSet> &&realGridSetIn)
        : ctx(ctxIn), realGridSet(std::move(realGridSetIn)) {}

    const Grid *gridAt(double x, double y) {
        const NS_PROJ::GenericShiftGrid *realGrid = realGridSet->gridAt(x, y);
        if (!realGrid) {
            return nullptr;
        }
        auto iter = mapGrids.find(realGrid);
        if (iter == mapGrids.end()) {
            iter = mapGrids
                       .insert(std::pair<const NS_PROJ::GenericShiftGrid *,
                                         std::unique_ptr<Grid>>(
                           realGrid,
                           std::unique_ptr<Grid>(new Grid(ctx, realGrid))))
                       .first;
        }
        return iter->second.get();
    }

  private:
    GridSet(const GridSet &) = delete;
    GridSet &operator=(const GridSet &) = delete;
};

struct EvaluatorIface : public EvaluatorIfacePrototype<Grid, GridSet> {

    PJ_CONTEXT *ctx;
    PJ *cart;

    EvaluatorIface(PJ_CONTEXT *ctxIn, PJ *cartIn) : ctx(ctxIn), cart(cartIn) {}

    ~EvaluatorIface() {
        if (cart)
            cart->destructor(cart, 0);
    }

    std::unique_ptr<GridSet> open(const std::string &filename) {
        auto realGridSet = NS_PROJ::GenericShiftGridSet::open(ctx, filename);
        if (!realGridSet) {
            pj_log(ctx, PJ_LOG_ERROR, "cannot open %s", filename.c_str());
            return nullptr;
        }
        return std::unique_ptr<GridSet>(
            new GridSet(ctx, std::move(realGridSet)));
    }

    bool isGeographicCRS(const std::string &crsDef) {
        PJ *P = proj_create(ctx, crsDef.c_str());
        if (P == nullptr) {
            return true; // reasonable default value
        }
        const auto type = proj_get_type(P);
        bool ret = (type == PJ_TYPE_GEOGRAPHIC_2D_CRS ||
                    type == PJ_TYPE_GEOGRAPHIC_3D_CRS);
        proj_destroy(P);
        return ret;
    }

    void geographicToGeocentric(double lam, double phi, double height, double a,
                                double b, double /*es*/, double &X, double &Y,
                                double &Z) {
        (void)a;
        (void)b;
        assert(cart->a == a);
        assert(cart->b == b);
        PJ_LPZ lpz;
        lpz.lam = lam;
        lpz.phi = phi;
        lpz.z = height;
        PJ_XYZ xyz = cart->fwd3d(lpz, cart);
        X = xyz.x;
        Y = xyz.y;
        Z = xyz.z;
    }

    void geocentricToGeographic(double X, double Y, double Z, double a,
                                double b, double /*es*/, double &lam,
                                double &phi, double &height) {
        (void)a;
        (void)b;
        assert(cart->a == a);
        assert(cart->b == b);
        PJ_XYZ xyz;
        xyz.x = X;
        xyz.y = Y;
        xyz.z = Z;
        PJ_LPZ lpz = cart->inv3d(xyz, cart);
        lam = lpz.lam;
        phi = lpz.phi;
        height = lpz.z;
    }

#ifdef DEBUG_DEFMODEL
    void log(const std::string &msg) {
        pj_log(ctx, PJ_LOG_TRACE, "%s", msg.c_str());
    }
#endif

  private:
    EvaluatorIface(const EvaluatorIface &) = delete;
    EvaluatorIface &operator=(const EvaluatorIface &) = delete;
};

struct defmodelData {
    std::unique_ptr<Evaluator<Grid, GridSet, EvaluatorIface>> evaluator{};
    EvaluatorIface evaluatorIface;

    explicit defmodelData(PJ_CONTEXT *ctx, PJ *cart)
        : evaluatorIface(ctx, cart) {}

    defmodelData(const defmodelData &) = delete;
    defmodelData &operator=(const defmodelData &) = delete;
};

} // namespace

static PJ *destructor(PJ *P, int errlev) {
    if (nullptr == P)
        return nullptr;

    auto Q = static_cast<struct defmodelData *>(P->opaque);
    delete Q;
    P->opaque = nullptr;

    return pj_default_destructor(P, errlev);
}

static void forward_4d(PJ_COORD &coo, PJ *P) {
    auto *Q = (struct defmodelData *)P->opaque;

    if (coo.xyzt.t == HUGE_VAL) {
        coo = proj_coord_error();
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_MISSING_TIME);
        return;
    }

    if (!Q->evaluator->forward(Q->evaluatorIface, coo.xyzt.x, coo.xyzt.y,
                               coo.xyzt.z, coo.xyzt.t, coo.xyzt.x, coo.xyzt.y,
                               coo.xyzt.z)) {
        coo = proj_coord_error();
    }
}

static void reverse_4d(PJ_COORD &coo, PJ *P) {
    auto *Q = (struct defmodelData *)P->opaque;

    if (coo.xyzt.t == HUGE_VAL) {
        coo = proj_coord_error();
        proj_errno_set(P, PROJ_ERR_COORD_TRANSFM_MISSING_TIME);
        return;
    }

    if (!Q->evaluator->inverse(Q->evaluatorIface, coo.xyzt.x, coo.xyzt.y,
                               coo.xyzt.z, coo.xyzt.t, coo.xyzt.x, coo.xyzt.y,
                               coo.xyzt.z)) {
        coo = proj_coord_error();
    }
}

// Function called by proj_assign_context() when a new context is assigned to
// an existing PJ object. Mostly to deal with objects being passed between
// threads.
static void reassign_context(PJ *P, PJ_CONTEXT *ctx) {
    auto *Q = (struct defmodelData *)P->opaque;
    if (Q->evaluatorIface.ctx != ctx) {
        Q->evaluator->clearGridCache();
        Q->evaluatorIface.ctx = ctx;
    }
}

PJ *PJ_TRANSFORMATION(defmodel, 1) {
    // Pass a dummy ellipsoid definition that will be overridden just afterwards
    auto cart = proj_create(P->ctx, "+proj=cart +a=1");
    if (cart == nullptr)
        return destructor(P, PROJ_ERR_OTHER /*ENOMEM*/);

    /* inherit ellipsoid definition from P to Q->cart */
    pj_inherit_ellipsoid_def(P, cart);

    auto Q = new defmodelData(P->ctx, cart);
    P->opaque = (void *)Q;
    P->destructor = destructor;
    P->reassign_context = reassign_context;

    const char *model = pj_param(P->ctx, P->params, "smodel").s;
    if (!model) {
        proj_log_error(P, _("+model= should be specified."));
        return destructor(P, PROJ_ERR_INVALID_OP_MISSING_ARG);
    }

    auto file = NS_PROJ::FileManager::open_resource_file(P->ctx, model);
    if (nullptr == file) {
        proj_log_error(P, _("Cannot open %s"), model);
        return destructor(P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }
    file->seek(0, SEEK_END);
    unsigned long long size = file->tell();
    // Arbitrary threshold to avoid ingesting an arbitrarily large JSON file,
    // that could be a denial of service risk. 10 MB should be sufficiently
    // large for any valid use !
    if (size > 10 * 1024 * 1024) {
        proj_log_error(P, _("File %s too large"), model);
        return destructor(P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }
    file->seek(0);
    std::string jsonStr;
    jsonStr.resize(static_cast<size_t>(size));
    if (file->read(&jsonStr[0], jsonStr.size()) != jsonStr.size()) {
        proj_log_error(P, _("Cannot read %s"), model);
        return destructor(P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }

    try {
        Q->evaluator.reset(new Evaluator<Grid, GridSet, EvaluatorIface>(
            MasterFile::parse(jsonStr), Q->evaluatorIface, P->a, P->b));
    } catch (const std::exception &e) {
        proj_log_error(P, _("invalid model: %s"), e.what());
        return destructor(P, PROJ_ERR_INVALID_OP_FILE_NOT_FOUND_OR_INVALID);
    }

    P->fwd4d = forward_4d;
    P->inv4d = reverse_4d;

    if (Q->evaluator->isGeographicCRS()) {
        P->left = PJ_IO_UNITS_RADIANS;
        P->right = PJ_IO_UNITS_RADIANS;
    } else {
        P->left = PJ_IO_UNITS_PROJECTED;
        P->right = PJ_IO_UNITS_PROJECTED;
    }

    return P;
}
