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

#ifndef DEFORMATON_MODEL_NAMESPACE
#error "Should be included only by defmodel.hpp"
#endif

#include <algorithm>
#include <limits>

namespace DEFORMATON_MODEL_NAMESPACE {

// ---------------------------------------------------------------------------

static const std::string STR_DEGREE("degree");
static const std::string STR_METRE("metre");

static const std::string STR_ADDITION("addition");
static const std::string STR_GEOCENTRIC("geocentric");

static const std::string STR_BILINEAR("bilinear");
static const std::string STR_GEOCENTRIC_BILINEAR("geocentric_bilinear");

static const std::string STR_NONE("none");
static const std::string STR_HORIZONTAL("horizontal");
static const std::string STR_VERTICAL("vertical");
static const std::string STR_3D("3d");

constexpr double DEFMODEL_PI = 3.14159265358979323846;
constexpr double DEG_TO_RAD_CONSTANT = 3.14159265358979323846 / 180.;
inline constexpr double DegToRad(double d) { return d * DEG_TO_RAD_CONSTANT; }

// ---------------------------------------------------------------------------

enum class DisplacementType { NONE, HORIZONTAL, VERTICAL, THREE_D };

// ---------------------------------------------------------------------------

/** Internal class to offer caching services over a Grid */
template <class Grid> struct GridEx {
    const Grid *grid;

    bool smallResx; // lesser than one degree

    double sinhalfresx;
    double coshalfresx;

    double sinresy;
    double cosresy;

    int last_ix0 = -1;
    int last_iy0 = -1;
    double dX00 = 0;
    double dY00 = 0;
    double dZ00 = 0;
    double dX01 = 0;
    double dY01 = 0;
    double dZ01 = 0;
    double dX10 = 0;
    double dY10 = 0;
    double dZ10 = 0;
    double dX11 = 0;
    double dY11 = 0;
    double dZ11 = 0;
    double sinphi0 = 0;
    double cosphi0 = 0;
    double sinphi1 = 0;
    double cosphi1 = 0;

    explicit GridEx(const Grid *gridIn)
        : grid(gridIn), smallResx(grid->resx < DegToRad(1)),
          sinhalfresx(sin(grid->resx / 2)), coshalfresx(cos(grid->resx / 2)),
          sinresy(sin(grid->resy)), cosresy(cos(grid->resy)) {}

    // Return geocentric offset (dX, dY, dZ) relative to a point
    // where x0 = -resx / 2
    inline void getBilinearGeocentric(int ix0, int iy0, double de00,
                                      double dn00, double de01, double dn01,
                                      double de10, double dn10, double de11,
                                      double dn11, double m00, double m01,
                                      double m10, double m11, double &dX,
                                      double &dY, double &dZ) {
        // If interpolating in the same cell as before, then we can skip
        // the recomputation of dXij, dYij and dZij
        if (ix0 != last_ix0 || iy0 != last_iy0) {

            last_ix0 = ix0;
            if (iy0 != last_iy0) {
                const double y0 = grid->miny + iy0 * grid->resy;
                sinphi0 = sin(y0);
                cosphi0 = cos(y0);

                // Use trigonometric formulas to avoid new calls to sin/cos
                sinphi1 =
                    /* sin(y0+grid->resyRad) = */ sinphi0 * cosresy +
                    cosphi0 * sinresy;
                cosphi1 =
                    /* cos(y0+grid->resyRad) = */ cosphi0 * cosresy -
                    sinphi0 * sinresy;

                last_iy0 = iy0;
            }

            // Using "traditional" formulas to convert from easting, northing
            // offsets to geocentric offsets
            const double sinlam00 = -sinhalfresx;
            const double coslam00 = coshalfresx;
            const double sinphi00 = sinphi0;
            const double cosphi00 = cosphi0;
            const double dn00sinphi00 = dn00 * sinphi00;
            dX00 = -de00 * sinlam00 - dn00sinphi00 * coslam00;
            dY00 = de00 * coslam00 - dn00sinphi00 * sinlam00;
            dZ00 = dn00 * cosphi00;

            const double sinlam01 = -sinhalfresx;
            const double coslam01 = coshalfresx;
            const double sinphi01 = sinphi1;
            const double cosphi01 = cosphi1;
            const double dn01sinphi01 = dn01 * sinphi01;
            dX01 = -de01 * sinlam01 - dn01sinphi01 * coslam01;
            dY01 = de01 * coslam01 - dn01sinphi01 * sinlam01;
            dZ01 = dn01 * cosphi01;

            const double sinlam10 = sinhalfresx;
            const double coslam10 = coshalfresx;
            const double sinphi10 = sinphi0;
            const double cosphi10 = cosphi0;
            const double dn10sinphi10 = dn10 * sinphi10;
            dX10 = -de10 * sinlam10 - dn10sinphi10 * coslam10;
            dY10 = de10 * coslam10 - dn10sinphi10 * sinlam10;
            dZ10 = dn10 * cosphi10;

            const double sinlam11 = sinhalfresx;
            const double coslam11 = coshalfresx;
            const double sinphi11 = sinphi1;
            const double cosphi11 = cosphi1;
            const double dn11sinphi11 = dn11 * sinphi11;
            dX11 = -de11 * sinlam11 - dn11sinphi11 * coslam11;
            dY11 = de11 * coslam11 - dn11sinphi11 * sinlam11;
            dZ11 = dn11 * cosphi11;
        }

        dX = m00 * dX00 + m01 * dX01 + m10 * dX10 + m11 * dX11;
        dY = m00 * dY00 + m01 * dY01 + m10 * dY10 + m11 * dY11;
        dZ = m00 * dZ00 + m01 * dZ01 + m10 * dZ10 + m11 * dZ11;
    }
};

// ---------------------------------------------------------------------------

/** Internal class to offer caching services over a Component */
template <class Grid, class GridSet> struct ComponentEx {
    const Component &component;

    const bool isBilinearInterpolation; /* bilinear vs geocentric_bilinear */

    const DisplacementType displacementType;

    // Cache
    std::unique_ptr<GridSet> gridSet{};
    std::map<const Grid *, GridEx<Grid>> mapGrids{};

  private:
    mutable double mCachedDt = 0;
    mutable double mCachedValue = 0;

    static DisplacementType getDisplacementType(const std::string &s) {
        if (s == STR_HORIZONTAL)
            return DisplacementType::HORIZONTAL;
        if (s == STR_VERTICAL)
            return DisplacementType::VERTICAL;
        if (s == STR_3D)
            return DisplacementType::THREE_D;
        return DisplacementType::NONE;
    }

  public:
    explicit ComponentEx(const Component &componentIn)
        : component(componentIn),
          isBilinearInterpolation(
              componentIn.spatialModel().interpolationMethod == STR_BILINEAR),
          displacementType(getDisplacementType(component.displacementType())) {}

    double evaluateAt(double dt) const {
        if (dt == mCachedDt)
            return mCachedValue;
        mCachedDt = dt;
        mCachedValue = component.timeFunction()->evaluateAt(dt);
        return mCachedValue;
    }

    void clearGridCache() {
        gridSet.reset();
        mapGrids.clear();
    }
};

// ---------------------------------------------------------------------------

/** Converts a ISO8601 date-time string, formatted as "YYYY-MM-DDTHH:MM:SSZ",
 * into a decimal year.
 * Leap years are taken into account, but not leap seconds.
 */
static double ISO8601ToDecimalYear(const std::string &dt) {
    int year, month, day, hour, min, sec;
    if (sscanf(dt.c_str(), "%04d-%02d-%02dT%02d:%02d:%02dZ", &year, &month,
               &day, &hour, &min, &sec) != 6 ||
        year < 1582 || // Start of Gregorian calendar
        month < 1 || month > 12 || day < 1 || day > 31 || hour < 0 ||
        hour >= 24 || min < 0 || min >= 60 || sec < 0 || sec >= 61) {
        throw ParsingException("Wrong formatting / invalid date-time for " +
                               dt);
    }
    const bool isLeapYear =
        (((year % 4) == 0 && (year % 100) != 0) || (year % 400) == 0);
    // Given the intended use, we omit leap seconds...
    const int month_table[2][12] = {
        {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
        {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
    int dayInYear = day - 1;
    for (int m = 1; m < month; m++) {
        dayInYear += month_table[isLeapYear ? 1 : 0][m - 1];
    }
    if (day > month_table[isLeapYear ? 1 : 0][month - 1]) {
        throw ParsingException("Wrong formatting / invalid date-time for " +
                               dt);
    }
    return year + (dayInYear * 86400 + hour * 3600 + min * 60 + sec) /
                      (isLeapYear ? 86400. * 366 : 86400. * 365);
}

// ---------------------------------------------------------------------------

Epoch::Epoch(const std::string &dt) : mDt(dt) {
    if (!dt.empty()) {
        mDecimalYear = ISO8601ToDecimalYear(dt);
    }
}

// ---------------------------------------------------------------------------

double Epoch::toDecimalYear() const { return mDecimalYear; }

// ---------------------------------------------------------------------------

static std::string getString(const json &j, const char *key, bool optional) {
    if (!j.contains(key)) {
        if (optional) {
            return std::string();
        }
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json v = j[key];
    if (!v.is_string()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a string");
    }
    return v.get<std::string>();
}

static std::string getReqString(const json &j, const char *key) {
    return getString(j, key, false);
}

static std::string getOptString(const json &j, const char *key) {
    return getString(j, key, true);
}

// ---------------------------------------------------------------------------

static double getDouble(const json &j, const char *key, bool optional) {
    if (!j.contains(key)) {
        if (optional) {
            return std::numeric_limits<double>::quiet_NaN();
        }
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json v = j[key];
    if (!v.is_number()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a number");
    }
    return v.get<double>();
}

static double getReqDouble(const json &j, const char *key) {
    return getDouble(j, key, false);
}
static double getOptDouble(const json &j, const char *key) {
    return getDouble(j, key, true);
}

// ---------------------------------------------------------------------------

static json getObjectMember(const json &j, const char *key) {
    if (!j.contains(key)) {
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json obj = j[key];
    if (!obj.is_object()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a object");
    }
    return obj;
}

// ---------------------------------------------------------------------------

static json getArrayMember(const json &j, const char *key) {
    if (!j.contains(key)) {
        throw ParsingException(std::string("Missing \"") + key + "\" key");
    }
    const json obj = j[key];
    if (!obj.is_array()) {
        throw ParsingException(std::string("The value of \"") + key +
                               "\" should be a array");
    }
    return obj;
}

// ---------------------------------------------------------------------------

std::unique_ptr<MasterFile> MasterFile::parse(const std::string &text) {
    std::unique_ptr<MasterFile> dmmf(new MasterFile());
    json j;
    try {
        j = json::parse(text);
    } catch (const std::exception &e) {
        throw ParsingException(e.what());
    }
    if (!j.is_object()) {
        throw ParsingException("Not an object");
    }
    dmmf->mFileType = getReqString(j, "file_type");
    dmmf->mFormatVersion = getReqString(j, "format_version");
    dmmf->mName = getOptString(j, "name");
    dmmf->mVersion = getOptString(j, "version");
    dmmf->mLicense = getOptString(j, "license");
    dmmf->mDescription = getOptString(j, "description");
    dmmf->mPublicationDate = getOptString(j, "publication_date");

    if (j.contains("authority")) {
        const json jAuthority = j["authority"];
        if (!jAuthority.is_object()) {
            throw ParsingException("authority is not a object");
        }
        dmmf->mAuthority.name = getOptString(jAuthority, "name");
        dmmf->mAuthority.url = getOptString(jAuthority, "url");
        dmmf->mAuthority.address = getOptString(jAuthority, "address");
        dmmf->mAuthority.email = getOptString(jAuthority, "email");
    }

    if (j.contains("links")) {
        const json jLinks = j["links"];
        if (!jLinks.is_array()) {
            throw ParsingException("links is not an array");
        }
        for (const json &jLink : jLinks) {
            if (!jLink.is_object()) {
                throw ParsingException("links[] item is not an object");
            }
            Link link;
            link.href = getOptString(jLink, "href");
            link.rel = getOptString(jLink, "rel");
            link.type = getOptString(jLink, "type");
            link.title = getOptString(jLink, "title");
            dmmf->mLinks.emplace_back(std::move(link));
        }
    }
    dmmf->mSourceCRS = getReqString(j, "source_crs");
    dmmf->mTargetCRS = getReqString(j, "target_crs");
    dmmf->mDefinitionCRS = getReqString(j, "definition_crs");
    if (dmmf->mSourceCRS != dmmf->mDefinitionCRS) {
        throw ParsingException(
            "source_crs != definition_crs not currently supported");
    }
    dmmf->mReferenceEpoch = getOptString(j, "reference_epoch");
    dmmf->mUncertaintyReferenceEpoch =
        getOptString(j, "uncertainty_reference_epoch");
    dmmf->mHorizontalOffsetUnit = getOptString(j, "horizontal_offset_unit");
    if (!dmmf->mHorizontalOffsetUnit.empty() &&
        dmmf->mHorizontalOffsetUnit != STR_METRE &&
        dmmf->mHorizontalOffsetUnit != STR_DEGREE) {
        throw ParsingException("Unsupported value for horizontal_offset_unit");
    }
    dmmf->mVerticalOffsetUnit = getOptString(j, "vertical_offset_unit");
    if (!dmmf->mVerticalOffsetUnit.empty() &&
        dmmf->mVerticalOffsetUnit != STR_METRE) {
        throw ParsingException("Unsupported value for vertical_offset_unit");
    }
    dmmf->mHorizontalUncertaintyType =
        getOptString(j, "horizontal_uncertainty_type");
    dmmf->mHorizontalUncertaintyUnit =
        getOptString(j, "horizontal_uncertainty_unit");
    dmmf->mVerticalUncertaintyType =
        getOptString(j, "vertical_uncertainty_type");
    dmmf->mVerticalUncertaintyUnit =
        getOptString(j, "vertical_uncertainty_unit");
    dmmf->mHorizontalOffsetMethod = getOptString(j, "horizontal_offset_method");
    if (!dmmf->mHorizontalOffsetMethod.empty() &&
        dmmf->mHorizontalOffsetMethod != STR_ADDITION &&
        dmmf->mHorizontalOffsetMethod != STR_GEOCENTRIC) {
        throw ParsingException(
            "Unsupported value for horizontal_offset_method");
    }
    dmmf->mSpatialExtent = SpatialExtent::parse(getObjectMember(j, "extent"));

    const json jTimeExtent = getObjectMember(j, "time_extent");
    dmmf->mTimeExtent.first = Epoch(getReqString(jTimeExtent, "first"));
    dmmf->mTimeExtent.last = Epoch(getReqString(jTimeExtent, "last"));

    const json jComponents = getArrayMember(j, "components");
    for (const json &jComponent : jComponents) {
        dmmf->mComponents.emplace_back(Component::parse(jComponent));
        const auto &comp = dmmf->mComponents.back();
        if (comp.displacementType() == STR_HORIZONTAL ||
            comp.displacementType() == STR_3D) {
            if (dmmf->mHorizontalOffsetUnit.empty()) {
                throw ParsingException("horizontal_offset_unit should be "
                                       "defined as there is a component with "
                                       "displacement_type = horizontal/3d");
            }
            if (dmmf->mHorizontalOffsetMethod.empty()) {
                throw ParsingException("horizontal_offset_method should be "
                                       "defined as there is a component with "
                                       "displacement_type = horizontal/3d");
            }
        }
        if (comp.displacementType() == STR_VERTICAL ||
            comp.displacementType() == STR_3D) {
            if (dmmf->mVerticalOffsetUnit.empty()) {
                throw ParsingException("vertical_offset_unit should be defined "
                                       "as there is a component with "
                                       "displacement_type = vertical/3d");
            }
        }
        if (dmmf->mHorizontalOffsetUnit == STR_DEGREE &&
            comp.spatialModel().interpolationMethod != STR_BILINEAR) {
            throw ParsingException("horizontal_offset_unit = degree can only "
                                   "be used with interpolation_method = "
                                   "bilinear");
        }
    }

    if (dmmf->mHorizontalOffsetUnit == STR_DEGREE &&
        dmmf->mHorizontalOffsetMethod != STR_ADDITION) {
        throw ParsingException("horizontal_offset_unit = degree can only be "
                               "used with horizontal_offset_method = addition");
    }

    return dmmf;
}

// ---------------------------------------------------------------------------

SpatialExtent SpatialExtent::parse(const json &j) {
    SpatialExtent ex;

    const std::string type = getReqString(j, "type");
    if (type != "bbox") {
        throw ParsingException("unsupported type of extent");
    }

    const json jParameter = getObjectMember(j, "parameters");
    const json jBbox = getArrayMember(jParameter, "bbox");
    if (jBbox.size() != 4) {
        throw ParsingException("bbox is not an array of 4 numeric elements");
    }
    for (int i = 0; i < 4; i++) {
        if (!jBbox[i].is_number()) {
            throw ParsingException(
                "bbox is not an array of 4 numeric elements");
        }
    }
    ex.mMinx = jBbox[0].get<double>();
    ex.mMiny = jBbox[1].get<double>();
    ex.mMaxx = jBbox[2].get<double>();
    ex.mMaxy = jBbox[3].get<double>();

    ex.mMinxRad = DegToRad(ex.mMinx);
    ex.mMinyRad = DegToRad(ex.mMiny);
    ex.mMaxxRad = DegToRad(ex.mMaxx);
    ex.mMaxyRad = DegToRad(ex.mMaxy);

    return ex;
}

// ---------------------------------------------------------------------------

Component Component::parse(const json &j) {
    Component comp;
    if (!j.is_object()) {
        throw ParsingException("component is not an object");
    }
    comp.mDescription = getOptString(j, "description");
    comp.mSpatialExtent = SpatialExtent::parse(getObjectMember(j, "extent"));
    comp.mDisplacementType = getReqString(j, "displacement_type");
    if (comp.mDisplacementType != STR_NONE &&
        comp.mDisplacementType != STR_HORIZONTAL &&
        comp.mDisplacementType != STR_VERTICAL &&
        comp.mDisplacementType != STR_3D) {
        throw ParsingException("Unsupported value for displacement_type");
    }
    comp.mUncertaintyType = getReqString(j, "uncertainty_type");
    comp.mHorizontalUncertainty = getOptDouble(j, "horizontal_uncertainty");
    comp.mVerticalUncertainty = getOptDouble(j, "vertical_uncertainty");

    const json jSpatialModel = getObjectMember(j, "spatial_model");
    comp.mSpatialModel.type = getReqString(jSpatialModel, "type");
    comp.mSpatialModel.interpolationMethod =
        getReqString(jSpatialModel, "interpolation_method");
    if (comp.mSpatialModel.interpolationMethod != STR_BILINEAR &&
        comp.mSpatialModel.interpolationMethod != STR_GEOCENTRIC_BILINEAR) {
        throw ParsingException("Unsupported value for interpolation_method");
    }
    comp.mSpatialModel.filename = getReqString(jSpatialModel, "filename");
    comp.mSpatialModel.md5Checksum =
        getOptString(jSpatialModel, "md5_checksum");

    const json jTimeFunction = getObjectMember(j, "time_function");
    std::string timeFunctionType = getReqString(jTimeFunction, "type");
    const json jParameters = timeFunctionType == "constant"
                                 ? json()
                                 : getObjectMember(jTimeFunction, "parameters");

    if (timeFunctionType == "constant") {
        std::unique_ptr<ConstantTimeFunction> tf(new ConstantTimeFunction());
        tf->type = std::move(timeFunctionType);
        comp.mTimeFunction = std::move(tf);
    } else if (timeFunctionType == "velocity") {
        std::unique_ptr<VelocityTimeFunction> tf(new VelocityTimeFunction());
        tf->type = std::move(timeFunctionType);
        tf->referenceEpoch =
            Epoch(getReqString(jParameters, "reference_epoch"));
        comp.mTimeFunction = std::move(tf);
    } else if (timeFunctionType == "step") {
        std::unique_ptr<StepTimeFunction> tf(new StepTimeFunction());
        tf->type = std::move(timeFunctionType);
        tf->stepEpoch = Epoch(getReqString(jParameters, "step_epoch"));
        comp.mTimeFunction = std::move(tf);
    } else if (timeFunctionType == "reverse_step") {
        std::unique_ptr<ReverseStepTimeFunction> tf(
            new ReverseStepTimeFunction());
        tf->type = std::move(timeFunctionType);
        tf->stepEpoch = Epoch(getReqString(jParameters, "step_epoch"));
        comp.mTimeFunction = std::move(tf);
    } else if (timeFunctionType == "piecewise") {
        std::unique_ptr<PiecewiseTimeFunction> tf(new PiecewiseTimeFunction());
        tf->type = std::move(timeFunctionType);
        tf->beforeFirst = getReqString(jParameters, "before_first");
        if (tf->beforeFirst != "zero" && tf->beforeFirst != "constant" &&
            tf->beforeFirst != "linear") {
            throw ParsingException("Unsupported value for before_first");
        }
        tf->afterLast = getReqString(jParameters, "after_last");
        if (tf->afterLast != "zero" && tf->afterLast != "constant" &&
            tf->afterLast != "linear") {
            throw ParsingException("Unsupported value for afterLast");
        }
        const json jModel = getArrayMember(jParameters, "model");
        for (const json &jModelElt : jModel) {
            if (!jModelElt.is_object()) {
                throw ParsingException("model[] element is not an object");
            }
            PiecewiseTimeFunction::EpochScaleFactorTuple tuple;
            tuple.epoch = Epoch(getReqString(jModelElt, "epoch"));
            tuple.scaleFactor = getReqDouble(jModelElt, "scale_factor");
            tf->model.emplace_back(std::move(tuple));
        }

        comp.mTimeFunction = std::move(tf);
    } else if (timeFunctionType == "exponential") {
        std::unique_ptr<ExponentialTimeFunction> tf(
            new ExponentialTimeFunction());
        tf->type = std::move(timeFunctionType);
        tf->referenceEpoch =
            Epoch(getReqString(jParameters, "reference_epoch"));
        tf->endEpoch = Epoch(getOptString(jParameters, "end_epoch"));
        tf->relaxationConstant =
            getReqDouble(jParameters, "relaxation_constant");
        if (tf->relaxationConstant <= 0.0) {
            throw ParsingException("Invalid value for relaxation_constant");
        }
        tf->beforeScaleFactor =
            getReqDouble(jParameters, "before_scale_factor");
        tf->initialScaleFactor =
            getReqDouble(jParameters, "initial_scale_factor");
        tf->finalScaleFactor = getReqDouble(jParameters, "final_scale_factor");
        comp.mTimeFunction = std::move(tf);
    } else {
        throw ParsingException("Unsupported type of time function: " +
                               timeFunctionType);
    }

    return comp;
}

// ---------------------------------------------------------------------------

double Component::ConstantTimeFunction::evaluateAt(double) const { return 1.0; }

// ---------------------------------------------------------------------------

double Component::VelocityTimeFunction::evaluateAt(double dt) const {
    return dt - referenceEpoch.toDecimalYear();
}

// ---------------------------------------------------------------------------

double Component::StepTimeFunction::evaluateAt(double dt) const {
    if (dt < stepEpoch.toDecimalYear())
        return 0.0;
    return 1.0;
}

// ---------------------------------------------------------------------------

double Component::ReverseStepTimeFunction::evaluateAt(double dt) const {
    if (dt < stepEpoch.toDecimalYear())
        return -1.0;
    return 0.0;
}

// ---------------------------------------------------------------------------

double Component::PiecewiseTimeFunction::evaluateAt(double dt) const {
    if (model.empty()) {
        return 0.0;
    }

    const double dt1 = model[0].epoch.toDecimalYear();
    if (dt < dt1) {
        if (beforeFirst == "zero")
            return 0.0;
        if (beforeFirst == "constant" || model.size() == 1)
            return model[0].scaleFactor;

        // linear
        const double f1 = model[0].scaleFactor;
        const double dt2 = model[1].epoch.toDecimalYear();
        const double f2 = model[1].scaleFactor;
        if (dt1 == dt2)
            return f1;
        return (f1 * (dt2 - dt) + f2 * (dt - dt1)) / (dt2 - dt1);
    }
    for (size_t i = 1; i < model.size(); i++) {
        const double dtip1 = model[i].epoch.toDecimalYear();
        if (dt < dtip1) {
            const double dti = model[i - 1].epoch.toDecimalYear();
            const double fip1 = model[i].scaleFactor;
            const double fi = model[i - 1].scaleFactor;
            return (fi * (dtip1 - dt) + fip1 * (dt - dti)) / (dtip1 - dti);
        }
    }
    if (afterLast == "zero") {
        return 0.0;
    }
    if (afterLast == "constant" || model.size() == 1)
        return model.back().scaleFactor;

    // linear
    const double dtnm1 = model[model.size() - 2].epoch.toDecimalYear();
    const double fnm1 = model[model.size() - 2].scaleFactor;
    const double dtn = model.back().epoch.toDecimalYear();
    const double fn = model.back().scaleFactor;
    if (dtnm1 == dtn)
        return fn;
    return (fnm1 * (dtn - dt) + fn * (dt - dtnm1)) / (dtn - dtnm1);
}

// ---------------------------------------------------------------------------

double Component::ExponentialTimeFunction::evaluateAt(double dt) const {
    const double t0 = referenceEpoch.toDecimalYear();
    if (dt < t0)
        return beforeScaleFactor;
    if (!endEpoch.toString().empty()) {
        dt = std::min(dt, endEpoch.toDecimalYear());
    }
    return initialScaleFactor +
           (finalScaleFactor - initialScaleFactor) *
               (1.0 - std::exp(-(dt - t0) / relaxationConstant));
}

// ---------------------------------------------------------------------------

inline void DeltaEastingNorthingToLongLat(double cosphi, double de, double dn,
                                          double a, double b, double es,
                                          double &dlam, double &dphi) {
    const double oneMinuX = es * (1 - cosphi * cosphi);
    const double X = 1 - oneMinuX;
    const double sqrtX = sqrt(X);
#if 0
    // With es of Earth, absolute/relative error is at most 2e-8
    // const double sqrtX = 1 - 0.5 * oneMinuX * (1 + 0.25 * oneMinuX);
#endif
    dlam = de * sqrtX / (a * cosphi);
    dphi = dn * a * sqrtX * X / (b * b);
}

// ---------------------------------------------------------------------------

template <class Grid, class GridSet, class EvaluatorIface>
Evaluator<Grid, GridSet, EvaluatorIface>::Evaluator(
    std::unique_ptr<MasterFile> &&model, EvaluatorIface &iface, double a,
    double b)
    : mModel(std::move(model)), mA(a), mB(b), mEs(1 - (b * b) / (a * a)),
      mIsHorizontalUnitDegree(mModel->horizontalOffsetUnit() == STR_DEGREE),
      mIsAddition(mModel->horizontalOffsetMethod() == STR_ADDITION),
      mIsGeographicCRS(iface.isGeographicCRS(mModel->definitionCRS())) {
    if (!mIsGeographicCRS && mIsHorizontalUnitDegree) {
        throw EvaluatorException(
            "definition_crs = projected CRS and "
            "horizontal_offset_unit = degree are incompatible");
    }
    if (!mIsGeographicCRS && !mIsAddition) {
        throw EvaluatorException(
            "definition_crs = projected CRS and "
            "horizontal_offset_method = geocentric are incompatible");
    }
    mComponents.reserve(mModel->components().size());
    for (const auto &comp : mModel->components()) {
        mComponents.emplace_back(std::unique_ptr<ComponentEx<Grid, GridSet>>(
            new ComponentEx<Grid, GridSet>(comp)));
        if (!mIsGeographicCRS && !mComponents.back()->isBilinearInterpolation) {
            throw EvaluatorException(
                "definition_crs = projected CRS and "
                "interpolation_method = geocentric_bilinear are incompatible");
        }
    }
}

// ---------------------------------------------------------------------------

template <class Grid, class GridSet, class EvaluatorIface>
void Evaluator<Grid, GridSet, EvaluatorIface>::clearGridCache() {
    for (auto &comp : mComponents) {
        comp->clearGridCache();
    }
}

// ---------------------------------------------------------------------------

#ifdef DEBUG_DEFMODEL

static std::string shortName(const Component &comp) {
    const auto &desc = comp.description();
    return desc.substr(0, desc.find('\n')) + " (" +
           comp.spatialModel().filename + ")";
}

static std::string toString(double val) {
    char buffer[32];
    snprintf(buffer, sizeof(buffer), "%.9g", val);
    return buffer;
}

#endif

// ---------------------------------------------------------------------------

static bool bboxCheck(double &x, double &y, bool forInverseComputation,
                      const double minx, const double miny, const double maxx,
                      const double maxy, const double EPS,
                      const double extraMarginForInverse) {
    if (x < minx - EPS || x > maxx + EPS || y < miny - EPS || y > maxy + EPS) {
        if (!forInverseComputation) {
            return false;
        }
        // In case of iterative computation for inverse, allow to be a
        // slightly bit outside of the grid and clamp to the edges
        bool xOk = false;
        if (x >= minx - EPS && x <= maxx + EPS) {
            xOk = true;
        } else if (x > minx - extraMarginForInverse && x < minx) {
            x = minx;
            xOk = true;
        } else if (x < maxx + extraMarginForInverse && x > maxx) {
            x = maxx;
            xOk = true;
        }

        bool yOk = false;
        if (y >= miny - EPS && y <= maxy + EPS) {
            yOk = true;
        } else if (y > miny - extraMarginForInverse && y < miny) {
            y = miny;
            yOk = true;
        } else if (y < maxy + extraMarginForInverse && y > maxy) {
            y = maxy;
            yOk = true;
        }

        return xOk && yOk;
    }
    return true;
}

// ---------------------------------------------------------------------------

template <class Grid, class GridSet, class EvaluatorIface>
bool Evaluator<Grid, GridSet, EvaluatorIface>::forward(
    EvaluatorIface &iface, double x, double y, double z, double t,
    bool forInverseComputation, double &x_out, double &y_out, double &z_out)

{
    x_out = x;
    y_out = y;
    z_out = z;

    const double EPS = mIsGeographicCRS ? 1e-10 : 1e-5;

    // Check against global model spatial extent, potentially wrapping
    // longitude to match
    {
        const auto &extent = mModel->extent();
        const double minx = extent.minxNormalized(mIsGeographicCRS);
        const double maxx = extent.maxxNormalized(mIsGeographicCRS);
        if (mIsGeographicCRS) {
            while (x < minx - EPS) {
                x += 2.0 * DEFMODEL_PI;
            }
            while (x > maxx + EPS) {
                x -= 2.0 * DEFMODEL_PI;
            }
        }
        const double miny = extent.minyNormalized(mIsGeographicCRS);
        const double maxy = extent.maxyNormalized(mIsGeographicCRS);
        const double extraMarginForInverse =
            mIsGeographicCRS ? DegToRad(0.1) : 10000;
        if (!bboxCheck(x, y, forInverseComputation, minx, miny, maxx, maxy, EPS,
                       extraMarginForInverse)) {
#ifdef DEBUG_DEFMODEL
            iface.log("Calculation point " + toString(x) + "," + toString(y) +
                      " is outside the extents of the deformation model");
#endif
            return false;
        }
    }

    // Check against global model temporal extent
    {
        const auto &timeExtent = mModel->timeExtent();
        if (t < timeExtent.first.toDecimalYear() ||
            t > timeExtent.last.toDecimalYear()) {
#ifdef DEBUG_DEFMODEL
            iface.log("Calculation epoch " + toString(t) +
                      " is not valid for the deformation model");
#endif
            return false;
        }
    }

    // For mIsHorizontalUnitDegree
    double dlam = 0;
    double dphi = 0;

    // For !mIsHorizontalUnitDegree
    double de = 0;
    double dn = 0;

    double dz = 0;

    bool sincosphiInitialized = false;
    double sinphi = 0;
    double cosphi = 0;

    for (auto &compEx : mComponents) {
        const auto &comp = compEx->component;
        if (compEx->displacementType == DisplacementType::NONE) {
            continue;
        }
        const auto &extent = comp.extent();
        double xForGrid = x;
        double yForGrid = y;
        const double minx = extent.minxNormalized(mIsGeographicCRS);
        const double maxx = extent.maxxNormalized(mIsGeographicCRS);
        const double miny = extent.minyNormalized(mIsGeographicCRS);
        const double maxy = extent.maxyNormalized(mIsGeographicCRS);
        const double extraMarginForInverse = 0;
        if (!bboxCheck(xForGrid, yForGrid, forInverseComputation, minx, miny,
                       maxx, maxy, EPS, extraMarginForInverse)) {
#ifdef DEBUG_DEFMODEL
            iface.log(
                "Skipping component " + shortName(comp) +
                " due to point being outside of its declared spatial extent.");
#endif
            continue;
        }
        xForGrid = std::max(xForGrid, minx);
        yForGrid = std::max(yForGrid, miny);
        xForGrid = std::min(xForGrid, maxx);
        yForGrid = std::min(yForGrid, maxy);
        const auto tfactor = compEx->evaluateAt(t);
        if (tfactor == 0.0) {
#ifdef DEBUG_DEFMODEL
            iface.log("Skipping component " + shortName(comp) +
                      " due to time function evaluating to 0.");
#endif
            continue;
        }

#ifdef DEBUG_DEFMODEL
        iface.log("Entering component " + shortName(comp) +
                  " with time function evaluating to " + toString(tfactor) +
                  ".");
#endif

        if (compEx->gridSet == nullptr) {
            compEx->gridSet = iface.open(comp.spatialModel().filename);
            if (compEx->gridSet == nullptr) {
                return false;
            }
        }
        const Grid *grid = compEx->gridSet->gridAt(xForGrid, yForGrid);
        if (grid == nullptr) {
#ifdef DEBUG_DEFMODEL
            iface.log("Skipping component " + shortName(comp) +
                      " due to no grid found for this point in the grid set.");
#endif
            continue;
        }
        if (grid->width < 2 || grid->height < 2) {
            return false;
        }
        const double ix_d = (xForGrid - grid->minx) / grid->resx;
        const double iy_d = (yForGrid - grid->miny) / grid->resy;
        if (ix_d < -EPS || iy_d < -EPS || ix_d + 1 >= grid->width + EPS ||
            iy_d + 1 >= grid->height + EPS) {
#ifdef DEBUG_DEFMODEL
            iface.log("Skipping component " + shortName(comp) +
                      " due to point being outside of actual spatial extent of "
                      "grid " +
                      grid->name() + ".");
#endif
            continue;
        }
        const int ix0 = std::min(static_cast<int>(ix_d), grid->width - 2);
        const int iy0 = std::min(static_cast<int>(iy_d), grid->height - 2);
        const int ix1 = ix0 + 1;
        const int iy1 = iy0 + 1;
        const double frct_x = ix_d - ix0;
        const double frct_y = iy_d - iy0;
        const double one_minus_frct_x = 1. - frct_x;
        const double one_minus_frct_y = 1. - frct_y;
        const double m00 = one_minus_frct_x * one_minus_frct_y;
        const double m10 = frct_x * one_minus_frct_y;
        const double m01 = one_minus_frct_x * frct_y;
        const double m11 = frct_x * frct_y;

        if (compEx->displacementType == DisplacementType::VERTICAL) {
            double dz00 = 0;
            double dz01 = 0;
            double dz10 = 0;
            double dz11 = 0;
            if (!grid->getZOffset(ix0, iy0, dz00) ||
                !grid->getZOffset(ix1, iy0, dz10) ||
                !grid->getZOffset(ix0, iy1, dz01) ||
                !grid->getZOffset(ix1, iy1, dz11)) {
                return false;
            }
            const double dzInterp =
                dz00 * m00 + dz01 * m01 + dz10 * m10 + dz11 * m11;
#ifdef DEBUG_DEFMODEL
            iface.log("tfactor * dzInterp = " + toString(tfactor) + " * " +
                      toString(dzInterp) + ".");
#endif
            dz += tfactor * dzInterp;
        } else if (mIsHorizontalUnitDegree) {
            double dx00 = 0;
            double dy00 = 0;
            double dx01 = 0;
            double dy01 = 0;
            double dx10 = 0;
            double dy10 = 0;
            double dx11 = 0;
            double dy11 = 0;
            if (compEx->displacementType == DisplacementType::HORIZONTAL) {
                if (!grid->getLongLatOffset(ix0, iy0, dx00, dy00) ||
                    !grid->getLongLatOffset(ix1, iy0, dx10, dy10) ||
                    !grid->getLongLatOffset(ix0, iy1, dx01, dy01) ||
                    !grid->getLongLatOffset(ix1, iy1, dx11, dy11)) {
                    return false;
                }
            } else /* if (compEx->displacementType == DisplacementType::THREE_D)
                    */
            {
                double dz00 = 0;
                double dz01 = 0;
                double dz10 = 0;
                double dz11 = 0;
                if (!grid->getLongLatZOffset(ix0, iy0, dx00, dy00, dz00) ||
                    !grid->getLongLatZOffset(ix1, iy0, dx10, dy10, dz10) ||
                    !grid->getLongLatZOffset(ix0, iy1, dx01, dy01, dz01) ||
                    !grid->getLongLatZOffset(ix1, iy1, dx11, dy11, dz11)) {
                    return false;
                }
                const double dzInterp =
                    dz00 * m00 + dz01 * m01 + dz10 * m10 + dz11 * m11;
#ifdef DEBUG_DEFMODEL
                iface.log("tfactor * dzInterp = " + toString(tfactor) + " * " +
                          toString(dzInterp) + ".");
#endif
                dz += tfactor * dzInterp;
            }
            const double dlamInterp =
                dx00 * m00 + dx01 * m01 + dx10 * m10 + dx11 * m11;
            const double dphiInterp =
                dy00 * m00 + dy01 * m01 + dy10 * m10 + dy11 * m11;
#ifdef DEBUG_DEFMODEL
            iface.log("tfactor * dlamInterp = " + toString(tfactor) + " * " +
                      toString(dlamInterp) + ".");
            iface.log("tfactor * dphiInterp = " + toString(tfactor) + " * " +
                      toString(dphiInterp) + ".");
#endif
            dlam += tfactor * dlamInterp;
            dphi += tfactor * dphiInterp;
        } else /* horizontal unit is metre */ {
            double de00 = 0;
            double dn00 = 0;
            double de01 = 0;
            double dn01 = 0;
            double de10 = 0;
            double dn10 = 0;
            double de11 = 0;
            double dn11 = 0;
            if (compEx->displacementType == DisplacementType::HORIZONTAL) {
                if (!grid->getEastingNorthingOffset(ix0, iy0, de00, dn00) ||
                    !grid->getEastingNorthingOffset(ix1, iy0, de10, dn10) ||
                    !grid->getEastingNorthingOffset(ix0, iy1, de01, dn01) ||
                    !grid->getEastingNorthingOffset(ix1, iy1, de11, dn11)) {
                    return false;
                }
            } else /* if (compEx->displacementType == DisplacementType::THREE_D)
                    */
            {
                double dz00 = 0;
                double dz01 = 0;
                double dz10 = 0;
                double dz11 = 0;
                if (!grid->getEastingNorthingZOffset(ix0, iy0, de00, dn00,
                                                     dz00) ||
                    !grid->getEastingNorthingZOffset(ix1, iy0, de10, dn10,
                                                     dz10) ||
                    !grid->getEastingNorthingZOffset(ix0, iy1, de01, dn01,
                                                     dz01) ||
                    !grid->getEastingNorthingZOffset(ix1, iy1, de11, dn11,
                                                     dz11)) {
                    return false;
                }
                const double dzInterp =
                    dz00 * m00 + dz01 * m01 + dz10 * m10 + dz11 * m11;
#ifdef DEBUG_DEFMODEL
                iface.log("tfactor * dzInterp = " + toString(tfactor) + " * " +
                          toString(dzInterp) + ".");
#endif
                dz += tfactor * dzInterp;
            }
            if (compEx->isBilinearInterpolation) {
                const double deInterp =
                    de00 * m00 + de01 * m01 + de10 * m10 + de11 * m11;
                const double dnInterp =
                    dn00 * m00 + dn01 * m01 + dn10 * m10 + dn11 * m11;
#ifdef DEBUG_DEFMODEL
                iface.log("tfactor * deInterp = " + toString(tfactor) + " * " +
                          toString(deInterp) + ".");
                iface.log("tfactor * dnInterp = " + toString(tfactor) + " * " +
                          toString(dnInterp) + ".");
#endif
                de += tfactor * deInterp;
                dn += tfactor * dnInterp;
            } else /* geocentric_bilinear */ {
                double dX;
                double dY;
                double dZ;

                auto iter = compEx->mapGrids.find(grid);
                if (iter == compEx->mapGrids.end()) {
                    GridEx<Grid> gridWithCache(grid);
                    iter = compEx->mapGrids
                               .insert(std::pair<const Grid *, GridEx<Grid>>(
                                   grid, std::move(gridWithCache)))
                               .first;
                }
                GridEx<Grid> &gridwithCacheRef = iter->second;

                gridwithCacheRef.getBilinearGeocentric(
                    ix0, iy0, de00, dn00, de01, dn01, de10, dn10, de11, dn11,
                    m00, m01, m10, m11, dX, dY, dZ);
                if (!sincosphiInitialized) {
                    sincosphiInitialized = true;
                    sinphi = sin(y);
                    cosphi = cos(y);
                }
                const double lam_rel_to_cell_center =
                    (frct_x - 0.5) * grid->resx;
                // Use small-angle approximation of sin/cos when reasonable
                // Max abs/rel error on cos is 3.9e-9 and on sin 1.3e-11
                const double sinlam =
                    gridwithCacheRef.smallResx
                        ? lam_rel_to_cell_center *
                              (1 - (1. / 6) * (lam_rel_to_cell_center *
                                               lam_rel_to_cell_center))
                        : sin(lam_rel_to_cell_center);
                const double coslam = gridwithCacheRef.smallResx
                                          ? (1 - 0.5 * (lam_rel_to_cell_center *
                                                        lam_rel_to_cell_center))
                                          : cos(lam_rel_to_cell_center);

                // Convert back from geocentric deltas to easting, northing
                // deltas
                const double deInterp = -dX * sinlam + dY * coslam;
                const double dnInterp =
                    (-dX * coslam - dY * sinlam) * sinphi + dZ * cosphi;
#ifdef DEBUG_DEFMODEL
                iface.log("After geocentric_bilinear interpolation: tfactor * "
                          "deInterp = " +
                          toString(tfactor) + " * " + toString(deInterp) + ".");
                iface.log("After geocentric_bilinear interpolation: tfactor * "
                          "dnInterp = " +
                          toString(tfactor) + " * " + toString(dnInterp) + ".");
#endif
                de += tfactor * deInterp;
                dn += tfactor * dnInterp;
            }
        }
    }

    // Apply shifts depending on horizontal_offset_unit and
    // horizontal_offset_method
    if (mIsHorizontalUnitDegree) {
        x_out += dlam;
        y_out += dphi;
    } else {
#ifdef DEBUG_DEFMODEL
        iface.log("Total sum of de: " + toString(de));
        iface.log("Total sum of dn: " + toString(dn));
#endif
        if (mIsAddition && !mIsGeographicCRS) {
            x_out += de;
            y_out += dn;
        } else if (mIsAddition) {
            // Simple way of adding the offset
            if (!sincosphiInitialized) {
                cosphi = cos(y);
            }
            DeltaEastingNorthingToLongLat(cosphi, de, dn, mA, mB, mEs, dlam,
                                          dphi);
#ifdef DEBUG_DEFMODEL
            iface.log("Result dlam: " + toString(dlam));
            iface.log("Result dphi: " + toString(dphi));
#endif
            x_out += dlam;
            y_out += dphi;
        } else {
            // Geocentric way of adding the offset
            if (!sincosphiInitialized) {
                sinphi = sin(y);
                cosphi = cos(y);
            }
            const double sinlam = sin(x);
            const double coslam = cos(x);
            const double dnsinphi = dn * sinphi;
            const double dX = -de * sinlam - dnsinphi * coslam;
            const double dY = de * coslam - dnsinphi * sinlam;
            const double dZ = dn * cosphi;

            double X;
            double Y;
            double Z;
            iface.geographicToGeocentric(x, y, 0, mA, mB, mEs, X, Y, Z);
#ifdef DEBUG_DEFMODEL
            iface.log("Geocentric coordinate before: " + toString(X) + "," +
                      toString(Y) + "," + toString(Z));
            iface.log("Geocentric shift: " + toString(dX) + "," + toString(dY) +
                      "," + toString(dZ));
#endif
            X += dX;
            Y += dY;
            Z += dZ;
#ifdef DEBUG_DEFMODEL
            iface.log("Geocentric coordinate after: " + toString(X) + "," +
                      toString(Y) + "," + toString(Z));
#endif

            double h_out_ignored;
            iface.geocentricToGeographic(X, Y, Z, mA, mB, mEs, x_out, y_out,
                                         h_out_ignored);
        }
    }
#ifdef DEBUG_DEFMODEL
    iface.log("Total sum of dz: " + toString(dz));
#endif
    z_out += dz;

    return true;
}

// ---------------------------------------------------------------------------

template <class Grid, class GridSet, class EvaluatorIface>
bool Evaluator<Grid, GridSet, EvaluatorIface>::inverse(
    EvaluatorIface &iface, double x, double y, double z, double t,
    double &x_out, double &y_out, double &z_out)

{
    x_out = x;
    y_out = y;
    z_out = z;
    constexpr double EPS_HORIZ = 1e-12;
    constexpr double EPS_VERT = 1e-3;
    constexpr bool forInverseComputation = true;
    for (int i = 0; i < 10; i++) {
#ifdef DEBUG_DEFMODEL
        iface.log("Iteration " + std::to_string(i) + ": before forward: x=" +
                  toString(x_out) + ", y=" + toString(y_out));
#endif
        double x_new;
        double y_new;
        double z_new;
        if (!forward(iface, x_out, y_out, z_out, t, forInverseComputation,
                     x_new, y_new, z_new)) {
            return false;
        }
#ifdef DEBUG_DEFMODEL
        iface.log("After forward: x=" + toString(x_new) +
                  ", y=" + toString(y_new));
#endif
        const double dx = x_new - x;
        const double dy = y_new - y;
        const double dz = z_new - z;
        x_out -= dx;
        y_out -= dy;
        z_out -= dz;
        if (std::max(std::fabs(dx), std::fabs(dy)) < EPS_HORIZ &&
            std::fabs(dz) < EPS_VERT) {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

} // namespace DEFORMATON_MODEL_NAMESPACE
