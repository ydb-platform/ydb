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

/** This file implements the gridded deformation model proposol of
 * https://docs.google.com/document/d/1wiyrAmzqh8MZlzHSp3wf594Ob_M1LeFtDA5swuzvLZY
 * It is written in a generic way, independent of the rest of PROJ
 * infrastructure.
 *
 * Verbose debugging info can be turned on by setting the DEBUG_DEFMODEL macro
 */

#ifndef DEFMODEL_HPP
#define DEFMODEL_HPP

#ifdef PROJ_COMPILATION
#include "proj/internal/include_nlohmann_json.hpp"
#else
#include "nlohmann/json.hpp"
#endif

#include <algorithm>
#include <cmath>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#ifndef DEFORMATON_MODEL_NAMESPACE
#define DEFORMATON_MODEL_NAMESPACE DeformationModel
#endif

#include "defmodel_exceptions.hpp"

namespace DEFORMATON_MODEL_NAMESPACE {

using json = nlohmann::json;

// ---------------------------------------------------------------------------

/** Spatial extent as a bounding box. */
class SpatialExtent {
  public:
    /** Parse the provided object as an extent.
     *
     * @throws ParsingException in case of error.
     */
    static SpatialExtent parse(const json &j);

    double minx() const { return mMinx; }
    double miny() const { return mMiny; }
    double maxx() const { return mMaxx; }
    double maxy() const { return mMaxy; }

    double minxNormalized(bool bIsGeographic) const {
        return bIsGeographic ? mMinxRad : mMinx;
    }
    double minyNormalized(bool bIsGeographic) const {
        return bIsGeographic ? mMinyRad : mMiny;
    }
    double maxxNormalized(bool bIsGeographic) const {
        return bIsGeographic ? mMaxxRad : mMaxx;
    }
    double maxyNormalized(bool bIsGeographic) const {
        return bIsGeographic ? mMaxyRad : mMaxy;
    }

  protected:
    friend class MasterFile;
    friend class Component;
    SpatialExtent() = default;

  private:
    double mMinx = std::numeric_limits<double>::quiet_NaN();
    double mMiny = std::numeric_limits<double>::quiet_NaN();
    double mMaxx = std::numeric_limits<double>::quiet_NaN();
    double mMaxy = std::numeric_limits<double>::quiet_NaN();
    double mMinxRad = std::numeric_limits<double>::quiet_NaN();
    double mMinyRad = std::numeric_limits<double>::quiet_NaN();
    double mMaxxRad = std::numeric_limits<double>::quiet_NaN();
    double mMaxyRad = std::numeric_limits<double>::quiet_NaN();
};

// ---------------------------------------------------------------------------

/** Epoch */
class Epoch {
  public:
    /** Constructor from a ISO 8601 date-time. May throw ParsingException */
    explicit Epoch(const std::string &dt = std::string());

    /** Return ISO 8601 date-time */
    const std::string &toString() const { return mDt; }

    /** Return decimal year */
    double toDecimalYear() const;

  private:
    std::string mDt{};
    double mDecimalYear = 0;
};

// ---------------------------------------------------------------------------

/** Component of a deformation model. */
class Component {
  public:
    /** Parse the provided object as a component.
     *
     * @throws ParsingException in case of error.
     */
    static Component parse(const json &j);

    /** Get a text description of the component. */
    const std::string &description() const { return mDescription; }

    /** Get the region within which the component is defined. Outside this
     * region the component evaluates to 0. */
    const SpatialExtent &extent() const { return mSpatialExtent; }

    /** Get the displacement parameters defined by the component, one of
     * "none", "horizontal", "vertical", and "3d".  The "none" option allows
     *  for a component which defines uncertainty with different grids to those
     *  defining displacement. */
    const std::string &displacementType() const { return mDisplacementType; }

    /** Get the uncertainty parameters defined by the component,
     * one of "none", "horizontal", "vertical", "3d". */
    const std::string &uncertaintyType() const { return mUncertaintyType; }

    /** Get the horizontal uncertainty to use if it is not defined explicitly
     * in the spatial model. */
    double horizontalUncertainty() const { return mHorizontalUncertainty; }

    /** Get the vertical uncertainty to use if it is not defined explicitly in
     * the spatial model. */
    double verticalUncertainty() const { return mVerticalUncertainty; }

    struct SpatialModel {
        /** Specifies the type of the spatial model data file.  Initially it
         * is proposed that only "GeoTIFF" is supported. */
        std::string type{};

        /** How values in model should be interpolated. This proposal will
         * support "bilinear" and "geocentric_bilinear".  */
        std::string interpolationMethod{};

        /** Specifies location of the spatial model GeoTIFF file relative to
         * the master JSON file. */
        std::string filename{};

        /** A hex encoded MD5 checksum of the grid file that can be used to
         * validate that it is the correct version of the file. */
        std::string md5Checksum{};
    };

    /** Get the spatial model. */
    const SpatialModel &spatialModel() const { return mSpatialModel; }

    /** Generic type for a type function */
    struct TimeFunction {
        std::string type{};

        virtual ~TimeFunction();

        virtual double evaluateAt(double dt) const = 0;

      protected:
        TimeFunction() = default;
    };
    struct ConstantTimeFunction : public TimeFunction {

        virtual double evaluateAt(double dt) const override;
    };
    struct VelocityTimeFunction : public TimeFunction {
        /** Date/time at which the velocity function is zero. */
        Epoch referenceEpoch{};

        virtual double evaluateAt(double dt) const override;
    };

    struct StepTimeFunction : public TimeFunction {
        /** Epoch at which the step function transitions from 0 to 1. */
        Epoch stepEpoch{};

        virtual double evaluateAt(double dt) const override;
    };

    struct ReverseStepTimeFunction : public TimeFunction {
        /** Epoch at which the reverse step function transitions from 1. to 0 */
        Epoch stepEpoch{};

        virtual double evaluateAt(double dt) const override;
    };

    struct PiecewiseTimeFunction : public TimeFunction {
        /** One of "zero", "constant", and "linear", defines the behavior of
         * the function before the first defined epoch */
        std::string beforeFirst{};

        /** One of "zero", "constant", and "linear", defines the behavior of
         * the function after the last defined epoch */
        std::string afterLast{};

        struct EpochScaleFactorTuple {
            /** Defines the date/time of the data point. */
            Epoch epoch{};

            /** Function value at the above epoch */
            double scaleFactor = std::numeric_limits<double>::quiet_NaN();
        };

        /** A sorted array data points each defined by two elements.
         * The array is sorted in order of increasing epoch.
         * Note: where the time function includes a step it is represented by
         * two consecutive data points with the same epoch.  The first defines
         * the scale factor that applies before the epoch and the second the
         * scale factor that applies after the epoch. */
        std::vector<EpochScaleFactorTuple> model{};

        virtual double evaluateAt(double dt) const override;
    };

    struct ExponentialTimeFunction : public TimeFunction {
        /** The date/time at which the exponential decay starts. */
        Epoch referenceEpoch{};

        /** The date/time at which the exponential decay ends. */
        Epoch endEpoch{};

        /** The relaxation constant in years. */
        double relaxationConstant = std::numeric_limits<double>::quiet_NaN();

        /** The scale factor that applies before the reference epoch. */
        double beforeScaleFactor = std::numeric_limits<double>::quiet_NaN();

        /** Initial scale factor. */
        double initialScaleFactor = std::numeric_limits<double>::quiet_NaN();

        /** The scale factor the exponential function approaches. */
        double finalScaleFactor = std::numeric_limits<double>::quiet_NaN();

        virtual double evaluateAt(double dt) const override;
    };

    /** Get the time function. */
    const TimeFunction *timeFunction() const { return mTimeFunction.get(); }

  private:
    Component() = default;

    std::string mDescription{};
    SpatialExtent mSpatialExtent{};
    std::string mDisplacementType{};
    std::string mUncertaintyType{};
    double mHorizontalUncertainty = std::numeric_limits<double>::quiet_NaN();
    double mVerticalUncertainty = std::numeric_limits<double>::quiet_NaN();
    SpatialModel mSpatialModel{};
    std::unique_ptr<TimeFunction> mTimeFunction{};
};

Component::TimeFunction::~TimeFunction() = default;

// ---------------------------------------------------------------------------

/** Master file of a deformation model. */
class MasterFile {
  public:
    /** Parse the provided serialized JSON content and return an object.
     *
     * @throws ParsingException in case of error.
     */
    static std::unique_ptr<MasterFile> parse(const std::string &text);

    /** Get file type. Should always be "deformation_model_master_file" */
    const std::string &fileType() const { return mFileType; }

    /** Get the version of the format. At time of writing, only "1.0" is known
     */
    const std::string &formatVersion() const { return mFormatVersion; }

    /** Get brief descriptive name of the deformation model. */
    const std::string &name() const { return mName; }

    /** Get a string identifying the version of the deformation model.
     * The format for specifying version is defined by the agency
     * responsible for the deformation model. */
    const std::string &version() const { return mVersion; }

    /** Get a string identifying the license of the file.
     * e.g "Create Commons Attribution 4.0 International" */
    const std::string &license() const { return mLicense; }

    /** Get a text description of the model. Intended to be longer than name()
     */
    const std::string &description() const { return mDescription; }

    /** Get a text description of the model. Intended to be longer than name()
     */
    const std::string &publicationDate() const { return mPublicationDate; }

    /** Basic information on the agency responsible for the model. */
    struct Authority {
        std::string name{};
        std::string url{};
        std::string address{};
        std::string email{};
    };

    /** Get basic information on the agency responsible for the model. */
    const Authority &authority() const { return mAuthority; }

    /** Hyperlink related to the model. */
    struct Link {
        /** URL holding the information */
        std::string href{};

        /** Relationship to the dataset. e.g. "about", "source", "license",
         * "metadata" */
        std::string rel{};

        /** Mime type */
        std::string type{};

        /** Description of the link */
        std::string title{};
    };

    /** Get links to related information. */
    const std::vector<Link> links() const { return mLinks; }

    /** Get a string identifying the source CRS. That is the coordinate
     * reference system to which the deformation model applies. Typically
     * "EPSG:XXXX" */
    const std::string &sourceCRS() const { return mSourceCRS; }

    /** Get a string identifying the target CRS. That is, for a time
     * dependent coordinate transformation, the coordinate reference
     * system resulting from applying the deformation.
     * Typically "EPSG:XXXX" */
    const std::string &targetCRS() const { return mTargetCRS; }

    /** Get a string identifying the definition CRS. That is, the
     * coordinate reference system used to define the component spatial
     * models. Typically "EPSG:XXXX" */
    const std::string &definitionCRS() const { return mDefinitionCRS; }

    /** Get the nominal reference epoch of the deformation model. Formatted
     * as a ISO-8601 date-time. This is not necessarily used to calculate
     * the deformation model - each component defines its own time function. */
    const std::string &referenceEpoch() const { return mReferenceEpoch; }

    /** Get the epoch at which the uncertainties of the deformation model
     * are calculated. Formatted as a ISO-8601 date-time. */
    const std::string &uncertaintyReferenceEpoch() const {
        return mUncertaintyReferenceEpoch;
    }

    /** Unit of horizontal offsets. Only "metre" and "degree" are supported. */
    const std::string &horizontalOffsetUnit() const {
        return mHorizontalOffsetUnit;
    }

    /** Unit of vertical offsets. Only "metre" is supported. */
    const std::string &verticalOffsetUnit() const {
        return mVerticalOffsetUnit;
    }

    /** Type of horizontal uncertainty. e.g "circular 95% confidence limit" */
    const std::string &horizontalUncertaintyType() const {
        return mHorizontalUncertaintyType;
    }

    /** Unit of horizontal uncertainty. Only "metre" is supported. */
    const std::string &horizontalUncertaintyUnit() const {
        return mHorizontalUncertaintyUnit;
    }

    /** Type of vertical uncertainty. e.g "circular 95% confidence limit" */
    const std::string &verticalUncertaintyType() const {
        return mVerticalUncertaintyType;
    }

    /** Unit of vertical uncertainty. Only "metre" is supported. */
    const std::string &verticalUncertaintyUnit() const {
        return mVerticalUncertaintyUnit;
    }

    /** Defines how the horizontal offsets are applied to geographic
     * coordinates. Only "addition" and "geocentric" are supported */
    const std::string &horizontalOffsetMethod() const {
        return mHorizontalOffsetMethod;
    }

    /** Get the region within which the deformation model is defined.
     * It cannot be calculated outside this region */
    const SpatialExtent &extent() const { return mSpatialExtent; }

    /** Defines the range of times for which the model is valid, specified
     * by a first and a last value. The deformation model is undefined for
     * dates outside this range. */
    struct TimeExtent {
        Epoch first{};
        Epoch last{};
    };

    /** Get the range of times for which the model is valid. */
    const TimeExtent &timeExtent() const { return mTimeExtent; }

    /** Get an array of the components comprising the deformation model. */
    const std::vector<Component> &components() const { return mComponents; }

  private:
    MasterFile() = default;

    std::string mFileType{};
    std::string mFormatVersion{};
    std::string mName{};
    std::string mVersion{};
    std::string mLicense{};
    std::string mDescription{};
    std::string mPublicationDate{};
    Authority mAuthority{};
    std::vector<Link> mLinks{};
    std::string mSourceCRS{};
    std::string mTargetCRS{};
    std::string mDefinitionCRS{};
    std::string mReferenceEpoch{};
    std::string mUncertaintyReferenceEpoch{};
    std::string mHorizontalOffsetUnit{};
    std::string mVerticalOffsetUnit{};
    std::string mHorizontalUncertaintyType{};
    std::string mHorizontalUncertaintyUnit{};
    std::string mVerticalUncertaintyType{};
    std::string mVerticalUncertaintyUnit{};
    std::string mHorizontalOffsetMethod{};
    SpatialExtent mSpatialExtent{};
    TimeExtent mTimeExtent{};
    std::vector<Component> mComponents{};
};

// ---------------------------------------------------------------------------

/** Prototype for a Grid used by GridSet. Intended to be implemented
 * by user code */
struct GridPrototype {
    double minx = 0;
    double miny = 0;
    double resx = 0;
    double resy = 0;
    int width = 0;
    int height = 0;

    // cppcheck-suppress functionStatic
    bool getLongLatOffset(int /*ix*/, int /*iy*/, double & /*longOffsetRadian*/,
                          double & /*latOffsetRadian*/) const {
        throw UnimplementedException("getLongLatOffset unimplemented");
    }

    // cppcheck-suppress functionStatic
    bool getZOffset(int /*ix*/, int /*iy*/, double & /*zOffset*/) const {
        throw UnimplementedException("getZOffset unimplemented");
    }

    // cppcheck-suppress functionStatic
    bool getEastingNorthingOffset(int /*ix*/, int /*iy*/,
                                  double & /*eastingOffset*/,
                                  double & /*northingOffset*/) const {
        throw UnimplementedException("getEastingNorthingOffset unimplemented");
    }

    // cppcheck-suppress functionStatic
    bool getLongLatZOffset(int /*ix*/, int /*iy*/,
                           double & /*longOffsetRadian*/,
                           double & /*latOffsetRadian*/,
                           double & /*zOffset*/) const {
        throw UnimplementedException("getLongLatZOffset unimplemented");
#if 0
        return getLongLatOffset(ix, iy, longOffsetRadian, latOffsetRadian) &&
               getZOffset(ix, iy, zOffset);
#endif
    }

    // cppcheck-suppress functionStatic
    bool getEastingNorthingZOffset(int /*ix*/, int /*iy*/,
                                   double & /*eastingOffset*/,
                                   double & /*northingOffset*/,
                                   double & /*zOffset*/) const {
        throw UnimplementedException("getEastingNorthingOffset unimplemented");
#if 0
        return getEastingNorthingOffset(ix, iy, eastingOffset,
                                        northingOffset) &&
               getZOffset(ix, iy, zOffset);
#endif
    }

#ifdef DEBUG_DEFMODEL
    std::string name() const {
        throw UnimplementedException("name() unimplemented");
    }
#endif
};

// ---------------------------------------------------------------------------

/** Prototype for a GridSet used by EvaluatorIface. Intended to be implemented
 * by user code */
template <class Grid = GridPrototype> struct GridSetPrototype {
    // The return pointer should remain "stable" over time for a given grid
    // of a GridSet.
    // cppcheck-suppress functionStatic
    const Grid *gridAt(double /*x */, double /* y */) {
        throw UnimplementedException("gridAt unimplemented");
    }
};

// ---------------------------------------------------------------------------

/** Prototype for a EvaluatorIface used by Evaluator. Intended to be implemented
 * by user code */
template <class Grid = GridPrototype, class GridSet = GridSetPrototype<>>
struct EvaluatorIfacePrototype {

    std::unique_ptr<GridSet> open(const std::string & /* filename*/) {
        throw UnimplementedException("open unimplemented");
    }

    // cppcheck-suppress functionStatic
    void geographicToGeocentric(double /* lam */, double /* phi */,
                                double /* height*/, double /* a */,
                                double /* b */, double /*es*/, double & /* X */,
                                double & /* Y */, double & /* Z */) {
        throw UnimplementedException("geographicToGeocentric unimplemented");
    }

    // cppcheck-suppress functionStatic
    void geocentricToGeographic(double /* X */, double /* Y */, double /* Z */,
                                double /* a */, double /* b */, double /*es*/,
                                double & /* lam */, double & /* phi */,
                                double & /* height*/) {
        throw UnimplementedException("geocentricToGeographic unimplemented");
    }

    // cppcheck-suppress functionStatic
    bool isGeographicCRS(const std::string & /* crsDef */) {
        throw UnimplementedException("isGeographicCRS unimplemented");
    }

#ifdef DEBUG_DEFMODEL
    void log(const std::string & /* msg */) {
        throw UnimplementedException("log unimplemented");
    }
#endif
};

// ---------------------------------------------------------------------------

/** Internal class to offer caching services over a Component */
template <class Grid, class GridSet> struct ComponentEx;

// ---------------------------------------------------------------------------

/** Class to evaluate the transformation of a coordinate */
template <class Grid = GridPrototype, class GridSet = GridSetPrototype<>,
          class EvaluatorIface = EvaluatorIfacePrototype<>>
class Evaluator {
  public:
    /** Constructor. May throw EvaluatorException */
    explicit Evaluator(std::unique_ptr<MasterFile> &&model,
                       EvaluatorIface &iface, double a, double b);

    /** Evaluate displacement of a position given by (x,y,z,t) and
     * return it in (x_out,y_out_,z_out).
     * For geographic CRS (only supported at that time), x must be a
     * longitude, and y a latitude.
     */
    bool forward(EvaluatorIface &iface, double x, double y, double z, double t,
                 double &x_out, double &y_out, double &z_out) {
        return forward(iface, x, y, z, t, false, x_out, y_out, z_out);
    }

    /** Apply inverse transformation. */
    bool inverse(EvaluatorIface &iface, double x, double y, double z, double t,
                 double &x_out, double &y_out, double &z_out);

    /** Clear grid cache */
    void clearGridCache();

    /** Return whether the definition CRS is a geographic CRS */
    bool isGeographicCRS() const { return mIsGeographicCRS; }

  private:
    std::unique_ptr<MasterFile> mModel;
    const double mA;
    const double mB;
    const double mEs;
    const bool mIsHorizontalUnitDegree; /* degree vs metre */
    const bool mIsAddition;             /* addition vs geocentric */
    const bool mIsGeographicCRS;

    bool forward(EvaluatorIface &iface, double x, double y, double z, double t,
                 bool forInverseComputation, double &x_out, double &y_out,
                 double &z_out);

    std::vector<std::unique_ptr<ComponentEx<Grid, GridSet>>> mComponents{};
};

// ---------------------------------------------------------------------------

} // namespace DEFORMATON_MODEL_NAMESPACE

// ---------------------------------------------------------------------------

#include "defmodel_impl.hpp"

#endif // DEFMODEL_HPP
