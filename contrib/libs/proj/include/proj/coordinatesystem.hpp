/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  ISO19111:2019 implementation
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
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
 ****************************************************************************/

#ifndef CS_HH_INCLUDED
#define CS_HH_INCLUDED

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common.hpp"
#include "io.hpp"
#include "util.hpp"

NS_PROJ_START

/** osgeo.proj.cs namespace

    \brief Coordinate systems and their axis.
*/
namespace cs {

// ---------------------------------------------------------------------------

/** \brief The direction of positive increase in the coordinate value for a
 * coordinate system axis.
 *
 * \remark Implements AxisDirection from \ref ISO_19111_2019
 */
class AxisDirection : public util::CodeList {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL static const AxisDirection *
    valueOf(const std::string &nameIn) noexcept;
    //! @endcond

    AxisDirection(const AxisDirection &) = delete;
    AxisDirection &operator=(const AxisDirection &) = delete;
    AxisDirection(AxisDirection &&) = delete;
    AxisDirection &operator=(AxisDirection &&) = delete;

    PROJ_DLL static const AxisDirection NORTH;
    PROJ_DLL static const AxisDirection NORTH_NORTH_EAST;
    PROJ_DLL static const AxisDirection NORTH_EAST;
    PROJ_DLL static const AxisDirection EAST_NORTH_EAST;
    PROJ_DLL static const AxisDirection EAST;
    PROJ_DLL static const AxisDirection EAST_SOUTH_EAST;
    PROJ_DLL static const AxisDirection SOUTH_EAST;
    PROJ_DLL static const AxisDirection SOUTH_SOUTH_EAST;
    PROJ_DLL static const AxisDirection SOUTH;
    PROJ_DLL static const AxisDirection SOUTH_SOUTH_WEST;
    PROJ_DLL static const AxisDirection SOUTH_WEST;
    PROJ_DLL static const AxisDirection
        WEST_SOUTH_WEST; // note: was forgotten in WKT2-2015
    PROJ_DLL static const AxisDirection WEST;
    PROJ_DLL static const AxisDirection WEST_NORTH_WEST;
    PROJ_DLL static const AxisDirection NORTH_WEST;
    PROJ_DLL static const AxisDirection NORTH_NORTH_WEST;
    PROJ_DLL static const AxisDirection UP;
    PROJ_DLL static const AxisDirection DOWN;
    PROJ_DLL static const AxisDirection GEOCENTRIC_X;
    PROJ_DLL static const AxisDirection GEOCENTRIC_Y;
    PROJ_DLL static const AxisDirection GEOCENTRIC_Z;
    PROJ_DLL static const AxisDirection COLUMN_POSITIVE;
    PROJ_DLL static const AxisDirection COLUMN_NEGATIVE;
    PROJ_DLL static const AxisDirection ROW_POSITIVE;
    PROJ_DLL static const AxisDirection ROW_NEGATIVE;
    PROJ_DLL static const AxisDirection DISPLAY_RIGHT;
    PROJ_DLL static const AxisDirection DISPLAY_LEFT;
    PROJ_DLL static const AxisDirection DISPLAY_UP;
    PROJ_DLL static const AxisDirection DISPLAY_DOWN;
    PROJ_DLL static const AxisDirection FORWARD;
    PROJ_DLL static const AxisDirection AFT;
    PROJ_DLL static const AxisDirection PORT;
    PROJ_DLL static const AxisDirection STARBOARD;
    PROJ_DLL static const AxisDirection CLOCKWISE;
    PROJ_DLL static const AxisDirection COUNTER_CLOCKWISE;
    PROJ_DLL static const AxisDirection TOWARDS;
    PROJ_DLL static const AxisDirection AWAY_FROM;
    PROJ_DLL static const AxisDirection FUTURE;
    PROJ_DLL static const AxisDirection PAST;
    PROJ_DLL static const AxisDirection UNSPECIFIED;

  private:
    explicit AxisDirection(const std::string &nameIn);

    static std::map<std::string, const AxisDirection *> registry;
};

// ---------------------------------------------------------------------------

/** \brief Meaning of the axis value range specified through minimumValue and
 * maximumValue
 *
 * \remark Implements RangeMeaning from \ref ISO_19111_2019
 * \since 9.2
 */
class RangeMeaning : public util::CodeList {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL static const RangeMeaning *
    valueOf(const std::string &nameIn) noexcept;
    //! @endcond

    PROJ_DLL static const RangeMeaning EXACT;
    PROJ_DLL static const RangeMeaning WRAPAROUND;

  protected:
    friend class util::optional<RangeMeaning>;
    RangeMeaning();

  private:
    explicit RangeMeaning(const std::string &nameIn);

    static std::map<std::string, const RangeMeaning *> registry;
};

// ---------------------------------------------------------------------------

class Meridian;
/** Shared pointer of Meridian. */
using MeridianPtr = std::shared_ptr<Meridian>;
/** Non-null shared pointer of Meridian. */
using MeridianNNPtr = util::nn<MeridianPtr>;

/** \brief The meridian that the axis follows from the pole, for a coordinate
 * reference system centered on a pole.
 *
 * \note There is no modelling for this concept in \ref ISO_19111_2019
 *
 * \remark Implements MERIDIAN from \ref WKT2
 */
class PROJ_GCC_DLL Meridian : public common::IdentifiedObject,
                              public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~Meridian() override;
    //! @endcond

    PROJ_DLL const common::Angle &longitude() PROJ_PURE_DECL;

    // non-standard
    PROJ_DLL static MeridianNNPtr create(const common::Angle &longitudeIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)
                        //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    Angle angle_;
#endif

    PROJ_INTERNAL explicit Meridian(const common::Angle &longitudeIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Meridian(const Meridian &other) = delete;
    Meridian &operator=(const Meridian &other) = delete;
};

// ---------------------------------------------------------------------------

class CoordinateSystemAxis;
/** Shared pointer of CoordinateSystemAxis. */
using CoordinateSystemAxisPtr = std::shared_ptr<CoordinateSystemAxis>;
/** Non-null shared pointer of CoordinateSystemAxis. */
using CoordinateSystemAxisNNPtr = util::nn<CoordinateSystemAxisPtr>;

/** \brief The definition of a coordinate system axis.
 *
 * \remark Implements CoordinateSystemAxis from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CoordinateSystemAxis final : public common::IdentifiedObject,
                                                public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CoordinateSystemAxis() override;
    //! @endcond

    PROJ_DLL const std::string &abbreviation() PROJ_PURE_DECL;
    PROJ_DLL const AxisDirection &direction() PROJ_PURE_DECL;
    PROJ_DLL const common::UnitOfMeasure &unit() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<double> &minimumValue() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<double> &maximumValue() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<RangeMeaning> &rangeMeaning() PROJ_PURE_DECL;
    PROJ_DLL const MeridianPtr &meridian() PROJ_PURE_DECL;

    // Non-standard
    PROJ_DLL static CoordinateSystemAxisNNPtr
    create(const util::PropertyMap &properties,
           const std::string &abbreviationIn, const AxisDirection &directionIn,
           const common::UnitOfMeasure &unitIn,
           const MeridianPtr &meridianIn = nullptr);

    PROJ_DLL static CoordinateSystemAxisNNPtr
    create(const util::PropertyMap &properties,
           const std::string &abbreviationIn, const AxisDirection &directionIn,
           const common::UnitOfMeasure &unitIn,
           const util::optional<double> &minimumValueIn,
           const util::optional<double> &maximumValueIn,
           const util::optional<RangeMeaning> &rangeMeaningIn,
           const MeridianPtr &meridianIn = nullptr);

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL bool
        _isEquivalentTo(
            const util::IComparable *other,
            util::IComparable::Criterion criterion =
                util::IComparable::Criterion::STRICT,
            const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter, int order,
                                    bool disableAbbrev) const;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL static std::string normalizeAxisName(const std::string &str);

    PROJ_INTERNAL static CoordinateSystemAxisNNPtr
    createLAT_NORTH(const common::UnitOfMeasure &unit);
    PROJ_INTERNAL static CoordinateSystemAxisNNPtr
    createLONG_EAST(const common::UnitOfMeasure &unit);

    PROJ_INTERNAL CoordinateSystemAxisNNPtr
    alterUnit(const common::UnitOfMeasure &newUnit) const;

    //! @endcond

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    CoordinateSystemAxis(const CoordinateSystemAxis &other) = delete;
    CoordinateSystemAxis &operator=(const CoordinateSystemAxis &other) = delete;

    PROJ_INTERNAL CoordinateSystemAxis();
    /* cppcheck-suppress unusedPrivateFunction */
    INLINED_MAKE_SHARED
};

// ---------------------------------------------------------------------------

/** \brief Abstract class modelling a coordinate system (CS)
 *
 * A CS is the non-repeating sequence of coordinate system axes that spans a
 * given coordinate space. A CS is derived from a set of mathematical rules for
 * specifying how coordinates in a given space are to be assigned to points.
 * The coordinate values in a coordinate tuple shall be recorded in the order
 * in which the coordinate system axes associations are recorded.
 *
 * \remark Implements CoordinateSystem from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CoordinateSystem : public common::IdentifiedObject,
                                      public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CoordinateSystem() override;
    //! @endcond

    PROJ_DLL const std::vector<CoordinateSystemAxisNNPtr> &
    axisList() PROJ_PURE_DECL;

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        _exportToWKT(io::WKTFormatter *formatter)
            const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL virtual std::string getWKT2Type(bool) const = 0;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL explicit CoordinateSystem(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    CoordinateSystem(const CoordinateSystem &other) = delete;
    CoordinateSystem &operator=(const CoordinateSystem &other) = delete;
};

/** Shared pointer of CoordinateSystem. */
using CoordinateSystemPtr = std::shared_ptr<CoordinateSystem>;
/** Non-null shared pointer of CoordinateSystem. */
using CoordinateSystemNNPtr = util::nn<CoordinateSystemPtr>;

// ---------------------------------------------------------------------------

class SphericalCS;
/** Shared pointer of SphericalCS. */
using SphericalCSPtr = std::shared_ptr<SphericalCS>;
/** Non-null shared pointer of SphericalCS. */
using SphericalCSNNPtr = util::nn<SphericalCSPtr>;

/** \brief A three-dimensional coordinate system in Euclidean space with one
 * distance measured from the origin and two angular coordinates.
 *
 * Not to be confused with an ellipsoidal coordinate system based on an
 * ellipsoid "degenerated" into a sphere. A SphericalCS shall have three
 * axis associations.
 *
 * \remark Implements SphericalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL SphericalCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~SphericalCS() override;
    //! @endcond

    // non-standard

    PROJ_DLL static SphericalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2,
           const CoordinateSystemAxisNNPtr &axis3);

    PROJ_DLL static SphericalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "spherical";

  protected:
    PROJ_INTERNAL explicit SphericalCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    SphericalCS(const SphericalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class EllipsoidalCS;
/** Shared pointer of EllipsoidalCS. */
using EllipsoidalCSPtr = std::shared_ptr<EllipsoidalCS>;
/** Non-null shared pointer of EllipsoidalCS. */
using EllipsoidalCSNNPtr = util::nn<EllipsoidalCSPtr>;

/** \brief A two- or three-dimensional coordinate system in which position is
 * specified by geodetic latitude, geodetic longitude, and (in the
 * three-dimensional case) ellipsoidal height.
 *
 * An EllipsoidalCS shall have two or three associations.
 *
 * \remark Implements EllipsoidalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL EllipsoidalCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~EllipsoidalCS() override;
    //! @endcond

    // non-standard
    PROJ_DLL static EllipsoidalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2);

    PROJ_DLL static EllipsoidalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2,
           const CoordinateSystemAxisNNPtr &axis3);

    PROJ_DLL static EllipsoidalCSNNPtr
    createLatitudeLongitude(const common::UnitOfMeasure &unit);

    PROJ_DLL static EllipsoidalCSNNPtr createLatitudeLongitudeEllipsoidalHeight(
        const common::UnitOfMeasure &angularUnit,
        const common::UnitOfMeasure &linearUnit);

    PROJ_DLL static EllipsoidalCSNNPtr
    createLongitudeLatitude(const common::UnitOfMeasure &unit);

    PROJ_DLL static EllipsoidalCSNNPtr createLongitudeLatitudeEllipsoidalHeight(
        const common::UnitOfMeasure &angularUnit,
        const common::UnitOfMeasure &linearUnit);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "ellipsoidal";

    //! @cond Doxygen_Suppress

    /** \brief Typical axis order. */
    enum class AxisOrder {
        /** Latitude(North), Longitude(East) */
        LAT_NORTH_LONG_EAST,
        /** Latitude(North), Longitude(East), Height(up) */
        LAT_NORTH_LONG_EAST_HEIGHT_UP,
        /** Longitude(East), Latitude(North) */
        LONG_EAST_LAT_NORTH,
        /** Longitude(East), Latitude(North), Height(up) */
        LONG_EAST_LAT_NORTH_HEIGHT_UP,
        /** Other axis order. */
        OTHER
    };

    PROJ_INTERNAL AxisOrder axisOrder() const;

    PROJ_INTERNAL EllipsoidalCSNNPtr
    alterAngularUnit(const common::UnitOfMeasure &angularUnit) const;

    PROJ_INTERNAL EllipsoidalCSNNPtr
    alterLinearUnit(const common::UnitOfMeasure &linearUnit) const;

    //! @endcond

  protected:
    PROJ_INTERNAL explicit EllipsoidalCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  protected:
    EllipsoidalCS(const EllipsoidalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class VerticalCS;
/** Shared pointer of VerticalCS. */
using VerticalCSPtr = std::shared_ptr<VerticalCS>;
/** Non-null shared pointer of VerticalCS. */
using VerticalCSNNPtr = util::nn<VerticalCSPtr>;

/** \brief A one-dimensional coordinate system used to record the heights or
 * depths of points.
 *
 * Such a coordinate system is usually dependent on the Earth's gravity field.
 * A VerticalCS shall have one axis association.
 *
 * \remark Implements VerticalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL VerticalCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~VerticalCS() override;
    //! @endcond

    PROJ_DLL static VerticalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis);

    PROJ_DLL static VerticalCSNNPtr
    createGravityRelatedHeight(const common::UnitOfMeasure &unit);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "vertical";

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL VerticalCSNNPtr
        alterUnit(const common::UnitOfMeasure &unit) const;

    //! @endcond

  protected:
    PROJ_INTERNAL explicit VerticalCS(const CoordinateSystemAxisNNPtr &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    VerticalCS(const VerticalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class CartesianCS;
/** Shared pointer of CartesianCS. */
using CartesianCSPtr = std::shared_ptr<CartesianCS>;
/** Non-null shared pointer of CartesianCS. */
using CartesianCSNNPtr = util::nn<CartesianCSPtr>;

/** \brief A two- or three-dimensional coordinate system in Euclidean space
 * with orthogonal straight axes.
 *
 * All axes shall have the same length unit. A CartesianCS shall have two or
 * three axis associations; the number of associations shall equal the
 * dimension of the CS.
 *
 * \remark Implements CartesianCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CartesianCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CartesianCS() override;
    //! @endcond

    PROJ_DLL static CartesianCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2);
    PROJ_DLL static CartesianCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2,
           const CoordinateSystemAxisNNPtr &axis3);

    PROJ_DLL static CartesianCSNNPtr
    createEastingNorthing(const common::UnitOfMeasure &unit);

    PROJ_DLL static CartesianCSNNPtr
    createNorthingEasting(const common::UnitOfMeasure &unit);

    PROJ_DLL static CartesianCSNNPtr
    createNorthPoleEastingSouthNorthingSouth(const common::UnitOfMeasure &unit);

    PROJ_DLL static CartesianCSNNPtr
    createSouthPoleEastingNorthNorthingNorth(const common::UnitOfMeasure &unit);

    PROJ_DLL static CartesianCSNNPtr
    createWestingSouthing(const common::UnitOfMeasure &unit);

    PROJ_DLL static CartesianCSNNPtr
    createGeocentric(const common::UnitOfMeasure &unit);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE =
        "Cartesian"; // uppercase is intended

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL CartesianCSNNPtr
        alterUnit(const common::UnitOfMeasure &unit) const;

    //! @endcond

  protected:
    PROJ_INTERNAL explicit CartesianCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    CartesianCS(const CartesianCS &other) = delete;
};

// ---------------------------------------------------------------------------

class AffineCS;
/** Shared pointer of AffineCS. */
using AffineCSPtr = std::shared_ptr<AffineCS>;
/** Non-null shared pointer of AffineCS. */
using AffineCSNNPtr = util::nn<AffineCSPtr>;

/** \brief A two- or three-dimensional coordinate system in Euclidean space
 * with straight axes that are not necessarily orthogonal.
 *
 * \remark Implements AffineCS from \ref ISO_19111_2019
 * \since 9.2
 */
class PROJ_GCC_DLL AffineCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~AffineCS() override;
    //! @endcond

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "affine";

    PROJ_DLL static AffineCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2);
    PROJ_DLL static AffineCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis1,
           const CoordinateSystemAxisNNPtr &axis2,
           const CoordinateSystemAxisNNPtr &axis3);

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL AffineCSNNPtr
        alterUnit(const common::UnitOfMeasure &unit) const;

    //! @endcond

  protected:
    PROJ_INTERNAL explicit AffineCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    AffineCS(const AffineCS &other) = delete;
};

// ---------------------------------------------------------------------------

class OrdinalCS;
/** Shared pointer of OrdinalCS. */
using OrdinalCSPtr = std::shared_ptr<OrdinalCS>;
/** Non-null shared pointer of OrdinalCS. */
using OrdinalCSNNPtr = util::nn<OrdinalCSPtr>;

/** \brief n-dimensional coordinate system in which every axis uses integers.
 *
 * The number of associations shall equal the
 * dimension of the CS.
 *
 * \remark Implements OrdinalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL OrdinalCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~OrdinalCS() override;
    //! @endcond

    PROJ_DLL static OrdinalCSNNPtr
    create(const util::PropertyMap &properties,
           const std::vector<CoordinateSystemAxisNNPtr> &axisIn);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "ordinal";

  protected:
    PROJ_INTERNAL explicit OrdinalCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    OrdinalCS(const OrdinalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class ParametricCS;
/** Shared pointer of ParametricCS. */
using ParametricCSPtr = std::shared_ptr<ParametricCS>;
/** Non-null shared pointer of ParametricCS. */
using ParametricCSNNPtr = util::nn<ParametricCSPtr>;

/** \brief one-dimensional coordinate reference system which uses parameter
 * values or functions that may vary monotonically with height.
 *
 * \remark Implements ParametricCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ParametricCS final : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ParametricCS() override;
    //! @endcond

    PROJ_DLL static ParametricCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axisIn);

    /** Value of getWKT2Type() */
    static constexpr const char *WKT2_TYPE = "parametric";

  protected:
    PROJ_INTERNAL explicit ParametricCS(
        const std::vector<CoordinateSystemAxisNNPtr> &axisIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool) const override {
        return WKT2_TYPE;
    }

  private:
    ParametricCS(const ParametricCS &other) = delete;
};

// ---------------------------------------------------------------------------

class TemporalCS;
/** Shared pointer of TemporalCS. */
using TemporalCSPtr = std::shared_ptr<TemporalCS>;
/** Non-null shared pointer of TemporalCS. */
using TemporalCSNNPtr = util::nn<TemporalCSPtr>;

/** \brief (Abstract class) A one-dimensional coordinate system used to record
 * time.
 *
 * A TemporalCS shall have one axis association.
 *
 * \remark Implements TemporalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL TemporalCS : public CoordinateSystem {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalCS() override;
    //! @endcond

    /** WKT2:2015 type */
    static constexpr const char *WKT2_2015_TYPE = "temporal";

  protected:
    PROJ_INTERNAL explicit TemporalCS(const CoordinateSystemAxisNNPtr &axis);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string
    getWKT2Type(bool use2019Keywords) const override = 0;

  private:
    TemporalCS(const TemporalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class DateTimeTemporalCS;
/** Shared pointer of DateTimeTemporalCS. */
using DateTimeTemporalCSPtr = std::shared_ptr<DateTimeTemporalCS>;
/** Non-null shared pointer of DateTimeTemporalCS. */
using DateTimeTemporalCSNNPtr = util::nn<DateTimeTemporalCSPtr>;

/** \brief A one-dimensional coordinate system used to record time in dateTime
 * representation as defined in ISO 8601.
 *
 * A DateTimeTemporalCS shall have one axis association. It does not use
 * axisUnitID; the temporal quantities are defined through the ISO 8601
 * representation.
 *
 * \remark Implements DateTimeTemporalCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DateTimeTemporalCS final : public TemporalCS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DateTimeTemporalCS() override;
    //! @endcond

    PROJ_DLL static DateTimeTemporalCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis);

    /** WKT2:2019 type */
    static constexpr const char *WKT2_2019_TYPE = "TemporalDateTime";

  protected:
    PROJ_INTERNAL explicit DateTimeTemporalCS(
        const CoordinateSystemAxisNNPtr &axis);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool use2019Keywords) const override;

  private:
    DateTimeTemporalCS(const DateTimeTemporalCS &other) = delete;
};

// ---------------------------------------------------------------------------

class TemporalCountCS;
/** Shared pointer of TemporalCountCS. */
using TemporalCountCSPtr = std::shared_ptr<TemporalCountCS>;
/** Non-null shared pointer of TemporalCountCS. */
using TemporalCountCSNNPtr = util::nn<TemporalCountCSPtr>;

/** \brief A one-dimensional coordinate system used to record time as an
 * integer count.
 *
 * A TemporalCountCS shall have one axis association.
 *
 * \remark Implements TemporalCountCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL TemporalCountCS final : public TemporalCS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalCountCS() override;
    //! @endcond

    PROJ_DLL static TemporalCountCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis);

    /** WKT2:2019 type */
    static constexpr const char *WKT2_2019_TYPE = "TemporalCount";

  protected:
    PROJ_INTERNAL explicit TemporalCountCS(
        const CoordinateSystemAxisNNPtr &axis);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool use2019Keywords) const override;

  private:
    TemporalCountCS(const TemporalCountCS &other) = delete;
};

// ---------------------------------------------------------------------------

class TemporalMeasureCS;
/** Shared pointer of TemporalMeasureCS. */
using TemporalMeasureCSPtr = std::shared_ptr<TemporalMeasureCS>;
/** Non-null shared pointer of TemporalMeasureCS. */
using TemporalMeasureCSNNPtr = util::nn<TemporalMeasureCSPtr>;

/** \brief A one-dimensional coordinate system used to record a time as a
 * real number.
 *
 * A TemporalMeasureCS shall have one axis association.
 *
 * \remark Implements TemporalMeasureCS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL TemporalMeasureCS final : public TemporalCS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalMeasureCS() override;
    //! @endcond

    PROJ_DLL static TemporalMeasureCSNNPtr
    create(const util::PropertyMap &properties,
           const CoordinateSystemAxisNNPtr &axis);

    /** WKT2:2019 type */
    static constexpr const char *WKT2_2019_TYPE = "TemporalMeasure";

  protected:
    PROJ_INTERNAL explicit TemporalMeasureCS(
        const CoordinateSystemAxisNNPtr &axis);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL std::string getWKT2Type(bool use2019Keywords) const override;

  private:
    TemporalMeasureCS(const TemporalMeasureCS &other) = delete;
};

} // namespace cs

NS_PROJ_END

#endif //  CS_HH_INCLUDED
