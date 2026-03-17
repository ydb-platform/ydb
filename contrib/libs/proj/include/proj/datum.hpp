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

#ifndef DATUM_HH_INCLUDED
#define DATUM_HH_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "io.hpp"
#include "util.hpp"

NS_PROJ_START

/** osgeo.proj.datum namespace

    \brief Datum (the relationship of a coordinate system to the body).
 */
namespace datum {

// ---------------------------------------------------------------------------

/** \brief Abstract class of the relationship of a coordinate system to an
 * object, thus creating a coordinate reference system.
 *
 * For geodetic and vertical coordinate reference systems, it relates a
 * coordinate system to the Earth (or the celestial body considered). With
 * other types of coordinate reference systems, the datum may relate the
 * coordinate system to another physical or
 * virtual object. A datum uses a parameter or set of parameters that determine
 * the location of the origin of the coordinate reference system. Each datum
 * subtype can be associated with only specific types of coordinate reference
 * systems.
 *
 * \remark Implements Datum from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL Datum : public common::ObjectUsage,
                           public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~Datum() override;
    //! @endcond

    PROJ_DLL const util::optional<std::string> &anchorDefinition() const;
    PROJ_DLL const util::optional<common::Measure> &anchorEpoch() const;
    PROJ_DLL const util::optional<common::DateTime> &publicationDate() const;
    PROJ_DLL const common::IdentifiedObjectPtr &conventionalRS() const;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL Datum();

#ifdef DOXYGEN_ENABLED
    std::string *anchorDefinition_;
    Date *publicationDate_;
    common::IdentifiedObject *conventionalRS_;
#endif

  protected:
    PROJ_INTERNAL void setAnchor(const util::optional<std::string> &anchor);
    PROJ_INTERNAL void
    setAnchorEpoch(const util::optional<common::Measure> &anchorEpoch);

    PROJ_INTERNAL void
    setProperties(const util::PropertyMap
                      &properties); // throw(InvalidValueTypeException)

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Datum &operator=(const Datum &other) = delete;
    Datum(const Datum &other) = delete;
};

/** Shared pointer of Datum */
using DatumPtr = std::shared_ptr<Datum>;
/** Non-null shared pointer of Datum */
using DatumNNPtr = util::nn<DatumPtr>;

// ---------------------------------------------------------------------------

class DatumEnsemble;
/** Shared pointer of DatumEnsemble */
using DatumEnsemblePtr = std::shared_ptr<DatumEnsemble>;
/** Non-null shared pointer of DatumEnsemble */
using DatumEnsembleNNPtr = util::nn<DatumEnsemblePtr>;

/** \brief A collection of two or more geodetic or vertical reference frames
 * (or if not geodetic or vertical reference frame, a collection of two or more
 * datums) which for all but the highest accuracy requirements may be
 * considered to be insignificantly different from each other.
 *
 * Every frame within the datum ensemble must be a realizations of the same
 * Terrestrial Reference System or Vertical Reference System.
 *
 * \remark Implements DatumEnsemble from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DatumEnsemble final : public common::ObjectUsage,
                                         public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DatumEnsemble() override;
    //! @endcond

    PROJ_DLL const std::vector<DatumNNPtr> &datums() const;
    PROJ_DLL const metadata::PositionalAccuracyNNPtr &
    positionalAccuracy() const;

    PROJ_DLL static DatumEnsembleNNPtr create(
        const util::PropertyMap &properties,
        const std::vector<DatumNNPtr> &datumsIn,
        const metadata::PositionalAccuracyNNPtr &accuracy); // throw(Exception)

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_FOR_TEST DatumNNPtr
    asDatum(const io::DatabaseContextPtr &dbContext) const;
    //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    Datum datums_[];
    PositionalAccuracy positionalAccuracy_;
#endif

    PROJ_INTERNAL
    DatumEnsemble(const std::vector<DatumNNPtr> &datumsIn,
                  const metadata::PositionalAccuracyNNPtr &accuracy);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA

    DatumEnsemble(const DatumEnsemble &other) = delete;
    DatumEnsemble &operator=(const DatumEnsemble &other) = delete;
};

// ---------------------------------------------------------------------------

class PrimeMeridian;
/** Shared pointer of PrimeMeridian */
using PrimeMeridianPtr = std::shared_ptr<PrimeMeridian>;
/** Non-null shared pointer of PrimeMeridian */
using PrimeMeridianNNPtr = util::nn<PrimeMeridianPtr>;

/** \brief The origin meridian from which longitude values are determined.
 *
 * \note The default value for prime meridian name is "Greenwich". When the
 * default applies, the value for the longitude shall be 0 (degrees).
 *
 * \remark Implements PrimeMeridian from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL PrimeMeridian final : public common::IdentifiedObject,
                                         public io::IPROJStringExportable,
                                         public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~PrimeMeridian() override;
    //! @endcond

    PROJ_DLL const common::Angle &longitude() PROJ_PURE_DECL;

    // non-standard
    PROJ_DLL static PrimeMeridianNNPtr
    create(const util::PropertyMap &properties,
           const common::Angle &longitudeIn);

    PROJ_DLL static const PrimeMeridianNNPtr GREENWICH;
    PROJ_DLL static const PrimeMeridianNNPtr REFERENCE_MERIDIAN;
    PROJ_DLL static const PrimeMeridianNNPtr PARIS;

    PROJ_PRIVATE :

        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        _exportToPROJString(io::PROJStringFormatter *formatter)
            const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL static std::string
    getPROJStringWellKnownName(const common::Angle &angle);
    //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    Angle greenwichLongitude_;
#endif

    PROJ_INTERNAL explicit PrimeMeridian(
        const common::Angle &angle = common::Angle());
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    PrimeMeridian(const PrimeMeridian &other) = delete;
    PrimeMeridian &operator=(const PrimeMeridian &other) = delete;

    PROJ_INTERNAL static const PrimeMeridianNNPtr createGREENWICH();
    PROJ_INTERNAL static const PrimeMeridianNNPtr createREFERENCE_MERIDIAN();
    PROJ_INTERNAL static const PrimeMeridianNNPtr createPARIS();
};

// ---------------------------------------------------------------------------

class Ellipsoid;
/** Shared pointer of Ellipsoid */
using EllipsoidPtr = std::shared_ptr<Ellipsoid>;
/** Non-null shared pointer of Ellipsoid */
using EllipsoidNNPtr = util::nn<EllipsoidPtr>;

/** \brief A geometric figure that can be used to describe the approximate
 * shape of an object.
 *
 * For the Earth an oblate biaxial ellipsoid is used: in mathematical terms,
 * it is a surface formed by the rotation of an ellipse about its minor axis.
 *
 * \remark Implements Ellipsoid from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL Ellipsoid final : public common::IdentifiedObject,
                                     public io::IPROJStringExportable,
                                     public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~Ellipsoid() override;
    //! @endcond

    PROJ_DLL const common::Length &semiMajorAxis() PROJ_PURE_DECL;

    // Inlined from SecondDefiningParameter union
    PROJ_DLL const util::optional<common::Scale> &
    inverseFlattening() PROJ_PURE_DECL;
    PROJ_DLL const util::optional<common::Length> &
    semiMinorAxis() PROJ_PURE_DECL;
    PROJ_DLL bool isSphere() PROJ_PURE_DECL;

    PROJ_DLL const util::optional<common::Length> &
    semiMedianAxis() PROJ_PURE_DECL;

    // non-standard

    PROJ_DLL double computedInverseFlattening() PROJ_PURE_DECL;
    PROJ_DLL double squaredEccentricity() PROJ_PURE_DECL;
    PROJ_DLL common::Length computeSemiMinorAxis() const;

    PROJ_DLL const std::string &celestialBody() PROJ_PURE_DECL;

    PROJ_DLL static const std::string EARTH;

    PROJ_DLL static EllipsoidNNPtr
    createSphere(const util::PropertyMap &properties,
                 const common::Length &radius,
                 const std::string &celestialBody = EARTH);

    PROJ_DLL static EllipsoidNNPtr
    createFlattenedSphere(const util::PropertyMap &properties,
                          const common::Length &semiMajorAxisIn,
                          const common::Scale &invFlattening,
                          const std::string &celestialBody = EARTH);

    PROJ_DLL static EllipsoidNNPtr
    createTwoAxis(const util::PropertyMap &properties,
                  const common::Length &semiMajorAxisIn,
                  const common::Length &semiMinorAxisIn,
                  const std::string &celestialBody = EARTH);

    PROJ_DLL EllipsoidNNPtr identify() const;

    PROJ_DLL static const EllipsoidNNPtr CLARKE_1866;
    PROJ_DLL static const EllipsoidNNPtr WGS84;
    PROJ_DLL static const EllipsoidNNPtr GRS1980;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        _exportToWKT(io::WKTFormatter *formatter)
            const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)
                        //! @endcond

    PROJ_INTERNAL static std::string
    guessBodyName(const io::DatabaseContextPtr &dbContext, double a,
                  const std::string &ellpsName = std::string());

    PROJ_INTERNAL bool lookForProjWellKnownEllps(std::string &projEllpsName,
                                                 std::string &ellpsName) const;

  protected:
#ifdef DOXYGEN_ENABLED
    common::Length semiMajorAxis_;
    common::Scale *inverseFlattening_;
    common::Length *semiMinorAxis_;
    bool isSphere_;
    common::Length *semiMedianAxis_;
#endif

    PROJ_INTERNAL explicit Ellipsoid(const common::Length &radius,
                                     const std::string &celestialBody);

    PROJ_INTERNAL Ellipsoid(const common::Length &semiMajorAxisIn,
                            const common::Scale &invFlattening,
                            const std::string &celestialBody);

    PROJ_INTERNAL Ellipsoid(const common::Length &semiMajorAxisIn,
                            const common::Length &semiMinorAxisIn,
                            const std::string &celestialBody);

    PROJ_INTERNAL Ellipsoid(const Ellipsoid &other);

    INLINED_MAKE_SHARED

    PROJ_INTERNAL static const EllipsoidNNPtr createCLARKE_1866();
    PROJ_INTERNAL static const EllipsoidNNPtr createWGS84();
    PROJ_INTERNAL static const EllipsoidNNPtr createGRS1980();

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    Ellipsoid &operator=(const Ellipsoid &other) = delete;
};

// ---------------------------------------------------------------------------

class GeodeticReferenceFrame;
/** Shared pointer of GeodeticReferenceFrame */
using GeodeticReferenceFramePtr = std::shared_ptr<GeodeticReferenceFrame>;
/** Non-null shared pointer of GeodeticReferenceFrame */
using GeodeticReferenceFrameNNPtr = util::nn<GeodeticReferenceFramePtr>;

/** \brief The definition of the position, scale and orientation of a geocentric
 * Cartesian 3D coordinate system relative to the Earth.
 *
 * It may also identify a defined ellipsoid (or sphere) that approximates
 * the shape of the Earth and which is centred on and aligned to this
 * geocentric coordinate system. Older geodetic datums define the location and
 * orientation of a defined ellipsoid (or sphere) that approximates the shape
 * of the earth.
 *
 * \note The terminology "Datum" is often used to mean a GeodeticReferenceFrame.
 *
 * \note In \ref ISO_19111_2007, this class was called GeodeticDatum.
 *
 * \remark Implements GeodeticReferenceFrame from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL GeodeticReferenceFrame : public Datum {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeodeticReferenceFrame() override;
    //! @endcond

    PROJ_DLL const PrimeMeridianNNPtr &primeMeridian() PROJ_PURE_DECL;

    // We constraint more than the standard into which the ellipsoid might
    // be omitted for a CRS with a non-ellipsoidal CS
    PROJ_DLL const EllipsoidNNPtr &ellipsoid() PROJ_PURE_DECL;

    // non-standard
    PROJ_DLL static GeodeticReferenceFrameNNPtr
    create(const util::PropertyMap &properties, const EllipsoidNNPtr &ellipsoid,
           const util::optional<std::string> &anchor,
           const PrimeMeridianNNPtr &primeMeridian);

    PROJ_DLL static GeodeticReferenceFrameNNPtr
    create(const util::PropertyMap &properties, const EllipsoidNNPtr &ellipsoid,
           const util::optional<std::string> &anchor,
           const util::optional<common::Measure> &anchorEpoch,
           const PrimeMeridianNNPtr &primeMeridian);

    PROJ_DLL static const GeodeticReferenceFrameNNPtr
        EPSG_6267; // North American Datum 1927
    PROJ_DLL static const GeodeticReferenceFrameNNPtr
        EPSG_6269; // North American Datum 1983
    PROJ_DLL static const GeodeticReferenceFrameNNPtr EPSG_6326; // WGS 84

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL bool isEquivalentToNoExactTypeCheck(
        const util::IComparable *other, util::IComparable::Criterion criterion,
        const io::DatabaseContextPtr &dbContext) const;
    //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    PrimeMeridian primeMeridian_;
    Ellipsoid *ellipsoid_;
#endif

    PROJ_INTERNAL
    GeodeticReferenceFrame(const EllipsoidNNPtr &ellipsoidIn,
                           const PrimeMeridianNNPtr &primeMeridianIn);
    INLINED_MAKE_SHARED

    PROJ_INTERNAL static const GeodeticReferenceFrameNNPtr createEPSG_6267();
    PROJ_INTERNAL static const GeodeticReferenceFrameNNPtr createEPSG_6269();
    PROJ_INTERNAL static const GeodeticReferenceFrameNNPtr createEPSG_6326();

    bool hasEquivalentNameToUsingAlias(
        const IdentifiedObject *other,
        const io::DatabaseContextPtr &dbContext) const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    GeodeticReferenceFrame(const GeodeticReferenceFrame &other) = delete;
    GeodeticReferenceFrame &
    operator=(const GeodeticReferenceFrame &other) = delete;
};

// ---------------------------------------------------------------------------

class DynamicGeodeticReferenceFrame;
/** Shared pointer of DynamicGeodeticReferenceFrame */
using DynamicGeodeticReferenceFramePtr =
    std::shared_ptr<DynamicGeodeticReferenceFrame>;
/** Non-null shared pointer of DynamicGeodeticReferenceFrame */
using DynamicGeodeticReferenceFrameNNPtr =
    util::nn<DynamicGeodeticReferenceFramePtr>;

/** \brief A geodetic reference frame in which some of the parameters describe
 * time evolution of defining station coordinates.
 *
 * For example defining station coordinates having linear velocities to account
 * for crustal motion.
 *
 * \remark Implements DynamicGeodeticReferenceFrame from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DynamicGeodeticReferenceFrame final
    : public GeodeticReferenceFrame {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DynamicGeodeticReferenceFrame() override;
    //! @endcond

    PROJ_DLL const common::Measure &frameReferenceEpoch() const;
    PROJ_DLL const util::optional<std::string> &deformationModelName() const;

    // non-standard
    PROJ_DLL static DynamicGeodeticReferenceFrameNNPtr
    create(const util::PropertyMap &properties, const EllipsoidNNPtr &ellipsoid,
           const util::optional<std::string> &anchor,
           const PrimeMeridianNNPtr &primeMeridian,
           const common::Measure &frameReferenceEpochIn,
           const util::optional<std::string> &deformationModelNameIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
                        //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    Measure frameReferenceEpoch_;
#endif

    PROJ_INTERNAL DynamicGeodeticReferenceFrame(
        const EllipsoidNNPtr &ellipsoidIn,
        const PrimeMeridianNNPtr &primeMeridianIn,
        const common::Measure &frameReferenceEpochIn,
        const util::optional<std::string> &deformationModelNameIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DynamicGeodeticReferenceFrame(const DynamicGeodeticReferenceFrame &other) =
        delete;
    DynamicGeodeticReferenceFrame &
    operator=(const DynamicGeodeticReferenceFrame &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief The specification of the method by which the vertical reference frame
 * is realized.
 *
 * \remark Implements RealizationMethod from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL RealizationMethod : public util::CodeList {
  public:
    PROJ_DLL static const RealizationMethod LEVELLING;
    PROJ_DLL static const RealizationMethod GEOID;
    PROJ_DLL static const RealizationMethod TIDAL;

  private:
    PROJ_FRIEND_OPTIONAL(RealizationMethod);
    PROJ_DLL explicit RealizationMethod(
        const std::string &nameIn = std::string());
    PROJ_DLL RealizationMethod(const RealizationMethod &other) = default;
    PROJ_DLL RealizationMethod &operator=(const RealizationMethod &other);
};

// ---------------------------------------------------------------------------

class VerticalReferenceFrame;
/** Shared pointer of VerticalReferenceFrame */
using VerticalReferenceFramePtr = std::shared_ptr<VerticalReferenceFrame>;
/** Non-null shared pointer of VerticalReferenceFrame */
using VerticalReferenceFrameNNPtr = util::nn<VerticalReferenceFramePtr>;

/** \brief A textual description and/or a set of parameters identifying a
 * particular reference level surface used as a zero-height or zero-depth
 * surface, including its position with respect to the Earth.
 *
 * \note In \ref ISO_19111_2007, this class was called VerticalDatum.

 * \remark Implements VerticalReferenceFrame from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL VerticalReferenceFrame : public Datum {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~VerticalReferenceFrame() override;
    //! @endcond

    PROJ_DLL const util::optional<RealizationMethod> &realizationMethod() const;

    // non-standard
    PROJ_DLL static VerticalReferenceFrameNNPtr
    create(const util::PropertyMap &properties,
           const util::optional<std::string> &anchor =
               util::optional<std::string>(),
           const util::optional<RealizationMethod> &realizationMethodIn =
               util::optional<RealizationMethod>());

    PROJ_DLL static VerticalReferenceFrameNNPtr
    create(const util::PropertyMap &properties,
           const util::optional<std::string> &anchor,
           const util::optional<common::Measure> &anchorEpoch,
           const util::optional<RealizationMethod> &realizationMethodIn =
               util::optional<RealizationMethod>());

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL bool isEquivalentToNoExactTypeCheck(
        const util::IComparable *other, util::IComparable::Criterion criterion,
        const io::DatabaseContextPtr &dbContext) const;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL const std::string &getWKT1DatumType() const;

    //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    RealizationMethod realizationMethod_;
#endif

    PROJ_INTERNAL explicit VerticalReferenceFrame(
        const util::optional<RealizationMethod> &realizationMethodIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class DynamicVerticalReferenceFrame;
/** Shared pointer of DynamicVerticalReferenceFrame */
using DynamicVerticalReferenceFramePtr =
    std::shared_ptr<DynamicVerticalReferenceFrame>;
/** Non-null shared pointer of DynamicVerticalReferenceFrame */
using DynamicVerticalReferenceFrameNNPtr =
    util::nn<DynamicVerticalReferenceFramePtr>;

/** \brief A vertical reference frame in which some of the defining parameters
 * have time dependency.
 *
 * For example defining station heights have velocity to account for
 * post-glacial isostatic rebound motion.
 *
 * \remark Implements DynamicVerticalReferenceFrame from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DynamicVerticalReferenceFrame final
    : public VerticalReferenceFrame {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DynamicVerticalReferenceFrame() override;
    //! @endcond

    PROJ_DLL const common::Measure &frameReferenceEpoch() const;
    PROJ_DLL const util::optional<std::string> &deformationModelName() const;

    // non-standard
    PROJ_DLL static DynamicVerticalReferenceFrameNNPtr
    create(const util::PropertyMap &properties,
           const util::optional<std::string> &anchor,
           const util::optional<RealizationMethod> &realizationMethodIn,
           const common::Measure &frameReferenceEpochIn,
           const util::optional<std::string> &deformationModelNameIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
                        //! @endcond

  protected:
#ifdef DOXYGEN_ENABLED
    Measure frameReferenceEpoch_;
#endif

    PROJ_INTERNAL DynamicVerticalReferenceFrame(
        const util::optional<RealizationMethod> &realizationMethodIn,
        const common::Measure &frameReferenceEpochIn,
        const util::optional<std::string> &deformationModelNameIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DynamicVerticalReferenceFrame(const DynamicVerticalReferenceFrame &other) =
        delete;
    DynamicVerticalReferenceFrame &
    operator=(const DynamicVerticalReferenceFrame &other) = delete;
};

// ---------------------------------------------------------------------------

class TemporalDatum;
/** Shared pointer of TemporalDatum */
using TemporalDatumPtr = std::shared_ptr<TemporalDatum>;
/** Non-null shared pointer of TemporalDatum */
using TemporalDatumNNPtr = util::nn<TemporalDatumPtr>;

/** \brief The definition of the relationship of a temporal coordinate system
 * to an object. The object is normally time on the Earth.
 *
 * \remark Implements TemporalDatum from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL TemporalDatum final : public Datum {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalDatum() override;
    //! @endcond

    PROJ_DLL const common::DateTime &temporalOrigin() const;
    PROJ_DLL const std::string &calendar() const;

    PROJ_DLL static const std::string CALENDAR_PROLEPTIC_GREGORIAN;

    // non-standard
    PROJ_DLL static TemporalDatumNNPtr
    create(const util::PropertyMap &properties,
           const common::DateTime &temporalOriginIn,
           const std::string &calendarIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL TemporalDatum(const common::DateTime &temporalOriginIn,
                                const std::string &calendarIn);
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class EngineeringDatum;
/** Shared pointer of EngineeringDatum */
using EngineeringDatumPtr = std::shared_ptr<EngineeringDatum>;
/** Non-null shared pointer of EngineeringDatum */
using EngineeringDatumNNPtr = util::nn<EngineeringDatumPtr>;

/** \brief The definition of the origin and orientation of an engineering
 * coordinate reference system.
 *
 * \note The origin can be fixed with respect to the Earth (such as a defined
 * point at a construction site), or be a defined point on a moving vehicle
 * (such as on a ship or satellite), or a defined point of an image.
 *
 * \remark Implements EngineeringDatum from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL EngineeringDatum final : public Datum {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~EngineeringDatum() override;
    //! @endcond

    // non-standard
    PROJ_DLL static EngineeringDatumNNPtr
    create(const util::PropertyMap &properties,
           const util::optional<std::string> &anchor =
               util::optional<std::string>());

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL EngineeringDatum();
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

class ParametricDatum;
/** Shared pointer of ParametricDatum */
using ParametricDatumPtr = std::shared_ptr<ParametricDatum>;
/** Non-null shared pointer of ParametricDatum */
using ParametricDatumNNPtr = util::nn<ParametricDatumPtr>;

/** \brief Textual description and/or a set of parameters identifying a
 * particular reference surface used as the origin of a parametric coordinate
 * system, including its position with respect to the Earth.
 *
 * \remark Implements ParametricDatum from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ParametricDatum final : public Datum {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ParametricDatum() override;
    //! @endcond

    // non-standard
    PROJ_DLL static ParametricDatumNNPtr
    create(const util::PropertyMap &properties,
           const util::optional<std::string> &anchor =
               util::optional<std::string>());

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;
    //! @endcond

  protected:
    PROJ_INTERNAL ParametricDatum();
    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

} // namespace datum

NS_PROJ_END

#endif //  DATUM_HH_INCLUDED
