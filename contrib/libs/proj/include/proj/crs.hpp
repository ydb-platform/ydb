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

#ifndef CRS_HH_INCLUDED
#define CRS_HH_INCLUDED

#include <memory>
#include <string>
#include <vector>

#include "common.hpp"
#include "coordinateoperation.hpp"
#include "coordinatesystem.hpp"
#include "datum.hpp"
#include "io.hpp"
#include "util.hpp"

NS_PROJ_START

/** osgeo.proj.crs namespace

    \brief CRS (coordinate reference system = coordinate system with a datum).
*/
namespace crs {

// ---------------------------------------------------------------------------

class GeographicCRS;
/** Shared pointer of GeographicCRS */
using GeographicCRSPtr = std::shared_ptr<GeographicCRS>;
/** Non-null shared pointer of GeographicCRS */
using GeographicCRSNNPtr = util::nn<GeographicCRSPtr>;

class VerticalCRS;
/** Shared pointer of VerticalCRS */
using VerticalCRSPtr = std::shared_ptr<VerticalCRS>;
/** Non-null shared pointer of VerticalCRS */
using VerticalCRSNNPtr = util::nn<VerticalCRSPtr>;

class BoundCRS;
/** Shared pointer of BoundCRS */
using BoundCRSPtr = std::shared_ptr<BoundCRS>;
/** Non-null shared pointer of BoundCRS */
using BoundCRSNNPtr = util::nn<BoundCRSPtr>;

class CompoundCRS;
/** Shared pointer of CompoundCRS */
using CompoundCRSPtr = std::shared_ptr<CompoundCRS>;
/** Non-null shared pointer of CompoundCRS */
using CompoundCRSNNPtr = util::nn<CompoundCRSPtr>;

// ---------------------------------------------------------------------------

class CRS;
/** Shared pointer of CRS */
using CRSPtr = std::shared_ptr<CRS>;
/** Non-null shared pointer of CRS */
using CRSNNPtr = util::nn<CRSPtr>;

/** \brief Abstract class modelling a coordinate reference system which is
 * usually single but may be compound.
 *
 * \remark Implements CRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CRS : public common::ObjectUsage,
                         public io::IJSONExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CRS() override;
    //! @endcond

    // Non-standard

    PROJ_DLL bool isDynamic(bool considerWGS84AsDynamic = false) const;
    PROJ_DLL GeodeticCRSPtr extractGeodeticCRS() const;
    PROJ_DLL GeographicCRSPtr extractGeographicCRS() const;
    PROJ_DLL VerticalCRSPtr extractVerticalCRS() const;
    PROJ_DLL CRSNNPtr createBoundCRSToWGS84IfPossible(
        const io::DatabaseContextPtr &dbContext,
        operation::CoordinateOperationContext::IntermediateCRSUse
            allowIntermediateCRSUse) const;
    PROJ_DLL CRSNNPtr stripVerticalComponent() const;

    PROJ_DLL const BoundCRSPtr &canonicalBoundCRS() PROJ_PURE_DECL;

    PROJ_DLL std::list<std::pair<CRSNNPtr, int>>
    identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    PROJ_DLL std::list<CRSNNPtr>
    getNonDeprecated(const io::DatabaseContextNNPtr &dbContext) const;

    PROJ_DLL CRSNNPtr
    promoteTo3D(const std::string &newName,
                const io::DatabaseContextPtr &dbContext) const;

    PROJ_DLL CRSNNPtr demoteTo2D(const std::string &newName,
                                 const io::DatabaseContextPtr &dbContext) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL const GeodeticCRS *
        extractGeodeticCRSRaw() const;

    PROJ_FOR_TEST CRSNNPtr shallowClone() const;

    PROJ_FOR_TEST CRSNNPtr alterName(const std::string &newName) const;

    PROJ_FOR_TEST CRSNNPtr alterId(const std::string &authName,
                                   const std::string &code) const;

    PROJ_INTERNAL const std::string &getExtensionProj4() const noexcept;

    PROJ_FOR_TEST CRSNNPtr
    alterGeodeticCRS(const GeodeticCRSNNPtr &newGeodCRS) const;

    PROJ_FOR_TEST CRSNNPtr
    alterCSLinearUnit(const common::UnitOfMeasure &unit) const;

    PROJ_INTERNAL bool mustAxisOrderBeSwitchedForVisualization() const;

    PROJ_INTERNAL CRSNNPtr applyAxisOrderReversal(const char *nameSuffix) const;

    PROJ_FOR_TEST CRSNNPtr normalizeForVisualization() const;

    PROJ_INTERNAL CRSNNPtr allowNonConformantWKT1Export() const;

    PROJ_INTERNAL CRSNNPtr
    attachOriginalCompoundCRS(const CompoundCRSNNPtr &compoundCRS) const;

    PROJ_INTERNAL CRSNNPtr promoteTo3D(
        const std::string &newName, const io::DatabaseContextPtr &dbContext,
        const cs::CoordinateSystemAxisNNPtr &verticalAxisIfNotAlreadyPresent)
        const;

    PROJ_INTERNAL bool hasImplicitCS() const;

    PROJ_INTERNAL bool hasOver() const;

    PROJ_INTERNAL static CRSNNPtr
    getResolvedCRS(const CRSNNPtr &crs,
                   const io::AuthorityFactoryPtr &authFactory,
                   metadata::ExtentPtr &extentOut);

    PROJ_INTERNAL std::string getOriginatingAuthName() const;

    //! @endcond

  protected:
    PROJ_INTERNAL CRS();
    PROJ_INTERNAL CRS(const CRS &other);
    friend class BoundCRS;
    PROJ_INTERNAL void setCanonicalBoundCRS(const BoundCRSNNPtr &boundCRS);

    PROJ_INTERNAL virtual CRSNNPtr _shallowClone() const = 0;

    PROJ_INTERNAL virtual std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    PROJ_INTERNAL void
    setProperties(const util::PropertyMap
                      &properties); // throw(InvalidValueTypeException)

  private:
    PROJ_OPAQUE_PRIVATE_DATA
};

// ---------------------------------------------------------------------------

/** \brief Abstract class modelling a coordinate reference system consisting of
 * one Coordinate System and either one datum::Datum or one
 * datum::DatumEnsemble.
 *
 * \remark Implements SingleCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL SingleCRS : public CRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~SingleCRS() override;
    //! @endcond

    PROJ_DLL const datum::DatumPtr &datum() PROJ_PURE_DECL;
    PROJ_DLL const datum::DatumEnsemblePtr &datumEnsemble() PROJ_PURE_DECL;
    PROJ_DLL const cs::CoordinateSystemNNPtr &coordinateSystem() PROJ_PURE_DECL;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        exportDatumOrDatumEnsembleToWkt(io::WKTFormatter *formatter)
            const; // throw(io::FormattingException)

    PROJ_INTERNAL const datum::DatumNNPtr
    datumNonNull(const io::DatabaseContextPtr &dbContext) const;
    //! @endcond

  protected:
    PROJ_INTERNAL SingleCRS(const datum::DatumPtr &datumIn,
                            const datum::DatumEnsemblePtr &datumEnsembleIn,
                            const cs::CoordinateSystemNNPtr &csIn);
    PROJ_INTERNAL SingleCRS(const SingleCRS &other);

    PROJ_INTERNAL bool
    baseIsEquivalentTo(const util::IComparable *other,
                       util::IComparable::Criterion criterion =
                           util::IComparable::Criterion::STRICT,
                       const io::DatabaseContextPtr &dbContext = nullptr) const;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    SingleCRS &operator=(const SingleCRS &other) = delete;
};

/** Shared pointer of SingleCRS */
using SingleCRSPtr = std::shared_ptr<SingleCRS>;
/** Non-null shared pointer of SingleCRS */
using SingleCRSNNPtr = util::nn<SingleCRSPtr>;

// ---------------------------------------------------------------------------

class GeodeticCRS;
/** Shared pointer of GeodeticCRS */
using GeodeticCRSPtr = std::shared_ptr<GeodeticCRS>;
/** Non-null shared pointer of GeodeticCRS */
using GeodeticCRSNNPtr = util::nn<GeodeticCRSPtr>;

/** \brief A coordinate reference system associated with a geodetic reference
 * frame and a three-dimensional Cartesian or spherical coordinate system.
 *
 * If the geodetic reference frame is dynamic or if the geodetic CRS has an
 * association to a velocity model then the geodetic CRS is dynamic, else it
 * is static.
 *
 * \remark Implements GeodeticCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL GeodeticCRS : virtual public SingleCRS,
                                 public io::IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeodeticCRS() override;
    //! @endcond

    PROJ_DLL const datum::GeodeticReferenceFramePtr &datum() PROJ_PURE_DECL;

    PROJ_DLL const datum::PrimeMeridianNNPtr &primeMeridian() PROJ_PURE_DECL;
    PROJ_DLL const datum::EllipsoidNNPtr &ellipsoid() PROJ_PURE_DECL;

    // coordinateSystem() returns either a EllipsoidalCS, SphericalCS or
    // CartesianCS

    PROJ_DLL const std::vector<operation::PointMotionOperationNNPtr> &
    velocityModel() PROJ_PURE_DECL;

    // Non-standard

    PROJ_DLL bool isGeocentric() PROJ_PURE_DECL;

    PROJ_DLL bool isSphericalPlanetocentric() PROJ_PURE_DECL;

    PROJ_DLL static GeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFrameNNPtr &datum,
           const cs::SphericalCSNNPtr &cs);

    PROJ_DLL static GeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFrameNNPtr &datum,
           const cs::CartesianCSNNPtr &cs);

    PROJ_DLL static GeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFramePtr &datum,
           const datum::DatumEnsemblePtr &datumEnsemble,
           const cs::SphericalCSNNPtr &cs);

    PROJ_DLL static GeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFramePtr &datum,
           const datum::DatumEnsemblePtr &datumEnsemble,
           const cs::CartesianCSNNPtr &cs);

    PROJ_DLL static const GeodeticCRSNNPtr EPSG_4978; // WGS 84 Geocentric

    PROJ_DLL std::list<std::pair<GeodeticCRSNNPtr, int>>
    identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        addDatumInfoToPROJString(io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL const datum::GeodeticReferenceFrameNNPtr
    datumNonNull(const io::DatabaseContextPtr &dbContext) const;

    PROJ_INTERNAL void addGeocentricUnitConversionIntoPROJString(
        io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL void addAxisSwap(io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL void
    addAngularUnitConvertAndAxisSwap(io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    //! @endcond

  protected:
    PROJ_INTERNAL GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                              const datum::DatumEnsemblePtr &datumEnsembleIn,
                              const cs::EllipsoidalCSNNPtr &csIn);
    PROJ_INTERNAL GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                              const datum::DatumEnsemblePtr &datumEnsembleIn,
                              const cs::SphericalCSNNPtr &csIn);
    PROJ_INTERNAL GeodeticCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                              const datum::DatumEnsemblePtr &datumEnsembleIn,
                              const cs::CartesianCSNNPtr &csIn);
    PROJ_INTERNAL GeodeticCRS(const GeodeticCRS &other);

    PROJ_INTERNAL static GeodeticCRSNNPtr createEPSG_4978();

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL void _exportToJSONInternal(
        io::JSONFormatter *formatter,
        const char *objectName) const; // throw(FormattingException)

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    PROJ_INTERNAL bool
    _isEquivalentToNoTypeCheck(const util::IComparable *other,
                               util::IComparable::Criterion criterion,
                               const io::DatabaseContextPtr &dbContext) const;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA

    GeodeticCRS &operator=(const GeodeticCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief A coordinate reference system associated with a geodetic reference
 * frame and a two- or three-dimensional ellipsoidal coordinate system.
 *
 * If the geodetic reference frame is dynamic or if the geographic CRS has an
 * association to a velocity model then the geodetic CRS is dynamic, else it is
 * static.
 *
 * \remark Implements GeographicCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL GeographicCRS : public GeodeticCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~GeographicCRS() override;
    //! @endcond

    PROJ_DLL const cs::EllipsoidalCSNNPtr &coordinateSystem() PROJ_PURE_DECL;

    // Non-standard
    PROJ_DLL static GeographicCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFrameNNPtr &datum,
           const cs::EllipsoidalCSNNPtr &cs);
    PROJ_DLL static GeographicCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::GeodeticReferenceFramePtr &datum,
           const datum::DatumEnsemblePtr &datumEnsemble,
           const cs::EllipsoidalCSNNPtr &cs);

    PROJ_DLL GeographicCRSNNPtr
    demoteTo2D(const std::string &newName,
               const io::DatabaseContextPtr &dbContext) const;

    PROJ_DLL static const GeographicCRSNNPtr EPSG_4267; // NAD27
    PROJ_DLL static const GeographicCRSNNPtr EPSG_4269; // NAD83
    PROJ_DLL static const GeographicCRSNNPtr EPSG_4326; // WGS 84 2D
    PROJ_DLL static const GeographicCRSNNPtr OGC_CRS84; // CRS84 (Long, Lat)
    PROJ_DLL static const GeographicCRSNNPtr EPSG_4807; // NTF Paris
    PROJ_DLL static const GeographicCRSNNPtr EPSG_4979; // WGS 84 3D

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        PROJ_INTERNAL void
        _exportToPROJString(io::PROJStringFormatter *formatter)
            const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_DLL bool is2DPartOf3D(
        util::nn<const GeographicCRS *> other,
        const io::DatabaseContextPtr &dbContext = nullptr) PROJ_PURE_DECL;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    //! @endcond

  protected:
    PROJ_INTERNAL GeographicCRS(const datum::GeodeticReferenceFramePtr &datumIn,
                                const datum::DatumEnsemblePtr &datumEnsembleIn,
                                const cs::EllipsoidalCSNNPtr &csIn);
    PROJ_INTERNAL GeographicCRS(const GeographicCRS &other);

    PROJ_INTERNAL static GeographicCRSNNPtr createEPSG_4267();
    PROJ_INTERNAL static GeographicCRSNNPtr createEPSG_4269();
    PROJ_INTERNAL static GeographicCRSNNPtr createEPSG_4326();
    PROJ_INTERNAL static GeographicCRSNNPtr createOGC_CRS84();
    PROJ_INTERNAL static GeographicCRSNNPtr createEPSG_4807();
    PROJ_INTERNAL static GeographicCRSNNPtr createEPSG_4979();

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA

    GeographicCRS &operator=(const GeographicCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief A coordinate reference system having a vertical reference frame and
 * a one-dimensional vertical coordinate system used for recording
 * gravity-related heights or depths.
 *
 * Vertical CRSs make use of the direction of gravity to define the concept of
 * height or depth, but the relationship with gravity may not be
 * straightforward. If the vertical reference frame is dynamic or if the
 * vertical CRS has an association to a velocity model then the CRS is dynamic,
 * else it is static.
 *
 * \note Ellipsoidal heights cannot be captured in a vertical coordinate
 * reference system. They exist only as an inseparable part of a 3D coordinate
 * tuple defined in a geographic 3D coordinate reference system.
 *
 * \remark Implements VerticalCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL VerticalCRS : virtual public SingleCRS,
                                 public io::IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~VerticalCRS() override;
    //! @endcond

    PROJ_DLL const datum::VerticalReferenceFramePtr datum() const;
    PROJ_DLL const cs::VerticalCSNNPtr coordinateSystem() const;
    PROJ_DLL const std::vector<operation::TransformationNNPtr> &
    geoidModel() PROJ_PURE_DECL;
    PROJ_DLL const std::vector<operation::PointMotionOperationNNPtr> &
    velocityModel() PROJ_PURE_DECL;

    PROJ_DLL static VerticalCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::VerticalReferenceFrameNNPtr &datumIn,
           const cs::VerticalCSNNPtr &csIn);

    PROJ_DLL static VerticalCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::VerticalReferenceFramePtr &datumIn,
           const datum::DatumEnsemblePtr &datumEnsembleIn,
           const cs::VerticalCSNNPtr &csIn);

    PROJ_DLL std::list<std::pair<VerticalCRSNNPtr, int>>
    identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        addLinearUnitConvert(io::PROJStringFormatter *formatter) const;

    PROJ_INTERNAL const datum::VerticalReferenceFrameNNPtr
    datumNonNull(const io::DatabaseContextPtr &dbContext) const;

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    //! @endcond

  protected:
    PROJ_INTERNAL VerticalCRS(const datum::VerticalReferenceFramePtr &datumIn,
                              const datum::DatumEnsemblePtr &datumEnsembleIn,
                              const cs::VerticalCSNNPtr &csIn);
    PROJ_INTERNAL VerticalCRS(const VerticalCRS &other);

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    INLINED_MAKE_SHARED

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    VerticalCRS &operator=(const VerticalCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Abstract class modelling a single coordinate reference system that
 * is defined through the application of a specified coordinate conversion to
 * the definition of a previously established single coordinate reference
 * system referred to as the base CRS.
 *
 * A derived coordinate reference system inherits its datum (or datum ensemble)
 * from its base CRS. The coordinate conversion between the base and derived
 * coordinate reference system is implemented using the parameters and
 * formula(s) specified in the definition of the coordinate conversion.
 *
 * \remark Implements DerivedCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DerivedCRS : virtual public SingleCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedCRS() override;
    //! @endcond

    PROJ_DLL const SingleCRSNNPtr &baseCRS() PROJ_PURE_DECL;
    PROJ_DLL const operation::ConversionNNPtr derivingConversion() const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress

        // Use this method with extreme care ! It should never be used
        // to recreate a new Derived/ProjectedCRS !
        PROJ_INTERNAL const operation::ConversionNNPtr &
        derivingConversionRef() PROJ_PURE_DECL;

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedCRS(const SingleCRSNNPtr &baseCRSIn,
               const operation::ConversionNNPtr &derivingConversionIn,
               const cs::CoordinateSystemNNPtr &cs);
    PROJ_INTERNAL DerivedCRS(const DerivedCRS &other);

    PROJ_INTERNAL void setDerivingConversionCRS();

    PROJ_INTERNAL void baseExportToWKT(
        io::WKTFormatter *formatter, const std::string &keyword,
        const std::string &baseKeyword) const; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL virtual const char *className() const = 0;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DerivedCRS &operator=(const DerivedCRS &other) = delete;
};

/** Shared pointer of DerivedCRS */
using DerivedCRSPtr = std::shared_ptr<DerivedCRS>;
/** Non-null shared pointer of DerivedCRS */
using DerivedCRSNNPtr = util::nn<DerivedCRSPtr>;

// ---------------------------------------------------------------------------

class ProjectedCRS;
/** Shared pointer of ProjectedCRS */
using ProjectedCRSPtr = std::shared_ptr<ProjectedCRS>;
/** Non-null shared pointer of ProjectedCRS */
using ProjectedCRSNNPtr = util::nn<ProjectedCRSPtr>;

/** \brief A derived coordinate reference system which has a geodetic
 * (usually geographic) coordinate reference system as its base CRS, thereby
 * inheriting a geodetic reference frame, and is converted using a map
 * projection.
 *
 * It has a Cartesian coordinate system, usually two-dimensional but may be
 * three-dimensional; in the 3D case the base geographic CRSs ellipsoidal
 * height is passed through unchanged and forms the vertical axis of the
 * projected CRS's Cartesian coordinate system.
 *
 * \remark Implements ProjectedCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ProjectedCRS final : public DerivedCRS,
                                        public io::IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ProjectedCRS() override;
    //! @endcond

    PROJ_DLL const GeodeticCRSNNPtr &baseCRS() PROJ_PURE_DECL;
    PROJ_DLL const cs::CartesianCSNNPtr &coordinateSystem() PROJ_PURE_DECL;

    PROJ_DLL static ProjectedCRSNNPtr
    create(const util::PropertyMap &properties,
           const GeodeticCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::CartesianCSNNPtr &csIn);

    PROJ_DLL std::list<std::pair<ProjectedCRSNNPtr, int>>
    identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    PROJ_DLL ProjectedCRSNNPtr
    demoteTo2D(const std::string &newName,
               const io::DatabaseContextPtr &dbContext) const;

    PROJ_PRIVATE :
        //! @cond Doxygen_Suppress
        PROJ_INTERNAL void
        addUnitConvertAndAxisSwap(io::PROJStringFormatter *formatter,
                                  bool axisSpecFound) const;

    PROJ_INTERNAL static void addUnitConvertAndAxisSwap(
        const std::vector<cs::CoordinateSystemAxisNNPtr> &axisListIn,
        io::PROJStringFormatter *formatter, bool axisSpecFound);

    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_FOR_TEST ProjectedCRSNNPtr alterParametersLinearUnit(
        const common::UnitOfMeasure &unit, bool convertToNewUnit) const;

    //! @endcond

  protected:
    PROJ_INTERNAL
    ProjectedCRS(const GeodeticCRSNNPtr &baseCRSIn,
                 const operation::ConversionNNPtr &derivingConversionIn,
                 const cs::CartesianCSNNPtr &csIn);
    PROJ_INTERNAL ProjectedCRS(const ProjectedCRS &other);

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    PROJ_INTERNAL const char *className() const override {
        return "ProjectedCRS";
    }

    INLINED_MAKE_SHARED

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ProjectedCRS &operator=(const ProjectedCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class TemporalCRS;
/** Shared pointer of TemporalCRS */
using TemporalCRSPtr = std::shared_ptr<TemporalCRS>;
/** Non-null shared pointer of TemporalCRS */
using TemporalCRSNNPtr = util::nn<TemporalCRSPtr>;

/** \brief A coordinate reference system associated with a temporal datum and a
 * one-dimensional temporal coordinate system.
 *
 * \remark Implements TemporalCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL TemporalCRS : virtual public SingleCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~TemporalCRS() override;
    //! @endcond

    PROJ_DLL const datum::TemporalDatumNNPtr datum() const;

    PROJ_DLL const cs::TemporalCSNNPtr coordinateSystem() const;

    PROJ_DLL static TemporalCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::TemporalDatumNNPtr &datumIn,
           const cs::TemporalCSNNPtr &csIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL TemporalCRS(const datum::TemporalDatumNNPtr &datumIn,
                              const cs::TemporalCSNNPtr &csIn);
    PROJ_INTERNAL TemporalCRS(const TemporalCRS &other);

    INLINED_MAKE_SHARED

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    TemporalCRS &operator=(const TemporalCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class EngineeringCRS;
/** Shared pointer of EngineeringCRS */
using EngineeringCRSPtr = std::shared_ptr<EngineeringCRS>;
/** Non-null shared pointer of EngineeringCRS */
using EngineeringCRSNNPtr = util::nn<EngineeringCRSPtr>;

/** \brief Contextually local coordinate reference system associated with an
 * engineering datum.
 *
 * It is applied either to activities on or near the surface of the Earth
 * without geodetic corrections, or on moving platforms such as road vehicles,
 * vessels, aircraft or spacecraft, or as the internal CRS of an image.
 *
 * In \ref WKT2, it maps to a ENGINEERINGCRS / ENGCRS keyword. In \ref WKT1,
 * it maps to a LOCAL_CS keyword.
 *
 * \remark Implements EngineeringCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL EngineeringCRS : virtual public SingleCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~EngineeringCRS() override;
    //! @endcond

    PROJ_DLL const datum::EngineeringDatumNNPtr datum() const;

    PROJ_DLL static EngineeringCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::EngineeringDatumNNPtr &datumIn,
           const cs::CoordinateSystemNNPtr &csIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL EngineeringCRS(const datum::EngineeringDatumNNPtr &datumIn,
                                 const cs::CoordinateSystemNNPtr &csIn);
    PROJ_INTERNAL EngineeringCRS(const EngineeringCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    EngineeringCRS &operator=(const EngineeringCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class ParametricCRS;
/** Shared pointer of ParametricCRS */
using ParametricCRSPtr = std::shared_ptr<ParametricCRS>;
/** Non-null shared pointer of ParametricCRS */
using ParametricCRSNNPtr = util::nn<ParametricCRSPtr>;

/** \brief Contextually local coordinate reference system associated with an
 * engineering datum.
 *
 * This is applied either to activities on or near the surface of the Earth
 * without geodetic corrections, or on moving platforms such as road vehicles
 * vessels, aircraft or spacecraft, or as the internal CRS of an image.
 *
 * \remark Implements ParametricCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL ParametricCRS : virtual public SingleCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~ParametricCRS() override;
    //! @endcond

    PROJ_DLL const datum::ParametricDatumNNPtr datum() const;

    PROJ_DLL const cs::ParametricCSNNPtr coordinateSystem() const;

    PROJ_DLL static ParametricCRSNNPtr
    create(const util::PropertyMap &properties,
           const datum::ParametricDatumNNPtr &datumIn,
           const cs::ParametricCSNNPtr &csIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    //! @endcond

  protected:
    PROJ_INTERNAL ParametricCRS(const datum::ParametricDatumNNPtr &datumIn,
                                const cs::ParametricCSNNPtr &csIn);
    PROJ_INTERNAL ParametricCRS(const ParametricCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    ParametricCRS &operator=(const ParametricCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Exception thrown when attempting to create an invalid compound CRS
 */
class PROJ_GCC_DLL InvalidCompoundCRSException : public util::Exception {
  public:
    //! @cond Doxygen_Suppress
    PROJ_INTERNAL explicit InvalidCompoundCRSException(const char *message);
    PROJ_INTERNAL explicit InvalidCompoundCRSException(
        const std::string &message);
    PROJ_DLL
    InvalidCompoundCRSException(const InvalidCompoundCRSException &other);
    PROJ_DLL ~InvalidCompoundCRSException() override;
    //! @endcond
};

// ---------------------------------------------------------------------------

/** \brief A coordinate reference system describing the position of points
 * through two or more independent single coordinate reference systems.
 *
 * \note Two coordinate reference systems are independent of each other
 * if coordinate values in one cannot be converted or transformed into
 * coordinate values in the other.
 *
 * \note As a departure to \ref ISO_19111_2019, we allow to build a CompoundCRS
 * from CRS objects, whereas ISO19111:2019 restricts the components to
 * SingleCRS.
 *
 * \remark Implements CompoundCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL CompoundCRS final : public CRS,
                                       public io::IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~CompoundCRS() override;
    //! @endcond

    PROJ_DLL const std::vector<CRSNNPtr> &
    componentReferenceSystems() PROJ_PURE_DECL;

    PROJ_DLL std::list<std::pair<CompoundCRSNNPtr, int>>
    identify(const io::AuthorityFactoryPtr &authorityFactory) const;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
    //! @endcond

    PROJ_DLL static CompoundCRSNNPtr
    create(const util::PropertyMap &properties,
           const std::vector<CRSNNPtr>
               &components); // throw InvalidCompoundCRSException

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL static CRSNNPtr
    createLax(const util::PropertyMap &properties,
              const std::vector<CRSNNPtr> &components,
              const io::DatabaseContextPtr
                  &dbContext); // throw InvalidCompoundCRSException
                               //! @endcond

  protected:
    // relaxed: standard say SingleCRSNNPtr
    PROJ_INTERNAL explicit CompoundCRS(const std::vector<CRSNNPtr> &components);
    PROJ_INTERNAL CompoundCRS(const CompoundCRS &other);

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    CompoundCRS &operator=(const CompoundCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief A coordinate reference system with an associated transformation to
 * a target/hub CRS.
 *
 * The definition of a CRS is not dependent upon any relationship to an
 * independent CRS. However in an implementation that merges datasets
 * referenced to differing CRSs, it is sometimes useful to associate the
 * definition of the transformation that has been used with the CRS definition.
 * This facilitates the interrelationship of CRS by concatenating
 * transformations via a common or hub CRS. This is sometimes referred to as
 * "early-binding". \ref WKT2 permits the association of an abridged coordinate
 * transformation description with a coordinate reference system description in
 * a single text string. In a BoundCRS, the abridged coordinate transformation
 * is applied to the source CRS with the target CRS being the common or hub
 * system.
 *
 * Coordinates referring to a BoundCRS are expressed into its source/base CRS.
 *
 * This abstraction can for example model the concept of TOWGS84 datum shift
 * present in \ref WKT1.
 *
 * \note Contrary to other CRS classes of this package, there is no
 * \ref ISO_19111_2019 modelling of a BoundCRS.
 *
 * \remark Implements BoundCRS from \ref WKT2
 */
class PROJ_GCC_DLL BoundCRS final : public CRS,
                                    public io::IPROJStringExportable {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~BoundCRS() override;
    //! @endcond

    PROJ_DLL const CRSNNPtr &baseCRS() PROJ_PURE_DECL;
    PROJ_DLL CRSNNPtr baseCRSWithCanonicalBoundCRS() const;

    PROJ_DLL const CRSNNPtr &hubCRS() PROJ_PURE_DECL;
    PROJ_DLL const operation::TransformationNNPtr &
    transformation() PROJ_PURE_DECL;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)
    //! @endcond

    PROJ_DLL static BoundCRSNNPtr
    create(const util::PropertyMap &properties, const CRSNNPtr &baseCRSIn,
           const CRSNNPtr &hubCRSIn,
           const operation::TransformationNNPtr &transformationIn);

    PROJ_DLL static BoundCRSNNPtr
    create(const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
           const operation::TransformationNNPtr &transformationIn);

    PROJ_DLL static BoundCRSNNPtr
    createFromTOWGS84(const CRSNNPtr &baseCRSIn,
                      const std::vector<double> &TOWGS84Parameters);

    PROJ_DLL static BoundCRSNNPtr
    createFromNadgrids(const CRSNNPtr &baseCRSIn, const std::string &filename);

  protected:
    PROJ_INTERNAL
    BoundCRS(const CRSNNPtr &baseCRSIn, const CRSNNPtr &hubCRSIn,
             const operation::TransformationNNPtr &transformationIn);
    PROJ_INTERNAL BoundCRS(const BoundCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL void _exportToJSON(io::JSONFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL BoundCRSNNPtr shallowCloneAsBoundCRS() const;
    PROJ_INTERNAL bool isTOWGS84Compatible() const;
    PROJ_INTERNAL std::string
    getHDatumPROJ4GRIDS(const io::DatabaseContextPtr &databaseContext) const;
    PROJ_INTERNAL std::string
    getVDatumPROJ4GRIDS(const crs::GeographicCRS *geogCRSOfCompoundCRS,
                        const char **outGeoidCRSValue) const;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    BoundCRS &operator=(const BoundCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class DerivedGeodeticCRS;
/** Shared pointer of DerivedGeodeticCRS */
using DerivedGeodeticCRSPtr = std::shared_ptr<DerivedGeodeticCRS>;
/** Non-null shared pointer of DerivedGeodeticCRS */
using DerivedGeodeticCRSNNPtr = util::nn<DerivedGeodeticCRSPtr>;

/** \brief A derived coordinate reference system which has either a geodetic
 * or a geographic coordinate reference system as its base CRS, thereby
 * inheriting a geodetic reference frame, and associated with a 3D Cartesian
 * or spherical coordinate system.
 *
 * \remark Implements DerivedGeodeticCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DerivedGeodeticCRS final : public GeodeticCRS,
                                              public DerivedCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedGeodeticCRS() override;
    //! @endcond

    PROJ_DLL const GeodeticCRSNNPtr baseCRS() const;

    PROJ_DLL static DerivedGeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const GeodeticCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::CartesianCSNNPtr &csIn);

    PROJ_DLL static DerivedGeodeticCRSNNPtr
    create(const util::PropertyMap &properties,
           const GeodeticCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::SphericalCSNNPtr &csIn);

    //! @cond Doxygen_Suppress
    void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void
    _exportToJSON(io::JSONFormatter *formatter) const override {
        return DerivedCRS::_exportToJSON(formatter);
    }

    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedGeodeticCRS(const GeodeticCRSNNPtr &baseCRSIn,
                       const operation::ConversionNNPtr &derivingConversionIn,
                       const cs::CartesianCSNNPtr &csIn);
    PROJ_INTERNAL
    DerivedGeodeticCRS(const GeodeticCRSNNPtr &baseCRSIn,
                       const operation::ConversionNNPtr &derivingConversionIn,
                       const cs::SphericalCSNNPtr &csIn);
    PROJ_INTERNAL DerivedGeodeticCRS(const DerivedGeodeticCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    // cppcheck-suppress functionStatic
    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    PROJ_INTERNAL const char *className() const override {
        return "DerivedGeodeticCRS";
    }

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DerivedGeodeticCRS &operator=(const DerivedGeodeticCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class DerivedGeographicCRS;
/** Shared pointer of DerivedGeographicCRS */
using DerivedGeographicCRSPtr = std::shared_ptr<DerivedGeographicCRS>;
/** Non-null shared pointer of DerivedGeographicCRS */
using DerivedGeographicCRSNNPtr = util::nn<DerivedGeographicCRSPtr>;

/** \brief A derived coordinate reference system which has either a geodetic or
 * a geographic coordinate reference system as its base CRS, thereby inheriting
 * a geodetic reference frame, and an ellipsoidal coordinate system.
 *
 * A derived geographic CRS can be based on a geodetic CRS only if that
 * geodetic CRS definition includes an ellipsoid.
 *
 * \remark Implements DerivedGeographicCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DerivedGeographicCRS final : public GeographicCRS,
                                                public DerivedCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedGeographicCRS() override;
    //! @endcond

    PROJ_DLL const GeodeticCRSNNPtr baseCRS() const;

    PROJ_DLL static DerivedGeographicCRSNNPtr
    create(const util::PropertyMap &properties,
           const GeodeticCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::EllipsoidalCSNNPtr &csIn);

    PROJ_DLL DerivedGeographicCRSNNPtr
    demoteTo2D(const std::string &newName,
               const io::DatabaseContextPtr &dbContext) const;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void
    _exportToJSON(io::JSONFormatter *formatter) const override {
        return DerivedCRS::_exportToJSON(formatter);
    }

    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedGeographicCRS(const GeodeticCRSNNPtr &baseCRSIn,
                         const operation::ConversionNNPtr &derivingConversionIn,
                         const cs::EllipsoidalCSNNPtr &csIn);
    PROJ_INTERNAL DerivedGeographicCRS(const DerivedGeographicCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    PROJ_INTERNAL const char *className() const override {
        return "DerivedGeographicCRS";
    }

    // cppcheck-suppress functionStatic
    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DerivedGeographicCRS &operator=(const DerivedGeographicCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class DerivedProjectedCRS;
/** Shared pointer of DerivedProjectedCRS */
using DerivedProjectedCRSPtr = std::shared_ptr<DerivedProjectedCRS>;
/** Non-null shared pointer of DerivedProjectedCRS */
using DerivedProjectedCRSNNPtr = util::nn<DerivedProjectedCRSPtr>;

/** \brief A derived coordinate reference system which has a projected
 * coordinate reference system as its base CRS, thereby inheriting a geodetic
 * reference frame, but also inheriting the distortion characteristics of the
 * base projected CRS.
 *
 * A DerivedProjectedCRS is not a ProjectedCRS.
 *
 * \remark Implements DerivedProjectedCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DerivedProjectedCRS final : public DerivedCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedProjectedCRS() override;
    //! @endcond

    PROJ_DLL const ProjectedCRSNNPtr baseCRS() const;

    PROJ_DLL static DerivedProjectedCRSNNPtr
    create(const util::PropertyMap &properties,
           const ProjectedCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::CoordinateSystemNNPtr &csIn);

    PROJ_DLL DerivedProjectedCRSNNPtr
    demoteTo2D(const std::string &newName,
               const io::DatabaseContextPtr &dbContext) const;

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void
    addUnitConvertAndAxisSwap(io::PROJStringFormatter *formatter) const;
    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedProjectedCRS(const ProjectedCRSNNPtr &baseCRSIn,
                        const operation::ConversionNNPtr &derivingConversionIn,
                        const cs::CoordinateSystemNNPtr &csIn);
    PROJ_INTERNAL DerivedProjectedCRS(const DerivedProjectedCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL const char *className() const override {
        return "DerivedProjectedCRS";
    }

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DerivedProjectedCRS &operator=(const DerivedProjectedCRS &other) = delete;
};

// ---------------------------------------------------------------------------

class DerivedVerticalCRS;
/** Shared pointer of DerivedVerticalCRS */
using DerivedVerticalCRSPtr = std::shared_ptr<DerivedVerticalCRS>;
/** Non-null shared pointer of DerivedVerticalCRS */
using DerivedVerticalCRSNNPtr = util::nn<DerivedVerticalCRSPtr>;

/** \brief A derived coordinate reference system which has a vertical
 * coordinate reference system as its base CRS, thereby inheriting a vertical
 * reference frame, and a vertical coordinate system.
 *
 * \remark Implements DerivedVerticalCRS from \ref ISO_19111_2019
 */
class PROJ_GCC_DLL DerivedVerticalCRS final : public VerticalCRS,
                                              public DerivedCRS {
  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedVerticalCRS() override;
    //! @endcond

    PROJ_DLL const VerticalCRSNNPtr baseCRS() const;

    PROJ_DLL static DerivedVerticalCRSNNPtr
    create(const util::PropertyMap &properties,
           const VerticalCRSNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const cs::VerticalCSNNPtr &csIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void
    _exportToJSON(io::JSONFormatter *formatter) const override {
        return DerivedCRS::_exportToJSON(formatter);
    }

    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedVerticalCRS(const VerticalCRSNNPtr &baseCRSIn,
                       const operation::ConversionNNPtr &derivingConversionIn,
                       const cs::VerticalCSNNPtr &csIn);
    PROJ_INTERNAL DerivedVerticalCRS(const DerivedVerticalCRS &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL std::list<std::pair<CRSNNPtr, int>>
    _identify(const io::AuthorityFactoryPtr &authorityFactory) const override;

    PROJ_INTERNAL const char *className() const override {
        return "DerivedVerticalCRS";
    }

    // cppcheck-suppress functionStatic
    PROJ_INTERNAL void _exportToPROJString(io::PROJStringFormatter *formatter)
        const override; // throw(FormattingException)

    INLINED_MAKE_SHARED

  private:
    PROJ_OPAQUE_PRIVATE_DATA
    DerivedVerticalCRS &operator=(const DerivedVerticalCRS &other) = delete;
};

// ---------------------------------------------------------------------------

/** \brief Template representing a derived coordinate reference system.
 */
template <class DerivedCRSTraits>
class PROJ_GCC_DLL DerivedCRSTemplate final : public DerivedCRSTraits::BaseType,
                                              public DerivedCRS {
  protected:
    /** Base type */
    typedef typename DerivedCRSTraits::BaseType BaseType;
    /** CSType */
    typedef typename DerivedCRSTraits::CSType CSType;

  public:
    //! @cond Doxygen_Suppress
    PROJ_DLL ~DerivedCRSTemplate() override;
    //! @endcond

    /** Non-null shared pointer of DerivedCRSTemplate */
    typedef typename util::nn<std::shared_ptr<DerivedCRSTemplate>> NNPtr;
    /** Non-null shared pointer of BaseType */
    typedef util::nn<std::shared_ptr<BaseType>> BaseNNPtr;
    /** Non-null shared pointer of CSType */
    typedef util::nn<std::shared_ptr<CSType>> CSNNPtr;

    /** \brief Return the base CRS of a DerivedCRSTemplate.
     *
     * @return the base CRS.
     */
    PROJ_DLL const BaseNNPtr baseCRS() const;

    /** \brief Instantiate a DerivedCRSTemplate from a base CRS, a deriving
     * conversion and a cs::CoordinateSystem.
     *
     * @param properties See \ref general_properties.
     * At minimum the name should be defined.
     * @param baseCRSIn base CRS.
     * @param derivingConversionIn the deriving conversion from the base CRS to
     * this
     * CRS.
     * @param csIn the coordinate system.
     * @return new DerivedCRSTemplate.
     */
    PROJ_DLL static NNPtr
    create(const util::PropertyMap &properties, const BaseNNPtr &baseCRSIn,
           const operation::ConversionNNPtr &derivingConversionIn,
           const CSNNPtr &csIn);

    //! @cond Doxygen_Suppress
    PROJ_INTERNAL void _exportToWKT(io::WKTFormatter *formatter)
        const override; // throw(io::FormattingException)

    PROJ_INTERNAL void
    _exportToJSON(io::JSONFormatter *formatter) const override {
        return DerivedCRS::_exportToJSON(formatter);
    }
    //! @endcond

  protected:
    PROJ_INTERNAL
    DerivedCRSTemplate(const BaseNNPtr &baseCRSIn,
                       const operation::ConversionNNPtr &derivingConversionIn,
                       const CSNNPtr &csIn);
    // cppcheck-suppress noExplicitConstructor
    PROJ_INTERNAL DerivedCRSTemplate(const DerivedCRSTemplate &other);

    PROJ_INTERNAL CRSNNPtr _shallowClone() const override;

    PROJ_INTERNAL bool _isEquivalentTo(
        const util::IComparable *other,
        util::IComparable::Criterion criterion =
            util::IComparable::Criterion::STRICT,
        const io::DatabaseContextPtr &dbContext = nullptr) const override;

    PROJ_INTERNAL const char *className() const override;

    INLINED_MAKE_SHARED

  private:
    struct PROJ_INTERNAL Private;
    std::unique_ptr<Private> d;

    DerivedCRSTemplate &operator=(const DerivedCRSTemplate &other) = delete;
};

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PROJ_GCC_DLL DerivedEngineeringCRSTraits {
    typedef EngineeringCRS BaseType;
    typedef cs::CoordinateSystem CSType;
    // old x86_64-w64-mingw32-g++ has issues with static variables. use method
    // instead
    inline static const std::string &CRSName();
    inline static const std::string &WKTKeyword();
    inline static const std::string &WKTBaseKeyword();
    static const bool wkt2_2019_only = true;
};
//! @endcond

/** \brief A derived coordinate reference system which has an engineering
 * coordinate reference system as its base CRS, thereby inheriting an
 * engineering datum, and is associated with one of the coordinate system
 * types for an EngineeringCRS
 *
 * \remark Implements DerivedEngineeringCRS from \ref ISO_19111_2019
 */
#ifdef DOXYGEN_ENABLED
class DerivedEngineeringCRS
    : public DerivedCRSTemplate<DerivedEngineeringCRSTraits> {};
#else
using DerivedEngineeringCRS = DerivedCRSTemplate<DerivedEngineeringCRSTraits>;
#endif

#ifndef DO_NOT_DEFINE_EXTERN_DERIVED_CRS_TEMPLATE
extern template class DerivedCRSTemplate<DerivedEngineeringCRSTraits>;
#endif

/** Shared pointer of DerivedEngineeringCRS */
using DerivedEngineeringCRSPtr = std::shared_ptr<DerivedEngineeringCRS>;
/** Non-null shared pointer of DerivedEngineeringCRS */
using DerivedEngineeringCRSNNPtr = util::nn<DerivedEngineeringCRSPtr>;

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PROJ_GCC_DLL DerivedParametricCRSTraits {
    typedef ParametricCRS BaseType;
    typedef cs::ParametricCS CSType;
    // old x86_64-w64-mingw32-g++ has issues with static variables. use method
    // instead
    inline static const std::string &CRSName();
    inline static const std::string &WKTKeyword();
    inline static const std::string &WKTBaseKeyword();
    static const bool wkt2_2019_only = false;
};
//! @endcond

/** \brief A derived coordinate reference system which has a parametric
 * coordinate reference system as its base CRS, thereby inheriting a parametric
 * datum, and a parametric coordinate system.
 *
 * \remark Implements DerivedParametricCRS from \ref ISO_19111_2019
 */
#ifdef DOXYGEN_ENABLED
class DerivedParametricCRS
    : public DerivedCRSTemplate<DerivedParametricCRSTraits> {};
#else
using DerivedParametricCRS = DerivedCRSTemplate<DerivedParametricCRSTraits>;
#endif

#ifndef DO_NOT_DEFINE_EXTERN_DERIVED_CRS_TEMPLATE
extern template class DerivedCRSTemplate<DerivedParametricCRSTraits>;
#endif

/** Shared pointer of DerivedParametricCRS */
using DerivedParametricCRSPtr = std::shared_ptr<DerivedParametricCRS>;
/** Non-null shared pointer of DerivedParametricCRS */
using DerivedParametricCRSNNPtr = util::nn<DerivedParametricCRSPtr>;

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PROJ_GCC_DLL DerivedTemporalCRSTraits {
    typedef TemporalCRS BaseType;
    typedef cs::TemporalCS CSType;
    // old x86_64-w64-mingw32-g++ has issues with static variables. use method
    // instead
    inline static const std::string &CRSName();
    inline static const std::string &WKTKeyword();
    inline static const std::string &WKTBaseKeyword();
    static const bool wkt2_2019_only = false;
};
//! @endcond

/** \brief A derived coordinate reference system which has a temporal
 * coordinate reference system as its base CRS, thereby inheriting a temporal
 * datum, and a temporal coordinate system.
 *
 * \remark Implements DerivedTemporalCRS from \ref ISO_19111_2019
 */
#ifdef DOXYGEN_ENABLED
class DerivedTemporalCRS : public DerivedCRSTemplate<DerivedTemporalCRSTraits> {
};
#else
using DerivedTemporalCRS = DerivedCRSTemplate<DerivedTemporalCRSTraits>;
#endif

#ifndef DO_NOT_DEFINE_EXTERN_DERIVED_CRS_TEMPLATE
extern template class DerivedCRSTemplate<DerivedTemporalCRSTraits>;
#endif

/** Shared pointer of DerivedTemporalCRS */
using DerivedTemporalCRSPtr = std::shared_ptr<DerivedTemporalCRS>;
/** Non-null shared pointer of DerivedTemporalCRS */
using DerivedTemporalCRSNNPtr = util::nn<DerivedTemporalCRSPtr>;

// ---------------------------------------------------------------------------

} // namespace crs

NS_PROJ_END

#endif //  CRS_HH_INCLUDED
