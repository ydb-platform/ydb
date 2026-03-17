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

#ifndef FROM_PROJ_CPP
#define FROM_PROJ_CPP
#endif

#include "proj/datum.hpp"
#include "proj/common.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/datum_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

// PROJ include order is sensitive
// clang-format off
#include "proj.h"
#include "proj_internal.h"
// clang-format on

#include "proj_json_streaming_writer.hpp"

#include <cmath>
#include <cstdlib>
#include <memory>
#include <string>

using namespace NS_PROJ::internal;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<NS_PROJ::datum::DatumPtr>::~nn() = default;
template<> nn<NS_PROJ::datum::DatumEnsemblePtr>::~nn() = default;
template<> nn<NS_PROJ::datum::PrimeMeridianPtr>::~nn() = default;
template<> nn<NS_PROJ::datum::EllipsoidPtr>::~nn() = default;
template<> nn<NS_PROJ::datum::GeodeticReferenceFramePtr>::~nn() = default;
template<> nn<NS_PROJ::datum::DynamicGeodeticReferenceFramePtr>::~nn() = default;
template<> nn<NS_PROJ::datum::VerticalReferenceFramePtr>::~nn() = default;
template<> nn<NS_PROJ::datum::DynamicVerticalReferenceFramePtr>::~nn() = default;
template<> nn<NS_PROJ::datum::EngineeringDatumPtr>::~nn() = default;
template<> nn<NS_PROJ::datum::TemporalDatumPtr>::~nn() = default;
template<> nn<NS_PROJ::datum::ParametricDatumPtr>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace datum {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
static util::PropertyMap createMapNameEPSGCode(const char *name, int code) {
    return util::PropertyMap()
        .set(common::IdentifiedObject::NAME_KEY, name)
        .set(metadata::Identifier::CODESPACE_KEY, metadata::Identifier::EPSG)
        .set(metadata::Identifier::CODE_KEY, code);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Datum::Private {
    util::optional<std::string> anchorDefinition{};
    std::shared_ptr<util::optional<common::Measure>> anchorEpoch =
        std::make_shared<util::optional<common::Measure>>();
    util::optional<common::DateTime> publicationDate{};
    common::IdentifiedObjectPtr conventionalRS{};

    // cppcheck-suppress functionStatic
    void exportAnchorDefinition(io::WKTFormatter *formatter) const;

    // cppcheck-suppress functionStatic
    void exportAnchorEpoch(io::WKTFormatter *formatter) const;

    // cppcheck-suppress functionStatic
    void exportAnchorDefinition(io::JSONFormatter *formatter) const;

    // cppcheck-suppress functionStatic
    void exportAnchorEpoch(io::JSONFormatter *formatter) const;
};

// ---------------------------------------------------------------------------

void Datum::Private::exportAnchorDefinition(io::WKTFormatter *formatter) const {
    if (anchorDefinition) {
        formatter->startNode(io::WKTConstants::ANCHOR, false);
        formatter->addQuotedString(*anchorDefinition);
        formatter->endNode();
    }
}

// ---------------------------------------------------------------------------

void Datum::Private::exportAnchorEpoch(io::WKTFormatter *formatter) const {
    if (anchorEpoch->has_value()) {
        formatter->startNode(io::WKTConstants::ANCHOREPOCH, false);
        const double year =
            (*anchorEpoch)->convertToUnit(common::UnitOfMeasure::YEAR);
        formatter->add(getRoundedEpochInDecimalYear(year));
        formatter->endNode();
    }
}

// ---------------------------------------------------------------------------

void Datum::Private::exportAnchorDefinition(
    io::JSONFormatter *formatter) const {
    if (anchorDefinition) {
        auto writer = formatter->writer();
        writer->AddObjKey("anchor");
        writer->Add(*anchorDefinition);
    }
}

// ---------------------------------------------------------------------------

void Datum::Private::exportAnchorEpoch(io::JSONFormatter *formatter) const {
    if (anchorEpoch->has_value()) {
        auto writer = formatter->writer();
        writer->AddObjKey("anchor_epoch");
        const double year =
            (*anchorEpoch)->convertToUnit(common::UnitOfMeasure::YEAR);
        writer->Add(getRoundedEpochInDecimalYear(year));
    }
}

//! @endcond

// ---------------------------------------------------------------------------

Datum::Datum() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

#ifdef notdef
Datum::Datum(const Datum &other)
    : ObjectUsage(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Datum::~Datum() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the anchor definition.
 *
 * A description - possibly including coordinates of an identified point or
 * points - of the relationship used to anchor a coordinate system to the
 * Earth or alternate object.
 * <ul>
 * <li>For modern geodetic reference frames the anchor may be a set of station
 * coordinates; if the reference frame is dynamic it will also include
 * coordinate velocities. For a traditional geodetic datum, this anchor may be
 * a point known as the fundamental point, which is traditionally the point
 * where the relationship between geoid and ellipsoid is defined, together
 * with a direction from that point.</li>
 * <li>For a vertical reference frame the anchor may be the zero level at one
 * or more defined locations or a conventionally defined surface.</li>
 * <li>For an engineering datum, the anchor may be an identified physical point
 * with the orientation defined relative to the object.</li>
 * </ul>
 *
 * @return the anchor definition, or empty.
 */
const util::optional<std::string> &Datum::anchorDefinition() const {
    return d->anchorDefinition;
}

// ---------------------------------------------------------------------------

/** \brief Return the anchor epoch.
 *
 * Epoch at which a static reference frame matches a dynamic reference frame
 * from which it has been derived.
 *
 * Note: Not to be confused with the frame reference epoch of dynamic geodetic
 * and dynamic vertical reference frames. Nor with the epoch at which a
 * reference frame is defined to be aligned with another reference frame;
 * this information should be included in the datum anchor definition.
 *
 * @return the anchor epoch, or empty.
 * @since 9.2
 */
const util::optional<common::Measure> &Datum::anchorEpoch() const {
    return *(d->anchorEpoch);
}

// ---------------------------------------------------------------------------

/** \brief Return the date on which the datum definition was published.
 *
 * \note Departure from \ref ISO_19111_2019 : we return a DateTime instead of
 * a Citation::Date.
 *
 * @return the publication date, or empty.
 */
const util::optional<common::DateTime> &Datum::publicationDate() const {
    return d->publicationDate;
}

// ---------------------------------------------------------------------------

/** \brief Return the conventional reference system.
 *
 * This is the name, identifier, alias and remarks for the terrestrial
 * reference system or vertical reference system realized by this reference
 * frame, for example "ITRS" for ITRF88 through ITRF2008 and ITRF2014, or
 * "EVRS" for EVRF2000 and EVRF2007.
 *
 * @return the conventional reference system, or nullptr.
 */
const common::IdentifiedObjectPtr &Datum::conventionalRS() const {
    return d->conventionalRS;
}

// ---------------------------------------------------------------------------

void Datum::setAnchor(const util::optional<std::string> &anchor) {
    d->anchorDefinition = anchor;
}

// ---------------------------------------------------------------------------

void Datum::setAnchorEpoch(const util::optional<common::Measure> &anchorEpoch) {
    d->anchorEpoch =
        std::make_shared<util::optional<common::Measure>>(anchorEpoch);
}

// ---------------------------------------------------------------------------

void Datum::setProperties(
    const util::PropertyMap &properties) // throw(InvalidValueTypeException)
{
    std::string publicationDateResult;
    properties.getStringValue("PUBLICATION_DATE", publicationDateResult);
    if (!publicationDateResult.empty()) {
        d->publicationDate = common::DateTime::create(publicationDateResult);
    }
    std::string anchorEpoch;
    properties.getStringValue("ANCHOR_EPOCH", anchorEpoch);
    if (!anchorEpoch.empty()) {
        bool success = false;
        const double anchorEpochYear = c_locale_stod(anchorEpoch, success);
        if (success) {
            setAnchorEpoch(util::optional<common::Measure>(
                common::Measure(anchorEpochYear, common::UnitOfMeasure::YEAR)));
        }
    }
    ObjectUsage::setProperties(properties);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool Datum::_isEquivalentTo(const util::IComparable *other,
                            util::IComparable::Criterion criterion,
                            const io::DatabaseContextPtr &dbContext) const {
    auto otherDatum = dynamic_cast<const Datum *>(other);
    if (otherDatum == nullptr ||
        !ObjectUsage::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    if (criterion == util::IComparable::Criterion::STRICT) {
        if ((anchorDefinition().has_value() ^
             otherDatum->anchorDefinition().has_value())) {
            return false;
        }
        if (anchorDefinition().has_value() &&
            otherDatum->anchorDefinition().has_value() &&
            *anchorDefinition() != *otherDatum->anchorDefinition()) {
            return false;
        }

        if ((publicationDate().has_value() ^
             otherDatum->publicationDate().has_value())) {
            return false;
        }
        if (publicationDate().has_value() &&
            otherDatum->publicationDate().has_value() &&
            publicationDate()->toString() !=
                otherDatum->publicationDate()->toString()) {
            return false;
        }

        if (((conventionalRS() != nullptr) ^
             (otherDatum->conventionalRS() != nullptr))) {
            return false;
        }
        if (conventionalRS() && otherDatum->conventionalRS() &&
            conventionalRS()->_isEquivalentTo(
                otherDatum->conventionalRS().get(), criterion, dbContext)) {
            return false;
        }
    }
    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct PrimeMeridian::Private {
    common::Angle longitude_{};

    explicit Private(const common::Angle &longitude) : longitude_(longitude) {}
};
//! @endcond

// ---------------------------------------------------------------------------

PrimeMeridian::PrimeMeridian(const common::Angle &longitudeIn)
    : d(std::make_unique<Private>(longitudeIn)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
PrimeMeridian::PrimeMeridian(const PrimeMeridian &other)
    : common::IdentifiedObject(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
PrimeMeridian::~PrimeMeridian() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the longitude of the prime meridian.
 *
 * It is measured from the internationally-recognised reference meridian
 * ('Greenwich meridian'), positive eastward.
 * The default value is 0 degrees.
 *
 * @return the longitude of the prime meridian.
 */
const common::Angle &PrimeMeridian::longitude() PROJ_PURE_DEFN {
    return d->longitude_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a PrimeMeridian.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param longitudeIn the longitude of the prime meridian.
 * @return new PrimeMeridian.
 */
PrimeMeridianNNPtr PrimeMeridian::create(const util::PropertyMap &properties,
                                         const common::Angle &longitudeIn) {
    auto pm(PrimeMeridian::nn_make_shared<PrimeMeridian>(longitudeIn));
    pm->setProperties(properties);
    return pm;
}

// ---------------------------------------------------------------------------

const PrimeMeridianNNPtr PrimeMeridian::createGREENWICH() {
    return create(createMapNameEPSGCode("Greenwich", 8901), common::Angle(0));
}

// ---------------------------------------------------------------------------

const PrimeMeridianNNPtr PrimeMeridian::createREFERENCE_MERIDIAN() {
    return create(util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                          "Reference meridian"),
                  common::Angle(0));
}

// ---------------------------------------------------------------------------

const PrimeMeridianNNPtr PrimeMeridian::createPARIS() {
    return create(createMapNameEPSGCode("Paris", 8903),
                  common::Angle(2.5969213, common::UnitOfMeasure::GRAD));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void PrimeMeridian::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    std::string l_name(name()->description().has_value() ? nameStr()
                                                         : "Greenwich");
    if (!(isWKT2 && formatter->primeMeridianOmittedIfGreenwich() &&
          l_name == "Greenwich")) {
        formatter->startNode(io::WKTConstants::PRIMEM, !identifiers().empty());

        if (formatter->useESRIDialect()) {
            bool aliasFound = false;
            const auto &dbContext = formatter->databaseContext();
            if (dbContext) {
                auto l_alias = dbContext->getAliasFromOfficialName(
                    l_name, "prime_meridian", "ESRI");
                if (!l_alias.empty()) {
                    l_name = std::move(l_alias);
                    aliasFound = true;
                }
            }
            if (!aliasFound && dbContext) {
                auto authFactory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), "ESRI");
                aliasFound =
                    authFactory
                        ->createObjectsFromName(
                            l_name,
                            {io::AuthorityFactory::ObjectType::PRIME_MERIDIAN},
                            false // approximateMatch
                            )
                        .size() == 1;
            }
            if (!aliasFound) {
                l_name = io::WKTFormatter::morphNameToESRI(l_name);
            }
        }

        formatter->addQuotedString(l_name);
        const auto &l_long = longitude();
        if (formatter->primeMeridianInDegree()) {
            formatter->add(l_long.convertToUnit(common::UnitOfMeasure::DEGREE));
        } else {
            formatter->add(l_long.value());
        }
        const auto &unit = l_long.unit();
        if (isWKT2) {
            if (!(formatter
                      ->primeMeridianOrParameterUnitOmittedIfSameAsAxis() &&
                  unit == *(formatter->axisAngularUnit()))) {
                unit._exportToWKT(formatter, io::WKTConstants::ANGLEUNIT);
            }
        } else if (!formatter->primeMeridianInDegree()) {
            unit._exportToWKT(formatter);
        }
        if (formatter->outputId()) {
            formatID(formatter);
        }
        formatter->endNode();
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void PrimeMeridian::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("PrimeMeridian", !identifiers().empty()));

    writer->AddObjKey("name");
    std::string l_name =
        name()->description().has_value() ? nameStr() : "Greenwich";
    writer->Add(l_name);

    const auto &l_long = longitude();
    writer->AddObjKey("longitude");
    const auto &unit = l_long.unit();
    if (unit == common::UnitOfMeasure::DEGREE) {
        writer->Add(l_long.value(), 15);
    } else {
        auto longitudeContext(formatter->MakeObjectContext(nullptr, false));
        writer->AddObjKey("value");
        writer->Add(l_long.value(), 15);
        writer->AddObjKey("unit");
        unit._exportToJSON(formatter);
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::string
PrimeMeridian::getPROJStringWellKnownName(const common::Angle &angle) {
    const double valRad = angle.getSIValue();
    std::string projPMName;
    PJ_CONTEXT *ctxt = proj_context_create();
    auto proj_pm = proj_list_prime_meridians();
    for (int i = 0; proj_pm[i].id != nullptr; ++i) {
        double valRefRad = dmstor_ctx(ctxt, proj_pm[i].defn, nullptr);
        if (::fabs(valRad - valRefRad) < 1e-10) {
            projPMName = proj_pm[i].id;
            break;
        }
    }
    proj_context_destroy(ctxt);
    return projPMName;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void PrimeMeridian::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(FormattingException)
{
    if (longitude().getSIValue() != 0) {
        std::string projPMName(getPROJStringWellKnownName(longitude()));
        if (!projPMName.empty()) {
            formatter->addParam("pm", projPMName);
        } else {
            const double valDeg =
                longitude().convertToUnit(common::UnitOfMeasure::DEGREE);
            formatter->addParam("pm", valDeg);
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool PrimeMeridian::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherPM = dynamic_cast<const PrimeMeridian *>(other);
    if (otherPM == nullptr ||
        !IdentifiedObject::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    // In MapInfo, the Paris prime meridian is returned as 2.3372291666667
    // instead of the official value of 2.33722917, which is a relative
    // error in the 1e-9 range.
    return longitude()._isEquivalentTo(otherPM->longitude(), criterion, 1e-8);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Ellipsoid::Private {
    common::Length semiMajorAxis_{};
    util::optional<common::Scale> inverseFlattening_{};
    util::optional<common::Length> semiMinorAxis_{};
    util::optional<common::Length> semiMedianAxis_{};
    std::string celestialBody_{};

    explicit Private(const common::Length &radius,
                     const std::string &celestialBody)
        : semiMajorAxis_(radius), celestialBody_(celestialBody) {}

    Private(const common::Length &semiMajorAxisIn,
            const common::Scale &invFlattening,
            const std::string &celestialBody)
        : semiMajorAxis_(semiMajorAxisIn), inverseFlattening_(invFlattening),
          celestialBody_(celestialBody) {}

    Private(const common::Length &semiMajorAxisIn,
            const common::Length &semiMinorAxisIn,
            const std::string &celestialBody)
        : semiMajorAxis_(semiMajorAxisIn), semiMinorAxis_(semiMinorAxisIn),
          celestialBody_(celestialBody) {}
};
//! @endcond

// ---------------------------------------------------------------------------

Ellipsoid::Ellipsoid(const common::Length &radius,
                     const std::string &celestialBodyIn)
    : d(std::make_unique<Private>(radius, celestialBodyIn)) {}

// ---------------------------------------------------------------------------

Ellipsoid::Ellipsoid(const common::Length &semiMajorAxisIn,
                     const common::Scale &invFlattening,
                     const std::string &celestialBodyIn)
    : d(std::make_unique<Private>(semiMajorAxisIn, invFlattening,
                                  celestialBodyIn)) {}

// ---------------------------------------------------------------------------

Ellipsoid::Ellipsoid(const common::Length &semiMajorAxisIn,
                     const common::Length &semiMinorAxisIn,
                     const std::string &celestialBodyIn)
    : d(std::make_unique<Private>(semiMajorAxisIn, semiMinorAxisIn,
                                  celestialBodyIn)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
Ellipsoid::Ellipsoid(const Ellipsoid &other)
    : common::IdentifiedObject(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Ellipsoid::~Ellipsoid() = default;

Ellipsoid::Ellipsoid(const Ellipsoid &other)
    : IdentifiedObject(other), d(std::make_unique<Private>(*(other.d))) {}

//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the length of the semi-major axis of the ellipsoid.
 *
 * @return the semi-major axis.
 */
const common::Length &Ellipsoid::semiMajorAxis() PROJ_PURE_DEFN {
    return d->semiMajorAxis_;
}

// ---------------------------------------------------------------------------

/** \brief Return the inverse flattening value of the ellipsoid, if the
 * ellipsoid
 * has been defined with this value.
 *
 * @see computeInverseFlattening() that will always return a valid value of the
 * inverse flattening, whether the ellipsoid has been defined through inverse
 * flattening or semi-minor axis.
 *
 * @return the inverse flattening value of the ellipsoid, or empty.
 */
const util::optional<common::Scale> &
Ellipsoid::inverseFlattening() PROJ_PURE_DEFN {
    return d->inverseFlattening_;
}

// ---------------------------------------------------------------------------

/** \brief Return the length of the semi-minor axis of the ellipsoid, if the
 * ellipsoid
 * has been defined with this value.
 *
 * @see computeSemiMinorAxis() that will always return a valid value of the
 * semi-minor axis, whether the ellipsoid has been defined through inverse
 * flattening or semi-minor axis.
 *
 * @return the semi-minor axis of the ellipsoid, or empty.
 */
const util::optional<common::Length> &
Ellipsoid::semiMinorAxis() PROJ_PURE_DEFN {
    return d->semiMinorAxis_;
}

// ---------------------------------------------------------------------------

/** \brief Return whether the ellipsoid is spherical.
 *
 * That is to say is semiMajorAxis() == computeSemiMinorAxis().
 *
 * A sphere is completely defined by the semi-major axis, which is the radius
 * of the sphere.
 *
 * @return true if the ellipsoid is spherical.
 */
bool Ellipsoid::isSphere() PROJ_PURE_DEFN {
    if (d->inverseFlattening_.has_value()) {
        return d->inverseFlattening_->value() == 0;
    }

    if (semiMinorAxis().has_value()) {
        return semiMajorAxis() == *semiMinorAxis();
    }

    return true;
}

// ---------------------------------------------------------------------------

/** \brief Return the length of the semi-median axis of a triaxial ellipsoid
 *
 * This parameter is not required for a biaxial ellipsoid.
 *
 * @return the semi-median axis of the ellipsoid, or empty.
 */
const util::optional<common::Length> &
Ellipsoid::semiMedianAxis() PROJ_PURE_DEFN {
    return d->semiMedianAxis_;
}

// ---------------------------------------------------------------------------

/** \brief Return or compute the inverse flattening value of the ellipsoid.
 *
 * If computed, the inverse flattening is the result of a / (a - b),
 * where a is the semi-major axis and b the semi-minor axis.
 *
 * @return the inverse flattening value of the ellipsoid, or 0 for a sphere.
 */
double Ellipsoid::computedInverseFlattening() PROJ_PURE_DEFN {
    if (d->inverseFlattening_.has_value()) {
        return d->inverseFlattening_->getSIValue();
    }

    if (d->semiMinorAxis_.has_value()) {
        const double a = d->semiMajorAxis_.getSIValue();
        const double b = d->semiMinorAxis_->getSIValue();
        return (a == b) ? 0.0 : a / (a - b);
    }

    return 0.0;
}

// ---------------------------------------------------------------------------

/** \brief Return the squared eccentricity of the ellipsoid.
 *
 * @return the squared eccentricity, or a negative value if invalid.
 */
double Ellipsoid::squaredEccentricity() PROJ_PURE_DEFN {
    const double rf = computedInverseFlattening();
    // coverity[divide_by_zero]
    const double f = rf != 0.0 ? 1. / rf : 0.0;
    const double e2 = f * (2 - f);
    return e2;
}

// ---------------------------------------------------------------------------

/** \brief Return or compute the length of the semi-minor axis of the ellipsoid.
 *
 * If computed, the semi-minor axis is the result of a * (1 - 1 / rf)
 * where a is the semi-major axis and rf the reverse/inverse flattening.

 * @return the semi-minor axis of the ellipsoid.
 */
common::Length Ellipsoid::computeSemiMinorAxis() const {
    if (d->semiMinorAxis_.has_value()) {
        return *d->semiMinorAxis_;
    }

    if (inverseFlattening().has_value()) {
        return common::Length(
            (1.0 - 1.0 / d->inverseFlattening_->getSIValue()) *
                d->semiMajorAxis_.value(),
            d->semiMajorAxis_.unit());
    }

    return d->semiMajorAxis_;
}

// ---------------------------------------------------------------------------

/** \brief Return the name of the celestial body on which the ellipsoid refers
 * to.
 */
const std::string &Ellipsoid::celestialBody() PROJ_PURE_DEFN {
    return d->celestialBody_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Ellipsoid as a sphere.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param radius the sphere radius (semi-major axis).
 * @param celestialBody Name of the celestial body on which the ellipsoid refers
 * to.
 * @return new Ellipsoid.
 */
EllipsoidNNPtr Ellipsoid::createSphere(const util::PropertyMap &properties,
                                       const common::Length &radius,
                                       const std::string &celestialBody) {
    auto ellipsoid(Ellipsoid::nn_make_shared<Ellipsoid>(radius, celestialBody));
    ellipsoid->setProperties(properties);
    return ellipsoid;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Ellipsoid from its inverse/reverse flattening.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param semiMajorAxisIn the semi-major axis.
 * @param invFlattening the inverse/reverse flattening. If set to 0, this will
 * be considered as a sphere.
 * @param celestialBody Name of the celestial body on which the ellipsoid refers
 * to.
 * @return new Ellipsoid.
 */
EllipsoidNNPtr Ellipsoid::createFlattenedSphere(
    const util::PropertyMap &properties, const common::Length &semiMajorAxisIn,
    const common::Scale &invFlattening, const std::string &celestialBody) {
    if (invFlattening.value() == 0) {
        auto ellipsoid(Ellipsoid::nn_make_shared<Ellipsoid>(semiMajorAxisIn,
                                                            celestialBody));
        ellipsoid->setProperties(properties);
        return ellipsoid;
    } else {
        auto ellipsoid(Ellipsoid::nn_make_shared<Ellipsoid>(
            semiMajorAxisIn, invFlattening, celestialBody));
        ellipsoid->setProperties(properties);
        return ellipsoid;
    }
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Ellipsoid from the value of its two semi axis.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param semiMajorAxisIn the semi-major axis.
 * @param semiMinorAxisIn the semi-minor axis.
 * @param celestialBody Name of the celestial body on which the ellipsoid refers
 * to.
 * @return new Ellipsoid.
 */
EllipsoidNNPtr Ellipsoid::createTwoAxis(const util::PropertyMap &properties,
                                        const common::Length &semiMajorAxisIn,
                                        const common::Length &semiMinorAxisIn,
                                        const std::string &celestialBody) {
    auto ellipsoid(Ellipsoid::nn_make_shared<Ellipsoid>(
        semiMajorAxisIn, semiMinorAxisIn, celestialBody));
    ellipsoid->setProperties(properties);
    return ellipsoid;
}

// ---------------------------------------------------------------------------

const EllipsoidNNPtr Ellipsoid::createCLARKE_1866() {
    return createTwoAxis(createMapNameEPSGCode("Clarke 1866", 7008),
                         common::Length(6378206.4), common::Length(6356583.8));
}

// ---------------------------------------------------------------------------

const EllipsoidNNPtr Ellipsoid::createWGS84() {
    return createFlattenedSphere(createMapNameEPSGCode("WGS 84", 7030),
                                 common::Length(6378137),
                                 common::Scale(298.257223563));
}

// ---------------------------------------------------------------------------

const EllipsoidNNPtr Ellipsoid::createGRS1980() {
    return createFlattenedSphere(createMapNameEPSGCode("GRS 1980", 7019),
                                 common::Length(6378137),
                                 common::Scale(298.257222101));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Ellipsoid::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(isWKT2 ? io::WKTConstants::ELLIPSOID
                                : io::WKTConstants::SPHEROID,
                         !identifiers().empty());
    {
        std::string l_name(nameStr());
        if (l_name.empty()) {
            formatter->addQuotedString("unnamed");
        } else {
            if (formatter->useESRIDialect()) {
                if (l_name == "WGS 84") {
                    l_name = "WGS_1984";
                } else {
                    bool aliasFound = false;
                    const auto &dbContext = formatter->databaseContext();
                    if (dbContext) {
                        auto l_alias = dbContext->getAliasFromOfficialName(
                            l_name, "ellipsoid", "ESRI");
                        if (!l_alias.empty()) {
                            l_name = std::move(l_alias);
                            aliasFound = true;
                        }
                    }
                    if (!aliasFound && dbContext) {
                        auto authFactory = io::AuthorityFactory::create(
                            NN_NO_CHECK(dbContext), "ESRI");
                        aliasFound = authFactory
                                         ->createObjectsFromName(
                                             l_name,
                                             {io::AuthorityFactory::ObjectType::
                                                  ELLIPSOID},
                                             false // approximateMatch
                                             )
                                         .size() == 1;
                    }
                    if (!aliasFound) {
                        l_name = io::WKTFormatter::morphNameToESRI(l_name);
                    }
                }
            }
            formatter->addQuotedString(l_name);
        }
        const auto &semiMajor = semiMajorAxis();
        if (isWKT2) {
            formatter->add(semiMajor.value());
        } else {
            formatter->add(semiMajor.getSIValue());
        }
        formatter->add(computedInverseFlattening());
        const auto &unit = semiMajor.unit();
        if (isWKT2 && !(formatter->ellipsoidUnitOmittedIfMetre() &&
                        unit == common::UnitOfMeasure::METRE)) {
            unit._exportToWKT(formatter, io::WKTConstants::LENGTHUNIT);
        }
        if (formatter->outputId()) {
            formatID(formatter);
        }
    }
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Ellipsoid::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("Ellipsoid", !identifiers().empty()));

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    const auto &semiMajor = semiMajorAxis();
    const auto &semiMajorUnit = semiMajor.unit();
    writer->AddObjKey(isSphere() ? "radius" : "semi_major_axis");
    if (semiMajorUnit == common::UnitOfMeasure::METRE) {
        writer->Add(semiMajor.value(), 15);
    } else {
        auto objContext(formatter->MakeObjectContext(nullptr, false));
        writer->AddObjKey("value");
        writer->Add(semiMajor.value(), 15);

        writer->AddObjKey("unit");
        semiMajorUnit._exportToJSON(formatter);
    }

    if (!isSphere()) {
        const auto &l_inverseFlattening = inverseFlattening();
        if (l_inverseFlattening.has_value()) {
            writer->AddObjKey("inverse_flattening");
            writer->Add(l_inverseFlattening->getSIValue(), 15);
        } else {
            writer->AddObjKey("semi_minor_axis");
            const auto &l_semiMinorAxis(semiMinorAxis());
            const auto &semiMinorAxisUnit(l_semiMinorAxis->unit());
            if (semiMinorAxisUnit == common::UnitOfMeasure::METRE) {
                writer->Add(l_semiMinorAxis->value(), 15);
            } else {
                auto objContext(formatter->MakeObjectContext(nullptr, false));
                writer->AddObjKey("value");
                writer->Add(l_semiMinorAxis->value(), 15);

                writer->AddObjKey("unit");
                semiMinorAxisUnit._exportToJSON(formatter);
            }
        }
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

bool Ellipsoid::lookForProjWellKnownEllps(std::string &projEllpsName,
                                          std::string &ellpsName) const {
    const double a = semiMajorAxis().getSIValue();
    const double b = computeSemiMinorAxis().getSIValue();
    const double rf = computedInverseFlattening();
    auto proj_ellps = proj_list_ellps();
    for (int i = 0; proj_ellps[i].id != nullptr; i++) {
        assert(strncmp(proj_ellps[i].major, "a=", 2) == 0);
        const double a_iter = c_locale_stod(proj_ellps[i].major + 2);
        if (::fabs(a - a_iter) < 1e-10 * a_iter) {
            if (strncmp(proj_ellps[i].ell, "b=", 2) == 0) {
                const double b_iter = c_locale_stod(proj_ellps[i].ell + 2);
                if (::fabs(b - b_iter) < 1e-10 * b_iter) {
                    projEllpsName = proj_ellps[i].id;
                    ellpsName = proj_ellps[i].name;
                    if (starts_with(ellpsName, "GRS 1980")) {
                        ellpsName = "GRS 1980";
                    }
                    return true;
                }
            } else {
                assert(strncmp(proj_ellps[i].ell, "rf=", 3) == 0);
                const double rf_iter = c_locale_stod(proj_ellps[i].ell + 3);
                if (::fabs(rf - rf_iter) < 1e-10 * rf_iter) {
                    projEllpsName = proj_ellps[i].id;
                    ellpsName = proj_ellps[i].name;
                    if (starts_with(ellpsName, "GRS 1980")) {
                        ellpsName = "GRS 1980";
                    }
                    return true;
                }
            }
        }
    }
    return false;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Ellipsoid::_exportToPROJString(
    io::PROJStringFormatter *formatter) const // throw(FormattingException)
{
    const double a = semiMajorAxis().getSIValue();

    std::string projEllpsName;
    std::string ellpsName;
    if (lookForProjWellKnownEllps(projEllpsName, ellpsName)) {
        formatter->addParam("ellps", projEllpsName);
        return;
    }

    if (isSphere()) {
        formatter->addParam("R", a);
    } else {
        formatter->addParam("a", a);
        if (inverseFlattening().has_value()) {
            const double rf = computedInverseFlattening();
            formatter->addParam("rf", rf);
        } else {
            const double b = computeSemiMinorAxis().getSIValue();
            formatter->addParam("b", b);
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return a Ellipsoid object where some parameters are better
 * identified.
 *
 * @return a new Ellipsoid.
 */
EllipsoidNNPtr Ellipsoid::identify() const {
    auto newEllipsoid = Ellipsoid::nn_make_shared<Ellipsoid>(*this);
    newEllipsoid->assignSelf(
        util::nn_static_pointer_cast<util::BaseObject>(newEllipsoid));

    if (name()->description()->empty() || nameStr() == "unknown") {
        std::string projEllpsName;
        std::string ellpsName;
        if (lookForProjWellKnownEllps(projEllpsName, ellpsName)) {
            newEllipsoid->setProperties(
                util::PropertyMap().set(IdentifiedObject::NAME_KEY, ellpsName));
        }
    }

    return newEllipsoid;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool Ellipsoid::_isEquivalentTo(const util::IComparable *other,
                                util::IComparable::Criterion criterion,
                                const io::DatabaseContextPtr &dbContext) const {
    auto otherEllipsoid = dynamic_cast<const Ellipsoid *>(other);
    if (otherEllipsoid == nullptr ||
        (criterion == util::IComparable::Criterion::STRICT &&
         !IdentifiedObject::_isEquivalentTo(other, criterion, dbContext))) {
        return false;
    }

    // PROJ "clrk80" name is "Clarke 1880 mod." and GDAL tends to
    // export to it a number of Clarke 1880 variants, so be lax
    if (criterion != util::IComparable::Criterion::STRICT &&
        (nameStr() == "Clarke 1880 mod." ||
         otherEllipsoid->nameStr() == "Clarke 1880 mod.")) {
        return std::fabs(semiMajorAxis().getSIValue() -
                         otherEllipsoid->semiMajorAxis().getSIValue()) <
                   1e-8 * semiMajorAxis().getSIValue() &&
               std::fabs(computedInverseFlattening() -
                         otherEllipsoid->computedInverseFlattening()) <
                   1e-5 * computedInverseFlattening();
    }

    if (!semiMajorAxis()._isEquivalentTo(otherEllipsoid->semiMajorAxis(),
                                         criterion)) {
        return false;
    }

    const auto &l_semiMinorAxis = semiMinorAxis();
    const auto &l_other_semiMinorAxis = otherEllipsoid->semiMinorAxis();
    if (l_semiMinorAxis.has_value() && l_other_semiMinorAxis.has_value()) {
        if (!l_semiMinorAxis->_isEquivalentTo(*l_other_semiMinorAxis,
                                              criterion)) {
            return false;
        }
    }

    const auto &l_inverseFlattening = inverseFlattening();
    const auto &l_other_sinverseFlattening =
        otherEllipsoid->inverseFlattening();
    if (l_inverseFlattening.has_value() &&
        l_other_sinverseFlattening.has_value()) {
        if (!l_inverseFlattening->_isEquivalentTo(*l_other_sinverseFlattening,
                                                  criterion)) {
            return false;
        }
    }

    if (criterion == util::IComparable::Criterion::STRICT) {
        if ((l_semiMinorAxis.has_value() ^ l_other_semiMinorAxis.has_value())) {
            return false;
        }

        if ((l_inverseFlattening.has_value() ^
             l_other_sinverseFlattening.has_value())) {
            return false;
        }

    } else {
        if (!computeSemiMinorAxis()._isEquivalentTo(
                otherEllipsoid->computeSemiMinorAxis(), criterion)) {
            return false;
        }
    }

    const auto &l_semiMedianAxis = semiMedianAxis();
    const auto &l_other_semiMedianAxis = otherEllipsoid->semiMedianAxis();
    if ((l_semiMedianAxis.has_value() ^ l_other_semiMedianAxis.has_value())) {
        return false;
    }
    if (l_semiMedianAxis.has_value() && l_other_semiMedianAxis.has_value()) {
        if (!l_semiMedianAxis->_isEquivalentTo(*l_other_semiMedianAxis,
                                               criterion)) {
            return false;
        }
    }
    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

std::string Ellipsoid::guessBodyName(const io::DatabaseContextPtr &dbContext,
                                     double a, const std::string &ellpsName) {
    constexpr double earthMeanRadius = 6375000.0;
    if (std::fabs(a - earthMeanRadius) <
        REL_ERROR_FOR_SAME_CELESTIAL_BODY * earthMeanRadius) {
        return Ellipsoid::EARTH;
    }
    if (dbContext) {
        try {
            auto factory = io::AuthorityFactory::create(NN_NO_CHECK(dbContext),
                                                        std::string());
            if (!ellpsName.empty()) {
                auto matches = factory->createObjectsFromName(
                    ellpsName, {io::AuthorityFactory::ObjectType::ELLIPSOID},
                    true, 1);
                if (!matches.empty()) {
                    auto ellps =
                        static_cast<const Ellipsoid *>(matches.front().get());
                    if (std::fabs(a - ellps->semiMajorAxis().getSIValue()) <
                        REL_ERROR_FOR_SAME_CELESTIAL_BODY * a) {
                        return ellps->celestialBody();
                    }
                }
            }
            return factory->identifyBodyFromSemiMajorAxis(
                a, REL_ERROR_FOR_SAME_CELESTIAL_BODY);
        } catch (const std::exception &) {
        }
    }
    return NON_EARTH_BODY;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct GeodeticReferenceFrame::Private {
    PrimeMeridianNNPtr primeMeridian_;
    EllipsoidNNPtr ellipsoid_;

    Private(const EllipsoidNNPtr &ellipsoidIn,
            const PrimeMeridianNNPtr &primeMeridianIn)
        : primeMeridian_(primeMeridianIn), ellipsoid_(ellipsoidIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

GeodeticReferenceFrame::GeodeticReferenceFrame(
    const EllipsoidNNPtr &ellipsoidIn,
    const PrimeMeridianNNPtr &primeMeridianIn)
    : d(std::make_unique<Private>(ellipsoidIn, primeMeridianIn)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
GeodeticReferenceFrame::GeodeticReferenceFrame(
    const GeodeticReferenceFrame &other)
    : Datum(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
GeodeticReferenceFrame::~GeodeticReferenceFrame() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the PrimeMeridian associated with a GeodeticReferenceFrame.
 *
 * @return the PrimeMeridian.
 */
const PrimeMeridianNNPtr &
GeodeticReferenceFrame::primeMeridian() PROJ_PURE_DEFN {
    return d->primeMeridian_;
}

// ---------------------------------------------------------------------------

/** \brief Return the Ellipsoid associated with a GeodeticReferenceFrame.
 *
 * \note The \ref ISO_19111_2019 modelling allows (but discourages) a
 * GeodeticReferenceFrame
 * to not be associated with a Ellipsoid in the case where it is used by a
 * geocentric crs::GeodeticCRS. We have made the choice of making the ellipsoid
 * specification compulsory.
 *
 * @return the Ellipsoid.
 */
const EllipsoidNNPtr &GeodeticReferenceFrame::ellipsoid() PROJ_PURE_DEFN {
    return d->ellipsoid_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param ellipsoid the Ellipsoid.
 * @param anchor the anchor definition, or empty.
 * @param primeMeridian the PrimeMeridian.
 * @return new GeodeticReferenceFrame.
 */
GeodeticReferenceFrameNNPtr
GeodeticReferenceFrame::create(const util::PropertyMap &properties,
                               const EllipsoidNNPtr &ellipsoid,
                               const util::optional<std::string> &anchor,
                               const PrimeMeridianNNPtr &primeMeridian) {
    GeodeticReferenceFrameNNPtr grf(
        GeodeticReferenceFrame::nn_make_shared<GeodeticReferenceFrame>(
            ellipsoid, primeMeridian));
    grf->setAnchor(anchor);
    grf->setProperties(properties);
    return grf;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a GeodeticReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param ellipsoid the Ellipsoid.
 * @param anchor the anchor definition, or empty.
 * @param anchorEpoch the anchor epoch, or empty.
 * @param primeMeridian the PrimeMeridian.
 * @return new GeodeticReferenceFrame.
 * @since 9.2
 */
GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::create(
    const util::PropertyMap &properties, const EllipsoidNNPtr &ellipsoid,
    const util::optional<std::string> &anchor,
    const util::optional<common::Measure> &anchorEpoch,
    const PrimeMeridianNNPtr &primeMeridian) {
    GeodeticReferenceFrameNNPtr grf(
        GeodeticReferenceFrame::nn_make_shared<GeodeticReferenceFrame>(
            ellipsoid, primeMeridian));
    grf->setAnchor(anchor);
    grf->setAnchorEpoch(anchorEpoch);
    grf->setProperties(properties);
    return grf;
}

// ---------------------------------------------------------------------------

const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::createEPSG_6267() {
    return create(createMapNameEPSGCode("North American Datum 1927", 6267),
                  Ellipsoid::CLARKE_1866, util::optional<std::string>(),
                  PrimeMeridian::GREENWICH);
}

// ---------------------------------------------------------------------------

const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::createEPSG_6269() {
    return create(createMapNameEPSGCode("North American Datum 1983", 6269),
                  Ellipsoid::GRS1980, util::optional<std::string>(),
                  PrimeMeridian::GREENWICH);
}

// ---------------------------------------------------------------------------

const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::createEPSG_6326() {
    return create(createMapNameEPSGCode("World Geodetic System 1984", 6326),
                  Ellipsoid::WGS84, util::optional<std::string>(),
                  PrimeMeridian::GREENWICH);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticReferenceFrame::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    const auto &ids = identifiers();
    formatter->startNode(io::WKTConstants::DATUM, !ids.empty());
    std::string l_name(nameStr());
    if (l_name.empty()) {
        l_name = "unnamed";
    }
    if (!isWKT2) {
        if (formatter->useESRIDialect()) {
            if (l_name == "World Geodetic System 1984") {
                l_name = "D_WGS_1984";
            } else {
                bool aliasFound = false;
                const auto &dbContext = formatter->databaseContext();
                if (dbContext) {
                    auto l_alias = dbContext->getAliasFromOfficialName(
                        l_name, "geodetic_datum", "ESRI");
                    size_t pos;
                    if (!l_alias.empty()) {
                        l_name = std::move(l_alias);
                        aliasFound = true;
                    } else if ((pos = l_name.find(" (")) != std::string::npos) {
                        l_alias = dbContext->getAliasFromOfficialName(
                            l_name.substr(0, pos), "geodetic_datum", "ESRI");
                        if (!l_alias.empty()) {
                            l_name = std::move(l_alias);
                            aliasFound = true;
                        }
                    }
                }
                if (!aliasFound && dbContext) {
                    auto authFactory = io::AuthorityFactory::create(
                        NN_NO_CHECK(dbContext), "ESRI");
                    aliasFound = authFactory
                                     ->createObjectsFromName(
                                         l_name,
                                         {io::AuthorityFactory::ObjectType::
                                              GEODETIC_REFERENCE_FRAME},
                                         false // approximateMatch
                                         )
                                     .size() == 1;
                }
                if (!aliasFound) {
                    // For now, there's no ESRI alias for this CRS. Fallback to
                    // ETRS89
                    if (l_name == "ETRS89-NOR [EUREF89]") {
                        l_name = "D_ETRS_1989";
                    } else {
                        l_name = io::WKTFormatter::morphNameToESRI(l_name);
                        if (!starts_with(l_name, "D_")) {
                            l_name = "D_" + l_name;
                        }
                    }
                }
            }
        } else {
            // Replace spaces by underscore for datum names coming from EPSG
            // so as to emulate GDAL < 3 importFromEPSG()
            if (ids.size() == 1 && *(ids.front()->codeSpace()) == "EPSG") {
                l_name = io::WKTFormatter::morphNameToESRI(l_name);
            } else if (ids.empty()) {
                const auto &dbContext = formatter->databaseContext();
                if (dbContext) {
                    auto factory = io::AuthorityFactory::create(
                        NN_NO_CHECK(dbContext), std::string());
                    // We use anonymous authority and approximate matching, so
                    // as to trigger the caching done in createObjectsFromName()
                    // in that case.
                    auto matches = factory->createObjectsFromName(
                        l_name,
                        {io::AuthorityFactory::ObjectType::
                             GEODETIC_REFERENCE_FRAME},
                        true, 2);
                    if (matches.size() == 1) {
                        const auto &match = matches.front();
                        const auto &matchId = match->identifiers();
                        if (matchId.size() == 1 &&
                            *(matchId.front()->codeSpace()) == "EPSG" &&
                            metadata::Identifier::isEquivalentName(
                                l_name.c_str(), match->nameStr().c_str())) {
                            l_name = io::WKTFormatter::morphNameToESRI(l_name);
                        }
                    }
                }
            }
            if (l_name == "World_Geodetic_System_1984") {
                l_name = "WGS_1984";
            }
        }
    }
    formatter->addQuotedString(l_name);

    ellipsoid()->_exportToWKT(formatter);
    if (isWKT2) {
        Datum::getPrivate()->exportAnchorDefinition(formatter);
        if (formatter->use2019Keywords()) {
            Datum::getPrivate()->exportAnchorEpoch(formatter);
        }
    } else {
        const auto &TOWGS84Params = formatter->getTOWGS84Parameters();
        if (TOWGS84Params.size() == 7) {
            formatter->startNode(io::WKTConstants::TOWGS84, false);
            for (const auto &val : TOWGS84Params) {
                formatter->add(val, 12);
            }
            formatter->endNode();
        }
        std::string extension = formatter->getHDatumExtension();
        if (!extension.empty()) {
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4_GRIDS");
            formatter->addQuotedString(extension);
            formatter->endNode();
        }
    }
    if (formatter->outputId()) {
        formatID(formatter);
    }
    // the PRIMEM is exported as a child of the CRS
    formatter->endNode();

    if (formatter->isAtTopLevel()) {
        const auto &l_primeMeridian(primeMeridian());
        if (l_primeMeridian->nameStr() != "Greenwich") {
            l_primeMeridian->_exportToWKT(formatter);
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void GeodeticReferenceFrame::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto dynamicGRF = dynamic_cast<const DynamicGeodeticReferenceFrame *>(this);

    auto objectContext(formatter->MakeObjectContext(
        dynamicGRF ? "DynamicGeodeticReferenceFrame" : "GeodeticReferenceFrame",
        !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    Datum::getPrivate()->exportAnchorDefinition(formatter);
    Datum::getPrivate()->exportAnchorEpoch(formatter);

    if (dynamicGRF) {
        writer->AddObjKey("frame_reference_epoch");
        writer->Add(dynamicGRF->frameReferenceEpoch().value());
    }

    writer->AddObjKey("ellipsoid");
    formatter->setOmitTypeInImmediateChild();
    ellipsoid()->_exportToJSON(formatter);

    const auto &l_primeMeridian(primeMeridian());
    if (l_primeMeridian->nameStr() != "Greenwich") {
        writer->AddObjKey("prime_meridian");
        formatter->setOmitTypeInImmediateChild();
        primeMeridian()->_exportToJSON(formatter);
    }

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

bool GeodeticReferenceFrame::isEquivalentToNoExactTypeCheck(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherGRF = dynamic_cast<const GeodeticReferenceFrame *>(other);
    if (otherGRF == nullptr ||
        !Datum::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    return primeMeridian()->_isEquivalentTo(otherGRF->primeMeridian().get(),
                                            criterion, dbContext) &&
           ellipsoid()->_isEquivalentTo(otherGRF->ellipsoid().get(), criterion,
                                        dbContext);
}

// ---------------------------------------------------------------------------

bool GeodeticReferenceFrame::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (criterion == Criterion::STRICT &&
        !util::isOfExactType<GeodeticReferenceFrame>(*other)) {
        return false;
    }
    return isEquivalentToNoExactTypeCheck(other, criterion, dbContext);
}

//! @endcond

// ---------------------------------------------------------------------------

bool GeodeticReferenceFrame::hasEquivalentNameToUsingAlias(
    const IdentifiedObject *other,
    const io::DatabaseContextPtr &dbContext) const {

    const auto compare = [this, other,
                          &dbContext](const std::string &thisName,
                                      const std::string &otherName) {
        if (thisName == otherName || thisName == "unknown" ||
            otherName == "unknown") {
            return true;
        }

        if (ci_starts_with(thisName, UNKNOWN_BASED_ON) ||
            ci_starts_with(otherName, UNKNOWN_BASED_ON)) {
            // Note: they cannot be equal based on initial test.
            return false;
        }

        if (dbContext) {
            if (!identifiers().empty()) {
                const auto &id = identifiers().front();

                const std::string officialNameFromId = dbContext->getName(
                    "geodetic_datum", *(id->codeSpace()), id->code());
                const auto aliasesResult = dbContext->getAliases(
                    *(id->codeSpace()), id->code(), thisName, "geodetic_datum",
                    std::string());

                const auto isNameMatching =
                    [&aliasesResult,
                     &officialNameFromId](const std::string &name) {
                        const char *nameCstr = name.c_str();
                        if (metadata::Identifier::isEquivalentName(
                                nameCstr, officialNameFromId.c_str())) {
                            return true;
                        } else {
                            for (const auto &aliasResult : aliasesResult) {
                                if (metadata::Identifier::isEquivalentName(
                                        nameCstr, aliasResult.c_str())) {
                                    return true;
                                }
                            }
                        }
                        return false;
                    };

                return isNameMatching(thisName) && isNameMatching(otherName);
            } else if (!other->identifiers().empty()) {
                auto otherGRF =
                    dynamic_cast<const GeodeticReferenceFrame *>(other);
                if (otherGRF) {
                    return otherGRF->hasEquivalentNameToUsingAlias(this,
                                                                   dbContext);
                }
                return false;
            }

            auto aliasesResult =
                dbContext->getAliases(std::string(), std::string(), thisName,
                                      "geodetic_datum", std::string());
            const char *otherNamePtr = otherName.c_str();
            for (const auto &aliasResult : aliasesResult) {
                if (metadata::Identifier::isEquivalentName(
                        otherNamePtr, aliasResult.c_str())) {
                    return true;
                }
            }
        }
        return false;
    };

    // Try to work around issues with Esri style "D_" name prefixing
    // Cf https://github.com/OSGeo/PROJ/issues/4514
    const bool thisStartsWithDUnderscore = ci_starts_with(nameStr(), "D_");
    const bool otherStartsWithDUnderscore =
        ci_starts_with(other->nameStr(), "D_");
    if (thisStartsWithDUnderscore && !otherStartsWithDUnderscore) {
        const std::string thisNameMod = nameStr().substr(2);
        return metadata::Identifier::isEquivalentName(
                   thisNameMod.c_str(), other->nameStr().c_str()) ||
               compare(thisNameMod, other->nameStr());
    } else if (!thisStartsWithDUnderscore && otherStartsWithDUnderscore) {
        const std::string otherNameMod = other->nameStr().substr(2);
        return metadata::Identifier::isEquivalentName(nameStr().c_str(),
                                                      otherNameMod.c_str()) ||
               compare(nameStr(), otherNameMod);
    } else {
        return compare(nameStr(), other->nameStr());
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DynamicGeodeticReferenceFrame::Private {
    common::Measure frameReferenceEpoch{};
    util::optional<std::string> deformationModelName{};

    explicit Private(const common::Measure &frameReferenceEpochIn)
        : frameReferenceEpoch(frameReferenceEpochIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

DynamicGeodeticReferenceFrame::DynamicGeodeticReferenceFrame(
    const EllipsoidNNPtr &ellipsoidIn,
    const PrimeMeridianNNPtr &primeMeridianIn,
    const common::Measure &frameReferenceEpochIn,
    const util::optional<std::string> &deformationModelNameIn)
    : GeodeticReferenceFrame(ellipsoidIn, primeMeridianIn),
      d(std::make_unique<Private>(frameReferenceEpochIn)) {
    d->deformationModelName = deformationModelNameIn;
}

// ---------------------------------------------------------------------------

#ifdef notdef
DynamicGeodeticReferenceFrame::DynamicGeodeticReferenceFrame(
    const DynamicGeodeticReferenceFrame &other)
    : GeodeticReferenceFrame(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DynamicGeodeticReferenceFrame::~DynamicGeodeticReferenceFrame() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the epoch to which the coordinates of stations defining the
 * dynamic geodetic reference frame are referenced.
 *
 * Usually given as a decimal year e.g. 2016.47.
 *
 * @return the frame reference epoch.
 */
const common::Measure &
DynamicGeodeticReferenceFrame::frameReferenceEpoch() const {
    return d->frameReferenceEpoch;
}

// ---------------------------------------------------------------------------

/** \brief Return the name of the deformation model.
 *
 * @note This is an extension to the \ref ISO_19111_2019 modeling, to
 * hold the content of the DYNAMIC.MODEL WKT2 node.
 *
 * @return the name of the deformation model.
 */
const util::optional<std::string> &
DynamicGeodeticReferenceFrame::deformationModelName() const {
    return d->deformationModelName;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool DynamicGeodeticReferenceFrame::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (criterion == Criterion::STRICT &&
        !util::isOfExactType<DynamicGeodeticReferenceFrame>(*other)) {
        return false;
    }
    if (!GeodeticReferenceFrame::isEquivalentToNoExactTypeCheck(
            other, criterion, dbContext)) {
        return false;
    }
    auto otherDGRF = dynamic_cast<const DynamicGeodeticReferenceFrame *>(other);
    if (otherDGRF == nullptr) {
        // we can go here only if criterion != Criterion::STRICT, and thus
        // given the above check we can consider the objects equivalent.
        return true;
    }
    return frameReferenceEpoch()._isEquivalentTo(
               otherDGRF->frameReferenceEpoch(), criterion) &&
           metadata::Identifier::isEquivalentName(
               deformationModelName()->c_str(),
               otherDGRF->deformationModelName()->c_str());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DynamicGeodeticReferenceFrame::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (isWKT2 && formatter->use2019Keywords()) {
        formatter->startNode(io::WKTConstants::DYNAMIC, false);
        formatter->startNode(io::WKTConstants::FRAMEEPOCH, false);
        formatter->add(
            frameReferenceEpoch().convertToUnit(common::UnitOfMeasure::YEAR));
        formatter->endNode();
        if (deformationModelName().has_value() &&
            !deformationModelName()->empty()) {
            formatter->startNode(io::WKTConstants::MODEL, false);
            formatter->addQuotedString(*deformationModelName());
            formatter->endNode();
        }
        formatter->endNode();
    }
    GeodeticReferenceFrame::_exportToWKT(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a DynamicGeodeticReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param ellipsoid the Ellipsoid.
 * @param anchor the anchor definition, or empty.
 * @param primeMeridian the PrimeMeridian.
 * @param frameReferenceEpochIn the frame reference epoch.
 * @param deformationModelNameIn deformation model name, or empty
 * @return new DynamicGeodeticReferenceFrame.
 */
DynamicGeodeticReferenceFrameNNPtr DynamicGeodeticReferenceFrame::create(
    const util::PropertyMap &properties, const EllipsoidNNPtr &ellipsoid,
    const util::optional<std::string> &anchor,
    const PrimeMeridianNNPtr &primeMeridian,
    const common::Measure &frameReferenceEpochIn,
    const util::optional<std::string> &deformationModelNameIn) {
    DynamicGeodeticReferenceFrameNNPtr grf(
        DynamicGeodeticReferenceFrame::nn_make_shared<
            DynamicGeodeticReferenceFrame>(ellipsoid, primeMeridian,
                                           frameReferenceEpochIn,
                                           deformationModelNameIn));
    grf->setAnchor(anchor);
    grf->setProperties(properties);
    return grf;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DatumEnsemble::Private {
    std::vector<DatumNNPtr> datums{};
    metadata::PositionalAccuracyNNPtr positionalAccuracy;

    Private(const std::vector<DatumNNPtr> &datumsIn,
            const metadata::PositionalAccuracyNNPtr &accuracy)
        : datums(datumsIn), positionalAccuracy(accuracy) {}
};
//! @endcond

// ---------------------------------------------------------------------------

DatumEnsemble::DatumEnsemble(const std::vector<DatumNNPtr> &datumsIn,
                             const metadata::PositionalAccuracyNNPtr &accuracy)
    : d(std::make_unique<Private>(datumsIn, accuracy)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
DatumEnsemble::DatumEnsemble(const DatumEnsemble &other)
    : common::ObjectUsage(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DatumEnsemble::~DatumEnsemble() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the set of datums which may be considered to be
 * insignificantly different from each other.
 *
 * @return the set of datums of the DatumEnsemble.
 */
const std::vector<DatumNNPtr> &DatumEnsemble::datums() const {
    return d->datums;
}

// ---------------------------------------------------------------------------

/** \brief Return the inaccuracy introduced through use of this collection of
 * datums.
 *
 * It is an indication of the differences in coordinate values at all points
 * between the various realizations that have been grouped into this datum
 * ensemble.
 *
 * @return the accuracy.
 */
const metadata::PositionalAccuracyNNPtr &
DatumEnsemble::positionalAccuracy() const {
    return d->positionalAccuracy;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DatumNNPtr
DatumEnsemble::asDatum(const io::DatabaseContextPtr &dbContext) const {

    const auto &l_datums = datums();
    auto *grf = dynamic_cast<const GeodeticReferenceFrame *>(l_datums[0].get());

    const auto &l_identifiers = identifiers();
    if (dbContext) {
        if (!l_identifiers.empty()) {
            const auto &id = l_identifiers[0];
            try {
                auto factory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), *(id->codeSpace()));
                if (grf) {
                    return factory->createGeodeticDatum(id->code());
                } else {
                    return factory->createVerticalDatum(id->code());
                }
            } catch (const std::exception &) {
            }
        }
    }

    std::string l_name(nameStr());
    if (grf) {
        // Remap to traditional datum names
        if (l_name == "World Geodetic System 1984 ensemble") {
            l_name = "World Geodetic System 1984";
        } else if (l_name ==
                   "European Terrestrial Reference System 1989 ensemble") {
            l_name = "European Terrestrial Reference System 1989";
        }
    }
    auto props =
        util::PropertyMap().set(common::IdentifiedObject::NAME_KEY, l_name);
    if (isDeprecated()) {
        props.set(common::IdentifiedObject::DEPRECATED_KEY, true);
    }
    if (!l_identifiers.empty()) {
        const auto &id = l_identifiers[0];
        props.set(metadata::Identifier::CODESPACE_KEY, *(id->codeSpace()))
            .set(metadata::Identifier::CODE_KEY, id->code());
    }
    const auto &l_usages = domains();
    if (!l_usages.empty()) {

        auto array(util::ArrayOfBaseObject::create());
        for (const auto &usage : l_usages) {
            array->add(usage);
        }
        props.set(common::ObjectUsage::OBJECT_DOMAIN_KEY,
                  util::nn_static_pointer_cast<util::BaseObject>(array));
    }
    const auto anchor = util::optional<std::string>();

    if (grf) {
        return GeodeticReferenceFrame::create(props, grf->ellipsoid(), anchor,
                                              grf->primeMeridian());
    } else {
        assert(dynamic_cast<VerticalReferenceFrame *>(l_datums[0].get()));
        return datum::VerticalReferenceFrame::create(props, anchor);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DatumEnsemble::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2 || !formatter->use2019Keywords()) {
        return asDatum(formatter->databaseContext())->_exportToWKT(formatter);
    }

    const auto &l_datums = datums();
    assert(!l_datums.empty());

    formatter->startNode(io::WKTConstants::ENSEMBLE, false);
    const auto &l_name = nameStr();
    if (!l_name.empty()) {
        formatter->addQuotedString(l_name);
    } else {
        formatter->addQuotedString("unnamed");
    }

    for (const auto &datum : l_datums) {
        formatter->startNode(io::WKTConstants::MEMBER,
                             !datum->identifiers().empty());
        const auto &l_datum_name = datum->nameStr();
        if (!l_datum_name.empty()) {
            formatter->addQuotedString(l_datum_name);
        } else {
            formatter->addQuotedString("unnamed");
        }
        if (formatter->outputId()) {
            datum->formatID(formatter);
        }
        formatter->endNode();
    }

    auto grfFirst = std::dynamic_pointer_cast<GeodeticReferenceFrame>(
        l_datums[0].as_nullable());
    if (grfFirst) {
        grfFirst->ellipsoid()->_exportToWKT(formatter);
    }

    formatter->startNode(io::WKTConstants::ENSEMBLEACCURACY, false);
    formatter->add(positionalAccuracy()->value());
    formatter->endNode();

    // In theory, we should do the following, but currently the WKT grammar
    // doesn't allow this
    // ObjectUsage::baseExportToWKT(formatter);
    if (formatter->outputId()) {
        formatID(formatter);
    }

    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DatumEnsemble::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto objectContext(
        formatter->MakeObjectContext("DatumEnsemble", !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    const auto &l_datums = datums();
    writer->AddObjKey("members");
    {
        auto membersContext(writer->MakeArrayContext(false));
        for (const auto &datum : l_datums) {
            auto memberContext(writer->MakeObjectContext());
            writer->AddObjKey("name");
            const auto &l_datum_name = datum->nameStr();
            if (!l_datum_name.empty()) {
                writer->Add(l_datum_name);
            } else {
                writer->Add("unnamed");
            }
            datum->formatID(formatter);
        }
    }

    auto grfFirst = std::dynamic_pointer_cast<GeodeticReferenceFrame>(
        l_datums[0].as_nullable());
    if (grfFirst) {
        writer->AddObjKey("ellipsoid");
        formatter->setOmitTypeInImmediateChild();
        grfFirst->ellipsoid()->_exportToJSON(formatter);
    }

    writer->AddObjKey("accuracy");
    writer->Add(positionalAccuracy()->value());

    formatID(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a DatumEnsemble.
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param datumsIn Array of at least 2 datums.
 * @param accuracy Accuracy of the datum ensemble
 * @return new DatumEnsemble.
 * @throw util::Exception in case of error.
 */
DatumEnsembleNNPtr DatumEnsemble::create(
    const util::PropertyMap &properties,
    const std::vector<DatumNNPtr> &datumsIn,
    const metadata::PositionalAccuracyNNPtr &accuracy) // throw(Exception)
{
    if (datumsIn.size() < 2) {
        throw util::Exception("ensemble should have at least 2 datums");
    }
    if (auto grfFirst =
            dynamic_cast<const GeodeticReferenceFrame *>(datumsIn[0].get())) {
        for (size_t i = 1; i < datumsIn.size(); i++) {
            auto grf =
                dynamic_cast<const GeodeticReferenceFrame *>(datumsIn[i].get());
            if (!grf) {
                throw util::Exception(
                    "ensemble should have consistent datum types");
            }
            if (!grfFirst->ellipsoid()->_isEquivalentTo(
                    grf->ellipsoid().get())) {
                throw util::Exception(
                    "ensemble should have datums with identical ellipsoid");
            }
            if (!grfFirst->primeMeridian()->_isEquivalentTo(
                    grf->primeMeridian().get())) {
                throw util::Exception(
                    "ensemble should have datums with identical "
                    "prime meridian");
            }
        }
    } else if (dynamic_cast<VerticalReferenceFrame *>(datumsIn[0].get())) {
        for (size_t i = 1; i < datumsIn.size(); i++) {
            if (!dynamic_cast<VerticalReferenceFrame *>(datumsIn[i].get())) {
                throw util::Exception(
                    "ensemble should have consistent datum types");
            }
        }
    }
    auto ensemble(
        DatumEnsemble::nn_make_shared<DatumEnsemble>(datumsIn, accuracy));
    ensemble->setProperties(properties);
    return ensemble;
}

// ---------------------------------------------------------------------------

RealizationMethod::RealizationMethod(const std::string &nameIn)
    : CodeList(nameIn) {}

// ---------------------------------------------------------------------------

RealizationMethod &
RealizationMethod::operator=(const RealizationMethod &other) {
    CodeList::operator=(other);
    return *this;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct VerticalReferenceFrame::Private {
    util::optional<RealizationMethod> realizationMethod_{};

    // 2005 = CS_VD_GeoidModelDerived from OGC 01-009
    std::string wkt1DatumType_{"2005"};
};
//! @endcond

// ---------------------------------------------------------------------------

VerticalReferenceFrame::VerticalReferenceFrame(
    const util::optional<RealizationMethod> &realizationMethodIn)
    : d(std::make_unique<Private>()) {
    if (!realizationMethodIn->toString().empty()) {
        d->realizationMethod_ = *realizationMethodIn;
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalReferenceFrame::~VerticalReferenceFrame() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the method through which this vertical reference frame is
 * realized.
 *
 * @return the realization method.
 */
const util::optional<RealizationMethod> &
VerticalReferenceFrame::realizationMethod() const {
    return d->realizationMethod_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param anchor the anchor definition, or empty.
 * @param realizationMethodIn the realization method, or empty.
 * @return new VerticalReferenceFrame.
 */
VerticalReferenceFrameNNPtr VerticalReferenceFrame::create(
    const util::PropertyMap &properties,
    const util::optional<std::string> &anchor,
    const util::optional<RealizationMethod> &realizationMethodIn) {
    auto rf(VerticalReferenceFrame::nn_make_shared<VerticalReferenceFrame>(
        realizationMethodIn));
    rf->setAnchor(anchor);
    rf->setProperties(properties);
    properties.getStringValue("VERT_DATUM_TYPE", rf->d->wkt1DatumType_);
    return rf;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param anchor the anchor definition, or empty.
 * @param anchorEpoch the anchor epoch, or empty.
 * @param realizationMethodIn the realization method, or empty.
 * @return new VerticalReferenceFrame.
 * @since 9.2
 */
VerticalReferenceFrameNNPtr VerticalReferenceFrame::create(
    const util::PropertyMap &properties,
    const util::optional<std::string> &anchor,
    const util::optional<common::Measure> &anchorEpoch,
    const util::optional<RealizationMethod> &realizationMethodIn) {
    auto rf(VerticalReferenceFrame::nn_make_shared<VerticalReferenceFrame>(
        realizationMethodIn));
    rf->setAnchor(anchor);
    rf->setAnchorEpoch(anchorEpoch);
    rf->setProperties(properties);
    properties.getStringValue("VERT_DATUM_TYPE", rf->d->wkt1DatumType_);
    return rf;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const std::string &VerticalReferenceFrame::getWKT1DatumType() const {
    return d->wkt1DatumType_;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalReferenceFrame::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(isWKT2 ? io::WKTConstants::VDATUM
                         : formatter->useESRIDialect()
                             ? io::WKTConstants::VDATUM
                             : io::WKTConstants::VERT_DATUM,
                         !identifiers().empty());
    std::string l_name(nameStr());
    if (!l_name.empty()) {
        if (!isWKT2 && formatter->useESRIDialect()) {
            bool aliasFound = false;
            const auto &dbContext = formatter->databaseContext();
            if (dbContext) {
                auto l_alias = dbContext->getAliasFromOfficialName(
                    l_name, "vertical_datum", "ESRI");
                if (!l_alias.empty()) {
                    l_name = std::move(l_alias);
                    aliasFound = true;
                }
            }
            if (!aliasFound && dbContext) {
                auto authFactory = io::AuthorityFactory::create(
                    NN_NO_CHECK(dbContext), "ESRI");
                aliasFound = authFactory
                                 ->createObjectsFromName(
                                     l_name,
                                     {io::AuthorityFactory::ObjectType::
                                          VERTICAL_REFERENCE_FRAME},
                                     false // approximateMatch
                                     )
                                 .size() == 1;
            }
            if (!aliasFound) {
                l_name = io::WKTFormatter::morphNameToESRI(l_name);
            }
        }
        formatter->addQuotedString(l_name);
    } else {
        formatter->addQuotedString("unnamed");
    }
    if (isWKT2) {
        Datum::getPrivate()->exportAnchorDefinition(formatter);
        if (formatter->use2019Keywords()) {
            Datum::getPrivate()->exportAnchorEpoch(formatter);
        }
    } else if (!formatter->useESRIDialect()) {
        formatter->add(d->wkt1DatumType_);
        const auto &extension = formatter->getVDatumExtension();
        if (!extension.empty()) {
            formatter->startNode(io::WKTConstants::EXTENSION, false);
            formatter->addQuotedString("PROJ4_GRIDS");
            formatter->addQuotedString(extension);
            formatter->endNode();
        }
    }
    if (formatter->outputId()) {
        formatID(formatter);
    }
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void VerticalReferenceFrame::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto dynamicGRF = dynamic_cast<const DynamicVerticalReferenceFrame *>(this);

    auto objectContext(formatter->MakeObjectContext(
        dynamicGRF ? "DynamicVerticalReferenceFrame" : "VerticalReferenceFrame",
        !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    const auto &l_name = nameStr();
    if (l_name.empty()) {
        writer->Add("unnamed");
    } else {
        writer->Add(l_name);
    }

    Datum::getPrivate()->exportAnchorDefinition(formatter);
    Datum::getPrivate()->exportAnchorEpoch(formatter);

    if (dynamicGRF) {
        writer->AddObjKey("frame_reference_epoch");
        writer->Add(dynamicGRF->frameReferenceEpoch().value());
    }

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool VerticalReferenceFrame::isEquivalentToNoExactTypeCheck(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherVRF = dynamic_cast<const VerticalReferenceFrame *>(other);
    if (otherVRF == nullptr ||
        !Datum::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    if ((realizationMethod().has_value() ^
         otherVRF->realizationMethod().has_value())) {
        return false;
    }
    if (realizationMethod().has_value() &&
        otherVRF->realizationMethod().has_value()) {
        if (*(realizationMethod()) != *(otherVRF->realizationMethod())) {
            return false;
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

bool VerticalReferenceFrame::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (criterion == Criterion::STRICT &&
        !util::isOfExactType<VerticalReferenceFrame>(*other)) {
        return false;
    }
    return isEquivalentToNoExactTypeCheck(other, criterion, dbContext);
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DynamicVerticalReferenceFrame::Private {
    common::Measure frameReferenceEpoch{};
    util::optional<std::string> deformationModelName{};

    explicit Private(const common::Measure &frameReferenceEpochIn)
        : frameReferenceEpoch(frameReferenceEpochIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

DynamicVerticalReferenceFrame::DynamicVerticalReferenceFrame(
    const util::optional<RealizationMethod> &realizationMethodIn,
    const common::Measure &frameReferenceEpochIn,
    const util::optional<std::string> &deformationModelNameIn)
    : VerticalReferenceFrame(realizationMethodIn),
      d(std::make_unique<Private>(frameReferenceEpochIn)) {
    d->deformationModelName = deformationModelNameIn;
}

// ---------------------------------------------------------------------------

#ifdef notdef
DynamicVerticalReferenceFrame::DynamicVerticalReferenceFrame(
    const DynamicVerticalReferenceFrame &other)
    : VerticalReferenceFrame(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DynamicVerticalReferenceFrame::~DynamicVerticalReferenceFrame() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the epoch to which the coordinates of stations defining the
 * dynamic geodetic reference frame are referenced.
 *
 * Usually given as a decimal year e.g. 2016.47.
 *
 * @return the frame reference epoch.
 */
const common::Measure &
DynamicVerticalReferenceFrame::frameReferenceEpoch() const {
    return d->frameReferenceEpoch;
}

// ---------------------------------------------------------------------------

/** \brief Return the name of the deformation model.
 *
 * @note This is an extension to the \ref ISO_19111_2019 modeling, to
 * hold the content of the DYNAMIC.MODEL WKT2 node.
 *
 * @return the name of the deformation model.
 */
const util::optional<std::string> &
DynamicVerticalReferenceFrame::deformationModelName() const {
    return d->deformationModelName;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool DynamicVerticalReferenceFrame::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    if (criterion == Criterion::STRICT &&
        !util::isOfExactType<DynamicVerticalReferenceFrame>(*other)) {
        return false;
    }
    if (!VerticalReferenceFrame::isEquivalentToNoExactTypeCheck(
            other, criterion, dbContext)) {
        return false;
    }
    auto otherDGRF = dynamic_cast<const DynamicVerticalReferenceFrame *>(other);
    if (otherDGRF == nullptr) {
        // we can go here only if criterion != Criterion::STRICT, and thus
        // given the above check we can consider the objects equivalent.
        return true;
    }
    return frameReferenceEpoch()._isEquivalentTo(
               otherDGRF->frameReferenceEpoch(), criterion) &&
           metadata::Identifier::isEquivalentName(
               deformationModelName()->c_str(),
               otherDGRF->deformationModelName()->c_str());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void DynamicVerticalReferenceFrame::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (isWKT2 && formatter->use2019Keywords()) {
        formatter->startNode(io::WKTConstants::DYNAMIC, false);
        formatter->startNode(io::WKTConstants::FRAMEEPOCH, false);
        formatter->add(
            frameReferenceEpoch().convertToUnit(common::UnitOfMeasure::YEAR));
        formatter->endNode();
        if (!deformationModelName()->empty()) {
            formatter->startNode(io::WKTConstants::MODEL, false);
            formatter->addQuotedString(*deformationModelName());
            formatter->endNode();
        }
        formatter->endNode();
    }
    VerticalReferenceFrame::_exportToWKT(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a DynamicVerticalReferenceFrame
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param anchor the anchor definition, or empty.
 * @param realizationMethodIn the realization method, or empty.
 * @param frameReferenceEpochIn the frame reference epoch.
 * @param deformationModelNameIn deformation model name, or empty
 * @return new DynamicVerticalReferenceFrame.
 */
DynamicVerticalReferenceFrameNNPtr DynamicVerticalReferenceFrame::create(
    const util::PropertyMap &properties,
    const util::optional<std::string> &anchor,
    const util::optional<RealizationMethod> &realizationMethodIn,
    const common::Measure &frameReferenceEpochIn,
    const util::optional<std::string> &deformationModelNameIn) {
    DynamicVerticalReferenceFrameNNPtr grf(
        DynamicVerticalReferenceFrame::nn_make_shared<
            DynamicVerticalReferenceFrame>(realizationMethodIn,
                                           frameReferenceEpochIn,
                                           deformationModelNameIn));
    grf->setAnchor(anchor);
    grf->setProperties(properties);
    return grf;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct TemporalDatum::Private {
    common::DateTime temporalOrigin_;
    std::string calendar_;

    Private(const common::DateTime &temporalOriginIn,
            const std::string &calendarIn)
        : temporalOrigin_(temporalOriginIn), calendar_(calendarIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

TemporalDatum::TemporalDatum(const common::DateTime &temporalOriginIn,
                             const std::string &calendarIn)
    : d(std::make_unique<Private>(temporalOriginIn, calendarIn)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalDatum::~TemporalDatum() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the date and time to which temporal coordinates are
 * referenced, expressed in conformance with ISO 8601.
 *
 * @return the temporal origin.
 */
const common::DateTime &TemporalDatum::temporalOrigin() const {
    return d->temporalOrigin_;
}

// ---------------------------------------------------------------------------

/** \brief Return the calendar to which the temporal origin is referenced
 *
 * Default value: TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN.
 *
 * @return the calendar.
 */
const std::string &TemporalDatum::calendar() const { return d->calendar_; }

// ---------------------------------------------------------------------------

/** \brief Instantiate a TemporalDatum
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param temporalOriginIn the temporal origin into which temporal coordinates
 * are referenced.
 * @param calendarIn the calendar (generally
 * TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN)
 * @return new TemporalDatum.
 */
TemporalDatumNNPtr
TemporalDatum::create(const util::PropertyMap &properties,
                      const common::DateTime &temporalOriginIn,
                      const std::string &calendarIn) {
    auto datum(TemporalDatum::nn_make_shared<TemporalDatum>(temporalOriginIn,
                                                            calendarIn));
    datum->setProperties(properties);
    return datum;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void TemporalDatum::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        throw io::FormattingException(
            "TemporalDatum can only be exported to WKT2");
    }
    formatter->startNode(io::WKTConstants::TDATUM, !identifiers().empty());
    formatter->addQuotedString(nameStr());
    if (formatter->use2019Keywords()) {
        formatter->startNode(io::WKTConstants::CALENDAR, false);
        formatter->addQuotedString(calendar());
        formatter->endNode();
    }

    const auto &timeOriginStr = temporalOrigin().toString();
    if (!timeOriginStr.empty()) {
        formatter->startNode(io::WKTConstants::TIMEORIGIN, false);
        if (temporalOrigin().isISO_8601()) {
            formatter->add(timeOriginStr);
        } else {
            formatter->addQuotedString(timeOriginStr);
        }
        formatter->endNode();
    }

    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void TemporalDatum::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto objectContext(
        formatter->MakeObjectContext("TemporalDatum", !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    writer->Add(nameStr());

    writer->AddObjKey("calendar");
    writer->Add(calendar());

    const auto &timeOriginStr = temporalOrigin().toString();
    if (!timeOriginStr.empty()) {
        writer->AddObjKey("time_origin");
        writer->Add(timeOriginStr);
    }

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool TemporalDatum::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherTD = dynamic_cast<const TemporalDatum *>(other);
    if (otherTD == nullptr ||
        !Datum::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    return temporalOrigin().toString() ==
               otherTD->temporalOrigin().toString() &&
           calendar() == otherTD->calendar();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct EngineeringDatum::Private {};
//! @endcond

// ---------------------------------------------------------------------------

EngineeringDatum::EngineeringDatum() : d(nullptr) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
EngineeringDatum::~EngineeringDatum() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a EngineeringDatum
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param anchor the anchor definition, or empty.
 * @return new EngineeringDatum.
 */
EngineeringDatumNNPtr
EngineeringDatum::create(const util::PropertyMap &properties,
                         const util::optional<std::string> &anchor) {
    auto datum(EngineeringDatum::nn_make_shared<EngineeringDatum>());
    datum->setAnchor(anchor);
    datum->setProperties(properties);
    return datum;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void EngineeringDatum::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(isWKT2 ? io::WKTConstants::EDATUM
                                : io::WKTConstants::LOCAL_DATUM,
                         !identifiers().empty());
    formatter->addQuotedString(nameStr());
    if (isWKT2) {
        Datum::getPrivate()->exportAnchorDefinition(formatter);
    } else {
        // Somewhat picked up arbitrarily from OGC 01-009:
        // CS_LD_Max (Attribute) : 32767
        // Highest possible value for local datum types.
        formatter->add(32767);
    }
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void EngineeringDatum::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto objectContext(formatter->MakeObjectContext("EngineeringDatum",
                                                    !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    writer->Add(nameStr());

    Datum::getPrivate()->exportAnchorDefinition(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool EngineeringDatum::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDatum = dynamic_cast<const EngineeringDatum *>(other);
    if (otherDatum == nullptr) {
        return false;
    }
    if (criterion != util::IComparable::Criterion::STRICT &&
        (nameStr().empty() || nameStr() == UNKNOWN_ENGINEERING_DATUM) &&
        (otherDatum->nameStr().empty() ||
         otherDatum->nameStr() == UNKNOWN_ENGINEERING_DATUM)) {
        return true;
    }
    return Datum::_isEquivalentTo(other, criterion, dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ParametricDatum::Private {};
//! @endcond

// ---------------------------------------------------------------------------

ParametricDatum::ParametricDatum() : d(nullptr) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ParametricDatum::~ParametricDatum() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParametricDatum
 *
 * @param properties See \ref general_properties.
 * At minimum the name should be defined.
 * @param anchor the anchor definition, or empty.
 * @return new ParametricDatum.
 */
ParametricDatumNNPtr
ParametricDatum::create(const util::PropertyMap &properties,
                        const util::optional<std::string> &anchor) {
    auto datum(ParametricDatum::nn_make_shared<ParametricDatum>());
    datum->setAnchor(anchor);
    datum->setProperties(properties);
    return datum;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ParametricDatum::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    if (!isWKT2) {
        throw io::FormattingException(
            "ParametricDatum can only be exported to WKT2");
    }
    formatter->startNode(io::WKTConstants::PDATUM, !identifiers().empty());
    formatter->addQuotedString(nameStr());
    Datum::getPrivate()->exportAnchorDefinition(formatter);
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ParametricDatum::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto objectContext(formatter->MakeObjectContext("ParametricDatum",
                                                    !identifiers().empty()));
    auto writer = formatter->writer();

    writer->AddObjKey("name");
    writer->Add(nameStr());

    Datum::getPrivate()->exportAnchorDefinition(formatter);

    ObjectUsage::baseExportToJSON(formatter);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool ParametricDatum::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherTD = dynamic_cast<const ParametricDatum *>(other);
    if (otherTD == nullptr ||
        !Datum::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    return true;
}
//! @endcond

} // namespace datum
NS_PROJ_END
