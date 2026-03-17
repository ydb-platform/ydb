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

#include "proj/coordinatesystem.hpp"
#include "proj/common.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/coordinatesystem_internal.hpp"
#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "proj_json_streaming_writer.hpp"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<NS_PROJ::cs::MeridianPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::CoordinateSystemAxisPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::CoordinateSystemPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::SphericalCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::EllipsoidalCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::CartesianCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::TemporalCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::TemporalCountCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::TemporalMeasureCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::DateTimeTemporalCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::VerticalCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::ParametricCSPtr>::~nn() = default;
template<> nn<NS_PROJ::cs::OrdinalCSPtr>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace cs {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Meridian::Private {
    common::Angle longitude_{};

    explicit Private(const common::Angle &longitude) : longitude_(longitude) {}
};
//! @endcond

// ---------------------------------------------------------------------------

Meridian::Meridian(const common::Angle &longitudeIn)
    : d(std::make_unique<Private>(longitudeIn)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
Meridian::Meridian(const Meridian &other)
    : IdentifiedObject(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Meridian::~Meridian() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the longitude of the meridian that the axis follows from the
 * pole.
 *
 * @return the longitude.
 */
const common::Angle &Meridian::longitude() PROJ_PURE_DEFN {
    return d->longitude_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Meridian.
 *
 * @param longitudeIn longitude of the meridian that the axis follows from the
 * pole.
 * @return new Meridian.
 */
MeridianNNPtr Meridian::create(const common::Angle &longitudeIn) {
    return Meridian::nn_make_shared<Meridian>(longitudeIn);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Meridian::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    formatter->startNode(io::WKTConstants::MERIDIAN, !identifiers().empty());
    formatter->add(longitude().value());
    longitude().unit()._exportToWKT(formatter, io::WKTConstants::ANGLEUNIT);
    if (formatter->outputId()) {
        formatID(formatter);
    }
    formatter->endNode();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void Meridian::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("Meridian", !identifiers().empty()));

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
struct CoordinateSystemAxis::Private {
    std::string abbreviation{};
    const AxisDirection *direction = &(AxisDirection::UNSPECIFIED);
    common::UnitOfMeasure unit{};
    util::optional<RangeMeaning> rangeMeaning = util::optional<RangeMeaning>();
    util::optional<double> minimumValue{};
    util::optional<double> maximumValue{};
    MeridianPtr meridian{};
};
//! @endcond

// ---------------------------------------------------------------------------

CoordinateSystemAxis::CoordinateSystemAxis() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

#ifdef notdef
CoordinateSystemAxis::CoordinateSystemAxis(const CoordinateSystemAxis &other)
    : IdentifiedObject(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateSystemAxis::~CoordinateSystemAxis() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the axis abbreviation.
 *
 * The abbreviation used for this coordinate system axis; this abbreviation
 * is also used to identify the coordinates in the coordinate tuple.
 * Examples are X and Y.
 *
 * @return the abbreviation.
 */
const std::string &CoordinateSystemAxis::abbreviation() PROJ_PURE_DEFN {
    return d->abbreviation;
}

// ---------------------------------------------------------------------------

/** \brief Return the axis direction.
 *
 * The direction of this coordinate system axis (or in the case of Cartesian
 * projected coordinates, the direction of this coordinate system axis locally)
 * Examples: north or south, east or west, up or down. Within any set of
 * coordinate system axes, only one of each pair of terms can be used. For
 * Earth-fixed CRSs, this direction is often approximate and intended to
 * provide a human interpretable meaning to the axis. When a geodetic reference
 * frame is used, the precise directions of the axes may therefore vary
 * slightly from this approximate direction. Note that an EngineeringCRS often
 * requires specific descriptions of the directions of its coordinate system
 * axes.
 *
 * @return the direction.
 */
const AxisDirection &CoordinateSystemAxis::direction() PROJ_PURE_DEFN {
    return *(d->direction);
}

// ---------------------------------------------------------------------------

/** \brief Return the axis unit.
 *
 * This is the spatial unit or temporal quantity used for this coordinate
 * system axis. The value of a coordinate in a coordinate tuple shall be
 * recorded using this unit.
 *
 * @return the axis unit.
 */
const common::UnitOfMeasure &CoordinateSystemAxis::unit() PROJ_PURE_DEFN {
    return d->unit;
}

// ---------------------------------------------------------------------------

/** \brief Return the minimum value normally allowed for this axis, in the unit
 * for the axis.
 *
 * @return the minimum value, or empty.
 */
const util::optional<double> &
CoordinateSystemAxis::minimumValue() PROJ_PURE_DEFN {
    return d->minimumValue;
}

// ---------------------------------------------------------------------------

/** \brief Return the maximum value normally allowed for this axis, in the unit
 * for the axis.
 *
 * @return the maximum value, or empty.
 */
const util::optional<double> &
CoordinateSystemAxis::maximumValue() PROJ_PURE_DEFN {
    return d->maximumValue;
}

// ---------------------------------------------------------------------------

/** \brief Return the range meaning
 *
 * @return the range meaning, or empty.
 * @since 9.2
 */
const util::optional<RangeMeaning> &
CoordinateSystemAxis::rangeMeaning() PROJ_PURE_DEFN {
    return d->rangeMeaning;
}

// ---------------------------------------------------------------------------

/** \brief Return the meridian that the axis follows from the pole, for a
 * coordinate
 * reference system centered on a pole.
 *
 * @return the meridian, or null.
 */
const MeridianPtr &CoordinateSystemAxis::meridian() PROJ_PURE_DEFN {
    return d->meridian;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateSystemAxis.
 *
 * @param properties See \ref general_properties. The name should generally be
 * defined.
 * @param abbreviationIn Axis abbreviation (might be empty)
 * @param directionIn Axis direction
 * @param unitIn Axis unit
 * @param meridianIn The meridian that the axis follows from the pole, for a
 * coordinate
 * reference system centered on a pole, or nullptr
 * @return a new CoordinateSystemAxis.
 */
CoordinateSystemAxisNNPtr CoordinateSystemAxis::create(
    const util::PropertyMap &properties, const std::string &abbreviationIn,
    const AxisDirection &directionIn, const common::UnitOfMeasure &unitIn,
    const MeridianPtr &meridianIn) {
    auto csa(CoordinateSystemAxis::nn_make_shared<CoordinateSystemAxis>());
    csa->setProperties(properties);
    csa->d->abbreviation = abbreviationIn;
    csa->d->direction = &directionIn;
    csa->d->unit = unitIn;
    csa->d->meridian = meridianIn;
    return csa;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CoordinateSystemAxis.
 *
 * @param properties See \ref general_properties. The name should generally be
 * defined.
 * @param abbreviationIn Axis abbreviation (might be empty)
 * @param directionIn Axis direction
 * @param unitIn Axis unit
 * @param minimumValueIn Minimum value along axis
 * @param maximumValueIn Maximum value along axis
 * @param rangeMeaningIn Range Meaning
 * @param meridianIn The meridian that the axis follows from the pole, for a
 * coordinate
 * reference system centered on a pole, or nullptr
 * @return a new CoordinateSystemAxis.
 * @since 9.2
 */
CoordinateSystemAxisNNPtr CoordinateSystemAxis::create(
    const util::PropertyMap &properties, const std::string &abbreviationIn,
    const AxisDirection &directionIn, const common::UnitOfMeasure &unitIn,
    const util::optional<double> &minimumValueIn,
    const util::optional<double> &maximumValueIn,
    const util::optional<RangeMeaning> &rangeMeaningIn,
    const MeridianPtr &meridianIn) {
    auto csa(CoordinateSystemAxis::nn_make_shared<CoordinateSystemAxis>());
    csa->setProperties(properties);
    csa->d->abbreviation = abbreviationIn;
    csa->d->direction = &directionIn;
    csa->d->unit = unitIn;
    csa->d->minimumValue = minimumValueIn;
    csa->d->maximumValue = maximumValueIn;
    csa->d->rangeMeaning = rangeMeaningIn;
    csa->d->meridian = meridianIn;
    return csa;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateSystemAxis::_exportToWKT(
    // cppcheck-suppress passedByValue
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    _exportToWKT(formatter, 0, false);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::string CoordinateSystemAxis::normalizeAxisName(const std::string &str) {
    if (str.empty()) {
        return str;
    }
    // on import, transform from WKT2 "longitude" to "Longitude", as in the
    // EPSG database.
    return toupper(str.substr(0, 1)) + str.substr(1);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateSystemAxis::_exportToWKT(io::WKTFormatter *formatter, int order,
                                        bool disableAbbrev) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;
    formatter->startNode(io::WKTConstants::AXIS, !identifiers().empty());
    const std::string &axisName = nameStr();
    const std::string &abbrev = abbreviation();
    std::string parenthesizedAbbrev =
        std::string("(").append(abbrev).append(")");
    std::string dir = direction().toString();
    std::string axisDesignation;

    // It seems that the convention in WKT2 for axis name is first letter in
    // lower case. Whereas in WKT1 GDAL, it is in upper case (as in the EPSG
    // database)
    if (!axisName.empty()) {
        if (isWKT2) {
            axisDesignation =
                tolower(axisName.substr(0, 1)) + axisName.substr(1);
        } else {
            if (axisName == "Geodetic latitude") {
                axisDesignation = "Latitude";
            } else if (axisName == "Geodetic longitude") {
                axisDesignation = "Longitude";
            } else {
                axisDesignation = axisName;
            }
        }
    }

    if (!disableAbbrev && isWKT2 &&
        // For geodetic CS, export the axis name without abbreviation
        !(axisName == AxisName::Latitude || axisName == AxisName::Longitude)) {
        if (!axisDesignation.empty() && !abbrev.empty()) {
            axisDesignation += " ";
        }
        if (!abbrev.empty()) {
            axisDesignation += parenthesizedAbbrev;
        }
    }
    if (!isWKT2) {
        dir = toupper(dir);

        if (direction() == AxisDirection::GEOCENTRIC_Z) {
            dir = AxisDirectionWKT1::NORTH;
        } else if (AxisDirectionWKT1::valueOf(dir) == nullptr) {
            dir = AxisDirectionWKT1::OTHER;
        }
    } else if (!abbrev.empty()) {
        // For geocentric CS, just put the abbreviation
        if (direction() == AxisDirection::GEOCENTRIC_X ||
            direction() == AxisDirection::GEOCENTRIC_Y ||
            direction() == AxisDirection::GEOCENTRIC_Z) {
            axisDesignation = std::move(parenthesizedAbbrev);
        }
        // For cartesian CS with Easting/Northing, export only the abbreviation
        else if ((order == 1 && axisName == AxisName::Easting &&
                  abbrev == AxisAbbreviation::E) ||
                 (order == 2 && axisName == AxisName::Northing &&
                  abbrev == AxisAbbreviation::N)) {
            axisDesignation = std::move(parenthesizedAbbrev);
        }
    }
    formatter->addQuotedString(axisDesignation);
    formatter->add(dir);
    const auto &l_meridian = meridian();
    if (isWKT2 && l_meridian) {
        l_meridian->_exportToWKT(formatter);
    }
    if (formatter->outputAxisOrder() && order > 0) {
        formatter->startNode(io::WKTConstants::ORDER, false);
        formatter->add(order);
        formatter->endNode();
    }
    if (formatter->outputUnit() &&
        unit().type() != common::UnitOfMeasure::Type::NONE) {
        unit()._exportToWKT(formatter);
    }
    if (isWKT2 && formatter->use2019Keywords()) {
        if (d->minimumValue.has_value()) {
            formatter->startNode(io::WKTConstants::AXISMINVALUE, false);
            formatter->add(*(d->minimumValue));
            formatter->endNode();
        }
        if (d->maximumValue.has_value()) {
            formatter->startNode(io::WKTConstants::AXISMAXVALUE, false);
            formatter->add(*(d->maximumValue));
            formatter->endNode();
        }
        if (d->minimumValue.has_value() && d->maximumValue.has_value() &&
            d->rangeMeaning.has_value()) {
            formatter->startNode(io::WKTConstants::RANGEMEANING, false);
            formatter->add(d->rangeMeaning->toString());
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
void CoordinateSystemAxis::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(
        formatter->MakeObjectContext("Axis", !identifiers().empty()));

    writer->AddObjKey("name");
    writer->Add(nameStr());

    writer->AddObjKey("abbreviation");
    writer->Add(abbreviation());

    writer->AddObjKey("direction");
    writer->Add(direction().toString());

    const auto &l_meridian = meridian();
    if (l_meridian) {
        writer->AddObjKey("meridian");
        formatter->setOmitTypeInImmediateChild();
        l_meridian->_exportToJSON(formatter);
    }

    const auto &l_unit(unit());
    if (l_unit == common::UnitOfMeasure::METRE ||
        l_unit == common::UnitOfMeasure::DEGREE) {
        writer->AddObjKey("unit");
        writer->Add(l_unit.name());
    } else if (l_unit.type() != common::UnitOfMeasure::Type::NONE) {
        writer->AddObjKey("unit");
        l_unit._exportToJSON(formatter);
    }

    if (d->minimumValue.has_value()) {
        writer->AddObjKey("minimum_value");
        writer->Add(*(d->minimumValue));
    }

    if (d->maximumValue.has_value()) {
        writer->AddObjKey("maximum_value");
        writer->Add(*(d->maximumValue));
    }

    if (d->minimumValue.has_value() && d->maximumValue.has_value() &&
        d->rangeMeaning.has_value()) {
        writer->AddObjKey("range_meaning");
        writer->Add(d->rangeMeaning->toString());
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool CoordinateSystemAxis::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherCSA = dynamic_cast<const CoordinateSystemAxis *>(other);
    if (otherCSA == nullptr) {
        return false;
    }
    // For approximate comparison, only care about axis direction and unit.
    if (!(*(d->direction) == *(otherCSA->d->direction) &&
          d->unit._isEquivalentTo(otherCSA->d->unit, criterion))) {
        return false;
    }
    if (criterion == util::IComparable::Criterion::STRICT) {
        if (!IdentifiedObject::_isEquivalentTo(other, criterion, dbContext)) {
            return false;
        }
        if (abbreviation() != otherCSA->abbreviation()) {
            return false;
        }
        // TODO other metadata
    }

    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateSystemAxisNNPtr
CoordinateSystemAxis::alterUnit(const common::UnitOfMeasure &newUnit) const {
    return create(util::PropertyMap().set(IdentifiedObject::NAME_KEY, name()),
                  abbreviation(), direction(), newUnit, meridian());
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct CoordinateSystem::Private {
    std::vector<CoordinateSystemAxisNNPtr> axisList{};

    explicit Private(const std::vector<CoordinateSystemAxisNNPtr> &axisListIn)
        : axisList(axisListIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

CoordinateSystem::CoordinateSystem(
    const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : d(std::make_unique<Private>(axisIn)) {}

// ---------------------------------------------------------------------------

#ifdef notdef
CoordinateSystem::CoordinateSystem(const CoordinateSystem &other)
    : IdentifiedObject(other), d(std::make_unique<Private>(*other.d)) {}
#endif

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateSystem::~CoordinateSystem() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the list of axes of this coordinate system.
 *
 * @return the axes.
 */
const std::vector<CoordinateSystemAxisNNPtr> &
CoordinateSystem::axisList() PROJ_PURE_DEFN {
    return d->axisList;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateSystem::_exportToWKT(
    io::WKTFormatter *formatter) const // throw(FormattingException)
{
    if (formatter->outputAxis() != io::WKTFormatter::OutputAxisRule::YES) {
        return;
    }
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;

    const auto &l_axisList = axisList();
    if (isWKT2) {
        formatter->startNode(io::WKTConstants::CS_, !identifiers().empty());
        formatter->add(getWKT2Type(formatter->use2019Keywords()));
        formatter->add(static_cast<int>(l_axisList.size()));
        formatter->endNode();
        formatter->startNode(std::string(),
                             false); // anonymous indentation level
    }

    common::UnitOfMeasure unit = common::UnitOfMeasure::NONE;
    bool bAllSameUnit = true;
    bool bFirstUnit = true;
    for (const auto &axis : l_axisList) {
        const auto &l_unit = axis->unit();
        if (bFirstUnit) {
            unit = l_unit;
            bFirstUnit = false;
        } else if (unit != l_unit) {
            bAllSameUnit = false;
        }
    }

    formatter->pushOutputUnit(
        isWKT2 && (!bAllSameUnit || !formatter->outputCSUnitOnlyOnceIfSame()));

    int order = 1;
    const bool disableAbbrev =
        (l_axisList.size() == 3 &&
         l_axisList[0]->nameStr() == AxisName::Latitude &&
         l_axisList[1]->nameStr() == AxisName::Longitude &&
         l_axisList[2]->nameStr() == AxisName::Ellipsoidal_height);

    for (auto &axis : l_axisList) {
        int axisOrder = (isWKT2 && l_axisList.size() > 1) ? order : 0;
        axis->_exportToWKT(formatter, axisOrder, disableAbbrev);
        order++;
    }
    if (isWKT2 && !l_axisList.empty() && bAllSameUnit &&
        formatter->outputCSUnitOnlyOnceIfSame()) {
        unit._exportToWKT(formatter);
    }

    formatter->popOutputUnit();

    if (isWKT2) {
        formatter->endNode();
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void CoordinateSystem::_exportToJSON(
    io::JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    auto objectContext(formatter->MakeObjectContext("CoordinateSystem",
                                                    !identifiers().empty()));

    writer->AddObjKey("subtype");
    writer->Add(getWKT2Type(true));

    writer->AddObjKey("axis");
    {
        auto axisContext(writer->MakeArrayContext(false));
        const auto &l_axisList = axisList();
        for (auto &axis : l_axisList) {
            formatter->setOmitTypeInImmediateChild();
            axis->_exportToJSON(formatter);
        }
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool CoordinateSystem::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherCS = dynamic_cast<const CoordinateSystem *>(other);
    if (otherCS == nullptr ||
        !IdentifiedObject::_isEquivalentTo(other, criterion, dbContext)) {
        return false;
    }
    const auto &list = axisList();
    const auto &otherList = otherCS->axisList();
    if (list.size() != otherList.size()) {
        return false;
    }
    if (getWKT2Type(true) != otherCS->getWKT2Type(true)) {
        return false;
    }
    for (size_t i = 0; i < list.size(); i++) {
        if (!list[i]->_isEquivalentTo(otherList[i].get(), criterion,
                                      dbContext)) {
            return false;
        }
    }
    return true;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
SphericalCS::~SphericalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

SphericalCS::SphericalCS(const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

#ifdef notdef
SphericalCS::SphericalCS(const SphericalCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a SphericalCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @param axis3 The third axis.
 * @return a new SphericalCS.
 */
SphericalCSNNPtr SphericalCS::create(const util::PropertyMap &properties,
                                     const CoordinateSystemAxisNNPtr &axis1,
                                     const CoordinateSystemAxisNNPtr &axis2,
                                     const CoordinateSystemAxisNNPtr &axis3) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2, axis3};
    auto cs(SphericalCS::nn_make_shared<SphericalCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a SphericalCS with 2 axis.
 *
 * This is an extension to ISO19111 to support (planet)-ocentric CS with
 * geocentric latitude.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @return a new SphericalCS.
 */
SphericalCSNNPtr SphericalCS::create(const util::PropertyMap &properties,
                                     const CoordinateSystemAxisNNPtr &axis1,
                                     const CoordinateSystemAxisNNPtr &axis2) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2};
    auto cs(SphericalCS::nn_make_shared<SphericalCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
EllipsoidalCS::~EllipsoidalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

EllipsoidalCS::EllipsoidalCS(
    const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

#ifdef notdef
EllipsoidalCS::EllipsoidalCS(const EllipsoidalCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @return a new EllipsoidalCS.
 */
EllipsoidalCSNNPtr
EllipsoidalCS::create(const util::PropertyMap &properties,
                      const CoordinateSystemAxisNNPtr &axis1,
                      const CoordinateSystemAxisNNPtr &axis2) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2};
    auto cs(EllipsoidalCS::nn_make_shared<EllipsoidalCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @param axis3 The third axis.
 * @return a new EllipsoidalCS.
 */
EllipsoidalCSNNPtr
EllipsoidalCS::create(const util::PropertyMap &properties,
                      const CoordinateSystemAxisNNPtr &axis1,
                      const CoordinateSystemAxisNNPtr &axis2,
                      const CoordinateSystemAxisNNPtr &axis3) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2, axis3};
    auto cs(EllipsoidalCS::nn_make_shared<EllipsoidalCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CoordinateSystemAxisNNPtr
CoordinateSystemAxis::createLAT_NORTH(const common::UnitOfMeasure &unit) {
    return create(
        util::PropertyMap().set(IdentifiedObject::NAME_KEY, AxisName::Latitude),
        AxisAbbreviation::lat, AxisDirection::NORTH, unit);
}

CoordinateSystemAxisNNPtr
CoordinateSystemAxis::createLONG_EAST(const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                          AxisName::Longitude),
                  AxisAbbreviation::lon, AxisDirection::EAST, unit);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS with a Latitude (first) and Longitude
 * (second) axis.
 *
 * @param unit Angular unit of the axes.
 * @return a new EllipsoidalCS.
 */
EllipsoidalCSNNPtr
EllipsoidalCS::createLatitudeLongitude(const common::UnitOfMeasure &unit) {
    return EllipsoidalCS::create(util::PropertyMap(),
                                 CoordinateSystemAxis::createLAT_NORTH(unit),
                                 CoordinateSystemAxis::createLONG_EAST(unit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS with a Latitude (first), Longitude
 * (second) axis and ellipsoidal height (third) axis.
 *
 * @param angularUnit Angular unit of the latitude and longitude axes.
 * @param linearUnit Linear unit of the ellipsoidal height axis.
 * @return a new EllipsoidalCS.
 */
EllipsoidalCSNNPtr EllipsoidalCS::createLatitudeLongitudeEllipsoidalHeight(
    const common::UnitOfMeasure &angularUnit,
    const common::UnitOfMeasure &linearUnit) {
    return EllipsoidalCS::create(
        util::PropertyMap(), CoordinateSystemAxis::createLAT_NORTH(angularUnit),
        CoordinateSystemAxis::createLONG_EAST(angularUnit),
        CoordinateSystemAxis::create(
            util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                    AxisName::Ellipsoidal_height),
            AxisAbbreviation::h, AxisDirection::UP, linearUnit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS with a Longitude (first) and Latitude
 * (second) axis.
 *
 * @param unit Angular unit of the axes.
 * @return a new EllipsoidalCS.
 */
EllipsoidalCSNNPtr
EllipsoidalCS::createLongitudeLatitude(const common::UnitOfMeasure &unit) {
    return EllipsoidalCS::create(util::PropertyMap(),
                                 CoordinateSystemAxis::createLONG_EAST(unit),
                                 CoordinateSystemAxis::createLAT_NORTH(unit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a EllipsoidalCS with a Longitude (first), Latitude
 * (second) axis and ellipsoidal height (third) axis.
 *
 * @param angularUnit Angular unit of the latitude and longitude axes.
 * @param linearUnit Linear unit of the ellipsoidal height axis.
 * @return a new EllipsoidalCS.
 * @since 7.0
 */
EllipsoidalCSNNPtr EllipsoidalCS::createLongitudeLatitudeEllipsoidalHeight(
    const common::UnitOfMeasure &angularUnit,
    const common::UnitOfMeasure &linearUnit) {
    return EllipsoidalCS::create(
        util::PropertyMap(), CoordinateSystemAxis::createLONG_EAST(angularUnit),
        CoordinateSystemAxis::createLAT_NORTH(angularUnit),
        CoordinateSystemAxis::create(
            util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                    AxisName::Ellipsoidal_height),
            AxisAbbreviation::h, AxisDirection::UP, linearUnit));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
/** \brief Return the axis order in an enumerated way. */
EllipsoidalCS::AxisOrder EllipsoidalCS::axisOrder() const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    const auto &dir0 = l_axisList[0]->direction();
    const auto &dir1 = l_axisList[1]->direction();
    if (&dir0 == &AxisDirection::NORTH && &dir1 == &AxisDirection::EAST) {
        if (l_axisList.size() == 2) {
            return AxisOrder::LAT_NORTH_LONG_EAST;
        } else if (&l_axisList[2]->direction() == &AxisDirection::UP) {
            return AxisOrder::LAT_NORTH_LONG_EAST_HEIGHT_UP;
        }
    } else if (&dir0 == &AxisDirection::EAST &&
               &dir1 == &AxisDirection::NORTH) {
        if (l_axisList.size() == 2) {
            return AxisOrder::LONG_EAST_LAT_NORTH;
        } else if (&l_axisList[2]->direction() == &AxisDirection::UP) {
            return AxisOrder::LONG_EAST_LAT_NORTH_HEIGHT_UP;
        }
    }

    return AxisOrder::OTHER;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
EllipsoidalCSNNPtr EllipsoidalCS::alterAngularUnit(
    const common::UnitOfMeasure &angularUnit) const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    if (l_axisList.size() == 2) {
        return EllipsoidalCS::create(util::PropertyMap(),
                                     l_axisList[0]->alterUnit(angularUnit),
                                     l_axisList[1]->alterUnit(angularUnit));
    } else {
        assert(l_axisList.size() == 3);
        return EllipsoidalCS::create(
            util::PropertyMap(), l_axisList[0]->alterUnit(angularUnit),
            l_axisList[1]->alterUnit(angularUnit), l_axisList[2]);
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
EllipsoidalCSNNPtr
EllipsoidalCS::alterLinearUnit(const common::UnitOfMeasure &linearUnit) const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    if (l_axisList.size() == 2) {
        return EllipsoidalCS::create(util::PropertyMap(), l_axisList[0],
                                     l_axisList[1]);
    } else {
        assert(l_axisList.size() == 3);
        return EllipsoidalCS::create(util::PropertyMap(), l_axisList[0],
                                     l_axisList[1],
                                     l_axisList[2]->alterUnit(linearUnit));
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalCS::~VerticalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalCS::VerticalCS(const CoordinateSystemAxisNNPtr &axisIn)
    : CoordinateSystem(std::vector<CoordinateSystemAxisNNPtr>{axisIn}) {}
//! @endcond

// ---------------------------------------------------------------------------

#ifdef notdef
VerticalCS::VerticalCS(const VerticalCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalCS.
 *
 * @param properties See \ref general_properties.
 * @param axis The axis.
 * @return a new VerticalCS.
 */
VerticalCSNNPtr VerticalCS::create(const util::PropertyMap &properties,
                                   const CoordinateSystemAxisNNPtr &axis) {
    auto cs(VerticalCS::nn_make_shared<VerticalCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a VerticalCS with a Gravity-related height axis
 *
 * @param unit linear unit.
 * @return a new VerticalCS.
 */
VerticalCSNNPtr
VerticalCS::createGravityRelatedHeight(const common::UnitOfMeasure &unit) {
    auto cs(VerticalCS::nn_make_shared<VerticalCS>(CoordinateSystemAxis::create(
        util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                "Gravity-related height"),
        "H", AxisDirection::UP, unit)));
    return cs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
VerticalCSNNPtr VerticalCS::alterUnit(const common::UnitOfMeasure &unit) const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    return VerticalCS::nn_make_shared<VerticalCS>(
        l_axisList[0]->alterUnit(unit));
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CartesianCS::~CartesianCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

CartesianCS::CartesianCS(const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

#ifdef notdef
CartesianCS::CartesianCS(const CartesianCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr CartesianCS::create(const util::PropertyMap &properties,
                                     const CoordinateSystemAxisNNPtr &axis1,
                                     const CoordinateSystemAxisNNPtr &axis2) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2};
    auto cs(CartesianCS::nn_make_shared<CartesianCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @param axis3 The third axis.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr CartesianCS::create(const util::PropertyMap &properties,
                                     const CoordinateSystemAxisNNPtr &axis1,
                                     const CoordinateSystemAxisNNPtr &axis2,
                                     const CoordinateSystemAxisNNPtr &axis3) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2, axis3};
    auto cs(CartesianCS::nn_make_shared<CartesianCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS with a Easting (first) and Northing
 * (second) axis.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr
CartesianCS::createEastingNorthing(const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Easting),
                      AxisAbbreviation::E, AxisDirection::EAST, unit),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Northing),
                      AxisAbbreviation::N, AxisDirection::NORTH, unit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS with a Northing (first) and Easting
 * (second) axis.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr
CartesianCS::createNorthingEasting(const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Northing),
                      AxisAbbreviation::N, AxisDirection::NORTH, unit),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Easting),
                      AxisAbbreviation::E, AxisDirection::EAST, unit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS with a Westing (first) and Southing
 * (second) axis.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr
CartesianCS::createWestingSouthing(const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Easting),
                      AxisAbbreviation::Y, AxisDirection::WEST, unit),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Northing),
                      AxisAbbreviation::X, AxisDirection::SOUTH, unit));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS, north-pole centered,
 * with a Easting (first) South-Oriented and
 * Northing (second) South-Oriented axis.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr CartesianCS::createNorthPoleEastingSouthNorthingSouth(
    const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Easting),
                      AxisAbbreviation::E, AxisDirection::SOUTH, unit,
                      Meridian::create(common::Angle(90))),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Northing),
                      AxisAbbreviation::N, AxisDirection::SOUTH, unit,
                      Meridian::create(common::Angle(180))));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS, south-pole centered,
 * with a Easting (first) North-Oriented and
 * Northing (second) North-Oriented axis.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr CartesianCS::createSouthPoleEastingNorthNorthingNorth(
    const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Easting),
                      AxisAbbreviation::E, AxisDirection::NORTH, unit,
                      Meridian::create(common::Angle(90))),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Northing),
                      AxisAbbreviation::N, AxisDirection::NORTH, unit,
                      Meridian::create(common::Angle(0))));
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a CartesianCS with the three geocentric axes.
 *
 * @param unit Linear unit of the axes.
 * @return a new CartesianCS.
 */
CartesianCSNNPtr
CartesianCS::createGeocentric(const common::UnitOfMeasure &unit) {
    return create(util::PropertyMap(),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Geocentric_X),
                      AxisAbbreviation::X, AxisDirection::GEOCENTRIC_X, unit),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Geocentric_Y),
                      AxisAbbreviation::Y, AxisDirection::GEOCENTRIC_Y, unit),
                  CoordinateSystemAxis::create(
                      util::PropertyMap().set(IdentifiedObject::NAME_KEY,
                                              AxisName::Geocentric_Z),
                      AxisAbbreviation::Z, AxisDirection::GEOCENTRIC_Z, unit));
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
CartesianCSNNPtr
CartesianCS::alterUnit(const common::UnitOfMeasure &unit) const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    if (l_axisList.size() == 2) {
        return CartesianCS::create(util::PropertyMap(),
                                   l_axisList[0]->alterUnit(unit),
                                   l_axisList[1]->alterUnit(unit));
    } else {
        assert(l_axisList.size() == 3);
        return CartesianCS::create(
            util::PropertyMap(), l_axisList[0]->alterUnit(unit),
            l_axisList[1]->alterUnit(unit), l_axisList[2]->alterUnit(unit));
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AffineCS::~AffineCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

AffineCS::AffineCS(const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a AffineCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @return a new AffineCS.
 */
AffineCSNNPtr AffineCS::create(const util::PropertyMap &properties,
                               const CoordinateSystemAxisNNPtr &axis1,
                               const CoordinateSystemAxisNNPtr &axis2) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2};
    auto cs(AffineCS::nn_make_shared<AffineCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a AffineCS.
 *
 * @param properties See \ref general_properties.
 * @param axis1 The first axis.
 * @param axis2 The second axis.
 * @param axis3 The third axis.
 * @return a new AffineCS.
 */
AffineCSNNPtr AffineCS::create(const util::PropertyMap &properties,
                               const CoordinateSystemAxisNNPtr &axis1,
                               const CoordinateSystemAxisNNPtr &axis2,
                               const CoordinateSystemAxisNNPtr &axis3) {
    std::vector<CoordinateSystemAxisNNPtr> axis{axis1, axis2, axis3};
    auto cs(AffineCS::nn_make_shared<AffineCS>(axis));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
AffineCSNNPtr AffineCS::alterUnit(const common::UnitOfMeasure &unit) const {
    const auto &l_axisList = CoordinateSystem::getPrivate()->axisList;
    if (l_axisList.size() == 2) {
        return AffineCS::create(util::PropertyMap(),
                                l_axisList[0]->alterUnit(unit),
                                l_axisList[1]->alterUnit(unit));
    } else {
        assert(l_axisList.size() == 3);
        return AffineCS::create(
            util::PropertyMap(), l_axisList[0]->alterUnit(unit),
            l_axisList[1]->alterUnit(unit), l_axisList[2]->alterUnit(unit));
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
OrdinalCS::~OrdinalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

OrdinalCS::OrdinalCS(const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

#ifdef notdef
OrdinalCS::OrdinalCS(const OrdinalCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a OrdinalCS.
 *
 * @param properties See \ref general_properties.
 * @param axisIn List of axis.
 * @return a new OrdinalCS.
 */
OrdinalCSNNPtr
OrdinalCS::create(const util::PropertyMap &properties,
                  const std::vector<CoordinateSystemAxisNNPtr> &axisIn) {
    auto cs(OrdinalCS::nn_make_shared<OrdinalCS>(axisIn));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ParametricCS::~ParametricCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

ParametricCS::ParametricCS(const std::vector<CoordinateSystemAxisNNPtr> &axisIn)
    : CoordinateSystem(axisIn) {}

// ---------------------------------------------------------------------------

#ifdef notdef
ParametricCS::ParametricCS(const ParametricCS &) = default;
#endif

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParametricCS.
 *
 * @param properties See \ref general_properties.
 * @param axisIn Axis.
 * @return a new ParametricCS.
 */
ParametricCSNNPtr
ParametricCS::create(const util::PropertyMap &properties,
                     const CoordinateSystemAxisNNPtr &axisIn) {
    auto cs(ParametricCS::nn_make_shared<ParametricCS>(
        std::vector<CoordinateSystemAxisNNPtr>{axisIn}));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

AxisDirection::AxisDirection(const std::string &nameIn) : CodeList(nameIn) {
    auto lowerName = tolower(nameIn);
    assert(registry.find(lowerName) == registry.end());
    registry[lowerName] = this;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const AxisDirection *
AxisDirection::valueOf(const std::string &nameIn) noexcept {
    auto iter = registry.find(tolower(nameIn));
    if (iter == registry.end())
        return nullptr;
    return iter->second;
}
//! @endcond

// ---------------------------------------------------------------------------

RangeMeaning::RangeMeaning(const std::string &nameIn) : CodeList(nameIn) {
    auto lowerName = tolower(nameIn);
    assert(registry.find(lowerName) == registry.end());
    registry[lowerName] = this;
}

// ---------------------------------------------------------------------------

RangeMeaning::RangeMeaning() : CodeList(std::string()) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const RangeMeaning *RangeMeaning::valueOf(const std::string &nameIn) noexcept {
    auto iter = registry.find(tolower(nameIn));
    if (iter == registry.end())
        return nullptr;
    return iter->second;
}
//! @endcond

//! @cond Doxygen_Suppress
// ---------------------------------------------------------------------------

AxisDirectionWKT1::AxisDirectionWKT1(const std::string &nameIn)
    : CodeList(nameIn) {
    auto lowerName = tolower(nameIn);
    assert(registry.find(lowerName) == registry.end());
    registry[lowerName] = this;
}

// ---------------------------------------------------------------------------

const AxisDirectionWKT1 *AxisDirectionWKT1::valueOf(const std::string &nameIn) {
    auto iter = registry.find(tolower(nameIn));
    if (iter == registry.end())
        return nullptr;
    return iter->second;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalCS::~TemporalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalCS::TemporalCS(const CoordinateSystemAxisNNPtr &axisIn)
    : CoordinateSystem(std::vector<CoordinateSystemAxisNNPtr>{axisIn}) {}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DateTimeTemporalCS::~DateTimeTemporalCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

DateTimeTemporalCS::DateTimeTemporalCS(const CoordinateSystemAxisNNPtr &axisIn)
    : TemporalCS(axisIn) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a DateTimeTemporalCS.
 *
 * @param properties See \ref general_properties.
 * @param axisIn The axis.
 * @return a new DateTimeTemporalCS.
 */
DateTimeTemporalCSNNPtr
DateTimeTemporalCS::create(const util::PropertyMap &properties,
                           const CoordinateSystemAxisNNPtr &axisIn) {
    auto cs(DateTimeTemporalCS::nn_make_shared<DateTimeTemporalCS>(axisIn));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

std::string DateTimeTemporalCS::getWKT2Type(bool use2019Keywords) const {
    return use2019Keywords ? WKT2_2019_TYPE : WKT2_2015_TYPE;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalCountCS::~TemporalCountCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

TemporalCountCS::TemporalCountCS(const CoordinateSystemAxisNNPtr &axisIn)
    : TemporalCS(axisIn) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a TemporalCountCS.
 *
 * @param properties See \ref general_properties.
 * @param axisIn The axis.
 * @return a new TemporalCountCS.
 */
TemporalCountCSNNPtr
TemporalCountCS::create(const util::PropertyMap &properties,
                        const CoordinateSystemAxisNNPtr &axisIn) {
    auto cs(TemporalCountCS::nn_make_shared<TemporalCountCS>(axisIn));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

std::string TemporalCountCS::getWKT2Type(bool use2019Keywords) const {
    return use2019Keywords ? WKT2_2019_TYPE : WKT2_2015_TYPE;
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
TemporalMeasureCS::~TemporalMeasureCS() = default;
//! @endcond

// ---------------------------------------------------------------------------

TemporalMeasureCS::TemporalMeasureCS(const CoordinateSystemAxisNNPtr &axisIn)
    : TemporalCS(axisIn) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a TemporalMeasureCS.
 *
 * @param properties See \ref general_properties.
 * @param axisIn The axis.
 * @return a new TemporalMeasureCS.
 */
TemporalMeasureCSNNPtr
TemporalMeasureCS::create(const util::PropertyMap &properties,
                          const CoordinateSystemAxisNNPtr &axisIn) {
    auto cs(TemporalMeasureCS::nn_make_shared<TemporalMeasureCS>(axisIn));
    cs->setProperties(properties);
    return cs;
}

// ---------------------------------------------------------------------------

std::string TemporalMeasureCS::getWKT2Type(bool use2019Keywords) const {
    return use2019Keywords ? WKT2_2019_TYPE : WKT2_2015_TYPE;
}

} // namespace cs
NS_PROJ_END
