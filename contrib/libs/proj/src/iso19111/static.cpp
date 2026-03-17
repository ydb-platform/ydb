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

#include "proj/common.hpp"
#include "proj/coordinateoperation.hpp"
#include "proj/coordinatesystem.hpp"
#include "proj/crs.hpp"
#include "proj/datum.hpp"
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "operation/oputils.hpp"
#include "proj/internal/coordinatesystem_internal.hpp"
#include "proj/internal/io_internal.hpp"

#include <map>
#include <set>
#include <string>

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

// We put all static definitions in the same compilation unit, and in
// increasing order of dependency, to avoid the "static initialization fiasco"
// See https://isocpp.org/wiki/faq/ctors#static-init-order

using namespace NS_PROJ::crs;
using namespace NS_PROJ::datum;
using namespace NS_PROJ::io;
using namespace NS_PROJ::metadata;
using namespace NS_PROJ::operation;
using namespace NS_PROJ::util;

NS_PROJ_START

// ---------------------------------------------------------------------------

const NameSpaceNNPtr NameSpace::GLOBAL(NameSpace::createGLOBAL());

// ---------------------------------------------------------------------------

/** \brief Key to set the authority citation of a metadata::Identifier.
 *
 * The value is to be provided as a string or a metadata::Citation.
 */
const std::string Identifier::AUTHORITY_KEY("authority");

/** \brief Key to set the code of a metadata::Identifier.
 *
 * The value is to be provided as a integer or a string.
 */
const std::string Identifier::CODE_KEY("code");

/** \brief Key to set the organization responsible for definition and
 * maintenance of the code of a metadata::Identifier.
 *
 * The value is to be provided as a string.
 */
const std::string Identifier::CODESPACE_KEY("codespace");

/** \brief Key to set the version identifier for the namespace of a
 * metadata::Identifier.
 *
 * The value is to be provided as a string.
 */
const std::string Identifier::VERSION_KEY("version");

/** \brief Key to set the natural language description of the meaning of the
 * code value of a metadata::Identifier.
 *
 * The value is to be provided as a string.
 */
const std::string Identifier::DESCRIPTION_KEY("description");

/** \brief Key to set the URI of a metadata::Identifier.
 *
 * The value is to be provided as a string.
 */
const std::string Identifier::URI_KEY("uri");

/** \brief EPSG codespace.
 */
const std::string Identifier::EPSG("EPSG");

/** \brief OGC codespace.
 */
const std::string Identifier::OGC("OGC");

// ---------------------------------------------------------------------------

/** \brief Key to set the name of a common::IdentifiedObject
 *
 * The value is to be provided as a string or metadata::IdentifierNNPtr.
 */
const std::string common::IdentifiedObject::NAME_KEY("name");

/** \brief Key to set the identifier(s) of a common::IdentifiedObject
 *
 * The value is to be provided as a common::IdentifierNNPtr or a
 * util::ArrayOfBaseObjectNNPtr
 * of common::IdentifierNNPtr.
 */
const std::string common::IdentifiedObject::IDENTIFIERS_KEY("identifiers");

/** \brief Key to set the alias(es) of a common::IdentifiedObject
 *
 * The value is to be provided as string, a util::GenericNameNNPtr or a
 * util::ArrayOfBaseObjectNNPtr
 * of util::GenericNameNNPtr.
 */
const std::string common::IdentifiedObject::ALIAS_KEY("alias");

/** \brief Key to set the remarks of a common::IdentifiedObject
 *
 * The value is to be provided as a string.
 */
const std::string common::IdentifiedObject::REMARKS_KEY("remarks");

/** \brief Key to set the deprecation flag of a common::IdentifiedObject
 *
 * The value is to be provided as a boolean.
 */
const std::string common::IdentifiedObject::DEPRECATED_KEY("deprecated");

// ---------------------------------------------------------------------------

/** \brief Key to set the scope of a common::ObjectUsage
 *
 * The value is to be provided as a string.
 */
const std::string common::ObjectUsage::SCOPE_KEY("scope");

/** \brief Key to set the domain of validity of a common::ObjectUsage
 *
 * The value is to be provided as a common::ExtentNNPtr.
 */
const std::string
    common::ObjectUsage::DOMAIN_OF_VALIDITY_KEY("domainOfValidity");

/** \brief Key to set the object domain(s) of a common::ObjectUsage
 *
 * The value is to be provided as a common::ObjectDomainNNPtr or a
 * util::ArrayOfBaseObjectNNPtr
 * of common::ObjectDomainNNPtr.
 */
const std::string common::ObjectUsage::OBJECT_DOMAIN_KEY("objectDomain");

// ---------------------------------------------------------------------------

/** \brief World extent. */
const ExtentNNPtr
    Extent::WORLD(Extent::createFromBBOX(-180, -90, 180, 90,
                                         util::optional<std::string>("World")));

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::vector<std::string> WKTConstants::constants_;

const char *WKTConstants::createAndAddToConstantList(const char *text) {
    WKTConstants::constants_.push_back(text);
    return text;
}

#define DEFINE_WKT_CONSTANT(x)                                                 \
    const std::string WKTConstants::x(createAndAddToConstantList(#x))

DEFINE_WKT_CONSTANT(GEOCCS);
DEFINE_WKT_CONSTANT(GEOGCS);
DEFINE_WKT_CONSTANT(DATUM);
DEFINE_WKT_CONSTANT(UNIT);
DEFINE_WKT_CONSTANT(SPHEROID);
DEFINE_WKT_CONSTANT(AXIS);
DEFINE_WKT_CONSTANT(PRIMEM);
DEFINE_WKT_CONSTANT(AUTHORITY);
DEFINE_WKT_CONSTANT(PROJCS);
DEFINE_WKT_CONSTANT(PROJECTION);
DEFINE_WKT_CONSTANT(PARAMETER);
DEFINE_WKT_CONSTANT(VERT_CS);
DEFINE_WKT_CONSTANT(VERTCS);
DEFINE_WKT_CONSTANT(VERT_DATUM);
DEFINE_WKT_CONSTANT(COMPD_CS);
DEFINE_WKT_CONSTANT(TOWGS84);
DEFINE_WKT_CONSTANT(EXTENSION);
DEFINE_WKT_CONSTANT(LOCAL_CS);
DEFINE_WKT_CONSTANT(LOCAL_DATUM);
DEFINE_WKT_CONSTANT(LINUNIT);

DEFINE_WKT_CONSTANT(GEODCRS);
DEFINE_WKT_CONSTANT(LENGTHUNIT);
DEFINE_WKT_CONSTANT(ANGLEUNIT);
DEFINE_WKT_CONSTANT(SCALEUNIT);
DEFINE_WKT_CONSTANT(TIMEUNIT);
DEFINE_WKT_CONSTANT(ELLIPSOID);
const std::string WKTConstants::CS_(createAndAddToConstantList("CS"));
DEFINE_WKT_CONSTANT(ID);
DEFINE_WKT_CONSTANT(PROJCRS);
DEFINE_WKT_CONSTANT(BASEGEODCRS);
DEFINE_WKT_CONSTANT(MERIDIAN);
DEFINE_WKT_CONSTANT(ORDER);
DEFINE_WKT_CONSTANT(ANCHOR);
DEFINE_WKT_CONSTANT(ANCHOREPOCH);
DEFINE_WKT_CONSTANT(CONVERSION);
DEFINE_WKT_CONSTANT(METHOD);
DEFINE_WKT_CONSTANT(REMARK);
DEFINE_WKT_CONSTANT(GEOGCRS);
DEFINE_WKT_CONSTANT(BASEGEOGCRS);
DEFINE_WKT_CONSTANT(SCOPE);
DEFINE_WKT_CONSTANT(AREA);
DEFINE_WKT_CONSTANT(BBOX);
DEFINE_WKT_CONSTANT(CITATION);
DEFINE_WKT_CONSTANT(URI);
DEFINE_WKT_CONSTANT(VERTCRS);
DEFINE_WKT_CONSTANT(VDATUM);
DEFINE_WKT_CONSTANT(COMPOUNDCRS);
DEFINE_WKT_CONSTANT(PARAMETERFILE);
DEFINE_WKT_CONSTANT(COORDINATEOPERATION);
DEFINE_WKT_CONSTANT(SOURCECRS);
DEFINE_WKT_CONSTANT(TARGETCRS);
DEFINE_WKT_CONSTANT(INTERPOLATIONCRS);
DEFINE_WKT_CONSTANT(OPERATIONACCURACY);
DEFINE_WKT_CONSTANT(CONCATENATEDOPERATION);
DEFINE_WKT_CONSTANT(STEP);
DEFINE_WKT_CONSTANT(BOUNDCRS);
DEFINE_WKT_CONSTANT(ABRIDGEDTRANSFORMATION);
DEFINE_WKT_CONSTANT(DERIVINGCONVERSION);
DEFINE_WKT_CONSTANT(TDATUM);
DEFINE_WKT_CONSTANT(CALENDAR);
DEFINE_WKT_CONSTANT(TIMEORIGIN);
DEFINE_WKT_CONSTANT(TIMECRS);
DEFINE_WKT_CONSTANT(VERTICALEXTENT);
DEFINE_WKT_CONSTANT(TIMEEXTENT);
DEFINE_WKT_CONSTANT(USAGE);
DEFINE_WKT_CONSTANT(DYNAMIC);
DEFINE_WKT_CONSTANT(FRAMEEPOCH);
DEFINE_WKT_CONSTANT(MODEL);
DEFINE_WKT_CONSTANT(VELOCITYGRID);
DEFINE_WKT_CONSTANT(ENSEMBLE);
DEFINE_WKT_CONSTANT(MEMBER);
DEFINE_WKT_CONSTANT(ENSEMBLEACCURACY);
DEFINE_WKT_CONSTANT(DERIVEDPROJCRS);
DEFINE_WKT_CONSTANT(BASEPROJCRS);
DEFINE_WKT_CONSTANT(EDATUM);
DEFINE_WKT_CONSTANT(ENGCRS);
DEFINE_WKT_CONSTANT(PDATUM);
DEFINE_WKT_CONSTANT(PARAMETRICCRS);
DEFINE_WKT_CONSTANT(PARAMETRICUNIT);
DEFINE_WKT_CONSTANT(BASEVERTCRS);
DEFINE_WKT_CONSTANT(BASEENGCRS);
DEFINE_WKT_CONSTANT(BASEPARAMCRS);
DEFINE_WKT_CONSTANT(BASETIMECRS);
DEFINE_WKT_CONSTANT(VERSION);
DEFINE_WKT_CONSTANT(GEOIDMODEL);
DEFINE_WKT_CONSTANT(COORDINATEMETADATA);
DEFINE_WKT_CONSTANT(EPOCH);
DEFINE_WKT_CONSTANT(AXISMINVALUE);
DEFINE_WKT_CONSTANT(AXISMAXVALUE);
DEFINE_WKT_CONSTANT(RANGEMEANING);
DEFINE_WKT_CONSTANT(POINTMOTIONOPERATION);

DEFINE_WKT_CONSTANT(GEODETICCRS);
DEFINE_WKT_CONSTANT(GEODETICDATUM);
DEFINE_WKT_CONSTANT(PROJECTEDCRS);
DEFINE_WKT_CONSTANT(PRIMEMERIDIAN);
DEFINE_WKT_CONSTANT(GEOGRAPHICCRS);
DEFINE_WKT_CONSTANT(TRF);
DEFINE_WKT_CONSTANT(VERTICALCRS);
DEFINE_WKT_CONSTANT(VERTICALDATUM);
DEFINE_WKT_CONSTANT(VRF);
DEFINE_WKT_CONSTANT(TIMEDATUM);
DEFINE_WKT_CONSTANT(TEMPORALQUANTITY);
DEFINE_WKT_CONSTANT(ENGINEERINGDATUM);
DEFINE_WKT_CONSTANT(ENGINEERINGCRS);
DEFINE_WKT_CONSTANT(PARAMETRICDATUM);

//! @endcond

// ---------------------------------------------------------------------------

namespace common {

/** \brief "Empty"/"None", unit of measure of type NONE. */
const UnitOfMeasure UnitOfMeasure::NONE("", 1.0, UnitOfMeasure::Type::NONE);

/** \brief Scale unity, unit of measure of type SCALE. */
const UnitOfMeasure UnitOfMeasure::SCALE_UNITY("unity", 1.0,
                                               UnitOfMeasure::Type::SCALE,
                                               Identifier::EPSG, "9201");

/** \brief Parts-per-million, unit of measure of type SCALE. */
const UnitOfMeasure UnitOfMeasure::PARTS_PER_MILLION("parts per million", 1e-6,
                                                     UnitOfMeasure::Type::SCALE,
                                                     Identifier::EPSG, "9202");

/** \brief Metre, unit of measure of type LINEAR (SI unit). */
const UnitOfMeasure UnitOfMeasure::METRE("metre", 1.0,
                                         UnitOfMeasure::Type::LINEAR,
                                         Identifier::EPSG, "9001");

/** \brief Foot, unit of measure of type LINEAR. */
const UnitOfMeasure UnitOfMeasure::FOOT("foot", 0.3048,
                                        UnitOfMeasure::Type::LINEAR,
                                        Identifier::EPSG, "9002");

/** \brief US survey foot, unit of measure of type LINEAR. */
const UnitOfMeasure UnitOfMeasure::US_FOOT("US survey foot",
                                           0.304800609601219241184,
                                           UnitOfMeasure::Type::LINEAR,
                                           Identifier::EPSG, "9003");

/** \brief Degree, unit of measure of type ANGULAR. */
const UnitOfMeasure UnitOfMeasure::DEGREE("degree", M_PI / 180.,
                                          UnitOfMeasure::Type::ANGULAR,
                                          Identifier::EPSG, "9122");

/** \brief Arc-second, unit of measure of type ANGULAR. */
const UnitOfMeasure UnitOfMeasure::ARC_SECOND("arc-second", M_PI / 180. / 3600.,
                                              UnitOfMeasure::Type::ANGULAR,
                                              Identifier::EPSG, "9104");

/** \brief Grad, unit of measure of type ANGULAR. */
const UnitOfMeasure UnitOfMeasure::GRAD("grad", M_PI / 200.,
                                        UnitOfMeasure::Type::ANGULAR,
                                        Identifier::EPSG, "9105");

/** \brief Radian, unit of measure of type ANGULAR (SI unit). */
const UnitOfMeasure UnitOfMeasure::RADIAN("radian", 1.0,
                                          UnitOfMeasure::Type::ANGULAR,
                                          Identifier::EPSG, "9101");

/** \brief Microradian, unit of measure of type ANGULAR. */
const UnitOfMeasure UnitOfMeasure::MICRORADIAN("microradian", 1e-6,
                                               UnitOfMeasure::Type::ANGULAR,
                                               Identifier::EPSG, "9109");

/** \brief Second, unit of measure of type TIME (SI unit). */
const UnitOfMeasure UnitOfMeasure::SECOND("second", 1.0,
                                          UnitOfMeasure::Type::TIME,
                                          Identifier::EPSG, "1040");

/** \brief Year, unit of measure of type TIME */
const UnitOfMeasure UnitOfMeasure::YEAR("year", 31556925.445,
                                        UnitOfMeasure::Type::TIME,
                                        Identifier::EPSG, "1029");

/** \brief Metre per year, unit of measure of type LINEAR. */
const UnitOfMeasure UnitOfMeasure::METRE_PER_YEAR("metres per year",
                                                  1.0 / 31556925.445,
                                                  UnitOfMeasure::Type::LINEAR,
                                                  Identifier::EPSG, "1042");

/** \brief Arc-second per year, unit of measure of type ANGULAR. */
const UnitOfMeasure UnitOfMeasure::ARC_SECOND_PER_YEAR(
    "arc-seconds per year", M_PI / 180. / 3600. / 31556925.445,
    UnitOfMeasure::Type::ANGULAR, Identifier::EPSG, "1043");

/** \brief Parts-per-million per year, unit of measure of type SCALE. */
const UnitOfMeasure UnitOfMeasure::PPM_PER_YEAR("parts per million per year",
                                                1e-6 / 31556925.445,
                                                UnitOfMeasure::Type::SCALE,
                                                Identifier::EPSG, "1036");

} // namespace common

// ---------------------------------------------------------------------------

namespace cs {
std::map<std::string, const AxisDirection *> AxisDirection::registry;

/** Axis positive direction is north. In a geodetic or projected CRS, north is
 * defined through the geodetic reference frame. In an engineering CRS, north
 * may be defined with respect to an engineering object rather than a
 * geographical direction. */
const AxisDirection AxisDirection::NORTH("north");

/** Axis positive direction is approximately north-north-east. */
const AxisDirection AxisDirection::NORTH_NORTH_EAST("northNorthEast");

/** Axis positive direction is approximately north-east. */
const AxisDirection AxisDirection::NORTH_EAST("northEast");

/** Axis positive direction is approximately east-north-east. */
const AxisDirection AxisDirection::EAST_NORTH_EAST("eastNorthEast");

/** Axis positive direction is 90deg clockwise from north. */
const AxisDirection AxisDirection::EAST("east");

/** Axis positive direction is approximately east-south-east. */
const AxisDirection AxisDirection::EAST_SOUTH_EAST("eastSouthEast");

/** Axis positive direction is approximately south-east. */
const AxisDirection AxisDirection::SOUTH_EAST("southEast");

/** Axis positive direction is approximately south-south-east. */
const AxisDirection AxisDirection::SOUTH_SOUTH_EAST("southSouthEast");

/** Axis positive direction is 180deg clockwise from north. */
const AxisDirection AxisDirection::SOUTH("south");

/** Axis positive direction is approximately south-south-west. */
const AxisDirection AxisDirection::SOUTH_SOUTH_WEST("southSouthWest");

/** Axis positive direction is approximately south-west. */
const AxisDirection AxisDirection::SOUTH_WEST("southWest");

/** Axis positive direction is approximately west-south-west. */
const AxisDirection AxisDirection::WEST_SOUTH_WEST("westSouthWest");

/** Axis positive direction is 270deg clockwise from north. */
const AxisDirection AxisDirection::WEST("west");

/** Axis positive direction is approximately west-north-west. */
const AxisDirection AxisDirection::WEST_NORTH_WEST("westNorthWest");

/** Axis positive direction is approximately north-west. */
const AxisDirection AxisDirection::NORTH_WEST("northWest");

/** Axis positive direction is approximately north-north-west. */
const AxisDirection AxisDirection::NORTH_NORTH_WEST("northNorthWest");

/** Axis positive direction is up relative to gravity. */
const AxisDirection AxisDirection::UP("up");

/** Axis positive direction is down relative to gravity. */
const AxisDirection AxisDirection::DOWN("down");

/** Axis positive direction is in the equatorial plane from the centre of the
 * modelled Earth towards the intersection of the equator with the prime
 * meridian. */
const AxisDirection AxisDirection::GEOCENTRIC_X("geocentricX");

/** Axis positive direction is in the equatorial plane from the centre of the
 * modelled Earth towards the intersection of the equator and the meridian 90deg
 * eastwards from the prime meridian. */
const AxisDirection AxisDirection::GEOCENTRIC_Y("geocentricY");

/** Axis positive direction is from the centre of the modelled Earth parallel to
 * its rotation axis and towards its north pole. */
const AxisDirection AxisDirection::GEOCENTRIC_Z("geocentricZ");

/** Axis positive direction is towards higher pixel column. */
const AxisDirection AxisDirection::COLUMN_POSITIVE("columnPositive");

/** Axis positive direction is towards lower pixel column. */
const AxisDirection AxisDirection::COLUMN_NEGATIVE("columnNegative");

/** Axis positive direction is towards higher pixel row. */
const AxisDirection AxisDirection::ROW_POSITIVE("rowPositive");

/** Axis positive direction is towards lower pixel row. */
const AxisDirection AxisDirection::ROW_NEGATIVE("rowNegative");

/** Axis positive direction is right in display. */
const AxisDirection AxisDirection::DISPLAY_RIGHT("displayRight");

/** Axis positive direction is left in display. */
const AxisDirection AxisDirection::DISPLAY_LEFT("displayLeft");

/** Axis positive direction is towards top of approximately vertical display
 * surface. */
const AxisDirection AxisDirection::DISPLAY_UP("displayUp");

/** Axis positive direction is towards bottom of approximately vertical display
 * surface. */
const AxisDirection AxisDirection::DISPLAY_DOWN("displayDown");

/** Axis positive direction is forward; for an observer at the centre of the
 * object this is will be towards its front, bow or nose. */
const AxisDirection AxisDirection::FORWARD("forward");

/** Axis positive direction is aft; for an observer at the centre of the object
 * this will be towards its back, stern or tail. */
const AxisDirection AxisDirection::AFT("aft");

/** Axis positive direction is port; for an observer at the centre of the object
 * this will be towards its left. */
const AxisDirection AxisDirection::PORT("port");

/** Axis positive direction is starboard; for an observer at the centre of the
 * object this will be towards its right. */
const AxisDirection AxisDirection::STARBOARD("starboard");

/** Axis positive direction is clockwise from a specified direction. */
const AxisDirection AxisDirection::CLOCKWISE("clockwise");

/** Axis positive direction is counter clockwise from a specified direction. */
const AxisDirection AxisDirection::COUNTER_CLOCKWISE("counterClockwise");

/** Axis positive direction is towards the object. */
const AxisDirection AxisDirection::TOWARDS("towards");

/** Axis positive direction is away from the object. */
const AxisDirection AxisDirection::AWAY_FROM("awayFrom");

/** Temporal axis positive direction is towards the future. */
const AxisDirection AxisDirection::FUTURE("future");

/** Temporal axis positive direction is towards the past. */
const AxisDirection AxisDirection::PAST("past");

/** Axis positive direction is unspecified. */
const AxisDirection AxisDirection::UNSPECIFIED("unspecified");

// ---------------------------------------------------------------------------

std::map<std::string, const RangeMeaning *> RangeMeaning::registry;

/** any value between and including minimumValue and maximumValue is valid. */
const RangeMeaning RangeMeaning::EXACT("exact");

/** Axis is continuous with values wrapping around at the minimumValue and
 * maximumValue */
const RangeMeaning RangeMeaning::WRAPAROUND("wraparound");

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress

std::map<std::string, const AxisDirectionWKT1 *> AxisDirectionWKT1::registry;

const AxisDirectionWKT1 AxisDirectionWKT1::NORTH("NORTH");
const AxisDirectionWKT1 AxisDirectionWKT1::EAST("EAST");
const AxisDirectionWKT1 AxisDirectionWKT1::SOUTH("SOUTH");
const AxisDirectionWKT1 AxisDirectionWKT1::WEST("WEST");
const AxisDirectionWKT1 AxisDirectionWKT1::UP("UP");
const AxisDirectionWKT1 AxisDirectionWKT1::DOWN("DOWN");
const AxisDirectionWKT1 AxisDirectionWKT1::OTHER("OTHER");

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const std::string AxisName::Longitude("Longitude");
const std::string AxisName::Latitude("Latitude");
const std::string AxisName::Easting("Easting");
const std::string AxisName::Northing("Northing");
const std::string AxisName::Westing("Westing");
const std::string AxisName::Southing("Southing");
const std::string AxisName::Ellipsoidal_height("Ellipsoidal height");
const std::string AxisName::Geocentric_X("Geocentric X");
const std::string AxisName::Geocentric_Y("Geocentric Y");
const std::string AxisName::Geocentric_Z("Geocentric Z");
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
const std::string AxisAbbreviation::lon("lon");
const std::string AxisAbbreviation::lat("lat");
const std::string AxisAbbreviation::E("E");
const std::string AxisAbbreviation::N("N");
const std::string AxisAbbreviation::h("h");
const std::string AxisAbbreviation::X("X");
const std::string AxisAbbreviation::Y("Y");
const std::string AxisAbbreviation::Z("Z");
//! @endcond

} // namespace cs

// ---------------------------------------------------------------------------

/** \brief The realization is by adjustment of a levelling network fixed to one
 * or more tide gauges. */
const RealizationMethod RealizationMethod::LEVELLING("levelling");
/** \brief The realization is through a geoid height model or a height
 * correction model. This is applied to a specified geodetic CRS. */
const RealizationMethod RealizationMethod::GEOID("geoid");
/** \brief The realization is through a tidal model or by tidal predictions. */
const RealizationMethod RealizationMethod::TIDAL("tidal");

// ---------------------------------------------------------------------------

/** \brief The Greenwich PrimeMeridian */
const PrimeMeridianNNPtr
    PrimeMeridian::GREENWICH(PrimeMeridian::createGREENWICH());
/** \brief The "Reference Meridian" PrimeMeridian.
 *
 * This is a meridian of longitude 0 to be used with non-Earth bodies. */
const PrimeMeridianNNPtr PrimeMeridian::REFERENCE_MERIDIAN(
    PrimeMeridian::createREFERENCE_MERIDIAN());
/** \brief The Paris PrimeMeridian */
const PrimeMeridianNNPtr PrimeMeridian::PARIS(PrimeMeridian::createPARIS());

// ---------------------------------------------------------------------------

/** \brief Earth celestial body */
const std::string Ellipsoid::EARTH("Earth");

/** \brief The EPSG:7008 / "Clarke 1866" Ellipsoid */
const EllipsoidNNPtr Ellipsoid::CLARKE_1866(Ellipsoid::createCLARKE_1866());

/** \brief The EPSG:7030 / "WGS 84" Ellipsoid */
const EllipsoidNNPtr Ellipsoid::WGS84(Ellipsoid::createWGS84());

/** \brief The EPSG:7019 / "GRS 1980" Ellipsoid */
const EllipsoidNNPtr Ellipsoid::GRS1980(Ellipsoid::createGRS1980());

// ---------------------------------------------------------------------------

/** \brief The EPSG:6267 / "North_American_Datum_1927" GeodeticReferenceFrame */
const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::EPSG_6267(
    GeodeticReferenceFrame::createEPSG_6267());

/** \brief The EPSG:6269 / "North_American_Datum_1983" GeodeticReferenceFrame */
const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::EPSG_6269(
    GeodeticReferenceFrame::createEPSG_6269());

/** \brief The EPSG:6326 / "WGS_1984" GeodeticReferenceFrame */
const GeodeticReferenceFrameNNPtr GeodeticReferenceFrame::EPSG_6326(
    GeodeticReferenceFrame::createEPSG_6326());

// ---------------------------------------------------------------------------

/** \brief The proleptic Gregorian calendar. */
const std::string
    TemporalDatum::CALENDAR_PROLEPTIC_GREGORIAN("proleptic Gregorian");

// ---------------------------------------------------------------------------

/** \brief EPSG:4978 / "WGS 84" Geocentric */
const GeodeticCRSNNPtr GeodeticCRS::EPSG_4978(GeodeticCRS::createEPSG_4978());

// ---------------------------------------------------------------------------

/** \brief EPSG:4267 / "NAD27" 2D GeographicCRS */
const GeographicCRSNNPtr
    GeographicCRS::EPSG_4267(GeographicCRS::createEPSG_4267());

/** \brief EPSG:4269 / "NAD83" 2D GeographicCRS */
const GeographicCRSNNPtr
    GeographicCRS::EPSG_4269(GeographicCRS::createEPSG_4269());

/** \brief EPSG:4326 / "WGS 84" 2D GeographicCRS */
const GeographicCRSNNPtr
    GeographicCRS::EPSG_4326(GeographicCRS::createEPSG_4326());

/** \brief OGC:CRS84 / "CRS 84" 2D GeographicCRS (long, lat)*/
const GeographicCRSNNPtr
    GeographicCRS::OGC_CRS84(GeographicCRS::createOGC_CRS84());

/** \brief EPSG:4807 / "NTF (Paris)" 2D GeographicCRS */
const GeographicCRSNNPtr
    GeographicCRS::EPSG_4807(GeographicCRS::createEPSG_4807());

/** \brief EPSG:4979 / "WGS 84" 3D GeographicCRS */
const GeographicCRSNNPtr
    GeographicCRS::EPSG_4979(GeographicCRS::createEPSG_4979());

// ---------------------------------------------------------------------------

/** \brief Key to set the operation version of a operation::CoordinateOperation
 *
 * The value is to be provided as a string.
 */
const std::string
    operation::CoordinateOperation::OPERATION_VERSION_KEY("operationVersion");

//! @cond Doxygen_Suppress
const common::Measure operation::nullMeasure{};

const std::string operation::INVERSE_OF = "Inverse of ";

const std::string operation::AXIS_ORDER_CHANGE_2D_NAME =
    "axis order change (2D)";
const std::string operation::AXIS_ORDER_CHANGE_3D_NAME =
    "axis order change (geographic3D horizontal)";
//! @endcond

// ---------------------------------------------------------------------------

NS_PROJ_END
