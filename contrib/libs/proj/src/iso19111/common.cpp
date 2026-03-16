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
#include "proj/io.hpp"
#include "proj/metadata.hpp"
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"
#include "proj/internal/io_internal.hpp"

#include "proj.h"
#include "proj_internal.h"

#include "proj_json_streaming_writer.hpp"

#include <cmath> // M_PI
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;
using namespace NS_PROJ::io;
using namespace NS_PROJ::metadata;
using namespace NS_PROJ::util;

#if 0
namespace dropbox{ namespace oxygen {
template<> nn<NS_PROJ::common::IdentifiedObjectPtr>::~nn() = default;
template<> nn<NS_PROJ::common::ObjectDomainPtr>::~nn() = default;
template<> nn<NS_PROJ::common::ObjectUsagePtr>::~nn() = default;
template<> nn<NS_PROJ::common::UnitOfMeasurePtr>::~nn() = default;
}}
#endif

NS_PROJ_START
namespace common {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct UnitOfMeasure::Private {
    std::string name_{};
    double toSI_ = 1.0;
    UnitOfMeasure::Type type_{UnitOfMeasure::Type::UNKNOWN};
    std::string codeSpace_{};
    std::string code_{};

    Private(const std::string &nameIn, double toSIIn,
            UnitOfMeasure::Type typeIn, const std::string &codeSpaceIn,
            const std::string &codeIn)
        : name_(nameIn), toSI_(toSIIn), type_(typeIn), codeSpace_(codeSpaceIn),
          code_(codeIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Creates a UnitOfMeasure. */
UnitOfMeasure::UnitOfMeasure(const std::string &nameIn, double toSIIn,
                             UnitOfMeasure::Type typeIn,
                             const std::string &codeSpaceIn,
                             const std::string &codeIn)
    : d(std::make_unique<Private>(nameIn, toSIIn, typeIn, codeSpaceIn,
                                  codeIn)) {}

// ---------------------------------------------------------------------------

UnitOfMeasure::UnitOfMeasure(const UnitOfMeasure &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
UnitOfMeasure::~UnitOfMeasure() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
UnitOfMeasure &UnitOfMeasure::operator=(const UnitOfMeasure &other) {
    if (this != &other) {
        *d = *(other.d);
    }
    return *this;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
UnitOfMeasure &UnitOfMeasure::operator=(UnitOfMeasure &&other) {
    *d = std::move(*(other.d));
    other.d = nullptr;
    BaseObject::operator=(std::move(static_cast<BaseObject &&>(other)));
    return *this;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
UnitOfMeasureNNPtr UnitOfMeasure::create(const UnitOfMeasure &other) {
    return util::nn_make_shared<UnitOfMeasure>(other);
}
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the name of the unit of measure. */
const std::string &UnitOfMeasure::name() PROJ_PURE_DEFN { return d->name_; }

// ---------------------------------------------------------------------------

/** \brief Return the conversion factor to the unit of the
 * International System of Units of the same Type.
 *
 * For example, for foot, this would be 0.3048 (metre)
 *
 * @return the conversion factor, or 0 if no conversion exists.
 */
double UnitOfMeasure::conversionToSI() PROJ_PURE_DEFN { return d->toSI_; }

// ---------------------------------------------------------------------------

/** \brief Return the type of the unit of measure.
 */
UnitOfMeasure::Type UnitOfMeasure::type() PROJ_PURE_DEFN { return d->type_; }

// ---------------------------------------------------------------------------

/** \brief Return the code space of the unit of measure.
 *
 * For example "EPSG"
 *
 * @return the code space, or empty string.
 */
const std::string &UnitOfMeasure::codeSpace() PROJ_PURE_DEFN {
    return d->codeSpace_;
}

// ---------------------------------------------------------------------------

/** \brief Return the code of the unit of measure.
 *
 * @return the code, or empty string.
 */
const std::string &UnitOfMeasure::code() PROJ_PURE_DEFN { return d->code_; }

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void UnitOfMeasure::_exportToWKT(
    WKTFormatter *formatter,
    const std::string &unitType) const // throw(FormattingException)
{
    const bool isWKT2 = formatter->version() == WKTFormatter::Version::WKT2;

    const auto l_type = type();
    if (!unitType.empty()) {
        formatter->startNode(unitType, !codeSpace().empty());
    } else if (formatter->forceUNITKeyword() && l_type != Type::PARAMETRIC) {
        formatter->startNode(WKTConstants::UNIT, !codeSpace().empty());
    } else {
        if (isWKT2 && l_type == Type::LINEAR) {
            formatter->startNode(WKTConstants::LENGTHUNIT,
                                 !codeSpace().empty());
        } else if (isWKT2 && l_type == Type::ANGULAR) {
            formatter->startNode(WKTConstants::ANGLEUNIT, !codeSpace().empty());
        } else if (isWKT2 && l_type == Type::SCALE) {
            formatter->startNode(WKTConstants::SCALEUNIT, !codeSpace().empty());
        } else if (isWKT2 && l_type == Type::TIME) {
            formatter->startNode(WKTConstants::TIMEUNIT, !codeSpace().empty());
        } else if (isWKT2 && l_type == Type::PARAMETRIC) {
            formatter->startNode(WKTConstants::PARAMETRICUNIT,
                                 !codeSpace().empty());
        } else {
            formatter->startNode(WKTConstants::UNIT, !codeSpace().empty());
        }
    }

    {
        const auto &l_name = name();
        const bool esri = formatter->useESRIDialect();
        if (esri) {
            if (ci_equal(l_name, "degree")) {
                formatter->addQuotedString("Degree");
            } else if (ci_equal(l_name, "grad")) {
                formatter->addQuotedString("Grad");
            } else if (ci_equal(l_name, "metre")) {
                formatter->addQuotedString("Meter");
            } else {
                formatter->addQuotedString(l_name);
            }
        } else {
            formatter->addQuotedString(l_name);
        }
        const auto &factor = conversionToSI();
        if (!isWKT2 || l_type != Type::TIME || factor != 0.0) {
            // Some TIMEUNIT do not have a conversion factor
            formatter->add(factor);
        }
        if (!codeSpace().empty() && formatter->outputId()) {
            formatter->startNode(
                isWKT2 ? WKTConstants::ID : WKTConstants::AUTHORITY, false);
            formatter->addQuotedString(codeSpace());
            const auto &l_code = code();
            if (isWKT2) {
                try {
                    (void)std::stoi(l_code);
                    formatter->add(l_code);
                } catch (const std::exception &) {
                    formatter->addQuotedString(l_code);
                }
            } else {
                formatter->addQuotedString(l_code);
            }
            formatter->endNode();
        }
    }
    formatter->endNode();
}

// ---------------------------------------------------------------------------

void UnitOfMeasure::_exportToJSON(
    JSONFormatter *formatter) const // throw(FormattingException)
{
    auto writer = formatter->writer();
    const auto &l_codeSpace = codeSpace();
    auto objContext(
        formatter->MakeObjectContext(nullptr, !l_codeSpace.empty()));
    writer->AddObjKey("type");
    const auto l_type = type();
    if (l_type == Type::LINEAR) {
        writer->Add("LinearUnit");
    } else if (l_type == Type::ANGULAR) {
        writer->Add("AngularUnit");
    } else if (l_type == Type::SCALE) {
        writer->Add("ScaleUnit");
    } else if (l_type == Type::TIME) {
        writer->Add("TimeUnit");
    } else if (l_type == Type::PARAMETRIC) {
        writer->Add("ParametricUnit");
    } else {
        writer->Add("Unit");
    }

    writer->AddObjKey("name");
    const auto &l_name = name();
    writer->Add(l_name);

    const auto &factor = conversionToSI();
    writer->AddObjKey("conversion_factor");
    writer->Add(factor, 15);

    if (!l_codeSpace.empty() && formatter->outputId()) {
        writer->AddObjKey("id");
        auto idContext(formatter->MakeObjectContext(nullptr, false));
        writer->AddObjKey("authority");
        writer->Add(l_codeSpace);
        writer->AddObjKey("code");
        const auto &l_code = code();
        try {
            writer->Add(std::stoi(l_code));
        } catch (const std::exception &) {
            writer->Add(l_code);
        }
    }
}

//! @endcond

// ---------------------------------------------------------------------------

/** Returns whether two units of measures are equal.
 *
 * The comparison is based on the name.
 */
bool UnitOfMeasure::operator==(const UnitOfMeasure &other) PROJ_PURE_DEFN {
    return name() == other.name();
}

// ---------------------------------------------------------------------------

/** Returns whether two units of measures are different.
 *
 * The comparison is based on the name.
 */
bool UnitOfMeasure::operator!=(const UnitOfMeasure &other) PROJ_PURE_DEFN {
    return name() != other.name();
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
std::string UnitOfMeasure::exportToPROJString() const {
    if (type() == Type::LINEAR) {
        auto proj_units = pj_list_linear_units();
        for (int i = 0; proj_units[i].id != nullptr; i++) {
            if (::fabs(proj_units[i].factor - conversionToSI()) <
                1e-10 * conversionToSI()) {
                return proj_units[i].id;
            }
        }
    } else if (type() == Type::ANGULAR) {
        auto proj_angular_units = pj_list_angular_units();
        for (int i = 0; proj_angular_units[i].id != nullptr; i++) {
            if (::fabs(proj_angular_units[i].factor - conversionToSI()) <
                1e-10 * conversionToSI()) {
                return proj_angular_units[i].id;
            }
        }
    }
    return std::string();
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool UnitOfMeasure::_isEquivalentTo(
    const UnitOfMeasure &other, util::IComparable::Criterion criterion) const {
    if (criterion == util::IComparable::Criterion::STRICT) {
        return operator==(other);
    }
    return std::fabs(conversionToSI() - other.conversionToSI()) <=
           1e-10 * std::fabs(conversionToSI());
}

//! @endcond
// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct Measure::Private {
    double value_ = 0.0;
    UnitOfMeasure unit_{};

    Private(double valueIn, const UnitOfMeasure &unitIn)
        : value_(valueIn), unit_(unitIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Measure.
 */
Measure::Measure(double valueIn, const UnitOfMeasure &unitIn)
    : d(std::make_unique<Private>(valueIn, unitIn)) {}

// ---------------------------------------------------------------------------

Measure::Measure(const Measure &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Measure::~Measure() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the unit of the Measure.
 */
const UnitOfMeasure &Measure::unit() PROJ_PURE_DEFN { return d->unit_; }

// ---------------------------------------------------------------------------

/** \brief Return the value of the Measure, after conversion to the
 * corresponding
 * unit of the International System.
 */
double Measure::getSIValue() PROJ_PURE_DEFN {
    return d->value_ * d->unit_.conversionToSI();
}

// ---------------------------------------------------------------------------

/** \brief Return the value of the measure, expressed in the unit()
 */
double Measure::value() PROJ_PURE_DEFN { return d->value_; }

// ---------------------------------------------------------------------------

/** \brief Return the value of this measure expressed into the provided unit.
 */
double Measure::convertToUnit(const UnitOfMeasure &otherUnit) PROJ_PURE_DEFN {
    return getSIValue() / otherUnit.conversionToSI();
}

// ---------------------------------------------------------------------------

/** \brief Return whether two measures are equal.
 *
 * The comparison is done both on the value and the unit.
 */
bool Measure::operator==(const Measure &other) PROJ_PURE_DEFN {
    return d->value_ == other.d->value_ && d->unit_ == other.d->unit_;
}

// ---------------------------------------------------------------------------

/** \brief Returns whether an object is equivalent to another one.
 * @param other other object to compare to
 * @param criterion comparison criterion.
 * @param maxRelativeError Maximum relative error allowed.
 * @return true if objects are equivalent.
 */
bool Measure::_isEquivalentTo(const Measure &other,
                              util::IComparable::Criterion criterion,
                              double maxRelativeError) const {
    if (criterion == util::IComparable::Criterion::STRICT) {
        return operator==(other);
    }
    const double SIValue = getSIValue();
    const double otherSIValue = other.getSIValue();
    // It is arguable that we have to deal with infinite values, but this
    // helps robustify some situations.
    if (std::isinf(SIValue) && std::isinf(otherSIValue))
        return SIValue * otherSIValue > 0;
    return std::fabs(SIValue - otherSIValue) <=
           maxRelativeError * std::fabs(SIValue);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Scale.
 *
 * @param valueIn value
 */
Scale::Scale(double valueIn) : Measure(valueIn, UnitOfMeasure::SCALE_UNITY) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Scale.
 *
 * @param valueIn value
 * @param unitIn unit. Constraint: unit.type() == UnitOfMeasure::Type::SCALE
 */
Scale::Scale(double valueIn, const UnitOfMeasure &unitIn)
    : Measure(valueIn, unitIn) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Scale::Scale(const Scale &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Scale::~Scale() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Angle.
 *
 * @param valueIn value
 */
Angle::Angle(double valueIn) : Measure(valueIn, UnitOfMeasure::DEGREE) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Angle.
 *
 * @param valueIn value
 * @param unitIn unit. Constraint: unit.type() == UnitOfMeasure::Type::ANGULAR
 */
Angle::Angle(double valueIn, const UnitOfMeasure &unitIn)
    : Measure(valueIn, unitIn) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Angle::Angle(const Angle &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Angle::~Angle() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a Length.
 *
 * @param valueIn value
 */
Length::Length(double valueIn) : Measure(valueIn, UnitOfMeasure::METRE) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a Length.
 *
 * @param valueIn value
 * @param unitIn unit. Constraint: unit.type() == UnitOfMeasure::Type::LINEAR
 */
Length::Length(double valueIn, const UnitOfMeasure &unitIn)
    : Measure(valueIn, unitIn) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Length::Length(const Length &) = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
Length::~Length() = default;
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DateTime::Private {
    std::string str_{};

    explicit Private(const std::string &str) : str_(str) {}
};
//! @endcond

// ---------------------------------------------------------------------------

DateTime::DateTime() : d(std::make_unique<Private>(std::string())) {}

// ---------------------------------------------------------------------------

DateTime::DateTime(const std::string &str)
    : d(std::make_unique<Private>(str)) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DateTime::DateTime(const DateTime &other)
    : d(std::make_unique<Private>(*(other.d))) {}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DateTime &DateTime::operator=(const DateTime &other) {
    d->str_ = other.d->str_;
    return *this;
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DateTime::~DateTime() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Instantiate a DateTime. */
DateTime DateTime::create(const std::string &str) { return DateTime(str); }

// ---------------------------------------------------------------------------

/** \brief Return whether the DateTime is ISO:8601 compliant.
 *
 * \remark The current implementation is really simplistic, and aimed at
 * detecting date-times that are not ISO:8601 compliant.
 */
bool DateTime::isISO_8601() const {
    return !d->str_.empty() && d->str_[0] >= '0' && d->str_[0] <= '9' &&
           d->str_.find(' ') == std::string::npos;
}

// ---------------------------------------------------------------------------

/** \brief Return the DateTime as a string.
 */
std::string DateTime::toString() const { return d->str_; }

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
// cppcheck-suppress copyCtorAndEqOperator
struct IdentifiedObject::Private {
    IdentifierNNPtr name{Identifier::create()};
    std::vector<IdentifierNNPtr> identifiers{};
    std::vector<GenericNameNNPtr> aliases{};
    std::string remarks{};
    bool isDeprecated{};

    void setIdentifiers(const PropertyMap &properties);
    void setName(const PropertyMap &properties);
    void setAliases(const PropertyMap &properties);
};
//! @endcond

// ---------------------------------------------------------------------------

IdentifiedObject::IdentifiedObject() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

IdentifiedObject::IdentifiedObject(const IdentifiedObject &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
IdentifiedObject::~IdentifiedObject() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the name of the object.
 *
 * Generally, the only interesting field of the name will be
 * name()->description().
 */
const IdentifierNNPtr &IdentifiedObject::name() PROJ_PURE_DEFN {
    return d->name;
}

// ---------------------------------------------------------------------------

/** \brief Return the name of the object.
 *
 * Return *(name()->description())
 */
const std::string &IdentifiedObject::nameStr() PROJ_PURE_DEFN {
    return *(d->name->description());
}

// ---------------------------------------------------------------------------

/** \brief Return the identifier(s) of the object
 *
 * Generally, those will have Identifier::code() and Identifier::codeSpace()
 * filled.
 */
const std::vector<IdentifierNNPtr> &
IdentifiedObject::identifiers() PROJ_PURE_DEFN {
    return d->identifiers;
}

// ---------------------------------------------------------------------------

/** \brief Return the alias(es) of the object.
 */
const std::vector<GenericNameNNPtr> &
IdentifiedObject::aliases() PROJ_PURE_DEFN {
    return d->aliases;
}

// ---------------------------------------------------------------------------

/** \brief Return the (first) alias of the object as a string.
 *
 * Shortcut for aliases()[0]->toFullyQualifiedName()->toString()
 */
std::string IdentifiedObject::alias() PROJ_PURE_DEFN {
    if (d->aliases.empty())
        return std::string();
    return d->aliases[0]->toFullyQualifiedName()->toString();
}

// ---------------------------------------------------------------------------

/** \brief Return the EPSG code.
 * @return code, or 0 if not found
 */
int IdentifiedObject::getEPSGCode() PROJ_PURE_DEFN {
    for (const auto &id : identifiers()) {
        if (ci_equal(*(id->codeSpace()), metadata::Identifier::EPSG)) {
            return ::atoi(id->code().c_str());
        }
    }
    return 0;
}

// ---------------------------------------------------------------------------

/** \brief Return the remarks.
 */
const std::string &IdentifiedObject::remarks() PROJ_PURE_DEFN {
    return d->remarks;
}

// ---------------------------------------------------------------------------

/** \brief Return whether the object is deprecated.
 *
 * \remark Extension of \ref ISO_19111_2019
 */
bool IdentifiedObject::isDeprecated() PROJ_PURE_DEFN { return d->isDeprecated; }

// ---------------------------------------------------------------------------
//! @cond Doxygen_Suppress

void IdentifiedObject::Private::setName(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    const auto pVal = properties.get(NAME_KEY);
    if (!pVal) {
        return;
    }
    if (const auto genVal = dynamic_cast<const BoxedValue *>(pVal->get())) {
        if (genVal->type() == BoxedValue::Type::STRING) {
            name = Identifier::createFromDescription(genVal->stringValue());
        } else {
            throw InvalidValueTypeException("Invalid value type for " +
                                            NAME_KEY);
        }
    } else {
        if (auto identifier =
                util::nn_dynamic_pointer_cast<Identifier>(*pVal)) {
            name = NN_NO_CHECK(identifier);
        } else {
            throw InvalidValueTypeException("Invalid value type for " +
                                            NAME_KEY);
        }
    }
}

// ---------------------------------------------------------------------------

void IdentifiedObject::Private::setIdentifiers(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    auto pVal = properties.get(IDENTIFIERS_KEY);
    if (!pVal) {

        pVal = properties.get(Identifier::CODE_KEY);
        if (pVal) {
            identifiers.clear();
            identifiers.push_back(
                Identifier::create(std::string(), properties));
        }
        return;
    }
    if (auto identifier = util::nn_dynamic_pointer_cast<Identifier>(*pVal)) {
        identifiers.clear();
        identifiers.push_back(NN_NO_CHECK(identifier));
    } else {
        if (auto array = dynamic_cast<const ArrayOfBaseObject *>(pVal->get())) {
            identifiers.clear();
            for (const auto &val : *array) {
                identifier = util::nn_dynamic_pointer_cast<Identifier>(val);
                if (identifier) {
                    identifiers.push_back(NN_NO_CHECK(identifier));
                } else {
                    throw InvalidValueTypeException("Invalid value type for " +
                                                    IDENTIFIERS_KEY);
                }
            }
        } else {
            throw InvalidValueTypeException("Invalid value type for " +
                                            IDENTIFIERS_KEY);
        }
    }
}

// ---------------------------------------------------------------------------

void IdentifiedObject::Private::setAliases(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    const auto pVal = properties.get(ALIAS_KEY);
    if (!pVal) {
        return;
    }
    if (auto l_name = util::nn_dynamic_pointer_cast<GenericName>(*pVal)) {
        aliases.clear();
        aliases.push_back(NN_NO_CHECK(l_name));
    } else {
        if (const auto array =
                dynamic_cast<const ArrayOfBaseObject *>(pVal->get())) {
            aliases.clear();
            for (const auto &val : *array) {
                l_name = util::nn_dynamic_pointer_cast<GenericName>(val);
                if (l_name) {
                    aliases.push_back(NN_NO_CHECK(l_name));
                } else {
                    if (auto genVal =
                            dynamic_cast<const BoxedValue *>(val.get())) {
                        if (genVal->type() == BoxedValue::Type::STRING) {
                            aliases.push_back(NameFactory::createLocalName(
                                nullptr, genVal->stringValue()));
                        } else {
                            throw InvalidValueTypeException(
                                "Invalid value type for " + ALIAS_KEY);
                        }
                    } else {
                        throw InvalidValueTypeException(
                            "Invalid value type for " + ALIAS_KEY);
                    }
                }
            }
        } else {
            std::string temp;
            if (properties.getStringValue(ALIAS_KEY, temp)) {
                aliases.clear();
                aliases.push_back(NameFactory::createLocalName(nullptr, temp));
            } else {
                throw InvalidValueTypeException("Invalid value type for " +
                                                ALIAS_KEY);
            }
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

void IdentifiedObject::setProperties(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    d->setName(properties);
    d->setIdentifiers(properties);
    d->setAliases(properties);

    properties.getStringValue(REMARKS_KEY, d->remarks);

    {
        const auto pVal = properties.get(DEPRECATED_KEY);
        if (pVal) {
            if (const auto genVal =
                    dynamic_cast<const BoxedValue *>(pVal->get())) {
                if (genVal->type() == BoxedValue::Type::BOOLEAN) {
                    d->isDeprecated = genVal->booleanValue();
                } else {
                    throw InvalidValueTypeException("Invalid value type for " +
                                                    DEPRECATED_KEY);
                }
            } else {
                throw InvalidValueTypeException("Invalid value type for " +
                                                DEPRECATED_KEY);
            }
        }
    }
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void IdentifiedObject::formatID(WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == WKTFormatter::Version::WKT2;
    for (const auto &id : identifiers()) {
        id->_exportToWKT(formatter);
        if (!isWKT2) {
            break;
        }
    }
}

// ---------------------------------------------------------------------------

void IdentifiedObject::formatRemarks(WKTFormatter *formatter) const {
    if (!remarks().empty()) {
        formatter->startNode(WKTConstants::REMARK, false);
        formatter->addQuotedString(remarks());
        formatter->endNode();
    }
}

// ---------------------------------------------------------------------------

void IdentifiedObject::formatID(JSONFormatter *formatter) const {
    const auto &ids(identifiers());
    auto writer = formatter->writer();
    if (ids.size() == 1) {
        writer->AddObjKey("id");
        ids.front()->_exportToJSON(formatter);
    } else if (!ids.empty()) {
        writer->AddObjKey("ids");
        auto arrayContext(writer->MakeArrayContext());
        for (const auto &id : ids) {
            id->_exportToJSON(formatter);
        }
    }
}

// ---------------------------------------------------------------------------

void IdentifiedObject::formatRemarks(JSONFormatter *formatter) const {
    if (!remarks().empty()) {
        auto writer = formatter->writer();
        writer->AddObjKey("remarks");
        writer->Add(remarks());
    }
}

// ---------------------------------------------------------------------------

bool IdentifiedObject::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherIdObj = dynamic_cast<const IdentifiedObject *>(other);
    if (!otherIdObj)
        return false;
    return _isEquivalentTo(otherIdObj, criterion, dbContext);
}

// ---------------------------------------------------------------------------

bool IdentifiedObject::_isEquivalentTo(
    const IdentifiedObject *otherIdObj, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) PROJ_PURE_DEFN {
    if (criterion == util::IComparable::Criterion::STRICT) {
        if (!ci_equal(nameStr(), otherIdObj->nameStr())) {
            return false;
        }
        // TODO test id etc
    } else {
        if (!metadata::Identifier::isEquivalentName(
                nameStr().c_str(), otherIdObj->nameStr().c_str())) {
            return hasEquivalentNameToUsingAlias(otherIdObj, dbContext);
        }
    }
    return true;
}

// ---------------------------------------------------------------------------

bool IdentifiedObject::hasEquivalentNameToUsingAlias(
    const IdentifiedObject *, const io::DatabaseContextPtr &) const {
    return false;
}

//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ObjectDomain::Private {
    optional<std::string> scope_{};
    ExtentPtr domainOfValidity_{};

    Private(const optional<std::string> &scopeIn, const ExtentPtr &extent)
        : scope_(scopeIn), domainOfValidity_(extent) {}
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ObjectDomain::ObjectDomain(const optional<std::string> &scopeIn,
                           const ExtentPtr &extent)
    : d(std::make_unique<Private>(scopeIn, extent)) {}
//! @endcond

// ---------------------------------------------------------------------------

ObjectDomain::ObjectDomain(const ObjectDomain &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ObjectDomain::~ObjectDomain() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the scope.
 *
 * @return the scope, or empty.
 */
const optional<std::string> &ObjectDomain::scope() PROJ_PURE_DEFN {
    return d->scope_;
}

// ---------------------------------------------------------------------------

/** \brief Return the domain of validity.
 *
 * @return the domain of validity, or nullptr.
 */
const ExtentPtr &ObjectDomain::domainOfValidity() PROJ_PURE_DEFN {
    return d->domainOfValidity_;
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ObjectDomain.
 */
ObjectDomainNNPtr ObjectDomain::create(const optional<std::string> &scopeIn,
                                       const ExtentPtr &extent) {
    return ObjectDomain::nn_make_shared<ObjectDomain>(scopeIn, extent);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ObjectDomain::_exportToWKT(WKTFormatter *formatter) const {
    if (d->scope_.has_value()) {
        formatter->startNode(WKTConstants::SCOPE, false);
        formatter->addQuotedString(*(d->scope_));
        formatter->endNode();
    } else if (formatter->use2019Keywords()) {
        formatter->startNode(WKTConstants::SCOPE, false);
        formatter->addQuotedString("unknown");
        formatter->endNode();
    }
    if (d->domainOfValidity_) {
        if (d->domainOfValidity_->description().has_value()) {
            formatter->startNode(WKTConstants::AREA, false);
            formatter->addQuotedString(*(d->domainOfValidity_->description()));
            formatter->endNode();
        }
        if (d->domainOfValidity_->geographicElements().size() == 1) {
            const auto bbox = dynamic_cast<const GeographicBoundingBox *>(
                d->domainOfValidity_->geographicElements()[0].get());
            if (bbox) {
                formatter->startNode(WKTConstants::BBOX, false);
                formatter->add(bbox->southBoundLatitude());
                formatter->add(bbox->westBoundLongitude());
                formatter->add(bbox->northBoundLatitude());
                formatter->add(bbox->eastBoundLongitude());
                formatter->endNode();
            }
        }
        if (d->domainOfValidity_->verticalElements().size() == 1) {
            auto extent = d->domainOfValidity_->verticalElements()[0];
            formatter->startNode(WKTConstants::VERTICALEXTENT, false);
            formatter->add(extent->minimumValue());
            formatter->add(extent->maximumValue());
            extent->unit()->_exportToWKT(formatter);
            formatter->endNode();
        }
        if (d->domainOfValidity_->temporalElements().size() == 1) {
            auto extent = d->domainOfValidity_->temporalElements()[0];
            formatter->startNode(WKTConstants::TIMEEXTENT, false);
            if (DateTime::create(extent->start()).isISO_8601()) {
                formatter->add(extent->start());
            } else {
                formatter->addQuotedString(extent->start());
            }
            if (DateTime::create(extent->stop()).isISO_8601()) {
                formatter->add(extent->stop());
            } else {
                formatter->addQuotedString(extent->stop());
            }
            formatter->endNode();
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ObjectDomain::_exportToJSON(JSONFormatter *formatter) const {
    auto writer = formatter->writer();
    if (d->scope_.has_value()) {
        writer->AddObjKey("scope");
        writer->Add(*(d->scope_));
    }
    if (d->domainOfValidity_) {
        if (d->domainOfValidity_->description().has_value()) {
            writer->AddObjKey("area");
            writer->Add(*(d->domainOfValidity_->description()));
        }
        if (d->domainOfValidity_->geographicElements().size() == 1) {
            const auto bbox = dynamic_cast<const GeographicBoundingBox *>(
                d->domainOfValidity_->geographicElements()[0].get());
            if (bbox) {
                writer->AddObjKey("bbox");
                auto bboxContext(writer->MakeObjectContext());
                writer->AddObjKey("south_latitude");
                writer->Add(bbox->southBoundLatitude(), 15);
                writer->AddObjKey("west_longitude");
                writer->Add(bbox->westBoundLongitude(), 15);
                writer->AddObjKey("north_latitude");
                writer->Add(bbox->northBoundLatitude(), 15);
                writer->AddObjKey("east_longitude");
                writer->Add(bbox->eastBoundLongitude(), 15);
            }
        }
        if (d->domainOfValidity_->verticalElements().size() == 1) {
            const auto &verticalExtent =
                d->domainOfValidity_->verticalElements().front();
            writer->AddObjKey("vertical_extent");
            auto bboxContext(writer->MakeObjectContext());
            writer->AddObjKey("minimum");
            writer->Add(verticalExtent->minimumValue(), 15);
            writer->AddObjKey("maximum");
            writer->Add(verticalExtent->maximumValue(), 15);
            const auto &unit = verticalExtent->unit();
            if (*unit != common::UnitOfMeasure::METRE) {
                writer->AddObjKey("unit");
                unit->_exportToJSON(formatter);
            }
        }
        if (d->domainOfValidity_->temporalElements().size() == 1) {
            const auto &temporalExtent =
                d->domainOfValidity_->temporalElements().front();
            writer->AddObjKey("temporal_extent");
            auto bboxContext(writer->MakeObjectContext());
            writer->AddObjKey("start");
            writer->Add(temporalExtent->start());
            writer->AddObjKey("end");
            writer->Add(temporalExtent->stop());
        }
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool ObjectDomain::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherDomain = dynamic_cast<const ObjectDomain *>(other);
    if (!otherDomain)
        return false;
    if (scope().has_value() != otherDomain->scope().has_value())
        return false;
    if (*scope() != *otherDomain->scope())
        return false;
    if ((domainOfValidity().get() != nullptr) ^
        (otherDomain->domainOfValidity().get() != nullptr))
        return false;
    return domainOfValidity().get() == nullptr ||
           domainOfValidity()->_isEquivalentTo(
               otherDomain->domainOfValidity().get(), criterion, dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ObjectUsage::Private {
    std::vector<ObjectDomainNNPtr> domains_{};
};
//! @endcond

// ---------------------------------------------------------------------------

ObjectUsage::ObjectUsage() : d(std::make_unique<Private>()) {}

// ---------------------------------------------------------------------------

ObjectUsage::ObjectUsage(const ObjectUsage &other)
    : IdentifiedObject(other), d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ObjectUsage::~ObjectUsage() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the domains of the object.
 */
const std::vector<ObjectDomainNNPtr> &ObjectUsage::domains() PROJ_PURE_DEFN {
    return d->domains_;
}

// ---------------------------------------------------------------------------

void ObjectUsage::setProperties(
    const PropertyMap &properties) // throw(InvalidValueTypeException)
{
    IdentifiedObject::setProperties(properties);

    optional<std::string> scope;
    properties.getStringValue(SCOPE_KEY, scope);

    ExtentPtr domainOfValidity;
    {
        const auto pVal = properties.get(DOMAIN_OF_VALIDITY_KEY);
        if (pVal) {
            domainOfValidity = util::nn_dynamic_pointer_cast<Extent>(*pVal);
            if (!domainOfValidity) {
                throw InvalidValueTypeException("Invalid value type for " +
                                                DOMAIN_OF_VALIDITY_KEY);
            }
        }
    }

    if (scope.has_value() || domainOfValidity) {
        d->domains_.emplace_back(ObjectDomain::create(scope, domainOfValidity));
    }

    {
        const auto pVal = properties.get(OBJECT_DOMAIN_KEY);
        if (pVal) {
            if (auto objectDomain =
                    util::nn_dynamic_pointer_cast<ObjectDomain>(*pVal)) {
                d->domains_.emplace_back(NN_NO_CHECK(objectDomain));
            } else if (const auto array =
                           dynamic_cast<const ArrayOfBaseObject *>(
                               pVal->get())) {
                for (const auto &val : *array) {
                    objectDomain =
                        util::nn_dynamic_pointer_cast<ObjectDomain>(val);
                    if (objectDomain) {
                        d->domains_.emplace_back(NN_NO_CHECK(objectDomain));
                    } else {
                        throw InvalidValueTypeException(
                            "Invalid value type for " + OBJECT_DOMAIN_KEY);
                    }
                }
            } else {
                throw InvalidValueTypeException("Invalid value type for " +
                                                OBJECT_DOMAIN_KEY);
            }
        }
    }
}

// ---------------------------------------------------------------------------

void ObjectUsage::baseExportToWKT(WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == WKTFormatter::Version::WKT2;
    if (isWKT2 && formatter->outputUsage()) {
        auto l_domains = domains();
        if (!l_domains.empty()) {
            if (formatter->use2019Keywords()) {
                for (const auto &domain : l_domains) {
                    formatter->startNode(WKTConstants::USAGE, false);
                    domain->_exportToWKT(formatter);
                    formatter->endNode();
                }
            } else {
                l_domains[0]->_exportToWKT(formatter);
            }
        }
    }
    if (formatter->outputId()) {
        formatID(formatter);
    }
    if (isWKT2) {
        formatRemarks(formatter);
    }
}

// ---------------------------------------------------------------------------

void ObjectUsage::baseExportToJSON(JSONFormatter *formatter) const {

    auto writer = formatter->writer();
    if (formatter->outputUsage()) {
        const auto &l_domains = domains();
        if (l_domains.size() == 1) {
            l_domains[0]->_exportToJSON(formatter);
        } else if (!l_domains.empty()) {
            writer->AddObjKey("usages");
            auto arrayContext(writer->MakeArrayContext(false));
            for (const auto &domain : l_domains) {
                auto objContext(writer->MakeObjectContext());
                domain->_exportToJSON(formatter);
            }
        }
    }

    if (formatter->outputId()) {
        formatID(formatter);
    }
    formatRemarks(formatter);
}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool ObjectUsage::_isEquivalentTo(
    const util::IComparable *other, util::IComparable::Criterion criterion,
    const io::DatabaseContextPtr &dbContext) const {
    auto otherObjUsage = dynamic_cast<const ObjectUsage *>(other);
    if (!otherObjUsage)
        return false;

    // TODO: incomplete
    return IdentifiedObject::_isEquivalentTo(other, criterion, dbContext);
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct DataEpoch::Private {
    Measure coordinateEpoch_{};

    explicit Private(const Measure &coordinateEpochIn)
        : coordinateEpoch_(coordinateEpochIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

DataEpoch::DataEpoch() : d(std::make_unique<Private>(Measure())) {}

// ---------------------------------------------------------------------------

DataEpoch::DataEpoch(const Measure &coordinateEpochIn)
    : d(std::make_unique<Private>(coordinateEpochIn)) {}

// ---------------------------------------------------------------------------

DataEpoch::DataEpoch(const DataEpoch &other)
    : d(std::make_unique<Private>(*(other.d))) {}

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
DataEpoch::~DataEpoch() = default;
//! @endcond

// ---------------------------------------------------------------------------

/** \brief Return the coordinate epoch, as a measure in decimal year.
 */
const Measure &DataEpoch::coordinateEpoch() const {
    return d->coordinateEpoch_;
}

} // namespace common
NS_PROJ_END
