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
#include "proj/util.hpp"

#include "proj/internal/internal.hpp"

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace NS_PROJ::internal;

// ---------------------------------------------------------------------------

NS_PROJ_START
namespace operation {

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
struct ParameterValue::Private {
    ParameterValue::Type type_{ParameterValue::Type::STRING};
    std::unique_ptr<common::Measure> measure_{};
    std::unique_ptr<std::string> stringValue_{};
    int integerValue_{};
    bool booleanValue_{};

    explicit Private(const common::Measure &valueIn)
        : type_(ParameterValue::Type::MEASURE),
          measure_(std::make_unique<common::Measure>(valueIn)) {}

    Private(const std::string &stringValueIn, ParameterValue::Type typeIn)
        : type_(typeIn),
          stringValue_(std::make_unique<std::string>(stringValueIn)) {}

    explicit Private(int integerValueIn)
        : type_(ParameterValue::Type::INTEGER), integerValue_(integerValueIn) {}

    explicit Private(bool booleanValueIn)
        : type_(ParameterValue::Type::BOOLEAN), booleanValue_(booleanValueIn) {}
};
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
ParameterValue::~ParameterValue() = default;
//! @endcond

// ---------------------------------------------------------------------------

ParameterValue::ParameterValue(const common::Measure &measureIn)
    : d(std::make_unique<Private>(measureIn)) {}

// ---------------------------------------------------------------------------

ParameterValue::ParameterValue(const std::string &stringValueIn,
                               ParameterValue::Type typeIn)
    : d(std::make_unique<Private>(stringValueIn, typeIn)) {}

// ---------------------------------------------------------------------------

ParameterValue::ParameterValue(int integerValueIn)
    : d(std::make_unique<Private>(integerValueIn)) {}

// ---------------------------------------------------------------------------

ParameterValue::ParameterValue(bool booleanValueIn)
    : d(std::make_unique<Private>(booleanValueIn)) {}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a Measure (i.e. a value associated
 * with a
 * unit)
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr ParameterValue::create(const common::Measure &measureIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(measureIn);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a string value.
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr ParameterValue::create(const char *stringValueIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(
        std::string(stringValueIn), ParameterValue::Type::STRING);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a string value.
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr ParameterValue::create(const std::string &stringValueIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(
        stringValueIn, ParameterValue::Type::STRING);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a filename.
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr
ParameterValue::createFilename(const std::string &stringValueIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(
        stringValueIn, ParameterValue::Type::FILENAME);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a integer value.
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr ParameterValue::create(int integerValueIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(integerValueIn);
}

// ---------------------------------------------------------------------------

/** \brief Instantiate a ParameterValue from a boolean value.
 *
 * @return a new ParameterValue.
 */
ParameterValueNNPtr ParameterValue::create(bool booleanValueIn) {
    return ParameterValue::nn_make_shared<ParameterValue>(booleanValueIn);
}

// ---------------------------------------------------------------------------

/** \brief Returns the type of a parameter value.
 *
 * @return the type.
 */
const ParameterValue::Type &ParameterValue::type() PROJ_PURE_DEFN {
    return d->type_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the value as a Measure (assumes type() == Type::MEASURE)
 * @return the value as a Measure.
 */
const common::Measure &ParameterValue::value() PROJ_PURE_DEFN {
    return *d->measure_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the value as a string (assumes type() == Type::STRING)
 * @return the value as a string.
 */
const std::string &ParameterValue::stringValue() PROJ_PURE_DEFN {
    return *d->stringValue_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the value as a filename (assumes type() == Type::FILENAME)
 * @return the value as a filename.
 */
const std::string &ParameterValue::valueFile() PROJ_PURE_DEFN {
    return *d->stringValue_;
}

// ---------------------------------------------------------------------------

/** \brief Returns the value as a integer (assumes type() == Type::INTEGER)
 * @return the value as a integer.
 */
int ParameterValue::integerValue() PROJ_PURE_DEFN { return d->integerValue_; }

// ---------------------------------------------------------------------------

/** \brief Returns the value as a boolean (assumes type() == Type::BOOLEAN)
 * @return the value as a boolean.
 */
bool ParameterValue::booleanValue() PROJ_PURE_DEFN { return d->booleanValue_; }

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
void ParameterValue::_exportToWKT(io::WKTFormatter *formatter) const {
    const bool isWKT2 = formatter->version() == io::WKTFormatter::Version::WKT2;

    const auto &l_type = type();
    if (l_type == Type::MEASURE) {
        const auto &l_value = value();
        if (formatter->abridgedTransformation()) {
            const auto &unit = l_value.unit();
            const auto &unitType = unit.type();
            if (unitType == common::UnitOfMeasure::Type::LINEAR) {
                formatter->add(l_value.getSIValue());
            } else if (unitType == common::UnitOfMeasure::Type::ANGULAR) {
                formatter->add(
                    l_value.convertToUnit(common::UnitOfMeasure::ARC_SECOND));
            } else if (unit == common::UnitOfMeasure::PARTS_PER_MILLION) {
                formatter->add(1.0 + l_value.value() * 1e-6);
            } else {
                formatter->add(l_value.value());
            }
        } else {
            const auto &unit = l_value.unit();
            if (isWKT2) {
                formatter->add(l_value.value());
            } else {
                // In WKT1, as we don't output the natural unit, output to the
                // registered linear / angular unit.
                const auto &unitType = unit.type();
                if (unitType == common::UnitOfMeasure::Type::LINEAR) {
                    const auto &targetUnit = *(formatter->axisLinearUnit());
                    if (targetUnit.conversionToSI() == 0.0) {
                        throw io::FormattingException(
                            "cannot convert value to target linear unit");
                    }
                    formatter->add(l_value.convertToUnit(targetUnit));
                } else if (unitType == common::UnitOfMeasure::Type::ANGULAR) {
                    const auto &targetUnit = *(formatter->axisAngularUnit());
                    if (targetUnit.conversionToSI() == 0.0) {
                        throw io::FormattingException(
                            "cannot convert value to target angular unit");
                    }
                    formatter->add(l_value.convertToUnit(targetUnit));
                } else {
                    formatter->add(l_value.getSIValue());
                }
            }
            if (isWKT2 && unit != common::UnitOfMeasure::NONE) {
                if (!formatter
                         ->primeMeridianOrParameterUnitOmittedIfSameAsAxis() ||
                    (unit != common::UnitOfMeasure::SCALE_UNITY &&
                     unit != *(formatter->axisLinearUnit()) &&
                     unit != *(formatter->axisAngularUnit()))) {
                    unit._exportToWKT(formatter);
                }
            }
        }
    } else if (l_type == Type::STRING || l_type == Type::FILENAME) {
        formatter->addQuotedString(stringValue());
    } else if (l_type == Type::INTEGER) {
        formatter->add(integerValue());
    } else {
        throw io::FormattingException("boolean parameter value not handled");
    }
}
//! @endcond

// ---------------------------------------------------------------------------

//! @cond Doxygen_Suppress
bool ParameterValue::_isEquivalentTo(const util::IComparable *other,
                                     util::IComparable::Criterion criterion,
                                     const io::DatabaseContextPtr &) const {
    auto otherPV = dynamic_cast<const ParameterValue *>(other);
    if (otherPV == nullptr) {
        return false;
    }
    if (type() != otherPV->type()) {
        return false;
    }
    switch (type()) {
    case Type::MEASURE: {
        return value()._isEquivalentTo(otherPV->value(), criterion, 2e-10);
    }

    case Type::STRING:
    case Type::FILENAME: {
        return stringValue() == otherPV->stringValue();
    }

    case Type::INTEGER: {
        return integerValue() == otherPV->integerValue();
    }

    case Type::BOOLEAN: {
        return booleanValue() == otherPV->booleanValue();
    }

    default: {
        assert(false);
        break;
    }
    }
    return true;
}
//! @endcond
// ---------------------------------------------------------------------------

} // namespace operation

NS_PROJ_END
