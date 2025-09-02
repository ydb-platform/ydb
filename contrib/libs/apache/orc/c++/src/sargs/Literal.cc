/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "orc/sargs/Literal.hh"

#include <cmath>
#include <functional>
#include <limits>
#include <sstream>

namespace orc {

  Literal::Literal(PredicateDataType type) {
    type_ = type;
    value_.DecimalVal = 0;
    size_ = 0;
    isNull_ = true;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = 0;
  }

  Literal::Literal(int64_t val) {
    type_ = PredicateDataType::LONG;
    value_.IntVal = val;
    size_ = sizeof(val);
    isNull_ = false;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = hashCode();
  }

  Literal::Literal(double val) {
    type_ = PredicateDataType::FLOAT;
    value_.DoubleVal = val;
    size_ = sizeof(val);
    isNull_ = false;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = hashCode();
  }

  Literal::Literal(bool val) {
    type_ = PredicateDataType::BOOLEAN;
    value_.BooleanVal = val;
    size_ = sizeof(val);
    isNull_ = false;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = hashCode();
  }

  Literal::Literal(PredicateDataType type, int64_t val) {
    if (type != PredicateDataType::DATE) {
      throw std::invalid_argument("only DATE is supported here!");
    }
    type_ = type;
    value_.IntVal = val;
    size_ = sizeof(val);
    isNull_ = false;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = hashCode();
  }

  Literal::Literal(const char* str, size_t size) {
    type_ = PredicateDataType::STRING;
    value_.Buffer = new char[size];
    memcpy(value_.Buffer, str, size);
    size_ = size;
    isNull_ = false;
    precision_ = 0;
    scale_ = 0;
    hashCode_ = hashCode();
  }

  Literal::Literal(Int128 val, int32_t precision, int32_t scale) {
    type_ = PredicateDataType::DECIMAL;
    value_.DecimalVal = val;
    precision_ = precision;
    scale_ = scale;
    size_ = sizeof(Int128);
    isNull_ = false;
    hashCode_ = hashCode();
  }

  Literal::Literal(int64_t second, int32_t nanos) {
    type_ = PredicateDataType::TIMESTAMP;
    value_.TimeStampVal.second = second;
    value_.TimeStampVal.nanos = nanos;
    precision_ = 0;
    scale_ = 0;
    size_ = sizeof(Timestamp);
    isNull_ = false;
    hashCode_ = hashCode();
  }

  Literal::Literal(const Literal& r)
      : type_(r.type_), size_(r.size_), isNull_(r.isNull_), hashCode_(r.hashCode_) {
    if (type_ == PredicateDataType::STRING) {
      value_.Buffer = new char[r.size_];
      memcpy(value_.Buffer, r.value_.Buffer, r.size_);
      precision_ = 0;
      scale_ = 0;
    } else if (type_ == PredicateDataType::DECIMAL) {
      precision_ = r.precision_;
      scale_ = r.scale_;
      value_ = r.value_;
    } else if (type_ == PredicateDataType::TIMESTAMP) {
      value_.TimeStampVal = r.value_.TimeStampVal;
    } else {
      value_ = r.value_;
      precision_ = 0;
      scale_ = 0;
    }
  }

  Literal::~Literal() {
    if (type_ == PredicateDataType::STRING && value_.Buffer) {
      delete[] value_.Buffer;
      value_.Buffer = nullptr;
    }
  }

  Literal& Literal::operator=(const Literal& r) {
    if (this != &r) {
      if (type_ == PredicateDataType::STRING && value_.Buffer) {
        delete[] value_.Buffer;
        value_.Buffer = nullptr;
      }

      type_ = r.type_;
      size_ = r.size_;
      isNull_ = r.isNull_;
      precision_ = r.precision_;
      scale_ = r.scale_;
      if (type_ == PredicateDataType::STRING) {
        value_.Buffer = new char[r.size_];
        memcpy(value_.Buffer, r.value_.Buffer, r.size_);
      } else if (type_ == PredicateDataType::TIMESTAMP) {
        value_.TimeStampVal = r.value_.TimeStampVal;
      } else {
        value_ = r.value_;
      }
      hashCode_ = r.hashCode_;
    }
    return *this;
  }

  std::string Literal::toString() const {
    if (isNull_) {
      return "null";
    }

    std::ostringstream sstream;
    switch (type_) {
      case PredicateDataType::LONG:
        sstream << value_.IntVal;
        break;
      case PredicateDataType::DATE:
        sstream << value_.DateVal;
        break;
      case PredicateDataType::TIMESTAMP:
        sstream << value_.TimeStampVal.second << "." << value_.TimeStampVal.nanos;
        break;
      case PredicateDataType::FLOAT:
        sstream << value_.DoubleVal;
        break;
      case PredicateDataType::BOOLEAN:
        sstream << (value_.BooleanVal ? "true" : "false");
        break;
      case PredicateDataType::STRING:
        sstream << std::string(value_.Buffer, size_);
        break;
      case PredicateDataType::DECIMAL:
        sstream << value_.DecimalVal.toDecimalString(scale_);
        break;
    }
    return sstream.str();
  }

  size_t Literal::hashCode() const {
    if (isNull_) {
      return 0;
    }

    switch (type_) {
      case PredicateDataType::LONG:
        return std::hash<int64_t>{}(value_.IntVal);
      case PredicateDataType::DATE:
        return std::hash<int64_t>{}(value_.DateVal);
      case PredicateDataType::TIMESTAMP:
        return std::hash<int64_t>{}(value_.TimeStampVal.second) * 17 +
               std::hash<int32_t>{}(value_.TimeStampVal.nanos);
      case PredicateDataType::FLOAT:
        return std::hash<double>{}(value_.DoubleVal);
      case PredicateDataType::BOOLEAN:
        return std::hash<bool>{}(value_.BooleanVal);
      case PredicateDataType::STRING:
        return std::hash<std::string>{}(std::string(value_.Buffer, size_));
      case PredicateDataType::DECIMAL:
        // current glibc does not support hash<int128_t>
        return std::hash<int64_t>{}(value_.IntVal);
      default:
        return 0;
    }
  }

  bool Literal::operator==(const Literal& r) const {
    if (this == &r) {
      return true;
    }
    if (hashCode_ != r.hashCode_ || type_ != r.type_ || isNull_ != r.isNull_) {
      return false;
    }

    if (isNull_) {
      return true;
    }

    switch (type_) {
      case PredicateDataType::LONG:
        return value_.IntVal == r.value_.IntVal;
      case PredicateDataType::DATE:
        return value_.DateVal == r.value_.DateVal;
      case PredicateDataType::TIMESTAMP:
        return value_.TimeStampVal == r.value_.TimeStampVal;
      case PredicateDataType::FLOAT:
        return std::fabs(value_.DoubleVal - r.value_.DoubleVal) <
               std::numeric_limits<double>::epsilon();
      case PredicateDataType::BOOLEAN:
        return value_.BooleanVal == r.value_.BooleanVal;
      case PredicateDataType::STRING:
        return size_ == r.size_ && memcmp(value_.Buffer, r.value_.Buffer, size_) == 0;
      case PredicateDataType::DECIMAL:
        return value_.DecimalVal == r.value_.DecimalVal;
      default:
        return true;
    }
  }

  bool Literal::operator!=(const Literal& r) const {
    return !(*this == r);
  }

  inline void validate(const bool& isNull, const PredicateDataType& type,
                       const PredicateDataType& expected) {
    if (isNull) {
      throw std::logic_error("cannot get value when it is null!");
    }
    if (type != expected) {
      throw std::logic_error("predicate type mismatch");
    }
  }

  int64_t Literal::getLong() const {
    validate(isNull_, type_, PredicateDataType::LONG);
    return value_.IntVal;
  }

  int64_t Literal::getDate() const {
    validate(isNull_, type_, PredicateDataType::DATE);
    return value_.DateVal;
  }

  Literal::Timestamp Literal::getTimestamp() const {
    validate(isNull_, type_, PredicateDataType::TIMESTAMP);
    return value_.TimeStampVal;
  }

  double Literal::getFloat() const {
    validate(isNull_, type_, PredicateDataType::FLOAT);
    return value_.DoubleVal;
  }

  std::string Literal::getString() const {
    validate(isNull_, type_, PredicateDataType::STRING);
    return std::string(value_.Buffer, size_);
  }

  bool Literal::getBool() const {
    validate(isNull_, type_, PredicateDataType::BOOLEAN);
    return value_.BooleanVal;
  }

  Decimal Literal::getDecimal() const {
    validate(isNull_, type_, PredicateDataType::DECIMAL);
    return Decimal(value_.DecimalVal, scale_);
  }

}  // namespace orc
