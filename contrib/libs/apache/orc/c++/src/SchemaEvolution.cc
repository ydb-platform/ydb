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

#include "SchemaEvolution.hh"
#include "orc/Exceptions.hh"
#include "orc/Type.hh"

namespace orc {

  SchemaEvolution::SchemaEvolution(const std::shared_ptr<Type>& readType, const Type* fileType)
      : readType_(readType) {
    if (readType_) {
      buildConversion(readType_.get(), fileType);
    } else {
      for (uint64_t i = 0; i <= fileType->getMaximumColumnId(); ++i) {
        safePPDConversionMap_.insert(i);
      }
    }
  }

  const Type* SchemaEvolution::getReadType(const Type& fileType) const {
    auto ret = readTypeMap_.find(fileType.getColumnId());
    return ret == readTypeMap_.cend() ? &fileType : ret->second;
  }

  inline void invalidConversion(const Type* readType, const Type* fileType) {
    throw SchemaEvolutionError("Cannot convert from " + fileType->toString() + " to " +
                               readType->toString());
  }

  struct EnumClassHash {
    template <typename T>
    std::size_t operator()(T t) const {
      return static_cast<std::size_t>(t);
    }
  };

  bool isNumeric(const Type& type) {
    auto kind = type.getKind();
    return kind == BOOLEAN || kind == BYTE || kind == SHORT || kind == INT || kind == LONG ||
           kind == FLOAT || kind == DOUBLE;
  }

  bool isStringVariant(const Type& type) {
    auto kind = type.getKind();
    return kind == STRING || kind == CHAR || kind == VARCHAR;
  }

  bool isDecimal(const Type& type) {
    auto kind = type.getKind();
    return kind == DECIMAL;
  }

  bool isTimestamp(const Type& type) {
    auto kind = type.getKind();
    return kind == TIMESTAMP || kind == TIMESTAMP_INSTANT;
  }

  struct ConversionCheckResult {
    bool isValid;
    bool needConvert;
  };

  ConversionCheckResult checkConversion(const Type& readType, const Type& fileType) {
    ConversionCheckResult ret = {false, false};
    if (readType.getKind() == fileType.getKind()) {
      ret.isValid = true;
      if (fileType.getKind() == CHAR || fileType.getKind() == VARCHAR) {
        ret.needConvert = readType.getMaximumLength() != fileType.getMaximumLength();
      } else if (fileType.getKind() == DECIMAL) {
        ret.needConvert = readType.getPrecision() != fileType.getPrecision() ||
                          readType.getScale() != fileType.getScale();
      }
    } else {
      switch (fileType.getKind()) {
        case BOOLEAN:
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE: {
          ret.isValid = ret.needConvert = isNumeric(readType) || isStringVariant(readType) ||
                                          isDecimal(readType) || isTimestamp(readType);
          break;
        }
        case DECIMAL: {
          ret.isValid = ret.needConvert =
              isNumeric(readType) || isStringVariant(readType) || isTimestamp(readType);
          break;
        }
        case STRING:
        case CHAR:
        case VARCHAR: {
          ret.isValid = ret.needConvert = isStringVariant(readType) || isNumeric(readType) ||
                                          isTimestamp(readType) || isDecimal(readType);
          break;
        }
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
        case DATE:
        case BINARY:
        case GEOMETRY:
        case GEOGRAPHY: {
          // Not support
          break;
        }
        case STRUCT:
        case LIST:
        case MAP:
        case UNION: {
          ret.isValid = ret.needConvert = false;
          break;
        }
        default:
          break;
      }
    }
    return ret;
  }

  void SchemaEvolution::buildConversion(const Type* readType, const Type* fileType) {
    if (fileType == nullptr) {
      throw SchemaEvolutionError("File does not have " + readType->toString());
    }

    auto [valid, convert] = checkConversion(*readType, *fileType);
    if (!valid) {
      invalidConversion(readType, fileType);
    }
    readTypeMap_.emplace(readType->getColumnId(), convert ? readType : fileType);

    // check whether PPD conversion is safe
    buildSafePPDConversionMap(readType, fileType);

    for (uint64_t i = 0; i < readType->getSubtypeCount(); ++i) {
      auto subType = readType->getSubtype(i);
      if (subType) {
        // null subType means that this is a sub column of map/list type
        // and it does not exist in the file. simply skip it.
        buildConversion(subType, fileType->getTypeByColumnId(subType->getColumnId()));
      }
    }
  }

  bool SchemaEvolution::needConvert(const Type& fileType) const {
    auto _readType = getReadType(fileType);
    if (_readType == &fileType) {
      return false;
    }
    // it does not check valid here as verified by buildConversion()
    return checkConversion(*_readType, fileType).needConvert;
  }

  inline bool isPrimitive(const Type* type) {
    auto kind = type->getKind();
    return kind != STRUCT && kind != MAP && kind != LIST && kind != UNION;
  }

  void SchemaEvolution::buildSafePPDConversionMap(const Type* readType, const Type* fileType) {
    if (readType == nullptr || !isPrimitive(readType) || fileType == nullptr ||
        !isPrimitive(fileType)) {
      return;
    }

    bool isSafe = false;
    if (readType == fileType) {
      // short cut for same type
      isSafe = true;
    } else if (readType->getKind() == DECIMAL && fileType->getKind() == DECIMAL) {
      // for decimals alone do equality check to not mess up with precision change
      if (fileType->getPrecision() == readType_->getPrecision() &&
          fileType->getScale() == readType_->getScale()) {
        isSafe = true;
      }
    } else {
      // only integer and string evolutions are safe
      // byte -> short -> int -> long
      // string <-> char <-> varchar
      // NOTE: Float to double evolution is not safe as floats are stored as
      // doubles in ORC's internal index, but when doing predicate evaluation
      // for queries like "select * from orc_float where f = 74.72" the constant
      // on the filter is converted from string -> double so the precisions will
      // be different and the comparison will fail.
      // Soon, we should convert all sargs that compare equality between floats
      // or doubles to range predicates.
      // Similarly string -> char and varchar -> char and vice versa is impossible
      // as ORC stores char with padded spaces in its internal index.
      switch (fileType->getKind()) {
        case BYTE: {
          if (readType_->getKind() == SHORT || readType_->getKind() == INT ||
              readType_->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case SHORT: {
          if (readType_->getKind() == INT || readType_->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case INT: {
          if (readType_->getKind() == LONG) {
            isSafe = true;
          }
          break;
        }
        case STRING: {
          if (readType_->getKind() == VARCHAR) {
            isSafe = true;
          }
          break;
        }
        case VARCHAR: {
          if (readType_->getKind() == STRING) {
            isSafe = true;
          }
          break;
        }
        case BOOLEAN:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BINARY:
        case GEOMETRY:
        case GEOGRAPHY:
        case TIMESTAMP:
        case LIST:
        case MAP:
        case STRUCT:
        case UNION:
        case DECIMAL:
        case DATE:
        case CHAR:
        case TIMESTAMP_INSTANT:
          break;
      }
    }

    if (isSafe) {
      safePPDConversionMap_.insert(fileType->getColumnId());
    }
  }

  bool SchemaEvolution::isSafePPDConversion(uint64_t columnId) const {
    return safePPDConversionMap_.find(columnId) != safePPDConversionMap_.cend();
  }

}  // namespace orc
