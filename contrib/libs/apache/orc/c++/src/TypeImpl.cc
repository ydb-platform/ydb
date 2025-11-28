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

#include "TypeImpl.hh"
#include "Adaptor.hh"
#include "orc/Exceptions.hh"
#include "orc/Type.hh"

#include <iostream>
#include <memory>
#include <sstream>

namespace orc {

  Type::~Type() {
    // PASS
  }

  TypeImpl::TypeImpl(TypeKind kind) {
    parent_ = nullptr;
    columnId_ = -1;
    maximumColumnId_ = -1;
    kind_ = kind;
    maxLength_ = 0;
    precision_ = 0;
    scale_ = 0;
    subtypeCount_ = 0;
  }

  TypeImpl::TypeImpl(TypeKind kind, uint64_t maxLength) {
    parent_ = nullptr;
    columnId_ = -1;
    maximumColumnId_ = -1;
    kind_ = kind;
    maxLength_ = maxLength;
    precision_ = 0;
    scale_ = 0;
    subtypeCount_ = 0;
  }

  TypeImpl::TypeImpl(TypeKind kind, uint64_t precision, uint64_t scale) {
    parent_ = nullptr;
    columnId_ = -1;
    maximumColumnId_ = -1;
    kind_ = kind;
    maxLength_ = 0;
    precision_ = precision;
    scale_ = scale;
    subtypeCount_ = 0;
  }

  TypeImpl::TypeImpl(TypeKind kind, const std::string& crs) {
    parent_ = nullptr;
    columnId_ = -1;
    maximumColumnId_ = -1;
    kind_ = kind;
    maxLength_ = 0;
    precision_ = 0;
    scale_ = 0;
    subtypeCount_ = 0;
    crs_ = crs;
    edgeInterpolationAlgorithm_ = geospatial::EdgeInterpolationAlgorithm::SPHERICAL;
  }

  TypeImpl::TypeImpl(TypeKind kind, const std::string& crs,
                     geospatial::EdgeInterpolationAlgorithm algo) {
    parent_ = nullptr;
    columnId_ = -1;
    maximumColumnId_ = -1;
    kind_ = kind;
    maxLength_ = 0;
    precision_ = 0;
    scale_ = 0;
    subtypeCount_ = 0;
    crs_ = crs;
    edgeInterpolationAlgorithm_ = algo;
  }

  uint64_t TypeImpl::assignIds(uint64_t root) const {
    columnId_ = static_cast<int64_t>(root);
    uint64_t current = root + 1;
    for (uint64_t i = 0; i < subtypeCount_; ++i) {
      current = dynamic_cast<TypeImpl*>(subTypes_[i].get())->assignIds(current);
    }
    maximumColumnId_ = static_cast<int64_t>(current) - 1;
    return current;
  }

  void TypeImpl::ensureIdAssigned() const {
    if (columnId_ == -1) {
      const TypeImpl* root = this;
      while (root->parent_ != nullptr) {
        root = root->parent_;
      }
      root->assignIds(0);
    }
  }

  uint64_t TypeImpl::getColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(columnId_);
  }

  uint64_t TypeImpl::getMaximumColumnId() const {
    ensureIdAssigned();
    return static_cast<uint64_t>(maximumColumnId_);
  }

  TypeKind TypeImpl::getKind() const {
    return kind_;
  }

  uint64_t TypeImpl::getSubtypeCount() const {
    return subtypeCount_;
  }

  const Type* TypeImpl::getSubtype(uint64_t i) const {
    return subTypes_[i].get();
  }

  const std::string& TypeImpl::getFieldName(uint64_t i) const {
    return fieldNames_[i];
  }

  uint64_t TypeImpl::getMaximumLength() const {
    return maxLength_;
  }

  uint64_t TypeImpl::getPrecision() const {
    return precision_;
  }

  uint64_t TypeImpl::getScale() const {
    return scale_;
  }

  const std::string& TypeImpl::getCrs() const {
    return crs_;
  }

  geospatial::EdgeInterpolationAlgorithm TypeImpl::getAlgorithm() const {
    return edgeInterpolationAlgorithm_;
  }

  Type& TypeImpl::setAttribute(const std::string& key, const std::string& value) {
    attributes_[key] = value;
    return *this;
  }

  bool TypeImpl::hasAttributeKey(const std::string& key) const {
    return attributes_.find(key) != attributes_.end();
  }

  Type& TypeImpl::removeAttribute(const std::string& key) {
    auto it = attributes_.find(key);
    if (it == attributes_.end()) {
      throw std::range_error("Key not found: " + key);
    }
    attributes_.erase(it);
    return *this;
  }

  std::vector<std::string> TypeImpl::getAttributeKeys() const {
    std::vector<std::string> ret;
    ret.reserve(attributes_.size());
    for (auto& attribute : attributes_) {
      ret.push_back(attribute.first);
    }
    return ret;
  }

  std::string TypeImpl::getAttributeValue(const std::string& key) const {
    auto it = attributes_.find(key);
    if (it == attributes_.end()) {
      throw std::range_error("Key not found: " + key);
    }
    return it->second;
  }

  void TypeImpl::setIds(uint64_t columnId, uint64_t maxColumnId) {
    columnId_ = static_cast<int64_t>(columnId);
    maximumColumnId_ = static_cast<int64_t>(maxColumnId);
  }

  void TypeImpl::addChildType(std::unique_ptr<Type> childType) {
    TypeImpl* child = dynamic_cast<TypeImpl*>(childType.get());
    subTypes_.push_back(std::move(childType));
    if (child != nullptr) {
      child->parent_ = this;
    }
    subtypeCount_ += 1;
  }

  Type* TypeImpl::addStructField(const std::string& fieldName, std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    fieldNames_.push_back(fieldName);
    return this;
  }

  Type* TypeImpl::addUnionChild(std::unique_ptr<Type> fieldType) {
    addChildType(std::move(fieldType));
    return this;
  }

  bool isUnquotedFieldName(std::string fieldName) {
    for (auto& ch : fieldName) {
      if (!isalnum(ch) && ch != '_') {
        return false;
      }
    }
    return true;
  }

  namespace geospatial {
    std::string AlgoToString(EdgeInterpolationAlgorithm algo) {
      switch (algo) {
        case EdgeInterpolationAlgorithm::SPHERICAL:
          return "speherial";
        case VINCENTY:
          return "vincenty";
        case THOMAS:
          return "thomas";
        case ANDOYER:
          return "andoyer";
        case KARNEY:
          return "karney";
        default:
          throw InvalidArgument("Unknown algo");
      }
    }

    EdgeInterpolationAlgorithm AlgoFromString(const std::string& algo) {
      if (algo == "speherial") {
        return EdgeInterpolationAlgorithm::SPHERICAL;
      }
      if (algo == "vincenty") {
        return VINCENTY;
      }
      if (algo == "thomas") {
        return THOMAS;
      }
      if (algo == "andoyer") {
        return ANDOYER;
      }
      if (algo == "karney") {
        return KARNEY;
      }
      throw InvalidArgument("Unknown algo: " + algo);
    }

  }  // namespace geospatial

  std::string TypeImpl::toString() const {
    switch (static_cast<int64_t>(kind_)) {
      case BOOLEAN:
        return "boolean";
      case BYTE:
        return "tinyint";
      case SHORT:
        return "smallint";
      case INT:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case STRING:
        return "string";
      case BINARY:
        return "binary";
      case TIMESTAMP:
        return "timestamp";
      case TIMESTAMP_INSTANT:
        return "timestamp with local time zone";
      case LIST:
        return "array<" + (subTypes_[0] ? subTypes_[0]->toString() : "void") + ">";
      case MAP:
        return "map<" + (subTypes_[0] ? subTypes_[0]->toString() : "void") + "," +
               (subTypes_[1] ? subTypes_[1]->toString() : "void") + ">";
      case STRUCT: {
        std::string result = "struct<";
        for (size_t i = 0; i < subTypes_.size(); ++i) {
          if (i != 0) {
            result += ",";
          }
          if (isUnquotedFieldName(fieldNames_[i])) {
            result += fieldNames_[i];
          } else {
            std::string name(fieldNames_[i]);
            size_t pos = 0;
            while ((pos = name.find("`", pos)) != std::string::npos) {
              name.replace(pos, 1, "``");
              pos += 2;
            }
            result += "`";
            result += name;
            result += "`";
          }
          result += ":";
          result += subTypes_[i]->toString();
        }
        result += ">";
        return result;
      }
      case UNION: {
        std::string result = "uniontype<";
        for (size_t i = 0; i < subTypes_.size(); ++i) {
          if (i != 0) {
            result += ",";
          }
          result += subTypes_[i]->toString();
        }
        result += ">";
        return result;
      }
      case DECIMAL: {
        std::stringstream result;
        result << "decimal(" << precision_ << "," << scale_ << ")";
        return result.str();
      }
      case DATE:
        return "date";
      case VARCHAR: {
        std::stringstream result;
        result << "varchar(" << maxLength_ << ")";
        return result.str();
      }
      case CHAR: {
        std::stringstream result;
        result << "char(" << maxLength_ << ")";
        return result.str();
      }
      case GEOMETRY: {
        std::stringstream result;
        result << "geometry(" << crs_ << ")";
        return result.str();
      }
      case GEOGRAPHY: {
        std::stringstream result;
        result << "geography(" << crs_ << ","
               << geospatial::AlgoToString(edgeInterpolationAlgorithm_) << ")";
        return result.str();
      }
      default:
        throw NotImplementedYet("Unknown type");
    }
  }

  std::unique_ptr<ColumnVectorBatch> TypeImpl::createRowBatch(uint64_t capacity,
                                                              MemoryPool& memoryPool,
                                                              bool encoded) const {
    return createRowBatch(capacity, memoryPool, encoded, /*useTightNumericVector=*/false);
  }

  std::unique_ptr<ColumnVectorBatch> TypeImpl::createRowBatch(uint64_t capacity,
                                                              MemoryPool& memoryPool, bool encoded,
                                                              bool useTightNumericVector) const {
    switch (static_cast<int64_t>(kind_)) {
      case BOOLEAN:
        if (useTightNumericVector) {
          return std::make_unique<ByteVectorBatch>(capacity, memoryPool);
        }
        return std::make_unique<LongVectorBatch>(capacity, memoryPool);
      case BYTE:
        if (useTightNumericVector) {
          return std::make_unique<ByteVectorBatch>(capacity, memoryPool);
        }
        return std::make_unique<LongVectorBatch>(capacity, memoryPool);
      case SHORT:
        if (useTightNumericVector) {
          return std::make_unique<ShortVectorBatch>(capacity, memoryPool);
        }
        return std::make_unique<LongVectorBatch>(capacity, memoryPool);
      case INT:
        if (useTightNumericVector) {
          return std::make_unique<IntVectorBatch>(capacity, memoryPool);
        }
        return std::make_unique<LongVectorBatch>(capacity, memoryPool);
      case LONG:
      case DATE:
        return std::make_unique<LongVectorBatch>(capacity, memoryPool);

      case FLOAT:
        if (useTightNumericVector) {
          return std::make_unique<FloatVectorBatch>(capacity, memoryPool);
        }
        return std::make_unique<DoubleVectorBatch>(capacity, memoryPool);
      case DOUBLE:
        return std::make_unique<DoubleVectorBatch>(capacity, memoryPool);

      case STRING:
      case BINARY:
      case CHAR:
      case VARCHAR:
      case GEOMETRY:
      case GEOGRAPHY:
        return encoded ? std::make_unique<EncodedStringVectorBatch>(capacity, memoryPool)
                       : std::make_unique<StringVectorBatch>(capacity, memoryPool);

      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return std::make_unique<TimestampVectorBatch>(capacity, memoryPool);

      case STRUCT: {
        auto result = std::make_unique<StructVectorBatch>(capacity, memoryPool);
        for (uint64_t i = 0; i < getSubtypeCount(); ++i) {
          result->fields.push_back(
              getSubtype(i)
                  ->createRowBatch(capacity, memoryPool, encoded, useTightNumericVector)
                  .release());
        }
        return result;
      }

      case LIST: {
        auto result = std::make_unique<ListVectorBatch>(capacity, memoryPool);
        if (getSubtype(0) != nullptr) {
          result->elements =
              getSubtype(0)->createRowBatch(capacity, memoryPool, encoded, useTightNumericVector);
        }
        return result;
      }

      case MAP: {
        auto result = std::make_unique<MapVectorBatch>(capacity, memoryPool);
        if (getSubtype(0) != nullptr) {
          result->keys =
              getSubtype(0)->createRowBatch(capacity, memoryPool, encoded, useTightNumericVector);
        }
        if (getSubtype(1) != nullptr) {
          result->elements =
              getSubtype(1)->createRowBatch(capacity, memoryPool, encoded, useTightNumericVector);
        }
        return result;
      }

      case DECIMAL: {
        if (getPrecision() == 0 || getPrecision() > 18) {
          return std::make_unique<Decimal128VectorBatch>(capacity, memoryPool);
        } else {
          return std::make_unique<Decimal64VectorBatch>(capacity, memoryPool);
        }
      }

      case UNION: {
        auto result = std::make_unique<UnionVectorBatch>(capacity, memoryPool);
        for (uint64_t i = 0; i < getSubtypeCount(); ++i) {
          result->children.push_back(
              getSubtype(i)
                  ->createRowBatch(capacity, memoryPool, encoded, useTightNumericVector)
                  .release());
        }
        return result;
      }

      default:
        throw NotImplementedYet("not supported yet");
    }
  }

  std::unique_ptr<Type> createPrimitiveType(TypeKind kind) {
    return std::make_unique<TypeImpl>(kind);
  }

  std::unique_ptr<Type> createCharType(TypeKind kind, uint64_t maxLength) {
    return std::make_unique<TypeImpl>(kind, maxLength);
  }

  std::unique_ptr<Type> createDecimalType(uint64_t precision, uint64_t scale) {
    return std::make_unique<TypeImpl>(DECIMAL, precision, scale);
  }

  std::unique_ptr<Type> createStructType() {
    return std::make_unique<TypeImpl>(STRUCT);
  }

  std::unique_ptr<Type> createListType(std::unique_ptr<Type> elements) {
    auto result = std::make_unique<TypeImpl>(LIST);
    result->addChildType(std::move(elements));
    return result;
  }

  std::unique_ptr<Type> createMapType(std::unique_ptr<Type> key, std::unique_ptr<Type> value) {
    auto result = std::make_unique<TypeImpl>(MAP);
    result->addChildType(std::move(key));
    result->addChildType(std::move(value));
    return result;
  }

  std::unique_ptr<Type> createUnionType() {
    return std::make_unique<TypeImpl>(UNION);
  }

  std::unique_ptr<Type> createGeometryType(const std::string& crs) {
    return std::make_unique<TypeImpl>(GEOMETRY, crs);
  }

  std::unique_ptr<Type> createGeographyType(const std::string& crs,
                                            geospatial::EdgeInterpolationAlgorithm algo) {
    return std::make_unique<TypeImpl>(GEOGRAPHY, crs, algo);
  }

  std::string printProtobufMessage(const google::protobuf::Message& message);
  std::unique_ptr<Type> convertType(const proto::Type& type, const proto::Footer& footer) {
    std::unique_ptr<Type> ret;
    switch (static_cast<int64_t>(type.kind())) {
      case proto::Type_Kind_BOOLEAN:
      case proto::Type_Kind_BYTE:
      case proto::Type_Kind_SHORT:
      case proto::Type_Kind_INT:
      case proto::Type_Kind_LONG:
      case proto::Type_Kind_FLOAT:
      case proto::Type_Kind_DOUBLE:
      case proto::Type_Kind_STRING:
      case proto::Type_Kind_BINARY:
      case proto::Type_Kind_TIMESTAMP:
      case proto::Type_Kind_TIMESTAMP_INSTANT:
      case proto::Type_Kind_DATE:
        ret = std::make_unique<TypeImpl>(static_cast<TypeKind>(type.kind()));
        break;

      case proto::Type_Kind_CHAR:
      case proto::Type_Kind_VARCHAR:
        ret = std::make_unique<TypeImpl>(static_cast<TypeKind>(type.kind()), type.maximum_length());
        break;

      case proto::Type_Kind_GEOMETRY:
        ret = std::make_unique<TypeImpl>(static_cast<TypeKind>(type.kind()), type.crs());
        break;

      case proto::Type_Kind_GEOGRAPHY:
        ret = std::make_unique<TypeImpl>(
            static_cast<TypeKind>(type.kind()), type.crs(),
            static_cast<geospatial::EdgeInterpolationAlgorithm>(type.algorithm()));
        break;

      case proto::Type_Kind_DECIMAL:
        ret = std::make_unique<TypeImpl>(DECIMAL, type.precision(), type.scale());
        break;

      case proto::Type_Kind_LIST:
      case proto::Type_Kind_MAP:
      case proto::Type_Kind_UNION: {
        ret = std::make_unique<TypeImpl>(static_cast<TypeKind>(type.kind()));
        if (type.kind() == proto::Type_Kind_LIST && type.subtypes_size() != 1)
          throw ParseError("Illegal LIST type that doesn't contain one subtype");
        if (type.kind() == proto::Type_Kind_MAP && type.subtypes_size() != 2)
          throw ParseError("Illegal MAP type that doesn't contain two subtypes");
        if (type.kind() == proto::Type_Kind_UNION && type.subtypes_size() == 0)
          throw ParseError("Illegal UNION type that doesn't contain any subtypes");
        for (int i = 0; i < type.subtypes_size(); ++i) {
          ret->addUnionChild(convertType(footer.types(static_cast<int>(type.subtypes(i))), footer));
        }
        break;
      }

      case proto::Type_Kind_STRUCT: {
        ret = std::make_unique<TypeImpl>(STRUCT);
        if (type.subtypes_size() > type.field_names_size())
          throw ParseError("Illegal STRUCT type that contains less field_names than subtypes");
        for (int i = 0; i < type.subtypes_size(); ++i) {
          ret->addStructField(
              type.field_names(i),
              convertType(footer.types(static_cast<int>(type.subtypes(i))), footer));
        }
        break;
      }
      default:
        throw NotImplementedYet("Unknown type kind");
    }
    for (int i = 0; i < type.attributes_size(); ++i) {
      const auto& attribute = type.attributes(i);
      ret->setAttribute(attribute.key(), attribute.value());
    }
    return ret;
  }

  /**
   * Build a clone of the file type, projecting columns from the selected
   * vector. This routine assumes that the parent of any selected column
   * is also selected. The column ids are copied from the fileType.
   * @param fileType the type in the file
   * @param selected is each column by id selected
   * @return a clone of the fileType filtered by the selection array
   */
  std::unique_ptr<Type> buildSelectedType(const Type* fileType, const std::vector<bool>& selected) {
    if (fileType == nullptr || !selected[fileType->getColumnId()]) {
      return nullptr;
    }

    std::unique_ptr<TypeImpl> result;
    switch (static_cast<int>(fileType->getKind())) {
      case BOOLEAN:
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case BINARY:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
      case DATE:
        result = std::make_unique<TypeImpl>(fileType->getKind());
        break;

      case DECIMAL:
        result = std::make_unique<TypeImpl>(fileType->getKind(), fileType->getPrecision(),
                                            fileType->getScale());
        break;

      case VARCHAR:
      case CHAR:
        result = std::make_unique<TypeImpl>(fileType->getKind(), fileType->getMaximumLength());
        break;
      case GEOMETRY:
        result = std::make_unique<TypeImpl>(fileType->getKind(), fileType->getCrs());
        break;
      case GEOGRAPHY:
        result = std::make_unique<TypeImpl>(fileType->getKind(), fileType->getCrs(),
                                            fileType->getAlgorithm());
        break;

      case LIST:
        result = std::make_unique<TypeImpl>(fileType->getKind());
        result->addChildType(buildSelectedType(fileType->getSubtype(0), selected));
        break;

      case MAP:
        result = std::make_unique<TypeImpl>(fileType->getKind());
        result->addChildType(buildSelectedType(fileType->getSubtype(0), selected));
        result->addChildType(buildSelectedType(fileType->getSubtype(1), selected));
        break;

      case STRUCT: {
        result = std::make_unique<TypeImpl>(fileType->getKind());
        for (uint64_t child = 0; child < fileType->getSubtypeCount(); ++child) {
          std::unique_ptr<Type> childType =
              buildSelectedType(fileType->getSubtype(child), selected);
          if (childType.get() != nullptr) {
            result->addStructField(fileType->getFieldName(child), std::move(childType));
          }
        }
        break;
      }

      case UNION: {
        result = std::make_unique<TypeImpl>(fileType->getKind());
        for (uint64_t child = 0; child < fileType->getSubtypeCount(); ++child) {
          std::unique_ptr<Type> childType =
              buildSelectedType(fileType->getSubtype(child), selected);
          if (childType.get() != nullptr) {
            result->addUnionChild(std::move(childType));
          }
        }
        break;
      }

      default:
        throw NotImplementedYet("Unknown type kind");
    }
    result->setIds(fileType->getColumnId(), fileType->getMaximumColumnId());
    for (auto& key : fileType->getAttributeKeys()) {
      const auto& value = fileType->getAttributeValue(key);
      result->setAttribute(key, value);
    }
    return result;
  }

  std::unique_ptr<Type> Type::buildTypeFromString(const std::string& input) {
    size_t size = input.size();
    std::pair<std::unique_ptr<Type>, size_t> res = TypeImpl::parseType(input, 0, size);
    if (res.second != size) {
      throw std::logic_error("Invalid type string.");
    }
    return std::move(res.first);
  }

  std::unique_ptr<Type> TypeImpl::parseArrayType(const std::string& input, size_t start,
                                                 size_t end) {
    auto result = std::make_unique<TypeImpl>(LIST);
    if (input[start] != '<') {
      throw std::logic_error("Missing < after array.");
    }
    std::pair<std::unique_ptr<Type>, size_t> res = TypeImpl::parseType(input, start + 1, end);
    if (res.second != end) {
      throw std::logic_error("Array type must contain exactly one sub type.");
    }
    result->addChildType(std::move(res.first));
    return result;
  }

  std::unique_ptr<Type> TypeImpl::parseMapType(const std::string& input, size_t start, size_t end) {
    auto result = std::make_unique<TypeImpl>(MAP);
    if (input[start] != '<') {
      throw std::logic_error("Missing < after map.");
    }
    std::pair<std::unique_ptr<Type>, size_t> key = TypeImpl::parseType(input, start + 1, end);
    if (input[key.second] != ',') {
      throw std::logic_error("Missing comma after key.");
    }
    std::pair<std::unique_ptr<Type>, size_t> val = TypeImpl::parseType(input, key.second + 1, end);
    if (val.second != end) {
      throw std::logic_error("Map type must contain exactly two sub types.");
    }
    result->addChildType(std::move(key.first));
    result->addChildType(std::move(val.first));
    return result;
  }

  std::pair<std::string, size_t> TypeImpl::parseName(const std::string& input, const size_t start,
                                                     const size_t end) {
    size_t pos = start;
    if (input[pos] == '`') {
      bool closed = false;
      std::ostringstream oss;
      while (pos < end) {
        char ch = input[++pos];
        if (ch == '`') {
          if (pos < end && input[pos + 1] == '`') {
            ++pos;
            oss.put('`');
          } else {
            closed = true;
            break;
          }
        } else {
          oss.put(ch);
        }
      }
      if (!closed) {
        throw std::logic_error("Invalid field name. Unmatched quote");
      }
      if (oss.tellp() == std::streamoff(0)) {
        throw std::logic_error("Empty quoted field name.");
      }
      return std::make_pair(oss.str(), pos + 1);
    } else {
      while (pos < end && (isalnum(input[pos]) || input[pos] == '_')) {
        ++pos;
      }
      if (pos == start) {
        throw std::logic_error("Missing field name.");
      }
      return std::make_pair(input.substr(start, pos - start), pos);
    }
  }

  std::unique_ptr<Type> TypeImpl::parseStructType(const std::string& input, size_t start,
                                                  size_t end) {
    auto result = std::make_unique<TypeImpl>(STRUCT);
    size_t pos = start + 1;
    if (input[start] != '<') {
      throw std::logic_error("Missing < after struct.");
    }
    while (pos < end) {
      std::pair<std::string, size_t> nameRes = parseName(input, pos, end);
      pos = nameRes.second;
      if (input[pos] != ':') {
        throw std::logic_error("Invalid struct type. Field name can not contain '" +
                               std::string(1, input[pos]) + "'.");
      }
      std::pair<std::unique_ptr<Type>, size_t> typeRes = TypeImpl::parseType(input, ++pos, end);
      result->addStructField(nameRes.first, std::move(typeRes.first));
      pos = typeRes.second;
      if (pos != end && input[pos] != ',') {
        throw std::logic_error("Missing comma after field.");
      }
      ++pos;
    }

    return result;
  }

  std::unique_ptr<Type> TypeImpl::parseUnionType(const std::string& input, size_t start,
                                                 size_t end) {
    auto result = std::make_unique<TypeImpl>(UNION);
    size_t pos = start + 1;
    if (input[start] != '<') {
      throw std::logic_error("Missing < after uniontype.");
    }
    while (pos < end) {
      std::pair<std::unique_ptr<Type>, size_t> res = TypeImpl::parseType(input, pos, end);
      result->addChildType(std::move(res.first));
      pos = res.second;
      if (pos != end && input[pos] != ',') {
        throw std::logic_error("Missing comma after union sub type.");
      }
      ++pos;
    }

    return result;
  }

  std::unique_ptr<Type> TypeImpl::parseDecimalType(const std::string& input, size_t start,
                                                   size_t end) {
    if (input[start] != '(') {
      throw std::logic_error("Missing ( after decimal.");
    }
    size_t pos = start + 1;
    size_t sep = input.find(',', pos);
    if (sep + 1 >= end || sep == std::string::npos) {
      throw std::logic_error("Decimal type must specify precision and scale.");
    }
    uint64_t precision = static_cast<uint64_t>(atoi(input.substr(pos, sep - pos).c_str()));
    uint64_t scale = static_cast<uint64_t>(atoi(input.substr(sep + 1, end - sep - 1).c_str()));
    return std::make_unique<TypeImpl>(DECIMAL, precision, scale);
  }

  std::unique_ptr<Type> TypeImpl::parseGeographyType(const std::string& input, size_t start,
                                                     size_t end) {
    if (input[start] != '(') {
      throw std::logic_error("Missing ( after geography.");
    }
    size_t pos = start + 1;
    size_t sep = input.find(',', pos);
    if (sep + 1 >= end || sep == std::string::npos) {
      throw std::logic_error("Geography type must specify CRS.");
    }
    std::string crs = input.substr(pos, sep - pos);
    std::string algoStr = input.substr(sep + 1, end - sep - 1);
    geospatial::EdgeInterpolationAlgorithm algo = geospatial::AlgoFromString(algoStr);
    return std::make_unique<TypeImpl>(GEOGRAPHY, crs, algo);
  }

  void validatePrimitiveType(std::string category, const std::string& input, const size_t pos) {
    if (input[pos] == '<' || input[pos] == '(') {
      std::ostringstream oss;
      oss << "Invalid " << input[pos] << " after " << category << " type.";
      throw std::logic_error(oss.str());
    }
  }

  std::unique_ptr<Type> TypeImpl::parseCategory(std::string category, const std::string& input,
                                                size_t start, size_t end) {
    if (category == "boolean") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(BOOLEAN);
    } else if (category == "tinyint") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(BYTE);
    } else if (category == "smallint") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(SHORT);
    } else if (category == "int") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(INT);
    } else if (category == "bigint") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(LONG);
    } else if (category == "float") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(FLOAT);
    } else if (category == "double") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(DOUBLE);
    } else if (category == "string") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(STRING);
    } else if (category == "binary") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(BINARY);
    } else if (category == "timestamp") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(TIMESTAMP);
    } else if (category == "timestamp with local time zone") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(TIMESTAMP_INSTANT);
    } else if (category == "array") {
      return parseArrayType(input, start, end);
    } else if (category == "map") {
      return parseMapType(input, start, end);
    } else if (category == "struct") {
      return parseStructType(input, start, end);
    } else if (category == "uniontype") {
      return parseUnionType(input, start, end);
    } else if (category == "decimal") {
      return parseDecimalType(input, start, end);
    } else if (category == "date") {
      validatePrimitiveType(category, input, start);
      return std::make_unique<TypeImpl>(DATE);
    } else if (category == "varchar") {
      if (input[start] != '(') {
        throw std::logic_error("Missing ( after varchar.");
      }
      uint64_t maxLength =
          static_cast<uint64_t>(atoi(input.substr(start + 1, end - start + 1).c_str()));
      return std::make_unique<TypeImpl>(VARCHAR, maxLength);
    } else if (category == "char") {
      if (input[start] != '(') {
        throw std::logic_error("Missing ( after char.");
      }
      uint64_t maxLength =
          static_cast<uint64_t>(atoi(input.substr(start + 1, end - start + 1).c_str()));
      return std::make_unique<TypeImpl>(CHAR, maxLength);
    } else if (category == "geometry") {
      if (input[start] != '(') {
        throw std::logic_error("Missing ( after geometry.");
      }
      std::string crs = input.substr(start + 1, end - start + 1);
      return std::make_unique<TypeImpl>(GEOMETRY, crs);
    } else if (category == "geography") {
      return parseGeographyType(input, start, end);
    } else {
      throw std::logic_error("Unknown type " + category);
    }
  }

  std::pair<std::unique_ptr<Type>, size_t> TypeImpl::parseType(const std::string& input,
                                                               size_t start, size_t end) {
    size_t pos = start;
    while (pos < end && (isalpha(input[pos]) || input[pos] == ' ')) {
      ++pos;
    }
    size_t endPos = pos;
    size_t nextPos = pos + 1;
    if (input[pos] == '<') {
      int count = 1;
      while (nextPos < end) {
        if (input[nextPos] == '<') {
          ++count;
        } else if (input[nextPos] == '>') {
          --count;
        }
        if (count == 0) {
          break;
        }
        ++nextPos;
      }
      if (nextPos == end) {
        throw std::logic_error("Invalid type string. Cannot find closing >");
      }
      endPos = nextPos + 1;
    } else if (input[pos] == '(') {
      while (nextPos < end && input[nextPos] != ')') {
        ++nextPos;
      }
      if (nextPos == end) {
        throw std::logic_error("Invalid type string. Cannot find closing )");
      }
      endPos = nextPos + 1;
    }

    std::string category = input.substr(start, pos - start);
    return std::make_pair(parseCategory(category, input, pos, nextPos), endPos);
  }

  const Type* TypeImpl::getTypeByColumnId(uint64_t colIdx) const {
    if (getColumnId() == colIdx) {
      return this;
    }

    for (uint64_t i = 0; i != getSubtypeCount(); ++i) {
      const Type* ret = getSubtype(i)->getTypeByColumnId(colIdx);
      if (ret != nullptr) {
        return ret;
      }
    }
    return nullptr;
  }

}  // namespace orc
