// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet/arrow/schema_internal.h"

#include "arrow/extension/json.h"
#include "arrow/type.h"

#include "parquet/properties.h"

using ArrowType = ::arrow20::DataType;
using ArrowTypeId = ::arrow20::Type;
using ParquetType = parquet20::Type;

namespace parquet20::arrow20 {

using ::arrow20::Result;
using ::arrow20::Status;
using ::arrow20::internal::checked_cast;

Result<std::shared_ptr<ArrowType>> MakeArrowDecimal(const LogicalType& logical_type) {
  const auto& decimal = checked_cast<const DecimalLogicalType&>(logical_type);
  if (decimal.precision() <= ::arrow20::Decimal128Type::kMaxPrecision) {
    return ::arrow20::Decimal128Type::Make(decimal.precision(), decimal.scale());
  }
  return ::arrow20::Decimal256Type::Make(decimal.precision(), decimal.scale());
}

Result<std::shared_ptr<ArrowType>> MakeArrowInt(const LogicalType& logical_type) {
  const auto& integer = checked_cast<const IntLogicalType&>(logical_type);
  switch (integer.bit_width()) {
    case 8:
      return integer.is_signed() ? ::arrow20::int8() : ::arrow20::uint8();
    case 16:
      return integer.is_signed() ? ::arrow20::int16() : ::arrow20::uint16();
    case 32:
      return integer.is_signed() ? ::arrow20::int32() : ::arrow20::uint32();
    default:
      return Status::TypeError(logical_type.ToString(),
                               " cannot annotate physical type Int32");
  }
}

Result<std::shared_ptr<ArrowType>> MakeArrowInt64(const LogicalType& logical_type) {
  const auto& integer = checked_cast<const IntLogicalType&>(logical_type);
  switch (integer.bit_width()) {
    case 64:
      return integer.is_signed() ? ::arrow20::int64() : ::arrow20::uint64();
    default:
      return Status::TypeError(logical_type.ToString(),
                               " cannot annotate physical type Int64");
  }
}

Result<std::shared_ptr<ArrowType>> MakeArrowTime32(const LogicalType& logical_type) {
  const auto& time = checked_cast<const TimeLogicalType&>(logical_type);
  switch (time.time_unit()) {
    case LogicalType::TimeUnit::MILLIS:
      return ::arrow20::time32(::arrow20::TimeUnit::MILLI);
    default:
      return Status::TypeError(logical_type.ToString(),
                               " cannot annotate physical type Time32");
  }
}

Result<std::shared_ptr<ArrowType>> MakeArrowTime64(const LogicalType& logical_type) {
  const auto& time = checked_cast<const TimeLogicalType&>(logical_type);
  switch (time.time_unit()) {
    case LogicalType::TimeUnit::MICROS:
      return ::arrow20::time64(::arrow20::TimeUnit::MICRO);
    case LogicalType::TimeUnit::NANOS:
      return ::arrow20::time64(::arrow20::TimeUnit::NANO);
    default:
      return Status::TypeError(logical_type.ToString(),
                               " cannot annotate physical type Time64");
  }
}

Result<std::shared_ptr<ArrowType>> MakeArrowTimestamp(const LogicalType& logical_type) {
  const auto& timestamp = checked_cast<const TimestampLogicalType&>(logical_type);
  const bool utc_normalized = timestamp.is_adjusted_to_utc();
  static const char* utc_timezone = "UTC";
  switch (timestamp.time_unit()) {
    case LogicalType::TimeUnit::MILLIS:
      return (utc_normalized ? ::arrow20::timestamp(::arrow20::TimeUnit::MILLI, utc_timezone)
                             : ::arrow20::timestamp(::arrow20::TimeUnit::MILLI));
    case LogicalType::TimeUnit::MICROS:
      return (utc_normalized ? ::arrow20::timestamp(::arrow20::TimeUnit::MICRO, utc_timezone)
                             : ::arrow20::timestamp(::arrow20::TimeUnit::MICRO));
    case LogicalType::TimeUnit::NANOS:
      return (utc_normalized ? ::arrow20::timestamp(::arrow20::TimeUnit::NANO, utc_timezone)
                             : ::arrow20::timestamp(::arrow20::TimeUnit::NANO));
    default:
      return Status::TypeError("Unrecognized time unit in timestamp logical_type: ",
                               logical_type.ToString());
  }
}

Result<std::shared_ptr<ArrowType>> FromByteArray(
    const LogicalType& logical_type, const ArrowReaderProperties& reader_properties) {
  switch (logical_type.type()) {
    case LogicalType::Type::STRING:
      return ::arrow20::utf8();
    case LogicalType::Type::DECIMAL:
      return MakeArrowDecimal(logical_type);
    case LogicalType::Type::NONE:
    case LogicalType::Type::ENUM:
    case LogicalType::Type::BSON:
      return ::arrow20::binary();
    case LogicalType::Type::JSON:
      if (reader_properties.get_arrow_extensions_enabled()) {
        return ::arrow20::extension::json(::arrow20::utf8());
      }
      // When the original Arrow schema isn't stored and Arrow extensions are disabled,
      // LogicalType::JSON is read as utf8().
      return ::arrow20::utf8();
    default:
      return Status::NotImplemented("Unhandled logical logical_type ",
                                    logical_type.ToString(), " for binary array");
  }
}

Result<std::shared_ptr<ArrowType>> FromFLBA(const LogicalType& logical_type,
                                            int32_t physical_length) {
  switch (logical_type.type()) {
    case LogicalType::Type::DECIMAL:
      return MakeArrowDecimal(logical_type);
    case LogicalType::Type::FLOAT16:
      return ::arrow20::float16();
    case LogicalType::Type::NONE:
    case LogicalType::Type::INTERVAL:
    case LogicalType::Type::UUID:
      return ::arrow20::fixed_size_binary(physical_length);
    default:
      return Status::NotImplemented("Unhandled logical logical_type ",
                                    logical_type.ToString(),
                                    " for fixed-length binary array");
  }
}

::arrow20::Result<std::shared_ptr<ArrowType>> FromInt32(const LogicalType& logical_type) {
  switch (logical_type.type()) {
    case LogicalType::Type::INT:
      return MakeArrowInt(logical_type);
    case LogicalType::Type::DATE:
      return ::arrow20::date32();
    case LogicalType::Type::TIME:
      return MakeArrowTime32(logical_type);
    case LogicalType::Type::DECIMAL:
      return MakeArrowDecimal(logical_type);
    case LogicalType::Type::NONE:
      return ::arrow20::int32();
    default:
      return Status::NotImplemented("Unhandled logical type ", logical_type.ToString(),
                                    " for INT32");
  }
}

Result<std::shared_ptr<ArrowType>> FromInt64(const LogicalType& logical_type) {
  switch (logical_type.type()) {
    case LogicalType::Type::INT:
      return MakeArrowInt64(logical_type);
    case LogicalType::Type::DECIMAL:
      return MakeArrowDecimal(logical_type);
    case LogicalType::Type::TIMESTAMP:
      return MakeArrowTimestamp(logical_type);
    case LogicalType::Type::TIME:
      return MakeArrowTime64(logical_type);
    case LogicalType::Type::NONE:
      return ::arrow20::int64();
    default:
      return Status::NotImplemented("Unhandled logical type ", logical_type.ToString(),
                                    " for INT64");
  }
}

Result<std::shared_ptr<ArrowType>> GetArrowType(
    Type::type physical_type, const LogicalType& logical_type, int type_length,
    const ArrowReaderProperties& reader_properties) {
  if (logical_type.is_null()) {
    return ::arrow20::null();
  }

  if (logical_type.is_invalid()) {
    return GetArrowType(physical_type, *NoLogicalType::Make(), type_length,
                        reader_properties);
  }

  switch (physical_type) {
    case ParquetType::BOOLEAN:
      return ::arrow20::boolean();
    case ParquetType::INT32:
      return FromInt32(logical_type);
    case ParquetType::INT64:
      return FromInt64(logical_type);
    case ParquetType::INT96:
      return ::arrow20::timestamp(reader_properties.coerce_int96_timestamp_unit());
    case ParquetType::FLOAT:
      return ::arrow20::float32();
    case ParquetType::DOUBLE:
      return ::arrow20::float64();
    case ParquetType::BYTE_ARRAY:
      return FromByteArray(logical_type, reader_properties);
    case ParquetType::FIXED_LEN_BYTE_ARRAY:
      return FromFLBA(logical_type, type_length);
    default: {
      // PARQUET-1565: This can occur if the file is corrupt
      return Status::IOError("Invalid physical column type: ",
                             TypeToString(physical_type));
    }
  }
}

Result<std::shared_ptr<ArrowType>> GetArrowType(
    const schema::PrimitiveNode& primitive,
    const ArrowReaderProperties& reader_properties) {
  return GetArrowType(primitive.physical_type(), *primitive.logical_type(),
                      primitive.type_length(), reader_properties);
}

}  // namespace parquet20::arrow20
