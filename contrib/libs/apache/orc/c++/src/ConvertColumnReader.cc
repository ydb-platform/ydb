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

#include "ConvertColumnReader.hh"

namespace orc {

  // Assume that we are using tight numeric vector batch
  using BooleanVectorBatch = ByteVectorBatch;

  ConvertColumnReader::ConvertColumnReader(const Type& _readType, const Type& fileType,
                                           StripeStreams& stripe, bool _throwOnOverflow)
      : ColumnReader(_readType, stripe), readType(_readType), throwOnOverflow(_throwOnOverflow) {
    reader = buildReader(fileType, stripe, /*useTightNumericVector=*/true,
                         /*throwOnOverflow=*/false, /*convertToReadType*/ false);
    data =
        fileType.createRowBatch(0, memoryPool, /*encoded=*/false, /*useTightNumericVector=*/true);
  }

  void ConvertColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) {
    reader->next(*data, numValues, notNull);
    rowBatch.resize(data->capacity);
    rowBatch.numElements = data->numElements;
    rowBatch.hasNulls = data->hasNulls;
    if (!rowBatch.hasNulls) {
      memset(rowBatch.notNull.data(), 1, data->notNull.size());
    } else {
      memcpy(rowBatch.notNull.data(), data->notNull.data(), data->notNull.size());
    }
  }

  uint64_t ConvertColumnReader::skip(uint64_t numValues) {
    return reader->skip(numValues);
  }

  void ConvertColumnReader::seekToRowGroup(
      std::unordered_map<uint64_t, PositionProvider>& positions) {
    reader->seekToRowGroup(positions);
  }

  static inline bool canFitInLong(double value) {
    constexpr double MIN_LONG_AS_DOUBLE = -0x1p63;
    constexpr double MAX_LONG_AS_DOUBLE_PLUS_ONE = 0x1p63;
    return ((MIN_LONG_AS_DOUBLE - value < 1.0) && (value < MAX_LONG_AS_DOUBLE_PLUS_ONE));
  }

  template <typename FileType, typename ReadType>
  static inline void handleOverflow(ColumnVectorBatch& dstBatch, uint64_t idx, bool shouldThrow) {
    if (!shouldThrow) {
      dstBatch.notNull.data()[idx] = 0;
      dstBatch.hasNulls = true;
    } else {
      std::ostringstream ss;
      ss << "Overflow when convert from " << typeid(FileType).name() << " to "
         << typeid(ReadType).name();
      throw SchemaEvolutionError(ss.str());
    }
  }

  // return false if overflow
  template <typename ReadType>
  static bool downCastToInteger(ReadType& dstValue, int64_t inputLong) {
    dstValue = static_cast<ReadType>(inputLong);
    if constexpr (std::is_same<ReadType, int64_t>::value) {
      return true;
    }
    if (static_cast<int64_t>(dstValue) != inputLong) {
      return false;
    }
    return true;
  }

  template <typename DestBatchPtrType>
  static inline DestBatchPtrType SafeCastBatchTo(ColumnVectorBatch* batch) {
    auto result = dynamic_cast<DestBatchPtrType>(batch);
    if (result == nullptr) {
      std::ostringstream ss;
      ss << "Bad cast when convert from ColumnVectorBatch to "
         << typeid(typename std::remove_const<
                       typename std::remove_pointer<DestBatchPtrType>::type>::type)
                .name();
      throw InvalidArgument(ss.str());
    }
    return result;
  }

  // set null or throw exception if overflow
  template <typename ReadType, typename FileType>
  static inline void convertNumericElement(const FileType& srcValue, ReadType& destValue,
                                           ColumnVectorBatch& destBatch, uint64_t idx,
                                           bool shouldThrow) {
    constexpr bool isFileTypeFloatingPoint(std::is_floating_point<FileType>::value);
    constexpr bool isReadTypeFloatingPoint(std::is_floating_point<ReadType>::value);
    int64_t longValue = static_cast<int64_t>(srcValue);
    if (isFileTypeFloatingPoint) {
      if (isReadTypeFloatingPoint) {
        destValue = static_cast<ReadType>(srcValue);
      } else {
        if (!canFitInLong(static_cast<double>(srcValue)) ||
            !downCastToInteger(destValue, longValue)) {
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
        }
      }
    } else {
      if (isReadTypeFloatingPoint) {
        destValue = static_cast<ReadType>(srcValue);
        if (destValue != destValue) {  // check is NaN
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
        }
      } else {
        if (!downCastToInteger(destValue, static_cast<int64_t>(srcValue))) {
          handleOverflow<FileType, ReadType>(destBatch, idx, shouldThrow);
        }
      }
    }
  }

  // { boolean, byte, short, int, long, float, double } ->
  // { byte, short, int, long, float, double }
  template <typename FileTypeBatch, typename ReadTypeBatch, typename ReadType>
  class NumericConvertColumnReader : public ConvertColumnReader {
   public:
    NumericConvertColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                               bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);
      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      if (rowBatch.hasNulls) {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (rowBatch.notNull[i]) {
            convertNumericElement<ReadType>(srcBatch.data[i], dstBatch.data[i], rowBatch, i,
                                            throwOnOverflow);
          }
        }
      } else {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          convertNumericElement<ReadType>(srcBatch.data[i], dstBatch.data[i], rowBatch, i,
                                          throwOnOverflow);
        }
      }
    }
  };

  // { boolean, byte, short, int, long, float, double } -> { boolean }
  template <typename FileTypeBatch>
  class NumericConvertColumnReader<FileTypeBatch, BooleanVectorBatch, bool>
      : public ConvertColumnReader {
   public:
    NumericConvertColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                               bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);
      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<BooleanVectorBatch*>(&rowBatch);
      if (rowBatch.hasNulls) {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (rowBatch.notNull[i]) {
            dstBatch.data[i] = (static_cast<int64_t>(srcBatch.data[i]) == 0 ? 0 : 1);
          }
        }
      } else {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          dstBatch.data[i] = (static_cast<int64_t>(srcBatch.data[i]) == 0 ? 0 : 1);
        }
      }
    }
  };

  class ConvertToStringVariantColumnReader : public ConvertColumnReader {
   public:
    ConvertToStringVariantColumnReader(const Type& _readType, const Type& fileType,
                                       StripeStreams& stripe, bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

    virtual uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) = 0;

   protected:
    std::vector<std::string> strBuffer;
  };

  void ConvertToStringVariantColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                                                char* notNull) {
    ConvertColumnReader::next(rowBatch, numValues, notNull);

    // cache converted string in the buffer
    auto totalLength = convertToStrBuffer(rowBatch, numValues);

    // contact string values to blob buffer of vector batch
    auto& dstBatch = *SafeCastBatchTo<StringVectorBatch*>(&rowBatch);
    dstBatch.blob.resize(totalLength);
    char* blob = dstBatch.blob.data();
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
        const auto size = strBuffer[i].size();
        ::memcpy(blob, strBuffer[i].c_str(), size);
        dstBatch.data[i] = blob;
        dstBatch.length[i] = static_cast<int32_t>(size);
        blob += size;
      }
    }
    strBuffer.clear();
  }

  class BooleanToStringVariantColumnReader : public ConvertToStringVariantColumnReader {
   public:
    BooleanToStringVariantColumnReader(const Type& _readType, const Type& fileType,
                                       StripeStreams& stripe, bool _throwOnOverflow)
        : ConvertToStringVariantColumnReader(_readType, fileType, stripe, _throwOnOverflow) {
      trueValue = "TRUE";
      falseValue = "FALSE";
      if (readType.getKind() == CHAR || readType.getKind() == VARCHAR) {
        if (readType.getMaximumLength() < 5) {
          throw SchemaEvolutionError("Invalid maximum length for boolean type: " +
                                     std::to_string(readType.getMaximumLength()));
        }
        if (readType.getKind() == CHAR) {
          trueValue.resize(readType.getMaximumLength(), ' ');
          falseValue.resize(readType.getMaximumLength(), ' ');
        }
      }
    }

    uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) override;

   private:
    std::string trueValue;
    std::string falseValue;
  };

  uint64_t BooleanToStringVariantColumnReader::convertToStrBuffer(ColumnVectorBatch& rowBatch,
                                                                  uint64_t numValues) {
    uint64_t size = 0;
    strBuffer.resize(numValues);
    const auto& srcBatch = *SafeCastBatchTo<const BooleanVectorBatch*>(data.get());
    // cast the bool value to string
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
        strBuffer[i] = (srcBatch.data[i] ? trueValue : falseValue);
        size += strBuffer[i].size();
      }
    }
    return size;
  }

  template <typename FileTypeBatch>
  class NumericToStringVariantColumnReader : public ConvertToStringVariantColumnReader {
   public:
    NumericToStringVariantColumnReader(const Type& _readType, const Type& fileType,
                                       StripeStreams& stripe, bool _throwOnOverflow)
        : ConvertToStringVariantColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}
    uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) override;
  };

  template <typename FileTypeBatch>
  uint64_t NumericToStringVariantColumnReader<FileTypeBatch>::convertToStrBuffer(
      ColumnVectorBatch& rowBatch, uint64_t numValues) {
    uint64_t size = 0;
    strBuffer.resize(numValues);
    const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
    if (readType.getKind() == STRING) {
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          strBuffer[i] = std::to_string(srcBatch.data[i]);
          size += strBuffer[i].size();
        }
      }
    } else if (readType.getKind() == VARCHAR) {
      const auto maxLength = readType.getMaximumLength();
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          strBuffer[i] = std::to_string(srcBatch.data[i]);
          if (strBuffer[i].size() > maxLength) {
            handleOverflow<decltype(srcBatch.data[i]), std::string>(rowBatch, i, throwOnOverflow);
          } else {
            size += strBuffer[i].size();
          }
        }
      }
    } else if (readType.getKind() == CHAR) {
      const auto maxLength = readType.getMaximumLength();
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          strBuffer[i] = std::to_string(srcBatch.data[i]);
          if (strBuffer[i].size() > maxLength) {
            handleOverflow<decltype(srcBatch.data[i]), std::string>(rowBatch, i, throwOnOverflow);
          } else {
            strBuffer[i].resize(maxLength, ' ');
            size += strBuffer[i].size();
          }
        }
      }
    } else {
      throw SchemaEvolutionError("Invalid type for numeric to string conversion: " +
                                 readType.toString());
    }
    return size;
  }

  template <typename FileTypeBatch, typename ReadTypeBatch, bool isFloatingFileType>
  class NumericToDecimalColumnReader : public ConvertColumnReader {
   public:
    NumericToDecimalColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                                 bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {
      precision = static_cast<int32_t>(readType.getPrecision());
      scale = static_cast<int32_t>(readType.getScale());
      bool overflow = false;
      upperBound = scaleUpInt128ByPowerOfTen(1, precision, overflow);
    }

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      dstBatch.precision = precision;
      dstBatch.scale = scale;
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          if constexpr (isFloatingFileType) {
            convertDoubleToDecimal(dstBatch, i, srcBatch.data[i]);
          } else {
            convertIntegerToDecimal(dstBatch, i, srcBatch.data[i]);
          }
        }
      }
    }

   private:
    template <typename SrcType>
    void convertDoubleToDecimal(ReadTypeBatch& dstBatch, uint64_t idx, SrcType value) {
      const auto result = convertDecimal(value, precision, scale);
      Int128 i128 = result.second;
      if (result.first) {
        handleOverflow<SrcType, decltype(dstBatch.values[idx])>(dstBatch, idx, throwOnOverflow);
        return;
      }

      if constexpr (std::is_same<ReadTypeBatch, Decimal64VectorBatch>::value) {
        if (!i128.fitsInLong()) {
          handleOverflow<SrcType, decltype(dstBatch.values[idx])>(dstBatch, idx, throwOnOverflow);
        } else {
          dstBatch.values[idx] = i128.toLong();
        }
      } else {
        dstBatch.values[idx] = i128;
      }
    }

    template <typename SrcType>
    void convertIntegerToDecimal(ReadTypeBatch& dstBatch, uint64_t idx, SrcType value) {
      int fromScale = 0;
      auto result = convertDecimal(value, fromScale, precision, scale);
      if (result.first) {
        handleOverflow<SrcType, decltype(dstBatch.values[idx])>(dstBatch, idx, throwOnOverflow);
      } else {
        if constexpr (std::is_same<ReadTypeBatch, Decimal64VectorBatch>::value) {
          if (!result.second.fitsInLong()) {
            handleOverflow<SrcType, decltype(dstBatch.values[idx])>(dstBatch, idx, throwOnOverflow);
          } else {
            dstBatch.values[idx] = result.second.toLong();
          }
        } else {
          dstBatch.values[idx] = result.second;
        }
      }
    }

    int32_t precision;
    int32_t scale;
    int64_t scaleMultiplier;
    Int128 upperBound;
  };

  class ConvertToTimestampColumnReader : public ConvertColumnReader {
   public:
    ConvertToTimestampColumnReader(const Type& _readType, const Type& fileType,
                                   StripeStreams& stripe, bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow),
          readerTimezone(readType.getKind() == TIMESTAMP_INSTANT ? &getTimezoneByName("GMT")
                                                                 : &stripe.getReaderTimezone()),
          needConvertTimezone(readerTimezone != &getTimezoneByName("GMT")) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

   protected:
    const orc::Timezone* readerTimezone;
    const bool needConvertTimezone;
  };

  // avoid emitting vtable in every translation unit
  void ConvertToTimestampColumnReader::next(ColumnVectorBatch& rowBatch, uint64_t numValues,
                                            char* notNull) {
    ConvertColumnReader::next(rowBatch, numValues, notNull);
  }

  template <typename FileTypeBatch>
  class NumericToTimestampColumnReader : public ConvertToTimestampColumnReader {
   public:
    NumericToTimestampColumnReader(const Type& _readType, const Type& fileType,
                                   StripeStreams& stripe, bool _throwOnOverflow)
        : ConvertToTimestampColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertToTimestampColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<TimestampVectorBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          convertToTimestamp(dstBatch, i, srcBatch.data[i]);
        }
      }
    }

   private:
    template <typename FileType>
    void convertToTimestamp(TimestampVectorBatch& dstBatch, uint64_t idx, FileType value);
  };

  template <typename FileTypeBatch>
  template <typename FileType>
  void NumericToTimestampColumnReader<FileTypeBatch>::convertToTimestamp(
      TimestampVectorBatch& dstBatch, uint64_t idx, FileType value) {
    if constexpr (std::is_floating_point<FileType>::value) {
      if (value > static_cast<FileType>(std::numeric_limits<int64_t>::max()) ||
          value < static_cast<FileType>(std::numeric_limits<int64_t>::min())) {
        handleOverflow<FileType, int64_t>(dstBatch, idx, throwOnOverflow);
        return;
      }
      dstBatch.data[idx] = static_cast<int64_t>(value);
      dstBatch.nanoseconds[idx] = static_cast<int32_t>(
          static_cast<double>(value - static_cast<FileType>(dstBatch.data[idx])) * 1e9);
      if (dstBatch.nanoseconds[idx] < 0) {
        dstBatch.data[idx] -= 1;
        dstBatch.nanoseconds[idx] += static_cast<int32_t>(1e9);
      }
    } else {
      dstBatch.data[idx] = value;
      dstBatch.nanoseconds[idx] = 0;
    }
    if (needConvertTimezone) {
      dstBatch.data[idx] = readerTimezone->convertFromUTC(dstBatch.data[idx]);
    }
  }

  template <typename FileTypeBatch, typename ReadTypeBatch, typename ReadType>
  class DecimalToNumericColumnReader : public ConvertColumnReader {
   public:
    DecimalToNumericColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                                 bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {
      precision = fileType.getPrecision();
      scale = fileType.getScale();
      factor = 1;
      for (int i = 0; i < scale; i++) {
        factor *= 10;
      }
    }

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          if constexpr (std::is_floating_point_v<ReadType>) {
            convertDecimalToDouble(dstBatch, i, srcBatch);
          } else {
            convertDecimalToInteger(dstBatch, i, srcBatch);
          }
        }
      }
    }

   private:
    void convertDecimalToInteger(ReadTypeBatch& dstBatch, uint64_t idx,
                                 const FileTypeBatch& srcBatch) {
      using FileType = decltype(srcBatch.values[idx]);
      Int128 result = scaleDownInt128ByPowerOfTen(srcBatch.values[idx], scale);
      if (!result.fitsInLong()) {
        handleOverflow<FileType, ReadType>(dstBatch, idx, throwOnOverflow);
        return;
      }
      convertNumericElement<ReadType, int64_t>(result.toLong(), dstBatch.data[idx], dstBatch, idx,
                                               throwOnOverflow);
    }

    void convertDecimalToDouble(ReadTypeBatch& dstBatch, uint64_t idx,
                                const FileTypeBatch& srcBatch) {
      double doubleValue = Int128(srcBatch.values[idx]).toDouble();
      dstBatch.data[idx] = static_cast<ReadType>(doubleValue) / static_cast<ReadType>(factor);
    }

    int32_t precision;
    int32_t scale;
    int64_t factor;
  };

  template <typename FileTypeBatch>
  class DecimalToNumericColumnReader<FileTypeBatch, BooleanVectorBatch, bool>
      : public ConvertColumnReader {
   public:
    DecimalToNumericColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                                 bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<BooleanVectorBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          dstBatch.data[i] = srcBatch.values[i] == 0 ? 0 : 1;
        }
      }
    }
  };

  template <typename FileTypeBatch, typename ReadTypeBatch>
  class DecimalConvertColumnReader : public ConvertColumnReader {
   public:
    DecimalConvertColumnReader(const Type& _readType, const Type& fileType, StripeStreams& stripe,
                               bool _throwOnOverflow)
        : ConvertColumnReader(_readType, fileType, stripe, _throwOnOverflow) {
      fromPrecision = fileType.getPrecision();
      fromScale = fileType.getScale();
      toPrecision = _readType.getPrecision();
      toScale = _readType.getScale();
    }

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          convertDecimalToDecimal(dstBatch, i, srcBatch);
        }
      }
    }

   private:
    void convertDecimalToDecimal(ReadTypeBatch& dstBatch, uint64_t idx,
                                 const FileTypeBatch& srcBatch) {
      using FileType = decltype(srcBatch.values[idx]);
      using ReadType = decltype(dstBatch.values[idx]);

      auto [overflows, resultI128] =
          convertDecimal(srcBatch.values[idx], fromScale, toPrecision, toScale);
      if (overflows) {
        handleOverflow<FileType, ReadType>(dstBatch, idx, throwOnOverflow);
      }
      if constexpr (std::is_same_v<ReadTypeBatch, Decimal64VectorBatch>) {
        if (!resultI128.fitsInLong()) {
          handleOverflow<FileType, ReadType>(dstBatch, idx, throwOnOverflow);
        } else {
          dstBatch.values[idx] = resultI128.toLong();
        }
      } else {
        dstBatch.values[idx] = resultI128;
      }
    }

    int32_t fromPrecision;
    int32_t fromScale;
    int32_t toPrecision;
    int32_t toScale;
  };

#define DEFINE_NUMERIC_CONVERT_READER(FROM, TO, TYPE) \
  using FROM##To##TO##ColumnReader =                  \
      NumericConvertColumnReader<FROM##VectorBatch, TO##VectorBatch, TYPE>;

#define DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(FROM, TO) \
  using FROM##To##TO##ColumnReader = NumericToStringVariantColumnReader<FROM##VectorBatch>;

#define DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(FROM, IS_FROM_FLOATING)                       \
  using FROM##To##Decimal64##ColumnReader =                                                    \
      NumericToDecimalColumnReader<FROM##VectorBatch, Decimal64VectorBatch, IS_FROM_FLOATING>; \
  using FROM##To##Decimal128##ColumnReader =                                                   \
      NumericToDecimalColumnReader<FROM##VectorBatch, Decimal128VectorBatch, IS_FROM_FLOATING>;

#define DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(FROM) \
  using FROM##ToTimestampColumnReader = NumericToTimestampColumnReader<FROM##VectorBatch>;

#define DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(TO, TYPE)                       \
  using Decimal64##To##TO##ColumnReader =                                        \
      DecimalToNumericColumnReader<Decimal64VectorBatch, TO##VectorBatch, TYPE>; \
  using Decimal128##To##TO##ColumnReader =                                       \
      DecimalToNumericColumnReader<Decimal128VectorBatch, TO##VectorBatch, TYPE>;

#define DEFINE_DECIMAL_CONVERT_TO_DECIMAL_READER(TO)                     \
  using Decimal64##To##TO##ColumnReader =                                \
      DecimalConvertColumnReader<Decimal64VectorBatch, TO##VectorBatch>; \
  using Decimal128##To##TO##ColumnReader =                               \
      DecimalConvertColumnReader<Decimal128VectorBatch, TO##VectorBatch>;

  DEFINE_NUMERIC_CONVERT_READER(Boolean, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Short, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Short, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Short, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Short, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Int, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Int, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Long, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Long, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Float, float)
  // Floating to integer
  DEFINE_NUMERIC_CONVERT_READER(Float, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Float, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Float, Long, int64_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Boolean, bool)
  DEFINE_NUMERIC_CONVERT_READER(Double, Byte, int8_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Short, int16_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Int, int32_t)
  DEFINE_NUMERIC_CONVERT_READER(Double, Long, int64_t)
  // Integer to Floating
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Short, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Int, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Long, Float, float)
  DEFINE_NUMERIC_CONVERT_READER(Boolean, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Byte, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Short, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Int, Double, double)
  DEFINE_NUMERIC_CONVERT_READER(Long, Double, double)

  // Numeric to String/Char
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Byte, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Short, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Int, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Long, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Float, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Double, String)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Byte, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Short, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Int, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Long, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Float, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Double, Char)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Byte, Varchar)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Short, Varchar)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Int, Varchar)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Long, Varchar)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Float, Varchar)
  DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER(Double, Varchar)
  using BooleanToStringColumnReader = BooleanToStringVariantColumnReader;
  using BooleanToCharColumnReader = BooleanToStringVariantColumnReader;
  using BooleanToVarcharColumnReader = BooleanToStringVariantColumnReader;

  // Numeric to Decimal
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Boolean, false)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Byte, false)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Short, false)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Int, false)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Long, false)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Float, true)
  DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER(Double, true)

  // Numeric to Timestamp
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Boolean)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Byte)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Short)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Int)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Long)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Float)
  DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER(Double)

  // Decimal to Numeric
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Boolean, bool)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Byte, int8_t)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Short, int16_t)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Int, int32_t)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Long, int64_t)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Float, float)
  DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER(Double, double)

  // Decimal to Decimal
  DEFINE_DECIMAL_CONVERT_TO_DECIMAL_READER(Decimal64)
  DEFINE_DECIMAL_CONVERT_TO_DECIMAL_READER(Decimal128)

#define CREATE_READER(NAME) \
  return std::make_unique<NAME>(_readType, fileType, stripe, throwOnOverflow);

#define CASE_CREATE_READER(TYPE, CONVERT) \
  case TYPE:                              \
    CREATE_READER(CONVERT##ColumnReader)

  const static int32_t MAX_PRECISION_64 = 18;

  static inline bool isDecimal64(const Type& type) {
    return type.getPrecision() > 0 && type.getPrecision() <= MAX_PRECISION_64;
  }

#define CASE_CREATE_FROM_DECIMAL_READER(TYPE, TO)   \
  case TYPE: {                                      \
    if (isDecimal64(fileType)) {                    \
      CREATE_READER(Decimal64To##TO##ColumnReader)  \
    } else {                                        \
      CREATE_READER(Decimal128To##TO##ColumnReader) \
    }                                               \
  }

#define CASE_CREATE_DECIMAL_READER(FROM)            \
  case DECIMAL: {                                   \
    if (isDecimal64(_readType)) {                   \
      CREATE_READER(FROM##ToDecimal64ColumnReader)  \
    } else {                                        \
      CREATE_READER(FROM##ToDecimal128ColumnReader) \
    }                                               \
  }

#define CASE_EXCEPTION                                                                 \
  default:                                                                             \
    throw SchemaEvolutionError("Cannot convert from " + fileType.toString() + " to " + \
                               _readType.toString());

  std::unique_ptr<ColumnReader> buildConvertReader(const Type& fileType, StripeStreams& stripe,
                                                   bool useTightNumericVector,
                                                   bool throwOnOverflow) {
    if (!useTightNumericVector) {
      throw SchemaEvolutionError(
          "SchemaEvolution only support tight vector, please create ColumnVectorBatch with "
          "option useTightNumericVector");
    }
    const auto& _readType = *stripe.getSchemaEvolution()->getReadType(fileType);

    switch (fileType.getKind()) {
      case BOOLEAN: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BYTE, BooleanToByte)
          CASE_CREATE_READER(SHORT, BooleanToShort)
          CASE_CREATE_READER(INT, BooleanToInt)
          CASE_CREATE_READER(LONG, BooleanToLong)
          CASE_CREATE_READER(FLOAT, BooleanToFloat)
          CASE_CREATE_READER(DOUBLE, BooleanToDouble)
          CASE_CREATE_READER(STRING, BooleanToString)
          CASE_CREATE_READER(CHAR, BooleanToChar)
          CASE_CREATE_READER(VARCHAR, BooleanToVarchar)
          CASE_CREATE_DECIMAL_READER(Boolean)
          CASE_CREATE_READER(TIMESTAMP, BooleanToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, BooleanToTimestamp)
          case BOOLEAN:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case BYTE: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, ByteToBoolean)
          CASE_CREATE_READER(SHORT, ByteToShort)
          CASE_CREATE_READER(INT, ByteToInt)
          CASE_CREATE_READER(LONG, ByteToLong)
          CASE_CREATE_READER(FLOAT, ByteToFloat)
          CASE_CREATE_READER(DOUBLE, ByteToDouble)
          CASE_CREATE_READER(STRING, ByteToString)
          CASE_CREATE_READER(CHAR, ByteToChar)
          CASE_CREATE_READER(VARCHAR, ByteToVarchar)
          CASE_CREATE_DECIMAL_READER(Byte)
          CASE_CREATE_READER(TIMESTAMP, ByteToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, ByteToTimestamp)
          case BYTE:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case SHORT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, ShortToBoolean)
          CASE_CREATE_READER(BYTE, ShortToByte)
          CASE_CREATE_READER(INT, ShortToInt)
          CASE_CREATE_READER(LONG, ShortToLong)
          CASE_CREATE_READER(FLOAT, ShortToFloat)
          CASE_CREATE_READER(DOUBLE, ShortToDouble)
          CASE_CREATE_READER(STRING, ShortToString)
          CASE_CREATE_READER(CHAR, ShortToChar)
          CASE_CREATE_READER(VARCHAR, ShortToVarchar)
          CASE_CREATE_DECIMAL_READER(Short)
          CASE_CREATE_READER(TIMESTAMP, ShortToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, ShortToTimestamp)
          case SHORT:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case INT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, IntToBoolean)
          CASE_CREATE_READER(BYTE, IntToByte)
          CASE_CREATE_READER(SHORT, IntToShort)
          CASE_CREATE_READER(LONG, IntToLong)
          CASE_CREATE_READER(FLOAT, IntToFloat)
          CASE_CREATE_READER(DOUBLE, IntToDouble)
          CASE_CREATE_READER(STRING, IntToString)
          CASE_CREATE_READER(CHAR, IntToChar)
          CASE_CREATE_READER(VARCHAR, IntToVarchar)
          CASE_CREATE_DECIMAL_READER(Int)
          CASE_CREATE_READER(TIMESTAMP, IntToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, IntToTimestamp)
          case INT:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case LONG: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, LongToBoolean)
          CASE_CREATE_READER(BYTE, LongToByte)
          CASE_CREATE_READER(SHORT, LongToShort)
          CASE_CREATE_READER(INT, LongToInt)
          CASE_CREATE_READER(FLOAT, LongToFloat)
          CASE_CREATE_READER(DOUBLE, LongToDouble)
          CASE_CREATE_READER(STRING, LongToString)
          CASE_CREATE_READER(CHAR, LongToChar)
          CASE_CREATE_READER(VARCHAR, LongToVarchar)
          CASE_CREATE_DECIMAL_READER(Long)
          CASE_CREATE_READER(TIMESTAMP, LongToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, LongToTimestamp)
          case LONG:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case FLOAT: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, FloatToBoolean)
          CASE_CREATE_READER(BYTE, FloatToByte)
          CASE_CREATE_READER(SHORT, FloatToShort)
          CASE_CREATE_READER(INT, FloatToInt)
          CASE_CREATE_READER(LONG, FloatToLong)
          CASE_CREATE_READER(DOUBLE, FloatToDouble)
          CASE_CREATE_READER(STRING, FloatToString)
          CASE_CREATE_READER(CHAR, FloatToChar)
          CASE_CREATE_READER(VARCHAR, FloatToVarchar)
          CASE_CREATE_DECIMAL_READER(Float)
          CASE_CREATE_READER(TIMESTAMP, FloatToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, FloatToTimestamp)
          case FLOAT:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case DOUBLE: {
        switch (_readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, DoubleToBoolean)
          CASE_CREATE_READER(BYTE, DoubleToByte)
          CASE_CREATE_READER(SHORT, DoubleToShort)
          CASE_CREATE_READER(INT, DoubleToInt)
          CASE_CREATE_READER(LONG, DoubleToLong)
          CASE_CREATE_READER(FLOAT, DoubleToFloat)
          CASE_CREATE_READER(STRING, DoubleToString)
          CASE_CREATE_READER(CHAR, DoubleToChar)
          CASE_CREATE_READER(VARCHAR, DoubleToVarchar)
          CASE_CREATE_DECIMAL_READER(Double)
          CASE_CREATE_READER(TIMESTAMP, DoubleToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, DoubleToTimestamp)
          case DOUBLE:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case STRING:
      case BINARY:
      case TIMESTAMP:
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      case DECIMAL: {
        switch (_readType.getKind()) {
          CASE_CREATE_FROM_DECIMAL_READER(BOOLEAN, Boolean)
          CASE_CREATE_FROM_DECIMAL_READER(BYTE, Byte)
          CASE_CREATE_FROM_DECIMAL_READER(SHORT, Short)
          CASE_CREATE_FROM_DECIMAL_READER(INT, Int)
          CASE_CREATE_FROM_DECIMAL_READER(LONG, Long)
          CASE_CREATE_FROM_DECIMAL_READER(FLOAT, Float)
          CASE_CREATE_FROM_DECIMAL_READER(DOUBLE, Double)
          case DECIMAL: {
            if (isDecimal64(fileType)) {
              if (isDecimal64(_readType)) {
                CREATE_READER(Decimal64ToDecimal64ColumnReader)
              } else {
                CREATE_READER(Decimal64ToDecimal128ColumnReader)
              }
            } else {
              if (isDecimal64(_readType)) {
                CREATE_READER(Decimal128ToDecimal64ColumnReader)
              } else {
                CREATE_READER(Decimal128ToDecimal128ColumnReader)
              }
            }
          }
          case STRING:
          case CHAR:
          case VARCHAR:
          case TIMESTAMP:
          case TIMESTAMP_INSTANT:
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case DATE:
      case VARCHAR:
      case CHAR:
      case TIMESTAMP_INSTANT:
        CASE_EXCEPTION
    }
  }

#undef DEFINE_NUMERIC_CONVERT_READER
#undef DEFINE_NUMERIC_CONVERT_TO_STRING_VARINT_READER
#undef DEFINE_NUMERIC_CONVERT_TO_DECIMAL_READER
#undef DEFINE_NUMERIC_CONVERT_TO_TIMESTAMP_READER
#undef DEFINE_DECIMAL_CONVERT_TO_NUMERIC_READER
#undef DEFINE_DECIMAL_CONVERT_TO_DECIMAL_READER
#undef CASE_CREATE_FROM_DECIMAL_READER
#undef CASE_CREATE_READER
#undef CASE_EXCEPTION

}  // namespace orc
