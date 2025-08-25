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
#include "Utils.hh"

#include <optional>

namespace orc {

  // Assume that we are using tight numeric vector batch
  using BooleanVectorBatch = ByteVectorBatch;

  ConvertColumnReader::ConvertColumnReader(const Type& readType, const Type& fileType,
                                           StripeStreams& stripe, bool throwOnOverflow)
      : ColumnReader(readType, stripe), readType(readType), throwOnOverflow(throwOnOverflow) {
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

  static inline void handleParseFromStringError(ColumnVectorBatch& dstBatch, uint64_t idx,
                                                bool shouldThrow, const std::string& typeName,
                                                const std::string& str,
                                                const std::string& expectedFormat = "") {
    if (!shouldThrow) {
      dstBatch.notNull.data()[idx] = 0;
      dstBatch.hasNulls = true;
    } else {
      std::ostringstream ss;
      ss << "Failed to parse " << typeName << " from string:" << str;
      if (expectedFormat != "") {
        ss << " the following format \"" << expectedFormat << "\" is expected";
      }
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

    if (isFileTypeFloatingPoint) {
      if (isReadTypeFloatingPoint) {
        destValue = static_cast<ReadType>(srcValue);
      } else {
        if (!canFitInLong(static_cast<double>(srcValue)) ||
            !downCastToInteger(destValue, static_cast<int64_t>(srcValue))) {
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
    NumericConvertColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                               bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {}

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
    NumericConvertColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                               bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {}

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
    ConvertToStringVariantColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {}

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
    BooleanToStringVariantColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToStringVariantColumnReader(readType, fileType, stripe, throwOnOverflow) {
      trueValue_ = "TRUE";
      falseValue_ = "FALSE";
      if (readType.getKind() == CHAR || readType.getKind() == VARCHAR) {
        if (readType.getMaximumLength() < 5) {
          throw SchemaEvolutionError("Invalid maximum length for boolean type: " +
                                     std::to_string(readType.getMaximumLength()));
        }
        if (readType.getKind() == CHAR) {
          trueValue_.resize(readType.getMaximumLength(), ' ');
          falseValue_.resize(readType.getMaximumLength(), ' ');
        }
      }
    }

    uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) override;

   private:
    std::string trueValue_;
    std::string falseValue_;
  };

  uint64_t BooleanToStringVariantColumnReader::convertToStrBuffer(ColumnVectorBatch& rowBatch,
                                                                  uint64_t numValues) {
    uint64_t size = 0;
    strBuffer.resize(numValues);
    const auto& srcBatch = *SafeCastBatchTo<const BooleanVectorBatch*>(data.get());
    // cast the bool value to string
    for (uint64_t i = 0; i < numValues; ++i) {
      if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
        strBuffer[i] = (srcBatch.data[i] ? trueValue_ : falseValue_);
        size += strBuffer[i].size();
      }
    }
    return size;
  }

  template <typename FileTypeBatch>
  class NumericToStringVariantColumnReader : public ConvertToStringVariantColumnReader {
   public:
    NumericToStringVariantColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToStringVariantColumnReader(readType, fileType, stripe, throwOnOverflow) {}
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
    NumericToDecimalColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                                 bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {
      precision_ = static_cast<int32_t>(readType.getPrecision());
      scale_ = static_cast<int32_t>(readType.getScale());
      bool overflow = false;
      upperBound_ = scaleUpInt128ByPowerOfTen(1, precision_, overflow);
    }

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      dstBatch.precision = precision_;
      dstBatch.scale = scale_;
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
      const auto result = convertDecimal(value, precision_, scale_);
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
      auto result = convertDecimal(value, fromScale, precision_, scale_);
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

    int32_t precision_;
    int32_t scale_;
    int64_t scaleMultiplier_;
    Int128 upperBound_;
  };

  class ConvertToTimestampColumnReader : public ConvertColumnReader {
   public:
    ConvertToTimestampColumnReader(const Type& readType, const Type& fileType,
                                   StripeStreams& stripe, bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow),
          isInstant(readType.getKind() == TIMESTAMP_INSTANT),
          readerTimezone(isInstant ? &getTimezoneByName("GMT") : &stripe.getReaderTimezone()),
          needConvertTimezone(readerTimezone != &getTimezoneByName("GMT")) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override;

   protected:
    const bool isInstant;
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
    NumericToTimestampColumnReader(const Type& readType, const Type& fileType,
                                   StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToTimestampColumnReader(readType, fileType, stripe, throwOnOverflow) {}

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
    DecimalToNumericColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                                 bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {
      precision_ = fileType.getPrecision();
      scale_ = fileType.getScale();
      factor_ = 1;
      for (int i = 0; i < scale_; i++) {
        factor_ *= 10;
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
      Int128 result = scaleDownInt128ByPowerOfTen(srcBatch.values[idx], scale_);
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
      dstBatch.data[idx] = static_cast<ReadType>(doubleValue) / static_cast<ReadType>(factor_);
    }

    int32_t precision_;
    int32_t scale_;
    int64_t factor_;
  };

  template <typename FileTypeBatch>
  class DecimalToNumericColumnReader<FileTypeBatch, BooleanVectorBatch, bool>
      : public ConvertColumnReader {
   public:
    DecimalToNumericColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                                 bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {}

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
    DecimalConvertColumnReader(const Type& readType, const Type& fileType, StripeStreams& stripe,
                               bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {
      fromPrecision_ = fileType.getPrecision();
      fromScale_ = fileType.getScale();
      toPrecision_ = readType.getPrecision();
      toScale_ = readType.getScale();
    }

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      dstBatch.precision = toPrecision_;
      dstBatch.scale = toScale_;
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
          convertDecimal(srcBatch.values[idx], fromScale_, toPrecision_, toScale_);
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

    int32_t fromPrecision_;
    int32_t fromScale_;
    int32_t toPrecision_;
    int32_t toScale_;
  };

  template <typename FileTypeBatch>
  class DecimalToTimestampColumnReader : public ConvertToTimestampColumnReader {
   public:
    DecimalToTimestampColumnReader(const Type& readType, const Type& fileType,
                                   StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToTimestampColumnReader(readType, fileType, stripe, throwOnOverflow),
          precision_(static_cast<int32_t>(fileType.getPrecision())),
          scale_(static_cast<int32_t>(fileType.getScale())) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);
      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<TimestampVectorBatch*>(&rowBatch);
      for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          convertDecimalToTimestamp(dstBatch, i, srcBatch);
        }
      }
    }

   private:
    void convertDecimalToTimestamp(TimestampVectorBatch& dstBatch, uint64_t idx,
                                   const FileTypeBatch& srcBatch) {
      constexpr int SecondToNanoFactor = 9;
      // Following constant comes from java.time.Instant
      // '-1000000000-01-01T00:00Z'
      constexpr int64_t MIN_EPOCH_SECONDS = -31557014167219200L;
      // '1000000000-12-31T23:59:59.999999999Z'
      constexpr int64_t MAX_EPOCH_SECONDS = 31556889864403199L;
      // dummy variable, there's no risk of overflow
      bool overflow = false;

      Int128 i128(srcBatch.values[idx]);
      Int128 integerPortion = scaleDownInt128ByPowerOfTen(i128, scale_);
      if (integerPortion < MIN_EPOCH_SECONDS || integerPortion > MAX_EPOCH_SECONDS) {
        handleOverflow<Decimal, int64_t>(dstBatch, idx, throwOnOverflow);
        return;
      }
      i128 -= scaleUpInt128ByPowerOfTen(integerPortion, scale_, overflow);
      Int128 fractionPortion = std::move(i128);
      if (scale_ < SecondToNanoFactor) {
        fractionPortion =
            scaleUpInt128ByPowerOfTen(fractionPortion, SecondToNanoFactor - scale_, overflow);
      } else {
        fractionPortion = scaleDownInt128ByPowerOfTen(fractionPortion, scale_ - SecondToNanoFactor);
      }
      if (fractionPortion < 0) {
        fractionPortion += 1e9;
        integerPortion -= 1;
      }
      // line 630 has guaranteed toLong() will not overflow
      dstBatch.data[idx] = integerPortion.toLong();
      dstBatch.nanoseconds[idx] = fractionPortion.toLong();

      if (needConvertTimezone) {
        dstBatch.data[idx] = readerTimezone->convertFromUTC(dstBatch.data[idx]);
      }
    }

    const int32_t precision_;
    const int32_t scale_;
  };

  template <typename FileTypeBatch>
  class DecimalToStringVariantColumnReader : public ConvertToStringVariantColumnReader {
   public:
    DecimalToStringVariantColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToStringVariantColumnReader(readType, fileType, stripe, throwOnOverflow),
          scale_(fileType.getScale()) {}

    uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) override {
      uint64_t size = 0;
      strBuffer.resize(numValues);
      const auto& srcBatch = *SafeCastBatchTo<const FileTypeBatch*>(data.get());
      if (readType.getKind() == STRING) {
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
            strBuffer[i] = Int128(srcBatch.values[i]).toDecimalString(scale_, true);
            size += strBuffer[i].size();
          }
        }
      } else {
        const auto maxLength = readType.getMaximumLength();
        for (uint64_t i = 0; i < rowBatch.numElements; ++i) {
          if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
            strBuffer[i] = Int128(srcBatch.values[i]).toDecimalString(scale_, true);
          }
          if (strBuffer[i].size() > maxLength) {
            strBuffer[i].resize(maxLength);
          }
          size += strBuffer[i].size();
        }
      }
      return size;
    }

   private:
    const int32_t scale_;
  };

  template <typename ReadTypeBatch, typename ReadType>
  class StringVariantToNumericColumnReader : public ConvertColumnReader {
   public:
    StringVariantToNumericColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const StringVectorBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          if constexpr (std::is_floating_point_v<ReadType>) {
            convertToDouble(dstBatch, srcBatch, i);
          } else {
            convertToInteger(dstBatch, srcBatch, i);
          }
        }
      }
    }

   private:
    void convertToInteger(ReadTypeBatch& dstBatch, const StringVectorBatch& srcBatch,
                          uint64_t idx) {
      int64_t longValue = 0;
      const std::string longStr(srcBatch.data[idx], srcBatch.length[idx]);
      try {
        longValue = std::stoll(longStr);
      } catch (...) {
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Long", longStr);
        return;
      }
      if constexpr (std::is_same_v<ReadType, bool>) {
        dstBatch.data[idx] = longValue == 0 ? 0 : 1;
      } else {
        if (!downCastToInteger(dstBatch.data[idx], longValue)) {
          handleOverflow<std::string, ReadType>(dstBatch, idx, throwOnOverflow);
        }
      }
    }

    void convertToDouble(ReadTypeBatch& dstBatch, const StringVectorBatch& srcBatch, uint64_t idx) {
      const std::string floatValue(srcBatch.data[idx], srcBatch.length[idx]);
      try {
        if constexpr (std::is_same_v<ReadType, float>) {
          dstBatch.data[idx] = std::stof(floatValue);
        } else {
          dstBatch.data[idx] = std::stod(floatValue);
        }
      } catch (...) {
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, typeid(readType).name(),
                                   floatValue);
      }
    }
  };

  class StringVariantConvertColumnReader : public ConvertToStringVariantColumnReader {
   public:
    StringVariantConvertColumnReader(const Type& readType, const Type& fileType,
                                     StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToStringVariantColumnReader(readType, fileType, stripe, throwOnOverflow) {}

    uint64_t convertToStrBuffer(ColumnVectorBatch& rowBatch, uint64_t numValues) override {
      uint64_t size = 0;
      strBuffer.resize(numValues);
      const auto& srcBatch = *SafeCastBatchTo<const StringVectorBatch*>(data.get());
      const auto maxLength = readType.getMaximumLength();
      if (readType.getKind() == STRING) {
        for (uint64_t i = 0; i < numValues; ++i) {
          if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
            strBuffer[i] = std::string(srcBatch.data[i], srcBatch.length[i]);
            size += strBuffer[i].size();
          }
        }
      } else if (readType.getKind() == VARCHAR) {
        for (uint64_t i = 0; i < numValues; ++i) {
          if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
            const char* charData = srcBatch.data[i];
            uint64_t originLength = srcBatch.length[i];
            uint64_t itemLength = Utf8Utils::truncateBytesTo(maxLength, charData, originLength);
            strBuffer[i] = std::string(charData, itemLength);
            size += strBuffer[i].length();
          }
        }
      } else if (readType.getKind() == CHAR) {
        for (uint64_t i = 0; i < numValues; ++i) {
          if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
            const char* charData = srcBatch.data[i];
            uint64_t originLength = srcBatch.length[i];
            uint64_t charLength = Utf8Utils::charLength(charData, originLength);
            auto itemLength = Utf8Utils::truncateBytesTo(maxLength, charData, originLength);
            strBuffer[i] = std::string(srcBatch.data[i], itemLength);
            // the padding is exactly 1 byte per char
            if (charLength < maxLength) {
              strBuffer[i].resize(itemLength + maxLength - charLength, ' ');
            }
            size += strBuffer[i].length();
          }
        }
      } else {
        throw SchemaEvolutionError("Invalid type for numeric to string conversion: " +
                                   readType.toString());
      }
      return size;
    }
  };

  class StringVariantToTimestampColumnReader : public ConvertToTimestampColumnReader {
   public:
    StringVariantToTimestampColumnReader(const Type& readType, const Type& fileType,
                                         StripeStreams& stripe, bool throwOnOverflow)
        : ConvertToTimestampColumnReader(readType, fileType, stripe, throwOnOverflow) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertToTimestampColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const StringVectorBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<TimestampVectorBatch*>(&rowBatch);

      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          convertToTimestamp(dstBatch, i, std::string(srcBatch.data[i], srcBatch.length[i]));
        }
      }
    }

   private:
    // Algorithm: http://howardhinnant.github.io/date_algorithms.html
    // The algorithm implements a proleptic Gregorian calendar.
    int64_t daysFromProlepticGregorianCalendar(int32_t y, int32_t m, int32_t d) {
      y -= m <= 2;
      int32_t era = y / 400;
      int32_t yoe = y - era * 400;                                   // [0, 399]
      int32_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;  // [0, 365]
      int32_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;           // [0, 146096]
      return 1ll * era * 146097 + doe - 719468;
    }

    std::optional<std::pair<int64_t, int64_t>> tryBestToParseFromString(
        const std::string& timeStr) {
      int32_t year, month, day, hour, min, sec, nanos = 0;
      int32_t matched = std::sscanf(timeStr.c_str(), "%4d-%2d-%2d %2d:%2d:%2d.%d", &year, &month,
                                    &day, &hour, &min, &sec, &nanos);
      if (matched != 6 && matched != 7) {
        return std::nullopt;
      }
      if (nanos) {
        if (nanos < 0 || nanos >= 1e9) {
          return std::nullopt;
        }
        while (nanos < static_cast<int64_t>(1e8)) {
          nanos *= 10;
        }
      }
      int64_t daysSinceEpoch = daysFromProlepticGregorianCalendar(year, month, day);
      int64_t secondSinceEpoch = 60ll * (60 * (24L * daysSinceEpoch + hour) + min) + sec;
      return std::make_optional(std::pair<int64_t, int64_t>{secondSinceEpoch, nanos});
    }

    void convertToTimestamp(TimestampVectorBatch& dstBatch, uint64_t idx,
                            const std::string& timeStr) {
      // Expected timestamp_instant format string : yyyy-mm-dd hh:mm:ss[.xxx] timezone
      // Eg. "2019-07-09 13:11:00 America/Los_Angeles"
      // Expected timestamp format string         : yyyy-mm-dd hh:mm:ss[.xxx]
      // Eg. "2019-07-09 13:11:00"
      static std::string expectedTimestampInstantFormat = "yyyy-mm-dd hh:mm:ss[.xxx] timezone";
      static std::string expectedTimestampFormat = "yyyy-mm-dd hh:mm:ss[.xxx]";
      auto timestamp = tryBestToParseFromString(timeStr);
      if (!timestamp.has_value()) {
        if (!isInstant) {
          handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Timestamp", timeStr,
                                     expectedTimestampFormat);
          return;
        }
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Timestamp_Instant", timeStr,
                                   expectedTimestampInstantFormat);
        return;
      }

      auto& [second, nanos] = timestamp.value();

      if (isInstant) {
        size_t pos = 0;  // get the name of timezone
        pos = timeStr.find(' ', pos) + 1;
        pos = timeStr.find(' ', pos);
        if (pos == std::string::npos) {
          handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Timestamp_Instant", timeStr,
                                     expectedTimestampInstantFormat);
          return;
        }
        pos += 1;
        size_t subStrLength = timeStr.length() - pos;
        try {
          second = getTimezoneByName(timeStr.substr(pos, subStrLength)).convertFromUTC(second);
        } catch (const TimezoneError&) {
          handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Timestamp_Instant", timeStr,
                                     expectedTimestampInstantFormat);
          return;
        }
      } else {
        if (needConvertTimezone) {
          second = readerTimezone->convertFromUTC(second);
        }
      }
      dstBatch.data[idx] = second;
      dstBatch.nanoseconds[idx] = nanos;
    }
  };

  template <typename ReadTypeBatch>
  class StringVariantToDecimalColumnReader : public ConvertColumnReader {
   public:
    StringVariantToDecimalColumnReader(const Type& readType, const Type& fileType,
                                       StripeStreams& stripe, bool throwOnOverflow)
        : ConvertColumnReader(readType, fileType, stripe, throwOnOverflow),
          precision_(static_cast<int32_t>(readType.getPrecision())),
          scale_(static_cast<int32_t>(readType.getScale())) {}

    void next(ColumnVectorBatch& rowBatch, uint64_t numValues, char* notNull) override {
      ConvertColumnReader::next(rowBatch, numValues, notNull);

      const auto& srcBatch = *SafeCastBatchTo<const StringVectorBatch*>(data.get());
      auto& dstBatch = *SafeCastBatchTo<ReadTypeBatch*>(&rowBatch);
      for (uint64_t i = 0; i < numValues; ++i) {
        if (!rowBatch.hasNulls || rowBatch.notNull[i]) {
          convertToDecimal(dstBatch, i, std::string(srcBatch.data[i], srcBatch.length[i]));
        }
      }
    }

   private:
    void convertToDecimal(ReadTypeBatch& dstBatch, uint64_t idx, const std::string& decimalStr) {
      constexpr int32_t MAX_PRECISION_128 = 38;
      int32_t fromPrecision = 0;
      int32_t fromScale = 0;
      uint32_t start = 0;
      bool negative = false;
      if (decimalStr.empty()) {
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Decimal", decimalStr);
        return;
      }
      auto dotPos = decimalStr.find('.');
      if (dotPos == std::string::npos) {
        fromScale = 0;
        fromPrecision = decimalStr.length();
        dotPos = decimalStr.length();
      } else {
        if (dotPos + 1 == decimalStr.length()) {
          handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Decimal", decimalStr);
          return;
        }
        fromPrecision = decimalStr.length() - 1;
        fromScale = decimalStr.length() - dotPos - 1;
      }
      if (decimalStr.front() == '-') {
        negative = true;
        start++;
        fromPrecision--;
      }
      const std::string integerPortion = decimalStr.substr(start, dotPos - start);
      if (dotPos == start || fromPrecision > MAX_PRECISION_128 || fromPrecision <= 0 ||
          !std::all_of(integerPortion.begin(), integerPortion.end(), ::isdigit)) {
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Decimal", decimalStr);
        return;
      }

      Int128 i128;
      try {
        bool overflow = false;
        i128 = Int128(integerPortion);
        // overflow won't happen
        i128 *= scaleUpInt128ByPowerOfTen(Int128(1), fromScale, overflow);
      } catch (const std::exception& e) {
        handleParseFromStringError(dstBatch, idx, throwOnOverflow, "Decimal", decimalStr);
        return;
      }
      if (dotPos + 1 < decimalStr.length()) {
        const std::string fractionPortion = decimalStr.substr(dotPos + 1, fromScale);
        if (!std::all_of(fractionPortion.begin(), fractionPortion.end(), ::isdigit)) {
          handleOverflow<std::string, Int128>(dstBatch, idx, throwOnOverflow);
          return;
        }
        i128 += Int128(fractionPortion);
      }

      auto [overflow, result] = convertDecimal(i128, fromScale, precision_, scale_);
      if (overflow) {
        handleOverflow<std::string, Int128>(dstBatch, idx, throwOnOverflow);
        return;
      }
      if (negative) {
        result.negate();
      }

      if constexpr (std::is_same_v<ReadTypeBatch, Decimal128VectorBatch>) {
        dstBatch.values[idx] = result;
      } else {
        if (!result.fitsInLong()) {
          handleOverflow<std::string, decltype(dstBatch.values[idx])>(dstBatch, idx,
                                                                      throwOnOverflow);
        } else {
          dstBatch.values[idx] = result.toLong();
        }
      }
    }

    const int32_t precision_;
    const int32_t scale_;
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

#define DEFINE_DECIMAL_CONVERT_TO_TIMESTAMP_READER                                               \
  using Decimal64ToTimestampColumnReader = DecimalToTimestampColumnReader<Decimal64VectorBatch>; \
  using Decimal128ToTimestampColumnReader = DecimalToTimestampColumnReader<Decimal128VectorBatch>;

#define DEFINE_DECIMAL_CONVERT_TO_STRING_VARINT_READER(TO)                                        \
  using Decimal64To##TO##ColumnReader = DecimalToStringVariantColumnReader<Decimal64VectorBatch>; \
  using Decimal128To##TO##ColumnReader = DecimalToStringVariantColumnReader<Decimal128VectorBatch>;

#define DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(FROM, TO, TYPE) \
  using FROM##To##TO##ColumnReader = StringVariantToNumericColumnReader<TO##VectorBatch, TYPE>;

#define DEFINE_STRING_VARIANT_CONVERT_READER(FROM, TO) \
  using FROM##To##TO##ColumnReader = StringVariantConvertColumnReader;

#define DEFINE_STRING_VARIANT_CONVERT_TO_TIMESTAMP_READER(FROM, TO) \
  using FROM##To##TO##ColumnReader = StringVariantToTimestampColumnReader;

#define DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(FROM, TO) \
  using FROM##To##TO##ColumnReader = StringVariantToDecimalColumnReader<TO##VectorBatch>;

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

  DEFINE_DECIMAL_CONVERT_TO_TIMESTAMP_READER
  DEFINE_DECIMAL_CONVERT_TO_STRING_VARINT_READER(String)
  DEFINE_DECIMAL_CONVERT_TO_STRING_VARINT_READER(Char)
  DEFINE_DECIMAL_CONVERT_TO_STRING_VARINT_READER(Varchar)

  // String variant to numeric
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Boolean, bool)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Byte, int8_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Short, int16_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Int, int32_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Long, int64_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Float, float)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(String, Double, double)

  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Boolean, bool)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Byte, int8_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Short, int16_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Int, int32_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Long, int64_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Float, float)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Char, Double, double)

  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Boolean, bool)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Byte, int8_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Short, int16_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Int, int32_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Long, int64_t)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Float, float)
  DEFINE_STRING_VARIANT_CONVERT_TO_NUMERIC_READER(Varchar, Double, double)

  // String variant to string variant
  DEFINE_STRING_VARIANT_CONVERT_READER(String, String)
  DEFINE_STRING_VARIANT_CONVERT_READER(String, Char)
  DEFINE_STRING_VARIANT_CONVERT_READER(String, Varchar)
  DEFINE_STRING_VARIANT_CONVERT_READER(Char, Char)
  DEFINE_STRING_VARIANT_CONVERT_READER(Char, String)
  DEFINE_STRING_VARIANT_CONVERT_READER(Char, Varchar)
  DEFINE_STRING_VARIANT_CONVERT_READER(Varchar, String)
  DEFINE_STRING_VARIANT_CONVERT_READER(Varchar, Char)
  DEFINE_STRING_VARIANT_CONVERT_READER(Varchar, Varchar)

  // String variant to timestamp
  DEFINE_STRING_VARIANT_CONVERT_TO_TIMESTAMP_READER(String, Timestamp)
  DEFINE_STRING_VARIANT_CONVERT_TO_TIMESTAMP_READER(Char, Timestamp)
  DEFINE_STRING_VARIANT_CONVERT_TO_TIMESTAMP_READER(Varchar, Timestamp)

  // String variant to decimal
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(String, Decimal64)
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(String, Decimal128)
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(Char, Decimal64)
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(Char, Decimal128)
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(Varchar, Decimal64)
  DEFINE_STRING_VARIANT_CONVERT_CONVERT_TO_DECIMAL_READER(Varchar, Decimal128)

#define CREATE_READER(NAME) \
  return std::make_unique<NAME>(readType, fileType, stripe, throwOnOverflow);

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
    if (isDecimal64(readType)) {                    \
      CREATE_READER(FROM##ToDecimal64ColumnReader)  \
    } else {                                        \
      CREATE_READER(FROM##ToDecimal128ColumnReader) \
    }                                               \
  }

#define CASE_EXCEPTION                                                                 \
  default:                                                                             \
    throw SchemaEvolutionError("Cannot convert from " + fileType.toString() + " to " + \
                               readType.toString());

  std::unique_ptr<ColumnReader> buildConvertReader(const Type& fileType, StripeStreams& stripe,
                                                   bool useTightNumericVector,
                                                   bool throwOnOverflow) {
    if (!useTightNumericVector) {
      throw SchemaEvolutionError(
          "SchemaEvolution only support tight vector, please create ColumnVectorBatch with "
          "option useTightNumericVector");
    }
    const auto& readType = *stripe.getSchemaEvolution()->getReadType(fileType);

    switch (fileType.getKind()) {
      case BOOLEAN: {
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
        switch (readType.getKind()) {
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
      case DECIMAL: {
        switch (readType.getKind()) {
          CASE_CREATE_FROM_DECIMAL_READER(BOOLEAN, Boolean)
          CASE_CREATE_FROM_DECIMAL_READER(BYTE, Byte)
          CASE_CREATE_FROM_DECIMAL_READER(SHORT, Short)
          CASE_CREATE_FROM_DECIMAL_READER(INT, Int)
          CASE_CREATE_FROM_DECIMAL_READER(LONG, Long)
          CASE_CREATE_FROM_DECIMAL_READER(FLOAT, Float)
          CASE_CREATE_FROM_DECIMAL_READER(DOUBLE, Double)
          CASE_CREATE_FROM_DECIMAL_READER(STRING, String)
          CASE_CREATE_FROM_DECIMAL_READER(CHAR, Char)
          CASE_CREATE_FROM_DECIMAL_READER(VARCHAR, Varchar)
          CASE_CREATE_FROM_DECIMAL_READER(TIMESTAMP, Timestamp)
          CASE_CREATE_FROM_DECIMAL_READER(TIMESTAMP_INSTANT, Timestamp)
          case DECIMAL: {
            if (isDecimal64(fileType)) {
              if (isDecimal64(readType)) {
                CREATE_READER(Decimal64ToDecimal64ColumnReader)
              } else {
                CREATE_READER(Decimal64ToDecimal128ColumnReader)
              }
            } else {
              if (isDecimal64(readType)) {
                CREATE_READER(Decimal128ToDecimal64ColumnReader)
              } else {
                CREATE_READER(Decimal128ToDecimal128ColumnReader)
              }
            }
          }
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case STRING: {
        switch (readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, StringToBoolean)
          CASE_CREATE_READER(BYTE, StringToByte)
          CASE_CREATE_READER(SHORT, StringToShort)
          CASE_CREATE_READER(INT, StringToInt)
          CASE_CREATE_READER(LONG, StringToLong)
          CASE_CREATE_READER(FLOAT, StringToFloat)
          CASE_CREATE_READER(DOUBLE, StringToDouble)
          CASE_CREATE_READER(STRING, StringToString)
          CASE_CREATE_READER(CHAR, StringToChar)
          CASE_CREATE_READER(VARCHAR, StringToVarchar)
          CASE_CREATE_READER(TIMESTAMP, StringToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, StringToTimestamp)
          case DECIMAL: {
            if (isDecimal64(readType)) {
              CREATE_READER(StringToDecimal64ColumnReader)
            } else {
              CREATE_READER(StringToDecimal128ColumnReader)
            }
          }
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case CHAR: {
        switch (readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, CharToBoolean)
          CASE_CREATE_READER(BYTE, CharToByte)
          CASE_CREATE_READER(SHORT, CharToShort)
          CASE_CREATE_READER(INT, CharToInt)
          CASE_CREATE_READER(LONG, CharToLong)
          CASE_CREATE_READER(FLOAT, CharToFloat)
          CASE_CREATE_READER(DOUBLE, CharToDouble)
          CASE_CREATE_READER(STRING, CharToString)
          CASE_CREATE_READER(CHAR, CharToChar)
          CASE_CREATE_READER(VARCHAR, CharToVarchar)
          CASE_CREATE_READER(TIMESTAMP, CharToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, CharToTimestamp)
          case DECIMAL: {
            if (isDecimal64(readType)) {
              CREATE_READER(CharToDecimal64ColumnReader)
            } else {
              CREATE_READER(CharToDecimal128ColumnReader)
            }
          }
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case VARCHAR: {
        switch (readType.getKind()) {
          CASE_CREATE_READER(BOOLEAN, VarcharToBoolean)
          CASE_CREATE_READER(BYTE, VarcharToByte)
          CASE_CREATE_READER(SHORT, VarcharToShort)
          CASE_CREATE_READER(INT, VarcharToInt)
          CASE_CREATE_READER(LONG, VarcharToLong)
          CASE_CREATE_READER(FLOAT, VarcharToFloat)
          CASE_CREATE_READER(DOUBLE, VarcharToDouble)
          CASE_CREATE_READER(STRING, VarcharToString)
          CASE_CREATE_READER(CHAR, VarcharToChar)
          CASE_CREATE_READER(VARCHAR, VarcharToVarchar)
          CASE_CREATE_READER(TIMESTAMP, VarcharToTimestamp)
          CASE_CREATE_READER(TIMESTAMP_INSTANT, VarcharToTimestamp)
          case DECIMAL: {
            if (isDecimal64(readType)) {
              CREATE_READER(VarcharToDecimal64ColumnReader)
            } else {
              CREATE_READER(VarcharToDecimal128ColumnReader)
            }
          }
          case BINARY:
          case LIST:
          case MAP:
          case STRUCT:
          case UNION:
          case DATE:
            CASE_EXCEPTION
        }
      }
      case BINARY:
      case TIMESTAMP:
      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      case DATE:
      case TIMESTAMP_INSTANT:
        CASE_EXCEPTION
    }
  }

}  // namespace orc
