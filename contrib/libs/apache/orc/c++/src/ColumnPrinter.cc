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

#include "orc/ColumnPrinter.hh"
#include "orc/Int128.hh"
#include "orc/orc-config.hh"

#include "Adaptor.hh"

#include <time.h>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <typeinfo>

#ifdef __clang__
#pragma clang diagnostic ignored "-Wformat-security"
#endif

namespace orc {

  class VoidColumnPrinter : public ColumnPrinter {
   public:
    VoidColumnPrinter(std::string&, ColumnPrinter::Param);
    ~VoidColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BooleanColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* data_;

   public:
    BooleanColumnPrinter(std::string&, ColumnPrinter::Param);
    ~BooleanColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class LongColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* data_;

   public:
    LongColumnPrinter(std::string&, ColumnPrinter::Param);
    ~LongColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter : public ColumnPrinter {
   private:
    const double* data_;
    const bool isFloat_;

   public:
    DoubleColumnPrinter(std::string&, const Type& type, ColumnPrinter::Param);
    virtual ~DoubleColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class TimestampColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* seconds_;
    const int64_t* nanoseconds_;

   public:
    TimestampColumnPrinter(std::string&, ColumnPrinter::Param);
    ~TimestampColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DateColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* data_;

   public:
    DateColumnPrinter(std::string&, ColumnPrinter::Param);
    ~DateColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal64ColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* data_;
    int32_t scale_;
    ColumnPrinter::Param param_;

   public:
    Decimal64ColumnPrinter(std::string&, ColumnPrinter::Param);
    ~Decimal64ColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class Decimal128ColumnPrinter : public ColumnPrinter {
   private:
    const Int128* data_;
    int32_t scale_;
    ColumnPrinter::Param param_;

   public:
    Decimal128ColumnPrinter(std::string&, ColumnPrinter::Param);
    ~Decimal128ColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter : public ColumnPrinter {
   private:
    const char* const* start_;
    const int64_t* length_;

   public:
    StringColumnPrinter(std::string&, ColumnPrinter::Param);
    virtual ~StringColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class BinaryColumnPrinter : public ColumnPrinter {
   private:
    const char* const* start_;
    const int64_t* length_;

   public:
    BinaryColumnPrinter(std::string&, ColumnPrinter::Param);
    virtual ~BinaryColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class ListColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* offsets_;
    std::unique_ptr<ColumnPrinter> elementPrinter_;

   public:
    ListColumnPrinter(std::string&, const Type& type, ColumnPrinter::Param);
    virtual ~ListColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class MapColumnPrinter : public ColumnPrinter {
   private:
    const int64_t* offsets_;
    std::unique_ptr<ColumnPrinter> keyPrinter_;
    std::unique_ptr<ColumnPrinter> elementPrinter_;

   public:
    MapColumnPrinter(std::string&, const Type& type, ColumnPrinter::Param);
    virtual ~MapColumnPrinter() override {}
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class UnionColumnPrinter : public ColumnPrinter {
   private:
    const unsigned char* tags_;
    const uint64_t* offsets_;
    std::vector<std::unique_ptr<ColumnPrinter>> fieldPrinter_;

   public:
    UnionColumnPrinter(std::string&, const Type& type, ColumnPrinter::Param);
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StructColumnPrinter : public ColumnPrinter {
   private:
    std::vector<std::unique_ptr<ColumnPrinter>> fieldPrinter_;
    std::vector<std::string> fieldNames_;

   public:
    StructColumnPrinter(std::string&, const Type& type, ColumnPrinter::Param);
    void printRow(uint64_t rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  void writeChar(std::string& file, char ch) {
    file += ch;
  }

  void writeString(std::string& file, const char* ptr) {
    size_t len = strlen(ptr);
    file.append(ptr, len);
  }

  ColumnPrinter::ColumnPrinter(std::string& buffer) : buffer(buffer) {
    notNull = nullptr;
    hasNulls = false;
  }

  ColumnPrinter::~ColumnPrinter() {
    // PASS
  }

  void ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    hasNulls = batch.hasNulls;
    if (hasNulls) {
      notNull = batch.notNull.data();
    } else {
      notNull = nullptr;
    }
  }

  std::unique_ptr<ColumnPrinter> createColumnPrinter(std::string& buffer, const Type* type,
                                                     ColumnPrinter::Param param) {
    std::unique_ptr<ColumnPrinter> result;
    if (type == nullptr) {
      result = std::make_unique<VoidColumnPrinter>(buffer, param);
    } else {
      switch (static_cast<int64_t>(type->getKind())) {
        case BOOLEAN:
          result = std::make_unique<BooleanColumnPrinter>(buffer, param);
          break;

        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          result = std::make_unique<LongColumnPrinter>(buffer, param);
          break;

        case FLOAT:
        case DOUBLE:
          result = std::make_unique<DoubleColumnPrinter>(buffer, *type, param);
          break;

        case STRING:
        case VARCHAR:
        case CHAR:
          result = std::make_unique<StringColumnPrinter>(buffer, param);
          break;

        case BINARY:
        case GEOMETRY:
        case GEOGRAPHY:
          result = std::make_unique<BinaryColumnPrinter>(buffer, param);
          break;

        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
          result = std::make_unique<TimestampColumnPrinter>(buffer, param);
          break;

        case LIST:
          result = std::make_unique<ListColumnPrinter>(buffer, *type, param);
          break;

        case MAP:
          result = std::make_unique<MapColumnPrinter>(buffer, *type, param);
          break;

        case STRUCT:
          result = std::make_unique<StructColumnPrinter>(buffer, *type, param);
          break;

        case DECIMAL:
          if (type->getPrecision() == 0 || type->getPrecision() > 18) {
            result = std::make_unique<Decimal128ColumnPrinter>(buffer, param);
          } else {
            result = std::make_unique<Decimal64ColumnPrinter>(buffer, param);
          }
          break;

        case DATE:
          result = std::make_unique<DateColumnPrinter>(buffer, param);
          break;

        case UNION:
          result = std::make_unique<UnionColumnPrinter>(buffer, *type, param);
          break;

        default:
          throw std::logic_error("unknown batch type");
      }
    }
    return result;
  }

  VoidColumnPrinter::VoidColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer) {
    // PASS
  }

  void VoidColumnPrinter::reset(const ColumnVectorBatch&) {
    // PASS
  }

  void VoidColumnPrinter::printRow(uint64_t) {
    writeString(buffer, "null");
  }

  LongColumnPrinter::LongColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), data_(nullptr) {
    // PASS
  }

  void LongColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      const auto numBuffer = std::to_string(static_cast<int64_t>(data_[rowId]));
      writeString(buffer, numBuffer.c_str());
    }
  }

  DoubleColumnPrinter::DoubleColumnPrinter(std::string& buffer, const Type& type,
                                           ColumnPrinter::Param)
      : ColumnPrinter(buffer), data_(nullptr), isFloat_(type.getKind() == FLOAT) {
    // PASS
  }

  void DoubleColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      char numBuffer[64];
      snprintf(numBuffer, sizeof(numBuffer), isFloat_ ? "%.7g" : "%.14g", data_[rowId]);
      writeString(buffer, numBuffer);
    }
  }

  Decimal64ColumnPrinter::Decimal64ColumnPrinter(std::string& buffer, ColumnPrinter::Param param)
      : ColumnPrinter(buffer), data_(nullptr), scale_(0), param_(param) {
    // PASS
  }

  void Decimal64ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const Decimal64VectorBatch&>(batch).values.data();
    scale_ = dynamic_cast<const Decimal64VectorBatch&>(batch).scale;
  }

  std::string toDecimalString(int64_t value, int32_t scale, bool trimTrailingZeros) {
    return Int128(value).toDecimalString(scale, trimTrailingZeros);
  }

  void Decimal64ColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      bool trimTrailingZeros = param_.printDecimalTrimTrailingZeros;
      if (param_.printDecimalAsString) {
        writeChar(buffer, '"');
        writeString(buffer, toDecimalString(data_[rowId], scale_, trimTrailingZeros).c_str());
        writeChar(buffer, '"');
      } else {
        writeString(buffer, toDecimalString(data_[rowId], scale_, trimTrailingZeros).c_str());
      }
    }
  }

  Decimal128ColumnPrinter::Decimal128ColumnPrinter(std::string& buffer, ColumnPrinter::Param param)
      : ColumnPrinter(buffer), data_(nullptr), scale_(0), param_(param) {
    // PASS
  }

  void Decimal128ColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const Decimal128VectorBatch&>(batch).values.data();
    scale_ = dynamic_cast<const Decimal128VectorBatch&>(batch).scale;
  }

  void Decimal128ColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      bool trimTrailingZeros = param_.printDecimalTrimTrailingZeros;
      if (param_.printDecimalAsString) {
        writeChar(buffer, '"');
        writeString(buffer, data_[rowId].toDecimalString(scale_, trimTrailingZeros).c_str());
        writeChar(buffer, '"');
      } else {
        writeString(buffer, data_[rowId].toDecimalString(scale_, trimTrailingZeros).c_str());
      }
    }
  }

  StringColumnPrinter::StringColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), start_(nullptr), length_(nullptr) {
    // PASS
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start_ = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length_ = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '"');
      for (int64_t i = 0; i < length_[rowId]; ++i) {
        char ch = static_cast<char>(start_[rowId][i]);
        switch (ch) {
          case '\\':
            writeString(buffer, "\\\\");
            break;
          case '\b':
            writeString(buffer, "\\b");
            break;
          case '\f':
            writeString(buffer, "\\f");
            break;
          case '\n':
            writeString(buffer, "\\n");
            break;
          case '\r':
            writeString(buffer, "\\r");
            break;
          case '\t':
            writeString(buffer, "\\t");
            break;
          case '"':
            writeString(buffer, "\\\"");
            break;
          default:
            writeChar(buffer, ch);
            break;
        }
      }
      writeChar(buffer, '"');
    }
  }

  ListColumnPrinter::ListColumnPrinter(std::string& buffer, const Type& type,
                                       ColumnPrinter::Param param)
      : ColumnPrinter(buffer), offsets_(nullptr) {
    elementPrinter_ = createColumnPrinter(buffer, type.getSubtype(0), param);
  }

  void ListColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    offsets_ = dynamic_cast<const ListVectorBatch&>(batch).offsets.data();
    elementPrinter_->reset(*dynamic_cast<const ListVectorBatch&>(batch).elements);
  }

  void ListColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for (int64_t i = offsets_[rowId]; i < offsets_[rowId + 1]; ++i) {
        if (i != offsets_[rowId]) {
          writeString(buffer, ", ");
        }
        elementPrinter_->printRow(static_cast<uint64_t>(i));
      }
      writeChar(buffer, ']');
    }
  }

  MapColumnPrinter::MapColumnPrinter(std::string& buffer, const Type& type,
                                     ColumnPrinter::Param param)
      : ColumnPrinter(buffer), offsets_(nullptr) {
    keyPrinter_ = createColumnPrinter(buffer, type.getSubtype(0), param);
    elementPrinter_ = createColumnPrinter(buffer, type.getSubtype(1), param);
  }

  void MapColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const MapVectorBatch& myBatch = dynamic_cast<const MapVectorBatch&>(batch);
    offsets_ = myBatch.offsets.data();
    keyPrinter_->reset(*myBatch.keys);
    elementPrinter_->reset(*myBatch.elements);
  }

  void MapColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for (int64_t i = offsets_[rowId]; i < offsets_[rowId + 1]; ++i) {
        if (i != offsets_[rowId]) {
          writeString(buffer, ", ");
        }
        writeString(buffer, "{\"key\": ");
        keyPrinter_->printRow(static_cast<uint64_t>(i));
        writeString(buffer, ", \"value\": ");
        elementPrinter_->printRow(static_cast<uint64_t>(i));
        writeChar(buffer, '}');
      }
      writeChar(buffer, ']');
    }
  }

  UnionColumnPrinter::UnionColumnPrinter(std::string& buffer, const Type& type,
                                         ColumnPrinter::Param param)
      : ColumnPrinter(buffer), tags_(nullptr), offsets_(nullptr) {
    for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
      fieldPrinter_.push_back(createColumnPrinter(buffer, type.getSubtype(i), param));
    }
  }

  void UnionColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const UnionVectorBatch& unionBatch = dynamic_cast<const UnionVectorBatch&>(batch);
    tags_ = unionBatch.tags.data();
    offsets_ = unionBatch.offsets.data();
    for (size_t i = 0; i < fieldPrinter_.size(); ++i) {
      fieldPrinter_[i]->reset(*(unionBatch.children[i]));
    }
  }

  void UnionColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeString(buffer, "{\"tag\": ");
      const auto numBuffer = std::to_string(static_cast<int64_t>(tags_[rowId]));
      writeString(buffer, numBuffer.c_str());
      writeString(buffer, ", \"value\": ");
      fieldPrinter_[tags_[rowId]]->printRow(offsets_[rowId]);
      writeChar(buffer, '}');
    }
  }

  StructColumnPrinter::StructColumnPrinter(std::string& buffer, const Type& type,
                                           ColumnPrinter::Param param)
      : ColumnPrinter(buffer) {
    for (unsigned int i = 0; i < type.getSubtypeCount(); ++i) {
      fieldNames_.push_back(type.getFieldName(i));
      fieldPrinter_.push_back(createColumnPrinter(buffer, type.getSubtype(i), param));
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const StructVectorBatch& structBatch = dynamic_cast<const StructVectorBatch&>(batch);
    for (size_t i = 0; i < fieldPrinter_.size(); ++i) {
      fieldPrinter_[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '{');
      for (unsigned int i = 0; i < fieldPrinter_.size(); ++i) {
        if (i != 0) {
          writeString(buffer, ", ");
        }
        writeChar(buffer, '"');
        writeString(buffer, fieldNames_[i].c_str());
        writeString(buffer, "\": ");
        fieldPrinter_[i]->printRow(rowId);
      }
      writeChar(buffer, '}');
    }
  }

  DateColumnPrinter::DateColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), data_(nullptr) {
    // PASS
  }

  void DateColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      const time_t timeValue = data_[rowId] * 24 * 60 * 60;
      struct tm tmValue;
      gmtime_r(&timeValue, &tmValue);
      char timeBuffer[11];
      strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d", &tmValue);
      writeChar(buffer, '"');
      writeString(buffer, timeBuffer);
      writeChar(buffer, '"');
    }
  }

  void DateColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BooleanColumnPrinter::BooleanColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), data_(nullptr) {
    // PASS
  }

  void BooleanColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeString(buffer, (data_[rowId] ? "true" : "false"));
    }
  }

  void BooleanColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    data_ = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  BinaryColumnPrinter::BinaryColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), start_(nullptr), length_(nullptr) {
    // PASS
  }

  void BinaryColumnPrinter::printRow(uint64_t rowId) {
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      writeChar(buffer, '[');
      for (int64_t i = 0; i < length_[rowId]; ++i) {
        if (i != 0) {
          writeString(buffer, ", ");
        }
        const auto numBuffer = std::to_string(static_cast<int>(start_[rowId][i]) & 0xff);
        writeString(buffer, numBuffer.c_str());
      }
      writeChar(buffer, ']');
    }
  }

  void BinaryColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    start_ = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length_ = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  TimestampColumnPrinter::TimestampColumnPrinter(std::string& buffer, ColumnPrinter::Param)
      : ColumnPrinter(buffer), seconds_(nullptr), nanoseconds_(nullptr) {
    // PASS
  }

  void TimestampColumnPrinter::printRow(uint64_t rowId) {
    const int64_t NANO_DIGITS = 9;
    if (hasNulls && !notNull[rowId]) {
      writeString(buffer, "null");
    } else {
      int64_t nanos = nanoseconds_[rowId];
      time_t secs = static_cast<time_t>(seconds_[rowId]);
      struct tm tmValue;
      gmtime_r(&secs, &tmValue);
      char timeBuffer[20];
      strftime(timeBuffer, sizeof(timeBuffer), "%Y-%m-%d %H:%M:%S", &tmValue);
      writeChar(buffer, '"');
      writeString(buffer, timeBuffer);
      writeChar(buffer, '.');
      // remove trailing zeros off the back of the nanos value.
      int64_t zeroDigits = 0;
      if (nanos == 0) {
        zeroDigits = 8;
      } else {
        while (nanos % 10 == 0) {
          nanos /= 10;
          zeroDigits += 1;
        }
      }
      const auto numBuffer = std::to_string(static_cast<int64_t>(nanos));
      const int64_t padDigits = NANO_DIGITS - zeroDigits - static_cast<int64_t>(numBuffer.size());
      for (int i = 0; i < padDigits; ++i) {
        writeChar(buffer, '0');
      }
      writeString(buffer, numBuffer.c_str());
      writeChar(buffer, '"');
    }
  }

  void TimestampColumnPrinter::reset(const ColumnVectorBatch& batch) {
    ColumnPrinter::reset(batch);
    const TimestampVectorBatch& ts = dynamic_cast<const TimestampVectorBatch&>(batch);
    seconds_ = ts.data.data();
    nanoseconds_ = ts.nanoseconds.data();
  }
}  // namespace orc
