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

/*
 * This file contains code adapted from the Apache Arrow project.
 *
 * Original source:
 * https://github.com/apache/arrow/blob/main/cpp/src/parquet/geospatial/statistics.cc
 *
 * The original code is licensed under the Apache License, Version 2.0.
 *
 * Modifications may have been made from the original source.
 */

#include "orc/Geospatial.hh"
#include "orc/Exceptions.hh"

#include "Geospatial.hh"

#include <algorithm>
#include <cstring>
#include <optional>
#include <sstream>

namespace orc::geospatial {

  template <typename T>
  inline std::enable_if_t<std::is_trivially_copyable_v<T>, T> safeLoadAs(const uint8_t* unaligned) {
    std::remove_const_t<T> ret;
    std::memcpy(&ret, unaligned, sizeof(T));
    return ret;
  }

  template <typename U, typename T>
  inline std::enable_if_t<std::is_trivially_copyable_v<T> && std::is_trivially_copyable_v<U> &&
                              sizeof(T) == sizeof(U),
                          U>
  safeCopy(T value) {
    std::remove_const_t<U> ret;
    std::memcpy(&ret, static_cast<const void*>(&value), sizeof(T));
    return ret;
  }

  static bool isLittleEndian() {
    static union {
      uint32_t i;
      char c[4];
    } num = {0x01020304};
    return num.c[0] == 4;
  }

#if defined(_MSC_VER)
#include <intrin.h>  // IWYU pragma: keep
#define ORC_BYTE_SWAP64 _byteswap_uint64
#define ORC_BYTE_SWAP32 _byteswap_ulong
#else
#define ORC_BYTE_SWAP64 __builtin_bswap64
#define ORC_BYTE_SWAP32 __builtin_bswap32
#endif

  // Swap the byte order (i.e. endianness)
  static inline uint32_t byteSwap(uint32_t value) {
    return static_cast<uint32_t>(ORC_BYTE_SWAP32(value));
  }
  static inline double byteSwap(double value) {
    const uint64_t swapped = ORC_BYTE_SWAP64(safeCopy<uint64_t>(value));
    return safeCopy<double>(swapped);
  }

  std::string BoundingBox::toString() const {
    std::stringstream ss;
    ss << "BoundingBox{xMin=" << min[0] << ", xMax=" << max[0] << ", yMin=" << min[1]
       << ", yMax=" << max[1] << ", zMin=" << min[2] << ", zMax=" << max[2] << ", mMin=" << min[3]
       << ", mMax=" << max[3] << "}";
    return ss.str();
  }

  /// \brief Object to keep track of the low-level consumption of a well-known binary
  /// geometry
  ///
  /// Briefly, ISO well-known binary supported by the Parquet spec is an endian byte
  /// (0x01 or 0x00), followed by geometry type + dimensions encoded as a (uint32_t),
  /// followed by geometry-specific data. Coordinate sequences are represented by a
  /// uint32_t (the number of coordinates) plus a sequence of doubles (number of coordinates
  /// multiplied by the number of dimensions).
  class WKBBuffer {
   public:
    WKBBuffer() : data_(nullptr), size_(0) {}
    WKBBuffer(const uint8_t* data, int64_t size) : data_(data), size_(size) {}

    uint8_t readUInt8() {
      return readChecked<uint8_t>();
    }

    uint32_t readUInt32(bool swap) {
      auto value = readChecked<uint32_t>();
      return swap ? byteSwap(value) : value;
    }

    template <typename Coord, typename Visit>
    void readCoords(uint32_t nCoords, bool swap, Visit&& visit) {
      size_t total_bytes = nCoords * sizeof(Coord);
      if (size_ < total_bytes) {
      }

      if (swap) {
        Coord coord;
        for (uint32_t i = 0; i < nCoords; i++) {
          coord = readUnchecked<Coord>();
          for (auto& c : coord) {
            c = byteSwap(c);
          }

          std::forward<Visit>(visit)(coord);
        }
      } else {
        for (uint32_t i = 0; i < nCoords; i++) {
          std::forward<Visit>(visit)(readUnchecked<Coord>());
        }
      }
    }

    size_t size() const {
      return size_;
    }

   private:
    const uint8_t* data_;
    size_t size_;

    template <typename T>
    T readChecked() {
      if (size_ < sizeof(T)) {
        std::stringstream ss;
        ss << "Can't read" << sizeof(T) << " bytes from WKBBuffer with " << size_ << " remaining";
        throw ParseError(ss.str());
      }

      return readUnchecked<T>();
    }

    template <typename T>
    T readUnchecked() {
      T out = safeLoadAs<T>(data_);
      data_ += sizeof(T);
      size_ -= sizeof(T);
      return out;
    }
  };

  using GeometryTypeAndDimensions = std::pair<GeometryType, Dimensions>;

  namespace {

    std::optional<GeometryTypeAndDimensions> parseGeometryType(uint32_t wkbGeometryType) {
      // The number 1000 can be used because WKB geometry types are constructed
      // on purpose such that this relationship is true (e.g., LINESTRING ZM maps
      // to 3002).
      uint32_t geometryTypeComponent = wkbGeometryType % 1000;
      uint32_t dimensionsComponent = wkbGeometryType / 1000;

      auto minGeometryTypeValue = static_cast<uint32_t>(GeometryType::VALUE_MIN);
      auto maxGeometryTypeValue = static_cast<uint32_t>(GeometryType::VALUE_MAX);
      auto minDimensionValue = static_cast<uint32_t>(Dimensions::VALUE_MIN);
      auto maxDimensionValue = static_cast<uint32_t>(Dimensions::VALUE_MAX);

      if (geometryTypeComponent < minGeometryTypeValue ||
          geometryTypeComponent > maxGeometryTypeValue || dimensionsComponent < minDimensionValue ||
          dimensionsComponent > maxDimensionValue) {
        return std::nullopt;
      }

      return std::make_optional(
          GeometryTypeAndDimensions{static_cast<GeometryType>(geometryTypeComponent),
                                    static_cast<Dimensions>(dimensionsComponent)});
    }

  }  // namespace

  std::vector<int32_t> WKBGeometryBounder::geometryTypes() const {
    std::vector<int32_t> out(geospatialTypes_.begin(), geospatialTypes_.end());
    std::sort(out.begin(), out.end());
    return out;
  }

  void WKBGeometryBounder::mergeGeometry(std::string_view bytesWkb) {
    if (!isValid_) {
      return;
    }
    mergeGeometry(reinterpret_cast<const uint8_t*>(bytesWkb.data()), bytesWkb.size());
  }

  void WKBGeometryBounder::mergeGeometry(const uint8_t* bytesWkb, size_t bytesSize) {
    if (!isValid_) {
      return;
    }
    WKBBuffer src{bytesWkb, static_cast<int64_t>(bytesSize)};
    try {
      mergeGeometryInternal(&src, /*record_wkb_type=*/true);
    } catch (const ParseError&) {
      invalidate();
      return;
    }
    if (src.size() != 0) {
      // "Exepcted zero bytes after consuming WKB
      invalidate();
    }
  }

  void WKBGeometryBounder::mergeGeometryInternal(WKBBuffer* src, bool recordWkbType) {
    uint8_t endian = src->readUInt8();
    bool swap = endian != 0x00;
    if (isLittleEndian()) {
      swap = endian != 0x01;
    }

    uint32_t wkbGeometryType = src->readUInt32(swap);
    auto geometryTypeAndDimensions = parseGeometryType(wkbGeometryType);
    if (!geometryTypeAndDimensions.has_value()) {
      invalidate();
      return;
    }
    auto& [geometry_type, dimensions] = geometryTypeAndDimensions.value();

    // Keep track of geometry types encountered if at the top level
    if (recordWkbType) {
      geospatialTypes_.insert(static_cast<int32_t>(wkbGeometryType));
    }

    switch (geometry_type) {
      case GeometryType::POINT:
        mergeSequence(src, dimensions, 1, swap);
        break;

      case GeometryType::LINESTRING: {
        uint32_t nCoords = src->readUInt32(swap);
        mergeSequence(src, dimensions, nCoords, swap);
        break;
      }
      case GeometryType::POLYGON: {
        uint32_t n_parts = src->readUInt32(swap);
        for (uint32_t i = 0; i < n_parts; i++) {
          uint32_t nCoords = src->readUInt32(swap);
          mergeSequence(src, dimensions, nCoords, swap);
        }
        break;
      }

      // These are all encoded the same in WKB, even though this encoding would
      // allow for parts to be of a different geometry type or different dimensions.
      // For the purposes of bounding, this does not cause us problems. We pass
      // record_wkb_type = false because we do not want the child geometry to be
      // added to the geometry_types list (e.g., for a MultiPoint, we only want
      // the code for MultiPoint to be added, not the code for Point).
      case GeometryType::MULTIPOINT:
      case GeometryType::MULTILINESTRING:
      case GeometryType::MULTIPOLYGON:
      case GeometryType::GEOMETRYCOLLECTION: {
        uint32_t n_parts = src->readUInt32(swap);
        for (uint32_t i = 0; i < n_parts; i++) {
          mergeGeometryInternal(src, /*record_wkb_type*/ false);
        }
        break;
      }
    }
  }

  void WKBGeometryBounder::mergeSequence(WKBBuffer* src, Dimensions dimensions, uint32_t nCoords,
                                         bool swap) {
    switch (dimensions) {
      case Dimensions::XY:
        src->readCoords<BoundingBox::XY>(nCoords, swap,
                                         [&](BoundingBox::XY coord) { box_.updateXY(coord); });
        break;
      case Dimensions::XYZ:
        src->readCoords<BoundingBox::XYZ>(nCoords, swap,
                                          [&](BoundingBox::XYZ coord) { box_.updateXYZ(coord); });
        break;
      case Dimensions::XYM:
        src->readCoords<BoundingBox::XYM>(nCoords, swap,
                                          [&](BoundingBox::XYM coord) { box_.updateXYM(coord); });
        break;
      case Dimensions::XYZM:
        src->readCoords<BoundingBox::XYZM>(
            nCoords, swap, [&](BoundingBox::XYZM coord) { box_.updateXYZM(coord); });
        break;
      default:
        invalidate();
    }
  }

}  // namespace orc::geospatial
