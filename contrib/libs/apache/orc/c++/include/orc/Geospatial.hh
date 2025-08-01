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
 * https://github.com/apache/arrow/blob/main/cpp/src/parquet/geospatial/statistics.h
 *
 * The original code is licensed under the Apache License, Version 2.0.
 *
 * Modifications may have been made from the original source.
 */

#ifndef ORC_GEOSPATIAL_HH
#define ORC_GEOSPATIAL_HH

#include <array>
#include <cmath>
#include <ostream>
#include <string>

namespace orc::geospatial {

  constexpr double INF = std::numeric_limits<double>::infinity();
  // The maximum number of dimensions supported (X, Y, Z, M)
  inline constexpr int MAX_DIMENSIONS = 4;

  // Supported combinations of geometry dimensions
  enum class Dimensions {
    XY = 0,    // X and Y only
    XYZ = 1,   // X, Y, and Z
    XYM = 2,   // X, Y, and M
    XYZM = 3,  // X, Y, Z, and M
    VALUE_MIN = 0,
    VALUE_MAX = 3
  };

  // Supported geometry types according to ISO WKB
  enum class GeometryType {
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
    MULTIPOINT = 4,
    MULTILINESTRING = 5,
    MULTIPOLYGON = 6,
    GEOMETRYCOLLECTION = 7,
    VALUE_MIN = 1,
    VALUE_MAX = 7
  };

  // BoundingBox represents the minimum bounding rectangle (or box) for a geometry.
  // It supports up to 4 dimensions (X, Y, Z, M).
  struct BoundingBox {
    using XY = std::array<double, 2>;
    using XYZ = std::array<double, 3>;
    using XYM = std::array<double, 3>;
    using XYZM = std::array<double, 4>;

    // Default constructor: initializes to an empty bounding box.
    BoundingBox() : min{INF, INF, INF, INF}, max{-INF, -INF, -INF, -INF} {}
    // Constructor with explicit min/max values.
    BoundingBox(const XYZM& mins, const XYZM& maxes) : min(mins), max(maxes) {}
    BoundingBox(const BoundingBox& other) = default;
    BoundingBox& operator=(const BoundingBox&) = default;

    // Update the bounding box to include a 2D coordinate.
    void updateXY(const XY& coord) {
      updateInternal(coord);
    }
    // Update the bounding box to include a 3D coordinate (XYZ).
    void updateXYZ(const XYZ& coord) {
      updateInternal(coord);
    }
    // Update the bounding box to include a 3D coordinate (XYM).
    void updateXYM(const XYM& coord) {
      std::array<int, 3> dims = {0, 1, 3};
      for (int i = 0; i < 3; ++i) {
        auto dim = dims[i];
        if (!std::isnan(min[dim]) && !std::isnan(max[dim])) {
          min[dim] = std::min(min[dim], coord[i]);
          max[dim] = std::max(max[dim], coord[i]);
        }
      }
    }
    // Update the bounding box to include a 4D coordinate (XYZM).
    void updateXYZM(const XYZM& coord) {
      updateInternal(coord);
    }

    // Reset the bounding box to its initial empty state.
    void reset() {
      for (int i = 0; i < MAX_DIMENSIONS; ++i) {
        min[i] = INF;
        max[i] = -INF;
      }
    }

    // Invalidate the bounding box (set all values to NaN).
    void invalidate() {
      for (int i = 0; i < MAX_DIMENSIONS; ++i) {
        min[i] = std::numeric_limits<double>::quiet_NaN();
        max[i] = std::numeric_limits<double>::quiet_NaN();
      }
    }

    // Check if the bound for a given dimension is empty.
    bool boundEmpty(int dim) const {
      return std::isinf(min[dim] - max[dim]);
    }

    // Check if the bound for a given dimension is valid (not NaN).
    bool boundValid(int dim) const {
      return !std::isnan(min[dim]) && !std::isnan(max[dim]);
    }

    // Get the lower bound (min values).
    const XYZM& lowerBound() const {
      return min;
    }
    // Get the upper bound (max values).
    const XYZM& upperBound() const {
      return max;
    }

    // Get validity for each dimension.
    std::array<bool, MAX_DIMENSIONS> dimensionValid() const {
      return {boundValid(0), boundValid(1), boundValid(2), boundValid(3)};
    }
    // Get emptiness for each dimension.
    std::array<bool, MAX_DIMENSIONS> dimensionEmpty() const {
      return {boundEmpty(0), boundEmpty(1), boundEmpty(2), boundEmpty(3)};
    }

    // Merge another bounding box into this one.
    void merge(const BoundingBox& other) {
      for (int i = 0; i < MAX_DIMENSIONS; ++i) {
        if (std::isnan(min[i]) || std::isnan(max[i]) || std::isnan(other.min[i]) ||
            std::isnan(other.max[i])) {
          min[i] = std::numeric_limits<double>::quiet_NaN();
          max[i] = std::numeric_limits<double>::quiet_NaN();
        } else {
          min[i] = std::min(min[i], other.min[i]);
          max[i] = std::max(max[i], other.max[i]);
        }
      }
    }

    // Convert the bounding box to a string representation.
    std::string toString() const;

    XYZM min;  // Minimum values for each dimension
    XYZM max;  // Maximum values for each dimension

   private:
    // Internal update function for XY, XYZ, or XYZM coordinates.
    template <typename Coord>
    void updateInternal(const Coord& coord) {
      for (size_t i = 0; i < coord.size(); ++i) {
        if (!std::isnan(min[i]) && !std::isnan(max[i])) {
          min[i] = std::min(min[i], coord[i]);
          max[i] = std::max(max[i], coord[i]);
        }
      }
    }
  };

  inline bool operator==(const BoundingBox& lhs, const BoundingBox& rhs) {
    return lhs.min == rhs.min && lhs.max == rhs.max;
  }
  inline bool operator!=(const BoundingBox& lhs, const BoundingBox& rhs) {
    return !(lhs == rhs);
  }
  inline std::ostream& operator<<(std::ostream& os, const BoundingBox& obj) {
    os << obj.toString();
    return os;
  }

}  // namespace orc::geospatial

#endif  // ORC_GEOSPATIAL_HH
