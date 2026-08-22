
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

#ifndef ORC_GEOSPATIAL_IMPL_HH
#define ORC_GEOSPATIAL_IMPL_HH

#include "orc/Geospatial.hh"

#include <unordered_set>
#include <vector>

namespace orc {
  namespace geospatial {
    class WKBBuffer;

    class WKBGeometryBounder {
     public:
      void mergeGeometry(std::string_view bytesWkb);
      void mergeGeometry(const uint8_t* bytesWkb, size_t bytesSize);

      void mergeBox(const BoundingBox& box) {
        box_.merge(box);
      }
      void mergeGeometryTypes(const std::vector<int>& geospatialTypes) {
        geospatialTypes_.insert(geospatialTypes.begin(), geospatialTypes.end());
      }
      void merge(const WKBGeometryBounder& other) {
        if (!isValid() || !other.isValid()) {
          invalidate();
          return;
        }
        box_.merge(other.box_);
        geospatialTypes_.insert(other.geospatialTypes_.begin(), other.geospatialTypes_.end());
      }

      // Get the bounding box for the merged geometries.
      const BoundingBox& bounds() const {
        return box_;
      }

      // Get the set of geometry types encountered during merging.
      // Returns a sorted vector of geometry type IDs.
      std::vector<int32_t> geometryTypes() const;

      void reset() {
        isValid_ = true;
        box_.reset();
        geospatialTypes_.clear();
      }
      bool isValid() const {
        return isValid_;
      }
      void invalidate() {
        isValid_ = false;
        box_.invalidate();
        geospatialTypes_.clear();
      }

     private:
      BoundingBox box_;
      std::unordered_set<int32_t> geospatialTypes_;
      bool isValid_ = true;

      void mergeGeometryInternal(WKBBuffer* src, bool recordWkbType);
      void mergeSequence(WKBBuffer* src, Dimensions dimensions, uint32_t nCoords, bool swap);
    };
  }  // namespace geospatial
}  // namespace orc

#endif
