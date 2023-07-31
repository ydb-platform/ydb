#pragma once

#include "common.h"

namespace NReverseGeocoder {
    struct Y_PACKED TRegion {
        TGeoId RegionId;
        TNumber KvsOffset;
        TNumber KvsNumber;
        TSquare Square;
        TNumber PolygonsNumber;
        ui32 Unused;

        bool operator==(const TRegion& r) const {
            return RegionId == r.RegionId;
        }

        bool operator<(const TRegion& r) const {
            return RegionId < r.RegionId;
        }

        bool operator<(const TGeoId& r) const {
            return RegionId < r;
        }

        friend bool operator<(const TGeoId& regionId, const TRegion& r) {
            return regionId < r.RegionId;
        }

        bool Better(const TRegion& r) const {
            return Square < r.Square;
        }
    };

    static_assert(sizeof(TRegion) == 32, "NReverseGeocoder::TRegion size mismatch");

}
