#pragma once

#include <util/system/compiler.h>
#include <util/system/types.h>

namespace NReverseGeocoder {
    using TCoordinate = i32;
    using TGeoId = ui64;
    using TNumber = ui32;
    using TRef = ui32;
    using TSquare = i64;
    using TVersion = ui64;

    const double EARTH_RADIUS = 6371000.0;

    inline TCoordinate ToCoordinate(double x) {
        return x * 1e6;
    }

    inline double ToDouble(TCoordinate x) {
        return x / 1e6;
    }

}
