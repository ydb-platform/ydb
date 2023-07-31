#pragma once

#include "common.h"

namespace NReverseGeocoder {
    // k and v is offsets on blobs in geographical data blobs array. See geo_data.h
    // for details.
    struct TKv {
        TNumber K;
        TNumber V;
    };

}
