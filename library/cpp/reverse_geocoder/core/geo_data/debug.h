#pragma once

#include "geo_data.h"

#include <util/stream/output.h>

namespace NReverseGeocoder {
    namespace NGeoData {
        size_t Space(const IGeoData& g);

        void Show(IOutputStream& out, const IGeoData& g);

        bool Equals(const IGeoData& a, const IGeoData& b);

    }
}
