#include "dynamic_sketch.h"

namespace NKikimr::NKll {
    bool TryAcceptSmallWeight(ui64 weight, ui64 w0, std::mt19937_64& rng) {
        Y_ASSERT(w0 > 0);
        Y_ASSERT(weight > 0 && weight < w0);
        Y_ASSERT((w0 & (w0 - 1)) == 0);

        return (static_cast<ui64>(rng()) & (w0 - 1)) < weight;
    }
}
