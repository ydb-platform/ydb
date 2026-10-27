#include "jitter.h"

#include <util/random/normal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

double DefaultJitterRandomGenerator()
{
    // StdNormalRandom is unlikely to produce a value outside of [-Max, Max] range.
    constexpr double Max = 7.0;
    return std::clamp(StdNormalRandom<double>() / Max, -1.0, +1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
