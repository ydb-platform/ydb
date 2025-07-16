#include "events.h"

namespace NYql::NDq {

bool operator<(const TMetricTimeRange& a, const TMetricTimeRange& b) {
    return std::tie(a.Selectors, a.Program, a.From, a.To) < 
           std::tie(b.Selectors, b.Program, b.From, b.To);
}

} // namespace NYql::NDq
