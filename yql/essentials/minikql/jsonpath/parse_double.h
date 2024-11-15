#include <util/generic/string.h>

namespace NYql::NJsonPath {

// Parses double literal. Respects exponential format like `-23.5e-10`.
// On parsing error returns NaN double value (can be checked using `std::isnan`).
// On double overflow returns INF double value (can be checked using `std::isinf`).
double ParseDouble(const TStringBuf literal);

}