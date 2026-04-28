#pragma once

#include <util/generic/string.h>

namespace NKikimr::NKqp {

// Replaces string literals in the SQL query with '***removed***' using
// the SQL parser (grammar-aware masking). Numbers, identifiers, keywords
// and query shape are preserved so the result stays useful for logging.
// If parsing fails, returns the original query unchanged.
TString MaskSensitiveLiterals(const TString& query);

} // namespace NKikimr::NKqp
