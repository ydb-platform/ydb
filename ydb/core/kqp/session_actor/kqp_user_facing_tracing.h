#pragma once

#include <util/generic/string.h>

namespace NKikimr::NKqp {

class TKqpQueryState;

// Finalizes the user-facing span of a finished query: builds its phase children
// (Queued/Compile/Execute) from timings available at reply time and ends the span
// with the outcome. No-op when the user-facing channel didn't sample this query.
void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode);

} // namespace NKikimr::NKqp
