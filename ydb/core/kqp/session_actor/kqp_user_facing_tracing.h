#pragma once

#include <util/generic/string.h>

namespace NKikimr::NKqp {

class TKqpQueryState;

void InitializeUserFacingQueryText(TKqpQueryState& state);

// Consumes the sampled trace context and renders the finished query.
void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode);

// Derives the root name from the physical query rather than raw SQL text.
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
