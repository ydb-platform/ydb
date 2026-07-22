#pragma once

#include <util/generic/string.h>

namespace NKikimr::NKqp {

class TKqpQueryState;

// Renders the whole user-facing trace of a finished query at reply time. On failure errorMessage
// (query issues) becomes the span status message. No-op when the user channel didn't sample this
// query; consumes the trace context, so a repeated call is a no-op too.
void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode,
    const TString& errorMessage = {});

// Snapshots the root span name (e.g. "UPSERT /path/table") from the just-compiled statement —
// the physical plan knows the actual operation, unlike the raw text (comments/CTEs fool any
// substring search). The strongest verb across a script's statements wins (DDL > write > SELECT).
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
