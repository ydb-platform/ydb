#pragma once

#include "named_span.h"
#include "universal_span_wilson_api.h"

namespace NKikimr {

using TLazyRetroSpan = TUniversalSpanWilsonApi<TNamedSpan>;

} // namespace NKikimr
