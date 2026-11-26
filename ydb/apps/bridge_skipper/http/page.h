#pragma once

#include <ydb/apps/bridge_skipper/runtime/skipper.h>

namespace NBridgeSkipper {

// The canonical projection is shared by /api/state and initial server rendering.
NJson::TJsonValue BuildPresentation(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow);
TString RenderPage(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow);

} // namespace NBridgeSkipper
