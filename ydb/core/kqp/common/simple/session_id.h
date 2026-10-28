#pragma once

#include <util/generic/strbuf.h>
#include <util/system/types.h>

#include <optional>

namespace NKikimr::NKqp {

// Validates the complete session identifier and returns its owning node.
std::optional<ui32> ValidateSessionId(TStringBuf sessionId);

} // namespace NKikimr::NKqp
