#pragma once

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

inline NActors::NStructuredLog::TStructuredMessage LogPrefix() { return {}; }

} // namespace NKikimr::NPQ
