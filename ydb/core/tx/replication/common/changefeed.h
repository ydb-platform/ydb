#pragma once

#include <util/generic/string.h>

namespace NKikimr::NReplication {

inline constexpr TStringBuf KeyRangeCapability = "supports_key_range_autopartitioning";
extern const TString ReplicationConsumerName;

bool HasChangefeedCapability(TStringBuf replicationAttribute, TStringBuf capability);

} // namespace NKikimr::NReplication
