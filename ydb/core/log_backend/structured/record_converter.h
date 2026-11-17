#pragma once

#include <library/cpp/unified_agent_client/backend.h>

namespace NKikimr {

// Converts actor logger records that carry a valid _ydb_timestamp_us: it sets
// the message timestamp, and a payload starting with that time token and a space
// loses the token. Other records retain the default UA conversion, including
// audit and raw JSON messages.
THolder<NUnifiedAgent::IRecordConverter> MakeStructuredLogRecordConverter();

} // namespace NKikimr
