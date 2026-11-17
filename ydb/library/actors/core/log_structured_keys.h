#pragma once

#include <util/generic/string.h>

namespace NActors::NLog::NStructured {

// Metadata keys of the structured log format: FormatKey goes to Unified Agent
// session metadata, MessageKey and TimestampKey to UA message metadata; the
// record converter also sets the UA message timestamp from TimestampKey.
inline const TString FormatKey = "_ydb_log_format";
inline const TString FormatVersion = "structured_v1";
inline const TString MessageKey = "_ydb_message";
inline const TString TimestampKey = "_ydb_timestamp_us";

} // namespace NActors::NLog::NStructured
