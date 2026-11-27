#pragma once

#include <library/cpp/logger/backend.h>

#include <util/generic/ptr.h>

namespace NYql::NLog {

using TFormatter = std::function<TString(const TLogRecord&)>;

TString LegacyFormat(const TLogRecord& rec);

// Keep the legacy text layout with a YT-compatible timestamp (comma and six fractional digits).
TString LegacyFormatWithYtTimestamp(const TLogRecord& rec);

TString JsonFormat(const TLogRecord& rec);

TAutoPtr<TLogBackend> MakeFormattingLogBackend(TFormatter formatter, bool isStrict, TAutoPtr<TLogBackend> child);

} // namespace NYql::NLog
