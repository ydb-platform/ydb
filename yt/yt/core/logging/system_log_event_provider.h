#pragma once

#include "public.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

struct ISystemLogEventProvider
{
    virtual ~ISystemLogEventProvider() = default;

    //! Returns |std::nullopt| if the corresponding event should be skipped and not logged.
    virtual std::optional<TLogEvent> GetStartLogEvent() const = 0;
    virtual std::optional<TLogEvent> GetSkippedLogEvent(i64 count, TStringBuf skippedBy) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISystemLogEventProvider> CreateDefaultSystemLogEventProvider(
    bool systemMessagesEnabled,
    ELogFamily systemMessagesFamily);
std::unique_ptr<ISystemLogEventProvider> CreateDefaultSystemLogEventProvider(const TLogWriterConfigPtr& writerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
