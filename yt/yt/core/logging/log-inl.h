#ifndef LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#endif

#include <yt/yt/core/misc/error.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... TArgs>
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    const TError& error,
    TFormatString<TArgs...> format,
    TArgs&&... args)
{
    TTaggedPayloadWriter writer;
    auto* builder = writer.BeginMessage();
    AppendLogMessageWithFormat(builder, loggingContext, logger, format.Get(), std::forward<TArgs>(args)...);
    builder->AppendChar('\n');
    FormatValue(builder, error, TStringBuf("v"));
    writer.EndMessage();
    return {writer.Finish(), format.Get()};
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
