#ifndef LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion.
#include "log.h"
#endif
#undef LOG_INL_H_

#include <yt/yt/core/misc/error.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <size_t Length, class... TArgs>
TLogMessage BuildLogMessage(
    const TLoggingContext& loggingContext,
    const TLogger& logger,
    const TError& error,
    const char (&format)[Length],
    TArgs&&... args)
{
    TMessageStringBuilder builder;
    AppendLogMessageWithFormat(&builder, loggingContext, logger, format, std::forward<TArgs>(args)...);
    builder.AppendChar('\n');
    FormatValue(&builder, error, TStringBuf());
    return {builder.Flush(), format};
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
