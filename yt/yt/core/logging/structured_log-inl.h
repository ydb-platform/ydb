#ifndef STRUCTURED_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include structured_log.h"
// For the sake of sane code completion.
#include "fluent_log.h"
#include "structured_log.h"
#endif
#undef STRUCTURED_LOG_INL_H_

#include "fluent_log.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

template <typename... Values>
void LogStructuredEvent(
    const TLogger& logger,
    ELogLevel level,
    std::string_view message,
    std::tuple<const char*, Values>&&... tags)
{
    auto event = LogStructuredEventFluently(logger, level).Item("message").Value(message);

    ((event = event.Item(std::get<0>(tags)).Value(std::forward<Values>(std::get<1>(tags)))), ...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
