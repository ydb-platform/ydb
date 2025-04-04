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

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

consteval TStructuredLogKey::TStructuredLogKey(const char* key)
    : Key(key)
{
    if (std::string_view(key) == "message") {
        // Entering this branch causes a compilation failure that contains this
        // string in the error message.
        throw "Key 'message' is reserved and can not be used";
    }
}

template <typename T>
std::tuple<TStructuredLogKey, T> MakeTuple(TStructuredLogKey k, T&& t)
{
    return {k, std::forward<T>(t)};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename... Values>
void LogStructuredEvent(
    const TLogger& logger,
    ELogLevel level,
    std::string_view message,
    std::tuple<NDetail::TStructuredLogKey, Values>&&... tags)
{
    auto event = LogStructuredEventFluently(logger, level).Item("message").Value(message);

    ((event = event.Item(std::get<0>(tags).Key).Value(std::forward<Values>(std::get<1>(tags)))), ...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
