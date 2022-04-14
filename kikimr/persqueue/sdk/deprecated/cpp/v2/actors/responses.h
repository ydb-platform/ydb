#pragma once
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/iprocessor.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/responses.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>

namespace NPersQueue {

template <class TResponseType_, ui32 EventTypeId>
struct TPQLibResponseEvent: public NActors::TEventLocal<TPQLibResponseEvent<TResponseType_, EventTypeId>, EventTypeId> {
    using TResponseType = TResponseType_;

    explicit TPQLibResponseEvent(TResponseType&& response, ui64 requestId = 0)
        : Response(std::move(response))
        , RequestId(requestId)
    {
    }

    TResponseType Response;
    ui64 RequestId;
};

template <ui32 EventTypeId>
struct TEvPQLibLog : NActors::TEventLocal<TEvPQLibLog<EventTypeId>, EventTypeId> {
    TEvPQLibLog(const TString& msg, const TString& sourceId, const TString& sessionId, NActors::NLog::EPriority level)
    : Message(msg)
        , SourceId(sourceId)
        , SessionId(sessionId)
        , Level(level)
    {}

    TString Message;
    TString SourceId;
    TString SessionId;
    NActors::NLog::EPriority Level;
};

} // namespace NPersQueue
