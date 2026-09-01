#pragma once
#include <ranges>
#include <unordered_map>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/struct_log/create_message.h>

namespace NKikimr::NHive {

using NActors::NStructuredLog::TStructuredMessage;

struct TRequestInfo {
    TInstant StartTime;
    TActorId Recipient;
    TStructuredMessage Description;
};

struct TRequests {
    std::unordered_map<ui64, TRequestInfo> Requests;
    ui32 Cookie = 0;

    ui32 AddRequest(TStructuredMessage description, const TActorId& recipient) {
        Requests.emplace(++Cookie, TRequestInfo{TActivationContext::Now(), recipient, std::move(description)});
        return Cookie;
    }

    ui32 AddRequest(const IEventBase* event, const TActorId& recipient) {
        return AddRequest(YDB_LOG_CREATE_MESSAGE({"eventType", event->Type()}, {"eventHeader", event->ToStringHeader()}), recipient);
    }

    void FinishRequests(TActorId recipient) {
        auto it = Requests.begin();
        while (it != Requests.end()) {
            if (it->second.Recipient == recipient) {
                Requests.erase(it++);
            } else {
                it++;
            }
        }
    }

    void FinishRequest(ui64 cookie) {
        Requests.erase(cookie);
    }

    auto GetHangingRequests(TDuration duration) {
        TInstant cutoff = TActivationContext::Now() - duration;
        return std::views::values(Requests) | std::views::filter([cutoff](const TRequestInfo& request) { return request.StartTime < cutoff; });
    }
};
} // NKikimr::NHive
