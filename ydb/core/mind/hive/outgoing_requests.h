#pragma once
#include <ranges>
#include <unordered_map>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NHive {

struct TRequestInfo {
    TInstant StartTime;
    TActorId Recipient;
    TString Description;
};

struct TRequests {
    std::unordered_map<ui64, TRequestInfo> Requests;
    ui32 Cookie = 0;

    ui32 AddRequest(const TString& description, const TActorId& recipient) {
        Requests.emplace(++Cookie, TRequestInfo{TActivationContext::Now(), recipient, description});
        return Cookie;
    }

    ui32 AddRequest(const IEventBase* event, const TActorId& recipient) {
        return AddRequest(event->ToStringHeader(), recipient);
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
