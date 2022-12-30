#pragma once

#include "defs.h"

#include <ydb/core/base/events.h>
#include <ydb/core/protos/datashard_load.pb.h>

#include <library/cpp/actors/core/event_pb.h>

namespace NKikimr {

struct TEvDataShardLoad {
    enum EEv {
        EvTestLoadRequest = EventSpaceBegin(TKikimrEvents::ES_DATASHARD_LOAD),
        EvTestLoadResponse,

        EvTestLoadFinished,

        EvTestLoadInfoRequest,
        EvTestLoadInfoResponse,
    };

    struct TEvYCSBTestLoadRequest
        : public TEventPB<TEvYCSBTestLoadRequest,
                          NKikimrDataShardLoad::TEvYCSBTestLoadRequest,
                          EvTestLoadRequest>
    {
        TEvYCSBTestLoadRequest() = default;
    };

    struct TEvTestLoadResponse
        : public TEventPB<TEvTestLoadResponse,
                          NKikimrDataShardLoad::TEvTestLoadResponse,
                          EvTestLoadResponse>
    {
        TEvTestLoadResponse() = default;
    };

    struct TEvTestLoadFinished
        : public TEventPB<TEvTestLoadFinished,
                          NKikimrDataShardLoad::TEvTestLoadFinished,
                          EvTestLoadFinished> {

        TEvTestLoadFinished() = default;

        TEvTestLoadFinished(ui64 tag, const TString& error = {})
        {
            Record.SetTag(tag);
            if (error)
                Record.SetErrorReason(error);
        }

        TString ToString() const {
            TStringStream ss;
            ss << Record.GetTag();
            if (Record.HasErrorReason()) {
                ss << " failed: " << Record.GetErrorReason();
            } else {
                const auto& report = Record.GetReport();
                ss << " " << report;
            }

           return ss.Str();
        }
    };

    struct TEvTestLoadInfoRequest
        : public TEventPB<TEvTestLoadInfoRequest,
                          NKikimrDataShardLoad::TEvTestLoadInfoRequest,
                          EvTestLoadInfoRequest>
    {
        TEvTestLoadInfoRequest() = default;
    };

    struct TEvTestLoadInfoResponse
        : public TEventPB<TEvTestLoadInfoResponse,
                          NKikimrDataShardLoad::TEvTestLoadInfoResponse,
                          EvTestLoadInfoResponse>
    {
        TEvTestLoadInfoResponse() = default;

        TEvTestLoadInfoResponse(ui64 tag, const TString& data) {
            auto* report = Record.AddReports();
            report->SetTag(tag);
            report->SetInfo(std::move(data));
        }
    };
};

namespace NDataShardLoad {

IActor *CreateTestLoadActor(
    const NKikimrDataShardLoad::TEvYCSBTestLoadRequest& request,
    TActorId parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui64 tag);

} // NDataShardLoad
} // NKikimr
