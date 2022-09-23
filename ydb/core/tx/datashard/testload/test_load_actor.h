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
    };

    struct TEvTestLoadRequest
        : public TEventPB<TEvTestLoadRequest,
                          NKikimrDataShardLoad::TEvTestLoadRequest,
                          EvTestLoadRequest>
    {
        TEvTestLoadRequest() = default;
    };

    struct TEvTestLoadResponse
        : public TEventPB<TEvTestLoadResponse,
                          NKikimrDataShardLoad::TEvTestLoadResponse,
                          EvTestLoadResponse>
    {
        TEvTestLoadResponse() = default;
    };

    struct TLoadReport {
        TDuration Duration;
        ui64 OperationsOK = 0;
        ui64 OperationsError = 0;

        // info might contain result for multiple subtests
        TString Info;
        ui64 SubtestCount = 0;

        TString ToString() const {
            TStringStream ss;
            ss << "Load duration: " << Duration << ", OK=" << OperationsOK << ", Error=" << OperationsError;
            if (OperationsOK && Duration.Seconds()) {
                ui64 throughput = OperationsOK / Duration.Seconds();
                ss << ", throughput=" << throughput << " OK_ops/s";
            }
            if (SubtestCount) {
                ss << ", subtests: " << SubtestCount;
            }
            if (Info) {
                ss << ", Info: " << Info;
            }
            return ss.Str();
        }
    };

    struct TEvTestLoadFinished : public TEventLocal<TEvTestLoadFinished, EvTestLoadFinished> {
        ui64 Tag;
        std::optional<TLoadReport> Report;
        TString ErrorReason;

        TEvTestLoadFinished() = default;

        TEvTestLoadFinished(ui64 tag, const TString& error = {})
            : Tag(tag)
            , ErrorReason(error)
        {}
    };
};

namespace NDataShardLoad {

NActors::IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

} // NDataShardLoad
} // NKikimr
