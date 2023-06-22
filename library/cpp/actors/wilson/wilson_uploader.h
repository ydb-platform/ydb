#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/wilson/protos/trace.pb.h>

namespace NWilson {

    struct TEvWilson : NActors::TEventLocal<TEvWilson, NActors::TEvents::TSystem::Wilson> {
        opentelemetry::proto::trace::v1::Span Span;

        TEvWilson(opentelemetry::proto::trace::v1::Span *span) {
            Span.Swap(span);
        }
    };

    inline NActors::TActorId MakeWilsonUploaderId() {
        return NActors::TActorId(0, TStringBuf("WilsonUpload", 12));
    }

    NActors::IActor *CreateWilsonUploader(TString host, ui16 port, TString rootCA, TString serviceName);

} // NWilson
