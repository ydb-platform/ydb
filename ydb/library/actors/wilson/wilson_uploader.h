#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>
#include <grpc++/grpc++.h>

namespace NWilson {
    struct IGrpcSigner {
        virtual void SignClientContext(grpc::ClientContext& context) = 0;

        virtual ~IGrpcSigner() = default;
    };

    struct TEvWilson : NActors::TEventLocal<TEvWilson, NActors::TEvents::TSystem::Wilson> {
        opentelemetry::proto::trace::v1::Span Span;

        TEvWilson(opentelemetry::proto::trace::v1::Span *span) {
            Span.Swap(span);
        }
    };

    inline NActors::TActorId MakeWilsonUploaderId() {
        return NActors::TActorId(0, TStringBuf("WilsonUpload", 12));
    }

    struct WilsonUploaderParams {
        TString CollectorUrl;
        TString ServiceName;
        std::unique_ptr<IGrpcSigner> GrpcSigner;

        NActors::IActor* CreateUploader() &&;
    };

    NActors::IActor* CreateWilsonUploader(WilsonUploaderParams params);

} // NWilson
