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

    using TRegisterMonPageCallback = std::function<void(NActors::TActorSystem* actorSystem, const NActors::TActorId& actorId)>;

    struct TWilsonUploaderParams {
        TString CollectorUrl;
        TString ServiceName;
        std::unique_ptr<IGrpcSigner> GrpcSigner;
        TMap<TString, TString> Headers;

        ui64 MaxExportedSpansPerSecond = Max<ui64>();
        ui64 MaxSpansInBatch = 150;
        ui64 MaxBytesInBatch = 20'000'000;
        ui64 MaxBatchAccumulationMilliseconds = 1'000;
        ui32 SpanExportTimeoutSeconds = 60 * 60 * 24 * 365;
        ui64 MaxExportRequestsInflight = 1;

        TRegisterMonPageCallback RegisterMonPage;

        NActors::IActor* CreateUploader() &&;
    };

    NActors::IActor* CreateWilsonUploader(TWilsonUploaderParams params);

} // NWilson
