#include "wilson_uploader.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.pb.h>
#include <opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h>
#include <util/stream/file.h>
#include <util/string/hex.h>
#include <chrono>

namespace NWilson {

    using namespace NActors;

    namespace NServiceProto = opentelemetry::proto::collector::trace::v1;
    namespace NTraceProto = opentelemetry::proto::trace::v1;

    namespace {

        class TWilsonUploader
            : public TActorBootstrapped<TWilsonUploader>
        {
            static constexpr size_t WILSON_SERVICE_ID = 430;

            TString Host;
            ui16 Port;
            TString RootCA;
            TString ServiceName;

            std::shared_ptr<grpc::Channel> Channel;
            std::unique_ptr<NServiceProto::TraceService::Stub> Stub;
            grpc::CompletionQueue CQ;

            std::unique_ptr<IGrpcSigner> GrpcSigner;
            std::unique_ptr<grpc::ClientContext> Context;
            std::unique_ptr<grpc::ClientAsyncResponseReader<NServiceProto::ExportTraceServiceResponse>> Reader;
            NServiceProto::ExportTraceServiceResponse Response;
            grpc::Status Status;

            struct TSpanQueueItem {
                TMonotonic ExpirationTimestamp;
                NTraceProto::Span Span;
                ui32 Size;
            };

            std::deque<TSpanQueueItem> Spans;
            ui64 SpansSize = 0;
            TMonotonic NextSendTimestamp;
            ui32 MaxSpansAtOnce = 25;
            ui32 MaxSpansPerSecond = 10;
            TDuration MaxSpanTimeInQueue = TDuration::Seconds(60);

            bool WakeupScheduled = false;

        public:
            TWilsonUploader(WilsonUploaderParams params)
                : Host(std::move(params.Host))
                , Port(std::move(params.Port))
                , RootCA(std::move(params.RootCA))
                , ServiceName(std::move(params.ServiceName))
                , GrpcSigner(std::move(params.GrpcSigner))
            {}

            ~TWilsonUploader() {
                CQ.Shutdown();
            }

            static constexpr char ActorName[] = "WILSON_UPLOADER_ACTOR";

            void Bootstrap() {
                Become(&TThis::StateFunc);

                Channel = grpc::CreateChannel(TStringBuilder() << Host << ":" << Port, RootCA ? grpc::SslCredentials({
                    .pem_root_certs = TFileInput(RootCA).ReadAll(),
                }) : grpc::InsecureChannelCredentials());
                Stub = NServiceProto::TraceService::NewStub(Channel);

                LOG_INFO_S(*TlsActivationContext, WILSON_SERVICE_ID, "TWilsonUploader::Bootstrap");
            }

            void Handle(TEvWilson::TPtr ev) {
                if (SpansSize >= 100'000'000) {
                    LOG_ERROR_S(*TlsActivationContext, WILSON_SERVICE_ID, "dropped span due to overflow");
                } else {
                    const TMonotonic expirationTimestamp = TActivationContext::Monotonic() + MaxSpanTimeInQueue;
                    auto& span = ev->Get()->Span;
                    const ui32 size = span.ByteSizeLong();
                    Spans.push_back(TSpanQueueItem{expirationTimestamp, std::move(span), size});
                    SpansSize += size;
                    TryMakeProgress();
                }
            }

            void TryToSend() {
                const TMonotonic now = TActivationContext::Monotonic();

                ui32 numSpansDropped = 0;
                while (!Spans.empty()) {
                    const TSpanQueueItem& item = Spans.front();
                    if (item.ExpirationTimestamp <= now) {
                        SpansSize -= item.Size;
                        Spans.pop_front();
                        ++numSpansDropped;
                    } else {
                        break;
                    }
                }

                if (numSpansDropped) {
                    LOG_ERROR_S(*TlsActivationContext, WILSON_SERVICE_ID,
                        "dropped " << numSpansDropped << " span(s) due to expiration");
                }

                if (Context || Spans.empty()) {
                    return;
                } else if (now < NextSendTimestamp) {
                    ScheduleWakeup(NextSendTimestamp);
                    return;
                }

                NServiceProto::ExportTraceServiceRequest request;
                auto *rspan = request.add_resource_spans();
                auto *serviceNameAttr = rspan->mutable_resource()->add_attributes();
                serviceNameAttr->set_key("service.name");
                serviceNameAttr->mutable_value()->set_string_value(ServiceName);
                auto *sspan = rspan->add_scope_spans();

                NextSendTimestamp = now;
                for (ui32 i = 0; i < MaxSpansAtOnce && !Spans.empty(); ++i, Spans.pop_front()) {
                    auto& item = Spans.front();
                    auto& s = item.Span;

                    LOG_DEBUG_S(*TlsActivationContext, WILSON_SERVICE_ID, "exporting span"
                        << " TraceId# " << HexEncode(s.trace_id())
                        << " SpanId# " << HexEncode(s.span_id())
                        << " ParentSpanId# " << HexEncode(s.parent_span_id())
                        << " Name# " << s.name());

                    SpansSize -= item.Size;
                    s.Swap(sspan->add_spans());
                    NextSendTimestamp += TDuration::MicroSeconds(1'000'000 / MaxSpansPerSecond);
                }

                ScheduleWakeup(NextSendTimestamp);
                Context = std::make_unique<grpc::ClientContext>();
                if (GrpcSigner) {
                    GrpcSigner->SignClientContext(*Context);
                }
                Reader = Stub->AsyncExport(Context.get(), std::move(request), &CQ);
                Reader->Finish(&Response, &Status, nullptr);
            }

            void CheckIfDone() {
                if (Context) {
                    void *tag;
                    bool ok;
                    if (CQ.AsyncNext(&tag, &ok, std::chrono::system_clock::now()) == grpc::CompletionQueue::GOT_EVENT) {
                        if (!Status.ok()) {
                            LOG_ERROR_S(*TlsActivationContext, WILSON_SERVICE_ID,
                                "failed to commit traces: " << Status.error_message());
                        }

                        Reader.reset();
                        Context.reset();
                    } else {
                        ScheduleWakeup(TDuration::MilliSeconds(100));
                    }
                }
            }

            template<typename T>
            void ScheduleWakeup(T&& deadline) {
                if (!WakeupScheduled) {
                    TActivationContext::Schedule(deadline, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {},
                        nullptr, 0));
                    WakeupScheduled = true;
                }
            }

            void HandleWakeup() {
                Y_ABORT_UNLESS(WakeupScheduled);
                WakeupScheduled = false;
                TryMakeProgress();
            }

            void TryMakeProgress() {
                CheckIfDone();
                TryToSend();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvWilson, Handle);
                cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            );
        };

    } // anonymous

    IActor* CreateWilsonUploader(WilsonUploaderParams params) {
        return new TWilsonUploader(std::move(params));
    }

    IActor* WilsonUploaderParams::CreateUploader() && {
        return CreateWilsonUploader(std::move(*this));
    }

} // NWilson
