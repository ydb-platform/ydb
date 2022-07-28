#include "wilson_uploader.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/wilson/protos/service.pb.h>
#include <library/cpp/actors/wilson/protos/service.grpc.pb.h>
#include <util/stream/file.h>
#include <util/string/hex.h>
#include <grpc++/grpc++.h>
#include <chrono>

namespace NWilson {

    using namespace NActors;

    namespace NServiceProto = opentelemetry::proto::collector::trace::v1;

    namespace {

        class TWilsonUploader
            : public TActorBootstrapped<TWilsonUploader>
        {
            TString Host;
            ui16 Port;
            TString RootCA;

            std::shared_ptr<grpc::Channel> Channel;
            std::unique_ptr<NServiceProto::TraceService::Stub> Stub;
            grpc::CompletionQueue CQ;

            std::unique_ptr<grpc::ClientContext> Context;
            std::unique_ptr<grpc::ClientAsyncResponseReader<NServiceProto::ExportTraceServiceResponse>> Reader;
            NServiceProto::ExportTraceServiceRequest Request;
            NServiceProto::ExportTraceServiceResponse Response;
            grpc::Status Status;

            bool WakeupScheduled = false;

        public:
            TWilsonUploader(TString host, ui16 port, TString rootCA)
                : Host(std::move(host))
                , Port(std::move(port))
                , RootCA(std::move(rootCA))
            {}

            ~TWilsonUploader() {
                CQ.Shutdown();
            }

            void Bootstrap() {
                Become(&TThis::StateFunc);

                Channel = grpc::CreateChannel(TStringBuilder() << Host << ":" << Port, grpc::SslCredentials({
                    .pem_root_certs = TFileInput(RootCA).ReadAll(),
                }));
                Stub = NServiceProto::TraceService::NewStub(Channel);

                LOG_INFO_S(*TlsActivationContext, 430, "TWilsonUploader::Bootstrap");
            }

            void Handle(TEvWilson::TPtr ev) {
                CheckIfDone();

                auto *rspan = Request.resource_spans_size() ? Request.mutable_resource_spans(0) : Request.add_resource_spans();
                auto *sspan = rspan->scope_spans_size() ? rspan->mutable_scope_spans(0) : rspan->add_scope_spans();
                ev->Get()->Span.Swap(sspan->add_spans());

                if (!Context) {
                    SendRequest();
                }
            }

            void SendRequest() {
                Y_VERIFY(!Reader && !Context);
                Context = std::make_unique<grpc::ClientContext>();
                for (const auto& rs : Request.resource_spans()) {
                    for (const auto& ss : rs.scope_spans()) {
                        for (const auto& s : ss.spans()) {
                            LOG_DEBUG_S(*TlsActivationContext, 430 /* NKikimrServices::WILSON */, "exporting span"
                                << " TraceId# " << HexEncode(s.trace_id())
                                << " SpanId# " << HexEncode(s.span_id())
                                << " ParentSpanId# " << HexEncode(s.parent_span_id())
                                << " Name# " << s.name());
                        }
                    }
                }
                Reader = Stub->AsyncExport(Context.get(), std::exchange(Request, {}), &CQ);
                Reader->Finish(&Response, &Status, nullptr);
            }

            void CheckIfDone() {
                if (Context) {
                    void *tag;
                    bool ok;
                    if (CQ.AsyncNext(&tag, &ok, std::chrono::system_clock::now()) == grpc::CompletionQueue::GOT_EVENT) {
                        if (!Status.ok()) {
                            LOG_ERROR_S(*TlsActivationContext, 430 /* NKikimrServices::WILSON */, 
                                "failed to commit traces: " << Status.error_message());
                        }

                        Reader.reset();
                        Context.reset();

                        if (Request.resource_spans_size()) {
                            SendRequest();
                        }
                    } else if (!WakeupScheduled) {
                        WakeupScheduled = true;
                        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
                    }
                }
            }

            void HandleWakeup() {
                Y_VERIFY(WakeupScheduled);
                WakeupScheduled = false;
                CheckIfDone();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvWilson, Handle);
                cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            );
        };

    } // anonymous

    IActor *CreateWilsonUploader(TString host, ui16 port, TString rootCA) {
        return new TWilsonUploader(std::move(host), port, std::move(rootCA));
    }

} // NWilson
