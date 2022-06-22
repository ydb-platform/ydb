#include "wilson_uploader.h"
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/wilson/protos/service.pb.h>
#include <library/cpp/actors/wilson/protos/service.grpc.pb.h>
#include <util/stream/file.h>
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
                Reader = Stub->AsyncExport(Context.get(), std::exchange(Request, {}), &CQ);
                Reader->Finish(&Response, &Status, nullptr);
            }

            void CheckIfDone() {
                if (Context) {
                    void *tag;
                    bool ok;
                    if (CQ.AsyncNext(&tag, &ok, std::chrono::system_clock::now()) == grpc::CompletionQueue::GOT_EVENT) {
                        Reader.reset();
                        Context.reset();

                        if (Request.resource_spans_size()) {
                            SendRequest();
                        }
                    }
                }
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvWilson, Handle);
            );
        };

    } // anonymous

    IActor *CreateWilsonUploader(TString host, ui16 port, TString rootCA) {
        return new TWilsonUploader(std::move(host), port, std::move(rootCA));
    }

} // NWilson
