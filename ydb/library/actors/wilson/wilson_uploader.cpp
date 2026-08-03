#include "wilson_uploader.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <contrib/proto/opentelemetry/opentelemetry/proto/collector/trace/v1/trace_service.pb.h>
#include <contrib/proto/opentelemetry/opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/stream/file.h>
#include <util/string/hex.h>

#include <chrono>
#include <queue>

#define YDB_LOG_THIS_FILE_COMPONENT WILSON_SERVICE_ID

namespace NWilson {

    using namespace NActors;

    namespace NServiceProto = opentelemetry::proto::collector::trace::v1;
    namespace NTraceProto = opentelemetry::proto::trace::v1;

    namespace {

        struct TSpan {
            TMonotonic ExpirationTimestamp;
            NTraceProto::Span Span;
            size_t Size;
        };

        class TBatch {
        private:
            ui64 MaxSpansInBatch;
            ui64 MaxBytesInBatch;

            NServiceProto::ExportTraceServiceRequest Request;
            NTraceProto::ScopeSpans* ScopeSpans;
            ui64 SizeBytes = 0;
            TMonotonic ExpirationTimestamp = TMonotonic::Zero();

        public:
            struct TData {
                NServiceProto::ExportTraceServiceRequest Request;
                ui64 SizeBytes;
                ui64 SizeSpans;
                TMonotonic ExpirationTimestamp;
            };

            TBatch(ui64 maxSpansInBatch, ui64 maxBytesInBatch, const TMap<TString, TString>& attributes)
                : MaxSpansInBatch(maxSpansInBatch)
                , MaxBytesInBatch(maxBytesInBatch)
            {
                auto *rspan = Request.add_resource_spans();
                for (const auto& [key, value] : attributes) {
                    auto *attr = rspan->mutable_resource()->add_attributes();
                    attr->set_key(key);
                    attr->mutable_value()->set_string_value(value);
                }
                ScopeSpans = rspan->add_scope_spans();
            }

            size_t SizeSpans() const {
                return ScopeSpans->spansSize();
            }

            bool IsEmpty() const {
                return SizeSpans() == 0;
            }

            bool Add(TSpan& span) {
                if (SizeBytes + span.Size > MaxBytesInBatch || SizeSpans() == MaxSpansInBatch) {
                    return false;
                }
                SizeBytes += span.Size;
                span.Span.Swap(ScopeSpans->add_spans());
                ExpirationTimestamp = span.ExpirationTimestamp;
                return true;
            }

            TData Complete() && {
                return TData {
                    .Request = std::move(Request),
                    .SizeBytes = SizeBytes,
                    .SizeSpans = SizeSpans(),
                    .ExpirationTimestamp = ExpirationTimestamp,
                };
            }
        };

        struct TExportRequestData : TIntrusiveListItem<TExportRequestData> {
            std::unique_ptr<grpc::ClientContext> Context;
            std::unique_ptr<grpc::ClientAsyncResponseReader<NServiceProto::ExportTraceServiceResponse>> Reader;
            grpc::Status Status;
            NServiceProto::ExportTraceServiceResponse Response;
        };

        class TWilsonUploader
            : public TActorBootstrapped<TWilsonUploader>
        {
            static constexpr size_t WILSON_SERVICE_ID = 430;

            ui64 MaxPendingSpanBytes = 100'000'000;
            ui64 MaxSpansPerSecond;
            ui64 MaxSpansInBatch;
            ui64 MaxBytesInBatch;
            TDuration MaxBatchAccumulation = TDuration::Seconds(1);
            TDuration MaxSpanTimeInQueue;
            ui64 MaxExportInflight;

            bool WakeupScheduled = false;

            TString CollectorUrl;
            const TMap<TString, TString> SpanAttributes;
            TMap<TString, TString> Headers;

            TRegisterMonPageCallback RegisterMonPage;

            std::shared_ptr<grpc::Channel> Channel;
            std::unique_ptr<NServiceProto::TraceService::Stub> Stub;
            grpc::CompletionQueue CQ;

            std::unique_ptr<IGrpcSigner> GrpcSigner;

            TBatch CurrentBatch;
            std::queue<TBatch::TData> BatchQueue;
            ui64 SpansSizeBytes = 0;
            TMonotonic NextSendTimestamp;

            bool BatchCompletionScheduled = false;
            TMonotonic NextBatchCompletion;

            TIntrusiveListWithAutoDelete<TExportRequestData, TDelete> ExportRequests;
            size_t ExportRequestsCount = 0;

            TString ErrStr;
            TString LastCommitTraceErrStr;

            NMonitoring::TDynamicCounters::TCounterPtr DroppedSpansCounter;
            NMonitoring::TDynamicCounters::TCounterPtr SentSpansCounter;
            NMonitoring::TDynamicCounters::TCounterPtr SentBytesCounter;
            NMonitoring::TDynamicCounters::TCounterPtr SentSpanBatchesOkCounter;
            NMonitoring::TDynamicCounters::TCounterPtr SentSpanBatchesErrCounter;

        public:
            TWilsonUploader(TWilsonUploaderParams params)
                : MaxSpansPerSecond(params.MaxExportedSpansPerSecond)
                , MaxSpansInBatch(params.MaxSpansInBatch)
                , MaxBytesInBatch(params.MaxBytesInBatch)
                , MaxBatchAccumulation(TDuration::MilliSeconds(params.MaxBatchAccumulationMilliseconds))
                , MaxSpanTimeInQueue(TDuration::Seconds(params.SpanExportTimeoutSeconds))
                , MaxExportInflight(params.MaxExportRequestsInflight)
                , CollectorUrl(std::move(params.CollectorUrl))
                , SpanAttributes(GetSpanAttributes(params.ServiceName))
                , Headers(params.Headers)
                , RegisterMonPage(params.RegisterMonPage)
                , GrpcSigner(std::move(params.GrpcSigner))
                , CurrentBatch(MaxSpansInBatch, MaxBytesInBatch, SpanAttributes)
                , DroppedSpansCounter(params.Counters ? params.Counters->GetCounter("WilsonUploaderDroppedSpans", true) : MakeIntrusive<NMonitoring::TCounterForPtr>(true))
                , SentSpansCounter(params.Counters ? params.Counters->GetCounter("WilsonUploaderSentSpans", true) : MakeIntrusive<NMonitoring::TCounterForPtr>(true))
                , SentBytesCounter(params.Counters ? params.Counters->GetCounter("WilsonUploaderSentBytes", true) : MakeIntrusive<NMonitoring::TCounterForPtr>(true))
                , SentSpanBatchesOkCounter(params.Counters ? params.Counters->GetCounter("WilsonUploaderSentSpanBatchesOk", "true") : MakeIntrusive<NMonitoring::TCounterForPtr>(true))
                , SentSpanBatchesErrCounter(params.Counters ? params.Counters->GetCounter("WilsonUploaderSentSpanBatchesErr", "true") : MakeIntrusive<NMonitoring::TCounterForPtr>(true))
            {}

            ~TWilsonUploader() {
                CQ.Shutdown();
            }

            static constexpr char ActorName[] = "WILSON_UPLOADER_ACTOR";

            static TMap<TString, TString> GetSpanAttributes(const TString& serviceName) {
                TMap<TString, TString> attributes;
                attributes["service.name"] = serviceName;
                if (TString podUid = getenv("POD_UID")) {
                    attributes["k8s.pod.uid"] = podUid;
                }
                return attributes;
            }

            void Bootstrap() {
                Become(&TThis::StateWork);

                if (MaxSpansPerSecond == 0) {
                    YDB_LOG_WARN("Max_spans_per_second should be greater than 0, changing to 1");
                    MaxSpansPerSecond = 1;
                }
                if (MaxSpansInBatch == 0) {
                    YDB_LOG_WARN("Max_spans_in_batch shold be greater than 0, changing to 1");
                    MaxSpansInBatch = 1;
                }
                if (MaxExportInflight == 0) {
                    YDB_LOG_WARN("Max_span_export_inflight should be greater than 0, changing to 1");
                    MaxExportInflight = 1;
                }

                TStringBuf scheme;
                TStringBuf host;
                ui16 port;
                if (!TryGetSchemeHostAndPort(CollectorUrl, scheme, host, port)) {
                    ErrStr = "Failed to parse collector url (" + CollectorUrl + " was provided). Wilson wouldn't work";
                    YDB_LOG_ERROR("Failed to parse collector url. Wilson wouldn't work",
                        {"url", CollectorUrl});
                    Become(&TThis::StateBroken);
                    return;
                } else if (scheme != "grpc://" && scheme != "grpcs://") {
                    TStringStream ss;
                    ss << "Wrong scheme provided: " << scheme << " (only grpc:// and grpcs:// are supported). Wilson wouldn't work";
                    ErrStr = ss.Str();
                    YDB_LOG_ERROR("Wrong scheme provided (only grpc:// and grpcs:// are supported). Wilson wouldn't work",
                        {"scheme", scheme});
                    Become(&TThis::StateBroken);
                    return;
                }
                Channel = grpc::CreateChannel(TStringBuilder() << host << ":" << port,
                                              scheme == "grpcs://" ? grpc::SslCredentials({}) : grpc::InsecureChannelCredentials());
                Stub = NServiceProto::TraceService::NewStub(Channel);

                YDB_LOG_INFO("TWilsonUploader::Bootstrap");
            }

            void Registered(TActorSystem* sys, const TActorId& owner) override {
                TActorBootstrapped<TWilsonUploader>::Registered(sys, owner);

                if (const auto& mon = RegisterMonPage) {
                    mon(sys, SelfId());
                }
            }

            void Handle(TEvWilson::TPtr ev) {
                if (SpansSizeBytes >= MaxPendingSpanBytes) {
                    DroppedSpansCounter->Inc();
                    YDB_LOG_ERROR("Dropped span due to overflow");
                } else {
                    const TMonotonic now = TActivationContext::Monotonic();
                    const TMonotonic expirationTimestamp = now + MaxSpanTimeInQueue;
                    auto& span = ev->Get()->Span;
                    const ui32 size = span.ByteSizeLong();
                    if (size > MaxBytesInBatch) {
                        DroppedSpansCounter->Inc();
                        YDB_LOG_ERROR("Dropped span of size which exceeds max batch size",
                            {"size", size},
                            {"maxBytesInBatch", MaxBytesInBatch});
                        return;
                    }
                    TSpan spanItem {
                        .ExpirationTimestamp = expirationTimestamp,
                        .Span = std::move(span),
                        .Size = size,
                    };
                    SpansSizeBytes += size;
                    if (CurrentBatch.IsEmpty()) {
                        ScheduleBatchCompletion(now);
                    }
                    if (CurrentBatch.Add(spanItem)) {
                        return;
                    }
                    CompleteCurrentBatch();
                    TryMakeProgress();
                    Y_ABORT_UNLESS(CurrentBatch.Add(spanItem), "failed to add span to empty batch");
                    ScheduleBatchCompletion(now);
                }
            }

            void ScheduleBatchCompletionEvent() {
                Y_ABORT_UNLESS(!BatchCompletionScheduled);
                auto cookie = NextBatchCompletion.GetValue();
                TActivationContext::Schedule(NextBatchCompletion, new IEventHandle(TEvents::TSystem::Wakeup, 0, SelfId(), {}, nullptr, cookie));
                YDB_LOG_TRACE("Scheduling batch completion w/",
                    {"cookie", cookie});
                BatchCompletionScheduled = true;
            }

            void ScheduleBatchCompletion(TMonotonic now) {
                NextBatchCompletion = now + MaxBatchAccumulation;
                if (!BatchCompletionScheduled) {
                    ScheduleBatchCompletionEvent();
                }
            }

            void CompleteCurrentBatch() {
                if (CurrentBatch.IsEmpty()) {
                    return;
                }
                BatchQueue.push(std::move(CurrentBatch).Complete());
                CurrentBatch = TBatch(MaxSpansInBatch, MaxBytesInBatch, SpanAttributes);
            }

            void TryToSend() {
                const TMonotonic now = TActivationContext::Monotonic();

                ui32 numSpansDropped = 0;
                while (!BatchQueue.empty()) {
                    const TBatch::TData& item = BatchQueue.front();
                    if (item.ExpirationTimestamp <= now) {
                        SpansSizeBytes -= item.SizeBytes;
                        numSpansDropped += item.SizeSpans;
                        BatchQueue.pop();
                    } else {
                        break;
                    }
                }

                if (numSpansDropped) {
                    DroppedSpansCounter->Add(numSpansDropped);
                    YDB_LOG_ERROR("Dropped span(s) due to expiration",
                        {"numSpansDropped", numSpansDropped});
                }

                if (ExportRequestsCount >= MaxExportInflight || BatchQueue.empty()) {
                    return;
                } else if (now < NextSendTimestamp) {
                    ScheduleWakeup(NextSendTimestamp);
                    return;
                }

                TBatch::TData batch = std::move(BatchQueue.front());
                BatchQueue.pop();

                YDB_LOG_DEBUG("Exporting batch of spans, total spans",
                    {"spans", batch.SizeSpans},
                    {"size", batch.SizeBytes});
                Y_ABORT_UNLESS(batch.Request.resource_spansSize() == 1 && batch.Request.resource_spans(0).scope_spansSize() == 1);
                for (const auto& span : batch.Request.resource_spans(0).scope_spans(0).spans()) {
                    YDB_LOG_DEBUG("Exporting span",
                        {"traceId", HexEncode(span.trace_id())},
                        {"spanId", HexEncode(span.span_id())},
                        {"parentSpanId", HexEncode(span.parent_span_id())},
                        {"name", span.name()});
                }
                SentSpansCounter->Add(batch.SizeSpans);
                SentBytesCounter->Add(batch.SizeBytes);

                NextSendTimestamp = now + TDuration::MicroSeconds((batch.SizeSpans * 1'000'000) / MaxSpansPerSecond);
                SpansSizeBytes -= batch.SizeBytes;

                ScheduleWakeup(NextSendTimestamp);

                auto context = std::make_unique<grpc::ClientContext>();
                if (GrpcSigner) {
                    GrpcSigner->SignClientContext(*context);
                }
                for (const auto& [key, value] : Headers) {
                    context->AddMetadata(key, value);
                }
                auto reader = Stub->AsyncExport(context.get(), std::move(batch.Request), &CQ);
                auto uploadData =  std::unique_ptr<TExportRequestData>(new TExportRequestData {
                    .Context = std::move(context),
                    .Reader = std::move(reader),
                });
                uploadData->Reader->Finish(&uploadData->Response, &uploadData->Status, uploadData.get());
                YDB_LOG_TRACE("Started export request",
                    {"request", static_cast<void*>(uploadData.get())});
                ExportRequests.PushBack(uploadData.release());
                ++ExportRequestsCount;
            }

            void ReapCompletedRequests() {
                if (ExportRequests.Empty()) {
                    return;
                }
                void* tag;
                bool ok;
                while (CQ.AsyncNext(&tag, &ok, std::chrono::system_clock::now()) == grpc::CompletionQueue::GOT_EVENT) {
                    auto node = std::unique_ptr<TExportRequestData>(static_cast<TExportRequestData*>(tag));
                    YDB_LOG_TRACE("Finished export request",
                        {"node", static_cast<void*>(node.get())});
                    if (!node->Status.ok()) {
                        SentSpanBatchesErrCounter->Inc();
                        LastCommitTraceErrStr = node->Status.error_message();

                        YDB_LOG_ERROR("Failed to commit",
                            {"traces", node->Status.error_message()});
                    } else {
                        SentSpanBatchesOkCounter->Inc();
                    }

                    --ExportRequestsCount;
                    node->Unlink();
                }

                if (!ExportRequests.Empty()) {
                    ScheduleWakeup(TDuration::MilliSeconds(100));
                }
            }

            template<typename T>
            void ScheduleWakeup(T&& deadline) {
                if (!WakeupScheduled) {
                    TActivationContext::Schedule(deadline,
                                                 new IEventHandle(TEvents::TSystem::Wakeup, 0,
                                                                  SelfId(), {}, nullptr, 0));
                    WakeupScheduled = true;
                }
            }

            void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
                const auto cookie = ev->Cookie;
                YDB_LOG_TRACE("Wakeup received w/",
                    {"cookie", cookie});
                if (cookie == 0) {
                    Y_ABORT_UNLESS(WakeupScheduled);
                    WakeupScheduled = false;
                } else {
                    Y_ABORT_UNLESS(BatchCompletionScheduled);
                    BatchCompletionScheduled = false;
                    if (cookie == NextBatchCompletion.GetValue()) {
                        CompleteCurrentBatch();
                    } else {
                        ScheduleBatchCompletionEvent();
                    }
                }
                TryMakeProgress();
            }

            void TryMakeProgress() {
                ReapCompletedRequests();
                TryToSend();
            }

            void HandleHttp(NMon::TEvHttpInfo::TPtr &ev) {
                TStringStream str;
                str.Reserve(64 << 10);

                bool isBroken = CurrentStateFunc() == &TThis::StateBroken;

                HTML(str) {
                    TAG(TH4) {str << "Current state";}
                    PARA() {
                        str << (isBroken ? "Broken" : "Works");
                    }
                    if (ErrStr) {
                        PARA() {
                            str << "Error: " << ErrStr;
                        }
                    }
                    if (LastCommitTraceErrStr) {
                        PARA() {
                            str << "Last commit traces error: " << LastCommitTraceErrStr;
                        }
                    }
                    PARA() {
                        str << "Current batch size: " << CurrentBatch.SizeSpans();
                    }
                    PARA() {
                        str << "Current batch queue size: " << BatchQueue.size();
                    }
                    PARA() {
                        str << "Sent spans: " << SentBytesCounter->Val();
                    }
                    PARA() {
                        str << "Dropped spans: " << DroppedSpansCounter->Val();
                    }
                    PARA() {
                        std::string state;
                        switch (Channel->GetState(false)) {
                            case GRPC_CHANNEL_IDLE:
                                state = "GRPC_CHANNEL_IDLE";
                                break;
                            case GRPC_CHANNEL_CONNECTING:
                                state = "GRPC_CHANNEL_CONNECTING";
                                break;
                            case GRPC_CHANNEL_READY:
                                state = "GRPC_CHANNEL_READY";
                                break;
                            case GRPC_CHANNEL_TRANSIENT_FAILURE:
                                state = "GRPC_CHANNEL_TRANSIENT_FAILURE";
                                break;
                            case GRPC_CHANNEL_SHUTDOWN:
                                state = "GRPC_CHANNEL_SHUTDOWN";
                                break;
                            default:
                                state = "UNKNOWN_STATE";
                                break;
                        }
                        str << "Channel state# " << state;
                    }
                    TAG(TH4) {str << "Config";}
                    PRE() {
                        str << "MaxPendingSpanBytes# " << MaxPendingSpanBytes << '\n';
                        str << "MaxSpansPerSecond# " << MaxSpansPerSecond << '\n';
                        str << "MaxSpansInBatch# " << MaxSpansInBatch << '\n';
                        str << "MaxBytesInBatch# " << MaxBytesInBatch << '\n';
                        str << "MaxBatchAccumulation# " << MaxBatchAccumulation << '\n';
                        str << "MaxSpanTimeInQueue# " << MaxSpanTimeInQueue << '\n';
                        str << "MaxExportInflight# " << MaxExportInflight << '\n';
                        str << "CollectorUrl# " << CollectorUrl << '\n';
                        str << "SpanAttributes# " << '\n';
                        for (const auto& [key, value] : SpanAttributes) {
                            str << '\t' << key << ": " << value << '\n';
                        }
                        str << "Headers# " << '\n';
                        for (const auto& [key, value] : Headers) {
                            str << '\t' << key << ": " << value << '\n';
                        }
                    }
                }

                auto* result = new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Html);

                Send(ev->Sender, result);
            }

            STRICT_STFUNC(StateWork,
                hFunc(TEvWilson, Handle);
                hFunc(TEvents::TEvWakeup, HandleWakeup);
                hFunc(NMon::TEvHttpInfo, HandleHttp);
            );

            STRICT_STFUNC(StateBroken,
                IgnoreFunc(TEvWilson);
                hFunc(NMon::TEvHttpInfo, HandleHttp);
            );
        };

    } // anonymous

    IActor* CreateWilsonUploader(TWilsonUploaderParams params) {
        return new TWilsonUploader(std::move(params));
    }

    IActor* TWilsonUploaderParams::CreateUploader() && {
        return CreateWilsonUploader(std::move(*this));
    }

} // NWilson
