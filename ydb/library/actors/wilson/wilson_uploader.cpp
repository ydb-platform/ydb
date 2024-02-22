#include "wilson_uploader.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.pb.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/stream/file.h>
#include <util/string/hex.h>

#include <chrono>
#include <queue>

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

            TBatch(ui64 maxSpansInBatch, ui64 maxBytesInBatch, TString serviceName)
                : MaxSpansInBatch(maxSpansInBatch)
                , MaxBytesInBatch(maxBytesInBatch)
            {
                auto *rspan = Request.add_resource_spans();
                auto *serviceNameAttr = rspan->mutable_resource()->add_attributes();
                serviceNameAttr->set_key("service.name");
                serviceNameAttr->mutable_value()->set_string_value(std::move(serviceName));
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
            TString ServiceName;

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

        public:
            TWilsonUploader(WilsonUploaderParams params)
                : MaxSpansPerSecond(params.MaxExportedSpansPerSecond)
                , MaxSpansInBatch(params.MaxSpansInBatch)
                , MaxBytesInBatch(params.MaxBytesInBatch)
                , MaxBatchAccumulation(params.MaxBatchAccumulation)
                , MaxSpanTimeInQueue(TDuration::Seconds(params.SpanExportTimeoutSeconds))
                , MaxExportInflight(params.MaxExportRequestsInflight)
                , CollectorUrl(std::move(params.CollectorUrl))
                , ServiceName(std::move(params.ServiceName))
                , GrpcSigner(std::move(params.GrpcSigner))
                , CurrentBatch(MaxSpansInBatch, MaxBytesInBatch, ServiceName)
            {}

            ~TWilsonUploader() {
                CQ.Shutdown();
            }

            static constexpr char ActorName[] = "WILSON_UPLOADER_ACTOR";

            void Bootstrap() {
                Become(&TThis::StateWork);

                if (MaxSpansPerSecond == 0) {
                    ALOG_WARN(WILSON_SERVICE_ID, "max_spans_per_second should be greater than 0, changing to 1");
                    MaxSpansPerSecond = 1;
                }
                if (MaxSpansInBatch == 0) {
                    ALOG_WARN(WILSON_SERVICE_ID, "max_spans_in_batch shold be greater than 0, changing to 1");
                    MaxSpansInBatch = 1;
                }
                if (MaxExportInflight == 0) {
                    ALOG_WARN(WILSON_SERVICE_ID, "max_span_export_inflight should be greater than 0, changing to 1");
                    MaxExportInflight = 1;
                }

                TStringBuf scheme;
                TStringBuf host;
                ui16 port;
                if (!TryGetSchemeHostAndPort(CollectorUrl, scheme, host, port)) {
                    ALOG_ERROR(WILSON_SERVICE_ID, "Failed to parse collector url (" << CollectorUrl << " was provided). Wilson wouldn't work");
                    Become(&TThis::StateBroken);
                    return;
                } else if (scheme != "grpc://" && scheme != "grpcs://") {
                    ALOG_ERROR(WILSON_SERVICE_ID, "Wrong scheme provided: " << scheme << " (only grpc:// and grpcs:// are supported). Wilson wouldn't work");
                    Become(&TThis::StateBroken);
                    return;
                }
                Channel = grpc::CreateChannel(TStringBuilder() << host << ":" << port,
                                              scheme == "grpcs://" ? grpc::SslCredentials({}) : grpc::InsecureChannelCredentials());
                Stub = NServiceProto::TraceService::NewStub(Channel);

                ALOG_INFO(WILSON_SERVICE_ID, "TWilsonUploader::Bootstrap");
            }

            void Handle(TEvWilson::TPtr ev) {
                if (SpansSizeBytes >= MaxPendingSpanBytes) {
                    ALOG_ERROR(WILSON_SERVICE_ID, "dropped span due to overflow");
                } else {
                    const TMonotonic now = TActivationContext::Monotonic();
                    const TMonotonic expirationTimestamp = now + MaxSpanTimeInQueue;
                    auto& span = ev->Get()->Span;
                    const ui32 size = span.ByteSizeLong();
                    if (size > MaxBytesInBatch) {
                        ALOG_ERROR(WILSON_SERVICE_ID, "dropped span of size " << size << ", which exceeds max batch size " << MaxBytesInBatch);
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
                ALOG_TRACE(WILSON_SERVICE_ID, "scheduling batch completion w/ cookie=" << cookie);
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
                CurrentBatch = TBatch(MaxSpansInBatch, MaxBytesInBatch, ServiceName);
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
                    ALOG_ERROR(WILSON_SERVICE_ID,
                        "dropped " << numSpansDropped << " span(s) due to expiration");
                }

                if (ExportRequestsCount >= MaxExportInflight || BatchQueue.empty()) {
                    return;
                } else if (now < NextSendTimestamp) {
                    ScheduleWakeup(NextSendTimestamp);
                    return;
                }

                TBatch::TData batch = std::move(BatchQueue.front());
                BatchQueue.pop();

                ALOG_DEBUG(WILSON_SERVICE_ID, "exporting batch of " << batch.SizeSpans << " spans, total spans size: " << batch.SizeBytes);
                Y_ABORT_UNLESS(batch.Request.resource_spansSize() == 1 && batch.Request.resource_spans(0).scope_spansSize() == 1);
                for (const auto& span : batch.Request.resource_spans(0).scope_spans(0).spans()) {
                    ALOG_DEBUG(WILSON_SERVICE_ID, "exporting span"
                        << " TraceId# " << HexEncode(span.trace_id())
                        << " SpanId# " << HexEncode(span.span_id())
                        << " ParentSpanId# " << HexEncode(span.parent_span_id())
                        << " Name# " << span.name());
                }

                NextSendTimestamp = now + TDuration::MicroSeconds((batch.SizeSpans * 1'000'000) / MaxSpansPerSecond);
                SpansSizeBytes -= batch.SizeBytes;

                ScheduleWakeup(NextSendTimestamp);

                auto context = std::make_unique<grpc::ClientContext>();
                if (GrpcSigner) {
                    GrpcSigner->SignClientContext(*context);
                }
                auto reader = Stub->AsyncExport(context.get(), std::move(batch.Request), &CQ);
                auto uploadData =  std::unique_ptr<TExportRequestData>(new TExportRequestData {
                    .Context = std::move(context),
                    .Reader = std::move(reader),
                });
                uploadData->Reader->Finish(&uploadData->Response, &uploadData->Status, uploadData.get());
                ALOG_TRACE(WILSON_SERVICE_ID, "started export request " << (void*)uploadData.get());
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
                    ALOG_TRACE(WILSON_SERVICE_ID, "finished export request " << (void*)node.get());
                    if (!node->Status.ok()) {
                        ALOG_ERROR(WILSON_SERVICE_ID,
                            "failed to commit traces: " << node->Status.error_message());
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
                ALOG_TRACE(WILSON_SERVICE_ID, "wakeup received w/ cookie=" << cookie);
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

            STRICT_STFUNC(StateWork,
                hFunc(TEvWilson, Handle);
                hFunc(TEvents::TEvWakeup, HandleWakeup);
            );

            STRICT_STFUNC(StateBroken,
                IgnoreFunc(TEvWilson);
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
