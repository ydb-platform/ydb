#include "wilson_span.h"
#include "wilson_uploader.h"
#include <util/system/backtrace.h>
#include <util/generic/overloaded.h>
#include <google/protobuf/text_format.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NWilson {

    using namespace NActors;

    void SerializeValue(TAttributeValue value, NCommonProto::AnyValue *pb) {
        switch (value.index()) {
            case 0:
                pb->set_string_value(std::get<0>(std::move(value)));
                break;

            case 1:
                pb->set_bool_value(std::get<1>(value));
                break;

            case 2:
                pb->set_int_value(std::get<2>(value));
                break;

            case 3:
                pb->set_double_value(std::get<3>(std::move(value)));
                break;

            case 4: {
                auto *array = pb->mutable_array_value();
                for (auto&& item : std::get<4>(std::move(value))) {
                    SerializeValue(std::move(item), array->add_values());
                }
                break;
            }

            case 5: {
                auto *kv = pb->mutable_kvlist_value();
                for (auto&& [key, value] : std::get<5>(std::move(value))) {
                    SerializeKeyValue(std::move(key), std::move(value), kv->add_values());
                }
                break;
            }

            case 6:
                pb->set_bytes_value(std::get<6>(std::move(value)));
                break;
        }
    }

    void SerializeKeyValue(TString key, TAttributeValue value, NCommonProto::KeyValue *pb) {
        pb->set_key(std::move(key));
        SerializeValue(std::move(value), pb->mutable_value());
    }

    TSpan::TSpan(ui8 verbosity, TTraceId parentId, std::variant<std::optional<TString>, const char*> name,
            TFlags flags, NActors::TActorSystem* actorSystem)
        : Data(parentId && parentId.IsWilsonTrace()
                ? std::make_unique<TData>(TInstant::Now(), GetCycleCount(), parentId.Span(verbosity), flags, actorSystem)
                : nullptr)
    {
        if (Y_UNLIKELY(*this)) {
            if (verbosity <= parentId.GetVerbosity()) {
                if (!parentId.IsRoot()) {
                    Data->Span.set_parent_span_id(parentId.GetSpanIdPtr(), parentId.GetSpanIdSize());
                }
                Data->Span.set_start_time_unix_nano(Data->StartTime.NanoSeconds());
                Data->Span.set_kind(opentelemetry::proto::trace::v1::Span::SPAN_KIND_INTERNAL);

                std::visit(TOverloaded{
                    [&](const char *name) {
                        Name(TString(name));
                    },
                    [&](std::optional<TString>& name) {
                        if (name) {
                            Name(std::move(*name));
                        }
                    }
                }, name);

                Attribute("node_id", Data->ActorSystem->NodeId);
            } else {
                Data->Ignored = true; // ignore this span due to verbosity mismatch, still allowing child spans to be created
            }
        }
    }

    TSpan::~TSpan() {
        if (Y_UNLIKELY(*this)) {
            if (std::uncaught_exceptions() != Data->UncaughtExceptions) {
                EndError("span terminated due to stack unwinding");
            } else if (Data->Flags & EFlags::AUTO_END) {
                End();
            } else {
                EndError("unterminated span");
            }
        }
    }

    TSpan& TSpan::Link(const TTraceId& traceId) {
        return Link(traceId, {});
    }

    void TSpan::Send() {
        if (Data->ActorSystem) {
            Data->ActorSystem->Send(new IEventHandle(MakeWilsonUploaderId(), {}, new TEvWilson(&Data->Span)));
        }
        Data->Sent = true;
    }

    TSpan& TSpan::operator=(TSpan&& other) {
        if (this != &other) {
            if (Y_UNLIKELY(*this)) {
                TStringStream err;
                err << "TSpan instance incorrectly overwritten at:\n";
                FormatBackTrace(&err);
                EndError(std::move(err.Str()));
            }
            Data = std::exchange(other.Data, nullptr);
        }
        return *this;
    }

    void TSpan::End() {
        if (Y_UNLIKELY(*this)) {
            if (!Data->EndAsIs) {
                Data->Span.set_trace_id(Data->TraceId.GetTraceIdPtr(), Data->TraceId.GetTraceIdSize());
                Data->Span.set_span_id(Data->TraceId.GetSpanIdPtr(), Data->TraceId.GetSpanIdSize());
                Data->Span.set_end_time_unix_nano(TimeUnixNano());
            }
            Send();
        } else {
            VerifyNotSent();
        }
    }

    const TSpan TSpan::Empty;

    TSpan::TData::TData(TInstant startTime, ui64 startCycles, TTraceId traceId, TFlags flags, NActors::TActorSystem* actorSystem)
        : StartTime(startTime)
        , StartCycles(startCycles)
        , TraceId(std::move(traceId))
        , Flags(flags)
        , ActorSystem(actorSystem ? actorSystem : (NActors::TlsActivationContext ? NActors::TActivationContext::ActorSystem() : nullptr))
    {
        Y_DEBUG_ABORT_UNLESS(ActorSystem, "Attempting to create NWilson::TSpan outside of actor system without providing actorSystem pointer");
    }

    TSpan TSpan::ConstructTerminated(const NWilson::TTraceId& parentId, const NWilson::TTraceId& spanId,
            TInstant startTs, TInstant endTs, NTraceProto::Status::StatusCode statusCode, const TString& name) {
        TSpan res;
        res.Data = std::make_unique<TData>(TInstant::Zero(), 0, NWilson::TTraceId(spanId), EFlags::NONE,
                nullptr);
        res.Data->Span.set_trace_id(parentId.GetTraceIdPtr(), parentId.GetTraceIdSize());
        res.Data->Span.set_parent_span_id(parentId.GetSpanIdPtr(), parentId.GetSpanIdSize());
        res.Data->Span.set_span_id(spanId.GetSpanIdPtr(), spanId.GetSpanIdSize());
        res.Data->Span.set_start_time_unix_nano(startTs.NanoSeconds());
        res.Data->Span.set_end_time_unix_nano(endTs.NanoSeconds());
        res.Data->Span.mutable_status()->set_code(statusCode);
        res.Name(name);
        res.Data->EndAsIs = true;
        return res;
    }

} // NWilson
