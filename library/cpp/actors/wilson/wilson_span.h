#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/wilson/protos/trace.pb.h>
#include <util/generic/hash.h>
#include <util/datetime/cputimer.h>

#include "wilson_trace.h"

namespace NWilson {

    enum class ERelation {
        FollowsFrom,
        ChildOf,
    };

    namespace NTraceProto = opentelemetry::proto::trace::v1;
    namespace NCommonProto = opentelemetry::proto::common::v1;

    struct TArrayValue;
    struct TKeyValueList;
    struct TBytes;

    using TAttributeValue = std::variant<
        TString,
        bool,
        i64,
        double,
        TArrayValue,
        TKeyValueList,
        TBytes
    >;

    struct TArrayValue : std::vector<TAttributeValue> {};
    struct TKeyValueList : THashMap<TString, TAttributeValue> {};
    struct TBytes : TString {};

    void SerializeKeyValue(TString key, TAttributeValue value, NCommonProto::KeyValue *pb);

    enum class EFlags : ui32 {
        NONE = 0,
        AUTO_END = 1,
    };

    Y_DECLARE_FLAGS(TFlags, EFlags);
    Y_DECLARE_OPERATORS_FOR_FLAGS(TFlags);

    class TSpan {
        struct TData {
            const TInstant StartTime;
            const ui64 StartCycles;
            const TTraceId TraceId;
            NTraceProto::Span Span;
            TFlags Flags;
            int UncaughtExceptions = std::uncaught_exceptions();
            bool Sent = false;
            bool Ignored = false;

            TData(TInstant startTime, ui64 startCycles, TTraceId traceId, TFlags flags)
                : StartTime(startTime)
                , StartCycles(startCycles)
                , TraceId(std::move(traceId))
                , Flags(flags)
            {}

            ~TData() {
                Y_VERIFY_DEBUG(Sent || Ignored);
            }
        };

        std::unique_ptr<TData> Data;

    public:
        TSpan() = default;
        TSpan(const TSpan&) = delete;
        TSpan(TSpan&&) = default;

        TSpan(ui8 verbosity, TTraceId parentId, std::optional<TString> name, TFlags flags = EFlags::NONE)
            : Data(parentId
                    ? std::make_unique<TData>(TInstant::Now(), GetCycleCount(), parentId.Span(verbosity), flags)
                    : nullptr)
        {
            if (Y_UNLIKELY(*this)) {
                if (verbosity <= parentId.GetVerbosity()) {
                    if (!parentId.IsRoot()) {
                        Data->Span.set_parent_span_id(parentId.GetSpanIdPtr(), parentId.GetSpanIdSize());
                    }
                    Data->Span.set_start_time_unix_nano(Data->StartTime.NanoSeconds());
                    Data->Span.set_kind(opentelemetry::proto::trace::v1::Span::SPAN_KIND_INTERNAL);

                    if (name) {
                        Name(std::move(*name));
                    }

                    Attribute("node_id", NActors::TActivationContext::ActorSystem()->NodeId);
                } else {
                    Data->Ignored = true; // ignore this span due to verbosity mismatch, still allowing child spans to be created
                }
            }
        }

        ~TSpan() {
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

        TSpan& operator =(const TSpan&) = delete;

        TSpan& operator =(TSpan&& other) {
            if (this != &other) {
                if (Y_UNLIKELY(*this)) {
                    EndError("TSpan instance incorrectly overwritten");
                }
                Data = std::exchange(other.Data, nullptr);
            }
            return *this;
        }

        explicit operator bool() const {
            return Data && !Data->Sent && !Data->Ignored;
        }

        TSpan& EnableAutoEnd() {
            if (Y_UNLIKELY(*this)) {
                Data->Flags |= EFlags::AUTO_END;
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        TSpan& Relation(ERelation /*relation*/) {
            if (Y_UNLIKELY(*this)) {
                // update relation in data somehow
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        TSpan& Name(TString name) {
            if (Y_UNLIKELY(*this)) {
                Data->Span.set_name(std::move(name));
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        TSpan& Attribute(TString name, TAttributeValue value) {
            if (Y_UNLIKELY(*this)) {
                SerializeKeyValue(std::move(name), std::move(value), Data->Span.add_attributes());
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        TSpan& Event(TString name, TKeyValueList attributes) {
            if (Y_UNLIKELY(*this)) {
                auto *event = Data->Span.add_events();
                event->set_time_unix_nano(TimeUnixNano());
                event->set_name(std::move(name));
                for (auto&& [key, value] : attributes) {
                    SerializeKeyValue(std::move(key), std::move(value), event->add_attributes());
                }
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        TSpan& Link(const TTraceId& traceId, TKeyValueList attributes) {
            if (Y_UNLIKELY(*this)) {
                auto *link = Data->Span.add_links();
                link->set_trace_id(traceId.GetTraceIdPtr(), traceId.GetTraceIdSize());
                link->set_span_id(traceId.GetSpanIdPtr(), traceId.GetSpanIdSize());
                for (auto&& [key, value] : attributes) {
                    SerializeKeyValue(std::move(key), std::move(value), link->add_attributes());
                }
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        void EndOk() {
            if (Y_UNLIKELY(*this)) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_OK);
                End();
            } else {
                VerifyNotSent();
            }
        }

        void EndError(TString error) {
            if (Y_UNLIKELY(*this)) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_ERROR);
                status->set_message(std::move(error));
                End();
            } else {
                VerifyNotSent();
            }
        }

        void End() {
            if (Y_UNLIKELY(*this)) {
                Data->Span.set_trace_id(Data->TraceId.GetTraceIdPtr(), Data->TraceId.GetTraceIdSize());
                Data->Span.set_span_id(Data->TraceId.GetSpanIdPtr(), Data->TraceId.GetSpanIdSize());
                Data->Span.set_end_time_unix_nano(TimeUnixNano());
                Send();
            } else {
                VerifyNotSent();
            }
        }

        TTraceId GetTraceId() const {
            return Data ? TTraceId(Data->TraceId) : TTraceId();
        }

    private:
        void Send();

        ui64 TimeUnixNano() const {
            const TInstant now = Data->StartTime + CyclesToDuration(GetCycleCount() - Data->StartCycles);
            return now.NanoSeconds();
        }

        void VerifyNotSent() {
            Y_VERIFY_DEBUG(!Data || !Data->Sent, "span has been ended");
        }
    };

} // NWilson
