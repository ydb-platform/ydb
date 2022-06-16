#pragma once

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

    class TSpan {
        struct TData {
            const TInstant StartTime;
            const ui64 StartCycles;
            const TTraceId TraceId;
            NTraceProto::Span Span;

            TData(TInstant startTime, ui64 startCycles, TTraceId traceId)
                : StartTime(startTime)
                , StartCycles(startCycles)
                , TraceId(std::move(traceId))
            {}
        };

        std::unique_ptr<TData> Data;

    public:
        TSpan() = default;
        TSpan(const TSpan&) = delete;
        TSpan(TSpan&&) = default;

        TSpan(ui8 verbosity, ERelation /*relation*/, TTraceId parentId, TInstant now, std::optional<TString> name)
            : Data(parentId ? std::make_unique<TData>(now, GetCycleCount(), parentId.Span(verbosity)) : nullptr)
        {
            if (*this) {
                if (!parentId.IsRoot()) {
                    Data->Span.set_parent_span_id(parentId.GetSpanIdPtr(), parentId.GetSpanIdSize());
                }
                Data->Span.set_start_time_unix_nano(now.NanoSeconds());

                if (name) {
                    Name(std::move(*name));
                }
            }
        }

        TSpan& operator =(const TSpan&) = delete;
        TSpan& operator =(TSpan&&) = default;

        operator bool() const {
            return static_cast<bool>(Data);
        }

        TSpan& Name(TString name) {
            if (*this) {
                Data->Span.set_name(std::move(name));
            }
            return *this;
        }

        TSpan& Attribute(TString name, TAttributeValue value) {
            if (*this) {
                SerializeKeyValue(std::move(name), std::move(value), Data->Span.add_attributes());
            }
            return *this;
        }

        TSpan& Event(TString name, TKeyValueList attributes) {
            if (*this) {
                auto *event = Data->Span.add_events();
                event->set_time_unix_nano(TimeUnixNano());
                event->set_name(std::move(name));
                for (auto&& [key, value] : attributes) {
                    SerializeKeyValue(std::move(key), std::move(value), event->add_attributes());
                }
            }
            return *this;
        }

        TSpan& Link(const TTraceId& traceId, TKeyValueList attributes) {
            if (*this) {
                auto *link = Data->Span.add_links();
                link->set_trace_id(traceId.GetTraceIdPtr(), traceId.GetTraceIdSize());
                link->set_span_id(traceId.GetSpanIdPtr(), traceId.GetSpanIdSize());
                for (auto&& [key, value] : attributes) {
                    SerializeKeyValue(std::move(key), std::move(value), link->add_attributes());
                }
            }
            return *this;
        }

        void EndOk() {
            if (*this) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_OK);
            }
            End();
        }

        void EndError(TString error) {
            if (*this) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_ERROR);
                status->set_message(std::move(error));
            }
            End();
        }

        void End() {
            if (*this) {
                Data->Span.set_end_time_unix_nano(TimeUnixNano());
                Data->Span.set_trace_id(Data->TraceId.GetTraceIdPtr(), Data->TraceId.GetTraceIdSize());
                Data->Span.set_span_id(Data->TraceId.GetSpanIdPtr(), Data->TraceId.GetSpanIdSize());
                Send();
                Data.reset(); // tracing finished
            }
        }

        operator TTraceId() const {
            return Data ? TTraceId(Data->TraceId) : TTraceId();
        }

    private:
        void Send();

        ui64 TimeUnixNano() const {
            const TInstant now = Data->StartTime + CyclesToDuration(GetCycleCount() - Data->StartCycles);
            return now.NanoSeconds();
        }
    };

} // NWilson
