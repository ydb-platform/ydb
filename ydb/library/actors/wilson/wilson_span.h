#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>
#include <util/generic/hash.h>
#include <util/generic/overloaded.h>
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
            NActors::TActorSystem* ActorSystem;

            TData(TInstant startTime, ui64 startCycles, TTraceId traceId, TFlags flags, NActors::TActorSystem* actorSystem)
                : StartTime(startTime)
                , StartCycles(startCycles)
                , TraceId(std::move(traceId))
                , Flags(flags)
                , ActorSystem(actorSystem ? actorSystem : (NActors::TlsActivationContext ? NActors::TActivationContext::ActorSystem() : nullptr))
            {
                Y_DEBUG_ABORT_UNLESS(ActorSystem, "Attempting to create NWilson::TSpan outside of actor system without providing actorSystem pointer");
            }

            ~TData() {
                Y_DEBUG_ABORT_UNLESS(Sent || Ignored);
            }
        };

        std::unique_ptr<TData> Data;

    public:
        TSpan() = default;
        TSpan(const TSpan&) = delete;
        TSpan(TSpan&&) = default;

        TSpan(ui8 verbosity, TTraceId parentId, std::variant<std::optional<TString>, const char*> name,
                TFlags flags = EFlags::NONE, NActors::TActorSystem* actorSystem = nullptr)
            : Data(parentId
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
        TSpan& operator=(TSpan&& other);

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

        template<typename T>
        TSpan& Name(T&& name) {
            if (Y_UNLIKELY(*this)) {
                Data->Span.set_name(std::forward<T>(name));
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        template<typename T, typename T1>
        TSpan& Attribute(T&& name, T1&& value) {
            if (Y_UNLIKELY(*this)) {
                SerializeKeyValue(std::forward<T>(name), std::forward<T1>(value), Data->Span.add_attributes());
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        template<typename T, typename T1 = std::initializer_list<std::pair<const char*, TAttributeValue>>>
        TSpan& Event(T&& name, T1&& attributes) {
            if (Y_UNLIKELY(*this)) {
                auto *event = Data->Span.add_events();
                event->set_time_unix_nano(TimeUnixNano());
                event->set_name(std::forward<T>(name));
                for (auto&& [key, value] : attributes) {
                    SerializeKeyValue(std::move(key), std::move(value), event->add_attributes());
                }
            } else {
                VerifyNotSent();
            }
            return *this;
        }

        template<typename T>
        TSpan& Event(T&& name) {
            return Event(std::forward<T>(name), {});
        }

        template<typename T = std::initializer_list<std::pair<TString, TAttributeValue>>>
        TSpan& Link(const TTraceId& traceId, T&& attributes) {
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

        TSpan& Link(const TTraceId& traceId);

        void EndOk() {
            if (Y_UNLIKELY(*this)) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_OK);
                End();
            } else {
                VerifyNotSent();
            }
        }

        template<typename T>
        void EndError(T&& error) {
            if (Y_UNLIKELY(*this)) {
                auto *status = Data->Span.mutable_status();
                status->set_code(NTraceProto::Status::STATUS_CODE_ERROR);
                status->set_message(std::forward<T>(error));
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

        NActors::TActorSystem* GetActorSystem() const {
            return Data ? Data->ActorSystem : nullptr;
        }

        TSpan CreateChild(ui8 verbosity, std::variant<std::optional<TString>, const char*> name, TFlags flags = EFlags::NONE) const {
            return TSpan(verbosity, GetTraceId(), std::move(name), flags, GetActorSystem());
        }

        TString GetName() const {
            return *this ? Data->Span.name() : TString();
        }

        static const TSpan Empty;

    private:
        void Send();

        ui64 TimeUnixNano() const {
            const TInstant now = Data->StartTime + CyclesToDuration(GetCycleCount() - Data->StartCycles);
            return now.NanoSeconds();
        }

        void VerifyNotSent() {
            Y_DEBUG_ABORT_UNLESS(!Data || !Data->Sent, "span has been ended");
        }
    };

} // NWilson
