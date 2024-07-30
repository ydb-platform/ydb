#pragma once

#include <ydb/library/actors/core/monotonic.h>

#include <util/random/random.h>
#include <util/random/fast.h>
#include <util/stream/output.h>

#include <array>

namespace NActorsProto {
    class TTraceId;
} // NActorsProto

namespace NWilson {
    class TTraceId {
        using TTrace = std::array<ui64, 2>;

        TTrace TraceId; // Random id of topmost client request
        ui64 SpanId;
        union {
            struct {
                ui32 Verbosity : 4;
                ui32 TimeToLive : 12;
            };
            ui32 Raw;
        };

    public:
        static constexpr ui8 MAX_VERBOSITY = 15;
        static constexpr ui32 MAX_TIME_TO_LIVE = 4095;

    private:
        TTraceId(TTrace traceId, ui64 spanId, ui8 verbosity, ui32 timeToLive)
            : TraceId(traceId)
        {
            if (timeToLive == Max<ui32>()) {
                timeToLive = MAX_TIME_TO_LIVE;
            }
            Y_ABORT_UNLESS(verbosity <= MAX_VERBOSITY);
            Y_ABORT_UNLESS(timeToLive <= MAX_TIME_TO_LIVE);
            SpanId = spanId;
            Verbosity = verbosity;
            TimeToLive = timeToLive;
        }

        static TTrace GenerateTraceId() {
            for (;;) {
                TTrace res;
                ui32 *p = reinterpret_cast<ui32*>(res.data());

                TReallyFastRng32 rng(RandomNumber<ui64>());
                p[0] = rng();
                p[1] = rng();
                p[2] = rng();
                p[3] = rng();

                if (res[0] || res[1]) {
                    return res;
                }
            }
        }

        static ui64 GenerateSpanId() {
            for (;;) {
                if (const ui64 res = RandomNumber<ui64>(); res) { // SpanId can't be zero
                    return res;
                }
            }
        }

    public:
        using TSerializedTraceId = char[sizeof(TTrace) + sizeof(ui64) + sizeof(ui32)];

    public:
        TTraceId(ui64) // NBS stub
            : TTraceId()
        {}

        TTraceId() {
            TraceId.fill(0);
            SpanId = 0;
            Raw = 0;
        }

        explicit TTraceId(TTrace traceId)
            : TraceId(traceId)
        {
            SpanId = 0;
            Raw = 0;
        }

        // allow move semantic
        TTraceId(TTraceId&& other)
            : TraceId(other.TraceId)
            , SpanId(other.SpanId)
            , Raw(other.Raw)
        {
            other.TraceId.fill(0);
            other.SpanId = 1; // make it explicitly invalid
            other.Raw = 0;
        }

        // explicit copy
        explicit TTraceId(const TTraceId& other)
            : TraceId(other.TraceId)
            , SpanId(other.SpanId)
            , Raw(other.Raw)
        {
            // validate trace id only when we are making a copy
            other.Validate();
        }

        TTraceId(const TSerializedTraceId& in) {
            const char *p = in;
            memcpy(TraceId.data(), p, sizeof(TraceId));
            p += sizeof(TraceId);
            memcpy(&SpanId, p, sizeof(SpanId));
            p += sizeof(SpanId);
            memcpy(&Raw, p, sizeof(Raw));
            p += sizeof(Raw);
            Y_DEBUG_ABORT_UNLESS(p - in == sizeof(TSerializedTraceId));
        }

        TTraceId(const NActorsProto::TTraceId& pb);

        void Serialize(TSerializedTraceId *out) const {
            char *p = *out;
            memcpy(p, TraceId.data(), sizeof(TraceId));
            p += sizeof(TraceId);
            memcpy(p, &SpanId, sizeof(SpanId));
            p += sizeof(SpanId);
            memcpy(p, &Raw, sizeof(Raw));
            p += sizeof(Raw);
            Y_DEBUG_ABORT_UNLESS(p - *out == sizeof(TSerializedTraceId));
        }

        void Serialize(NActorsProto::TTraceId *pb) const;

        TTraceId& operator=(TTraceId&& other) {
            if (this != &other) {
                TraceId = other.TraceId;
                SpanId = other.SpanId;
                Raw = other.Raw;
                other.TraceId.fill(0);
                other.SpanId = 1; // make it explicitly invalid
                other.Raw = 0;
            }
            return *this;
        }

        // do not allow implicit copy of trace id
        TTraceId& operator=(const TTraceId& other) = delete;

        static TTraceId NewTraceId(ui8 verbosity, ui32 timeToLive) {
            return TTraceId(GenerateTraceId(), 0, verbosity, timeToLive);
        }

        static TTraceId NewTraceIdThrottled(ui8 verbosity, ui32 timeToLive, std::atomic<NActors::TMonotonic>& counter,
                NActors::TMonotonic now, TDuration periodBetweenSamples) {
            static_assert(std::atomic<NActors::TMonotonic>::is_always_lock_free);
            for (;;) {
                NActors::TMonotonic ts = counter.load();
                if (now < ts) {
                    return {};
                } else if (counter.compare_exchange_strong(ts, now + periodBetweenSamples)) {
                    return NewTraceId(verbosity, timeToLive);
                }
            }
        }

        static TTraceId NewTraceId() { // NBS stub
            return TTraceId();
        }

        static TTraceId FromTraceparentHeader(const TStringBuf header, ui8 verbosity = 15);
        TString ToTraceresponseHeader() const;

        TTraceId Span(ui8 verbosity) const {
            Validate();
            if (!*this || !TimeToLive) {
                return TTraceId();
            } else if (verbosity <= Verbosity) {
                return TTraceId(TraceId, GenerateSpanId(), Verbosity, TimeToLive - 1);
            } else {
                return TTraceId(TraceId, SpanId, Verbosity, TimeToLive - 1);
            }
        }

        TTraceId Span() const { // compatibility stub
            return {};
        }

        // Check if request tracing is enabled
        explicit operator bool() const {
            return TraceId[0] || TraceId[1];
        }

        bool IsRoot() const {
            return !SpanId;
        }

        friend bool operator==(const TTraceId& x, const TTraceId& y) {
            return x.TraceId == y.TraceId && x.SpanId == y.SpanId && x.Raw == y.Raw;
        }

        ui8 GetVerbosity() const {
            return Verbosity;
        }

        ui32 GetTimeToLive() const {
            return TimeToLive;
        }

        const void *GetTraceIdPtr() const { return TraceId.data(); }
        static constexpr size_t GetTraceIdSize() { return sizeof(TTrace); }
        const void *GetSpanIdPtr() const { return &SpanId; }
        static constexpr size_t GetSpanIdSize() { return sizeof(ui64); }

        TString GetHexTraceId() const;

        void Validate() const {
            Y_DEBUG_ABORT_UNLESS(*this || !SpanId);
        }

        // for compatibility with NBS
        TTraceId Clone() const { return NWilson::TTraceId(*this); }
        ui64 GetTraceId() const { return 0; }
        void OutputSpanId(IOutputStream&) const {}
    };
}
