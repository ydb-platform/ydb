#pragma once

#include <library/cpp/string_utils/base64/base64.h>

#include <util/stream/output.h>
#include <util/random/random.h>

#include <util/string/printf.h>

namespace NWilson {
    class TTraceId {
        ui64 TraceId; // Random id of topmost client request
        ui64 SpanId;  // Span id of part of request currently being executed

    private:
        TTraceId(ui64 traceId, ui64 spanId)
            : TraceId(traceId)
            , SpanId(spanId)
        { 
        } 

        static ui64 GenerateTraceId() {
            ui64 traceId = 0;
            while (!traceId) {
                traceId = RandomNumber<ui64>();
            }
            return traceId;
        }

        static ui64 GenerateSpanId() {
            return RandomNumber<ui64>();
        }

    public:
        using TSerializedTraceId = char[2 * sizeof(ui64)];

    public:
        TTraceId()
            : TraceId(0)
            , SpanId(0)
        { 
        } 

        explicit TTraceId(ui64 traceId)
            : TraceId(traceId)
            , SpanId(0)
        {
        }

        TTraceId(const TSerializedTraceId& in)
            : TraceId(reinterpret_cast<const ui64*>(in)[0]) 
            , SpanId(reinterpret_cast<const ui64*>(in)[1]) 
        { 
        } 

        // allow move semantic
        TTraceId(TTraceId&& other)
            : TraceId(other.TraceId)
            , SpanId(other.SpanId)
        {
            other.TraceId = 0;
            other.SpanId = 1; // explicitly mark invalid
        }

        TTraceId& operator=(TTraceId&& other) { 
            TraceId = other.TraceId;
            SpanId = other.SpanId;
            other.TraceId = 0;
            other.SpanId = 1; // explicitly mark invalid
            return *this;
        }

        // do not allow implicit copy of trace id
        TTraceId(const TTraceId& other) = delete;
        TTraceId& operator=(const TTraceId& other) = delete; 

        static TTraceId NewTraceId() {
            return TTraceId(GenerateTraceId(), 0);
        }

        // create separate branch from this point
        TTraceId SeparateBranch() const {
            return Clone();
        }

        TTraceId Clone() const {
            return TTraceId(TraceId, SpanId);
        }

        TTraceId Span() const {
            return *this ? TTraceId(TraceId, GenerateSpanId()) : TTraceId();
        }

        ui64 GetTraceId() const {
            return TraceId;
        }

        // Check if request tracing is enabled
        operator bool() const {
            return TraceId != 0;
        }

        // Output trace id into a string stream
        void Output(IOutputStream& s, const TTraceId& parentTraceId) const {
            union {
                ui8 buffer[3 * sizeof(ui64)];
                struct {
                    ui64 traceId;
                    ui64 spanId;
                    ui64 parentSpanId;
                } x;
            };

            x.traceId = TraceId;
            x.spanId = SpanId;
            x.parentSpanId = parentTraceId.SpanId;

            const size_t base64size = Base64EncodeBufSize(sizeof(x));
            char base64[base64size];
            char* end = Base64Encode(base64, buffer, sizeof(x)); 
            s << TStringBuf(base64, end);
        }

        // output just span id into stream
        void OutputSpanId(IOutputStream& s) const {
            const size_t base64size = Base64EncodeBufSize(sizeof(SpanId));
            char base64[base64size];
            char* end = Base64Encode(base64, reinterpret_cast<const ui8*>(&SpanId), sizeof(SpanId)); 

            // cut trailing padding character
            Y_VERIFY(end > base64 && end[-1] == '=');
            --end;

            s << TStringBuf(base64, end);
        }

        void CheckConsistency() {
            // if TraceId is zero, then SpanId must be zero too
            Y_VERIFY_DEBUG(*this || !SpanId);
        }

        friend bool operator==(const TTraceId& x, const TTraceId& y) { 
            return x.TraceId == y.TraceId && x.SpanId == y.SpanId;
        }

        TString ToString() const {
            return Sprintf("%" PRIu64 ":%" PRIu64, TraceId, SpanId);
        }

        bool IsFromSameTree(const TTraceId& other) const {
            return TraceId == other.TraceId;
        }

        void Serialize(TSerializedTraceId* out) { 
            ui64* p = reinterpret_cast<ui64*>(*out); 
            p[0] = TraceId;
            p[1] = SpanId;
        }
    };

}
