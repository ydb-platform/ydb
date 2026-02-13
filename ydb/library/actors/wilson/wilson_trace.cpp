#include "wilson_trace.h"

#include <util/random/random.h>
#include <util/random/fast.h>
#include <util/generic/algorithm.h>
#include <util/string/hex.h>

#include <ydb/library/actors/protos/actors.pb.h>

namespace NWilson {
    TTraceId::TTraceId(const NActorsProto::TTraceId& pb)
        : TTraceId()
    {
        if (pb.HasData()) {
            const auto& data = pb.GetData();
            if (data.size() == sizeof(TSerializedTraceId)) {
                *this = *reinterpret_cast<const TSerializedTraceId*>(data.data());
            }
        }
    }

    void TTraceId::Serialize(NActorsProto::TTraceId *pb) const {
        if (*this) {
            TSerializedTraceId data;
            Serialize(&data);
            pb->SetData(reinterpret_cast<const char*>(&data), sizeof(data));
        }
    }

    TTraceId TTraceId::FromTraceparentHeader(const TStringBuf header, ui8 verbosity) {
        constexpr size_t versionChars = 2; // Only version 0 is supported
        constexpr size_t versionStart = 0;

        constexpr size_t traceIdChars = 32;
        constexpr size_t traceIdStart = versionStart + versionChars + 1;
        static_assert(traceIdChars == TTraceId::GetTraceIdSize() * 2);

        constexpr size_t parentSpanIdChars = 16;
        constexpr size_t parentSpanIdStart = traceIdStart + traceIdChars + 1;
        static_assert(parentSpanIdChars == TTraceId::GetSpanIdSize() * 2);

        constexpr size_t traceFlagsChars = 2;
        constexpr size_t traceFlagsStart = parentSpanIdStart + parentSpanIdChars + 1;

        constexpr size_t expectedHeaderSize =
            versionChars + traceIdChars + parentSpanIdChars + traceFlagsChars + 3;

        if (header.size() != expectedHeaderSize) {
            return {};
        }

        // Specification only allows lower case letters, but we want to allow upper case
        // letters for convenience
        // https://w3c.github.io/trace-context/#traceparent-header-field-values
        auto isHex = [](char c) {
            return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
        };

        if (!AllOf(header.substr(versionStart, versionChars), isHex) ||
                !AllOf(header.substr(traceIdStart, traceIdChars), isHex) ||
                !AllOf(header.substr(parentSpanIdStart, parentSpanIdChars), isHex) ||
                !AllOf(header.substr(traceFlagsStart, traceFlagsChars), isHex)) {
            return {};
        }

        if (header[traceIdStart - 1] != '-' || header[parentSpanIdStart - 1] != '-' || header[traceFlagsStart - 1] != '-') {
            return {};
        }

        ui8 version;
        HexDecode(header.data(), versionChars, &version);
        if (version != 0) {
            return {};
        }

        TTrace traceId;
        ui64 spanId;
        static_assert(traceIdChars == 2 * sizeof(traceId));
        static_assert(parentSpanIdChars == 2 * sizeof(spanId));
        HexDecode(header.data() + traceIdStart, traceIdChars, &traceId);
        HexDecode(header.data() + parentSpanIdStart, parentSpanIdChars, &spanId);

        if ((traceId[0] == 0 && traceId[1] == 0) || spanId == 0) {
            return {};
        }

        return TTraceId(traceId, spanId, verbosity, Max<ui32>());
    }

    TString TTraceId::ToTraceresponseHeader() const {
        if (!*this) {
            return {};
        }

        TString result;
        result.reserve(55); // 2 + 1 + 32 + 1 + 16 + 1 + 2 = 55

        result += "00-";
        result += GetHexTraceId();
        result += "-";
        result += HexEncode(GetSpanIdPtr(), GetSpanIdSize());
        result += "-00";

        std::for_each(result.begin(), result.vend(), [](char& c) { c = std::tolower(c); });

        return result;
    }

    TString TTraceId::GetHexTraceId() const {
        return HexEncode(GetTraceIdPtr(), GetTraceIdSize());
    }

    TString TTraceId::GetHexFullTraceId() const {
        return HexEncode(GetTraceIdPtr(), GetTraceIdSize()) + "." +
                HexEncode(GetSpanIdPtr(), GetSpanIdSize());
    }

    TTraceId::TTrace TTraceId::GenerateTraceId() {
        static thread_local TReallyFastRng32 rng(RandomNumber<ui64>());

        for (;;) {
            TTrace res;

            res[0] = (ui64)rng() | ((ui64)rng() << 32);
            res[1] = (ui64)rng() | ((ui64)rng() << 32);

            if (Y_LIKELY(res[0])) {
                return res;
            }
        }
    }

    ui64 TTraceId::GenerateSpanId() {
        for (;;) {
            if (const ui64 res = RandomNumber<ui64>(); res) { // SpanId can't be zero
                return res;
            }
        }
    }

    TTraceId TTraceId::NewTraceIdThrottled(ui8 verbosity, ui32 timeToLive, std::atomic<NActors::TMonotonic>& counter,
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

    bool TTraceId::IsRetroTrace() const {
        return *this && RetroTrace;
    }

    bool TTraceId::IsWilsonTrace() const {
        return *this && !RetroTrace;
    }

    bool TTraceId::IsSameTrace(const NWilson::TTraceId& other) const {
        return std::memcmp(GetTraceIdPtr(), other.GetTraceIdPtr(), GetTraceIdSize()) == 0;
    }

    TTraceId TTraceId::MakeRetroIfEmpty(ui8 verbosity, ui32 ttl) {
        if (*this) {
            return TTraceId(*this);
        } else {
            return NewTraceId(verbosity, ttl, true);
        }
    }
}
