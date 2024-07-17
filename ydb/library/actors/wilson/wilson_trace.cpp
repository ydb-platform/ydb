#include "wilson_trace.h"

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

        if (header.Size() != expectedHeaderSize) {
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
        HexDecode(header.Data(), versionChars, &version);
        if (version != 0) {
            return {};
        }

        TTrace traceId;
        ui64 spanId;
        static_assert(traceIdChars == 2 * sizeof(traceId));
        static_assert(parentSpanIdChars == 2 * sizeof(spanId));
        HexDecode(header.Data() + traceIdStart, traceIdChars, &traceId);
        HexDecode(header.Data() + parentSpanIdStart, parentSpanIdChars, &spanId);

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
}
