#pragma once

#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>

namespace NKikimr::NDDisk {

struct TSpaceMetrics {
    struct TData {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "data_bytes";
        static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };
    struct TChecksums {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "checksum_bytes";
        static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };
    struct TPersistentBuffer {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "pb_bytes";
        static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };
    struct TReserve {
        using TValueType = ui64;
        static constexpr TStringBuf Name = "reserve_bytes";
        static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };
    using TFields = std::tuple<TData, TChecksums, TPersistentBuffer, TReserve>;
};

// Byte gauges can increase and decrease. Keep exact values and quantize timestamps
// to 1 ms; sampling still happens once per second.
using TDDiskMetricStorage = NActors::TCompressedLineStorage<1'000, NActors::TIntegerEncoding<>>;
using TSpaceMetricsFrontend = NActors::TGroupLineFrontend<TSpaceMetrics, TDDiskMetricStorage>;
using TMemoryMetricsFrontend = NActors::TRawLineFrontend<ui64, TDDiskMetricStorage>;

// The five displayed operations share one sampling timestamp and clock.
struct TOperationMetrics {
    inline static constexpr std::array<TStringBuf, 11> Names = {
        "read_requests", "read_bytes", "write_requests", "write_bytes",
        "sync_requests", "sync_bytes", "direct_read_requests", "direct_read_bytes",
        "direct_write_requests", "direct_write_bytes", "monotonic_us"};
    template<size_t I>
    struct TField {
        using TValueType = ui64;
        static constexpr TStringBuf Name = Names[I];
        static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };
    using TFields = std::tuple<TField<0>, TField<1>, TField<2>, TField<3>,
        TField<4>, TField<5>, TField<6>, TField<7>, TField<8>, TField<9>, TField<10>>;
};
using TOperationMetricStorage = NActors::TCompressedLineStorage<1'000,
    NActors::TIntegerEncoding<NActors::ELineValueEncoding::UnsignedDelta>>;
using TOperationMetricsFrontend = NActors::TGroupLineFrontend<TOperationMetrics, TOperationMetricStorage>;

template<size_t... I>
auto OperationMetricValues(const std::array<ui64, 11>& values, std::index_sequence<I...>) {
    return TOperationMetricsFrontend::TValueType(
        TOperationMetricsFrontend::Value<TOperationMetrics::TField<I>>(values[I])...);
}

template<size_t... I>
std::array<ui64, 11> ReadOperationMetricValues(const TOperationMetricsFrontend::TValueType& value,
        std::index_sequence<I...>) {
    return {value.Get<TOperationMetrics::TField<I>>()...};
}

} // namespace NKikimr::NDDisk
