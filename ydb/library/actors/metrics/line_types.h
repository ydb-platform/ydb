#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/hp_timer.h>

#include <cstring>
#include <optional>
#include <span>
#include <type_traits>

namespace NActors {

    struct TLabel {
        TString Name;
        TString Value;

        bool operator==(const TLabel& rhs) const noexcept;
    };

    struct TInMemoryMetricsConfig {
        ui64 MemoryBytes = 0;
        ui32 ChunkSizeBytes = 4096;
        ui32 MaxLines = 0;
        TVector<TLabel> CommonLabels;
        TVector<TString> AllowedMetricPrefixes;
    };

    struct TLineKey {
        TString Name;
        // Line identity is order-independent by labels.
        // Labels stored in TLineKey must be canonically sorted by (Name, Value).
        TVector<TLabel> Labels;

        bool operator==(const TLineKey& rhs) const noexcept;
    };

    struct TLineKeyHash {
        size_t operator()(const TLineKey& key) const noexcept;
    };

    struct TChunkView {
        ui32 ChunkId = 0;
        TInstant FirstTs;
        TInstant LastTs;
    };

    struct TInMemoryMetricsStats {
        ui64 MemoryUsedBytes = 0;
        ui64 CommittedBytes = 0;
        ui64 FreeChunks = 0;
        ui64 UsedChunks = 0;
        ui64 SealedChunks = 0;
        ui64 WritableChunks = 0;
        ui64 RetiringChunks = 0;
        ui64 Lines = 0;
        ui64 ClosedLines = 0;
        ui64 ReuseWatermark = 0;
        ui64 AppendFailuresTotal = 0;
    };

    namespace NInMemoryMetricsPrivate {
        template<class TValue>
        ui64 EncodeLineValue(const TValue& value) noexcept {
            static_assert(std::is_trivially_copyable_v<TValue>);
            static_assert(sizeof(TValue) <= sizeof(ui64));

            ui64 encoded = 0;
            std::memcpy(&encoded, &value, sizeof(TValue));
            return encoded;
        }

        template<class TValue>
        TValue DecodeLineValue(ui64 value) noexcept {
            static_assert(std::is_trivially_copyable_v<TValue>);
            static_assert(sizeof(TValue) <= sizeof(ui64));

            TValue decoded{};
            std::memcpy(&decoded, &value, sizeof(TValue));
            return decoded;
        }
    } // namespace NInMemoryMetricsPrivate

    TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> labels);
    TVector<TLabel> NormalizeCommonLabels(std::span<const TLabel> labels);

} // namespace NActors
