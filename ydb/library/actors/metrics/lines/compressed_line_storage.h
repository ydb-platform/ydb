#pragma once

#include "../line_storage.h"
#include <ydb/library/actors/util/datetime.h>

#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <tuple>

namespace NActors {
    struct TUncompressedLineStorage {
        static constexpr bool Enabled = false;
    };

    enum class ELineValueEncoding {
        Absolute,
        UnsignedDelta,
        SignedDelta,
    };

    template<ELineValueEncoding Encoding = ELineValueEncoding::SignedDelta>
    struct TIntegerEncoding {
        static constexpr auto Mode = Encoding;

        template<class T>
        static bool Encode(const T& value, ui64* encoded) noexcept {
            static_assert(std::is_integral_v<T> && sizeof(T) <= sizeof(ui64));
            *encoded = static_cast<ui64>(value);
            return true;
        }

        template<class T>
        static T Decode(ui64 encoded) noexcept {
            return static_cast<T>(encoded);
        }
    };

    template<unsigned Places, ELineValueEncoding Encoding = ELineValueEncoding::SignedDelta>
    struct TDecimalEncoding {
        static_assert(Places <= 9);
        static constexpr auto Mode = Encoding;
        static constexpr ui64 Scale = [] {
            ui64 scale = 1;
            for (unsigned i = 0; i < Places; ++i) {
                scale *= 10;
            }
            return scale;
        }();

        template<class T>
        static bool Encode(const T& value, ui64* encoded) noexcept {
            static_assert(std::is_floating_point_v<T>);
            const long double scaled = std::round(static_cast<long double>(value) * Scale);
            // Upper bound is exclusive: INT64_MAX can round to 2^63 on some platforms.
            if (!std::isfinite(scaled) || scaled < -0x1p63L || scaled >= 0x1p63L) {
                return false;
            }
            *encoded = static_cast<ui64>(static_cast<i64>(scaled));
            return true;
        }

        template<class T>
        static T Decode(ui64 encoded) noexcept {
            return static_cast<T>(static_cast<i64>(encoded)) / Scale;
        }
    };

    // Step is in microseconds; zero preserves the exact cycle timestamp.
    // Each field has a compile-time codec; one codec applies to every field.
    template<ui64 TimestampStepUs = 0, class... TEncodings>
    struct TCompressedLineStorage {
        static constexpr bool Enabled = true;
        static_assert(sizeof...(TEncodings) > 0);
        static_assert(TimestampStepUs <= 1'000'000'000);
        using TCodecs = std::tuple<TEncodings...>;

        template<size_t I>
        using TCodec = std::tuple_element_t<sizeof...(TEncodings) == 1 ? 0 : I, TCodecs>;

        template<size_t I, class T>
        static bool Encode(const T& value, ui64* encoded) noexcept {
            return TCodec<I>::Encode(value, encoded);
        }

        template<size_t I, class T>
        static T Decode(ui64 encoded) noexcept {
            return TCodec<I>::template Decode<T>(encoded);
        }

        static ui64 TimestampStep() noexcept {
            if constexpr (TimestampStepUs == 0) {
                return 1;
            } else {
                static const ui64 step = std::max<ui64>(1, Us2Ts(TimestampStepUs));
                return step;
            }
        }

        // Tag is in the first byte's high bits. Sizes: 8/1/2/4 bytes,
        // with respectively 62/6/14/30 payload bits. Absolute values use
        // tag zero followed by eight bytes, preserving all 64 value bits.
        static size_t Pack(ui64 value, char* output, bool absolute = false) noexcept {
            const size_t size = absolute ? 8 : value < (ui64(1) << 6) ? 1
                : value < (ui64(1) << 14) ? 2 : value < (ui64(1) << 30) ? 4 : 8;
            const ui8 tag = size == 1 ? 1 : size == 2 ? 2 : size == 4 ? 3 : 0;
            output[0] = static_cast<char>((tag << 6) | (value & 63));
            value >>= 6;
            for (size_t i = 1; i < size; ++i) {
                output[i] = static_cast<char>(value & 255);
                value >>= 8;
            }
            return size;
        }

        static bool Unpack(const char** cursor, const char* end, ui64* value, ui8* tag) noexcept {
            if (*cursor == end) {
                return false;
            }
            const auto first = static_cast<ui8>(**cursor);
            *tag = first >> 6;
            const size_t size = *tag == 0 ? 8 : *tag == 1 ? 1 : *tag == 2 ? 2 : 4;
            if (size_t(end - *cursor) < size) {
                return false;
            }
            *value = first & 63;
            for (size_t i = 1; i < size; ++i) {
                *value |= ui64(static_cast<ui8>((*cursor)[i])) << (6 + 8 * (i - 1));
            }
            *cursor += size;
            return true;
        }

        template<size_t N>
        struct THeader {
            ui64 Version = 1;
            // Writer-only cache. Readers MUST NOT read these mutable members:
            // snapshots can hold an earlier committed prefix of this chunk.
            ui64 LastTimestamp = 0;
            std::array<ui64, N> LastValues = {};
        };

        template<size_t N>
        struct TRecord {
            NHPTimer::STime Timestamp;
            std::array<ui64, N> Values;
        };

        template<size_t I>
        static size_t PackValue(ui64 value, ui64 previous, bool first, char* output) noexcept {
            ui64 delta = 0;
            bool compact = !first;
            if constexpr (TCodec<I>::Mode == ELineValueEncoding::Absolute) {
                compact = false;
            } else if constexpr (TCodec<I>::Mode == ELineValueEncoding::UnsignedDelta) {
                compact &= value >= previous;
                delta = value - previous;
            } else {
                // Modular difference supports unsigned and signed values,
                // including crossings of signed zero. Large differences escape.
                const ui64 difference = value - previous;
                const ui64 magnitude = difference <= ui64(std::numeric_limits<i64>::max())
                    ? difference : ui64(0) - difference;
                compact &= magnitude < (ui64(1) << 29);
                delta = difference <= ui64(std::numeric_limits<i64>::max())
                    ? magnitude * 2 : magnitude * 2 - 1;
            }
            if (compact && delta < (ui64(1) << 30)) {
                return Pack(delta, output);
            }
            output[0] = 0;
            for (size_t i = 0; i < 8; ++i) {
                output[i + 1] = static_cast<char>(value >> (i * 8));
            }
            return 9;
        }

        template<size_t I>
        static bool UnpackValue(const char** cursor, const char* end, ui64 previous, ui64* value) noexcept {
            if (*cursor == end) {
                return false;
            }
            if (static_cast<ui8>(**cursor) >> 6 == 0) {
                if (**cursor != 0 || end - *cursor < 9) {
                    return false;
                }
                *value = 0;
                for (size_t i = 0; i < 8; ++i) {
                    *value |= ui64(static_cast<ui8>((*cursor)[i + 1])) << (8 * i);
                }
                *cursor += 9;
                return true;
            }
            ui64 delta;
            ui8 tag;
            if (!Unpack(cursor, end, &delta, &tag)) {
                return false;
            }
            if constexpr (TCodec<I>::Mode == ELineValueEncoding::SignedDelta) {
                *value = previous + ((delta & 1) ? ui64(0) - (delta / 2 + 1) : delta / 2);
            } else if constexpr (TCodec<I>::Mode == ELineValueEncoding::UnsignedDelta) {
                if (delta > std::numeric_limits<ui64>::max() - previous) {
                    return false;
                }
                *value = previous + delta;
            } else {
                return false;
            }
            return true;
        }

        template<size_t N, size_t... I>
        static bool Write(void* opaque, TWritableChunkMemory& memory, std::index_sequence<I...>) noexcept {
            static_assert(sizeof...(TEncodings) == 1 || sizeof...(TEncodings) == N);
            const auto& record = *static_cast<const TRecord<N>*>(opaque);
            if (record.Timestamp < 0 || ui64(record.Timestamp) >= (ui64(1) << 62)) {
                return false;
            }
            const bool first = memory.UsedPayloadBytes == 0;
            if (memory.Payload.size() < sizeof(THeader<N>)) {
                return false;
            }
            auto* header = reinterpret_cast<THeader<N>*>(memory.Payload.data());
            const ui64 step = TimestampStep();
            const ui64 timestamp = ui64(record.Timestamp) / step * step;
            const ui64 previous = first ? 0 : header->LastTimestamp;
            const ui64 delta = timestamp >= previous ? (timestamp - previous) / step : ui64(-1);
            const bool absolute = first || timestamp < previous || delta >= (ui64(1) << 30);
            std::array<char, 8 + 9 * N> encoded;
            size_t size = Pack(absolute ? timestamp : delta, encoded.data(), absolute);
            ((size += PackValue<I>(record.Values[I], first ? 0 : header->LastValues[I], first, encoded.data() + size)), ...);
            const size_t offset = first ? sizeof(THeader<N>) : memory.UsedPayloadBytes;
            if (offset + size > memory.Payload.size()) {
                return false;
            }
            if (first) {
                new (header) THeader<N>();
            }
            std::memcpy(memory.Payload.data() + offset, encoded.data(), size);
            header->LastTimestamp = timestamp;
            header->LastValues = record.Values;
            memory.UsedPayloadBytes = offset + size;
            // Keep actual source timestamps for retention and range boundaries.
            if (first) {
                memory.FirstTs = record.Timestamp;
            }
            memory.LastTs = record.Timestamp;
            return true;
        }

        template<size_t N>
        static bool WriteRecord(void* opaque, TWritableChunkMemory& memory) noexcept {
            return Write<N>(opaque, memory, std::make_index_sequence<N>{});
        }

        template<size_t N, class TCallback, size_t... I>
        static void ReadChunk(const TLineSnapshot& snapshot,
                              const NInMemoryMetricsPrivate::TChunkSnapshotView& chunk,
                              TCallback&& callback, std::index_sequence<I...>) {
            if (chunk.Payload.size() < sizeof(THeader<N>)) {
                return;
            }
            ui64 version;
            std::memcpy(&version, chunk.Payload.data(), sizeof(version));
            if (version != 1) {
                return;
            }
            const char* cursor = chunk.Payload.data() + sizeof(THeader<N>);
            const char* end = chunk.Payload.data() + chunk.Payload.size();
            ui64 timestamp = 0;
            std::array<ui64, N> values = {};
            bool first = true;
            while (cursor != end) {
                ui64 delta;
                ui8 tag;
                if (!Unpack(&cursor, end, &delta, &tag) || (first && tag != 0)) {
                    return;
                }
                if (tag == 0) {
                    timestamp = delta;
                } else {
                    const ui64 step = TimestampStep();
                    if (delta > (((ui64(1) << 62) - 1) - timestamp) / step) {
                        return;
                    }
                    timestamp += delta * step;
                }
                // An independent chunk must begin with absolute field values.
                bool valid = true;
                ((valid = valid && (!first || (cursor != end && *cursor == 0))
                    && UnpackValue<I>(&cursor, end, values[I], &values[I])), ...);
                if (!valid) {
                    return;
                }
                callback(NInMemoryMetricsPrivate::TLineSnapshotAccess::DecodeTimestampTs(snapshot, timestamp), values);
                first = false;
            }
        }

        template<size_t N, class TCallback>
        static void ForEachRecord(const TLineSnapshot& snapshot, TCallback&& callback) {
            NInMemoryMetricsPrivate::TLineSnapshotAccess::ForEachChunk(snapshot, [&](const auto& chunk) {
                ReadChunk<N>(snapshot, chunk, callback, std::make_index_sequence<N>{});
            });
        }
    };
} // namespace NActors
