#pragma once

#include "knn-defines.h"

#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>

#include <functional>

namespace NKnnVectorSerialization {

    template <typename TTo>
    class TSerializer {
    private:
        IOutputStream* OutStream_ = nullptr;

    public:
        TSerializer() = delete;
        TSerializer(IOutputStream* outStream)
            : OutStream_(outStream)
        {
        }
        ~TSerializer() {
            if (OutStream_) {
                Finish();
            }
        }

        template <typename TFrom>
        void HandleElement(const TFrom& from) {
            Y_ENSURE(OutStream_);

            TTo to = static_cast<TTo>(from);
            OutStream_->Write(&to, sizeof(TTo));
        }

        void Finish() {
            Y_ENSURE(OutStream_);
            Y_DEFER {
                OutStream_ = nullptr;
            };

            const auto format = Format<TTo>;
            OutStream_->Write(&format, HeaderLen);
        }
    };

    template <typename TTo>
    class TDeserializer {
    private:
        const TStringBuf Data_;

    public:
        TDeserializer() = delete;
        TDeserializer(const TStringBuf data)
            : Data_(data)
        {
            Y_ENSURE(data.size() >= HeaderLen);
            Y_ENSURE((data.size() - HeaderLen) % sizeof(TTo) == 0);
            Y_ENSURE(data[data.size() - HeaderLen] == Format<TTo>);
        }

        size_t GetElementCount() const {
            return (Data_.size() - HeaderLen) / sizeof(TTo);
        }

        void DoDeserialize(std::function<void(const TTo&)>&& elementHandler) const {
            const TConstArrayRef<TTo> array(reinterpret_cast<const TTo*>(Data_.data()), GetElementCount());
            for (auto& element : array) {
                elementHandler(element);
            }
        }
    };

    // Encode all positive floats as bit 1, negative floats as bit 0.
    // Bits serialized to ui64, from high to low bit and written in native endianess.
    // The tail encoded as ui64 or ui32, ui16, ui8 respectively.
    // After these we write count of not written bits in last byte that we need to respect.
    // So length of vector in bits is 8 * ((count of written bytes) - 1 (format) - 1 (for x)) - x (count of bits not written in last byte).
    template <>
    class TSerializer<bool> {
    private:
        IOutputStream* OutStream_ = nullptr;
        ui64 Accumulator_ = 0;
        ui8 FilledBits_ = 0;

    public:
        TSerializer() = delete;
        TSerializer(IOutputStream* outStream)
            : OutStream_(outStream)
        {
        }
        ~TSerializer() {
            if (OutStream_ != nullptr) {
                Finish();
            }
        }

        template <typename TFrom>
        void HandleElement(const TFrom& from) {
            Y_ENSURE(OutStream_);

            if (from > 0) {
                Accumulator_ |= 1;
            }

            if (++FilledBits_ == 64) {
                OutStream_->Write(&Accumulator_, sizeof(ui64));
                Accumulator_ = 0;
                FilledBits_ = 0;
            }
            Accumulator_ <<= 1;
        }

        void Finish() {
            Y_ENSURE(OutStream_);
            Y_DEFER {
                OutStream_ = nullptr;
            };

            Accumulator_ >>= 1;
            Y_ASSERT(FilledBits_ < 64);
            FilledBits_ += 7;

            auto write = [&](auto v) {
                OutStream_->Write(&v, sizeof(v));
            };
            auto tailWriteIf = [&]<typename T>() {
                if (FilledBits_ < sizeof(T) * 8) {
                    return;
                }
                write(static_cast<T>(Accumulator_));
                if constexpr (sizeof(T) < sizeof(Accumulator_)) {
                    Accumulator_ >>= sizeof(T) * 8;
                }
                FilledBits_ -= sizeof(T) * 8;
            };
            tailWriteIf.template operator()<ui64>();
            tailWriteIf.template operator()<ui32>();
            tailWriteIf.template operator()<ui16>();
            tailWriteIf.template operator()<ui8>();

            Y_ASSERT(FilledBits_ < 8);
            write(static_cast<ui8>(7 - FilledBits_));
            write(EFormat::BitVector);
        }
    };

    template <typename TTo>
    inline size_t GetBufferSize(const size_t elementCount) {
        if constexpr (std::is_same_v<TTo, bool>) {
            // We expect byte length of the result is (bit-length / 8 + bit-length % 8 != 0) + bits-count (1 byte) + HeaderLen
            // First part can be optimized to (bit-length + 7) / 8
            return (elementCount + 7) / 8 + 1 + HeaderLen;
        } else {
            return elementCount * sizeof(TTo) + HeaderLen;
        }
    }

} // namespace NKnnVectorSerialization
