#pragma once

#include "knn-defines.h"
#include "knn-enumerator.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename TTo, typename TFrom = TTo>
class TKnnVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&](IOutputStream& outStream) {
            EnumerateVector<TFrom>(x, [&](TFrom from) {
                TTo to = static_cast<TTo>(from);
                outStream.Write(&to, sizeof(TTo));
            });
            const auto format = Format<TTo>;
            outStream.Write(&format, HeaderLen);
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(x.GetListLength() * sizeof(TTo) + HeaderLen);
            auto strRef = str.AsStringRef();
            TMemoryOutput memoryOutput(strRef.Data(), strRef.Size());

            serialize(memoryOutput);
            return str;
        } else {
            TString str;
            TStringOutput stringOutput(str);

            serialize(stringOutput);
            return valueBuilder->NewString(str);
        }
    }

    static TUnboxedValue Deserialize(const IValueBuilder* valueBuilder, const TStringRef& str) {
        auto vector = GetArray(str);
        if (Y_UNLIKELY(vector.empty()))
            return {};

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(vector.size(), items);

        for (auto element : vector) {
            *items++ = TUnboxedValuePod{static_cast<TFrom>(element)};
        }

        return res.Release();
    }

    static TArrayRef<const TTo> GetArray(const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (Y_UNLIKELY(len % sizeof(TTo) != 0))
            return {};

        const ui32 count = len / sizeof(TTo);

        return {reinterpret_cast<const TTo*>(buf), count};
    }
};

// Encode all positive floats as bit 1, negative floats as bit 0.
// Bits serialized to ui64, from high to low bit and written in native endianess.
// The tail encoded as ui64 or ui32, ui16, ui8 respectively.
// After these we write count of not written bits in last byte that we need to respect.
// So length of vector in bits is 8 * ((count of written bytes) - 1 (format) - 1 (for x)) - x (count of bits not written in last byte).
template <typename TFrom>
class TKnnBitVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x](IOutputStream& outStream) {
            ui64 accumulator = 0;
            ui8 filledBits = 0;

            EnumerateVector<TFrom>(x, [&](TFrom element) {
                if (element > 0)
                    accumulator |= 1;

                if (++filledBits == 64) {
                    outStream.Write(&accumulator, sizeof(ui64));
                    accumulator = 0;
                    filledBits = 0;
                }
                accumulator <<= 1;
            });
            accumulator >>= 1;
            Y_ASSERT(filledBits < 64);
            filledBits += 7;

            auto write = [&](auto v) {
                outStream.Write(&v, sizeof(v));
            };
            auto tailWriteIf = [&]<typename T>() {
                if (filledBits < sizeof(T) * 8) {
                    return;
                }
                write(static_cast<T>(accumulator));
                if constexpr (sizeof(T) < sizeof(accumulator)) {
                    accumulator >>= sizeof(T) * 8;
                }
                filledBits -= sizeof(T) * 8;
            };
            tailWriteIf.template operator()<ui64>();
            tailWriteIf.template operator()<ui32>();
            tailWriteIf.template operator()<ui16>();
            tailWriteIf.template operator()<ui8>();

            Y_ASSERT(filledBits < 8);
            write(static_cast<ui8>(7 - filledBits));
            write(EFormat::BitVector);
        };

        if (x.HasFastListLength()) {
            // We expect byte lenght of the result is (bit-length / 8 + bit-length % 8 != 0) + bits-count (1 byte) + HeaderLen
            // First part can be optimized to (bit-length + 7) / 8
            auto str = valueBuilder->NewStringNotFilled((x.GetListLength() + 7) / 8 + 1 + HeaderLen);
            auto strRef = str.AsStringRef();
            TMemoryOutput memoryOutput(strRef.Data(), strRef.Size());

            serialize(memoryOutput);
            return str;
        } else {
            TString str;
            TStringOutput stringOutput(str);

            serialize(stringOutput);
            return valueBuilder->NewString(str);
        }
    }
};

class TKnnSerializerFacade {
public:
    static TUnboxedValue Deserialize(const IValueBuilder* valueBuilder, const TStringRef& str) {
        if (Y_UNLIKELY(str.Size() == 0))
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float>::Deserialize(valueBuilder, str);
            case EFormat::Int8Vector:
                return TKnnVectorSerializer<i8, float>::Deserialize(valueBuilder, str);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<ui8, float>::Deserialize(valueBuilder, str);
            case EFormat::BitVector:
            default:
                return {};
        }
    }

    struct TBitArray {
        const ui64* data = nullptr;
        ui64 bitLen = 0;
    };

    static TBitArray GetBitArray(const TStringRef& str) {
        if (Y_UNLIKELY(str.Size() < 2))
            return {};
        const char* buf = str.Data();
        const ui64 len = 8 * (str.Size() - HeaderLen - 1) - static_cast<ui8>(buf[str.Size() - HeaderLen - 1]);
        return {reinterpret_cast<const ui64*>(buf), len};
    }
};
