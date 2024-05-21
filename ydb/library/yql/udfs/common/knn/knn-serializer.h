#pragma once

#include "knn-defines.h"
#include "knn-enumerator.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

template <typename T, EFormat Format>
class TKnnVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x](IOutputStream& outStream) {
            EnumerateVector(x, [&outStream](float floatElement) {
                T element = static_cast<T>(floatElement);
                outStream.Write(&element, sizeof(T));
            });
            const EFormat format = Format;
            outStream.Write(&format, HeaderLen);
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(HeaderLen + x.GetListLength() * sizeof(T));
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
            *items++ = TUnboxedValuePod{static_cast<float>(element)};
        }

        return res.Release();
    }

    static const TArrayRef<const T> GetArray(const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (Y_UNLIKELY(len % sizeof(T) != 0))
            return {};

        const ui32 count = len / sizeof(T);

        return MakeArrayRef(reinterpret_cast<const T*>(buf), count);
    }
};

// Encode all positive floats as bit 1, negative floats as bit 0.
// Bits serialized to ui64, from high to low bit and written in native endianess.
// The tail encoded as ui64 or ui32, ui16, ui8 respectively.
// After these we write count of not written bits in last byte that we need to respect.
// So length of vector in bits is 8 * ((count of written bytes) - 1 (format) - 1 (for x)) - x (count of bits not written in last byte).
class TKnnBitVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x](IOutputStream& outStream) {
            ui64 accumulator = 0;
            ui8 filledBits = 0;

            EnumerateVector(x, [&](float element) {
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
            tailWriteIf.operator()<ui64>();
            tailWriteIf.operator()<ui32>();
            tailWriteIf.operator()<ui16>();
            tailWriteIf.operator()<ui8>();

            Y_ASSERT(filledBits < 8);
            write(static_cast<ui8>(7 - filledBits));
            write(EFormat::BitVector);

            return true;
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled((x.GetListLength() + 7) / 8 + 1 + HeaderLen);
            auto strRef = str.AsStringRef();
            TMemoryOutput memoryOutput(strRef.Data(), strRef.Size());

            if (Y_UNLIKELY(!serialize(memoryOutput)))
                return {};

            return str;
        } else {
            TString str;
            TStringOutput stringOutput(str);

            if (Y_UNLIKELY(!serialize(stringOutput)))
                return {};

            return valueBuilder->NewString(str);
        }
    }

    static std::pair<const ui64*, ui64> GetArray(TStringRef str) {
        if (Y_UNLIKELY(str.Size() < 2))
            return {nullptr, 0};
        const char* buf = str.Data();
        const ui64 len = 8 * (str.Size() - HeaderLen - 1) - static_cast<ui8>(buf[str.Size() - HeaderLen - 1]);
        return {reinterpret_cast<const ui64*>(buf), len};
    }
};

class TKnnSerializerFacade {
public:
    static TUnboxedValue Serialize(EFormat format, const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float, EFormat::FloatVector>::Serialize(valueBuilder, x);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<ui8, EFormat::Uint8Vector>::Serialize(valueBuilder, x);
            case EFormat::BitVector:
                return TKnnBitVectorSerializer::Serialize(valueBuilder, x);
            default:
                return {};
        }
    }

    static TUnboxedValue Deserialize(const IValueBuilder* valueBuilder, const TStringRef& str) {
        if (Y_UNLIKELY(str.Size() == 0))
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float, EFormat::FloatVector>::Deserialize(valueBuilder, str);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<ui8, EFormat::Uint8Vector>::Deserialize(valueBuilder, str);
            case EFormat::BitVector:
            default:
                return {};
        }
    }

    template <typename T>
    static const TArrayRef<const T> GetArray(const TStringRef& str) {
        if (Y_UNLIKELY(str.Size() == 0))
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<T, EFormat::FloatVector>::GetArray(str);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<T, EFormat::Uint8Vector>::GetArray(str);
            case EFormat::BitVector:
            default:
                return {};
        }
    }
};
