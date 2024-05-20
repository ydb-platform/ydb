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
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (Y_UNLIKELY(len % sizeof(T) != 0))
            return {};

        const ui32 count = len / sizeof(T);

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(count, items);

        TMemoryInput inStr(buf, len);
        for (ui32 i = 0; i < count; ++i) {
            T element;
            if (Y_UNLIKELY(inStr.Read(&element, sizeof(T)) != sizeof(T)))
                return {};
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
// So 1024 float vector is serialized in 1024/8=128 bytes.
// Place all bits in ui64. So, only vector sizes divisible by 64 are supported.
class TKnnBitVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x](IOutputStream& outStream) {
            ui64 accumulator = 0;
            ui8 filledBits = 0;

            EnumerateVector(x, [&](float element) {
                if (element > 0)
                    accumulator |= 1ll << filledBits;
                
                ++filledBits;
                if (filledBits == 64) {
                    outStream.Write(&accumulator, sizeof(ui64));
                    accumulator = 0;
                    filledBits = 0;
                }
            });

            // only vector sizes divisible by 64 are supported
            if (Y_UNLIKELY(filledBits))
                return false;

            const EFormat format = EFormat::BitVector;
            outStream.Write(&format, HeaderLen);

            return true;
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(HeaderLen + x.GetListLength() / 8);
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

    static const TArrayRef<const ui64> GetArray64(const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = (str.Size() - HeaderLen) / sizeof(ui64);

        return MakeArrayRef(reinterpret_cast<const ui64*>(buf), len);
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
        if (str.Size() == 0)
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float, EFormat::FloatVector>::Deserialize(valueBuilder, str);
            case EFormat::Uint8Vector:
                return TKnnVectorSerializer<ui8, EFormat::Uint8Vector>::Deserialize(valueBuilder, str);
            case EFormat::BitVector:
                return {};
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
                return {};
            default:
                return {};
        }
    }
};
