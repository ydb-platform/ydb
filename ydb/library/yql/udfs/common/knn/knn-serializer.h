#pragma once

#include "knn-enumerator.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <library/cpp/dot_product/dot_product.h>
#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

enum EFormat : ui8 {
    FloatVector = 1,        // 4-byte per element
    FloatByteVector = 2,    // 1-byte per element
    BitVector = 10          // 1-bit per element
};

static constexpr size_t HeaderLen = sizeof(ui8);

template<typename T, EFormat Format>
class TKnnVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x] (IOutputStream& outStream) {
            EnumerateVector(x,  [&outStream] (float floatElement) { 
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

    static TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (len % sizeof(T) != 0)    
            return {};
        
        const ui32 count = len / sizeof(T);

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(count, items);
        
        TMemoryInput inStr(buf, len);
        for (ui32 i = 0; i < count; ++i) {
            T element;
            if (inStr.Read(&element, sizeof(T)) != sizeof(T))
                return {};
            *items++ = TUnboxedValuePod{static_cast<float>(element)};
        }

        return res.Release();
    }

    static const TArrayRef<const T> GetArray(const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (len % sizeof(T) != 0)    
            return {};
        
        const ui32 count = len / sizeof(T);

        return MakeArrayRef(reinterpret_cast<const T*>(buf), count);
    }
};

// Encode all positive floats as bit 1, negative floats as bit 0.
// So 1024 float vector is serialized in 1024/8=128 bytes.
// Place all bits in ui64. So, only vector sizes divisible by 64 are supported.
// Max vector lenght is 32767.
class TKnnBitVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x] (IOutputStream& outStream) {
            ui64 accumulator = 0;
            ui8 filledBits = 0;
            ui64 lenght = 0;

            EnumerateVector(x,  [&] (float element) { 
                if (element > 0)
                    accumulator |= 1ll << filledBits;

                ++filledBits;
                if (filledBits == 64) {
                    outStream.Write(&accumulator, sizeof(ui64));
                    lenght++;
                    accumulator = 0;
                    filledBits = 0;
                }
            });

            // only vector sizes divisible by 64 are supported 
            if (filledBits)
                return false;
            
            // max vector lenght is 32767
            if (lenght > UINT16_MAX)
                return false;

            const EFormat format = EFormat::BitVector;
            outStream.Write(&format, HeaderLen);

            return true;
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(HeaderLen + x.GetListLength() / 8);
            auto strRef = str.AsStringRef();
            TMemoryOutput memoryOutput(strRef.Data(), strRef.Size());

            if (!serialize(memoryOutput))
                return {};
            
            return str;
        } else {
            TString str;
            TStringOutput stringOutput(str);

            if (!serialize(stringOutput))
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
            case EFormat::FloatByteVector:
                return TKnnVectorSerializer<ui8, EFormat::FloatByteVector>::Serialize(valueBuilder, x);
            case EFormat::BitVector:
                return TKnnBitVectorSerializer::Serialize(valueBuilder, x);
            default:
                return {};
        }
    }

    static TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, const TStringRef& str) {
        if (str.Size() == 0)
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<float, EFormat::FloatVector>::Deserialize(valueBuilder, str);
            case EFormat::FloatByteVector:
                return TKnnVectorSerializer<ui8, EFormat::FloatByteVector>::Deserialize(valueBuilder, str);
            case EFormat::BitVector:
                return {};                
            default:
                return {};
        }
    }

    static std::optional<float> DotProduct(const TStringRef& str1, const TStringRef& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

        if (Y_UNLIKELY(format1 != format2))
            return {};

        switch (format1) {
            case EFormat::FloatVector: {
                const TArrayRef<const float> vector1 = GetArray<float>(str1); 
                const TArrayRef<const float> vector2 = GetArray<float>(str2); 

                if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
                    return {};

                return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
            }
            case EFormat::FloatByteVector: {
                const TArrayRef<const ui8> vector1 = GetArray<ui8>(str1); 
                const TArrayRef<const ui8> vector2 = GetArray<ui8>(str2); 

                if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
                    return {};

                return ::DotProduct(vector1.data(), vector2.data(), vector1.size());
            }
            default:
                return {};
        }
    }

    static std::optional<TTriWayDotProduct<float>> TriWayDotProduct(const TStringRef& str1, const TStringRef& str2) {
        const ui8 format1 = str1.Data()[str1.Size() - HeaderLen];
        const ui8 format2 = str2.Data()[str2.Size() - HeaderLen];

        if (Y_UNLIKELY(format1 != format2))
            return {};

        switch (format1) {
            case EFormat::FloatVector: {
                const TArrayRef<const float> vector1 = GetArray<float>(str1); 
                const TArrayRef<const float> vector2 = GetArray<float>(str2); 

                if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
                    return {};

                return ::TriWayDotProduct(vector1.data(), vector2.data(), vector1.size());
            }
            case EFormat::FloatByteVector: {
                const TArrayRef<const ui8> vector1 = GetArray<ui8>(str1); 
                const TArrayRef<const ui8> vector2 = GetArray<ui8>(str2); 

                if (vector1.size() != vector2.size() || vector1.empty() || vector2.empty())
                    return {};

                TTriWayDotProduct<float> result;
                result.LL = ::DotProduct(vector1.data(), vector1.data(), vector1.size());
                result.LR = ::DotProduct(vector1.data(), vector2.data(), vector1.size());
                result.RR = ::DotProduct(vector2.data(), vector2.data(), vector1.size());
                return result;
            }
            default:
                return {};
        }
    }

private:
    template<typename T>
    static const TArrayRef<const T> GetArray(const TStringRef& str) {
        if (str.Size() == 0)
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TKnnVectorSerializer<T, EFormat::FloatVector>::GetArray(str);
            case EFormat::FloatByteVector:
                return TKnnVectorSerializer<T, EFormat::FloatByteVector>::GetArray(str);
            case EFormat::BitVector:
                return {};                
            default:
                return {};
        }
    }
};

