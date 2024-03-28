#pragma once

#include "knn-enumerator.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/array_ref.h>
#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

enum EFormat : ui8 {
    FloatVector = 1
};

static constexpr size_t HeaderLen = sizeof(ui8);

class TFloatVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x] (IOutputStream& outStream) {
            EnumerateVector(x,  [&outStream] (float element) { outStream.Write(&element, sizeof(float)); });
            const EFormat format = EFormat::FloatVector;
            outStream.Write(&format, HeaderLen);
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(HeaderLen + x.GetListLength() * sizeof(float));
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

        if (len % sizeof(float) != 0)    
            return {};
        
        const ui32 count = len / sizeof(float);

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(count, items);
        
        TMemoryInput inStr(buf, len);
        for (ui32 i = 0; i < count; ++i) {
            float element;
            if (inStr.Read(&element, sizeof(float)) != sizeof(float))
                return {};
            *items++ = TUnboxedValuePod{element};
        }

        return res.Release();
    }

    static const TArrayRef<const float> GetArray(const TStringRef& str) {
        const char* buf = str.Data();
        const size_t len = str.Size() - HeaderLen;

        if (len % sizeof(float) != 0)    
            return {};
        
        const ui32 count = len / sizeof(float);

        return MakeArrayRef(reinterpret_cast<const float*>(buf), count);
    }
};


class TSerializerFacade {
public:
    static TUnboxedValue Serialize(EFormat format, const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        switch (format) {
            case EFormat::FloatVector:
                return TFloatVectorSerializer::Serialize(valueBuilder, x);
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
                return TFloatVectorSerializer::Deserialize(valueBuilder, str);
            default:
                return {};
        }
    }

    static const TArrayRef<const float> GetArray(const TStringRef& str) {
        if (str.Size() == 0)
            return {};

        const ui8 format = str.Data()[str.Size() - HeaderLen];
        switch (format) {
            case EFormat::FloatVector:
                return TFloatVectorSerializer::GetArray(str);
            default:
                return {};
        }
    }
};

