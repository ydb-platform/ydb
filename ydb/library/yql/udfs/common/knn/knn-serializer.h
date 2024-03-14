#pragma once

#include "knn-enumerator.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

enum EFormat : ui8 {
    FloatVector = 1
};


class TFloatVectorSerializer {
public:
    static TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) {
        auto serialize = [&x] (IOutputStream& outStream) {
            const EFormat format = EFormat::FloatVector;
            outStream.Write(&format, 1);
            EnumerateVector(x,  [&outStream] (float element) { outStream.Write(&element, sizeof(float)); });
        };

        if (x.HasFastListLength()) {
            auto str = valueBuilder->NewStringNotFilled(sizeof(ui8) + x.GetListLength() * sizeof(float));
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
        //skip format byte, it was already read
        const char* buf = str.Data() + 1;
        const size_t len = str.Size() - 1;

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

        ui8 formatByte = str.Data()[0];
        switch (formatByte) {
            case EFormat::FloatVector:
                return TFloatVectorSerializer::Deserialize(valueBuilder, str);
            default:
                return {};
        }
    }
};

