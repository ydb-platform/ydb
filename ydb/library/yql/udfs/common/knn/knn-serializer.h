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


class ISerializer {
public:
    virtual ~ISerializer() = default;

    virtual TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) const = 0;
    virtual TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, const TStringRef str) const = 0;

    const static size_t HeaderSize = sizeof(ui8);
};

class TDummySerializer : public ISerializer {
public:
    TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) const override {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(x);
        return {};
    }

    TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, const TStringRef str) const override {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(str);
        return {};
    }
};

class TFloatVectorSerializer : public ISerializer {
public:
    TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x) const override {
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

    TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, const TStringRef str) const override {
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
    static ISerializer& Get(ui8 formatByte) {
        switch (formatByte) {
            case EFormat::FloatVector:
                return FloatVectorSerializer;
            default:
                return DummySerializer;
        }
    }
private:
    inline static TFloatVectorSerializer FloatVectorSerializer;
    inline static TDummySerializer DummySerializer;
};

