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

    virtual ui32 CalcSerializedSize(const TUnboxedValue x) const = 0;
    virtual TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x, TString& str) const = 0;
    virtual TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, TStringRef str) const = 0;
};

class TDummySerializer : public ISerializer {
public:
    ui32 CalcSerializedSize(const TUnboxedValue x) const override {
        Y_UNUSED(x);
        return 0;
    }

    TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x, TString& str) const override {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(x);
        Y_UNUSED(str);
        return {};
    }

    TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, TStringRef str) const override {
        Y_UNUSED(valueBuilder);
        Y_UNUSED(str);
        return {};
    }
};

class TFloatVectorSerializer : public ISerializer {
public:
    ui32 CalcSerializedSize(const TUnboxedValue x) const override {
        return x.HasFastListLength() ? x.GetListLength() * sizeof(float): 0;
    }

    TUnboxedValue Serialize(const IValueBuilder* valueBuilder, const TUnboxedValue x, TString& str) const override {
        TStringOutput outStr(str);
        EnumerateVector(x,  [&outStr] (float element) { outStr.Write(&element, sizeof(float)); });
        return valueBuilder->NewString(str);
    }

    TUnboxedValue Deserialize(const IValueBuilder *valueBuilder, TStringRef str) const override {
        if (str.Size() % sizeof(float) != 0)    
            return {};
        
        const ui32 count = str.Size() / sizeof(float);

        TUnboxedValue* items = nullptr;
        auto res = valueBuilder->NewArray(count, items);
        
        TMemoryInput inStr(str);
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

