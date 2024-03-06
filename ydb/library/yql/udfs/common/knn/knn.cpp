#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>
#include <util/stream/format.h>

using namespace NYql;
using namespace NYql::NUdf;

enum EFormat : unsigned char {
    DoubleVector = 1
};

TString SerializeDoubleVector(const TUnboxedValuePod x) {
    const EFormat format = EFormat::DoubleVector;
    
    TString str;
    TStringOutput outStr(str);
    if (const auto elements = x.GetElements()) {
        const auto size = x.GetListLength();
        outStr.Reserve(1 + size * sizeof(double));
        outStr.Write(&format, sizeof(unsigned char));
        for (ui32 i = 0; i < size; ++i) {
            double element = elements[i].Get<double>();
            outStr.Write(&element, sizeof(double));
        }
    } else {
        outStr.Write(&format, sizeof(unsigned char));
        const auto it = x.GetListIterator();
        TUnboxedValue v;
        while(it.Next(v)) {
            double element = v.Get<double>();
            outStr.Write(&element, sizeof(double));
        }
    }
    return str;
}

NYql::NUdf::TUnboxedValue DeserializeDoubleVector(const IValueBuilder *valueBuilder, TStringRef str) {
    if (str.Size() % sizeof(double) != 0)    
        return TUnboxedValue(TUnboxedValuePod());
    
    const ui32 count = str.Size() / sizeof(double);

    TUnboxedValue* items = nullptr;
    auto res = valueBuilder->NewArray(count, items);
    
    for (ui32 i = 0; i < count; ++i) {
        double element;
        memcpy(&element, str.Data() + i * sizeof(double), sizeof(double));
        *items++ = TUnboxedValuePod{element};
    }

    return res.Release();
}

SIMPLE_STRICT_UDF(TToBinaryString, char*(TListType<double>)) {
    return valueBuilder->NewString(SerializeDoubleVector(args[0]));
}

SIMPLE_STRICT_UDF(TFromBinaryString, TOptional<TListType<double>>(const char*)) {
    TStringRef str = args[0].AsStringRef();
    if (str.Size() == 0)
        return TUnboxedValue(TUnboxedValuePod());

    const EFormat format = (EFormat)str.Data()[0];
    str = TStringRef{str.Data() + 1, str.Size() -1};
    switch (format) {
        case EFormat::DoubleVector: {
            return DeserializeDoubleVector(valueBuilder, str);
            break;
        }
        default:
            return TUnboxedValue(TUnboxedValuePod());
    }
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString
    )

REGISTER_MODULES(TKnnModule)

