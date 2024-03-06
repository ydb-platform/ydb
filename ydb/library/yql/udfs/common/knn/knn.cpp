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
        return TUnboxedValue();

    const EFormat format = (EFormat)str.Data()[0];
    str = TStringRef{str.Data() + 1, str.Size() -1};
    switch (format) {
        case EFormat::DoubleVector: {
            return DeserializeDoubleVector(valueBuilder, str);
            break;
        }
        default:
            return TUnboxedValue();
    }
}


std::optional<double> InnerProductDistance(const TUnboxedValuePod vector1, const TUnboxedValuePod vector2) {
    double ret = 0;

    if (vector1.HasFastListLength() && vector2.HasFastListLength()) {
        const auto elements1 = vector1.GetElements();
        const auto elements2 = vector2.GetElements();
        const auto size1 = vector1.GetListLength();
        const auto size2 = vector2.GetListLength();
        
        if (size1 != size2)
            return {};

        for (ui32 i = 0; i < size1; ++i) {
            double element1 = elements1[i].Get<double>();
            double element2 = elements2[i].Get<double>();
            ret += element1 * element2;
        }
    } else {
        TUnboxedValue value1, value2;
        const auto it1 = vector1.GetListIterator();
        const auto it2 = vector2.GetListIterator();    
        for (; it1.Next(value1) && it2.Next(value2);) {
            ret += value1.Get<double>() * value2.Get<double>();
        }

        // Lenght mismatch
        if (it1.Next(value1) || it2.Next(value2))
            return {};
    }

    return ret;
}

SIMPLE_STRICT_UDF(TInnerProductDistance, TOptional<double>(TOptional<TListType<double>>, TOptional<TListType<double>>)) {
    Y_UNUSED(valueBuilder);

    if (!args[0].HasValue() || !args[1].HasValue())
        return TUnboxedValue(TUnboxedValuePod());

    if (auto distance = InnerProductDistance(args[0], args[1]))
        return TUnboxedValuePod{distance.value()};
    else 
        return TUnboxedValuePod();
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString,
    TInnerProductDistance
    )

REGISTER_MODULES(TKnnModule)

