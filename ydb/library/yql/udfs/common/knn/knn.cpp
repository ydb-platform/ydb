#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <util/generic/buffer.h>

using namespace NYql;
using namespace NYql::NUdf;

TString SerializeVector(const TUnboxedValuePod x) {

    TStringBuilder res;
    if (const auto elements = x.GetElements()) {
        const auto size = x.GetListLength();
        for (ui32 i = 0U; i < size; ++i) {
            res << "," << elements[i].Get<double>();
        }
    } else {
        const auto it = x.GetListIterator();
        TUnboxedValue v;
        while(it.Next(v)) {
            res << v.Get<double>();
        }
    }

    return res;
}
SIMPLE_STRICT_UDF(TToBinaryString, char*(TListType<double>)) {
    return valueBuilder->NewString(SerializeVector(args[0]));
}

SIMPLE_UDF(TFromBinaryString, TListType<double>(char*)) {
    size_t size = atoi(args[0].AsStringRef().Data());
    TUnboxedValue* items = nullptr;
    auto res = valueBuilder->NewArray(size, items);
    for (ui64 i = 0ULL; i < size; ++i) {
        *items++ = TUnboxedValuePod{double(i)};
    }
    return res.Release();
}

SIMPLE_MODULE(TKnnModule,
    TFromBinaryString, 
    TToBinaryString
    )

REGISTER_MODULES(TKnnModule)

