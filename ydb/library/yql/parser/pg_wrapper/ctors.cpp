#include "ctors.h"
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "varatt.h"
}

#undef Max
#include "utils.h"

namespace NYql {

namespace {

NUdf::TUnboxedValuePod ScalarValueToPodImpl(const bool value)   { return NUdf::TUnboxedValuePod(Datum(BoolGetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const i8 value)     { return NUdf::TUnboxedValuePod(Datum(Int8GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const i16 value)    { return NUdf::TUnboxedValuePod(Datum(Int16GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const i32 value)    { return NUdf::TUnboxedValuePod(Datum(Int32GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const i64 value)    { return NUdf::TUnboxedValuePod(Datum(Int64GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const ui8 value)    { return NUdf::TUnboxedValuePod(Datum(UInt8GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const ui16 value)   { return NUdf::TUnboxedValuePod(Datum(UInt16GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const ui32 value)   { return NUdf::TUnboxedValuePod(Datum(UInt32GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const ui64 value)   { return NUdf::TUnboxedValuePod(Datum(UInt64GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const float value)  { return NUdf::TUnboxedValuePod(Datum(Float4GetDatum(value))); }
NUdf::TUnboxedValuePod ScalarValueToPodImpl(const double value) { return NUdf::TUnboxedValuePod(Datum(Float8GetDatum(value))); }

NUdf::TUnboxedValuePod ScalarValueToPodImpl(const NUdf::TStringRef value) {
    return PointerDatumToPod(Datum(MakeVar(value)));
}

NUdf::TUnboxedValuePod ScalarValueToPodImpl(const std::string_view value) {
    return ScalarValueToPodImpl(NUdf::TStringRef(value));
}

}

template<typename ValueType>
NUdf::TUnboxedValuePod ScalarValueToPod(const ValueType value) {
    return ScalarValueToPodImpl(value);
}

template NUdf::TUnboxedValuePod ScalarValueToPod(const bool value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const i8 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const i16 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const i32 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const i64 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const ui8 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const ui16 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const ui32 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const ui64 value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const float value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const double value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const std::string_view value);
template NUdf::TUnboxedValuePod ScalarValueToPod(const NUdf::TStringRef value);

}
