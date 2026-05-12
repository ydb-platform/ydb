#pragma once

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

namespace NKikimr::NStat::NAggFuncs {

using TTypeId = NYql::NUdf::TDataTypeId;
using TValue = NYql::NUdf::TUnboxedValuePod;

template<class TVisitor>
void VisitValue(TTypeId typeId, const TValue& val, TVisitor&& visitor) {
    switch (typeId) {
#define MAKE_PRIMITIVE_VISITOR(type, layout)                                  \
    case NUdf::TDataType<type>::Id:                                           \
        visitor.VisitPrimitive(typeId, val.Get<layout>());                    \
        break;

    KNOWN_FIXED_VALUE_TYPES(MAKE_PRIMITIVE_VISITOR)
#undef MAKE_PRIMITIVE_VISITOR

    case NUdf::TDataType<NUdf::TDecimal>::Id:
        visitor.VisitDecimal(val.GetInt128());
        break;
    default: {
        auto str = val.AsStringRef();
        visitor.VisitString(typeId, str.Data(), str.Size());
        break;
    }
    }
    // TODO: Pg types
    // TODO: UUID
    // TODO: JsonDocument?
}

template<typename TVisitor>
requires std::invocable<TVisitor&, const char*, size_t>
struct RawDataVisitor {
    TVisitor Visitor;

    explicit RawDataVisitor(TVisitor visitor) : Visitor(std::move(visitor)) {}

    template<typename TLayout>
    void VisitPrimitive(TTypeId, TLayout val) {
        Visitor(reinterpret_cast<const char*>(&val), sizeof(val));
    }

    void VisitString(TTypeId, const char* data, size_t size) {
        Visitor(data, size);
    }

    void VisitDecimal(const NYql::NDecimal::TInt128& val) {
        Visitor(reinterpret_cast<const char*>(&val), sizeof(val));
    }
};

} // NKikimr::NStat::NAggFuncs
