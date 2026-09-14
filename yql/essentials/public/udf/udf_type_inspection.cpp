#include "udf_type_inspection.h"

namespace NYql::NUdf {

TStubTypeVisitor::TStubTypeVisitor(ui16 compatibilityVersion)
    : TBase(compatibilityVersion)
{
}

TDataTypeInspector::TDataTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Data) {
        typeHelper.VisitType(type, this);
    }
}

TDataAndDecimalTypeInspector::TDataAndDecimalTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Data) {
        typeHelper.VisitType(type, this);
    }
}

TStructTypeInspector::TStructTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Struct) {
        typeHelper.VisitType(type, this);
    }
}

TListTypeInspector::TListTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::List) {
        typeHelper.VisitType(type, this);
    }
}

TOptionalTypeInspector::TOptionalTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Optional) {
        typeHelper.VisitType(type, this);
    }
}

TTupleTypeInspector::TTupleTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Tuple) {
        typeHelper.VisitType(type, this);
    }
}

TDictTypeInspector::TDictTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Dict) {
        typeHelper.VisitType(type, this);
    }
}

TCallableTypeInspector::TCallableTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Callable) {
        typeHelper.VisitType(type, this);
    }
}

TStreamTypeInspector::TStreamTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Stream) {
        typeHelper.VisitType(type, this);
    }
}

TVariantTypeInspector::TVariantTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Variant) {
        typeHelper.VisitType(type, this);
    }
}

TResourceTypeInspector::TResourceTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Resource) {
        typeHelper.VisitType(type, this);
    }
}

TTaggedTypeInspector::TTaggedTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Tagged) {
        typeHelper.VisitType(type, this);
    }
}

TPgTypeInspector::TPgTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Pg) {
        typeHelper.VisitType(type, this);
    }
}

TBlockTypeInspector::TBlockTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Block) {
        typeHelper.VisitType(type, this);
    }
}

TLinearTypeInspector::TLinearTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TStubTypeVisitor(UDF_ABI_COMPATIBILITY_VERSION_CURRENT)
{
    if (typeHelper.GetTypeKind(type) == ETypeKind::Linear) {
        typeHelper.VisitType(type, this);
    }
}

} // namespace NYql::NUdf
