#include "udf_type_printer.h"

namespace NYql {
namespace NUdf {

namespace {
static thread_local IOutputStream * Output_ = nullptr;
}

TTypePrinter1::TTypePrinter1(const ITypeInfoHelper& typeHelper, const TType* type)
    : TypeHelper_(typeHelper), Type_(type)
{}

void TTypePrinter1::Out(IOutputStream &o) const {
    Output_ = &o;
    OutImpl(Type_);
    Output_ = nullptr;
}

void TTypePrinter1::OutImpl(const TType* type) const {
    switch (TypeHelper_.GetTypeKind(type)) {
        case ETypeKind::Null: *Output_ << "Null"; break;
        case ETypeKind::Void: *Output_ << "Void"; break;
        case ETypeKind::EmptyList: *Output_ << "EmptyList"; break;
        case ETypeKind::EmptyDict: *Output_ << "EmptyDict"; break;
        case ETypeKind::Unknown: *Output_ << "Unknown"; break;
        default: TypeHelper_.VisitType(type, const_cast<TTypePrinter1*>(this));
    }
}

void TTypePrinter1::OnDataType(TDataTypeId typeId) {
    *Output_ << GetDataTypeInfo(GetDataSlot(typeId)).Name;
}

void TTypePrinter1::OnStruct(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes) {
    *Output_ << "Struct<";
    for (ui32 i = 0U; i < membersCount; ++i) {
        *Output_ << "'" << std::string_view(membersNames[i]) << "':";
        OutImpl(membersTypes[i]);
        if (i < membersCount - 1U)
            *Output_ << ',';

    }
    *Output_ << '>';
}

void TTypePrinter1::OnList(const TType* itemType) {
    *Output_ << "List<";
    OutImpl(itemType);
    *Output_ << '>';
}

void TTypePrinter1::OnOptional(const TType* itemType) {
    OutImpl(itemType);
    *Output_ << '?';
}

void TTypePrinter1::OnTuple(ui32 elementsCount, const TType** elementsTypes) {
    *Output_ << "Tuple<";
    for (ui32 i = 0U; i < elementsCount; ++i) {
        OutImpl(elementsTypes[i]);
        if (i < elementsCount - 1U)
            *Output_ << ',';

    }
    *Output_ << '>';
}

void TTypePrinter1::OnDict(const TType* keyType, const TType* valueType) {
    *Output_ << "Dict<";
    OutImpl(keyType);
    *Output_ << ',';
    OutImpl(valueType);
    *Output_ << '>';
}

void TTypePrinter1::OnCallable(const TType* returnType, ui32 argsCount, const TType** argsTypes, ui32 optionalArgsCount, const ICallablePayload* payload) {
    *Output_ << "Callable<(";
    for (ui32 i = 0U; i < argsCount; ++i) {
        if (optionalArgsCount && i == argsCount -  optionalArgsCount)
            *Output_ << '[';
        if (const std::string_view name = payload->GetArgumentName(i); !name.empty())
            *Output_ << "'" << name << "':";
        OutImpl(argsTypes[i]);
        if (ICallablePayload::TArgumentFlags::AutoMap == payload->GetArgumentFlags(i))
            *Output_ << "{Flags:AutoMap}";
        if (i < argsCount - 1U)
            *Output_ << ',';
    }
    *Output_ << (optionalArgsCount ? "])->" : ")->");
    OutImpl(returnType);
}

void TTypePrinter1::OnVariant(const TType* underlyingType) {
    *Output_ << "Variant<";
    OutImpl(underlyingType);
    *Output_ << '>';
}

void TTypePrinter1::OnStream(const TType* itemType) {
    *Output_ << "Stream<";
    OutImpl(itemType);
    *Output_ << '>';
}

void TTypePrinter1::OnDecimalImpl(ui8 precision, ui8 scale) {
    *Output_ << '(' << unsigned(precision) << ',' << unsigned(scale) << ')';
}

void TTypePrinter1::OnResourceImpl(TStringRef tag) {
    *Output_ << "Resource<'" << std::string_view(tag) << "'>";
}

void TTypePrinter1::OnTaggedImpl(const TType* baseType, TStringRef tag) {
    *Output_ << "Tagged<";
    OutImpl(baseType);
    *Output_ << ",'" << std::string_view(tag) << "'>";
}

}
}
