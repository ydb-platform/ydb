#include "udf_type_printer.h"
#include "udf_type_inspection.h"

#include <util/generic/scope.h>

namespace NYql {
namespace NUdf {

TTypePrinter1::TTypePrinter1(const ITypeInfoHelper1& typeHelper, const TType* type)
    : TypeHelper1_(typeHelper), Type_(type), Output_(nullptr)
{}

void TTypePrinter1::Out(IOutputStream &o) const {
    Output_ = &o;
    Y_DEFER {
        Output_ = nullptr;
    };
    OutImpl(Type_);
}

void TTypePrinter1::OutImpl(const TType* type) const {
    switch (TypeHelper1_.GetTypeKind(type)) {
        case ETypeKind::Null: *Output_ << "Null"; break;
        case ETypeKind::Void: *Output_ << "Void"; break;
        case ETypeKind::EmptyList: *Output_ << "EmptyList"; break;
        case ETypeKind::EmptyDict: *Output_ << "EmptyDict"; break;
        case ETypeKind::Unknown: *Output_ << "Unknown"; break;
        default: TypeHelper1_.VisitType(type, const_cast<TTypePrinter1*>(this));
    }
}

void TTypePrinter1::OnDataType(TDataTypeId typeId) {
    *Output_ << GetDataTypeInfo(GetDataSlot(typeId)).Name;
}

void TTypePrinter1::OnStruct(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes) {
    *Output_ << "Struct<";
    OutStructPayload(membersCount, membersNames, membersTypes);
    *Output_ << '>';
}

void TTypePrinter1::OutStructPayload(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes) {
    for (ui32 i = 0U; i < membersCount; ++i) {
        *Output_ << "'" << std::string_view(membersNames[i]) << "':";
        OutImpl(membersTypes[i]);
        if (i < membersCount - 1U)
            *Output_ << ',';
    }
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
    OutTuplePayload(elementsCount, elementsTypes);
    *Output_ << '>';
}

void TTypePrinter1::OutTuplePayload(ui32 elementsCount, const TType** elementsTypes) {
    for (ui32 i = 0U; i < elementsCount; ++i) {
        OutImpl(elementsTypes[i]);
        if (i < elementsCount - 1U)
            *Output_ << ',';
    }
}

void TTypePrinter1::OnDict(const TType* keyType, const TType* valueType) {
    const bool isSet = TypeHelper1_.GetTypeKind(valueType) == ETypeKind::Void;
    if (isSet) {
        *Output_ << "Set<";
    } else {
        *Output_ << "Dict<";
    }
    OutImpl(keyType);
    if (!isSet) {
        *Output_ << ',';
        OutImpl(valueType);
    }
    *Output_ << '>';
}

void TTypePrinter1::OnCallable(const TType* returnType, ui32 argsCount, const TType** argsTypes, ui32 optionalArgsCount, const ICallablePayload* payload) {
    *Output_ << "Callable<(";
    for (ui32 i = 0U; i < argsCount; ++i) {
        if (optionalArgsCount && i == argsCount -  optionalArgsCount)
            *Output_ << '[';
        if (payload) {
            const std::string_view name = payload->GetArgumentName(i);
            if (!name.empty())
                *Output_ << "'" << name << "':";
        }
        OutImpl(argsTypes[i]);
        if (payload) {
            if (ICallablePayload::TArgumentFlags::AutoMap == payload->GetArgumentFlags(i))
                *Output_ << "{Flags:AutoMap}";
        }
        if (i < argsCount - 1U)
            *Output_ << ',';
    }
    *Output_ << (optionalArgsCount ? "])->" : ")->");
    OutImpl(returnType);
    *Output_ << ">";
}

void TTypePrinter1::OnVariant(const TType* underlyingType) {
    switch (TypeHelper1_.GetTypeKind(underlyingType)) {
    case ETypeKind::Struct: {
        TStructTypeInspector s(TypeHelper1_, underlyingType);
        const bool isEnum = std::all_of(s.GetMemberTypes(), s.GetMemberTypes() + s.GetMembersCount(), [this](auto memberType) {
            return TypeHelper1_.GetTypeKind(memberType) == ETypeKind::Void;
        });

        if (isEnum) {
            *Output_ << "Enum<";
            OutEnumValues(s.GetMembersCount(), s.GetMemberNames());
        } else {
            *Output_ << "Variant<";
            OutStructPayload(s.GetMembersCount(), s.GetMemberNames(), s.GetMemberTypes());
        }
        break;
    }
    case ETypeKind::Tuple: {
        TTupleTypeInspector s(TypeHelper1_, underlyingType);
        *Output_ << "Variant<";
        OutTuplePayload(s.GetElementsCount(), s.GetElementTypes());
        break;
    }
    default:
        Y_ABORT_UNLESS(false, "Unexpected underlying type in Variant");
    }
    *Output_ << '>';
}

void TTypePrinter1::OutEnumValues(ui32 membersCount, TStringRef* membersNames) {
    for (ui32 i = 0U; i < membersCount; ++i) {
        *Output_ << "'" << std::string_view(membersNames[i]) << '\'';
        if (i < membersCount - 1U)
            *Output_ << ',';
    }
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

TTypePrinter5::TTypePrinter5(const ITypeInfoHelper2& typeHelper2, const TType* type)
    : TTypePrinter4(typeHelper2, type)
    , TypeHelper2_(typeHelper2)
{}

void TTypePrinter5::OnPgImpl(ui32 typeId) {
    auto* description = TypeHelper2_.FindPgTypeDescription(typeId);
    Y_ABORT_UNLESS(description);
    auto name = std::string_view(description->Name);
    if (name.starts_with('_')) {
        name.remove_prefix(1);
        *Output_ << '_';
    }
    *Output_ << "pg" << name;
}

void TTypePrinter6::OnBlockImpl(const TType* itemType, bool isScalar) {
    *Output_ << (isScalar ? "Scalar<" : "Block<");
    OutImpl(itemType);
    *Output_ << '>';
}

}
}
