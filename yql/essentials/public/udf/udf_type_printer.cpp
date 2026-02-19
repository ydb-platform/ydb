#include "udf_type_printer.h"
#include "udf_type_inspection.h"

#include <util/generic/scope.h>

namespace NYql::NUdf {

class TTypePrinter1: private TStubTypeVisitor {
public:
    TTypePrinter1(const ITypeInfoHelper1& typeHelper, const TType* type, ui16 compatibilityVersion);

    void Out(IOutputStream& o) const;

protected:
    void OnDataType(TDataTypeId typeId) final;
    void OnStruct(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes) final;
    void OnList(const TType* itemType) final;
    void OnOptional(const TType* itemType) final;
    void OnTuple(ui32 elementsCount, const TType** elementsTypes) final;
    void OnDict(const TType* keyType, const TType* valueType) final;
    void OnCallable(const TType* returnType, ui32 argsCount, const TType** argsTypes, ui32 optionalArgsCount, const ICallablePayload* payload) final;
    void OnVariant(const TType* underlyingType) final;
    void OnStream(const TType* itemType) final;
    void OutImpl(const TType* type) const;
    void OnDecimalImpl(ui8 precision, ui8 scale);
    void OnResourceImpl(TStringRef tag);
    void OnTaggedImpl(const TType* baseType, TStringRef tag);

private:
    void OutStructPayload(ui32 membersCount, TStringRef* membersNames, const TType** membersTypes);
    void OutTuplePayload(ui32 elementsCount, const TType** elementsTypes);
    void OutEnumValues(ui32 membersCount, TStringRef* membersNames);

protected:
    const ITypeInfoHelper1& TypeHelper1_;
    const TType* Type_;
    mutable IOutputStream* Output_;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
class TTypePrinter2: public TTypePrinter1 {
public:
    using TTypePrinter1::TTypePrinter1;

protected:
    void OnDecimal(ui8 precision, ui8 scale) final {
        OnDecimalImpl(precision, scale);
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
class TTypePrinter3: public TTypePrinter2 {
public:
    using TTypePrinter2::TTypePrinter2;

protected:
    void OnResource(TStringRef tag) final {
        OnResourceImpl(tag);
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
class TTypePrinter4: public TTypePrinter3 {
public:
    using TTypePrinter3::TTypePrinter3;

protected:
    void OnTagged(const TType* baseType, TStringRef tag) final {
        OnTaggedImpl(baseType, tag);
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class TTypePrinter5: public TTypePrinter4 {
public:
    TTypePrinter5(const ITypeInfoHelper2& typeHelper, const TType* type, ui16 compatibilityVersion);

protected:
    void OnPg(ui32 typeId) final {
        OnPgImpl(typeId);
    }

private:
    void OnPgImpl(ui32 typeId);

private:
    const ITypeInfoHelper2& TypeHelper2_;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class TTypePrinter6: public TTypePrinter5 {
public:
    using TTypePrinter5::TTypePrinter5;

protected:
    void OnBlock(const TType* itemType, bool isScalar) final {
        OnBlockImpl(itemType, isScalar);
    }

private:
    void OnBlockImpl(const TType* itemType, bool isScalar);
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 44)
class TTypePrinter7: public TTypePrinter6 {
public:
    using TTypePrinter6::TTypePrinter6;

protected:
    void OnLinear(const TType* itemType, bool isDynamic) final {
        OnLinearImpl(itemType, isDynamic);
    }

private:
    void OnLinearImpl(const TType* itemType, bool isDynamic);
};
#endif

TTypePrinter1::TTypePrinter1(const ITypeInfoHelper1& typeHelper, const TType* type, ui16 compatibilityVersion)
    : TStubTypeVisitor(compatibilityVersion)
    , TypeHelper1_(typeHelper)
    , Type_(type)
    , Output_(nullptr)
{
}

void TTypePrinter1::Out(IOutputStream& o) const {
    Output_ = &o;
    Y_DEFER {
        Output_ = nullptr;
    };
    OutImpl(Type_);
}

void TTypePrinter1::OutImpl(const TType* type) const {
    switch (TypeHelper1_.GetTypeKind(type)) {
        case ETypeKind::Null:
            *Output_ << "Null";
            break;
        case ETypeKind::Void:
            *Output_ << "Void";
            break;
        case ETypeKind::EmptyList:
            *Output_ << "EmptyList";
            break;
        case ETypeKind::EmptyDict:
            *Output_ << "EmptyDict";
            break;
        case ETypeKind::Unknown:
            *Output_ << "Unknown";
            break;
        default:
            TypeHelper1_.VisitType(type, const_cast<TTypePrinter1*>(this));
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
        if (i < membersCount - 1U) {
            *Output_ << ',';
        }
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
        if (i < elementsCount - 1U) {
            *Output_ << ',';
        }
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
        if (optionalArgsCount && i == argsCount - optionalArgsCount) {
            *Output_ << '[';
        }
        if (payload) {
            const std::string_view name = payload->GetArgumentName(i);
            if (!name.empty()) {
                *Output_ << "'" << name << "':";
            }
        }
        OutImpl(argsTypes[i]);
        if (payload) {
            if (ICallablePayload::TArgumentFlags::AutoMap == payload->GetArgumentFlags(i)) {
                *Output_ << "{Flags:AutoMap}";
            }
        }
        if (i < argsCount - 1U) {
            *Output_ << ',';
        }
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
        if (i < membersCount - 1U) {
            *Output_ << ',';
        }
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

TTypePrinter5::TTypePrinter5(const ITypeInfoHelper2& typeHelper, const TType* type, ui16 compatibilityVersion)
    : TTypePrinter4(typeHelper, type, compatibilityVersion)
    , TypeHelper2_(typeHelper)
{
}

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

void TTypePrinter7::OnLinearImpl(const TType* itemType, bool isDynamic) {
    if (isDynamic) {
        *Output_ << "Dynamic";
    }
    *Output_ << "Linear<";
    OutImpl(itemType);
    *Output_ << '>';
}

void TTypePrinter::Out(IOutputStream& o) const {
    TTypePrinter7 p(TypeHelper_, Type_, UDF_ABI_COMPATIBILITY_VERSION(2, 44));
    p.Out(o);
}

} // namespace NYql::NUdf
