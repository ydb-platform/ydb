#pragma once

#include "udf_string_ref.h"
#include "udf_types.h"

#include <library/cpp/containers/stack_vector/stack_vec.h> // TSmallVec
#include <util/generic/algorithm.h>

#include <util/system/yassert.h> // FAIL, VERIFY_DEBUG
#include <util/generic/ylimits.h> // Max


namespace NYql {
namespace NUdf {

//////////////////////////////////////////////////////////////////////////////
// TStubTypeVisitor
//////////////////////////////////////////////////////////////////////////////
class TStubTypeVisitor1: public ITypeVisitor
{
private:
    void OnDataType(TDataTypeId typeId) override;
    void OnStruct(
            ui32 membersCount,
            TStringRef* membersNames,
            const TType** membersTypes) override;
    void OnList(const TType* itemType) override;
    void OnOptional(const TType* itemType) override;
    void OnTuple(ui32 elementsCount, const TType** elementsTypes) override;
    void OnDict(const TType* keyType, const TType* valueType) override;
    void OnCallable(
            const TType* returnType,
            ui32 argsCount, const TType** argsTypes,
            ui32 optionalArgsCount,
            const ICallablePayload* payload) override;
    void OnVariant(const TType* underlyingType) override;
    void OnStream(const TType* itemType) override;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
class TStubTypeVisitor2: public TStubTypeVisitor1
{
public:
    void OnDecimal(ui8 precision, ui8 scale) override;
};
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
class TStubTypeVisitor3: public TStubTypeVisitor2
{
public:
    void OnResource(TStringRef tag) override;
};
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
class TStubTypeVisitor4: public TStubTypeVisitor3
{
public:
    void OnTagged(const TType* baseType, TStringRef tag) override;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class TStubTypeVisitor5: public TStubTypeVisitor4
{
public:
    void OnPg(ui32 typeId) override;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
class TStubTypeVisitor6: public TStubTypeVisitor5
{
public:
    void OnBlock(const TType* itemType, bool isScalar) override;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
using TStubTypeVisitor = TStubTypeVisitor6;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
using TStubTypeVisitor = TStubTypeVisitor5;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
using TStubTypeVisitor = TStubTypeVisitor4;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
using TStubTypeVisitor = TStubTypeVisitor3;
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
using TStubTypeVisitor = TStubTypeVisitor2;
#else
using TStubTypeVisitor = TStubTypeVisitor1;
#endif

//////////////////////////////////////////////////////////////////////////////
// TDataTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TDataTypeInspector: public TStubTypeVisitor
{
public:
    TDataTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Data) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return TypeId_ != 0; }
    TDataTypeId GetTypeId() const { return TypeId_; }
private:
    void OnDataType(TDataTypeId typeId) override {
        TypeId_ = typeId;
    }
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
    void OnDecimal(ui8, ui8) override {}
#endif
    TDataTypeId TypeId_ = 0;
};
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
//////////////////////////////////////////////////////////////////////////////
// TDataAndDecimalTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TDataAndDecimalTypeInspector: public TStubTypeVisitor
{
public:
    TDataAndDecimalTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Data) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return TypeId_ != 0; }
    TDataTypeId GetTypeId() const { return TypeId_; }
    ui8 GetPrecision() const {
        return Precision_;
    }

    ui8 GetScale() const {
        return Scale_;
    }

private:
    void OnDataType(TDataTypeId typeId) override {
        TypeId_ = typeId;
    }

    void OnDecimal(ui8 precision, ui8 scale) override {
        Precision_ = precision;
        Scale_ = scale;
    }

    TDataTypeId TypeId_ = 0;
    ui8 Precision_ = 0, Scale_ = 0;
};
#endif
//////////////////////////////////////////////////////////////////////////////
// TStructTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TStructTypeInspector: public TStubTypeVisitor
{
public:
    TStructTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Struct) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return MembersCount_ != Max<ui32>(); }
    ui32 GetMembersCount() const { return MembersCount_; }
    ui32 GetMemberIndex(TStringRef name) const {
        ui32 index = 0;
        for (TStringRef memberName: MembersNames_) {
            if (memberName == name) {
                return index;
            }
            index++;
        }
        return Max<ui32>();
    }
    const TStringRef& GetMemberName(ui32 i) const { return MembersNames_[i]; }
    const TType* GetMemberType(ui32 i) const { return MembersTypes_[i]; }
    TStringRef* GetMemberNames() { return MembersNames_.data(); }
    const TType** GetMemberTypes() { return MembersTypes_.data(); }

private:
    void OnStruct(
            ui32 membersCount,
            TStringRef* membersNames,
            const TType** membersTypes) override
    {
        MembersCount_ = membersCount;
        MembersNames_.reserve(membersCount);
        MembersTypes_.reserve(membersCount);

        for (ui32 i = 0; i < membersCount; i++) {
            MembersNames_.push_back(membersNames[i]);
            MembersTypes_.push_back(membersTypes[i]);
        }
    }

private:
    ui32 MembersCount_ = Max<ui32>();
    TSmallVec<TStringRef> MembersNames_;
    TSmallVec<const TType*> MembersTypes_;
};

//////////////////////////////////////////////////////////////////////////////
// TListTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TListTypeInspector: public TStubTypeVisitor
{
public:
    TListTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::List) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ItemType_ != nullptr; }
    const TType* GetItemType() const { return ItemType_; }

private:
    void OnList(const TType* itemType) override {
        ItemType_ = itemType;
    }

private:
    const TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TOptionalTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TOptionalTypeInspector: public TStubTypeVisitor
{
public:
    TOptionalTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Optional) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ItemType_ != nullptr; }
    const TType* GetItemType() const { return ItemType_; }

private:
    void OnOptional(const TType* itemType) override {
        ItemType_ = itemType;
    }

private:
    const TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TTupleTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TTupleTypeInspector: public TStubTypeVisitor
{
public:
    TTupleTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Tuple) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ElementsCount_ != Max<ui32>(); }
    ui32 GetElementsCount() const { return ElementsCount_; }
    const TType* GetElementType(ui32 i) const { return ElementsTypes_[i]; }
    const TType** GetElementTypes() { return ElementsTypes_.data(); }

private:
    void OnTuple(ui32 elementsCount, const TType** elementsTypes) override {
        ElementsCount_ = elementsCount;
        ElementsTypes_.reserve(elementsCount);

        for (ui32 i = 0; i < elementsCount; i++) {
            ElementsTypes_.push_back(elementsTypes[i]);
        }
    }

private:
    ui32 ElementsCount_ = Max<ui32>();
    TSmallVec<const TType*> ElementsTypes_;
};

//////////////////////////////////////////////////////////////////////////////
// TDictTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TDictTypeInspector: public TStubTypeVisitor
{
public:
    TDictTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Dict) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return KeyType_ != nullptr; }
    const TType* GetKeyType() const { return KeyType_; }
    const TType* GetValueType() const { return ValueType_; }

private:
    void OnDict(const TType* keyType, const TType* valueType) override {
        KeyType_ = keyType;
        ValueType_ = valueType;
    }

private:
    const TType* KeyType_ = nullptr;
    const TType* ValueType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TCallableTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TCallableTypeInspector: public TStubTypeVisitor
{
public:
    TCallableTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Callable) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ReturnType_ != nullptr; }
    const TType* GetReturnType() const { return ReturnType_; }
    ui32 GetArgsCount() const { return ArgsCount_; }
    const TType* GetArgType(ui32 i) const { return ArgsTypes_[i]; }
    ui32 GetOptionalArgsCount() const { return OptionalArgsCount_; }
    TStringRef GetPayload() const { return HasPayload_ ? Payload_ : TStringRef(); }
    TStringRef GetArgumentName(ui32 i) const { return HasPayload_ ? ArgsNames_[i] : TStringRef(); }
    ui64 GetArgumentFlags(ui32 i) const { return HasPayload_ ? ArgsFlags_[i] : 0; }

private:
    void OnCallable(
            const TType* returnType,
            ui32 argsCount, const TType** argsTypes,
            ui32 optionalArgsCount,
            const ICallablePayload* payload) override
    {
        ReturnType_ = returnType;
        ArgsCount_ = argsCount;
        OptionalArgsCount_ = optionalArgsCount;
        ArgsTypes_.reserve(argsCount);

        for (ui32 i = 0; i < argsCount; i++) {
            ArgsTypes_.push_back(argsTypes[i]);
        }

        HasPayload_ = (payload != nullptr);
        if (HasPayload_) {
            Payload_ = payload->GetPayload();
            ArgsNames_.reserve(argsCount);
            ArgsFlags_.reserve(argsCount);
            for (ui32 i = 0; i < argsCount; i++) {
                ArgsNames_.push_back(payload->GetArgumentName(i));
                ArgsFlags_.push_back(payload->GetArgumentFlags(i));
            }
        }
    }

private:
    const TType* ReturnType_ = nullptr;
    ui32 ArgsCount_ = ~0;
    TSmallVec<const TType*> ArgsTypes_;
    ui32 OptionalArgsCount_ = ~0;
    bool HasPayload_ = false;
    TStringRef Payload_;
    TSmallVec<TStringRef> ArgsNames_;
    TSmallVec<ui64> ArgsFlags_;
};

//////////////////////////////////////////////////////////////////////////////
// TStreamTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TStreamTypeInspector : public TStubTypeVisitor
{
public:
    TStreamTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Stream) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ItemType_ != nullptr; }
    const TType* GetItemType() const { return ItemType_; }

private:
    void OnStream(const TType* itemType) override {
        ItemType_ = itemType;
    }

private:
    const TType* ItemType_ = nullptr;
};

//////////////////////////////////////////////////////////////////////////////
// TVariantTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TVariantTypeInspector : public TStubTypeVisitor
{
public:
    TVariantTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Variant) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return UnderlyingType_ != nullptr; }
    const TType* GetUnderlyingType() const { return UnderlyingType_; }

private:
    void OnVariant(const TType* underlyingType) override {
        UnderlyingType_ = underlyingType;
    }

private:
    const TType* UnderlyingType_ = nullptr;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
//////////////////////////////////////////////////////////////////////////////
// TResourceTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TResourceTypeInspector: public TStubTypeVisitor
{
public:
    TResourceTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Resource) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return Tag_.Data() != nullptr; }
    TStringRef GetTag() const { return Tag_; }

private:
    void OnResource(TStringRef tag) override {
        Tag_ = tag;
    }

    TStringRef Tag_;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
//////////////////////////////////////////////////////////////////////////////
// TTaggedTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TTaggedTypeInspector: public TStubTypeVisitor
{
public:
    TTaggedTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Tagged) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return BaseType_ != nullptr; }
    const TType* GetBaseType() const { return BaseType_; }
    TStringRef GetTag() const { return Tag_; }

private:
    void OnTagged(const TType* baseType, TStringRef tag) override {
        BaseType_ = baseType;
        Tag_ = tag;
    }

    const TType* BaseType_ = nullptr;
    TStringRef Tag_;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
//////////////////////////////////////////////////////////////////////////////
// TPgTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TPgTypeInspector: public TStubTypeVisitor
{
public:
    TPgTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Pg) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return TypeId_ != 0; }
    ui32 GetTypeId() const { return TypeId_; }

private:
    void OnPg(ui32 typeId) override {
        TypeId_ = typeId;
    }

    ui32 TypeId_ = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
//////////////////////////////////////////////////////////////////////////////
// TBlockTypeInspector
//////////////////////////////////////////////////////////////////////////////
class TBlockTypeInspector: public TStubTypeVisitor
{
public:
    TBlockTypeInspector(const ITypeInfoHelper1& typeHelper, const TType* type) {
        if (typeHelper.GetTypeKind(type) == ETypeKind::Block) {
            typeHelper.VisitType(type, this);
        }
    }

    explicit operator bool() const { return ItemType_ != 0; }
    const TType* GetItemType() const { return ItemType_; }
    bool IsScalar() const { return IsScalar_; }

private:
    void OnBlock(const TType* itemType, bool isScalar) override {
        ItemType_ = itemType;
        IsScalar_ = isScalar;
    }

private:
    const TType* ItemType_ = nullptr;
    bool IsScalar_ = false;
};
#endif

inline void TStubTypeVisitor1::OnDataType(TDataTypeId typeId)
{
    Y_UNUSED(typeId);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnStruct(
        ui32 membersCount,
        TStringRef* membersNames,
        const TType** membersTypes)
{
    Y_UNUSED(membersCount); Y_UNUSED(membersNames); Y_UNUSED(membersTypes);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnList(const TType* itemType)
{
    Y_UNUSED(itemType);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnOptional(const TType* itemType)
{
    Y_UNUSED(itemType);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnTuple(ui32 elementsCount, const TType** elementsTypes)
{
    Y_UNUSED(elementsCount); Y_UNUSED(elementsTypes);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnDict(const TType* keyType, const TType* valueType)
{
    Y_UNUSED(keyType); Y_UNUSED(valueType);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnCallable(
        const TType* returnType,
        ui32 argsCount, const TType** argsTypes,
        ui32 optionalArgsCount,
        const ICallablePayload* payload)
{
    Y_UNUSED(returnType); Y_UNUSED(argsCount);
    Y_UNUSED(argsTypes); Y_UNUSED(optionalArgsCount);
    Y_UNUSED(payload);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnVariant(const TType* underlyingType) {
    Y_UNUSED(underlyingType);
    Y_ABORT("Not implemented");
}

inline void TStubTypeVisitor1::OnStream(const TType* itemType) {
    Y_UNUSED(itemType);
    Y_ABORT("Not implemented");
}
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 13)
inline void TStubTypeVisitor2::OnDecimal(ui8, ui8) {
    Y_ABORT("Not implemented");
}
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 15)
inline void TStubTypeVisitor3::OnResource(TStringRef) {
    Y_ABORT("Not implemented");
}
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 21)
inline void TStubTypeVisitor4::OnTagged(const TType*, TStringRef) {
    Y_ABORT("Not implemented");
}
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
inline void TStubTypeVisitor5::OnPg(ui32) {
    Y_ABORT("Not implemented");
}
#endif
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 26)
inline void TStubTypeVisitor6::OnBlock(const TType* itemType, bool isScalar) {
    Y_UNUSED(itemType);
    Y_UNUSED(isScalar);
    Y_ABORT("Not implemented");
}
#endif

} // namspace NUdf
} // namspace NYql
