#pragma once
#include "defs.h"
#include "mkql_mem_info.h"
#include "mkql_alloc.h"

#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <stack>

namespace NKikimr::NMiniKQL {

class TType;
class INodeVisitor;
class TNode;
class TListLiteral;

template <typename T>
class TTaggedPointer {
public:
    TTaggedPointer() {
    }
    TTaggedPointer(T* ptr, bool mark) {
        Y_DEBUG_ABORT_UNLESS((uintptr_t(ptr) & 1) == 0);
        Raw_ = (void*)(uintptr_t(ptr) | (mark ? 1 : 0));
    }

    T* GetPtr() const {
        return (T*)(uintptr_t(Raw_) & ~uintptr_t(1));
    }

    bool GetMark() const {
        return (uintptr_t(Raw_) & 1) != 0;
    }

private:
    void* Raw_;
};

struct TRuntimeNode {
    using TList = TSmallVec<TRuntimeNode>;

    TRuntimeNode()
        : Data(nullptr, true)
    {
    }

    TRuntimeNode(TNode* node, bool isImmediate)
        : Data(node, isImmediate)
    {
    }

    explicit operator bool() const {
        return Data.GetPtr();
    }

    ~TRuntimeNode() {
    }

    TType* GetRuntimeType() const;

    TType* GetStaticType() const;

    bool HasValue() const;
    TNode* GetValue() const;
    void Freeze();

    bool operator==(const TRuntimeNode& other) const;
    bool operator!=(const TRuntimeNode& other) const {
        return !(*this == other);
    }

    bool IsImmediate() const {
        return Data.GetMark();
    }

    TNode* GetNode() const {
        return Data.GetPtr();
    }

    TTaggedPointer<TNode> Data;
};

using TRuntimeNodePair = std::array<TRuntimeNode, 2U>;

class TTypeEnvironment;

class TNode: private TNonCopyable {
    friend class TTypeEnvironment;

public:
    TType* GetType() const {
        return Type_;
    }

    TType* GetGenericType() const {
        return Type_;
    }

    // may be used only as temporary storage, should be zero before visitation
    ui64 GetCookie() const {
        return Cookie_;
    }

    void SetCookie(ui64 cookie) {
        Cookie_ = cookie;
    }

    void Accept(INodeVisitor& visitor);
    bool Equals(const TNode& nodeToCompare) const;
    // replace map stored in cookies
    TNode* CloneOnCallableWrite(const TTypeEnvironment& env) const;
    void Freeze(const TTypeEnvironment& env);
    bool IsMergeable() const;

protected:
    explicit TNode(TType* type)
        : Type_(type)
        , Cookie_(0)
    {
    }

    TType* Type_;
    ui64 Cookie_;
};

class TTypeType;
class TTypeEnvironment;

// if kind is above 0xf, it must ends with 0x02 pattern (like Void)
#define MKQL_TYPE_KINDS(XX) \
    XX(Type, 0)             \
    XX(Variant, 1)          \
    XX(Void, 2)             \
    XX(Data, 3)             \
    XX(Stream, 4)           \
    XX(Struct, 5)           \
    XX(List, 6)             \
    XX(Optional, 7)         \
    XX(Dict, 8)             \
    XX(Callable, 9)         \
    XX(Any, 10)             \
    XX(Tuple, 11)           \
    XX(Resource, 12)        \
    XX(Flow, 13)            \
    XX(Null, 14)            \
    XX(ReservedKind, 15)    \
    XX(EmptyList, 16 + 2)   \
    XX(EmptyDict, 32 + 2)   \
    XX(Tagged, 48 + 7)      \
    XX(Block, 16 + 13)      \
    XX(Pg, 16 + 3)          \
    XX(Multi, 16 + 11)      \
    XX(Linear, 16 + 7)

class TTypeBase: public TNode {
public:
    enum class EKind: ui8 {
        MKQL_TYPE_KINDS(ENUM_VALUE_GEN)
    };

    inline EKind GetKind() const {
        return Kind;
    }

    bool IsSameType(const TTypeBase& typeToCompare) const;
    size_t CalcHash() const;

    TTypeBase(const TTypeBase& other)
        : TNode(other.Type_)
        , Kind(other.Kind)
        , SupportsPresort_(other.SupportsPresort_)
    {
    }

protected:
    TTypeBase(EKind kind, TTypeType* type, bool supportsPresort);
    TTypeBase()
        : TNode(nullptr)
        , Kind(EKind::Type)
        , SupportsPresort_(false)
    {
    }

    const EKind Kind;
    const bool SupportsPresort_;
};

class TType: public TTypeBase {
protected:
    TType(EKind kind, TTypeType* type, bool supportsPresort)
        : TTypeBase(kind, type, supportsPresort)
    {
    }

    TType()
        : TTypeBase()
    {
    }

public:
    static TStringBuf KindAsStr(EKind kind);
    TStringBuf GetKindAsStr() const;

#define MKQL_KIND_ACCESSOR(name, value) \
    inline bool Is##name() const {      \
        return Kind == EKind::name;     \
    }

    MKQL_TYPE_KINDS(MKQL_KIND_ACCESSOR)

#undef MKQL_KIND_ACCESSOR

    using TTypeBase::IsSameType;

    bool IsConvertableTo(const TType& typeToCompare, bool ignoreTagged = false) const;
    void Accept(INodeVisitor& visitor);
    TNode* CloneOnCallableWrite(const TTypeEnvironment& env) const;
    void Freeze(const TTypeEnvironment& env);
    bool IsPresortSupported() const {
        return SupportsPresort_;
    }
};

class TTypeType: public TType {
    friend class TTypeEnvironment;
    friend class TType;

public:
    using TType::IsSameType;
    bool IsSameType(const TTypeType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TTypeType& typeToCompare, bool ignoreTagged = false) const;

private:
    TTypeType()
        : TType()
    {
    }

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

    static TTypeType* Create(const TTypeEnvironment& env);
};

template <TType::EKind SingularKind>
class TSingularType: public TType {
    friend class TTypeEnvironment;
    friend class TType;

public:
    using TType::IsSameType;
    bool IsSameType(const TSingularType<SingularKind>& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TSingularType<SingularKind>& typeToCompare, bool ignoreTagged = false) const;

private:
    explicit TSingularType(TTypeType* type)
        : TType(SingularKind, type, true)
    {
    }

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

    static TSingularType<SingularKind>* Create(TTypeType* type, const TTypeEnvironment& env);
};

using TVoidType = TSingularType<TType::EKind::Void>;
using TNullType = TSingularType<TType::EKind::Null>;
using TEmptyListType = TSingularType<TType::EKind::EmptyList>;
using TEmptyDictType = TSingularType<TType::EKind::EmptyDict>;

template <TType::EKind SingularKind>
TType* GetTypeOfSingular(const TTypeEnvironment& env);

template <typename TLiteralType>
TLiteralType* GetEmptyLiteral(const TTypeEnvironment& env);

template <TType::EKind SingularKind>
class TSingular: public TNode {
    friend class TTypeEnvironment;
    friend class TNode;

public:
    TSingularType<SingularKind>* GetType() const {
        return static_cast<TSingularType<SingularKind>*>(GetGenericType());
    }

private:
    explicit TSingular(const TTypeEnvironment& env)
        : TNode(GetTypeOfSingular<SingularKind>(env))
    {
    }

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

    using TNode::Equals;
    bool Equals(const TSingular<SingularKind>& nodeToCompare) const;
    static TSingular<SingularKind>* Create(const TTypeEnvironment& env);
};

using TVoid = TSingular<TType::EKind::Void>;
using TNull = TSingular<TType::EKind::Null>;
using TEmptyList = TSingular<TType::EKind::EmptyList>;
using TEmptyDict = TSingular<TType::EKind::EmptyDict>;

class TDataLiteral;
class TStructLiteral;
class TListLiteral;
class TOptionalLiteral;
class TAnyType;
class TTupleLiteral;
class TResourceType;
class TDataType;
class TPgType;

// A non-owning reference to internalized string
// Created only by TTypeEnvironment::InternName
class TInternName {
public:
    TInternName()
    {
    }

    TInternName(const TInternName& other)
        : StrBuf_(other.StrBuf_)
    {
    }

    TInternName& operator=(const TInternName& other) {
        StrBuf_ = other.StrBuf_;
        return *this;
    }

    size_t Hash() const {
        return (size_t)StrBuf_.data();
    }

    explicit operator bool() const {
        return (bool)StrBuf_;
    }

    const TStringBuf& Str() const {
        return StrBuf_;
    }

    // Optimized comparison (only by pointer)
    bool operator==(const TInternName& other) const {
        Y_DEBUG_ABORT_UNLESS(StrBuf_.data() != other.StrBuf_.data() || StrBuf_.size() == other.StrBuf_.size(),
                             "Lengths must be equal if pointers are equal");
        return StrBuf_.data() == other.StrBuf_.data();
    }

    bool operator!=(const TInternName& other) const {
        return !this->operator==(other);
    }

    // Regular comparison (by content)
    bool operator==(const TStringBuf& other) const {
        return StrBuf_ == other;
    }

    bool operator!=(const TStringBuf& other) const {
        return !this->operator==(other);
    }

private:
    friend class TTypeEnvironment;

    explicit TInternName(const TStringBuf& strBuf)
        : StrBuf_(strBuf)
    {
    }

private:
    TStringBuf StrBuf_;
};

} // namespace NKikimr::NMiniKQL

template <>
struct THash<NKikimr::NMiniKQL::TInternName> {
    size_t operator()(const NKikimr::NMiniKQL::TInternName& val) const {
        return val.Hash();
    }
};

namespace NKikimr::NMiniKQL {

class TTypeEnvironment: private TNonCopyable {
public:
    explicit TTypeEnvironment(TScopedAlloc& alloc);

    ~TTypeEnvironment();

    template <typename T>
    T* Allocate() const {
        return (T*)Arena_.Alloc(sizeof(T));
    }

    void* AllocateBuffer(ui64 size) const {
        return Arena_.Alloc(size);
    }

    TInternName InternName(const TStringBuf& name) const;

    TTypeType* GetTypeOfTypeLazy() const;
    TVoidType* GetTypeOfVoidLazy() const;
    TVoid* GetVoidLazy() const;
    TNullType* GetTypeOfNullLazy() const;
    TNull* GetNullLazy() const;
    TEmptyListType* GetTypeOfEmptyListLazy() const;
    TEmptyList* GetEmptyListLazy() const;
    TEmptyDictType* GetTypeOfEmptyDictLazy() const;
    TEmptyDict* GetEmptyDictLazy() const;
    TStructLiteral* GetEmptyStructLazy() const;
    TListLiteral* GetListOfVoidLazy() const;
    TAnyType* GetAnyTypeLazy() const;
    TTupleLiteral* GetEmptyTupleLazy() const;
    TDataType* GetUi32Lazy() const;
    TDataType* GetUi64Lazy() const;

    std::vector<TNode*>& GetNodeStack() const;

    void ClearCookies() const;

    NUdf::TUnboxedValuePod NewStringValue(const NUdf::TStringRef& data) const {
        Y_DEBUG_ABORT_UNLESS(TlsAllocState);
        Y_DEBUG_ABORT_UNLESS(&Alloc_.Ref() == TlsAllocState, "%s", (TStringBuilder() << "typeEnv's: " << Alloc_.Ref().GetDebugInfo() << " Tls: " << TlsAllocState->GetDebugInfo()).data());
        if (data.Size() > NUdf::TUnboxedValue::InternalBufferSize) {
            auto value = NewString(data.Size());
            std::memcpy(value.Data(), data.Data(), data.Size());
            return NUdf::TUnboxedValuePod(std::move(value));
        } else {
            return NUdf::TUnboxedValuePod::Embedded(data);
        }
    }

    TGuard<TScopedAlloc> BindAllocator() const {
        return Guard(Alloc_);
    }

    TScopedAlloc& GetAllocator() const {
        return Alloc_;
    }

    const NUdf::TStringValue& NewString(ui32 size) const {
        Y_DEBUG_ABORT_UNLESS(TlsAllocState);
        Y_DEBUG_ABORT_UNLESS(&Alloc_.Ref() == TlsAllocState, "%s", (TStringBuilder() << "typeEnv's: " << Alloc_.Ref().GetDebugInfo() << " Tls: " << TlsAllocState->GetDebugInfo()).data());
        Strings_.emplace(size);
        return Strings_.top();
    }

private:
    TScopedAlloc& Alloc_;
    mutable TPagedArena Arena_;
    mutable std::stack<NUdf::TStringValue> Strings_;
    mutable THashSet<TStringBuf> NamesPool_;
    mutable std::vector<TNode*> Stack_;

    mutable TTypeType* TypeOfType_ = nullptr;
    mutable TVoidType* TypeOfVoid_ = nullptr;
    mutable TVoid* Void_ = nullptr;
    mutable TNullType* TypeOfNull_ = nullptr;
    mutable TNull* Null_ = nullptr;
    mutable TEmptyListType* TypeOfEmptyList_ = nullptr;
    mutable TEmptyList* EmptyList_ = nullptr;
    mutable TEmptyDictType* TypeOfEmptyDict_ = nullptr;
    mutable TEmptyDict* EmptyDict_ = nullptr;
    mutable TDataType* Ui32_ = nullptr;
    mutable TDataType* Ui64_ = nullptr;
    mutable TAnyType* AnyType_ = nullptr;
    mutable TStructLiteral* EmptyStruct_ = nullptr;
    mutable TTupleLiteral* EmptyTuple_ = nullptr;
    mutable TListLiteral* ListOfVoid_ = nullptr;
};

template <>
inline TType* GetTypeOfSingular<TType::EKind::Void>(const TTypeEnvironment& env) {
    return env.GetTypeOfVoidLazy();
}

template <>
inline TType* GetTypeOfSingular<TType::EKind::Null>(const TTypeEnvironment& env) {
    return env.GetTypeOfNullLazy();
}

template <>
inline TType* GetTypeOfSingular<TType::EKind::EmptyList>(const TTypeEnvironment& env) {
    return env.GetTypeOfEmptyListLazy();
}

template <>
inline TType* GetTypeOfSingular<TType::EKind::EmptyDict>(const TTypeEnvironment& env) {
    return env.GetTypeOfEmptyDictLazy();
}

template <>
inline TTupleLiteral* GetEmptyLiteral(const TTypeEnvironment& env) {
    return env.GetEmptyTupleLazy();
}

class TDataType: public TType {
    friend class TType;

public:
    static TDataType* Create(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TDataType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TDataType& typeToCompare, bool ignoreTagged = false) const;

    NUdf::TDataTypeId GetSchemeType() const {
        return SchemeType_;
    }

    TMaybe<NUdf::EDataSlot> GetDataSlot() const {
        return DataSlot_;
    }

protected:
    TDataType(NUdf::TDataTypeId schemeType, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    const NUdf::TDataTypeId SchemeType_;
    const TMaybe<NUdf::EDataSlot> DataSlot_;
};

class TDataDecimalType: public TDataType {
    friend class TType;

public:
    static TDataDecimalType* Create(ui8 precision, ui8 scale, const TTypeEnvironment& env);

    bool IsSameType(const TDataDecimalType& typeToCompare) const;
    size_t CalcHash() const;

    bool IsConvertableTo(const TDataDecimalType& typeToCompare, bool ignoreTagged = false) const;

    std::pair<ui8, ui8> GetParams() const;

private:
    TDataDecimalType(ui8 precision, ui8 scale, const TTypeEnvironment& env);

    const ui8 Precision_;
    const ui8 Scale_;
};

class TDataLiteral: public TNode, private NUdf::TUnboxedValuePod {
    friend class TNode;

public:
    static TDataLiteral* Create(const NUdf::TUnboxedValuePod& value, TDataType* type, const TTypeEnvironment& env);

    TDataType* GetType() const {
        return static_cast<TDataType*>(GetGenericType());
    }

    const NUdf::TUnboxedValuePod& AsValue() const {
        return *this;
    }

private:
    TDataLiteral(const NUdf::TUnboxedValuePod& value, TDataType* type);
    using TNode::Equals;
    bool Equals(const TDataLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);
};

class TPgType: public TType {
    friend class TType;

public:
    static TPgType* Create(ui32 typeId, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TPgType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TPgType& typeToCompare, bool ignoreTagged = false) const;

    ui32 GetTypeId() const {
        return TypeId_;
    }

    const TString& GetName() const;

protected:
    TPgType(ui32 typeId, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    const ui32 TypeId_;
};

struct TStructMember {
    TStructMember()
        : Type(nullptr)
        , Index(nullptr)
    {
    }

    TStructMember(const TStringBuf& name, TType* type, ui32* index = nullptr)
        : Name(name)
        , Type(type)
        , Index(index)
    {
    }

    bool operator<(const TStructMember& rhs) const {
        return Name < rhs.Name;
    }

    TStringBuf Name;
    TType* Type;
    ui32* Index;
};

class TStructType: public TType {
    friend class TType;

public:
    static TStructType* Create(const std::pair<TString, TType*>* members, ui32 membersCount, const TTypeEnvironment& env);
    static TStructType* Create(ui32 membersCount, const TStructMember* members, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TStructType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TStructType& typeToCompare, bool ignoreTagged = false) const;

    ui32 GetMembersCount() const {
        return MembersCount_;
    }

    TStringBuf GetMemberName(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < MembersCount_);
        return Members_[index].first.Str();
    }

    TInternName GetMemberNameStr(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < MembersCount_);
        return Members_[index].first;
    }

    TType* GetMemberType(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < MembersCount_);
        return Members_[index].second;
    }

    ui32 GetMemberIndex(const TStringBuf& name) const;
    TMaybe<ui32> FindMemberIndex(const TStringBuf& name) const;

private:
    TStructType(ui32 membersCount, std::pair<TInternName, TType*>* members, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);
    static bool CalculatePresortSupport(ui32 membersCount, std::pair<TInternName, TType*>* members);

private:
    ui32 MembersCount_;
    std::pair<TInternName, TType*>* Members_;
};

class TStructLiteral: public TNode {
    friend class TNode;

public:
    static TStructLiteral* Create(
        ui32 valuesCount, const TRuntimeNode* values, TStructType* type,
        const TTypeEnvironment& env, bool useCachedEmptyStruct = true);
    TStructType* GetType() const {
        return static_cast<TStructType*>(GetGenericType());
    }

    ui32 GetValuesCount() const {
        return GetType()->GetMembersCount();
    }

    TRuntimeNode GetValue(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < GetValuesCount());
        return Values_[index];
    }

private:
    TStructLiteral(TRuntimeNode* values, TStructType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TStructLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode* Values_;
};

class TListType: public TType {
    friend class TType;

public:
    static TListType* Create(TType* itemType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TListType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TListType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetItemType() const {
        return Data_;
    }

    TDataType* IndexDictKeyType() const {
        return IndexDictKey_;
    }

private:
    TListType(TType* itemType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
    TDataType* IndexDictKey_;
};

class TListLiteral: public TNode {
    friend class TNode;

public:
    static TListLiteral* Create(TRuntimeNode* items, ui32 count, TListType* type, const TTypeEnvironment& env);
    TListType* GetType() const {
        return static_cast<TListType*>(GetGenericType());
    }

    ui32 GetItemsCount() const {
        return Count_;
    }

    TRuntimeNode* GetItems() const {
        return Items_;
    }

private:
    TListLiteral(TRuntimeNode* items, ui32 count, TListType* type, const TTypeEnvironment& env, bool validate = true);

    using TNode::Equals;
    bool Equals(const TListLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode* Items_;
    ui32 Count_;
};

class TStreamType: public TType {
    friend class TType;

public:
    static TStreamType* Create(TType* itemType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TStreamType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TStreamType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetItemType() const {
        return Data_;
    }

private:
    TStreamType(TType* itemType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
};

class TFlowType: public TType {
    friend class TType;

public:
    static TFlowType* Create(TType* itemType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TFlowType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TFlowType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetItemType() const {
        return Data_;
    }

private:
    TFlowType(TType* itemType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
};

class TOptionalType: public TType {
    friend class TType;

public:
    static TOptionalType* Create(TType* itemType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TOptionalType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TOptionalType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetItemType() const {
        return Data_;
    }

private:
    TOptionalType(TType* itemType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
};

class TOptionalLiteral: public TNode {
    friend class TNode;

public:
    static TOptionalLiteral* Create(TRuntimeNode item, TOptionalType* type, const TTypeEnvironment& env);
    static TOptionalLiteral* Create(TOptionalType* type, const TTypeEnvironment& env);

    TOptionalType* GetType() const {
        return static_cast<TOptionalType*>(GetGenericType());
    }

    bool HasItem() const {
        return !!Item_.GetNode();
    }

    TRuntimeNode GetItem() const {
        Y_DEBUG_ABORT_UNLESS(Item_.GetNode());
        return Item_;
    }

private:
    TOptionalLiteral(TRuntimeNode item, TOptionalType* type, bool validate = true);
    explicit TOptionalLiteral(TOptionalType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TOptionalLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode Item_;
};

class TLinearType: public TType {
    friend class TType;

public:
    static TLinearType* Create(TType* itemType, bool isDynamic, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TLinearType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TLinearType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetItemType() const {
        return Data_;
    }

    bool IsDynamic() const {
        return IsDynamic_;
    }

private:
    TLinearType(TType* itemType, bool isDynamic, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
    bool IsDynamic_;
};

class TDictType: public TType {
    friend class TType;

public:
    static TDictType* Create(TType* keyType, TType* payloadType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TDictType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TDictType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetKeyType() const {
        return KeyType_;
    }

    TType* GetPayloadType() const {
        return PayloadType_;
    }

    static void EnsureValidDictKey(TType* keyType);

private:
    TDictType(TType* keyType, TType* payloadType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* KeyType_;
    TType* PayloadType_;
};

class TDictLiteral: public TNode {
    friend class TNode;

public:
    static TDictLiteral* Create(ui32 itemsCount, const std::pair<TRuntimeNode, TRuntimeNode>* items, TDictType* type, const TTypeEnvironment& env);
    TDictType* GetType() const {
        return static_cast<TDictType*>(GetGenericType());
    }

    ui32 GetItemsCount() const {
        return ItemsCount_;
    }

    std::pair<TRuntimeNode, TRuntimeNode> GetItem(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < ItemsCount_);
        return Items_[index];
    }

private:
    TDictLiteral(ui32 itemsCount, std::pair<TRuntimeNode, TRuntimeNode>* items, TDictType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TDictLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    ui32 ItemsCount_;
    std::pair<TRuntimeNode, TRuntimeNode>* Items_;
};

class TCallableType: public TType {
    friend class TType;

public:
    static TCallableType* Create(const TString& name, TType* returnType, ui32 argumentsCount,
                                 TType** arguments, TNode* payload, const TTypeEnvironment& env);
    static TCallableType* Create(TType* returnType, const TStringBuf& name, ui32 argumentsCount,
                                 TType** arguments, TNode* payload, const TTypeEnvironment& env);
    void SetOptionalArgumentsCount(ui32 count);
    ui32 GetOptionalArgumentsCount() const {
        return OptionalArgs_;
    }

    using TType::IsSameType;
    bool IsSameType(const TCallableType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TCallableType& typeToCompare, bool ignoreTagged = false) const;

    TStringBuf GetName() const {
        return Name_.Str();
    }

    TInternName GetNameStr() const {
        return Name_;
    }

    TType* GetReturnType() const {
        return ReturnType_;
    }

    ui32 GetArgumentsCount() const {
        return ArgumentsCount_;
    }

    TType* GetArgumentType(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < ArgumentsCount_);
        return Arguments_[index];
    }

    void DisableMerge() {
        IsMergeDisabled0_ = true;
    }

    bool IsMergeDisabled() const {
        return IsMergeDisabled0_;
    }

    TNode* GetPayload() const {
        return Payload_;
    }

private:
    TCallableType(const TInternName& name, TType* returnType, ui32 argumentsCount, TType** arguments,
                  TNode* payload, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    bool IsMergeDisabled0_;
    ui32 ArgumentsCount_;
    TInternName Name_;
    TType* ReturnType_;
    TType** Arguments_;
    TNode* Payload_;
    ui32 OptionalArgs_;
};

class TCallablePayload: public NUdf::ICallablePayload {
public:
    explicit TCallablePayload(NMiniKQL::TNode* node);

    NUdf::TStringRef GetPayload() const override {
        return Payload_;
    }

    NUdf::TStringRef GetArgumentName(ui32 index) const override {
        return ArgsNames_[index];
    }

    ui64 GetArgumentFlags(ui32 index) const override {
        return ArgsFlags_[index];
    }

private:
    NUdf::TStringRef Payload_;
    TVector<NUdf::TStringRef> ArgsNames_;
    TVector<ui64> ArgsFlags_;
};

class TCallable: public TNode {
    friend class TNode;

public:
    static TCallable* Create(ui32 inputsCount, const TRuntimeNode* inputs, TCallableType* type, const TTypeEnvironment& env);
    static TCallable* Create(TRuntimeNode result, TCallableType* type, const TTypeEnvironment& env);
    TCallableType* GetType() const {
        return static_cast<TCallableType*>(GetGenericType());
    }

    ui32 GetInputsCount() const {
        return InputsCount_;
    }

    TRuntimeNode GetInput(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < InputsCount_);
        return Inputs_[index];
    }

    bool HasResult() const {
        return !!Result_.GetNode();
    }

    TRuntimeNode GetResult() const {
        Y_DEBUG_ABORT_UNLESS(!!Result_.GetNode());
        return Result_;
    }

    void SetResult(TRuntimeNode result, const TTypeEnvironment& env);
    ui32 GetUniqueId() const {
        return UniqueId_;
    }

    void SetUniqueId(ui32 uniqueId) {
        UniqueId_ = uniqueId;
    }

private:
    TCallable(ui32 inputsCount, TRuntimeNode* inputs, TCallableType* type, bool validate = true);
    TCallable(TRuntimeNode result, TCallableType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TCallable& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    ui32 InputsCount_;
    ui32 UniqueId_;
    TRuntimeNode* Inputs_;
    TRuntimeNode Result_;
};

inline TTypeBase::TTypeBase(EKind kind, TTypeType* type, bool supportsPresort)
    : TNode(type)
    , Kind(kind)
    , SupportsPresort_(supportsPresort)
{
    Y_DEBUG_ABORT_UNLESS(kind != EKind::Type);
}

inline TType* TRuntimeNode::GetStaticType() const {
    MKQL_ENSURE(GetNode() != nullptr, "Node is a nullptr value");

    if (IsImmediate()) {
        return GetNode()->GetGenericType();
    } else {
        MKQL_ENSURE(GetNode()->GetType()->IsCallable(), "Wrong type");

        const auto& callable = static_cast<const TCallable&>(*GetNode());
        return callable.GetType()->GetReturnType();
    }
}

class TAnyType: public TType {
    friend class TTypeEnvironment;
    friend class TType;

public:
    using TType::IsSameType;
    bool IsSameType(const TAnyType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TAnyType& typeToCompare, bool ignoreTagged = false) const;

private:
    explicit TAnyType(TTypeType* type)
        : TType(EKind::Any, type, false)
    {
    }

    static TAnyType* Create(TTypeType* type, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);
};

class TAny: public TNode {
    friend class TNode;

public:
    static TAny* Create(const TTypeEnvironment& env);
    TAnyType* GetType() const {
        return static_cast<TAnyType*>(GetGenericType());
    }

    bool HasItem() const {
        return !!Item_.GetNode();
    }

    TRuntimeNode GetItem() const {
        Y_DEBUG_ABORT_UNLESS(Item_.GetNode());
        return Item_;
    }

    void SetItem(TRuntimeNode newItem);

private:
    explicit TAny(TAnyType* type)
        : TNode(type)
    {
    }

    using TNode::Equals;
    bool Equals(const TAny& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode Item_;
};

template <typename TDerived, TType::EKind DerivedKind>
class TTupleLikeType: public TType {
    friend class TType;
    using TSelf = TTupleLikeType<TDerived, DerivedKind>;

public:
    static TDerived* Create(ui32 elementsCount, TType* const* elements, const TTypeEnvironment& env) {
        TType** allocatedElements = nullptr;
        if (elementsCount) {
            allocatedElements = static_cast<TType**>(env.AllocateBuffer(elementsCount * sizeof(*allocatedElements)));
            for (ui32 i = 0; i < elementsCount; ++i) {
                allocatedElements[i] = elements[i];
            }
        }

        return ::new (env.Allocate<TDerived>()) TDerived(elementsCount, allocatedElements, env);
    }

    using TType::IsSameType;
    bool IsSameType(const TDerived& typeToCompare) const {
        if (this == &typeToCompare) {
            return true;
        }

        if (ElementsCount_ != typeToCompare.ElementsCount_) {
            return false;
        }

        for (size_t index = 0; index < ElementsCount_; ++index) {
            if (!Elements_[index]->IsSameType(*typeToCompare.Elements_[index])) {
                return false;
            }
        }

        return true;
    }

    size_t CalcHash() const {
        size_t hash = 0;
        for (size_t index = 0; index < ElementsCount_; ++index) {
            hash = CombineHashes(hash, Elements_[index]->CalcHash());
        }
        return hash;
    }

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TDerived& typeToCompare, bool ignoreTagged = false) const {
        if (this == &typeToCompare) {
            return true;
        }

        if (ElementsCount_ != typeToCompare.GetElementsCount()) {
            return false;
        }

        for (size_t index = 0; index < ElementsCount_; ++index) {
            if (!Elements_[index]->IsConvertableTo(*typeToCompare.GetElementType(index), ignoreTagged)) {
                return false;
            }
        }

        return true;
    }

    ui32 GetElementsCount() const {
        return ElementsCount_;
    }

    TType* GetElementType(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < ElementsCount_);
        return Elements_[index];
    }

    TArrayRef<TType* const> GetElements() const {
        return TArrayRef<TType* const>(Elements_, ElementsCount_);
    }

protected:
    TTupleLikeType(ui32 elementsCount, TType** elements, const TTypeEnvironment& env)
        : TType(DerivedKind, env.GetTypeOfTypeLazy(), CalculatePresortSupport(elementsCount, elements))
        , ElementsCount_(elementsCount)
        , Elements_(elements)
    {
    }

private:
    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
        bool needClone = false;
        for (ui32 i = 0; i < ElementsCount_; ++i) {
            if (Elements_[i]->GetCookie()) {
                needClone = true;
                break;
            }
        }

        if (!needClone) {
            return const_cast<TSelf*>(this);
        }

        TType** allocatedElements = nullptr;
        if (ElementsCount_) {
            allocatedElements = static_cast<TType**>(env.AllocateBuffer(ElementsCount_ * sizeof(*allocatedElements)));
            for (ui32 i = 0; i < ElementsCount_; ++i) {
                allocatedElements[i] = Elements_[i];
                auto newNode = (TNode*)Elements_[i]->GetCookie();
                if (newNode) {
                    allocatedElements[i] = static_cast<TType*>(newNode);
                }
            }
        }

        return ::new (env.Allocate<TDerived>()) TDerived(ElementsCount_, allocatedElements, env);
    }

    void DoFreeze(const TTypeEnvironment& env) {
        Y_UNUSED(env);
    }

    static bool CalculatePresortSupport(ui32 elementsCount, TType** elements) {
        for (ui32 i = 0; i < elementsCount; ++i) {
            if (!elements[i]->IsPresortSupported()) {
                return false;
            }
        }
        return true;
    }

private:
    ui32 ElementsCount_;
    TType** Elements_;
};

class TTupleType: public TTupleLikeType<TTupleType, TType::EKind::Tuple> {
private:
    friend class TType;
    using TBase = TTupleLikeType<TTupleType, TType::EKind::Tuple>;
    friend TBase;

    TTupleType(ui32 elementsCount, TType** elements, const TTypeEnvironment& env)
        : TBase(elementsCount, elements, env)
    {
    }
};

class TMultiType: public TTupleLikeType<TMultiType, TType::EKind::Multi> {
private:
    friend class TType;
    using TBase = TTupleLikeType<TMultiType, TType::EKind::Multi>;
    friend TBase;

    TMultiType(ui32 elementsCount, TType** elements, const TTypeEnvironment& env)
        : TBase(elementsCount, elements, env)
    {
    }
};

class TTupleLiteral: public TNode {
    friend class TNode;

public:
    static TTupleLiteral* Create(
        ui32 valuesCount, const TRuntimeNode* items, TTupleType* type,
        const TTypeEnvironment& env, bool useCachedEmptyTuple = true);
    TTupleType* GetType() const {
        return static_cast<TTupleType*>(GetGenericType());
    }

    ui32 GetValuesCount() const {
        return GetType()->GetElementsCount();
    }

    TRuntimeNode GetValue(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(index < GetValuesCount());
        return Values_[index];
    }

private:
    TTupleLiteral(TRuntimeNode* values, TTupleType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TTupleLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode* Values_;
};

class TResourceType: public TType {
    friend class TTypeEnvironment;
    friend class TType;

public:
    using TType::IsSameType;
    bool IsSameType(const TResourceType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TResourceType& typeToCompare, bool ignoreTagged = false) const;

    TStringBuf GetTag() const {
        return Tag_.Str();
    }

    TInternName GetTagStr() const {
        return Tag_;
    }

    static TResourceType* Create(const TStringBuf& tag, const TTypeEnvironment& env);

private:
    TResourceType(TTypeType* type, TInternName tag)
        : TType(EKind::Resource, type, false)
        , Tag_(tag)
    {
    }

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TInternName const Tag_;
};

class TTaggedType: public TType {
    friend class TType;

public:
    static TTaggedType* Create(TType* baseType, const TStringBuf& tag, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TTaggedType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TTaggedType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetBaseType() const {
        return BaseType_;
    }

    TStringBuf GetTag() const {
        return Tag_.Str();
    }

    TInternName GetTagStr() const {
        return Tag_;
    }

private:
    TTaggedType(TType* baseType, TInternName tag, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* BaseType_;
    TInternName const Tag_;
};

class TVariantType: public TType {
    friend class TType;

public:
    static TVariantType* Create(TType* underlyingType, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TVariantType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TVariantType& typeToCompare, bool ignoreTagged = false) const;

    TType* GetUnderlyingType() const {
        return Data_;
    }

    ui32 GetAlternativesCount() const {
        if (Data_->IsStruct()) {
            return static_cast<TStructType*>(Data_)->GetMembersCount();
        } else {
            return static_cast<TTupleType*>(Data_)->GetElementsCount();
        }
    }

    TType* GetAlternativeType(ui32 index) const {
        MKQL_ENSURE(index < GetAlternativesCount(), "Wrong index");
        if (Data_->IsStruct()) {
            return static_cast<TStructType*>(Data_)->GetMemberType(index);
        } else {
            return static_cast<TTupleType*>(Data_)->GetElementType(index);
        }
    }

private:
    TVariantType(TType* underlyingType, const TTypeEnvironment& env, bool validate = true);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* Data_;
};

class TVariantLiteral: public TNode {
    friend class TNode;

public:
    static TVariantLiteral* Create(TRuntimeNode item, ui32 index, TVariantType* type, const TTypeEnvironment& env);

    TVariantType* GetType() const {
        return static_cast<TVariantType*>(GetGenericType());
    }

    ui32 GetIndex() const {
        return Index_;
    }

    TRuntimeNode GetItem() const {
        return Item_;
    }

private:
    TVariantLiteral(TRuntimeNode item, ui32 index, TVariantType* type, bool validate = true);
    using TNode::Equals;
    bool Equals(const TVariantLiteral& nodeToCompare) const;

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TRuntimeNode Item_;
    ui32 Index_;
};

class TBlockType: public TType {
    friend class TType;

public:
    enum class EShape: ui8 {
        Scalar = 0,
        Many = 1
    };

public:
    static TBlockType* Create(TType* itemType, EShape shape, const TTypeEnvironment& env);

    using TType::IsSameType;
    bool IsSameType(const TBlockType& typeToCompare) const;
    size_t CalcHash() const;

    using TType::IsConvertableTo;
    bool IsConvertableTo(const TBlockType& typeToCompare, bool ignoreTagged = false) const;

    inline TType* GetItemType() const noexcept {
        return ItemType_;
    }

    inline EShape GetShape() const noexcept {
        return Shape_;
    }

private:
    TBlockType(TType* itemType, EShape shape, const TTypeEnvironment& env);

    TNode* DoCloneOnCallableWrite(const TTypeEnvironment& env) const;
    void DoFreeze(const TTypeEnvironment& env);

private:
    TType* ItemType_;
    EShape Shape_;
};

inline bool TRuntimeNode::operator==(const TRuntimeNode& other) const {
    return IsImmediate() == other.IsImmediate() && GetNode()->Equals(*other.GetNode());
}

inline TType* TRuntimeNode::GetRuntimeType() const {
    return GetNode()->GetGenericType();
}

bool IsNumericType(NUdf::TDataTypeId typeId);
bool IsCommonStringType(NUdf::TDataTypeId typeId);
bool IsDateType(NUdf::TDataTypeId typeId);
bool IsTzDateType(NUdf::TDataTypeId typeId);
bool IsIntervalType(NUdf::TDataTypeId typeId);

enum class EValueRepresentation {
    Embedded = 0,
    String = 1,
    Boxed = 2,
    Any = String | Boxed
};

EValueRepresentation GetValueRepresentation(const TType* type);
EValueRepresentation GetValueRepresentation(NUdf::TDataTypeId typeId);

TArrayRef<TType* const> GetWideComponents(const TFlowType* type);
TArrayRef<TType* const> GetWideComponents(const TStreamType* type);
TArrayRef<TType* const> GetWideComponents(const TType* type);

inline ui32 GetWideComponentsCount(const TFlowType* type) {
    return (ui32)GetWideComponents(type).size();
}

inline ui32 GetWideComponentsCount(const TStreamType* type) {
    return (ui32)GetWideComponents(type).size();
}

inline ui32 GetWideComponentsCount(const TType* type) {
    return (ui32)GetWideComponents(type).size();
}

template <TType::EKind SingularKind>
TSingularType<SingularKind>* TSingularType<SingularKind>::Create(TTypeType* type, const TTypeEnvironment& env) {
    return ::new (env.Allocate<TSingularType<SingularKind>>()) TSingularType<SingularKind>(type);
}

template <TType::EKind SingularKind>
bool TSingularType<SingularKind>::IsSameType(const TSingularType<SingularKind>& typeToCompare) const {
    Y_UNUSED(typeToCompare);
    return true;
}

template <TType::EKind SingularKind>
size_t TSingularType<SingularKind>::CalcHash() const {
    return IntHash((size_t)SingularKind);
}

template <TType::EKind SingularKind>
bool TSingularType<SingularKind>::IsConvertableTo(const TSingularType<SingularKind>& typeToCompare, bool ignoreTagged) const {
    Y_UNUSED(ignoreTagged);
    return IsSameType(typeToCompare);
}

template <TType::EKind SingularKind>
TNode* TSingularType<SingularKind>::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TSingularType<SingularKind>*>(this);
}

template <TType::EKind SingularKind>
void TSingularType<SingularKind>::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

template <TType::EKind SingularKind>
TSingular<SingularKind>* TSingular<SingularKind>::Create(const TTypeEnvironment& env) {
    return ::new (env.Allocate<TSingular<SingularKind>>()) TSingular<SingularKind>(env);
}

template <TType::EKind SingularKind>
bool TSingular<SingularKind>::Equals(const TSingular<SingularKind>& nodeToCompare) const {
    Y_UNUSED(nodeToCompare);
    return true;
}

template <TType::EKind SingularKind>
TNode* TSingular<SingularKind>::DoCloneOnCallableWrite(const TTypeEnvironment& env) const {
    Y_UNUSED(env);
    return const_cast<TSingular<SingularKind>*>(this);
}

template <TType::EKind SingularKind>
void TSingular<SingularKind>::DoFreeze(const TTypeEnvironment& env) {
    Y_UNUSED(env);
}

struct THasherTType {
    inline size_t operator()(const TTypeBase& t) const noexcept {
        return t.CalcHash();
    }
};

struct TEqualTType {
    inline bool operator()(const TTypeBase& lhs, const TTypeBase& rhs) const noexcept {
        return lhs.IsSameType(rhs);
    }
};

} // namespace NKikimr::NMiniKQL
