#pragma once

#include "yql_ast.h"
#include "yql_expr_types.h"
#include "yql_type_string.h"
#include "yql_expr_builder.h"
#include "yql_gc_nodes.h"
#include "yql_constraint.h"
#include "yql_pos_handle.h"

#include <ydb/library/yql/core/url_lister/interface/url_lister_manager.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>

#include <library/cpp/yson/node/node.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>
#include <library/cpp/enumbitset/enumbitset.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/generic/array_ref.h>
#include <util/generic/deque.h>
#include <util/generic/cast.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/set.h>
#include <util/generic/bt_exception.h>
#include <util/generic/algorithm.h>
#include <util/digest/murmur.h>

#include <algorithm>
#include <unordered_set>
#include <unordered_map>
#include <span>
#include <stack>

//#define YQL_CHECK_NODES_CONSISTENCY
#ifdef YQL_CHECK_NODES_CONSISTENCY
    #define ENSURE_NOT_DELETED \
        YQL_ENSURE(!Dead(), "Access to dead node # " << UniqueId_ << ": " << Type_ << " '" << ContentUnchecked() << "'");
    #define ENSURE_NOT_FROZEN \
        YQL_ENSURE(!Frozen(), "Change in frozen node # " << UniqueId_ << ": " << Type_ << " '" << ContentUnchecked() << "'");
    #define ENSURE_NOT_FROZEN_CTX \
        YQL_ENSURE(!Frozen, "Change in frozen expr context.");
#else
    #define ENSURE_NOT_DELETED Y_DEBUG_ABORT_UNLESS(!Dead(), "Access to dead node # %lu: %d '%s'", UniqueId_, (int)Type_, TString(ContentUnchecked()).data());
    #define ENSURE_NOT_FROZEN Y_DEBUG_ABORT_UNLESS(!Frozen());
    #define ENSURE_NOT_FROZEN_CTX Y_DEBUG_ABORT_UNLESS(!Frozen);
#endif

namespace NYql {

using NUdf::EDataSlot;

class TUnitExprType;
class TMultiExprType;
class TTupleExprType;
class TStructExprType;
class TItemExprType;
class TListExprType;
class TStreamExprType;
class TDataExprType;
class TPgExprType;
class TWorldExprType;
class TOptionalExprType;
class TCallableExprType;
class TResourceExprType;
class TTypeExprType;
class TDictExprType;
class TVoidExprType;
class TNullExprType;
class TGenericExprType;
class TTaggedExprType;
class TErrorExprType;
class TVariantExprType;
class TStreamExprType;
class TFlowExprType;
class TEmptyListExprType;
class TEmptyDictExprType;
class TBlockExprType;
class TScalarExprType;

const size_t DefaultMistypeDistance = 3;
const TString YqlVirtualPrefix = "_yql_virtual_";

extern const TStringBuf ZeroString;

struct TTypeAnnotationVisitor {
    virtual ~TTypeAnnotationVisitor() = default;

    virtual void Visit(const TUnitExprType& type) = 0;
    virtual void Visit(const TMultiExprType& type) = 0;
    virtual void Visit(const TTupleExprType& type) = 0;
    virtual void Visit(const TStructExprType& type) = 0;
    virtual void Visit(const TItemExprType& type) = 0;
    virtual void Visit(const TListExprType& type) = 0;
    virtual void Visit(const TStreamExprType& type) = 0;
    virtual void Visit(const TFlowExprType& type) = 0;
    virtual void Visit(const TDataExprType& type) = 0;
    virtual void Visit(const TPgExprType& type) = 0;
    virtual void Visit(const TWorldExprType& type) = 0;
    virtual void Visit(const TOptionalExprType& type) = 0;
    virtual void Visit(const TCallableExprType& type) = 0;
    virtual void Visit(const TResourceExprType& type) = 0;
    virtual void Visit(const TTypeExprType& type) = 0;
    virtual void Visit(const TDictExprType& type) = 0;
    virtual void Visit(const TVoidExprType& type) = 0;
    virtual void Visit(const TNullExprType& type) = 0;
    virtual void Visit(const TGenericExprType& type) = 0;
    virtual void Visit(const TTaggedExprType& type) = 0;
    virtual void Visit(const TErrorExprType& type) = 0;
    virtual void Visit(const TVariantExprType& type) = 0;
    virtual void Visit(const TEmptyListExprType& type) = 0;
    virtual void Visit(const TEmptyDictExprType& type) = 0;
    virtual void Visit(const TBlockExprType& type) = 0;
    virtual void Visit(const TScalarExprType& type) = 0;
};

enum ETypeAnnotationFlags : ui32 {
    TypeNonComposable = 0x01,
    TypeNonPersistable = 0x02,
    TypeNonComputable = 0x04,
    TypeNonInspectable = 0x08,
    TypeNonHashable = 0x10,
    TypeNonEquatable = 0x20,
    TypeNonComparable = 0x40,
    TypeHasNull = 0x80,
    TypeHasOptional = 0x100,
    TypeHasManyValues = 0x200,
    TypeHasBareYson = 0x400,
    TypeHasNestedOptional = 0x800,
    TypeNonPresortable = 0x1000,
    TypeHasDynamicSize = 0x2000,
    TypeNonComparableInternal = 0x4000,
};

const ui64 TypeHashMagic = 0x10000;

inline ui64 StreamHash(const void* buffer, size_t size, ui64 seed) {
    return MurmurHash(buffer, size, seed);
}

inline ui64 StreamHash(ui64 value, ui64 seed) {
    return MurmurHash(&value, sizeof(value), seed);
}

void ReportError(TExprContext& ctx, const TIssue& issue);

class TTypeAnnotationNode {
protected:
    TTypeAnnotationNode(ETypeAnnotationKind kind, ui32 flags, ui64 hash)
        : Kind(kind)
        , Flags(flags)
        , Hash(hash)
    {
    }

public:
    virtual ~TTypeAnnotationNode() = default;

    template <typename T>
    const T* Cast() const {
        static_assert(std::is_base_of<TTypeAnnotationNode, T>::value,
                      "Should be derived from TTypeAnnotationNode");

        const auto ret = dynamic_cast<const T*>(this);
        YQL_ENSURE(ret, "Cannot cast type " << *this << " to " << ETypeAnnotationKind(T::KindValue));
        return ret;
    }

    template <typename T>
    const T* UserCast(TPosition pos, TExprContext& ctx) const {
        static_assert(std::is_base_of<TTypeAnnotationNode, T>::value,
                      "Should be derived from TTypeAnnotationNode");

        const auto ret = dynamic_cast<const T*>(this);
        if (!ret) {
            ReportError(ctx, TIssue(pos, TStringBuilder() << "Cannot cast type " << *this << " to " << ETypeAnnotationKind(T::KindValue)));
        }

        return ret;
    }

    ETypeAnnotationKind GetKind() const {
        return Kind;
    }

    bool IsComposable() const {
        return (GetFlags() & TypeNonComposable) == 0;
    }

    bool IsPersistable() const {
        return (GetFlags() & TypeNonPersistable) == 0;
    }

    bool IsComputable() const {
        return (GetFlags() & TypeNonComputable) == 0;
    }

    bool IsInspectable() const {
        return (GetFlags() & TypeNonInspectable) == 0;
    }

    bool IsHashable() const {
        return IsPersistable() && (GetFlags() & TypeNonHashable) == 0;
    }

    bool IsEquatable() const {
        return IsPersistable() && (GetFlags() & TypeNonEquatable) == 0;
    }

    bool IsComparable() const {
        return IsPersistable() && (GetFlags() & TypeNonComparable) == 0;
    }

    bool IsComparableInternal() const {
        return IsPersistable() && (GetFlags() & TypeNonComparableInternal) == 0;
    }

    bool HasNull() const {
        return (GetFlags() & TypeHasNull) != 0;
    }

    bool HasOptional() const {
        return (GetFlags() & TypeHasOptional) != 0;
    }

    bool HasNestedOptional() const {
        return (GetFlags() & TypeHasNestedOptional) != 0;
    }

    bool HasOptionalOrNull() const {
        return (GetFlags() & (TypeHasOptional | TypeHasNull)) != 0;
    }

    bool IsOptionalOrNull() const {
        auto kind = GetKind();
        return kind == ETypeAnnotationKind::Optional || kind == ETypeAnnotationKind::Null || kind == ETypeAnnotationKind::Pg;
    }

    bool IsBlockOrScalar() const {
        return IsBlock() || IsScalar();
    }

    bool IsBlock() const {
        return GetKind() == ETypeAnnotationKind::Block;
    }

    bool IsScalar() const {
        return GetKind() == ETypeAnnotationKind::Scalar;
    }

    bool HasFixedSizeRepr() const {
        return (GetFlags() & (TypeHasDynamicSize | TypeNonPersistable | TypeNonComputable)) == 0;
    }

    bool IsSingleton() const {
        return (GetFlags() & TypeHasManyValues) == 0;
    }

    bool HasBareYson() const {
        return (GetFlags() & TypeHasBareYson) != 0;
    }

    bool IsPresortSupported() const {
        return (GetFlags() & TypeNonPresortable) == 0;
    }

    ui32 GetFlags() const {
        return Flags;
    }

    ui64 GetHash() const {
        return Hash;
    }

    bool Equals(const TTypeAnnotationNode& node) const;
    void Accept(TTypeAnnotationVisitor& visitor) const;

    void Out(IOutputStream& out) const {
        out << FormatType(this);
    }

    struct THash {
        size_t operator()(const TTypeAnnotationNode* node) const {
            return node->GetHash();
        }
    };

    struct TEqual {
        bool operator()(const TTypeAnnotationNode* one, const TTypeAnnotationNode* two) const {
            return one->Equals(*two);
        }
    };

    typedef std::vector<const TTypeAnnotationNode*> TListType;
    typedef std::span<const TTypeAnnotationNode*> TSpanType;
protected:
    template <typename T>
    static ui32 CombineFlags(const T& items) {
        ui32 flags = 0;
        for (auto& item : items) {
            flags |= item->GetFlags();
        }

        return flags;
    }

private:
    const ETypeAnnotationKind Kind;
    const ui32 Flags;
    const ui64 Hash;
};

class TUnitExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Unit;

    TUnitExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue,
            TypeNonComputable | TypeNonPersistable, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::Unit;
    }

    bool operator==(const TUnitExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

class TTupleExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Tuple;

    TTupleExprType(ui64 hash, const TTypeAnnotationNode::TListType& items)
        : TTypeAnnotationNode(KindValue, CombineFlags(items), hash)
        , Items(items)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode::TListType& items) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Tuple;
        hash = StreamHash(items.size(), hash);
        for (const auto& item : items) {
            hash = StreamHash(item->GetHash(), hash);
        }

        return hash;
    }

    size_t GetSize() const {
        return Items.size();
    }

    const TTypeAnnotationNode::TListType& GetItems() const {
        return Items;
    }

    bool operator==(const TTupleExprType& other) const {
        if (GetSize() != other.GetSize()) {
            return false;
        }

        for (ui32 i = 0, e = GetSize(); i < e; ++i) {
            if (GetItems()[i] != other.GetItems()[i]) {
                return false;
            }
        }

        return true;
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

private:
    TTypeAnnotationNode::TListType Items;
};

class TMultiExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Multi;

    TMultiExprType(ui64 hash, const TTypeAnnotationNode::TListType& items)
        : TTypeAnnotationNode(KindValue, CombineFlags(items), hash)
        , Items(items)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode::TListType& items) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Multi;
        hash = StreamHash(items.size(), hash);
        for (const auto& item : items) {
            hash = StreamHash(item->GetHash(), hash);
        }

        return hash;
    }

    size_t GetSize() const {
        return Items.size();
    }

    const TTypeAnnotationNode::TListType& GetItems() const {
        return Items;
    }

    bool operator==(const TMultiExprType& other) const {
        if (GetSize() != other.GetSize()) {
            return false;
        }

        for (ui32 i = 0, e = GetSize(); i < e; ++i) {
            if (GetItems()[i] != other.GetItems()[i]) {
                return false;
            }
        }

        return true;
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

private:
    TTypeAnnotationNode::TListType Items;
};

struct TExprContext;


bool ValidateName(TPosition position, TStringBuf name, TStringBuf descr, TExprContext& ctx);
bool ValidateName(TPositionHandle position, TStringBuf name, TStringBuf descr, TExprContext& ctx);

class TItemExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Item;

    TItemExprType(ui64 hash, const TStringBuf& name, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags(), hash)
        , Name(name)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TStringBuf& name, const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Item;
        hash = StreamHash(name.size(), hash);
        hash = StreamHash(name.data(), name.size(), hash);
        return StreamHash(itemType->GetHash(), hash);
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

    const TStringBuf& GetName() const {
        return Name;
    }

    TStringBuf GetCleanName(bool isVirtual) const;

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TItemExprType& other) const {
        return GetName() == other.GetName() && GetItemType() == other.GetItemType();
    }

    const TItemExprType* GetCleanItem(bool isVirtual, TExprContext& ctx) const;

private:
    const TStringBuf Name;
    const TTypeAnnotationNode* ItemType;
};

class TStructExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Struct;

    struct TItemLess {
        bool operator()(const TItemExprType* x, const TItemExprType* y) const {
            return x->GetName() < y->GetName();
        };

        bool operator()(const TItemExprType* x, const TStringBuf& y) const {
            return x->GetName() < y;
        };

        bool operator()(const TStringBuf& x, const TItemExprType* y) const {
            return x < y->GetName();
        };
    };

    TStructExprType(ui64 hash, const TVector<const TItemExprType*>& items)
        : TTypeAnnotationNode(KindValue, TypeNonComparable | CombineFlags(items), hash)
        , Items(items)
    {
    }

    static ui64 MakeHash(const TVector<const TItemExprType*>& items) {
        Y_DEBUG_ABORT_UNLESS(IsSorted(items.begin(), items.end(), TItemLess()));
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Struct;
        hash = StreamHash(items.size(), hash);
        for (const auto& item : items) {
            hash = StreamHash(item->GetHash(), hash);
        }

        return hash;
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

    size_t GetSize() const {
        return Items.size();
    }

    const TVector<const TItemExprType*>& GetItems() const {
        return Items;
    }

    TMaybe<ui32> FindItem(const TStringBuf& name) const {
        auto it = LowerBound(Items.begin(), Items.end(), name, TItemLess());
        if (it == Items.end() || (*it)->GetName() != name) {
            return TMaybe<ui32>();
        }

        return it - Items.begin();
    }

    TMaybe<ui32> FindItemI(const TStringBuf& name, bool* isVirtual) const {
        for (ui32 v = 0; v < 2; ++v) {
            if (isVirtual) {
                *isVirtual = v > 0;
            }

            auto nameToSearch = (v ? YqlVirtualPrefix : "") + name;
            auto strict = FindItem(nameToSearch);
            if (strict) {
                return strict;
            }

            TMaybe<ui32> ret;
            for (ui32 i = 0; i < Items.size(); ++i) {
                if (AsciiEqualsIgnoreCase(nameToSearch, Items[i]->GetName())) {
                    if (ret) {
                        return Nothing();
                    }

                    ret = i;
                }
            }

            if (ret) {
                return ret;
            }
        }

        return Nothing();
    }

    const TTypeAnnotationNode* FindItemType(const TStringBuf& name) const {
        const auto it = LowerBound(Items.begin(), Items.end(), name, TItemLess());
        if (it == Items.end() || (*it)->GetName() != name) {
            return nullptr;
        }

        return (*it)->GetItemType();
    }

    TMaybe<TStringBuf> FindMistype(const TStringBuf& name) const {
        for (const auto& item: Items) {
            if (NLevenshtein::Distance(name, item->GetName()) < DefaultMistypeDistance) {
                return item->GetName();
            }
        }
        return TMaybe<TStringBuf>();
    }

    bool operator==(const TStructExprType& other) const {
        if (GetSize() != other.GetSize()) {
            return false;
        }

        for (ui32 i = 0, e = GetSize(); i < e; ++i) {
            if (GetItems()[i] != other.GetItems()[i]) {
                return false;
            }
        }

        return true;
    }


    TString ToString() const {
        TStringBuilder sb;

        for (std::size_t i = 0; i < Items.size(); i++) {
            sb << i << ": " << Items[i]->GetName() << "(" << FormatType(Items[i]->GetItemType()) << ")";
            if (i != Items.size() - 1) {
                sb << ", ";
            }
        }

        return sb;
    }

private:
    TVector<const TItemExprType*> Items;
};

class TListExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::List;

    TListExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags() | TypeHasDynamicSize, hash)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::List;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TListExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TStreamExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Stream;

    TStreamExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags() | TypeNonPersistable, hash)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Stream;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TStreamExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TFlowExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Flow;

    TFlowExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags() | TypeNonPersistable, hash)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Flow;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TFlowExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TBlockExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Block;

    TBlockExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags() | TypeNonPersistable, hash)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Block;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TBlockExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TScalarExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Scalar;

    TScalarExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, itemType->GetFlags() | TypeNonPersistable, hash)
        , ItemType(itemType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Scalar;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TScalarExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TDataExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Data;

    TDataExprType(ui64 hash, EDataSlot slot)
        : TTypeAnnotationNode(KindValue, GetFlags(slot), hash)
        , Slot(slot)
    {
    }

    static ui32 GetFlags(EDataSlot slot) {
        ui32 ret = TypeHasManyValues;
        auto props = NUdf::GetDataTypeInfo(slot).Features;
        if (!(props & NUdf::CanHash)) {
            ret |= TypeNonHashable;
        }

        if (!(props & NUdf::CanEquate)) {
            ret |= TypeNonEquatable;
        }

        if (!(props & NUdf::CanCompare)) {
            ret |= TypeNonComparable;
            ret |= TypeNonComparableInternal;
        }

        if (slot == NUdf::EDataSlot::Yson) {
            ret |= TypeHasBareYson;
        }

        if (props & NUdf::StringType) {
            ret |= TypeHasDynamicSize;
        }

        return ret;
    }

    static ui64 MakeHash(EDataSlot slot) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Data;
        auto dataType = NUdf::GetDataTypeInfo(slot).Name;
        hash = StreamHash(dataType.size(), hash);
        return StreamHash(dataType.data(), dataType.size(), hash);
    }

    EDataSlot GetSlot() const {
        return Slot;
    }

    TStringBuf GetName() const {
        return NUdf::GetDataTypeInfo(Slot).Name;
    }

    bool operator==(const TDataExprType& other) const {
        return Slot == other.Slot;
    }

private:
    EDataSlot Slot;
};

class TDataExprParamsType : public TDataExprType {
public:
    TDataExprParamsType(ui64 hash, EDataSlot slot, const TStringBuf& one, const TStringBuf& two)
        : TDataExprType(hash, slot), One(one), Two(two)
    {}

    static ui64 MakeHash(EDataSlot slot, const TStringBuf& one, const TStringBuf& two) {
        auto hash = TDataExprType::MakeHash(slot);
        hash = StreamHash(one.size(), hash);
        hash = StreamHash(one.data(), one.size(), hash);
        hash = StreamHash(two.size(), hash);
        hash = StreamHash(two.data(), two.size(), hash);
        return hash;
    }

    const TStringBuf& GetParamOne() const {
        return One;
    }

    const TStringBuf& GetParamTwo() const {
        return Two;
    }

    bool operator==(const TDataExprParamsType& other) const {
        return GetSlot() == other.GetSlot() && GetParamOne() == other.GetParamOne() && GetParamTwo() == other.GetParamTwo();
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

private:
    const TStringBuf One, Two;
};

class TPgExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Pg;

    // TODO: TypeHasDynamicSize for Pg types
    TPgExprType(ui64 hash, ui32 typeId)
        : TTypeAnnotationNode(KindValue, GetFlags(typeId), hash)
        , TypeId(typeId)
    {
    }

    static ui64 MakeHash(ui32 typeId) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Pg;
        return StreamHash(typeId, hash);
    }

    const TString& GetName() const;

    ui32 GetId() const {
        return TypeId;
    }

    bool operator==(const TPgExprType& other) const {
        return TypeId == other.TypeId;
    }

private:
    ui32 GetFlags(ui32 typeId);

private:
    ui32 TypeId;
};

class TWorldExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::World;

    TWorldExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue,
            TypeNonComposable | TypeNonComputable | TypeNonPersistable | TypeNonInspectable, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::World;
    }

    bool operator==(const TWorldExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

class TOptionalExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Optional;

    TOptionalExprType(ui64 hash, const TTypeAnnotationNode* itemType)
        : TTypeAnnotationNode(KindValue, GetFlags(itemType), hash)
        , ItemType(itemType)
    {
    }

    static ui32 GetFlags(const TTypeAnnotationNode* itemType) {
        auto ret = TypeHasOptional | itemType->GetFlags();
        if (itemType->GetKind() == ETypeAnnotationKind::Data &&
            itemType->Cast<TDataExprType>()->GetSlot() == NUdf::EDataSlot::Yson) {
            ret = ret & ~TypeHasBareYson;
        }
        if (itemType->IsOptionalOrNull()) {
            ret |= TypeHasNestedOptional;
        }

        return ret;
    }

    static ui64 MakeHash(const TTypeAnnotationNode* itemType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Optional;
        return StreamHash(itemType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetItemType() const {
        return ItemType;
    }

    bool operator==(const TOptionalExprType& other) const {
        return GetItemType() == other.GetItemType();
    }

private:
    const TTypeAnnotationNode* ItemType;
};

class TVariantExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Variant;

    TVariantExprType(ui64 hash, const TTypeAnnotationNode* underlyingType)
        : TTypeAnnotationNode(KindValue, MakeFlags(underlyingType), hash)
        , UnderlyingType(underlyingType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* underlyingType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Variant;
        return StreamHash(underlyingType->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetUnderlyingType() const {
        return UnderlyingType;
    }

    bool operator==(const TVariantExprType& other) const {
        return GetUnderlyingType() == other.GetUnderlyingType();
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

    static ui32 MakeFlags(const TTypeAnnotationNode* underlyingType);

private:
    const TTypeAnnotationNode* UnderlyingType;
};

class TTypeExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Type;

    TTypeExprType(ui64 hash, const TTypeAnnotationNode* type)
        : TTypeAnnotationNode(KindValue, TypeNonPersistable | TypeNonComputable, hash)
        , Type(type)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* type) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Type;
        return StreamHash(type->GetHash(), hash);
    }

    const TTypeAnnotationNode* GetType() const {
        return Type;
    }

    bool operator==(const TTypeExprType& other) const {
        return GetType() == other.GetType();
    }

private:
    const TTypeAnnotationNode* Type;
};

class TDictExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Dict;

    TDictExprType(ui64 hash, const TTypeAnnotationNode* keyType, const TTypeAnnotationNode* payloadType)
        : TTypeAnnotationNode(KindValue, TypeNonComparable | TypeHasDynamicSize |
                              keyType->GetFlags() | payloadType->GetFlags(), hash)
        , KeyType(keyType)
        , PayloadType(payloadType)
    {
    }

    static ui64 MakeHash(const TTypeAnnotationNode* keyType, const TTypeAnnotationNode* payloadType) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Dict;
        return StreamHash(StreamHash(keyType->GetHash(), hash), payloadType->GetHash());
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

    const TTypeAnnotationNode* GetKeyType() const {
        return KeyType;
    }

    const TTypeAnnotationNode* GetPayloadType() const {
        return PayloadType;
    }

    bool operator==(const TDictExprType& other) const {
        return GetKeyType() == other.GetKeyType() &&
            GetPayloadType() == other.GetPayloadType();
    }

private:
    const TTypeAnnotationNode* KeyType;
    const TTypeAnnotationNode* PayloadType;
};

class TVoidExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Void;

    TVoidExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue, 0, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::Void;
    }

    bool operator==(const TVoidExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

class TNullExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Null;

    TNullExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue, TypeHasNull, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::Null;
    }

    bool operator==(const TNullExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

struct TArgumentFlags {
    enum {
        AutoMap = 0x01,
    };
};

class TCallableExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Callable;

    struct TArgumentInfo {
        const TTypeAnnotationNode* Type = nullptr;
        TStringBuf Name;
        ui64 Flags = 0;

        bool operator==(const TArgumentInfo& other) const {
            return Type == other.Type && Name == other.Name && Flags == other.Flags;
        }

        bool operator!=(const TArgumentInfo& other) const {
            return !(*this == other);
        }
    };

    TCallableExprType(ui64 hash, const TTypeAnnotationNode* returnType, const TVector<TArgumentInfo>& arguments
        , size_t optionalArgumentsCount, const TStringBuf& payload)
        : TTypeAnnotationNode(KindValue, MakeFlags(returnType), hash)
        , ReturnType(returnType)
        , Arguments(arguments)
        , OptionalArgumentsCount(optionalArgumentsCount)
        , Payload(payload)
    {
        for (ui32 i = 0; i < Arguments.size(); ++i) {
            const auto& arg = Arguments[i];
            if (!arg.Name.empty()) {
                IndexByName.insert({ arg.Name, i });
            }
        }
    }

    static ui64 MakeHash(const TTypeAnnotationNode* returnType, const TVector<TArgumentInfo>& arguments
        , size_t optionalArgumentsCount, const TStringBuf& payload) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Callable;
        hash = StreamHash(returnType->GetHash(), hash);
        hash = StreamHash(arguments.size(), hash);
        for (const auto& arg : arguments) {
            hash = StreamHash(arg.Name.size(), hash);
            hash = StreamHash(arg.Name.data(), arg.Name.size(), hash);
            hash = StreamHash(arg.Flags, hash);
            hash = StreamHash(arg.Type->GetHash(), hash);
        }

        hash = StreamHash(optionalArgumentsCount, hash);
        hash = StreamHash(payload.size(), hash);
        hash = StreamHash(payload.data(), payload.size(), hash);
        return hash;
    }

    const TTypeAnnotationNode* GetReturnType() const {
        return ReturnType;
    }

    size_t GetOptionalArgumentsCount() const {
        return OptionalArgumentsCount;
    }

    const TStringBuf& GetPayload() const {
        return Payload;
    }

    size_t GetArgumentsSize() const {
        return Arguments.size();
    }

    const TVector<TArgumentInfo>& GetArguments() const {
        return Arguments;
    }

    bool operator==(const TCallableExprType& other) const {
        if (GetArgumentsSize() != other.GetArgumentsSize()) {
            return false;
        }

        if (GetOptionalArgumentsCount() != other.GetOptionalArgumentsCount()) {
            return false;
        }

        if (GetReturnType() != other.GetReturnType()) {
            return false;
        }

        for (ui32 i = 0, e = GetArgumentsSize(); i < e; ++i) {
            if (GetArguments()[i] != other.GetArguments()[i]) {
                return false;
            }
        }

        return true;
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

    TMaybe<ui32> ArgumentIndexByName(const TStringBuf& name) const {
        auto it = IndexByName.find(name);
        if (it == IndexByName.end()) {
            return {};
        }

        return it->second;
    }

private:
    static ui32 MakeFlags(const TTypeAnnotationNode* returnType) {
        ui32 flags = TypeNonPersistable;
        flags |= returnType->GetFlags();
        return flags;
    }

private:
    const TTypeAnnotationNode* ReturnType;
    TVector<TArgumentInfo> Arguments;
    const size_t OptionalArgumentsCount;
    const TStringBuf Payload;
    THashMap<TStringBuf, ui32> IndexByName;
};

class TGenericExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Generic;

    TGenericExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue, TypeNonComputable, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::Generic;
    }

    bool operator==(const TGenericExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

class TResourceExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Resource;

    TResourceExprType(ui64 hash, const TStringBuf& tag)
        : TTypeAnnotationNode(KindValue, TypeNonPersistable | TypeHasManyValues, hash)
        , Tag(tag)
    {}

    static ui64 MakeHash(const TStringBuf& tag) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Resource;
        hash = StreamHash(tag.size(), hash);
        return StreamHash(tag.data(), tag.size(), hash);
    }

    const TStringBuf& GetTag() const {
        return Tag;
    }

    bool operator==(const TResourceExprType& other) const {
        return Tag == other.Tag;
    }

private:
    const TStringBuf Tag;
};

class TTaggedExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Tagged;

    TTaggedExprType(ui64 hash, const TTypeAnnotationNode* baseType, const TStringBuf& tag)
        : TTypeAnnotationNode(KindValue, baseType->GetFlags(), hash)
        , BaseType(baseType)
        , Tag(tag)
    {}

    static ui64 MakeHash(const TTypeAnnotationNode* baseType, const TStringBuf& tag) {
        ui64 hash = TypeHashMagic | (ui64)ETypeAnnotationKind::Tagged;
        hash = StreamHash(baseType->GetHash(), hash);
        hash = StreamHash(tag.size(), hash);
        return StreamHash(tag.data(), tag.size(), hash);
    }

    const TStringBuf& GetTag() const {
        return Tag;
    }

    const TTypeAnnotationNode* GetBaseType() const {
        return BaseType;
    }

    bool operator==(const TTaggedExprType& other) const {
        return Tag == other.Tag && GetBaseType() == other.GetBaseType();
    }

    bool Validate(TPosition position, TExprContext& ctx) const;
    bool Validate(TPositionHandle position, TExprContext& ctx) const;

private:
    const TTypeAnnotationNode* BaseType;
    const TStringBuf Tag;
};

class TErrorExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::Error;

    TErrorExprType(ui64 hash, const TIssue& error)
        : TTypeAnnotationNode(KindValue, 0, hash)
        , Error(error)
    {}

    static ui64 MakeHash(const TIssue& error) {
        return error.Hash();
    }

    const TIssue& GetError() const {
        return Error;
    }

    bool operator==(const TErrorExprType& other) const {
        return Error == other.Error;
    }

private:
    const TIssue Error;
};

class TEmptyListExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::EmptyList;

    TEmptyListExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue, 0, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::EmptyList;
    }

    bool operator==(const TEmptyListExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

class TEmptyDictExprType : public TTypeAnnotationNode {
public:
    static constexpr ETypeAnnotationKind KindValue = ETypeAnnotationKind::EmptyDict;

    TEmptyDictExprType(ui64 hash)
        : TTypeAnnotationNode(KindValue, 0, hash)
    {
    }

    static ui64 MakeHash() {
        return TypeHashMagic | (ui64)ETypeAnnotationKind::EmptyDict;
    }

    bool operator==(const TEmptyDictExprType& other) const {
        Y_UNUSED(other);
        return true;
    }
};

inline bool TTypeAnnotationNode::Equals(const TTypeAnnotationNode& node) const {
    if (this == &node) {
        return true;
    }

    if (Hash != node.GetHash()) {
        return false;
    }

    if (Kind != node.GetKind()) {
        return false;
    }

    switch (Kind) {
    case ETypeAnnotationKind::Unit:
        return static_cast<const TUnitExprType&>(*this) == static_cast<const TUnitExprType&>(node);

    case ETypeAnnotationKind::Tuple:
        return static_cast<const TTupleExprType&>(*this) == static_cast<const TTupleExprType&>(node);

    case ETypeAnnotationKind::Struct:
        return static_cast<const TStructExprType&>(*this) == static_cast<const TStructExprType&>(node);

    case ETypeAnnotationKind::Item:
        return static_cast<const TItemExprType&>(*this) == static_cast<const TItemExprType&>(node);

    case ETypeAnnotationKind::List:
        return static_cast<const TListExprType&>(*this) == static_cast<const TListExprType&>(node);

    case ETypeAnnotationKind::Data:
        return static_cast<const TDataExprType&>(*this) == static_cast<const TDataExprType&>(node);

    case ETypeAnnotationKind::Pg:
        return static_cast<const TPgExprType&>(*this) == static_cast<const TPgExprType&>(node);

    case ETypeAnnotationKind::World:
        return static_cast<const TWorldExprType&>(*this) == static_cast<const TWorldExprType&>(node);

    case ETypeAnnotationKind::Optional:
        return static_cast<const TOptionalExprType&>(*this) == static_cast<const TOptionalExprType&>(node);

    case ETypeAnnotationKind::Type:
        return static_cast<const TTypeExprType&>(*this) == static_cast<const TTypeExprType&>(node);

    case ETypeAnnotationKind::Dict:
        return static_cast<const TDictExprType&>(*this) == static_cast<const TDictExprType&>(node);

    case ETypeAnnotationKind::Void:
        return static_cast<const TVoidExprType&>(*this) == static_cast<const TVoidExprType&>(node);

    case ETypeAnnotationKind::Null:
        return static_cast<const TNullExprType&>(*this) == static_cast<const TNullExprType&>(node);

    case ETypeAnnotationKind::Callable:
        return static_cast<const TCallableExprType&>(*this) == static_cast<const TCallableExprType&>(node);

    case ETypeAnnotationKind::Generic:
        return static_cast<const TGenericExprType&>(*this) == static_cast<const TGenericExprType&>(node);

    case ETypeAnnotationKind::Resource:
        return static_cast<const TResourceExprType&>(*this) == static_cast<const TResourceExprType&>(node);

    case ETypeAnnotationKind::Tagged:
        return static_cast<const TTaggedExprType&>(*this) == static_cast<const TTaggedExprType&>(node);

    case ETypeAnnotationKind::Error:
        return static_cast<const TErrorExprType&>(*this) == static_cast<const TErrorExprType&>(node);

    case ETypeAnnotationKind::Variant:
        return static_cast<const TVariantExprType&>(*this) == static_cast<const TVariantExprType&>(node);

    case ETypeAnnotationKind::Stream:
        return static_cast<const TStreamExprType&>(*this) == static_cast<const TStreamExprType&>(node);

    case ETypeAnnotationKind::Flow:
        return static_cast<const TFlowExprType&>(*this) == static_cast<const TFlowExprType&>(node);

    case ETypeAnnotationKind::EmptyList:
        return static_cast<const TEmptyListExprType&>(*this) == static_cast<const TEmptyListExprType&>(node);

    case ETypeAnnotationKind::EmptyDict:
        return static_cast<const TEmptyDictExprType&>(*this) == static_cast<const TEmptyDictExprType&>(node);

    case ETypeAnnotationKind::Multi:
        return static_cast<const TMultiExprType&>(*this) == static_cast<const TMultiExprType&>(node);

    case ETypeAnnotationKind::Block:
        return static_cast<const TBlockExprType&>(*this) == static_cast<const TBlockExprType&>(node);

    case ETypeAnnotationKind::Scalar:
        return static_cast<const TScalarExprType&>(*this) == static_cast<const TScalarExprType&>(node);

    case ETypeAnnotationKind::LastType:
        YQL_ENSURE(false, "Incorrect type");

    }
    return false;
}

inline void TTypeAnnotationNode::Accept(TTypeAnnotationVisitor& visitor) const {
    switch (Kind) {
    case ETypeAnnotationKind::Unit:
        return visitor.Visit(static_cast<const TUnitExprType&>(*this));
    case ETypeAnnotationKind::Tuple:
        return visitor.Visit(static_cast<const TTupleExprType&>(*this));
    case ETypeAnnotationKind::Struct:
        return visitor.Visit(static_cast<const TStructExprType&>(*this));
    case ETypeAnnotationKind::Item:
        return visitor.Visit(static_cast<const TItemExprType&>(*this));
    case ETypeAnnotationKind::List:
        return visitor.Visit(static_cast<const TListExprType&>(*this));
    case ETypeAnnotationKind::Data:
        return visitor.Visit(static_cast<const TDataExprType&>(*this));
    case ETypeAnnotationKind::Pg:
        return visitor.Visit(static_cast<const TPgExprType&>(*this));
    case ETypeAnnotationKind::World:
        return visitor.Visit(static_cast<const TWorldExprType&>(*this));
    case ETypeAnnotationKind::Optional:
        return visitor.Visit(static_cast<const TOptionalExprType&>(*this));
    case ETypeAnnotationKind::Type:
        return visitor.Visit(static_cast<const TTypeExprType&>(*this));
    case ETypeAnnotationKind::Dict:
        return visitor.Visit(static_cast<const TDictExprType&>(*this));
    case ETypeAnnotationKind::Void:
        return visitor.Visit(static_cast<const TVoidExprType&>(*this));
    case ETypeAnnotationKind::Null:
        return visitor.Visit(static_cast<const TNullExprType&>(*this));
    case ETypeAnnotationKind::Callable:
        return visitor.Visit(static_cast<const TCallableExprType&>(*this));
    case ETypeAnnotationKind::Generic:
        return visitor.Visit(static_cast<const TGenericExprType&>(*this));
    case ETypeAnnotationKind::Resource:
        return visitor.Visit(static_cast<const TResourceExprType&>(*this));
    case ETypeAnnotationKind::Tagged:
        return visitor.Visit(static_cast<const TTaggedExprType&>(*this));
    case ETypeAnnotationKind::Error:
        return visitor.Visit(static_cast<const TErrorExprType&>(*this));
    case ETypeAnnotationKind::Variant:
        return visitor.Visit(static_cast<const TVariantExprType&>(*this));
    case ETypeAnnotationKind::Stream:
        return visitor.Visit(static_cast<const TStreamExprType&>(*this));
    case ETypeAnnotationKind::Flow:
        return visitor.Visit(static_cast<const TFlowExprType&>(*this));
    case ETypeAnnotationKind::EmptyList:
        return visitor.Visit(static_cast<const TEmptyListExprType&>(*this));
    case ETypeAnnotationKind::EmptyDict:
        return visitor.Visit(static_cast<const TEmptyDictExprType&>(*this));
    case ETypeAnnotationKind::Multi:
        return visitor.Visit(static_cast<const TMultiExprType&>(*this));
    case ETypeAnnotationKind::Block:
        return visitor.Visit(static_cast<const TBlockExprType&>(*this));
    case ETypeAnnotationKind::Scalar:
        return visitor.Visit(static_cast<const TScalarExprType&>(*this));
    case ETypeAnnotationKind::LastType:
        YQL_ENSURE(false, "Incorrect type");
    }
}

class TExprNode {
    friend class TExprNodeBuilder;
    friend class TExprNodeReplaceBuilder;
    friend struct TExprContext;

private:
    struct TExprFlags {
        enum : ui16 {
            Default = 0,
            Dead = 0x01,
            Frozen = 0x02,
        };
        static constexpr ui32 FlagsMask = 0x03; // all flags should fit here
    };

public:
    typedef TIntrusivePtr<TExprNode> TPtr;
    typedef std::vector<TPtr> TListType;
    typedef TArrayRef<const TPtr> TChildrenType;

    struct TPtrHash : private std::hash<const TExprNode*> {
        size_t operator()(const TPtr& p) const {
            return std::hash<const TExprNode*>::operator()(p.Get());
        }
    };

#define YQL_EXPR_NODE_TYPE_MAP(xx) \
    xx(List, 0) \
    xx(Atom, 1) \
    xx(Callable, 2) \
    xx(Lambda, 3) \
    xx(Argument, 4) \
    xx(Arguments, 5) \
    xx(World, 7)

    enum EType : ui8 {
        YQL_EXPR_NODE_TYPE_MAP(ENUM_VALUE_GEN)
    };

    static constexpr ui32 TypeMask = 0x07; // all types should fit here

#define YQL_EXPR_NODE_STATE_MAP(xx) \
    xx(Initial, 0) \
    xx(TypeInProgress, 1) \
    xx(TypePending, 2) \
    xx(TypeComplete, 3) \
    xx(ConstrInProgress, 4) \
    xx(ConstrPending, 5) \
    xx(ConstrComplete, 6) \
    xx(ExecutionRequired, 7) \
    xx(ExecutionInProgress, 8) \
    xx(ExecutionPending, 9) \
    xx(ExecutionComplete, 10) \
    xx(Error, 11) \
    xx(Last, 12)

    enum class EState : ui8 {
        YQL_EXPR_NODE_STATE_MAP(ENUM_VALUE_GEN)
    };

    static TPtr GetResult(const TPtr& node) {
        return node->Type() == Callable ? node->Result : node;
    }

    const TExprNode& GetResult() const {
        ENSURE_NOT_DELETED
        return Type() == Callable ? *Result : *this;
    }

    bool HasResult() const {
        ENSURE_NOT_DELETED
        return Type() != Callable || bool(Result);
    }

    void SetResult(TPtr&& result) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Result = std::move(result);
    }

    bool IsCallable(const std::string_view& name) const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Callable && Content() == name;
    }

    bool IsCallable(const std::initializer_list<std::string_view>& names) const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Callable && names.end() != std::find(names.begin(), names.end(), Content());
    }

    template <class TKey>
    bool IsCallable(const THashSet<TKey>& names) const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Callable && names.contains(Content());
    }

    bool IsCallable() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Callable;
    }

    bool IsAtom() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Atom;
    }

    bool IsWorld() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::World;
    }

    bool IsAtom(const std::string_view& content) const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Atom && Content() == content;
    }

    bool IsAtom(const std::initializer_list<std::string_view>& names) const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Atom && names.end() != std::find(names.begin(), names.end(), Content());
    }

    bool IsList() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::List;
    }

    bool IsLambda() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Lambda;
    }

    bool IsArgument() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Argument;
    }

    bool IsArguments() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::Arguments;
    }

    bool IsComposable() const {
        ENSURE_NOT_DELETED
        return !IsLambda() && TypeAnnotation_->IsComposable();
    }

    bool IsPersistable() const {
        ENSURE_NOT_DELETED
        return !IsLambda() && TypeAnnotation_->IsPersistable();
    }

    bool IsComputable() const {
        ENSURE_NOT_DELETED
        return !IsLambda() && TypeAnnotation_->IsComputable();
    }

    bool IsInspectable() const {
        ENSURE_NOT_DELETED
        return !IsLambda() && TypeAnnotation_->IsInspectable();
    }

    bool ForDisclosing() const {
        ENSURE_NOT_DELETED
        return Type() == TExprNode::List && ShallBeDisclosed;
    }

    void SetDisclosing() {
        ENSURE_NOT_DELETED
        Y_ENSURE(Type() == TExprNode::List, "Must be list.");
        ShallBeDisclosed = true;
    }

    ui32 GetFlagsToCompare() const {
        ENSURE_NOT_DELETED
        ui32 ret = Flags();
        if ((ret & TNodeFlags::BinaryContent) == 0) {
            ret |= TNodeFlags::ArbitraryContent | TNodeFlags::MultilineContent;
        }

        return ret;
    }

    TString Dump() const;

    bool StartsExecution() const {
        ENSURE_NOT_DELETED
        return State == EState::ExecutionComplete
            || State == EState::ExecutionInProgress
            || State == EState::ExecutionRequired
            || State == EState::ExecutionPending;
    }

    bool IsComplete() const {
        YQL_ENSURE(HasLambdaScope);
        return !OuterLambda;
    }

    bool IsLiteralList() const {
        YQL_ENSURE(IsList());
        return LiteralList;
    }

    void SetLiteralList(bool literal) {
        YQL_ENSURE(IsList());
        LiteralList = literal;
    }

    void Ref() {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(RefCount_ < Max<ui32>());
        ++RefCount_;
    }

    void UnRef() {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        if (!--RefCount_) {
            Result.Reset();
            Children_.clear();
            Constraints_.Clear();
            MarkDead();
        }
    }

    ui32 UseCount() const { return RefCount_; }
    bool Unique() const { return 1U == UseCount(); }

    bool Dead() const {
        return ExprFlags_ & TExprFlags::Dead;
    }

    TPositionHandle Pos() const {
        ENSURE_NOT_DELETED
        return Position_;
    }

    TPosition Pos(const TExprContext& ctx) const;

    EType Type() const {
        ENSURE_NOT_DELETED
        return (EType)Type_;
    }

    TListType::size_type ChildrenSize() const {
        ENSURE_NOT_DELETED
        return Children_.size();
    }

    TExprNode* Child(ui32 index) const {
        ENSURE_NOT_DELETED
        Y_ENSURE(index < Children_.size(), "index out of range");
        return Children_[index].Get();
    }

    TPtr ChildPtr(ui32 index) const {
        ENSURE_NOT_DELETED
        Y_ENSURE(index < Children_.size(), "index out of range");
        return Children_[index];
    }

    TPtr& ChildRef(ui32 index) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(index < Children_.size(), "index out of range");
        return Children_[index];
    }

    const TExprNode& Head() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return *Children_.front();
    }

    TExprNode& Head() {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return *Children_.front();
    }

    TPtr HeadPtr() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return Children_.front();
    }

    TPtr& HeadRef() {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(!Children_.empty(), "no children");
        return Children_.front();
    }

    const TExprNode& Tail() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return *Children_.back();
    }

    TExprNode& Tail() {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return *Children_.back();
    }

    TPtr TailPtr() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(!Children_.empty(), "no children");
        return Children_.back();
    }

    TPtr& TailRef() {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(!Children_.empty(), "no children");
        return Children_.back();
    }

    TChildrenType Children() const {
        ENSURE_NOT_DELETED
        return TChildrenType(Children_.data(), Children_.size());
    }

    TListType ChildrenList() const {
        ENSURE_NOT_DELETED
        return Children_;
    }

    void ChangeChildrenInplace(TListType&& newChildren) {
        ENSURE_NOT_DELETED
        Children_ = std::move(newChildren);
    }

    template<class F>
    void ForEachChild(const F& visitor) const {
        for (const auto& child : Children_)
            visitor(*child);
    }

    TStringBuf Content() const {
        ENSURE_NOT_DELETED
        return ContentUnchecked();
    }

    ui32 Flags() const {
        ENSURE_NOT_DELETED
        return Flags_;
    }

    void NormalizeAtomFlags(const TExprNode& otherAtom) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(Type_ == Atom && otherAtom.Type_ == Atom, "Expected atoms");
        Y_ENSURE((Flags_ & TNodeFlags::BinaryContent) ==
            (otherAtom.Flags_ & TNodeFlags::BinaryContent), "Mismatch binary atom flags");
        if (!(Flags_ & TNodeFlags::BinaryContent)) {
            Flags_ = Min(Flags_, otherAtom.Flags_);
        }
    }

    ui64 UniqueId() const {
        ENSURE_NOT_DELETED
        return UniqueId_;
    }

    const TConstraintNode* GetConstraint(TStringBuf name) const {
        ENSURE_NOT_DELETED
        Y_ENSURE(static_cast<EState>(State) >= EState::ConstrComplete);
        return Constraints_.GetConstraint(name);
    }

    template <class TConstraintType>
    const TConstraintType* GetConstraint() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(static_cast<EState>(State) >= EState::ConstrComplete);
        return Constraints_.GetConstraint<TConstraintType>();
    }

    const TConstraintNode::TListType& GetAllConstraints() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(static_cast<EState>(State) >= EState::ConstrComplete);
        return Constraints_.GetAllConstraints();
    }

    const TConstraintSet& GetConstraintSet() const {
        ENSURE_NOT_DELETED
        Y_ENSURE(static_cast<EState>(State) >= EState::ConstrComplete);
        return Constraints_;
    }

    void AddConstraint(const TConstraintNode* node) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(static_cast<EState>(State) >= EState::TypeComplete);
        Y_ENSURE(!StartsExecution());
        Constraints_.AddConstraint(node);
        State = EState::ConstrComplete;
    }

    void CopyConstraints(const TExprNode& node) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(static_cast<EState>(State) >= EState::TypeComplete);
        Constraints_ = node.Constraints_;
        State = EState::ConstrComplete;
    }

    void SetConstraints(const TConstraintSet& constraints) {
        ENSURE_NOT_DELETED
        ENSURE_NOT_FROZEN
        Y_ENSURE(static_cast<EState>(State) >= EState::TypeComplete);
        Constraints_ = constraints;
        State = EState::ConstrComplete;
    }

    static TPtr NewAtom(ui64 uniqueId, TPositionHandle pos, const TStringBuf& content, ui32 flags) {
        return Make(pos, Atom, {}, content, flags, uniqueId);
    }

    static TPtr NewArgument(ui64 uniqueId, TPositionHandle pos, const TStringBuf& name) {
        return Make(pos, Argument, {}, name, 0, uniqueId);
    }

    static TPtr NewArguments(ui64 uniqueId, TPositionHandle pos, TListType&& argNodes) {
        return Make(pos, Arguments, std::move(argNodes), ZeroString, 0, uniqueId);
    }

    static TPtr NewLambda(ui64 uniqueId, TPositionHandle pos, TListType&& lambda) {
        return Make(pos, Lambda, std::move(lambda), ZeroString, 0, uniqueId);
    }

    static TPtr NewLambda(ui64 uniqueId, TPositionHandle pos, TPtr&& args, TListType&& body) {
        TListType lambda(body.size() + 1U);
        lambda.front() = std::move(args);
        std::move(body.rbegin(), body.rend(), lambda.rbegin());
        return NewLambda(uniqueId, pos, std::move(lambda));
    }

    static TPtr NewLambda(ui64 uniqueId, TPositionHandle pos, TPtr&& args, TPtr&& body) {
        TListType children(body ? 2 : 1);
        children.front() = std::move(args);
        if (body) {
            children.back() = std::move(body);
        }

        return NewLambda(uniqueId, pos, std::move(children));
    }

    static TPtr NewWorld(ui64 uniqueId, TPositionHandle pos) {
        return Make(pos, World, {}, {}, 0, uniqueId);
    }

    static TPtr NewList(ui64 uniqueId, TPositionHandle pos, TListType&& children) {
        return Make(pos, List, std::move(children), ZeroString, 0, uniqueId);
    }

    static TPtr NewCallable(ui64 uniqueId, TPositionHandle pos, const TStringBuf& name, TListType&& children) {
        return Make(pos, Callable, std::move(children), name, 0, uniqueId);
    }

    TPtr Clone(ui64 newUniqueId) const {
        ENSURE_NOT_DELETED
        return Make(Position_, (EType)Type_, TListType(Children_), Content(), Flags_, newUniqueId);
    }

    TPtr CloneWithPosition(ui64 newUniqueId, TPositionHandle pos) const {
        ENSURE_NOT_DELETED
        return Make(pos, (EType)Type_, TListType(Children_), Content(), Flags_, newUniqueId);
    }

    static TPtr NewNode(TPositionHandle position, EType type, TListType&& children, const TStringBuf& content, ui32 flags, ui64 uniqueId) {
        return Make(position, type, std::move(children), content, flags, uniqueId);
    }

    TPtr ChangeContent(ui64 newUniqueId, const TStringBuf& content) const {
        ENSURE_NOT_DELETED
        return Make(Position_, (EType)Type_, TListType(Children_), content, Flags_, newUniqueId);
    }

    TPtr ChangeChildren(ui64 newUniqueId, TListType&& children) const {
        ENSURE_NOT_DELETED
        return Make(Position_, (EType)Type_, std::move(children), Content(), Flags_, newUniqueId);
    }

    TPtr ChangeChild(ui64 newUniqueId, ui32 index, TPtr&& child) const {
        ENSURE_NOT_DELETED
        Y_ENSURE(index < Children_.size(), "index out of range");
        TListType newChildren(Children_);
        newChildren[index] = std::move(child);
        return Make(Position_, (EType)Type_, std::move(newChildren), Content(), Flags_, newUniqueId);
    }

    void SetTypeAnn(const TTypeAnnotationNode* typeAnn) {
        TypeAnnotation_ = typeAnn;
        State = TypeAnnotation_ ? EState::TypeComplete : EState::Initial;
    }

    const TTypeAnnotationNode* GetTypeAnn() const {
        return TypeAnnotation_;
    }

    EState GetState() const {
        return State;
    }

    void SetState(EState state) {
        State = state;
    }

    ui32 GetArgIndex() const {
        YQL_ENSURE(Type() == EType::Argument);
        return ArgIndex;
    }

    void SetArgIndex(ui32 argIndex) {
        YQL_ENSURE(Type() == EType::Argument);
        YQL_ENSURE(argIndex <= Max<ui16>());
        ArgIndex = (ui16)argIndex;
    }

    ui64 GetHash() const {
        Y_DEBUG_ABORT_UNLESS(HashAbove == HashBelow);
        return HashAbove;
    }

    void SetHash(ui64 hash) {
        HashAbove = HashBelow = hash;
    }

    ui64 GetHashAbove() const {
        return HashAbove;
    }

    void SetHashAbove(ui64 hash) {
        HashAbove = hash;
    }

    ui64 GetHashBelow() const {
        return HashBelow;
    }

    void SetHashBelow(ui64 hash) {
        HashBelow = hash;
    }

    ui64 GetBloom() const {
        return Bloom;
    }

    void SetBloom(ui64 bloom) {
        Bloom = bloom;
    }

    // return pair of outer and inner lambda.
    std::optional<std::pair<const TExprNode*, const TExprNode*>> GetDependencyScope() const {
        if (HasLambdaScope) {
            return std::make_pair(OuterLambda, InnerLambda);
        }
        return std::nullopt;
    }

    void SetDependencyScope(const TExprNode* outerLambda, const TExprNode* innerLambda) {
        Y_DEBUG_ABORT_UNLESS(outerLambda == innerLambda || outerLambda->GetLambdaLevel() < innerLambda->GetLambdaLevel(), "Wrong scope of closures.");
        HasLambdaScope = 1;
        OuterLambda = outerLambda;
        InnerLambda = innerLambda;
    }

    ui16 GetLambdaLevel() const { return LambdaLevel; }
    void SetLambdaLevel(ui16 lambdaLevel) { LambdaLevel = lambdaLevel; }

    bool IsUsedInDependsOn() const {
        YQL_ENSURE(Type() == EType::Argument);
        return UsedInDependsOn;
    }

    void SetUsedInDependsOn() {
        YQL_ENSURE(Type() == EType::Argument);
        UsedInDependsOn = 1;
    }

    void SetUnorderedChildren() {
        YQL_ENSURE(Type() == EType::List || Type() == EType::Callable);
        UnordChildren = 1;
    }

    bool UnorderedChildren() const {
        YQL_ENSURE(Type() == EType::List || Type() == EType::Callable);
        return bool(UnordChildren);
    }

    ~TExprNode() {
        Y_ABORT_UNLESS(Dead(), "Node (id: %lu, type: %s, content: '%s') not dead on destruction.",
            UniqueId_, ToString(Type_).data(),  TString(ContentUnchecked()).data());
        Y_ABORT_UNLESS(!UseCount(), "Node (id: %lu, type: %s, content: '%s') has non-zero use count on destruction.",
            UniqueId_, ToString(Type_).data(),  TString(ContentUnchecked()).data());
    }

private:
    static TPtr Make(TPositionHandle position, EType type, TListType&& children, const TStringBuf& content, ui32 flags, ui64 uniqueId) {
        Y_ENSURE(flags <= TNodeFlags::FlagsMask);
        Y_ENSURE(children.size() <= Max<ui32>());
        Y_ENSURE(content.size() <= Max<ui32>());
        for (size_t i = 0; i < children.size(); ++i) {
            Y_ENSURE(children[i], "Unable to create node " << content << ": " << i << "th child is null");
        }
        return TPtr(new TExprNode(position, type, std::move(children), content.data(), ui32(content.size()), flags, uniqueId));
    }

    TExprNode(TPositionHandle position, EType type, TListType&& children,
        const char* content, ui32 contentSize, ui32 flags, ui64 uniqueId)
        : Children_(std::move(children))
        , Content_(content)
        , UniqueId_(uniqueId)
        , Position_(position)
        , ContentSize(contentSize)
        , Type_(type)
        , Flags_(flags)
        , ExprFlags_(TExprFlags::Default)
        , State(EState::Initial)
        , HasLambdaScope(0)
        , UsedInDependsOn(0)
        , UnordChildren(0)
        , ShallBeDisclosed(0)
        , LiteralList(0)
    {}

    TExprNode(const TExprNode&) = delete;
    TExprNode(TExprNode&&) = delete;
    TExprNode& operator=(const TExprNode&) = delete;
    TExprNode& operator=(TExprNode&&) = delete;

    bool Frozen() const {
        return ExprFlags_ & TExprFlags::Frozen;
    }

    void MarkFrozen(bool frozen = true) {
        if (frozen) {
            ExprFlags_ |= TExprFlags::Frozen;
        } else {
            ExprFlags_ &= ~TExprFlags::Frozen;
        }
    }

    void MarkDead() {
        ExprFlags_ |= TExprFlags::Dead;
    }

    TStringBuf ContentUnchecked() const {
        return TStringBuf(Content_, ContentSize);
    }

    TListType Children_;
    TConstraintSet Constraints_;

    const char* Content_ = nullptr;

    const TExprNode* OuterLambda = nullptr;
    const TExprNode* InnerLambda = nullptr;

    TPtr Result;

    ui64 HashAbove = 0ULL;
    ui64 HashBelow = 0ULL;
    ui64 Bloom = 0ULL;

    const ui64 UniqueId_;
    const TTypeAnnotationNode* TypeAnnotation_ = nullptr;

    const TPositionHandle Position_;
    ui32 RefCount_ = 0U;
    const ui32 ContentSize;

    ui16 ArgIndex = ui16(-1);
    ui16 LambdaLevel = 0; // filled together with OuterLambda
    ui16 IntermediateHashesCount = 0;

    static_assert(TypeMask <= 7, "EType wont fit in 3 bits, increase Type_ bitfield size");
    static_assert(TNodeFlags::FlagsMask <= 7, "TNodeFlags wont fit in 3 bits, increase Flags_ bitfield size");
    static_assert(TExprFlags::FlagsMask <= 3, "TExprFlags wont fit in 2 bits, increase ExprFlags_ bitfield size");
    static_assert(int(EState::Last) <= 16, "EState wont fit in 4 bits, increase State bitfield size");
    struct {
        ui8 Type_           : 3;
        ui8 Flags_          : 3;
        ui8 ExprFlags_      : 2;

        EState State        : 4;
        ui8 HasLambdaScope  : 1;
        ui8 UsedInDependsOn : 1;
        ui8 UnordChildren   : 1;
        ui8 ShallBeDisclosed: 1;
        ui8 LiteralList     : 1;
    };
};

class TExportTable {
public:
    using TSymbols = THashMap<TString, TExprNode::TPtr>;

    TExportTable() = default;
    TExportTable(TExprContext& ctx, TSymbols&& symbols)
        : Symbols_(std::move(symbols))
        , Ctx_(&ctx)
    {}

    const TSymbols& Symbols() const {
        return Symbols_;
    }

    TSymbols& Symbols(TExprContext& ctx) {
        if (Ctx_) {
            YQL_ENSURE(Ctx_ == &ctx);
        } else {
            Ctx_ = &ctx;
        }
        return Symbols_;
    }

    TExprContext& ExprCtx() const {
        YQL_ENSURE(Ctx_);
        return *Ctx_;
    }
private:
    TSymbols Symbols_;
    TExprContext* Ctx_ = nullptr;
};

using TModulesTable = THashMap<TString, TExportTable>;

class IModuleResolver {
public:
    typedef std::shared_ptr<IModuleResolver> TPtr;
    virtual bool AddFromFile(const std::string_view& file, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos = {}) = 0;
    virtual bool AddFromUrl(const std::string_view& file, const std::string_view& url, const std::string_view& tokenName, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos = {}) = 0;
    virtual bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos = {}) = 0;
    virtual bool AddFromMemory(const std::string_view& file, const TString& body, TExprContext& ctx, ui16 syntaxVersion, ui32 packageVersion, TPosition pos, TString& moduleName, std::vector<TString>* exports = nullptr, std::vector<TString>* imports = nullptr) = 0;
    virtual bool Link(TExprContext& ctx) = 0;
    virtual void UpdateNextUniqueId(TExprContext& ctx) const = 0;
    virtual ui64 GetNextUniqueId() const = 0;
    virtual void RegisterPackage(const TString& package) = 0;
    virtual bool SetPackageDefaultVersion(const TString& package, ui32 version) = 0;
    virtual const TExportTable* GetModule(const TString& module) const = 0;
    /*
    Create new resolver which will use already collected modules in readonly manner.
    Parent resolver should be alive while using child due to raw data sharing.
    */
    virtual IModuleResolver::TPtr CreateMutableChild() const = 0;
    virtual void SetFileAliasPrefix(TString&& prefix) = 0;
    virtual TString GetFileAliasPrefix() const = 0;
    virtual ~IModuleResolver() = default;
};

struct TExprStep {
    enum ELevel {
        Params,
        ExpandApplyForLambdas,
        ValidateProviders,
        Configure,
        ExprEval,
        DiscoveryIO,
        Epochs,
        Intents,
        LoadTablesMetadata,
        RewriteIO,
        Recapture,
        LastLevel
    };

    TExprStep()
    {
    }

    void Done(ELevel level) {
        Steps_.Set(level);
    }

    void Reset() {
        Steps_.Reset();
    }

    TExprStep& Repeat(ELevel level) {
        Steps_.Reset(level);
        return *this;
    }

    bool IsDone(ELevel level) {
        return Steps_.Test(level);
    }

private:
    TEnumBitSet<ELevel, Params, LastLevel> Steps_;
};

template <typename T>
struct TMakeTypeImpl;

template <class T>
using TNodeMap = std::unordered_map<const TExprNode*, T>;
using TNodeSet = std::unordered_set<const TExprNode*>;
using TNodeOnNodeOwnedMap = TNodeMap<TExprNode::TPtr>;
using TParentsMap = TNodeMap<TNodeSet>;

using TNodeMultiSet = std::unordered_multiset<const TExprNode*>;
using TParentsMultiMap = TNodeMap<TNodeMultiSet>;

template <>
struct TMakeTypeImpl<TVoidExprType> {
    static const TVoidExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TNullExprType> {
    static const TNullExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TEmptyListExprType> {
    static const TEmptyListExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TEmptyDictExprType> {
    static const TEmptyDictExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TUnitExprType> {
    static const TUnitExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TWorldExprType> {
    static const TWorldExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TGenericExprType> {
    static const TGenericExprType* Make(TExprContext& ctx);
};

template <>
struct TMakeTypeImpl<TItemExprType> {
    static const TItemExprType* Make(TExprContext& ctx, const TStringBuf& name, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TListExprType> {
    static const TListExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TOptionalExprType> {
    static const TOptionalExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TVariantExprType> {
    static const TVariantExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* underlyingType);
};

template <>
struct TMakeTypeImpl<TErrorExprType> {
    static const TErrorExprType* Make(TExprContext& ctx, const TIssue& error);
};

template <>
struct TMakeTypeImpl<TDictExprType> {
    static const TDictExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* keyType,
        const TTypeAnnotationNode* payloadType);
};

template <>
struct TMakeTypeImpl<TTypeExprType> {
    static const TTypeExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* baseType);
};

template <>
struct TMakeTypeImpl<TDataExprType> {
    static const TDataExprType* Make(TExprContext& ctx, EDataSlot slot);
};

template <>
struct TMakeTypeImpl<TPgExprType> {
    static const TPgExprType* Make(TExprContext& ctx, ui32 typeId);
};

template <>
struct TMakeTypeImpl<TDataExprParamsType> {
    static const TDataExprParamsType* Make(TExprContext& ctx, EDataSlot slot, const TStringBuf& one, const TStringBuf& two);
};

template <>
struct TMakeTypeImpl<TCallableExprType> {
    static const TCallableExprType* Make(
        TExprContext& ctx, const TTypeAnnotationNode* returnType, const TVector<TCallableExprType::TArgumentInfo>& arguments,
        size_t optionalArgumentsCount, const TStringBuf& payload);
};

template <>
struct TMakeTypeImpl<TResourceExprType> {
    static const TResourceExprType* Make(TExprContext& ctx, const TStringBuf& tag);
};

template <>
struct TMakeTypeImpl<TTaggedExprType> {
    static const TTaggedExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* baseType, const TStringBuf& tag);
};

template <>
struct TMakeTypeImpl<TStructExprType> {
    static const TStructExprType* Make(TExprContext& ctx, const TVector<const TItemExprType*>& items);
};

template <>
struct TMakeTypeImpl<TTupleExprType> {
    static const TTupleExprType* Make(TExprContext& ctx, const TTypeAnnotationNode::TListType& items);
};

template <>
struct TMakeTypeImpl<TMultiExprType> {
    static const TMultiExprType* Make(TExprContext& ctx, const TTypeAnnotationNode::TListType& items);
};

template <>
struct TMakeTypeImpl<TStreamExprType> {
    static const TStreamExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TFlowExprType> {
    static const TFlowExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TBlockExprType> {
    static const TBlockExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

template <>
struct TMakeTypeImpl<TScalarExprType> {
    static const TScalarExprType* Make(TExprContext& ctx, const TTypeAnnotationNode* itemType);
};

using TSingletonTypeCache = std::tuple<
    const TVoidExprType*,
    const TNullExprType*,
    const TUnitExprType*,
    const TEmptyListExprType*,
    const TEmptyDictExprType*,
    const TWorldExprType*,
    const TGenericExprType*,
    const TTupleExprType*,
    const TStructExprType*,
    const TMultiExprType*
>;

struct TExprContext : private TNonCopyable {
    class TFreezeGuard {
    public:
        TFreezeGuard(const TFreezeGuard&) = delete;
        TFreezeGuard& operator=(const TFreezeGuard&) = delete;

        TFreezeGuard(TExprContext& ctx)
            : Ctx(ctx)
        {
            Ctx.Freeze();
        }

        ~TFreezeGuard() {
            Ctx.UnFreeze();
        }

    private:
        TExprContext& Ctx;
    };

    TIssueManager IssueManager;
    TNodeMap<TIssues> AssociativeIssues;

    TMemoryPool StringPool;
    std::unordered_set<std::string_view> Strings;
    std::unordered_map<ui32, std::string_view> Indexes;

    std::stack<std::unique_ptr<const TTypeAnnotationNode>> TypeNodes;
    std::stack<std::unique_ptr<const TConstraintNode>> ConstraintNodes;
    std::deque<std::unique_ptr<TExprNode>> ExprNodes;

    TSingletonTypeCache SingletonTypeCache;
    std::unordered_set<const TTypeAnnotationNode*, TTypeAnnotationNode::THash, TTypeAnnotationNode::TEqual> TypeSet;
    std::unordered_set<const TConstraintNode*, TConstraintNode::THash, TConstraintNode::TEqual> ConstraintSet;
    std::unordered_map<const TTypeAnnotationNode*, TExprNode::TPtr> TypeAsNodeCache;
    std::unordered_set<TStringBuf, THash<TStringBuf>> DisabledConstraints;

    ui64 NextUniqueId = 0;
    ui64 NodeAllocationCounter = 0;
    ui64 NodesAllocationLimit = 3000000;
    ui64 StringsAllocationLimit = 100000000;
    ui64 RepeatTransformLimit = 1000000;
    ui64 RepeatTransformCounter = 0;
    ui64 TypeAnnNodeRepeatLimit = 1000;

    TGcNodeConfig GcConfig;

    std::unordered_multimap<ui64, TExprNode*> UniqueNodes;

    TExprStep Step;

    bool Frozen;

    explicit TExprContext(ui64 nextUniqueId = 0ULL);
    ~TExprContext();

    ui64 AllocateNextUniqueId() {
        ENSURE_NOT_FROZEN_CTX
        const auto ret = ++NextUniqueId;
        return ret;
    }

    TStringBuf AppendString(const TStringBuf& buf) {
        ENSURE_NOT_FROZEN_CTX
        if (buf.size() == 0) {
            return ZeroString;
        }

        auto it = Strings.find(buf);
        if (it != Strings.end()) {
            return *it;
        }

        auto newBuf = StringPool.AppendString(buf);
        Strings.insert(it, newBuf);
        return newBuf;
    }

    TPositionHandle AppendPosition(const TPosition& pos);
    TPosition GetPosition(TPositionHandle handle) const;

    TExprNodeBuilder Builder(TPositionHandle pos) {
        return TExprNodeBuilder(pos, *this);
    }

    [[nodiscard]]
    TExprNode::TPtr RenameNode(const TExprNode& node, const TStringBuf& name);
    [[nodiscard]]
    TExprNode::TPtr ShallowCopy(const TExprNode& node);
    [[nodiscard]]
    TExprNode::TPtr ShallowCopyWithPosition(const TExprNode& node, TPositionHandle pos);
    [[nodiscard]]
    TExprNode::TPtr ChangeChildren(const TExprNode& node, TExprNode::TListType&& children);
    [[nodiscard]]
    TExprNode::TPtr ChangeChild(const TExprNode& node, ui32 index, TExprNode::TPtr&& child);
    [[nodiscard]]
    TExprNode::TPtr ExactChangeChildren(const TExprNode& node, TExprNode::TListType&& children);
    [[nodiscard]]
    TExprNode::TPtr ExactShallowCopy(const TExprNode& node);
    [[nodiscard]]
    TExprNode::TPtr DeepCopyLambda(const TExprNode& node, TExprNode::TListType&& body);
    [[nodiscard]]
    TExprNode::TPtr DeepCopyLambda(const TExprNode& node, TExprNode::TPtr&& body = TExprNode::TPtr());
    [[nodiscard]]
    TExprNode::TPtr FuseLambdas(const TExprNode& outer, const TExprNode& inner);

    using TCustomDeepCopier = std::function<bool(const TExprNode& node, TExprNode::TListType& newChildren)>;

    [[nodiscard]]
    TExprNode::TPtr DeepCopy(const TExprNode& node, TExprContext& nodeContext, TNodeOnNodeOwnedMap& deepClones,
        bool internStrings, bool copyTypes, bool copyResult = false, TCustomDeepCopier customCopier = {});

    [[nodiscard]]
    TExprNode::TPtr SwapWithHead(const TExprNode& node);
    TExprNode::TPtr ReplaceNode(TExprNode::TPtr&& start, const TExprNode& src, TExprNode::TPtr dst);
    TExprNode::TPtr ReplaceNodes(TExprNode::TPtr&& start, const TNodeOnNodeOwnedMap& replaces);
    template<bool KeepTypeAnns = false>
    TExprNode::TListType ReplaceNodes(TExprNode::TListType&& start, const TNodeOnNodeOwnedMap& replaces);

    TExprNode::TPtr NewAtom(TPositionHandle pos, const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewAtom(AllocateNextUniqueId(), pos, AppendString(content), flags);
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewAtom(TPositionHandle pos, ui32 index) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewAtom(AllocateNextUniqueId(), pos, GetIndexAsString(index), TNodeFlags::Default);
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewArgument(TPositionHandle pos, const TStringBuf& name) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewArgument(AllocateNextUniqueId(), pos, AppendString(name));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewArguments(TPositionHandle pos, TExprNode::TListType&& argNodes) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewArguments(AllocateNextUniqueId(), pos, std::move(argNodes));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewLambda(TPositionHandle pos, TExprNode::TListType&& lambda) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewLambda(AllocateNextUniqueId(), pos, std::move(lambda));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewLambda(TPositionHandle pos, TExprNode::TPtr&& args, TExprNode::TListType&& body) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewLambda(AllocateNextUniqueId(), pos, std::move(args), std::move(body));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewLambda(TPositionHandle pos, TExprNode::TPtr&& args, TExprNode::TPtr&& body) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewLambda(AllocateNextUniqueId(), pos, std::move(args), std::move(body));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewWorld(TPositionHandle pos) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewWorld(AllocateNextUniqueId(), pos);
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewList(TPositionHandle pos, TExprNode::TListType&& children) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewList(AllocateNextUniqueId(), pos, std::move(children));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewCallable(TPositionHandle pos, const TStringBuf& name, TExprNode::TListType&& children) {
        ++NodeAllocationCounter;
        const auto node = TExprNode::NewCallable(AllocateNextUniqueId(), pos, AppendString(name), std::move(children));
        ExprNodes.emplace_back(node.Get());
        return node;
    }

    TExprNode::TPtr NewAtom(TPosition pos, const TStringBuf& content, ui32 flags = TNodeFlags::ArbitraryContent) {
        return NewAtom(AppendPosition(pos), content, flags);
    }

    TExprNode::TPtr NewAtom(TPosition pos, ui32 index) {
        return NewAtom(AppendPosition(pos), index);
    }

    TExprNode::TPtr NewArgument(TPosition pos, const TStringBuf& name) {
        return NewArgument(AppendPosition(pos), name);
    }

    TExprNode::TPtr NewArguments(TPosition pos, TExprNode::TListType&& argNodes) {
        return NewArguments(AppendPosition(pos), std::move(argNodes));
    }

    TExprNode::TPtr NewLambda(TPosition pos, TExprNode::TListType&& lambda) {
        return NewLambda(AppendPosition(pos), std::move(lambda));
    }

    TExprNode::TPtr NewLambda(TPosition pos, TExprNode::TPtr&& args, TExprNode::TListType&& body) {
        return NewLambda(AppendPosition(pos), std::move(args), std::move(body));
    }

    TExprNode::TPtr NewLambda(TPosition pos, TExprNode::TPtr&& args, TExprNode::TPtr&& body) {
        return NewLambda(AppendPosition(pos), std::move(args), std::move(body));
    }

    TExprNode::TPtr NewWorld(TPosition pos) {
        return NewWorld(AppendPosition(pos));
    }

    TExprNode::TPtr NewList(TPosition pos, TExprNode::TListType&& children) {
        return NewList(AppendPosition(pos), std::move(children));
    }

    TExprNode::TPtr NewCallable(TPosition pos, const TStringBuf& name, TExprNode::TListType&& children) {
        return NewCallable(AppendPosition(pos), name, std::move(children));
    }

    TExprNode::TPtr WrapByCallableIf(bool condition, const TStringBuf& callable, TExprNode::TPtr&& node);

    template <typename T, typename... Args>
    const T* MakeType(Args&&... args);

    template <typename T, typename... Args>
    const T* MakeConstraint(Args&&... args);

    void AddError(const TIssue& error) {
        ENSURE_NOT_FROZEN_CTX
        IssueManager.RaiseIssue(error);
    }

    bool AddWarning(const TIssue& warning) {
        ENSURE_NOT_FROZEN_CTX
        return IssueManager.RaiseWarning(warning);
    }

    void Freeze();
    void UnFreeze();

    void Reset();

    template <class TConstraint>
    bool IsConstraintEnabled() const {
        return DisabledConstraints.find(TConstraint::Name()) == DisabledConstraints.end();
    }

    std::string_view GetIndexAsString(ui32 index);
private:
    using TPositionHandleEqualPred = std::function<bool(TPositionHandle, TPositionHandle)>;
    using TPositionHandleHasher = std::function<size_t(TPositionHandle)>;

    bool IsEqual(TPositionHandle a, TPositionHandle b) const;
    size_t GetHash(TPositionHandle p) const;

    std::unordered_set<TPositionHandle, TPositionHandleHasher, TPositionHandleEqualPred> PositionSet;
    std::deque<TPosition> Positions;
};

template <typename T, typename... Args>
inline const T* TExprContext::MakeConstraint(Args&&... args) {
    ENSURE_NOT_FROZEN_CTX
    if (!IsConstraintEnabled<T>()) {
        return nullptr;
    }

    T sample(*this, std::forward<Args>(args)...);
    const auto it = ConstraintSet.find(&sample);
    if (ConstraintSet.cend() != it) {
        return static_cast<const T*>(*it);
    }

    ConstraintNodes.emplace(new T(std::move(sample)));
    const auto ins = ConstraintSet.emplace(ConstraintNodes.top().get());
    return static_cast<const T*>(*ins.first);
}

#undef ENSURE_NOT_DELETED
#undef ENSURE_NOT_FROZEN
#undef ENSURE_NOT_FROZEN_CTX

inline bool IsSameAnnotation(const TTypeAnnotationNode& left, const TTypeAnnotationNode& right) {
    return &left == &right;
}

template <typename T, typename... Args>
const T* TExprContext::MakeType(Args&&... args) {
    return TMakeTypeImpl<T>::Make(*this, std::forward<Args>(args)...);
}

struct TExprAnnotationFlags {
    enum {
        None = 0x00,
        Position = 0x01,
        Types = 0x02
    };
};

///////////////////////////////////////////////////////////////////////////////
// TNodeException
///////////////////////////////////////////////////////////////////////////////
class TNodeException: public yexception {
public:
    TNodeException();
    explicit TNodeException(const TExprNode& node);
    explicit TNodeException(const TExprNode* node);
    explicit TNodeException(const TPositionHandle& pos);

    inline const TPositionHandle& Pos() const {
        return Pos_;
    }

private:
    const TPositionHandle Pos_;
};

bool CompileExpr(TAstNode& astRoot, TExprNode::TPtr& exprRoot, TExprContext& ctx,
    IModuleResolver* resolver, IUrlListerManager* urlListerManager,
    bool hasAnnotations = false, ui32 typeAnnotationIndex = Max<ui32>(), ui16 syntaxVersion = 0);

bool CompileExpr(TAstNode& astRoot, TExprNode::TPtr& exprRoot, TExprContext& ctx,
    IModuleResolver* resolver, IUrlListerManager* urlListerManager,
    ui32 annotationFlags, ui16 syntaxVersion = 0);

struct TLibraryCohesion {
    TExportTable Exports;
    TNodeMap<std::pair<TString, TString>> Imports;
};

bool CompileExpr(TAstNode& astRoot, TLibraryCohesion& cohesion, TExprContext& ctx, ui16 syntaxVersion = 0);

const TTypeAnnotationNode* CompileTypeAnnotation(const TAstNode& node, TExprContext& ctx);

// validate consistency of arguments and lambdas
void CheckArguments(const TExprNode& root);

void CheckCounts(const TExprNode& root);

// Compare expression trees and return first diffrent nodes.
bool CompareExprTrees(const TExprNode*& one, const TExprNode*& two);

bool CompareExprTreeParts(const TExprNode& one, const TExprNode& two, const TNodeMap<ui32>& argsMap);

void GatherParents(const TExprNode& node, TParentsMap& parentsMap, bool withLeaves = false);
void GatherParentsMulti(const TExprNode& node, TParentsMultiMap& parentsMap, bool withLeaves = false);

struct TConvertToAstSettings {
    ui32 AnnotationFlags = 0;
    bool RefAtoms = false;
    std::function<bool(const TExprNode&)> NoInlineFunc;
    bool PrintArguments = false;
    bool AllowFreeArgs = false;
    bool NormalizeAtomFlags = false;
};

TAstParseResult ConvertToAst(const TExprNode& root, TExprContext& ctx, const TConvertToAstSettings& settings);

// refAtoms allows omit copying of atom bodies - they will be referenced from expr graph
TAstParseResult ConvertToAst(const TExprNode& root, TExprContext& ctx, ui32 annotationFlags, bool refAtoms);

TExprNode::TListType GetLambdaBody(const TExprNode& lambda);

TString SubstParameters(const TString& str, const TMaybe<NYT::TNode>& params, TSet<TString>* usedNames);

const TTypeAnnotationNode* GetSeqItemType(const TTypeAnnotationNode* seq);
const TTypeAnnotationNode& GetSeqItemType(const TTypeAnnotationNode& seq);

const TTypeAnnotationNode& RemoveOptionality(const TTypeAnnotationNode& type);

} // namespace NYql

template<>
inline void Out<NYql::TTypeAnnotationNode>(
        IOutputStream &out, const NYql::TTypeAnnotationNode& type)
{
    type.Out(out);
}

#include "yql_expr_builder.inl"
