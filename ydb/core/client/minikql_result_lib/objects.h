#pragma once

#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/scheme/scheme_type_id.h>

#include <google/protobuf/repeated_field.h>

#include <util/generic/hash.h>
#include <util/generic/yexception.h>


namespace NKikimr {
namespace NResultLib {

using TProtoValue = NKikimrMiniKQL::TValue;
using TProtoType = NKikimrMiniKQL::TType;
using EProtoTypeKind = NKikimrMiniKQL::ETypeKind;

class TOptional;
class TListType;
class TTuple;
class TStruct;
class TDict;


namespace NPrivate {

template <typename T>
inline bool HasData(const TProtoValue& value, NScheme::TTypeId schemeType) {
    Y_UNUSED(value);
    Y_UNUSED(schemeType);
    Y_ABORT("Not scpecified type.");
}

template <typename T>
inline T GetData(const TProtoValue& value, NScheme::TTypeId schemeType) {
    Y_UNUSED(value);
    Y_UNUSED(schemeType);
    Y_ABORT("Not scpecified type.");
}

#include "data_funcs.inl"

} // namespace NPrivate


#define ENSURE_KIND(type, expectedKind) \
    do {                                             \
        auto kind = (EProtoTypeKind) type.GetKind(); \
        Y_ENSURE(kind == EProtoTypeKind::expectedKind, \
            "Expected " #expectedKind " instead of " << NKikimrMiniKQL::ETypeKind_Name(kind));    \
        Y_ENSURE(type.Has ## expectedKind(), "No " #expectedKind " in type, seems like an error."); \
    } while (false);


class TOptional {
public:
    explicit TOptional(const TProtoValue& value, const TProtoType& type)
        : RootValue(value)
        , RootType(type)
    {
        ENSURE_KIND(RootType, Optional);
    }

    template <typename T>
    T GetItem() const;

    bool HasItem() const {
        return RootValue.HasOptional();
    }

private:
    const TProtoValue& RootValue;
    const TProtoType& RootType;
};


class TListType {
public:
    template <typename T>
    class iterator {
    public:
        iterator(google::protobuf::RepeatedPtrField<TProtoValue>::const_iterator item, const TProtoType& itemType);
        iterator(const iterator& it);

        iterator& operator++();
        iterator operator++(int);
        bool operator!=(const iterator& it) const;
        bool operator==(const iterator& it) const;
        T operator*() const;
        T Get() const;

    private:
        google::protobuf::RepeatedPtrField<TProtoValue>::const_iterator Item;
        const TProtoType& ItemType;
    };


    template <typename T>
    class TIterableList {
    public:
        TIterableList(const TListType& l)
            : List(l)
        {
        }

        iterator<T> begin() {
            return iterator<T>(List.RootValue.GetList().begin(), List.RootType.GetList().GetItem());
        }

        iterator<T> end() {
            return iterator<T>(List.RootValue.GetList().end(), List.RootType.GetList().GetItem());
        }

    private:
        const TListType& List;
    };


    explicit TListType(const TProtoValue& value, const TProtoType& type)
        : RootValue(value)
        , RootType(type)
        , Size(RootValue.ListSize())
    {
        ENSURE_KIND(RootType, List);
    }

    template <typename T>
    TIterableList<T> MakeIterable() {
        return TIterableList<T>(*this);
    }

    template <typename T>
    T GetItem(size_t index) {
        Y_ENSURE(CheckIndex(index), "List item index" << index << " is out of bounds.");
        iterator<T> it(RootValue.GetList().begin() + index, RootType.GetList().GetItem());
        return it.Get();
    }

    size_t GetSize() const {
        return Size;
    }

private:
    bool CheckIndex(size_t index) {
        return index < Size;
    }

private:
    const TProtoValue& RootValue;
    const TProtoType& RootType;
    const size_t Size;
};


class TTuple {
public:
    explicit TTuple(const TProtoValue& value, const TProtoType& type)
        : RootValue(value)
        , RootType(type)
        , Size(RootValue.TupleSize())
    {
        ENSURE_KIND(RootType, Tuple);
        Y_ENSURE(RootType.GetTuple().ElementSize() == RootValue.TupleSize(), "Size mismatch.");
    }

    template <typename T>
    T GetElement(size_t index) const;

    size_t GetSize() const {
        return Size;
    }

private:
    bool CheckIndex(size_t index) const {
        return index < Size;
    }

private:
    const TProtoValue& RootValue;
    const TProtoType& RootType;
    const size_t Size;
};


class TStruct {
public:
    explicit TStruct(const TProtoValue& value, const TProtoType& type)
        : RootValue(value)
        , RootType(type)
        , Size(RootValue.StructSize())
    {
        ENSURE_KIND(RootType, Struct);
        Y_ENSURE(RootType.GetStruct().MemberSize() == RootValue.StructSize(), "Size mismatch.");
    }

    template <typename T>
    T GetMember(const TStringBuf& memberName);

    template <typename T>
    T GetMember(size_t memberIndex);

    size_t GetMemberIndex(const TStringBuf& memberName) const {
        for (size_t i = 0, end = RootType.GetStruct().MemberSize(); i < end; ++i) {
            const TStringBuf& name = RootType.GetStruct().GetMember(i).GetName();
            if (name == memberName) {
                return i;
            }
        }
        ythrow yexception() << "Unknown Struct member name: " << memberName << ".";
    }

    size_t GetSize() const {
        return Size;
    }

private:
    bool CheckIndex(size_t index) const {
        return index < Size;
    }

private:
    const TProtoValue& RootValue;
    const TProtoType& RootType;
    const size_t Size;
};


class TDict {
public:
    explicit TDict(const TProtoValue& value, const TProtoType& type)
        : RootValue(value)
        , RootType(type)
    {
        ENSURE_KIND(RootType, Dict);
        ENSURE_KIND(RootType.GetDict().GetKey(), Data);

    }

    template <typename K, typename V>
    THashMap<K, V> GetHashMap() const;

private:
    const TProtoValue& RootValue;
    const TProtoType& RootType;
};


// TOptional.
template <typename T>
T TOptional::GetItem() const {
    Y_ENSURE(HasItem(), "Optional is empty!");
    const auto& itemType = RootType.GetOptional().GetItem();
    ENSURE_KIND(itemType, Data);
    auto schemeType = itemType.GetData().GetScheme();
    return NPrivate::GetData<T>(RootValue.GetOptional(), schemeType);
}

#include "optional_funcs.inl"


// TListType.
#include "list_funcs.inl"


// TTuple.
template <typename T>
T TTuple::GetElement(size_t index) const {
    Y_ENSURE(CheckIndex(index), "Tuple element index" << index << " is out of bounds.");
    const auto& elementType = RootType.GetTuple().GetElement(index);
    const auto& element = RootValue.GetTuple(index);
    ENSURE_KIND(elementType, Data);
    auto schemeType = elementType.GetData().GetScheme();
    return NPrivate::GetData<T>(element, schemeType);
}

#include "tuple_funcs.inl"


// TStruct.
template <typename T>
T TStruct::GetMember(const TStringBuf& memberName) {
    size_t memberIndex = GetMemberIndex(memberName);
    return GetMember<T>(memberIndex);
}

template <typename T>
T TStruct::GetMember(size_t memberIndex) {
    Y_ENSURE(CheckIndex(memberIndex), "Struct member index" << memberIndex << " is out of bounds.");
    const auto& memberType = RootType.GetStruct().GetMember(memberIndex).GetType();
    const auto& member = RootValue.GetStruct(memberIndex);
    ENSURE_KIND(memberType, Data);
    auto schemeType = memberType.GetData().GetScheme();
    return NPrivate::GetData<T>(member, schemeType);
}

#include "struct_funcs.inl"


// TDict.
template <typename K, typename V>
THashMap<K, V> TDict::GetHashMap() const {
    THashMap<K, V> m;
    ui32 keySchemeType = RootType.GetDict().GetKey().GetData().GetScheme();
    for (const auto& kvPair : RootValue.GetDict()) {
        auto dictKey = NPrivate::GetData<K>(kvPair.GetKey(), keySchemeType);
        const auto& dictValue = kvPair.GetPayload();
        const auto& dictValueType = RootType.GetDict().GetPayload();
        m[dictKey] = V(dictValue, dictValueType);
    }
    return m;
}


#undef ENSURE_KIND

} // namespace NResultLib
} // namespace NKikimr
