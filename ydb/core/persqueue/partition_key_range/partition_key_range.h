#pragma once

#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/library/yql/public/decimal/yql_wide_int.h>

#include <util/generic/maybe.h>

// forward declarations
namespace NKikimrPQ {
    class TPartitionKeyRange;
}

namespace NKikimr {
namespace NPQ {

// Converts an integer to a string in a 256-character alphabet
template <typename Type>
    requires std::integral<Type>
void AsString(TString& result, const Type value) {
#ifdef WORDS_BIGENDIAN
    result.append((const char*)&value, sizeof(Type));
#else
    auto b = (const char*)&value;
    for (auto* e = b + sizeof(Type); e != b;) {
        result.append(*(--e));
    }
#endif
}

template <typename Type>
    requires std::integral<Type>
TString AsKeyBound(const Type value) {
    TString key;
    key.reserve(sizeof(Type));
    AsString(key, value);
    return key;
}

template <typename Type>
    requires std::integral<Type>
TString AsKeyBound(const NYql::TWide<Type>& value) {
    TString key;
    key.reserve(sizeof(Type) << 1);

    i64 hi = ui64(value >> NYql::TWide<ui64>(0, sizeof(Type) << 3));
    ui64 lo = ui64(value);

    AsString(key, hi);
    AsString(key, lo);

    return key;
}

template <typename Type>
    requires std::integral<Type>
TString ToHex(const Type value) {
    static constexpr char prefix[] = "0x";
    static constexpr char alphabet[] = "0123456789ABCDEF";

    TString result;
    result.reserve((sizeof(Type) << 1) + sizeof(prefix));
    result.append(prefix);

#ifdef WORDS_BIGENDIAN
    char* c = (char*)&value;
    char* e = c + sizeof(Type);
    for (; c != e; ++c) {
#else
    unsigned char* e = (unsigned char*)&value;
    unsigned char* c = e + sizeof(Type);
    while (c-- != e) {
#endif
        result.append(alphabet[(*c & 0xF0u ) >> 4]);
        result.append(alphabet[*c & 0x0Fu]);
    }

    return result;
}

template <typename Type>
Type AsInt(const TString& bound) {
    Type result = 0;
#ifdef WORDS_BIGENDIAN
    memcpy((void*)bound.begin(), &result, std::min<size_t>(sizeof(Type), bound.size()));
#else
    auto s = std::min(sizeof(Type), bound.size());
    auto f = bound.begin();
    char* t = ((char*)&result) + sizeof(Type) - 1;

    for(; s--; ++f, --t) {
        *t = *f;
    }
#endif
    return result;
}

template<typename T>
inline NYql::TWide<T> AsWide(const TString& bound) {
    NYql::TWide<T> result = 0;
    if (bound.size() <= sizeof(T)) {
        result = AsInt<T>(bound);
        result <<= sizeof(T) << 3;
    } else {
        result = AsInt<T>(bound.substr(0, sizeof(T)));
        result <<= sizeof(T) << 3;
        result += AsInt<T>(bound.substr(sizeof(T)));
    }
    return result;
}


template<>
inline NYql::TWide<ui64> AsInt<NYql::TWide<ui64>>(const TString& bound) {
    return AsWide<ui64>(bound);
}

template<>
inline NYql::TWide<ui16> AsInt<NYql::TWide<ui16>>(const TString& bound) {
    return AsWide<ui16>(bound);
}

TString MiddleOf(const TString& fromBound, const TString& toBound);


struct TPartitionKeyRange {
    TMaybe<TSerializedCellVec> FromBound; // inclusive
    TMaybe<TSerializedCellVec> ToBound;   // exclusive

    static TPartitionKeyRange Parse(const NKikimrPQ::TPartitionKeyRange& proto);
    void Serialize(NKikimrPQ::TPartitionKeyRange& proto) const;

private:
    static void ParseBound(const TString& data, TMaybe<TSerializedCellVec>& bound);

}; // TPartitionKeyRange

} // NPQ
} // NKikimr
