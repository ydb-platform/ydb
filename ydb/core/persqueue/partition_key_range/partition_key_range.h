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
