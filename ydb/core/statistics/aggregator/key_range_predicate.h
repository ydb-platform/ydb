#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/array_ref.h>
#include <optional>
#include <utility>

namespace NKikimr::NStat {

constexpr ui32 MaxAnalyzeRowTableSubranges = 4096;

// DataShard EndKeyPrefix → disjoint PK ranges.
TVector<std::pair<ui64, TSerializedTableRange>> MakeShardSubranges(
    const TVector<TKeyDesc::TPartitionInfo>& partitions);

// Merge consecutive shard ranges down to ceil(tableBytes / budget) when known.
// Never splits a shard. Budget 0 or unknown size: one range per shard.
TVector<std::pair<ui64, TSerializedTableRange>> MakeBudgetedSubranges(
    const TVector<TKeyDesc::TPartitionInfo>& partitions,
    std::optional<ui64> tableBytesSize,
    ui64 rangeBudgetBytes);

// Types that can be encoded as YQL range-predicate parameters.
bool CanEncodeKeyBoundType(NScheme::TTypeId typeId);
bool CanEncodeKeyRangePredicate(TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes);

// YQL WHERE + DECLARE + params. Empty From/To (-inf/+inf) are omitted.
// Comparisons use DataShard NULL-as-min order, including NULL cells in bounds.
bool TryBuildKeyRangePredicate(
    TConstArrayRef<TString> keyColumnNames,
    TConstArrayRef<NScheme::TTypeInfo> keyColumnTypes,
    const TSerializedTableRange& range,
    TString& where,
    TString& declares,
    NYdb::TParamsBuilder& params,
    TString& error);

} // namespace NKikimr::NStat
