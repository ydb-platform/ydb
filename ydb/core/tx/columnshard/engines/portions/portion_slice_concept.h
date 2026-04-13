#pragma once

#include <ydb/core/tx/columnshard/common/portion.h>

#include <concepts>
#include <memory>

#include <util/system/types.h>

namespace NKikimr::NOlap {

/// Requirements formerly expressed by `IPortionInfo<TKey>` (optimizer slice surface).
template <class TKey, class TPortion>
concept CPortionInfoSlice = std::totally_ordered<TKey> && requires(const TPortion& p) {
    typename TPortion::TConstPtr;
    typename TPortion::TPtr;
    requires std::same_as<typename TPortion::TConstPtr, std::shared_ptr<const TPortion>>;
    requires std::same_as<typename TPortion::TPtr, std::shared_ptr<TPortion>>;
    { p.IndexKeyStart() } -> std::same_as<TKey>;
    { p.IndexKeyEnd() } -> std::same_as<TKey>;
    { p.GetPortionId() } -> std::same_as<ui64>;
    { p.GetRecordsCount() } -> std::same_as<ui32>;
    { p.GetTotalBlobBytes() } -> std::same_as<ui64>;
    { p.GetTotalRawBytes() } -> std::same_as<ui64>;
    { p.GetProduced() } -> std::same_as<NPortion::EProduced>;
};

} // namespace NKikimr::NOlap
