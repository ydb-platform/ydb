#pragma once

#include <yql/essentials/public/decimal/yql_decimal.h>

#include <util/generic/string.h>

namespace NKikimr::NDataStreams::V1 {

    struct THashKeyRange {
        NYql::NDecimal::TUint128 Start;
        NYql::NDecimal::TUint128 End;
    };

    THashKeyRange RangeFromShardNumber(ui32 shardNumber, ui32 shardCount);
    TString GetShardName(ui32 index);
    NYql::NDecimal::TUint128 HexBytesToDecimal(const TString &hex);
    ui32 ShardFromDecimal(const NYql::NDecimal::TUint128 &decimal, ui32 totalShardsCount);
    NYql::NDecimal::TUint128 BytesToDecimal(const TString &bytes);
    bool IsValidDecimal(const TString& bytes);
    TString Uint128ToDecimalString(NYql::NDecimal::TUint128 decimal, const NYql::NDecimal::TUint128& base = 10);
    ui32 CalculateShardFromSrcId(const TString& sourceId, ui32 partitionToTablet);
}
