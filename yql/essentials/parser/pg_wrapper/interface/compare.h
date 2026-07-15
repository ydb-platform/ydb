#pragma once

#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/arrow/block_item_comparator.h>
#include <yql/essentials/public/udf/arrow/block_item_hasher.h>

#include <expected>

namespace NKikimr::NMiniKQL {

class TPgType;

NUdf::IHash::TPtr MakePgHash(const TPgType* type);
NUdf::ICompare::TPtr MakePgCompare(const TPgType* type);
NUdf::IEquate::TPtr MakePgEquate(const TPgType* type);
NUdf::IBlockItemComparator::TPtr MakePgItemComparator(ui32 typeId);
NUdf::IBlockItemHasher::TPtr MakePgItemHasher(ui32 typeId);

enum class EPgCompareType {
    Less,
    Greater,
};

// Used only for type annotation / optimization purposes.
// Do not use in computation level!
//
// Expensive call:
// This call creates |TPgCompareOp| and its state, then invokes the comparison.
// For repeated comparisons, create the operator and state once and reuse them.
std::expected<bool, TString> PgCompareWithCasts(
    TStringBuf lhs, ui32 lhsTypeId,
    TStringBuf rhs, ui32 rhsTypeId,
    EPgCompareType cmpType);

} // namespace NKikimr::NMiniKQL
