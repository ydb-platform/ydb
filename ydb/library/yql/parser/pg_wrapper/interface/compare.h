#pragma once

#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_item_comparator.h>
#include <ydb/library/yql/public/udf/arrow/block_item_hasher.h>

namespace NKikimr {
namespace NMiniKQL {

class TPgType;

NUdf::IHash::TPtr MakePgHash(const TPgType* type);
NUdf::ICompare::TPtr MakePgCompare(const TPgType* type);
NUdf::IEquate::TPtr MakePgEquate(const TPgType* type);
NUdf::IBlockItemComparator::TPtr MakePgItemComparator(ui32 typeId);
NUdf::IBlockItemHasher::TPtr MakePgItemHasher(ui32 typeId);

} // namespace NMiniKQL
} // namespace NKikimr
