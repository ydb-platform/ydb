#pragma once

#include <ydb/library/yql/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

class TPgType;

NUdf::IHash::TPtr MakePgHash(const TPgType* type);
NUdf::ICompare::TPtr MakePgCompare(const TPgType* type);
NUdf::IEquate::TPtr MakePgEquate(const TPgType* type);

} // namespace NMiniKQL
} // namespace NKikimr
