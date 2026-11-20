#pragma once

#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

namespace NKikimr::NMiniKQL {

class TPgType;

struct TKqpComputeColumn {
    NTable::TTag Tag;
    NScheme::TTypeInfo Type;
    TString TypeMod;
    TPgType* PgType = nullptr;
};

} // namespace NKikimr::NMiniKQL
