#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h> 

namespace NKikimr {

TTableRange Intersect(TConstArrayRef<NScheme::TTypeId> types, const TTableRange& first, const TTableRange& second);

TString DebugPrintRange(TConstArrayRef<NScheme::TTypeId> types, const TTableRange& range, const NScheme::TTypeRegistry& typeRegistry);
TString DebugPrintPoint(TConstArrayRef<NScheme::TTypeId> types, const TConstArrayRef<TCell>& point, const NScheme::TTypeRegistry& typeRegistry);

TString DebugPrintPartitionInfo(const TKeyDesc::TPartitionInfo& partition, const TVector<NScheme::TTypeId>& keyTypes,
    const NScheme::TTypeRegistry& typeRegistry);

}
