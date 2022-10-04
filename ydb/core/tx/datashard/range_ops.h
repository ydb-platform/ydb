#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NKikimr {

TTableRange Intersect(TConstArrayRef<NScheme::TTypeInfo> types, const TTableRange& first, const TTableRange& second);

TString DebugPrintRange(TConstArrayRef<NScheme::TTypeInfo> types, const TTableRange& range, const NScheme::TTypeRegistry& typeRegistry);
TString DebugPrintRanges(TConstArrayRef<NScheme::TTypeInfo> types, const TSmallVec<TSerializedTableRange>& ranges, const NScheme::TTypeRegistry& typeRegistry);
TString DebugPrintPoint(TConstArrayRef<NScheme::TTypeInfo> types, const TConstArrayRef<TCell>& point, const NScheme::TTypeRegistry& typeRegistry);

TString DebugPrintPartitionInfo(const TKeyDesc::TPartitionInfo& partition, const TVector<NScheme::TTypeInfo>& keyTypes,
    const NScheme::TTypeRegistry& typeRegistry);

}
