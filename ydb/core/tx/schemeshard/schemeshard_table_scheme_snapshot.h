#pragma once

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

TMaybe<TIndexBuildId> GetRebuildIndexBuildId(TStringBuf indexName);

THashSet<TPathId> GetIndexesForBackup(const TSchemeShard* self, const TPathId& tablePathId);

bool MakeTableSchemeSnapshot(
    TSchemeShard* self,
    const TActorContext& ctx,
    const TPathId& sourcePathId,
    NKikimrSchemeOp::TBackupTask& snapshot,
    TString& error
);

} // namespace NKikimr::NSchemeShard
