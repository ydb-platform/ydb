#pragma once

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

bool MakeTableSchemeSnapshot(
    TSchemeShard* self,
    const TActorContext& ctx,
    const TPathId& sourcePathId,
    NKikimrSchemeOp::TBackupTask& snapshot,
    TString& error
);

} // namespace NKikimr::NSchemeShard
