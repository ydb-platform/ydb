#pragma once

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NKikimrSchemeOp::TBackupTask MakeBackupTableSchemeSnapshot(
    TSchemeShard* self,
    const TActorContext& ctx,
    const TPathId& sourcePathId
);

} // namespace NKikimr::NSchemeShard
