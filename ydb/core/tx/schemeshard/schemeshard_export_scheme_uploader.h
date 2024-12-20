#pragma once

#include "schemeshard_info_types.h"

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSchemeShard {

IActor* CreateSchemeUploader(TSchemeShard* schemeShard, TExportInfo::TPtr exportInfo, ui32 itemIdx, TTxId txId, const NKikimrSchemeOp::TBackupTask& task);

} // NSchemeShard
} // NKikimr
