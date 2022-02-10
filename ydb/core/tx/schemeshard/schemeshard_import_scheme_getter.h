#pragma once

#include "defs.h"
#include "schemeshard_info_types.h"

namespace NKikimr {
namespace NSchemeShard {

IActor* CreateSchemeGetter(const TActorId& replyTo, TImportInfo::TPtr importInfo, ui32 itemIdx);

} // NSchemeShard
} // NKikimr
