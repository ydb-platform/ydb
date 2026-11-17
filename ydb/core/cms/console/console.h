#pragma once

#include "events/console.h"

namespace NKikimr {
class TTabletStorageInfo;
}

namespace NKikimr::NConsole {

IActor *CreateConsole(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr::NConsole
