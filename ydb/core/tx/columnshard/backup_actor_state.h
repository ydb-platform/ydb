#pragma once

#include <util/system/types.h>

namespace NKikimr::NColumnShard {

enum class EBackupActorState : ui8 {
    Invalid,
    Init,
    Progress,
    Done
};

} // namespace NKikimr::NColumnShard