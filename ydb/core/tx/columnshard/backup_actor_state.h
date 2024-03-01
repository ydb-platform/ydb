#pragma once

#include <util/system/types.h>

namespace NKikimr::NColumnShard {

enum class BackupActorState : ui8 {
    Invalid,
    Init,
    Progress,
    Done
};

} // namespace NKikimr::NColumnShard