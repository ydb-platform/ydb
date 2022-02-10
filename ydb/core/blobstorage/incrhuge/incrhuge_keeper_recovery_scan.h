#pragma once

#include "defs.h"
#include "incrhuge_keeper_common.h"

namespace NKikimr {
    namespace NIncrHuge {

        enum class EScanCookie : ui64 {
            Recovery = 1,
            Defrag = 2
        };

        IActor *CreateRecoveryScanActor(TChunkIdx chunkIdx, bool indexOnly, TChunkSerNum chunkSerNum, ui64 cookie,
                const TKeeperCommonState& state);

    } // NIncrHuge
} // NKikimr
