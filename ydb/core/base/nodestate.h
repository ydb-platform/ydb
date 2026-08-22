#pragma once

namespace NKikimr {
    enum ENodeState {
        GOOD = 0,
        PRETTY_GOOD,
        MAY_BE_GOOD,
        MAY_BE_BAD,
        BAD,
        UNKNOWN,
        NODE_STATE_MAX
    };
}
