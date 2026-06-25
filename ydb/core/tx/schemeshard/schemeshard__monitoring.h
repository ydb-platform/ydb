#pragma once
#include <util/system/types.h>  // for ui8


namespace NKikimr::NSchemeShard {

// GENERATE_ENUM_SERIALIZATION_WITH_HEADER macro generates
// support methods and string messages from comments for this enum.
enum class ESweepAlert : ui8 {
    NONE = 0,
    START_OK = 1           /* "Start: OK." */,
    START_ERROR_1 = 2      /* "Start: ERROR: invalid format, expected 'shardidx' or 'position'." */,
    START_ERROR_2 = 3      /* "Start: ERROR: sweep already running or paused. Cancel first." */,
    PAUSE_OK = 4           /* "Pause: OK." */,
    PAUSE_ERROR = 5        /* "Pause: ERROR: sweep not running." */,
    RESUME_OK = 6          /* "Resume: OK." */,
    RESUME_ERROR = 7       /* "Resume: ERROR: sweep not paused." */,
    CANCEL_OK = 8          /* "Cancel: OK." */,
    CANCEL_ERROR = 9       /* "Cancel: ERROR: no current sweep." */,
    ERROR_DISABLED = 10     /* "ERROR: format switching is disabled." */,
};

}  // namespace NKikimr::NSchemeShard
