#pragma once
#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NCSIndex {

enum EEvents {
    EvAddData = EventSpaceBegin(TKikimrEvents::ES_EXT_INDEX),
    EvAddDataResult,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXT_INDEX), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXT_INDEX)");

}
