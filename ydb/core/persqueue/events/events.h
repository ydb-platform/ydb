#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NPQ::NEvents {

enum class EServices {
    GLOBAL = 0,
    INTERNAL = 1,
    DESCRIBER_SERVICE = 2,
    MLP = 4,
    CLOUD_EVENTS = 6,
    SCHEMA = 7,
    END
};

constexpr unsigned int InternalEventSpaceBegin(EServices service) {
    return EventSpaceBegin(TKikimrEvents::ES_PQ) + (static_cast<unsigned int>(service) << 9);
}

static_assert(EventSpaceBegin(TKikimrEvents::ES_PQ) == InternalEventSpaceBegin(EServices::GLOBAL));
static_assert(InternalEventSpaceBegin(EServices::END) <= EventSpaceEnd(TKikimrEvents::ES_PQ));

}
