#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NPQ::NEvents {

enum class EServices {
    GLOBAL = 0,
    DESCRIBER_SERVICE,
    END
};

constexpr unsigned int InternalEventSpaceBegin(EServices service) {
    return EventSpaceBegin(TKikimrEvents::ES_PQ) + (static_cast<unsigned int>(service) << 10);
}

static_assert(EventSpaceBegin(TKikimrEvents::ES_PQ) == InternalEventSpaceBegin(EServices::GLOBAL));
static_assert(InternalEventSpaceBegin(EServices::END) <= EventSpaceEnd(TKikimrEvents::ES_PQ));

}
