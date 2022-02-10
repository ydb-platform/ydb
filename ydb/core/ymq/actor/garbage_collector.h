#pragma once
#include "defs.h"

#include "actor.h"

#include <ydb/core/ymq/base/action.h>

namespace NKikimr::NSQS {
IActor* CreateGarbageCollector(const TActorId schemeCacheId, const TActorId queuesListReaderId);
}
