#pragma once
#include <ydb/core/ymq/actor/cfg/defs.h>

#include "actor.h"

#include <ydb/core/ymq/base/action.h>

namespace NKikimr::NSQS {
IActor* CreateGarbageCollector(const TActorId schemeCacheId, const TActorId queuesListReaderId);
}
