#pragma once

#include <ydb/core/base/defs.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NDataShardLoad {

using TUploadRequest = std::unique_ptr<IEventBase>;
using TRequestsVector = std::vector<TUploadRequest>;

} // NKikimr::NDataShardLoad
