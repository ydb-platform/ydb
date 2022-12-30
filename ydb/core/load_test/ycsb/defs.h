#pragma once

#include <ydb/core/base/defs.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/protos/services.pb.h>

namespace NKikimr::NDataShardLoad {

using TUploadRequest = std::unique_ptr<IEventBase>;
using TRequestsVector = std::vector<TUploadRequest>;

TString GetKey(size_t n);

static const TString Value = TString(100, 'x');

} // NKikimr::NDataShardLoad
