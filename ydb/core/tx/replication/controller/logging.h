#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NReplication::NController {

class TController;

NActors::NStructuredLog::TStructuredMessage CreateTabletLogPrefix(const TController* self);
NActors::NStructuredLog::TStructuredMessage CreateTabletLogPrefix(const TController* self, const TString& txName);
NActors::NStructuredLog::TStructuredMessage CreateActorLogPrefix(const TString& activity, ui64 rid = 0, ui64 tid = 0);

}
