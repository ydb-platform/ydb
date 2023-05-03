#pragma once
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event_local.h>
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NKqp {

// Sends result event back when the work is done.
NActors::IActor* CreateScriptExecutionsTableCreator(THolder<NActors::IEventBase> resultEvent);

NActors::IActor* CreateScriptExecutionCreatorActor(TEvKqp::TEvScriptRequest::TPtr&& ev);

} // namespace NKikimr::NKqp
