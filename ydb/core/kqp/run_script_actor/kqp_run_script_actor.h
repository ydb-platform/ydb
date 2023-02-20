#pragma once

#include <ydb/core/protos/kqp.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NKqp {

struct TEvKqpRunScriptActor {
};

NActors::IActor* CreateRunScriptActor(const NKikimrKqp::TEvQueryRequest& request);

} // namespace NKikimr::NKqp
