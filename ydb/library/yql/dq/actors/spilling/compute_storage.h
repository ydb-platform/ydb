#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>
#include <ydb/library/actors/core/actor.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

NKikimr::NMiniKQL::ISpiller::TPtr MakeSpiller(const TString& spillerName, std::function<void()>&& wakeUpCallback);

} // namespace NYql::NDq