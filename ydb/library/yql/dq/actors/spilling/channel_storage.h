#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <ydb/library/yql/dq/runtime/dq_spiller.h>
#include <ydb/library/actors/core/actor.h>


namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

IDqChannelStorage::TPtr CreateDqChannelStorage(IDqSpiller::TPtr spiller);

} // namespace NYql::NDq
