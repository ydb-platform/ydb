#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <library/cpp/actors/core/actor.h>

namespace NYql::NDq {

NYql::NDq::IDqChannelStorage::TPtr CreateDqChannelStorage(TTxId txId, ui64 channelId,
    NYql::NDq::IDqChannelStorage::TWakeUpCallback wakeUpCb, const NActors::TActorContext& ctx);

} // namespace NYql::NDq
