#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NKqp {

NYql::NDq::IDqChannelStorage::TPtr CreateKqpChannelStorage(ui64 txId, ui64 channelId,
    NYql::NDq::IDqChannelStorage::TWakeUpCallback wakeUpCb, const NActors::TActorContext& ctx);

} // namespace NKikimr::NKqp
