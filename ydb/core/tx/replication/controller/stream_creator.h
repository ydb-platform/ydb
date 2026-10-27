#pragma once

#include "replication.h"

#include <optional>

namespace NKikimr::NReplication::NController {

IActor* CreateStreamCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateStreamCreator(const TActorId& parent, const TActorId& proxy, ui64 rid, ui64 tid,
    const TReplication::ITarget::IConfig::TPtr& config,
    const TString& streamName, const TString& consumerName, const TDuration& bootstrapRetentionPeriod,
    const std::optional<TDuration>& resolvedTimestamps = std::nullopt,
    bool supportsTopicAutopartitioning = false, bool needCreate = true,
    bool initialScan = true,
    TDuration streamRetentionPeriod = TDuration::Seconds(1),
    TDuration consumerAvailabilityPeriod = TDuration::Days(1));

}
