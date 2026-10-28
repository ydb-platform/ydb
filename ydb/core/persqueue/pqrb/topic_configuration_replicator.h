#pragma once

#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

class TTopicConfigurationReplicator : public TBaseTabletActor<TTopicConfigurationReplicator>, private TConstantLogPrefix {
public:
    TTopicConfigurationReplicator(ui64 tabletId, const TActorId& tabletActor,
        const NKikimrPQ::TMirrorPartitionConfig& config);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);
    TStructuredMessage BuildLogPrefix() const override;

    const NKikimrPQ::TMirrorPartitionConfig Config;
};

} // namespace NKikimr::NPQ
