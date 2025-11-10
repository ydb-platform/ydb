#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TDLQMoverActor : public TBaseActor<TDLQMoverActor>
                     , public TConstantLogPrefix {

    static constexpr TDuration Timeout = TDuration::Seconds(5);

public:
    TDLQMoverActor(TDLQMoverSettings&& settings);

    void Bootstrap();
    void PassAway() override;

private:
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(StateDescribe);

    void CreateWriter();

    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPQ::TEvError::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);

    STFUNC(StateWork);

    void ProcessQueue();

    void ReplyError(TString&& error);

private:
    TDLQMoverSettings Settings;
    std::deque<ui64> Queue;

    NDescriber::TTopicInfo TopicInfo;
};

} // namespace NKikimr::NPQ::NMLP
