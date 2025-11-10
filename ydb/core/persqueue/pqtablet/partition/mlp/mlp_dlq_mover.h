#pragma once

#include "mlp.h"
#include "mlp_common.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NPQ::NMLP {

class TDLQMoverActor : public TBaseActor<TDLQMoverActor>
                     , public TConstantLogPrefix {

public:
    TDLQMoverActor(TDLQMoverSettings&& settings);

    void Bootstrap();
    void PassAway() override;
    TString BuildLogPrefix() const override;

private:
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(StateDescribe);

    void CreateWriter();
    void Handle(TEvPartitionWriter::TEvInitResult::TPtr&);
    void Handle(TEvPartitionWriter::TEvDisconnected::TPtr&);
    STFUNC(StateInit);

    void ProcessQueue();
    void Handle(TEvPersQueue::TEvResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(StateRead);

    void WaitWrite();
    void Handle(TEvPartitionWriter::TEvWriteAccepted::TPtr&);
    void Handle(TEvPartitionWriter::TEvWriteResponse::TPtr&);
    STFUNC(StateWrite);

    void ReplySuccess();
    void ReplyError(TString&& error);

    void SendToPQTablet(std::unique_ptr<IEventBase> ev);

private:
    TDLQMoverSettings Settings;

    TString ProducerId;
    ui64 SeqNo;
    std::deque<ui64> Queue;

    TString Error;
    std::vector<ui64> Processed;

    NDescriber::TTopicInfo TopicInfo;
    const IPartitionChooser::TPartitionInfo* TargetPartition;
    TActorId PartitionWriterActorId;

    bool FirstRequest = true;

    ui64 NextPartNo = 0;
    ui64 TotalPartNo = 0;
    ui64 WriteCookie = 0;
};

} // namespace NKikimr::NPQ::NMLP
