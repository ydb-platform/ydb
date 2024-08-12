#pragma once

#include <ydb/core/base/path.h>
#include "ydb/core/base/tablet_pipe.h"
#include "ydb/core/persqueue/events/internal.h"
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/generic/string.h>

namespace NKikimr {
namespace NPQ {

class TPartitionScaleRequest: public NActors::TActorBootstrapped<TPartitionScaleRequest> {
    using TBase = NActors::TActorBootstrapped<TPartitionScaleRequest>;

public:
    struct TEvPartitionScaleRequestDone : public TEventLocal<TEvPartitionScaleRequestDone, TEvPQ::EvPartitionScaleRequestDone> {
        TEvPartitionScaleRequestDone(TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status)
            : Status(status)
        {}

        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus Status;
    };

public:
    TPartitionScaleRequest(TString topicName, TString databasePath, ui64 pathId, ui64 pathVersion, std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit> splits, const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge> merges, NActors::TActorId parentActorId);

public:
    void Bootstrap(const NActors::TActorContext &ctx);
    void PassAway() override;
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&, const TActorContext &ctx);

private:
    STFUNC(StateWork)
    {
         switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }
    std::pair<TString, TString> SplitPath(const TString& path);
    void SendProposeRequest(const NActors::TActorContext &ctx);
    void FillProposeRequest(TEvTxUserProxy::TEvProposeTransaction& proposal, const TString& workingDir, const TString& topicName, const NActors::TActorContext &ctx);

private:
    const TString Topic;
    const TString DatabasePath;
    const ui64 PathId;
    const ui64 PathVersion;
    const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit> Splits;
    const std::vector<NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge> Merges;
    NActors::TActorId ParentActorId;
    NActors::TActorId SchemePipeActorId;
};

} // namespace NPQ
} // namespace NKikimr
