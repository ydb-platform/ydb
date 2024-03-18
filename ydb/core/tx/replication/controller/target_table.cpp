#include "logging.h"
#include "target_table.h"
#include "util.h"

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NReplication::NController {

class TTableWorkerRegistar: public TActorBootstrapped<TTableWorkerRegistar> {
    void Handle(TEvYdbProxy::TEvDescribeTopicResponse::TPtr& ev) {
        LOG_T("Handle " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            if (IsRetryableError(result)) {
                return Retry();
            }

            return; // TODO: hard error
        }

        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            auto ev = MakeHolder<TEvService::TEvRunWorker>();
            auto& record = ev->Record;

            auto& worker = *record.MutableWorker();
            worker.SetReplicationId(ReplicationId);
            worker.SetTargetId(TargetId);
            worker.SetWorkerId(partition.GetPartitionId());

            auto& readerSettings = *record.MutableCommand()->MutableRemoteTopicReader();
            readerSettings.MutableConnectionParams()->CopyFrom(ConnectionParams);
            readerSettings.SetTopicPath(SrcStreamPath);
            readerSettings.SetTopicPartitionId(partition.GetPartitionId());
            readerSettings.SetConsumerName(ReplicationConsumerName);

            auto& writerSettings = *record.MutableCommand()->MutableLocalTableWriter();
            PathIdFromPathId(DstPathId, writerSettings.MutablePathId());

            Send(Parent, std::move(ev));
        }

        PassAway();
    }

    void Retry() {
        LOG_D("Retry");
        Schedule(TDuration::Seconds(10), new TEvents::TEvWakeup());
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::REPLICATION_CONTROLLER_TABLE_WORKER_REGISTAR;
    }

    explicit TTableWorkerRegistar(
            const TActorId& parent,
            const TActorId& proxy,
            const NKikimrReplication::TConnectionParams& connectionParams,
            ui64 rid,
            ui64 tid,
            const TString& srcStreamPath,
            const TPathId& dstPathId)
        : Parent(parent)
        , YdbProxy(proxy)
        , ConnectionParams(connectionParams)
        , ReplicationId(rid)
        , TargetId(tid)
        , SrcStreamPath(srcStreamPath)
        , DstPathId(dstPathId)
        , LogPrefix("TableWorkerRegistar", ReplicationId, TargetId)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(YdbProxy, new TEvYdbProxy::TEvDescribeTopicRequest(SrcStreamPath, {}));
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvYdbProxy::TEvDescribeTopicResponse, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TActorId YdbProxy;
    const NKikimrReplication::TConnectionParams ConnectionParams;
    const ui64 ReplicationId;
    const ui64 TargetId;
    const TString SrcStreamPath;
    const TPathId DstPathId;
    const TActorLogPrefix LogPrefix;

}; // TTableWorkerRegistar

TTableTarget::TTableTarget(ui64 id, const TString& srcPath, const TString& dstPath)
    : TTargetWithStream(ETargetKind::Table, id, srcPath, dstPath)
{
}

IActor* TTableTarget::CreateWorkerRegistar(TReplication::TPtr replication, const TActorContext& ctx) const {
    return new TTableWorkerRegistar(ctx.SelfID, replication->GetYdbProxy(),
        replication->GetConfig().GetSrcConnectionParams(), replication->GetId(), GetId(),
        CanonizePath(ChildPath(SplitPath(GetSrcPath()), GetStreamName())), GetDstPathId());
}

}
