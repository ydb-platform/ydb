#include "cdc_trim_verifier.h"
#include "private_events.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NReplication::NController {
namespace {

class TCdcTrimVerifier: public TActorBootstrapped<TCdcTrimVerifier> {
    const TActorId Parent;
    const TPathId TopicId;
    const ui64 TrimId;
    THashMap<ui64, THashSet<ui32>> Pending;
    bool ValidPartitions = true;

public:
    TCdcTrimVerifier(const TActorId& parent, const TPathId& topicId, ui64 trimId,
            const NKikimrSchemeOp::TPersQueueGroupDescription& topic)
        : Parent(parent)
        , TopicId(topicId)
        , TrimId(trimId)
    {
        THashSet<ui32> ids;
        for (const auto& part : topic.GetPartitions()) {
            if (!part.GetTabletId() || !part.HasPartitionId() || part.GetPartitionId() >= topic.GetTotalGroupCount()
                || !ids.insert(part.GetPartitionId()).second)
            {
                ValidPartitions = false;
                return;
            }
            Pending[part.GetTabletId()].insert(part.GetPartitionId());
        }
        ValidPartitions = !Pending.empty() && ids.size() == topic.GetTotalGroupCount();
    }

    void Bootstrap() {
        if (!ValidPartitions || !TrimId) {
            return Finish("Incomplete partition list or missing trim request ID");
        }
        Become(&TThis::StateWork);
        RequestStatus();
    }

private:
    void RequestStatus() {
        for (const auto& [tabletId, _] : Pending) {
            Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvForward(
                new TEvPersQueue::TEvStatus(), tabletId, false));
        }
        Schedule(TDuration::Seconds(5), new TEvents::TEvWakeup());
    }

    void Handle(TEvPersQueue::TEvStatusResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        auto tablet = Pending.find(record.GetTabletId());
        if (tablet == Pending.end()) {
            return;
        }

        for (const auto& part : record.GetPartResult()) {
            if (!part.HasPartition() || !tablet->second.contains(part.GetPartition())
                || part.GetStatus() == NKikimrPQ::TStatusResponse::STATUS_INITIALIZING
                || !part.GetCdcReplicationTrimTxId())
            {
                continue;
            }
            if (part.GetCdcReplicationTrimTxId() != TrimId) {
                return Finish(TStringBuilder() << "Partition " << part.GetPartition() << " of topic " << TopicId
                    << " did not confirm trim request " << TrimId);
            }
            tablet->second.erase(part.GetPartition());
        }

        if (tablet->second.empty()) {
            Pending.erase(tablet);
        }
        if (Pending.empty()) {
            Finish({});
        }
    }

    void Finish(TString error) {
        Send(Parent, new TEvPrivate::TEvCdcTrimVerified(std::move(error)));
        PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPersQueue::TEvStatusResponse, Handle);
            sFunc(TEvents::TEvWakeup, RequestStatus);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }
};

}

IActor* CreateCdcTrimVerifier(const TActorId& parent, const TPathId& topicId, ui64 trimId,
        const NKikimrSchemeOp::TPersQueueGroupDescription& topic)
{
    return new TCdcTrimVerifier(parent, topicId, trimId, topic);
}

}
