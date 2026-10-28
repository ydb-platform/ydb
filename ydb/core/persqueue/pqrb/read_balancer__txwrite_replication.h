#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

namespace NKikimr::NPQ {

struct TPersQueueReadBalancer::TTxWriteReplicationDataFinished : public NTabletFlatExecutor::ITransaction {
    TPersQueueReadBalancer* const Self;
    std::vector<TReplicationDataFinishedRequest> Requests;

    TTxWriteReplicationDataFinished(TPersQueueReadBalancer* self, std::vector<TReplicationDataFinishedRequest> requests)
        : Self(self)
        , Requests(std::move(requests))
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        absl::flat_hash_set<ui32> updatedPartitions;
        for (const auto& request : Requests) {
            if (Self->PartitionsInfo.contains(request.PartitionId) && updatedPartitions.insert(request.PartitionId).second) {
                db.Table<NPQRBPrivate::Schema::Partitions>().Key(request.PartitionId).Update(
                    NIceDb::TUpdate<NPQRBPrivate::Schema::Partitions::ReplicationDataFinished>(true));
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        for (const auto& request : Requests) {
            auto it = Self->PartitionsInfo.find(request.PartitionId);
            if (it == Self->PartitionsInfo.end()) {
                continue;
            }
            it->second.ReplicationDataFinished = true;
            ctx.Send(request.Sender, new TEvPQ::TEvPartitionReplicationFinishedAck(request.PartitionId), 0, request.Cookie);
        }
        Self->ReplicationDataFinishedWriteInProgress = false;
        Self->ProcessReplicationDataFinishedRequests(ctx);
    }
};

} // namespace NKikimr::NPQ
