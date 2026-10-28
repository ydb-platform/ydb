#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

namespace NKikimr::NPQ {

struct TPersQueueReadBalancer::TTxWriteMovingDataFinished : public NTabletFlatExecutor::ITransaction {
    TPersQueueReadBalancer* const Self;
    std::vector<TMovingDataFinishedRequest> Requests;

    TTxWriteMovingDataFinished(TPersQueueReadBalancer* self, std::vector<TMovingDataFinishedRequest> requests)
        : Self(self)
        , Requests(std::move(requests))
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        absl::flat_hash_set<ui32> updatedPartitions;
        for (const auto& request : Requests) {
            if (Self->PartitionsInfo.contains(request.PartitionId) && updatedPartitions.insert(request.PartitionId).second) {
                db.Table<NPQRBPrivate::Schema::Partitions>().Key(request.PartitionId).Update(
                    NIceDb::TUpdate<NPQRBPrivate::Schema::Partitions::MovingDataFinished>(true));
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
            it->second.MovingDataFinished = true;
            ctx.Send(request.Sender, new TEvPQ::TEvPartitionMovingFinishedAck(request.PartitionId), 0, request.Cookie);
        }
        Self->MovingDataFinishedWriteInProgress = false;
        Self->ProcessMovingDataFinishedRequests(ctx);
    }
};

} // namespace NKikimr::NPQ
