#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

namespace NKikimr::NPQ {

struct TPersQueueReadBalancer::TTxCheckMovingDataFinished : public NTabletFlatExecutor::ITransaction {
    TPersQueueReadBalancer* const Self;
    bool AllPartitionsSynced = false;
    int ConfigVersion = -1;

    explicit TTxCheckMovingDataFinished(TPersQueueReadBalancer* self)
        : Self(self)
    {}

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) override {
        using TPartitions = NPQRBPrivate::Schema::Partitions;

        AllPartitionsSynced = false;
        ConfigVersion = Self->Version;
        if (!Self->WaitingResponse.empty()) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        auto partitions = db.Table<TPartitions>().Range().Select<TPartitions::MovingDataFinished>();
        if (!partitions.IsReady()) {
            return false;
        }

        // An empty table does not establish that the topic has been copied.
        if (partitions.EndOfSet()) {
            return true;
        }

        while (!partitions.EndOfSet()) {
            if (!partitions.GetValueOrDefault<TPartitions::MovingDataFinished>(false)) {
                return true;
            }
            if (!partitions.Next()) {
                return false;
            }
        }

        AllPartitionsSynced = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->OnMovingDataChecked(AllPartitionsSynced, ConfigVersion, ctx);
    }
};

} // namespace NKikimr::NPQ
