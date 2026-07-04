#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;
using namespace NPQRBPrivate;

struct TPersQueueReadBalancer::TTxWriteReceiveAttemptPartitions : public ITransaction {
    TPersQueueReadBalancer* const Self;
    TReceiveAttemptPartitionsWriteBatch Batch;

    TTxWriteReceiveAttemptPartitions(TPersQueueReadBalancer* self, TReceiveAttemptPartitionsWriteBatch batch)
        : Self(self)
        , Batch(std::move(batch))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& upsert : Batch.Upserts) {
            db.Table<Schema::ReceiveAttemptPartitions>().Key(upsert.Key.Consumer, upsert.Key.ReceiveAttemptId).Update(
                NIceDb::TUpdate<Schema::ReceiveAttemptPartitions::PartitionId>(upsert.PartitionId),
                NIceDb::TUpdate<Schema::ReceiveAttemptPartitions::Expiry>(upsert.ExpiryMicros)
            );
        }
        for (const auto& del : Batch.Deletes) {
            db.Table<Schema::ReceiveAttemptPartitions>().Key(del.Key.Consumer, del.Key.ReceiveAttemptId).Delete();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        Self->OnReceiveAttemptPartitionsWriteComplete(std::move(Batch), ctx);
    }
};

}
}
