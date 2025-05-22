#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;
using namespace NPQRBPrivate;

struct TPartInfo {
    ui32 PartitionId;
    ui32 Group;
    ui64 TabletId;
    NKikimrPQ::TPartitionKeyRange KeyRange;

    TPartInfo(const ui32 partitionId, const ui64 tabletId, const ui32 group, const NKikimrPQ::TPartitionKeyRange& keyRange)
        : PartitionId(partitionId)
        , Group(group)
        , TabletId(tabletId)
        , KeyRange(keyRange)
    {}
};

struct TPersQueueReadBalancer::TTxWrite : public ITransaction {
    TPersQueueReadBalancer * const Self;
    std::vector<ui32> DeletedPartitions;
    std::vector<TPartInfo> NewPartitions;
    std::vector<std::pair<ui64, TTabletInfo>> NewTablets;
    std::vector<std::pair<ui32, ui32>> NewGroups;
    std::vector<std::pair<ui64, TTabletInfo>> ReallocatedTablets;

    TTxWrite(TPersQueueReadBalancer *self, std::vector<ui32>&& deletedPartitions, std::vector<TPartInfo>&& newPartitions,
                 std::vector<std::pair<ui64, TTabletInfo>>&& newTablets, std::vector<std::pair<ui32, ui32>>&& newGroups,
                 std::vector<std::pair<ui64, TTabletInfo>>&& reallocatedTablets)
        : Self(self)
        , DeletedPartitions(std::move(deletedPartitions))
        , NewPartitions(std::move(newPartitions))
        , NewTablets(std::move(newTablets))
        , NewGroups(std::move(newGroups))
        , ReallocatedTablets(std::move(reallocatedTablets))
    {}

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        NIceDb::TNiceDb db(txc.DB);
        TString config;
        bool res = Self->TabletConfig.SerializeToString(&config);
        Y_ABORT_UNLESS(res);
        db.Table<Schema::Data>().Key(1).Update(
            NIceDb::TUpdate<Schema::Data::PathId>(Self->PathId),
            NIceDb::TUpdate<Schema::Data::Topic>(Self->Topic),
            NIceDb::TUpdate<Schema::Data::Path>(Self->Path),
            NIceDb::TUpdate<Schema::Data::Version>(Self->Version),
            NIceDb::TUpdate<Schema::Data::MaxPartsPerTablet>(Self->MaxPartsPerTablet),
            NIceDb::TUpdate<Schema::Data::SchemeShardId>(Self->SchemeShardId),
            NIceDb::TUpdate<Schema::Data::NextPartitionId>(Self->NextPartitionId),
            NIceDb::TUpdate<Schema::Data::Config>(config),
            NIceDb::TUpdate<Schema::Data::SubDomainPathId>(Self->SubDomainPathId ? Self->SubDomainPathId->LocalPathId : 0));
        for (auto& p : DeletedPartitions) {
            db.Table<Schema::Partitions>().Key(p).Delete();
        }
        for (auto& p : NewPartitions) {
            db.Table<Schema::Partitions>().Key(p.PartitionId).Update(
                NIceDb::TUpdate<Schema::Partitions::TabletId>(p.TabletId)
            );
        }
        for (auto & p : NewGroups) {
            db.Table<Schema::Groups>().Key(p.first, p.second).Update();
        }
        for (auto& p : NewTablets) {
            db.Table<Schema::Tablets>().Key(p.first).Update(
                NIceDb::TUpdate<Schema::Tablets::Owner>(p.second.Owner),
                NIceDb::TUpdate<Schema::Tablets::Idx>(p.second.Idx));
        }
        for (auto& p : ReallocatedTablets) {
            db.Table<Schema::Tablets>().Key(p.first).Update(
                NIceDb::TUpdate<Schema::Tablets::Owner>(p.second.Owner),
                NIceDb::TUpdate<Schema::Tablets::Idx>(p.second.Idx));
        }
        return true;
    }

    void Complete(const TActorContext &ctx) override {
        for (auto& actor : Self->WaitingResponse) {
            THolder<TEvPersQueue::TEvUpdateConfigResponse> res{new TEvPersQueue::TEvUpdateConfigResponse};
            res->Record.SetStatus(NKikimrPQ::OK);
            res->Record.SetTxId(Self->TxId);
            res->Record.SetOrigin(Self->TabletID());
            ctx.Send(actor, res.Release());
        }
        Self->WaitingResponse.clear();

        if (!Self->Inited) {
            Self->Inited = true;
            Self->InitDone(ctx);
        }
    }
};

}
}
