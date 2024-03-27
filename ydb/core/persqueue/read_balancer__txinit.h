#pragma once

#include "read_balancer.h"
#include "read_balancer__schema.h"

#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

namespace NKikimr {
namespace NPQ {

using namespace NTabletFlatExecutor;
using namespace NPQRBPrivate;

struct TPersQueueReadBalancer::TTxInit : public ITransaction {
    TPersQueueReadBalancer * const Self;

    TTxInit(TPersQueueReadBalancer *self)
        : Self(self)
    {}

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        try {
            Y_UNUSED(ctx); //read config
            NIceDb::TNiceDb db(txc.DB);

            auto dataRowset = db.Table<Schema::Data>().Range().Select();
            auto partsRowset = db.Table<Schema::Partitions>().Range().Select();
            auto groupsRowset = db.Table<Schema::Groups>().Range().Select();
            auto tabletsRowset = db.Table<Schema::Tablets>().Range().Select();

            if (!dataRowset.IsReady() || !partsRowset.IsReady() || !groupsRowset.IsReady() || !tabletsRowset.IsReady())
                return false;

            while (!dataRowset.EndOfSet()) { //found out topic info
                Y_ABORT_UNLESS(!Self->Inited);
                Self->PathId  = dataRowset.GetValue<Schema::Data::PathId>();
                Self->Topic   = dataRowset.GetValue<Schema::Data::Topic>();
                Self->Path    = dataRowset.GetValue<Schema::Data::Path>();
                Self->Version = dataRowset.GetValue<Schema::Data::Version>();
                Self->MaxPartsPerTablet = dataRowset.GetValueOrDefault<Schema::Data::MaxPartsPerTablet>(0);
                Self->SchemeShardId = dataRowset.GetValueOrDefault<Schema::Data::SchemeShardId>(0);
                Self->NextPartitionId = dataRowset.GetValueOrDefault<Schema::Data::NextPartitionId>(0);

                ui64 subDomainPathId = dataRowset.GetValueOrDefault<Schema::Data::SubDomainPathId>(0);
                if (subDomainPathId) {
                    Self->SubDomainPathId.emplace(Self->SchemeShardId, subDomainPathId);
                }

                TString config = dataRowset.GetValueOrDefault<Schema::Data::Config>("");
                if (!config.empty()) {
                    bool res = Self->TabletConfig.ParseFromString(config);
                    Y_ABORT_UNLESS(res);

                    Migrate(Self->TabletConfig);
                    Self->Consumers.clear();

                    for (auto& consumer : Self->TabletConfig.GetConsumers()) {
                        Self->Consumers[consumer.GetName()].ScalingSupport = consumer.HasScalingSupport() ? consumer.GetScalingSupport() : DefaultScalingSupport();
                    }

                    Self->PartitionGraph = MakePartitionGraph(Self->TabletConfig);
                }
                Self->Inited = true;
                if (!dataRowset.Next())
                    return false;
            }

            while (!partsRowset.EndOfSet()) { //found out tablets for partitions
                ++Self->NumActiveParts;
                ui32 part = partsRowset.GetValue<Schema::Partitions::Partition>();
                ui64 tabletId = partsRowset.GetValue<Schema::Partitions::TabletId>();

                Self->PartitionsInfo[part] = {tabletId, EPartitionState::EPS_FREE, TActorId(), part + 1};
                Self->AggregatedStats.AggrStats(part, partsRowset.GetValue<Schema::Partitions::DataSize>(), 
                                                partsRowset.GetValue<Schema::Partitions::UsedReserveSize>());

                if (!partsRowset.Next())
                    return false;
            }

            while (!groupsRowset.EndOfSet()) { //found out tablets for partitions
                ui32 groupId = groupsRowset.GetValue<Schema::Groups::GroupId>();
                ui32 partition = groupsRowset.GetValue<Schema::Groups::Partition>();
                Y_ABORT_UNLESS(groupId > 0);
                auto jt = Self->PartitionsInfo.find(partition);
                Y_ABORT_UNLESS(jt != Self->PartitionsInfo.end());
                jt->second.GroupId = groupId;

                Self->NoGroupsInBase = false;

                if (!groupsRowset.Next())
                    return false;
            }

            Y_ABORT_UNLESS(Self->ClientsInfo.empty());

            for (auto& p : Self->PartitionsInfo) {
                ui32 groupId = p.second.GroupId;
                Self->GroupsInfo[groupId].push_back(p.first);

            }
            Self->TotalGroups = Self->GroupsInfo.size();


            while (!tabletsRowset.EndOfSet()) { //found out tablets for partitions
                ui64 tabletId = tabletsRowset.GetValue<Schema::Tablets::TabletId>();
                TTabletInfo info;
                info.Owner = tabletsRowset.GetValue<Schema::Tablets::Owner>();
                info.Idx = tabletsRowset.GetValue<Schema::Tablets::Idx>();
                Self->MaxIdx = Max(Self->MaxIdx, info.Idx);

                Self->TabletsInfo[tabletId] = info;
                if (!tabletsRowset.Next())
                    return false;
            }

            Self->Generation = txc.Generation;
        } catch (const TNotReadyTabletException&) {
            return false;
        } catch (...) {
        Y_ABORT("there must be no leaked exceptions");
        }
        return true;
    }


    void Complete(const TActorContext& ctx) override {
        Self->SignalTabletActive(ctx);
        if (Self->Inited)
            Self->InitDone(ctx);
    }
};

}
}
