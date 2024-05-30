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
                        Self->Consumers[consumer.GetName()];
                    }
                    Self->PartitionGraph = MakePartitionGraph(Self->TabletConfig);
                    Self->UpdateConfigCounters();
                }
                Self->Inited = true;
                if (!dataRowset.Next())
                    return false;
            }

            std::map<ui32, TPersQueueReadBalancer::TPartitionInfo> partitionsInfo;
            while (!partsRowset.EndOfSet()) { //found out tablets for partitions
                ++Self->NumActiveParts;
                ui32 part = partsRowset.GetValue<Schema::Partitions::Partition>();
                ui64 tabletId = partsRowset.GetValue<Schema::Partitions::TabletId>();

                partitionsInfo[part] = {tabletId, {}};
                Self->AggregatedStats.AggrStats(part, partsRowset.GetValue<Schema::Partitions::DataSize>(),
                                                partsRowset.GetValue<Schema::Partitions::UsedReserveSize>());

                if (!partsRowset.Next())
                    return false;
            }
            Self->PartitionsInfo.insert(partitionsInfo.rbegin(), partitionsInfo.rend());

            Self->TotalGroups = Self->PartitionsInfo.size();

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
