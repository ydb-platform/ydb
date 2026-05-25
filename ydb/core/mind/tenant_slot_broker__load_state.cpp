#include "tenant_slot_broker_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/random_provider/random_provider.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER


namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxLoadState : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxLoadState(TTenantSlotBroker *self)
        : TBase(self)
    {
    }

    template <typename T>
    bool IsReady(T &t)
    {
        return t.IsReady();
    }

    template <typename T, typename ...Ts>
    bool IsReady(T &t, Ts &...args)
    {
        return t.IsReady() && IsReady(args...);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxLoadState Execute");

        ui64 reqId = Self->RequestId++;
        NIceDb::TNiceDb db(txc.DB);
        auto configRow = db.Table<Schema::Config>().Key(ConfigKey_Config).Select<Schema::Config::TColumns>();
        auto tenantRowset = db.Table<Schema::RequiredSlots>().Range().Select<Schema::RequiredSlots::TColumns>();
        auto allocationRowset = db.Table<Schema::SlotsAllocations>().Range().Select<Schema::SlotsAllocations::TColumns>();
        auto slotRowset = db.Table<Schema::Slots>().Range().Select<Schema::Slots::TColumns>();
        auto labelRowset = db.Table<Schema::SlotLabels>().Range().Select<Schema::SlotLabels::TColumns>();
        auto bannedSlotRowset = db.Table<Schema::BannedSlots>().Range().Select<Schema::BannedSlots::TColumns>();
        auto pinnedSlotRowset = db.Table<Schema::PinnedSlots>().Range().Select<Schema::PinnedSlots::TColumns>();

        if (!db.Precharge<Schema>())
            return false;

        if (!IsReady(configRow, tenantRowset, allocationRowset, slotRowset, labelRowset, bannedSlotRowset, pinnedSlotRowset))
            return false;

        if (configRow.IsValid()) {
            auto configString = configRow.GetValue<Schema::Config::Value>();
            NKikimrTenantSlotBroker::TConfig config;
            Y_PROTOBUF_SUPPRESS_NODISCARD config.ParseFromArray(configString.data(), configString.size());
            Self->LoadConfigFromProto(config);

            YDB_LOG_CTX_DEBUG(ctx, "Loaded",
                {"config", Endl},
                {"DebugString", config.DebugString()});
        } else {
            Self->LoadConfigFromProto(NKikimrTenantSlotBroker::TConfig());
            LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "Using default config.");
        }

        Self->ClearState();

        while (!tenantRowset.EndOfSet()) {
            TString tenantName = tenantRowset.GetValue<Schema::RequiredSlots::TenantName>();
            TTenant::TPtr tenant = Self->GetOrCreateTenant(tenantName);

            TSlotDescription descr;
            descr.SlotType = tenantRowset.GetValue<Schema::RequiredSlots::SlotType>();
            ui64 dc = tenantRowset.GetValue<Schema::RequiredSlots::DataCenter>();
            descr.DataCenter = DataCenterToString((ui32)dc);

            auto count = tenantRowset.GetValue<Schema::RequiredSlots::Count>();
            tenant->AddSlotsAllocation(descr, count);

            YDB_LOG_CTX_DEBUG(ctx, "Loaded required slots for tenant",
                {"descr", descr.ToString()},
                {"tenantName", tenantName});

            if (!tenantRowset.Next())
                return false;
        }

        while (!allocationRowset.EndOfSet()) {
            TString tenantName = allocationRowset.GetValue<Schema::SlotsAllocations::TenantName>();
            TTenant::TPtr tenant = Self->GetOrCreateTenant(tenantName);

            TSlotDescription descr;
            descr.SlotType = allocationRowset.GetValue<Schema::SlotsAllocations::SlotType>();
            descr.DataCenter = allocationRowset.GetValue<Schema::SlotsAllocations::DataCenter>();
            descr.ForceLocation = allocationRowset.GetValue<Schema::SlotsAllocations::ForceLocation>();
            descr.CollocationGroup = allocationRowset.GetValue<Schema::SlotsAllocations::CollocationGroup>();
            descr.ForceCollocation = allocationRowset.GetValue<Schema::SlotsAllocations::ForceCollocation>();

            auto count = allocationRowset.GetValue<Schema::SlotsAllocations::Count>();
            tenant->AddSlotsAllocation(descr, count);

            YDB_LOG_CTX_DEBUG(ctx, "Loaded required slots for tenant",
                {"descr", descr.ToString()},
                {"tenantName", tenantName});

            if (!allocationRowset.Next())
                return false;
        }

        while (!labelRowset.EndOfSet()) {
            TString tenantName = labelRowset.GetValue<Schema::SlotLabels::TenantName>();
            TString label = labelRowset.GetValue<Schema::SlotLabels::Label>();

            TTenant::TPtr tenant = Self->GetTenant(tenantName);
            Y_ABORT_UNLESS(tenant);
            tenant->AddUnusedSlotLabel(label);

            YDB_LOG_CTX_DEBUG(ctx, "Loaded slot label for tenant",
                {"label", label},
                {"tenantName", tenantName});

            if (!labelRowset.Next())
                return false;
        }

        while (!slotRowset.EndOfSet()) {
            TSlotId id(slotRowset.GetValue<Schema::Slots::NodeId>(),
                       slotRowset.GetValue<Schema::Slots::SlotId>());
            TString slotType = slotRowset.GetValue<Schema::Slots::SlotType>();
            TString assignedTenantName = slotRowset.GetValue<Schema::Slots::AssignedTenant>();
            TString dataCenter;

            if (slotRowset.HaveValue<Schema::Slots::DataCenterName>()) {
                dataCenter = slotRowset.GetValue<Schema::Slots::DataCenterName>();
            } else {
                dataCenter = DataCenterToString(slotRowset.GetValue<Schema::Slots::DataCenter>());
            }

            Y_ABORT_UNLESS(!Self->Slots.contains(id));
            TSlot::TPtr slot = new TSlot(id, slotType, dataCenter);
            slot->LastRequestId = reqId;
            Self->AddSlot(slot);

            if (assignedTenantName) {
                Y_ABORT_UNLESS(!slot->IsBanned);
                TSlotDescription usedAs;
                usedAs.SlotType = slotRowset.GetValue<Schema::Slots::UsedAsType>();
                if (slotRowset.HaveValue<Schema::Slots::UsedAsDataCenterName>()) {
                    usedAs.DataCenter = slotRowset.GetValue<Schema::Slots::UsedAsDataCenterName>();
                    usedAs.ForceLocation = slotRowset.GetValue<Schema::Slots::UsedAsForceLocation>();
                    usedAs.CollocationGroup = slotRowset.GetValue<Schema::Slots::UsedAsCollocationGroup>();
                    usedAs.ForceCollocation = slotRowset.GetValue<Schema::Slots::UsedAsForceCollocation>();
                } else {
                    auto dc = slotRowset.GetValue<Schema::Slots::UsedAsDataCenter>();
                    usedAs.DataCenter = DataCenterToString(dc);
                }
                auto label = slotRowset.GetValue<Schema::Slots::Label>();
                auto tenant = Self->GetTenant(assignedTenantName);
                Y_ABORT_UNLESS(tenant);

                Self->AttachSlotNoConfigureNoDb(slot, tenant, usedAs, label);
            }

            YDB_LOG_CTX_DEBUG(ctx, "Loaded",
                {"#_slot->IdString(true)", slot->IdString(true)});

            if (!slotRowset.Next())
                return false;
        }

        // Determine preferred data centers for collocation groups and
        // fill unhappy tenants.
        for (auto &pr : Self->Tenants) {
            pr.second->DetermineDataCenterForCollocationGroups();
            Self->AddUnhappyTenant(pr.second);
        }

        // Request node info to check slot's data center.
        for (auto &pr : Self->SlotsByNodeId) {
            YDB_LOG_CTX_DEBUG(ctx, "Taking ownership of tenant pool on node",
                {"first", pr.first});

            ctx.Send(MakeTenantPoolID(pr.first), new TEvTenantPool::TEvTakeOwnership(Self->Generation()));
            ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(pr.first));

            for (auto& slot : pr.second) {
                const ui64 randomDelay = TAppData::RandomProvider->GenRand64() % Self->PendingTimeout.GetValue();
                const TDuration pendingTimeout = Self->PendingTimeout + TDuration::FromValue(randomDelay);
                ctx.Schedule(pendingTimeout, new TEvPrivate::TEvCheckSlotStatus(slot, reqId));
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxLoadState Complete");

        Self->SwitchToWork(ctx);
        Self->TxCompleted(this, ctx);
    }

private:
};

ITransaction *TTenantSlotBroker::CreateTxLoadState()
{
    return new TTxLoadState(this);
}

} // NTenantSlotBroker
} // NKikimr
