#include "tenant_slot_broker_impl.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/config_helpers.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/nameservice.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace NTenantSlotBroker {

const ui32 TTenantSlotBroker::ConfigKey_Config = 1;

void TTenantSlotBroker::TCollocationGroup::SetPreferredDataCenter(const TString &dc)
{
    PreferredDataCenter = dc;
    for (auto allocation : Allocations)
        allocation->RecomputeSplitCount();
}

bool TTenantSlotBroker::TSlotsAllocation::IsSlotOk(const TString &type,
                                                   const TString &dc,
                                                   bool strictMatch) const
{
    if (type != Description.SlotType
        && Description.SlotType != ANY_SLOT_TYPE)
        return false;
    if (dc != Description.DataCenter
        && Description.DataCenter != ANY_DATA_CENTER) {
        if (strictMatch || Description.ForceLocation)
            return false;
    }
    if (Group) {
        if (Group->GetPreferredDataCenter() != dc
            && Group->GetPreferredDataCenter() != ANY_DATA_CENTER) {
            if (strictMatch || Description.ForceCollocation)
                return false;
        }
    }
    return true;
}

void TTenantSlotBroker::TSlotsAllocation::RecomputeSplitCount()
{
    ui64 count = 0;
    if (Group) {
        for (auto slot : AssignedSlots) {
            if (slot->DataCenter != Group->GetPreferredDataCenter())
                ++count;
        }
    }
    SetSplit(count);
}

const TSlotDescription TTenantSlotBroker::TTenant::PinnedSlotDescription = {PIN_DATA_CENTER, PIN_SLOT_TYPE};

void TTenantSlotBroker::TTenant::AddSlotsAllocation(const TSlotDescription &descr,
                                                    ui64 count)
{
    Y_ABORT_UNLESS(!Allocations.contains(descr));
    TSlotsAllocation::TPtr allocation = new TSlotsAllocation(descr, Stats);
    Allocations[descr] = allocation;
    if (descr.CollocationGroup
        && (descr.DataCenter == ANY_DATA_CENTER
            || !descr.ForceLocation)) {
        auto group = GetOrCreateCollocationGroup(descr.CollocationGroup);
        group->Allocations.insert(allocation);
        allocation->Group = group;
    }
    allocation->SetRequired(count);
    allocation->SetMissing(count);
}

TTenantSlotBroker::TSlotsAllocation::TPtr
TTenantSlotBroker::TTenant::GetOrCreateAllocation(const TSlotDescription &descr)
{
    if (!Allocations.contains(descr))
        AddSlotsAllocation(descr, 0);
    return Allocations.at(descr);
}

void TTenantSlotBroker::TTenant::ClearEmptyAllocations()
{
    auto it = Allocations.begin();
    while (it != Allocations.end()) {
        auto allocation = it->second;

        if (allocation->RequiredCount
            || allocation->PinnedCount) {
            ++it;
            continue;
        }

        if (allocation->Group)
            allocation->Group->Allocations.erase(allocation);

        Y_ABORT_UNLESS(!allocation->MissingCount);
        Y_ABORT_UNLESS(!allocation->PendingCount);
        auto cur = it;
        ++it;
        Allocations.erase(cur);
    }
}

TTenantSlotBroker::TCollocationGroup::TPtr
TTenantSlotBroker::TTenant::GetOrCreateCollocationGroup(ui32 group)
{
    auto it = Groups.find(group);
    if (it != Groups.end())
        return it->second;

    TCollocationGroup::TPtr res = new TCollocationGroup(group);
    Groups[group] = res;
    return res;
}

void TTenantSlotBroker::TTenant::DetermineDataCenterForCollocationGroups()
{
    for (auto &pr : Groups) {
        THashMap<TString, ui32> slots;

        for (auto allocation : pr.second->Allocations) {
            for (auto slot : allocation->AssignedSlots) {
                if (slot->DataCenter != ANY_DATA_CENTER)
                    slots[slot->DataCenter] += 1;
            }
        }

        // If there are no assigned slots then leave it until we are
        // going to assign some slot.
        if (slots.empty())
            continue;

        auto max = slots.begin();
        for (auto it = slots.begin(); it != slots.end(); ++it)
            if (it->second > max->second)
                max = it;

        pr.second->SetPreferredDataCenter(max->first);
    }
}

TString TTenantSlotBroker::TTenant::SlotLabelByNo(ui64 no) const
{
    return Sprintf("slot-%" PRIu64, no);
}

void TTenantSlotBroker::TTenant::AddSlotLabels(ui64 count,
                                               TTransactionContext &txc)
{
    NIceDb::TNiceDb db(txc.DB);
    ui64 no = 1;
    while (count) {
        auto label = SlotLabelByNo(no);
        if (!UsedSlotLabels.contains(label) && !UnusedSlotLabels.contains(label)) {
            db.Table<Schema::SlotLabels>().Key(Name, label).Update();
            UnusedSlotLabels.insert(label);
            --count;
        }
        ++no;
    }
}

void TTenantSlotBroker::TTenant::RemoveSlotLabels(ui64 count,
                                                  TTransactionContext &txc)
{
    Y_ABORT_UNLESS(UnusedSlotLabels.size() >= count);
    NIceDb::TNiceDb db(txc.DB);
    auto first = UnusedSlotLabels.begin();
    std::advance(first, UnusedSlotLabels.size() - count);
    for (auto it = first; it != UnusedSlotLabels.end(); ++it)
        db.Table<Schema::SlotLabels>().Key(Name, *it).Delete();
    UnusedSlotLabels.erase(first, UnusedSlotLabels.end());
}

TString TTenantSlotBroker::TTenant::MakePinnedSlotLabel()
{
    ui64 no = 1;
    while (true) {
        auto label = Sprintf("pinned-%" PRIu64, no);
        if (!PinnedSlotLabels.contains(label)) {
            AddPinnedSlotLabel(label);
            return label;
        }
        ++no;
    }
}

void TTenantSlotBroker::TTenant::AddPinnedSlotLabel(const TString &label)
{
    Y_ABORT_UNLESS(!PinnedSlotLabels.contains(label));
    PinnedSlotLabels.insert(label);
}

void TTenantSlotBroker::TTenant::RemovePinnedSlotLabel(const TString &label)
{
    Y_ABORT_UNLESS(PinnedSlotLabels.contains(label));
    PinnedSlotLabels.erase(label);
}

void TTenantSlotBroker::TTenant::MarkSlotLabelAsUsed(const TString &label)
{
    Y_ABORT_UNLESS(UnusedSlotLabels.contains(label));
    Y_ABORT_UNLESS(!UsedSlotLabels.contains(label));

    UnusedSlotLabels.erase(label);
    UsedSlotLabels.insert(label);
}

void TTenantSlotBroker::TTenant::MarkSlotLabelAsUnused(const TString &label)
{
    Y_ABORT_UNLESS(!UnusedSlotLabels.contains(label));
    Y_ABORT_UNLESS(UsedSlotLabels.contains(label));

    UnusedSlotLabels.insert(label);
    UsedSlotLabels.erase(label);
}

TString TTenantSlotBroker::TTenant::GetFirstUnusedSlotLabel() const
{
    Y_ABORT_UNLESS(!UnusedSlotLabels.empty(), "tenant %s has no unused slot labels", Name.data());
    return *UnusedSlotLabels.begin();
}

void TTenantSlotBroker::TTenant::AddUnusedSlotLabel(const TString &label)
{
    UnusedSlotLabels.insert(label);
}

void TTenantSlotBroker::TTenant::AddPinnedSlot(TSlot::TPtr slot)
{
    Y_ABORT_UNLESS(!slot->AssignedTenant);
    auto allocation = GetOrCreateAllocation(PinnedSlotDescription);
    allocation->AddAssignedSlot(slot);
    allocation->IncPinned();
    slot->AssignedTenant = this;
    slot->UsedAs = PinnedSlotDescription;
    if (slot->IsPending())
        allocation->IncPending();
}

void TTenantSlotBroker::TTenant::RemovePinnedSlot(TSlot::TPtr slot)
{
    Y_ABORT_UNLESS(slot->AssignedTenant == this);
    auto allocation = Allocations.at(PinnedSlotDescription);
    allocation->RemoveAssignedSlot(slot);
    allocation->DecPinned();

    if (slot->IsPending())
        allocation->DecPending();
    slot->AssignedTenant = nullptr;

    if (!allocation->PinnedCount)
        Allocations.erase(PinnedSlotDescription);
}

bool TTenantSlotBroker::TTenant::CanBeRemoved() const
{
    return !GetTotalRequired() && PinnedSlotLabels.empty();
}

void TTenantSlotBroker::TTenant::DbUpdateAllocation(const TSlotDescription &key,
                                                    TTransactionContext &txc)
{
    auto allocation = Allocations.at(key);
    auto &loc = allocation->Description;
    NIceDb::TNiceDb db(txc.DB);

    // Check if slot can be written in the old format to support downgrade.
    // TODO: remove this code and use only new format in 18.8.
    if (loc.DataCenter.size() <= 4
        && loc.ForceLocation
        && !loc.CollocationGroup
        && !loc.ForceCollocation) {
        auto row = db.Table<Schema::RequiredSlots>().Key(Name,
                                                         allocation->Description.SlotType,
                                                         DataCenterFromString(loc.DataCenter));
        if (allocation->RequiredCount)
            row.Update(NIceDb::TUpdate<Schema::RequiredSlots::Count>(allocation->RequiredCount));
        else
            row.Delete();
    } else {
        auto row = db.Table<Schema::SlotsAllocations>().Key(Name,
                                                            allocation->Description.SlotType,
                                                            loc.DataCenter,
                                                            loc.CollocationGroup,
                                                            loc.ForceLocation,
                                                            loc.ForceCollocation);
        if (allocation->RequiredCount)
            row.Update(NIceDb::TUpdate<Schema::SlotsAllocations::Count>(allocation->RequiredCount));
        else
            row.Delete();
    }
}

void TTenantSlotBroker::TFreeSlotsIndex::Add(TSlot::TPtr slot)
{
    FreeSlotsByType[slot->Type][slot->DataCenter].insert(slot);
    FreeSlotsByDataCenter[slot->DataCenter][slot->Type].insert(slot);
}

void TTenantSlotBroker::TFreeSlotsIndex::Remove(TSlot::TPtr slot)
{
    auto it1 = FreeSlotsByType.find(slot->Type);
    Y_ABORT_UNLESS(it1 != FreeSlotsByType.end());
    auto it2 = it1->second.find(slot->DataCenter);
    Y_ABORT_UNLESS(it2 != it1->second.end());
    it2->second.erase(slot);
    if (it2->second.empty()) {
        it1->second.erase(it2);
        if (it1->second.empty())
            FreeSlotsByType.erase(it1);
    }

    auto it3 = FreeSlotsByDataCenter.find(slot->DataCenter);
    Y_ABORT_UNLESS(it3 != FreeSlotsByDataCenter.end());
    auto it4 = it3->second.find(slot->Type);
    Y_ABORT_UNLESS(it4 != it3->second.end());
    it4->second.erase(slot);
    if (it4->second.empty()) {
        it3->second.erase(it4);
        if (it3->second.empty())
            FreeSlotsByDataCenter.erase(it3);
    }
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::TFreeSlotsIndex::Find()
{
    if (FreeSlotsByType.empty())
        return nullptr;

    auto maxIt = FreeSlotsByType.begin();
    ui64 maxCount = 0;
    for (auto &pr : maxIt->second)
        maxCount += pr.second.size();

    auto it = maxIt;
    for (++it; it != FreeSlotsByType.end(); ++it) {
        ui64 count = 0;
        for (auto &pr : it->second)
            count += pr.second.size();
        if (count > maxCount) {
            maxCount = count;
            maxIt = it;
        }
    }

    return FindByType(maxIt->first);
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::TFreeSlotsIndex::FindByType(const TString &type)
{
    auto it1 = FreeSlotsByType.find(type);
    if (it1 == FreeSlotsByType.end())
        return nullptr;

    auto maxIt = it1->second.begin();
    auto it2 = maxIt;
    for (++it2; it2 != it1->second.end(); ++it2)
        if (it2->second.size() > maxIt->second.size())
            maxIt = it2;

    return *maxIt->second.begin();
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::TFreeSlotsIndex::FindByDC(const TString &dataCenter)
{
    auto it1 = FreeSlotsByDataCenter.find(dataCenter);
    if (it1 == FreeSlotsByDataCenter.end())
        return nullptr;

    auto maxIt = it1->second.begin();
    auto it2 = maxIt;
    for (++it2; it2 != it1->second.end(); ++it2)
        if (it2->second.size() > maxIt->second.size())
            maxIt = it2;

    return *maxIt->second.begin();
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::TFreeSlotsIndex::Find(const TString &type,
                                                                        const TString &dataCenter)
{
    if (type == ANY_SLOT_TYPE && dataCenter == ANY_DATA_CENTER)
        return Find();
    if (type == ANY_SLOT_TYPE)
        return FindByDC(dataCenter);
    if (dataCenter == ANY_DATA_CENTER)
        return FindByType(type);

    auto it1 = FreeSlotsByType.find(type);
    if (it1 == FreeSlotsByType.end())
        return nullptr;

    auto it2 = it1->second.find(dataCenter);
    if (it2 == it1->second.end())
        return nullptr;

    return *it2->second.begin();
}

ui64 TTenantSlotBroker::Generation() const {
    return Executor()->Generation();
}

void TTenantSlotBroker::DefaultSignalTabletActive(const TActorContext &)
{
    // must be empty
}

void TTenantSlotBroker::OnActivateExecutor(const TActorContext &ctx)
{
    RequestId = Now().GetValue();

    DomainName = AppData()->DomainsInfo->GetDomain()->Name;

    auto tabletCounters = GetServiceCounters(AppData(ctx)->Counters, "tablets");
    tabletCounters->RemoveSubgroup("type", "TENANT_SLOT_BROKER");
    Counters = new TCounters(tabletCounters->GetSubgroup("type", "TENANT_SLOT_BROKER"));

    ProcessTx(CreateTxInitScheme(), ctx);
}

void TTenantSlotBroker::OnDetach(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "OnDetach");
    Die(ctx);
}

void TTenantSlotBroker::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &,
                                     const TActorContext &ctx)
{
    LOG_INFO(ctx, NKikimrServices::TENANT_SLOT_BROKER, "OnTabletDead: %" PRIu64, TabletID());

    if (Counters)
        Counters->Counters->ResetCounters();

    Die(ctx);
}

bool TTenantSlotBroker::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev,
                                            const TActorContext &ctx)
{
    if (!ev)
        return true;

    TStringStream str;
    HTML(str) {
        PRE() {
            str << "Served domain: " << AppData(ctx)->DomainsInfo->GetDomain()->Name << Endl
                << "Used timeout for pending slots: " << PendingTimeout.ToString() << Endl
                << "PendingAssignFreeSlots: " << PendingAssignFreeSlots << Endl;

            str << "Node ID to Data Center map:";
            ui64 count = 0;
            TSet<ui32> nodes;
            for (auto &pr : NodeIdToDataCenter)
                nodes.insert(pr.first);
            for (auto node : nodes) {
                if (count % 10 == 0)
                    str << Endl;
                str << "    " << node << " -> " << NodeIdToDataCenter.at(node);
                ++count;
            }
            str << Endl << Endl;

            TSet<TString> names;
            str << "Tenants:" << Endl;
            for (auto &pr : Tenants)
                names.insert(pr.first);
            for (auto &name : names) {
                auto tenant = Tenants.at(name);
                str << " - " << tenant->Name << Endl
                    << "   Total required slots: " << tenant->GetTotalRequired() << Endl
                    << "   Total missing slots: " << tenant->GetTotalMissing() << Endl
                    << "   Total pending slots: " << tenant->GetTotalPending() << Endl
                    << "   Total misplaced slots: " << tenant->GetTotalMisplaced() << Endl
                    << "   Total split slots: " << tenant->GetTotalSplit() << Endl;

                if (tenant->GetTotalRequired())
                    str << "   Slots (allocated + pending + missing [misplaced, split]):" << Endl;

                for (auto &pr : tenant->GetAllocations()) {
                    auto allocation = pr.second;
                    ui64 required = allocation->RequiredCount;
                    ui64 pending = allocation->PendingCount;
                    ui64 missing = allocation->MissingCount;
                    ui64 misplaced = allocation->MisplacedCount;
                    ui64 split = allocation->SplitCount;
                    ui64 pinned = allocation->PinnedCount;
                    ui64 allocated = required - pending - missing;
                    if (pinned) {
                        str << "     Pinned slots:";
                    } else {
                        str << "     " << allocation->Description.ToString() << ": "
                            << allocated << " + " << pending << " + " << missing
                            << " [" << misplaced << ", " << split << "]" << Endl;
                        str << "     Assigned slots:";
                    }
                    count = 0;
                    for (auto &slot : allocation->AssignedSlots) {
                        if (count && count % 5 == 0)
                            str << Endl << "                    ";
                        str << " " << slot->IdString();
                        ++count;
                    }
                    str << Endl;
                }
            }
            str << Endl;

            str << "Unhappy tenants:";
            for (auto &tenant : UnhappyTenants)
                str << " " << tenant->Name;
            str << Endl << Endl;

            str << "Slots:" << Endl;
            TSet<TSlotId> keys;
            for (auto &pr : Slots)
                keys.insert(pr.first);
            for (auto &key : keys) {
                auto slot = Slots.at(key);
                str << "  " << slot->IdString() << ": " << slot->Type << " in " << slot->DataCenter
                    << (slot->IsConnected ? " connected " : " disconnected ");
                if (slot->AssignedTenant)
                    str << "used by " << slot->AssignedTenant->Name << " as " << slot->UsedAs.ToString()
                        << " labeled '" << slot->Label << "'";
                else
                    str << "free";
                if (slot->IsBanned)
                    str << " banned";
                if (slot->IsPinned)
                    str << " pinned";
                if (slot->LastRequestId)
                    str << " pending assignment confirmation (RequestId: " << slot->LastRequestId << ")";
                str << Endl;
            }
            str << Endl;

            str << "Free slots by type:" << Endl;
            for (auto &pr1 : FreeSlots.FreeSlotsByType) {
                str << " - " << pr1.first << Endl;
                for (auto &pr2 : pr1.second) {
                    str << "     Data Center " << pr2.first << ":";
                    count = 0;
                    for (auto &slot : pr2.second) {
                        if (count && count % 5 == 0)
                            str << Endl << "                   ";
                        str << " " << slot->IdString();
                        ++count;
                    }
                    str << Endl;
                }
            }
            str << Endl;

            str << "Free slots by data center:" << Endl;
            for (auto &pr1 : FreeSlots.FreeSlotsByDataCenter) {
                str << " - Data Center " << pr1.first << Endl;
                for (auto &pr2 : pr1.second) {
                    str << "     " << pr2.first << ":";
                    count = 0;
                    for (auto &slot : pr2.second) {
                        if (count && count % 5 == 0)
                            str << Endl << "            ";
                        str << " " << slot->IdString();
                        ++count;
                    }
                    str << Endl;
                }
            }
            str << Endl;

            str << "Slots by node ID:" << Endl;
            nodes.clear();
            for (auto pr: SlotsByNodeId)
                nodes.insert(pr.first);
            for (auto node : nodes) {
                str << "  " << node << ":";
                for (auto &slot : SlotsByNodeId.at(node))
                    str << " " << slot->IdString();
                str << Endl;
            }
            str << Endl;

            str << "Tenant Slot Broker counters:" << Endl;
            Counters->AllCounters()->OutputPlainText(str, "   ");
            str << Endl;

            str << "Known pool pipes:";
            count = 0;
            for (auto &pipe : KnownPoolPipes) {
                if (count % 10 == 0)
                    str << Endl;
                str << "    " << pipe;
            }
            str << Endl;
        }
    }
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    return true;
}

void TTenantSlotBroker::Cleanup(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "Cleanup");

    NConsole::UnsubscribeViaConfigDispatcher(ctx, ctx.SelfID);
}

void TTenantSlotBroker::Die(const TActorContext &ctx)
{
    Cleanup(ctx);
    TActorBase::Die(ctx);
}

void TTenantSlotBroker::LoadConfigFromProto(const NKikimrTenantSlotBroker::TConfig &config)
{
    Config = config;
    PendingTimeout = TDuration::MicroSeconds(config.GetPendingSlotTimeout());
}

void TTenantSlotBroker::SwitchToWork(const TActorContext &ctx)
{
    Become(&TTenantSlotBroker::StateWork);
    SignalTabletActive(ctx);

    NConsole::SubscribeViaConfigDispatcher(ctx, {(ui32)NKikimrConsole::TConfigItem::TenantSlotBrokerConfigItem}, ctx.SelfID);
}

void TTenantSlotBroker::ClearState()
{
    // Break cross-references.
    for (auto &pr : Slots)
        pr.second->AssignedTenant = nullptr;

    NodeIdToDataCenter.clear();
    Tenants.clear();
    Slots.clear();
    SlotsByNodeId.clear();
    FreeSlots.FreeSlotsByType.clear();
    FreeSlots.FreeSlotsByDataCenter.clear();
    Tenants.clear();
    UnhappyTenants.clear();
    if (Counters)
        Counters->Clear();
}

TTenantSlotBroker::TTenant::TPtr TTenantSlotBroker::GetTenant(const TString &name)
{
    auto it = Tenants.find(name);
    if (it != Tenants.end())
        return it->second;
    return nullptr;
}

TTenantSlotBroker::TTenant::TPtr TTenantSlotBroker::AddTenant(const TString &name)
{
    Y_ABORT_UNLESS(!Tenants.contains(name));
    auto tenant = new TTenant(name, Counters);
    Tenants.emplace(name, tenant);
    *Counters->KnownTenants = Tenants.size();
    return tenant;
}

TTenantSlotBroker::TTenant::TPtr TTenantSlotBroker::GetOrCreateTenant(const TString &name)
{
    auto tenant = GetTenant(name);
    if (!tenant)
        tenant = AddTenant(name);
    return tenant;
}

void TTenantSlotBroker::MaybeRemoveTenant(TTenant::TPtr tenant)
{
    if (!tenant->CanBeRemoved())
        return;

    LOG_DEBUG_S(TActorContext::AsActorContext(), NKikimrServices::TENANT_SLOT_BROKER,
                "Remove tenant " << tenant->Name);
    Tenants.erase(tenant->Name);
    *Counters->KnownTenants = Tenants.size();
}

void TTenantSlotBroker::AddUnhappyTenant(TTenant::TPtr tenant)
{
    if (tenant->GetTotalMissing()) {
        UnhappyTenants.insert(tenant);
        *Counters->UnhappyTenants = UnhappyTenants.size();
    }
    if (tenant->GetTotalMisplaced()) {
        MisplacedTenants.insert(tenant);
        *Counters->MisplacedTenants = MisplacedTenants.size();
    }
    if (tenant->GetTotalSplit()) {
        SplitTenants.insert(tenant);
        *Counters->SplitTenants = SplitTenants.size();
    }
}

void TTenantSlotBroker::RemoveUnhappyTenant(TTenant::TPtr tenant)
{
    if (tenant->GetTotalMissing()) {
        UnhappyTenants.erase(tenant);
        *Counters->UnhappyTenants = UnhappyTenants.size();
    }
    if (tenant->GetTotalMisplaced()) {
        MisplacedTenants.erase(tenant);
        *Counters->MisplacedTenants = MisplacedTenants.size();
    }
    if (tenant->GetTotalSplit()) {
        SplitTenants.erase(tenant);
        *Counters->SplitTenants = SplitTenants.size();
    }
}

bool TTenantSlotBroker::HasUnhappyTenant() const
{
    return (!UnhappyTenants.empty()
            || !MisplacedTenants.empty()
            || !SplitTenants.empty());
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::GetSlot(const TSlotId &id)
{
    auto it = Slots.find(id);
    if (it != Slots.end())
        return it->second;
    return nullptr;
}

TTenantSlotBroker::TSlot::TPtr TTenantSlotBroker::GetSlot(ui32 nodeId,
                                                          const TString &id)
{
    return GetSlot(TSlotId(nodeId, id));
}

void TTenantSlotBroker::AddSlot(TSlot::TPtr slot)
{
    Slots.emplace(slot->Id, slot);
    SlotsByNodeId[slot->Id.NodeId].insert(slot);

    if (slot->IsFree())
        FreeSlots.Add(slot);

    AddSlotToCounters(slot);
}

void TTenantSlotBroker::AddSlot(TSlot::TPtr slot,
                                TTransactionContext &txc,
                                const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Added new " << slot->IdString(true));

    Y_ABORT_UNLESS(!slot->AssignedTenant);

    AddSlot(slot);

    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId);
    row.Update(NIceDb::TUpdate<Schema::Slots::SlotType>(slot->Type.data()),
               NIceDb::TUpdate<Schema::Slots::DataCenter>(DataCenterFromString(slot->DataCenter)),
               NIceDb::TUpdate<Schema::Slots::DataCenterName>(slot->DataCenter),
               NIceDb::TUpdate<Schema::Slots::AssignedTenant>(""),
               NIceDb::TUpdate<Schema::Slots::UsedAsType>(""),
               NIceDb::TUpdate<Schema::Slots::UsedAsDataCenter>(0),
               NIceDb::TUpdate<Schema::Slots::UsedAsDataCenterName>(""),
               NIceDb::TUpdate<Schema::Slots::UsedAsCollocationGroup>(0),
               NIceDb::TUpdate<Schema::Slots::UsedAsForceLocation>(true),
               NIceDb::TUpdate<Schema::Slots::UsedAsForceCollocation>(false));
}

void TTenantSlotBroker::SlotConnected(TSlot::TPtr slot)
{
    if (!slot->IsConnected) {
        slot->IsConnected = true;
        Counters->ConnectedSlots(slot->Type, slot->DataCenter)->Inc();
        Counters->DisconnectedSlots(slot->Type, slot->DataCenter)->Dec();
    }
}

void TTenantSlotBroker::SlotDisconnected(TSlot::TPtr slot)
{
    if (slot->IsConnected) {
        slot->IsConnected = false;
        Counters->ConnectedSlots(slot->Type, slot->DataCenter)->Dec();
        Counters->DisconnectedSlots(slot->Type, slot->DataCenter)->Inc();
    }
}

bool TTenantSlotBroker::UpdateSlotDataCenter(TSlot::TPtr slot,
                                             const TString &dataCenter,
                                             TTransactionContext &txc,
                                             const TActorContext &ctx)
{
    if (slot->DataCenter == dataCenter)
        return false;

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Update data center (" << dataCenter << ") for "
                << slot->IdString(true));

    bool res = false;
    // Check if slot has to be detached due to updated data center.
    if (slot->AssignedTenant
        && !slot->IsPinned
        && (slot->UsedAs.DataCenter != ANY_DATA_CENTER
            || slot->UsedAs.CollocationGroup)) {
        DetachSlot(slot, txc, ctx);
        res = true;
    }

    if (slot->IsFree())
        FreeSlots.Remove(slot);

    RemoveSlotFromCounters(slot);

    slot->DataCenter = dataCenter;

    AddSlotToCounters(slot);

    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId);
    row.Update(NIceDb::TUpdate<Schema::Slots::DataCenterName>(dataCenter));
    row.Update(NIceDb::TUpdate<Schema::Slots::DataCenter>(DataCenterFromString(dataCenter)));

    if (slot->IsFree()) {
        FreeSlots.Add(slot);
        res = true;
    }

    return res;
}

bool TTenantSlotBroker::UpdateSlotType(TSlot::TPtr slot,
                                       const TString &type,
                                       TTransactionContext &txc,
                                       const TActorContext &ctx)
{
    if (slot->Type == type)
        return false;

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Update type (" << type << ") for " << slot->IdString(true));

    bool res = false;
    // Check if slot has to be detached due to updated type.
    if (slot->AssignedTenant
        && !slot->IsPinned
        && slot->UsedAs.SlotType != ANY_SLOT_TYPE
        && slot->UsedAs.SlotType != type) {
        DetachSlot(slot, txc, ctx);
        res = true;
    }

    if (slot->IsFree())
        FreeSlots.Remove(slot);

    RemoveSlotFromCounters(slot);

    slot->Type = type;

    AddSlotToCounters(slot);

    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId);
    row.Update(NIceDb::TUpdate<Schema::Slots::SlotType>(type));

    if (slot->IsFree()) {
        FreeSlots.Add(slot);
        res = true;
    }

    return res;
}

void TTenantSlotBroker::RemoveSlot(TSlot::TPtr slot,
                                   TTransactionContext &txc,
                                   const TActorContext &/*ctx*/)
{
    if (slot->IsPinned)
        slot->AssignedTenant->RemovePinnedSlot(slot);
    else if (slot->AssignedTenant)
        DetachSlotNoConfigureNoDb(slot);
    if (slot->IsFree())
        FreeSlots.Remove(slot);
    Slots.erase(slot->Id);

    RemoveSlotFromCounters(slot);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId).Delete();

    auto &slots = SlotsByNodeId[slot->Id.NodeId];
    slots.erase(slot);
    if (slots.empty()) {
        SlotsByNodeId.erase(slot->Id.NodeId);
    }
}

void TTenantSlotBroker::AddSlotToCounters(TSlot::TPtr slot)
{
    if (slot->IsConnected)
        Counters->ConnectedSlots(slot->Type, slot->DataCenter)->Inc();
    else
        Counters->DisconnectedSlots(slot->Type, slot->DataCenter)->Inc();

    if (slot->IsPinned)
        Counters->PinnedSlots(slot->Type, slot->DataCenter)->Inc();
    else if (slot->AssignedTenant)
        Counters->AssignedSlots(slot->Type, slot->DataCenter)->Inc();
    else if (slot->IsBanned)
        Counters->BannedSlots(slot->Type, slot->DataCenter)->Inc();
    else
        Counters->FreeSlots(slot->Type, slot->DataCenter)->Inc();
}

void TTenantSlotBroker::RemoveSlotFromCounters(TSlot::TPtr slot)
{
    if (slot->IsConnected)
        Counters->ConnectedSlots(slot->Type, slot->DataCenter)->Dec();
    else
        Counters->DisconnectedSlots(slot->Type, slot->DataCenter)->Dec();

    if (slot->IsPinned)
        Counters->PinnedSlots(slot->Type, slot->DataCenter)->Dec();
    else if (slot->AssignedTenant)
        Counters->AssignedSlots(slot->Type, slot->DataCenter)->Dec();
    else if (slot->IsBanned)
        Counters->BannedSlots(slot->Type, slot->DataCenter)->Dec();
    else
        Counters->FreeSlots(slot->Type, slot->DataCenter)->Dec();
}

void TTenantSlotBroker::DetachSlotNoConfigureNoDb(TSlot::TPtr slot,
                                                  bool updateUnhappy)
{
    auto tenant = slot->AssignedTenant;
    if (!tenant)
        return;

    if (updateUnhappy)
        RemoveUnhappyTenant(tenant);

    Y_DEBUG_ABORT_UNLESS(!UnhappyTenants.contains(tenant));
    Y_DEBUG_ABORT_UNLESS(!MisplacedTenants.contains(tenant));
    Y_DEBUG_ABORT_UNLESS(!SplitTenants.contains(tenant));

    auto allocation = tenant->GetAllocation(slot->UsedAs);
    allocation->RemoveAssignedSlot(slot);
    if (slot->IsPending())
        allocation->DecPending();
    allocation->IncMissing();
    if (allocation->Description.DataCenter != ANY_DATA_CENTER
        && allocation->Description.DataCenter != slot->DataCenter)
        allocation->DecMisplaced();
    if (allocation->Group
        && allocation->Group->GetPreferredDataCenter() != slot->DataCenter)
        allocation->DecSplit();

    slot->AssignedTenant = nullptr;
    tenant->MarkSlotLabelAsUnused(slot->Label);

    Counters->AssignedSlots(slot->Type, slot->DataCenter)->Dec();
    Counters->FreeSlots(slot->Type, slot->DataCenter)->Inc();

    if (updateUnhappy)
        AddUnhappyTenant(tenant);
    if (slot->IsFree())
        FreeSlots.Add(slot);
}

void TTenantSlotBroker::DetachSlotNoConfigure(TSlot::TPtr slot,
                                              TTransactionContext &txc,
                                              bool updateUnhappy)
{
    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId);
    row.Update(NIceDb::TUpdate<Schema::Slots::AssignedTenant>(""),
               NIceDb::TUpdate<Schema::Slots::Label>(""));

    DetachSlotNoConfigureNoDb(slot, updateUnhappy);
}

void TTenantSlotBroker::DetachSlot(TSlot::TPtr slot,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx,
                                   bool updateUnhappy)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Detach tenant from " << slot->IdString(true));

    DetachSlotNoConfigure(slot, txc, updateUnhappy);

    if (slot->IsConnected)
        SendConfigureSlot(slot, ctx);
}

void TTenantSlotBroker::AttachSlotNoConfigureNoDb(TSlot::TPtr slot,
                                                  TTenant::TPtr tenant,
                                                  const TSlotDescription &usedAs,
                                                  const TString &label)
{
    Y_ABORT_UNLESS(!slot->AssignedTenant);
    Y_DEBUG_ABORT_UNLESS(!UnhappyTenants.contains(tenant));

    if (slot->IsFree())
        FreeSlots.Remove(slot);

    slot->AssignedTenant = tenant;
    slot->UsedAs = usedAs;
    slot->Label = label;

    tenant->MarkSlotLabelAsUsed(label);

    auto allocation = tenant->GetAllocation(usedAs);
    allocation->AddAssignedSlot(slot);
    allocation->DecMissing();
    if (allocation->Description.DataCenter != ANY_DATA_CENTER
        && allocation->Description.DataCenter != slot->DataCenter)
        allocation->IncMisplaced();
    if (allocation->Group
        && allocation->Group->GetPreferredDataCenter() != slot->DataCenter)
        allocation->IncSplit();

    Counters->FreeSlots(slot->Type, slot->DataCenter)->Dec();
    Counters->AssignedSlots(slot->Type, slot->DataCenter)->Inc();

    if (slot->IsPending())
        allocation->IncPending();
}

void TTenantSlotBroker::AttachSlotNoConfigure(TSlot::TPtr slot,
                                              TTenant::TPtr tenant,
                                              const TSlotDescription &usedAs,
                                              const TString &label,
                                              TTransactionContext &txc)
{
    AttachSlotNoConfigureNoDb(slot, tenant, usedAs, label);

    NIceDb::TNiceDb db(txc.DB);
    auto row = db.Table<Schema::Slots>().Key(slot->Id.NodeId, slot->Id.SlotId);
    row.Update(NIceDb::TUpdate<Schema::Slots::AssignedTenant>(tenant->Name),
               NIceDb::TUpdate<Schema::Slots::UsedAsType>(usedAs.SlotType),
               NIceDb::TUpdate<Schema::Slots::UsedAsDataCenter>(DataCenterFromString(usedAs.DataCenter)),
               NIceDb::TUpdate<Schema::Slots::Label>(label),
               NIceDb::TUpdate<Schema::Slots::UsedAsDataCenterName>(usedAs.DataCenter),
               NIceDb::TUpdate<Schema::Slots::UsedAsCollocationGroup>(usedAs.CollocationGroup),
               NIceDb::TUpdate<Schema::Slots::UsedAsForceLocation>(usedAs.ForceLocation),
               NIceDb::TUpdate<Schema::Slots::UsedAsForceCollocation>(usedAs.ForceCollocation));
}

void TTenantSlotBroker::AttachSlot(TSlot::TPtr slot,
                                   TTenant::TPtr tenant,
                                   const TSlotDescription &usedAs,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Attach tenant " << tenant->Name << " to " << slot->IdString(true));

    AttachSlotNoConfigure(slot, tenant, usedAs, tenant->GetFirstUnusedSlotLabel(), txc);

    if (slot->IsConnected)
        SendConfigureSlot(slot, ctx);
}

bool TTenantSlotBroker::MoveMisplacedSlots(TTenant::TPtr tenant,
                                           TSlotsAllocation::TPtr allocation,
                                           bool singleSlot,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx)
{
    bool assigned = false;
    Y_ABORT_UNLESS(!allocation->Group);
    Y_ABORT_UNLESS(!allocation->Description.ForceLocation);
    while (allocation->MisplacedCount) {
        TSlot::TPtr freeSlot = FreeSlots.Find(allocation->Description.SlotType,
                                              allocation->Description.DataCenter);

        if (!freeSlot)
            break;

        // Find misplaced slot and detach it.
        // TODO: probably misplaced slots index should be added.
        for (auto slot : allocation->AssignedSlots) {
            if (slot->DataCenter != allocation->Description.DataCenter) {
                DetachSlot(slot, txc, ctx, false);
                break;
            }
        }

        AttachSlot(freeSlot, tenant, allocation->Description, txc, ctx);
        assigned = true;

        if (singleSlot)
            break;
    }
    return assigned;
}

bool TTenantSlotBroker::AssignFreeSlots(TTenant::TPtr tenant,
                                        bool singleSlot,
                                        TTransactionContext &txc,
                                        const TActorContext &ctx)
{
    bool assigned = false;
    THashSet<ui32> visitedGroups;
    auto prev = tenant->GetMissing().end();
    while (!tenant->GetMissing().empty()
           && !FreeSlots.FreeSlotsByDataCenter.empty()) {
        // Slots allocation for group may remove current and some of following
        // allocations from set of missing. So we keep iterator for prev which
        // is guaranteed to stay valid and use it to get current allocation.
        auto cur = prev;
        if (cur == tenant->GetMissing().end())
            cur = tenant->GetMissing().begin();
        else
            ++cur;

        if (cur == tenant->GetMissing().end())
            break;

        auto allocation = *cur;

        if (allocation->Group) {
            if (!visitedGroups.contains(allocation->Group->Id)) {
                if (AssignFreeSlotsForGroup(tenant, allocation->Group, txc, ctx))
                    assigned = true;
                visitedGroups.insert(allocation->Group->Id);
            }
        } else {
            while (allocation->MissingCount) {
                TSlot::TPtr freeSlot = FreeSlots.Find(allocation->Description.SlotType,
                                                      allocation->Description.DataCenter);

                // Try to find slot in another data center if allowed.
                if (!freeSlot && !allocation->Description.ForceLocation)
                    freeSlot = FreeSlots.Find(allocation->Description.SlotType,
                                              ANY_DATA_CENTER);

                if (!freeSlot)
                    break;

                AttachSlot(freeSlot, tenant, allocation->Description, txc, ctx);
                assigned = true;
                if (singleSlot)
                    break;
            }
        }

        if (singleSlot && assigned)
            break;

        // Move prev iterator only if current allocation was not removed.
        if (allocation->MissingCount) {
            if (prev == tenant->GetMissing().end()) {
                prev = tenant->GetMissing().begin();
                Y_ABORT_UNLESS(*prev == allocation);
            } else {
                ++prev;
            }
        }
    }

    return assigned;
}

TTenantSlotBroker::TSlot::TPtr
TTenantSlotBroker::ExtractSlot(TFreeSlotsIndex &primary,
                               TFreeSlotsIndex &secondary,
                               const TString &type,
                               const TString &dc) const
{
    auto res = primary.Find(type, dc);
    if (res) {
        primary.Remove(res);
    } else {
        res = secondary.Find(type, dc);
        if (res)
            secondary.Remove(res);
    }
    return res;
}

TTenantSlotBroker::TLayout::TPtr
TTenantSlotBroker::ComputeLayoutForGroup(TCollocationGroup::TPtr group,
                                         const TString &dc)
{
    TLayout::TPtr layout = new TLayout;
    TFreeSlotsIndex index = FreeSlots;
    // Currently assigned slots are more prioritized to avoid useless detach.
    TFreeSlotsIndex current;

    for (auto allocation : group->Allocations) {
        for (auto slot : allocation->AssignedSlots) {
            current.Add(slot);
        }
    }

    // On the first round we assign slots to allocations with no preferred
    // data center or with preferred data center equal to the group one.
    // Assign only perfectly matching slots.
    for (auto allocation : group->Allocations) {
        auto &assigned = layout->AssignedSlots[allocation];

        if (allocation->Description.DataCenter != ANY_DATA_CENTER
            && allocation->Description.DataCenter != dc)
            continue;

        while (assigned.size() < allocation->RequiredCount) {
            auto slot = ExtractSlot(current, index, allocation->Description.SlotType, dc);
            if (slot) {
                assigned.insert(slot);
            } else {
                break;
            }
        }
    }

    // On the second round we process allocations mith mismatched preferred
    // data centers. Assign only either misplaced or split slots (not both).
    for (auto allocation : group->Allocations) {
        auto &assigned = layout->AssignedSlots[allocation];

        if (allocation->Description.DataCenter == ANY_DATA_CENTER
            || allocation->Description.DataCenter == dc)
            continue;

        while (assigned.size() < allocation->RequiredCount) {
            auto slot = ExtractSlot(current, index, allocation->Description.SlotType, dc);
            if (slot) {
                assigned.insert(slot);
                ++layout->MisplacedCount;
            } else {
                break;
            }
        }

        if (allocation->Description.ForceCollocation)
            continue;

        while (assigned.size() < allocation->RequiredCount) {
            auto slot = ExtractSlot(current, index, allocation->Description.SlotType,
                                    allocation->Description.DataCenter);
            if (slot) {
                assigned.insert(slot);
                ++layout->SplitCount;
            } else {
                break;
            }
        }
    }

    // On the third round we assign the rest of slots.
    for (auto allocation : group->Allocations) {
        auto &assigned = layout->AssignedSlots[allocation];

        if (allocation->Description.ForceCollocation)
            continue;

        while (assigned.size() < allocation->RequiredCount) {
            auto slot = ExtractSlot(current, index, allocation->Description.SlotType,
                                    ANY_DATA_CENTER);
            if (slot) {
                assigned.insert(slot);
                if (allocation->Description.DataCenter != ANY_DATA_CENTER
                    && allocation->Description.DataCenter != slot->DataCenter)
                    ++layout->MisplacedCount;
                Y_ABORT_UNLESS(slot->DataCenter != dc);
                ++layout->SplitCount;
            } else {
                break;
            }
        }
    }

    // Compute total missing.
    for (auto allocation : group->Allocations)
        layout->MissingCount += allocation->RequiredCount - layout->AssignedSlots[allocation].size();
    // Compute total detached.
    for (auto &pr1 : current.FreeSlotsByType)
        for (auto &pr2 : pr1.second)
            layout->DetachCount += pr2.second.size();

    return layout;
}

double TTenantSlotBroker::ComputeLayoutPenalty(ui64 required,
                                               ui64 missing,
                                               ui64 misplaced,
                                               ui64 split,
                                               ui64 detached) const
{
    double res = 0;
    Y_UNUSED(required);
    res += (double)missing;
    res += (double)misplaced * 0.1;
    res += (double)split * 0.95;
    res += (double)detached * 0.05;
    return res;
}

void TTenantSlotBroker::ApplyLayout(TTenant::TPtr tenant,
                                    TLayout::TPtr layout,
                                    TTransactionContext &txc,
                                    const TActorContext &ctx)
{
    THashSet<TSlot::TPtr> allAssignedSlots;

    for (auto &pr : layout->AssignedSlots)
        for (auto &slot : pr.second)
            allAssignedSlots.insert(slot);

    for (auto &pr : layout->AssignedSlots) {
        auto allocation = pr.first;
        auto &slots = pr.second;

        for (auto it = allocation->AssignedSlots.begin(); it != allocation->AssignedSlots.end(); ) {
            auto slot = *(it++);
            if (!allAssignedSlots.contains(slot))
                DetachSlot(slot, txc, ctx, false);
        }

        for (auto slot : slots) {
            if (!allocation->AssignedSlots.contains(slot)) {
                if (slot->AssignedTenant) {
                    auto label = slot->Label;
                    // Currently we are not expected to change slot's owner.
                    Y_ABORT_UNLESS(slot->AssignedTenant == tenant);
                    DetachSlotNoConfigureNoDb(slot, false);
                    AttachSlotNoConfigure(slot, tenant, allocation->Description, label, txc);
                } else {
                    AttachSlot(slot, tenant, allocation->Description, txc, ctx);
                }
            }
        }
    }
}

bool TTenantSlotBroker::AssignFreeSlotsForGroup(TTenant::TPtr tenant,
                                                TCollocationGroup::TPtr group,
                                                TTransactionContext &txc,
                                                const TActorContext &ctx)
{
    // DataCenter -> Layout
    THashMap<TString, TLayout::TPtr> layouts;

    THashSet<TString> preferredDCs;
    ui64 required = 0;
    ui64 currentMissing = 0;
    ui64 currentMisplaced = 0;
    ui64 currentSplit = 0;
    TString currentDC = group->GetPreferredDataCenter();
    for (auto allocation : group->Allocations) {
        required += allocation->RequiredCount;
        currentMissing += allocation->MissingCount;
        currentMisplaced += allocation->MisplacedCount;
        currentSplit += allocation->SplitCount;
        if (allocation->Description.DataCenter != ANY_DATA_CENTER) {
            Y_ABORT_UNLESS(!allocation->Description.ForceLocation);
            preferredDCs.insert(allocation->Description.DataCenter);
        }
    }

    // For each preferred data center check if we can find perfect layout.
    // Instantly apply one if found.
    for (auto &dc : preferredDCs) {
        auto layout = ComputeLayoutForGroup(group, dc);
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Computed layout for group " << group->Id << " in "
                    << dc << ": " << layout->ToString());
        if (!layout->MissingCount && !layout->MisplacedCount && !layout->SplitCount) {
            group->SetPreferredDataCenter(dc);
            ApplyLayout(tenant, layout, txc, ctx);
            return true;
        }
        layouts[dc] = layout;
    }

    // Check if it's OK to stay in current data center.
    if (currentDC && !layouts.contains(currentDC)) {
        auto layout = ComputeLayoutForGroup(group, currentDC);
        layouts[currentDC] = layout;
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Computed layout for group " << group->Id << " in "
                    << currentDC << ": " << layout->ToString());
        if (!layout->MissingCount && !layout->MisplacedCount && !layout->SplitCount) {
            group->SetPreferredDataCenter(currentDC);
            ApplyLayout(tenant, layout, txc, ctx);
            return true;
        }
    }

    // Compute layouts for other data centers.
    for (auto &pr : FreeSlots.FreeSlotsByDataCenter) {
        if (!layouts.contains(pr.first)) {
            layouts[pr.first] = ComputeLayoutForGroup(group, pr.first);
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Computed layout for group " << group->Id << " in "
                        << pr.first << ": " << layouts[pr.first]->ToString());
        }
    }

    // We expect at least one layout to be computed.
    if (layouts.empty())
        LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "AssignFreeSlotsForGroup: no computed layouts for group " << group->Id);

    // Now choose the best layout and apply it if it is better than the
    // current one.
    auto best = layouts.end();
    double bestPenalty = ComputeLayoutPenalty(required, currentMissing, currentMisplaced,
                                              currentSplit, 0);

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Current layout in " << currentDC << " has penalty " << bestPenalty);

    for (auto it = layouts.begin(); it != layouts.end(); ++it) {
        auto layout = it->second;
        auto penalty = ComputeLayoutPenalty(required, layout->MissingCount, layout->MisplacedCount,
                                            layout->SplitCount, layout->DetachCount);

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Layout in " << it->first << " has penalty " << penalty);

        if (bestPenalty > penalty) {
            best = it;
            bestPenalty = penalty;
        }
    }

    if (best != layouts.end()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Layout in " << best->first << " was chosen");
        if (best->first != currentDC)
            group->SetPreferredDataCenter(best->first);
        ApplyLayout(tenant, best->second, txc, ctx);
        return true;
    } else {
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Better layout was not found");
    }

    return false;
}

void TTenantSlotBroker::OnClientDisconnected(TActorId clientId,
                                             const TActorContext &ctx)
{
    if (KnownPoolPipes.contains(clientId)) {
        ui32 nodeId = clientId.NodeId();
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Pipe disconnected from pool on node " << nodeId);

        // Same dynamic node may be allocated in another data center next time.
        auto config = AppData(ctx)->DynamicNameserviceConfig;
        if (config && nodeId > config->MaxStaticNodeId)
            NodeIdToDataCenter.erase(nodeId);

        KnownPoolPipes.erase(clientId);
        *Counters->ConnectedPools = KnownPoolPipes.size();

        DisconnectNodeSlots(nodeId, ctx);
    }
}

void TTenantSlotBroker::DisconnectNodeSlots(ui32 nodeId,
                                            const TActorContext &ctx)
{
    auto it = SlotsByNodeId.find(nodeId);
    if (it == SlotsByNodeId.end())
        return;

    for (auto &slot : it->second) {
        if (slot->IsConnected) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Mark slot " << slot->IdString() << " as disconnected");

            if (slot->IsFree()) {
                FreeSlots.Remove(slot);
            } else if (slot->AssignedTenant && !slot->IsPending()) {
                slot->AssignedTenant->GetAllocation(slot->UsedAs)->IncPending();
            }

            SlotDisconnected(slot);
        }

        slot->LastRequestId = RequestId++;
        ctx.Schedule(PendingTimeout, new TEvPrivate::TEvCheckSlotStatus(slot, slot->LastRequestId)) ;
    }
}

void TTenantSlotBroker::SendConfigureSlot(TSlot::TPtr slot,
                                          const TActorContext &ctx)
{
    auto event = MakeHolder<TEvTenantPool::TEvConfigureSlot>();
    event->Record.SetSlotId(slot->Id.SlotId);
    if (slot->AssignedTenant) {
        if (!slot->IsPending())
            slot->AssignedTenant->GetAllocation(slot->UsedAs)->IncPending();
        event->Record.SetAssignedTenant(slot->AssignedTenant->Name);
        event->Record.SetLabel(slot->Label);
    } else if (slot->IsFree()) {
        // Slot is not considered free until it confirms detach.
        FreeSlots.Remove(slot);
    }
    slot->LastRequestId = RequestId++;

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Send configuration request " << slot->LastRequestId << " for " << slot->IdString(true));

    auto serviceId = MakeTenantPoolID(slot->Id.NodeId);
    ctx.Send(serviceId, event.Release(), IEventHandle::FlagTrackDelivery, slot->LastRequestId);
    ctx.Schedule(PendingTimeout, new TEvPrivate::TEvCheckSlotStatus(slot, slot->LastRequestId));
}

void TTenantSlotBroker::FillTenantState(const TString &name,
                                        NKikimrTenantSlotBroker::TTenantState &state)
{
    state.SetTenantName(name);

    auto tenant = GetTenant(name);
    if (tenant) {
        for (auto &pr : tenant->GetAllocations()) {
            auto allocation = pr.second;

            if (allocation->RequiredCount) {
                auto &required = *state.AddRequiredSlots();
                allocation->Description.Serialize(required);
                required.SetCount(allocation->RequiredCount);
            }

            if (allocation->MissingCount) {
                auto &missing = *state.AddMissingSlots();
                allocation->Description.Serialize(missing);
                missing.SetCount(allocation->MissingCount);
            }

            if (allocation->PendingCount) {
                auto &pending = *state.AddPendingSlots();
                allocation->Description.Serialize(pending);
                pending.SetCount(allocation->PendingCount);
            }

            if (allocation->MisplacedCount) {
                auto &misplaced = *state.AddMisplacedSlots();
                allocation->Description.Serialize(misplaced);
                misplaced.SetCount(allocation->MisplacedCount);
            }

            if (allocation->SplitCount) {
                auto &split = *state.AddSplitSlots();
                allocation->Description.Serialize(split);
                split.SetCount(allocation->SplitCount);
            }

            if (allocation->PinnedCount) {
                auto &pinned = *state.AddPinnedSlots();
                allocation->Description.Serialize(pinned);
                pinned.SetCount(allocation->PinnedCount);
            }

            for (auto &slot : allocation->AssignedSlots) {
                const auto &slotId = slot->Id;
                auto &assigned = *state.AddAssignedSlots();
                assigned.SetNodeId(slotId.NodeId);
                assigned.SetSlotId(slotId.SlotId);
            }
        }
    }
}

void TTenantSlotBroker::ScheduleTxAssignFreeSlots(const TActorContext &ctx)
{
    if (PendingAssignFreeSlots)
        return;

    PendingAssignFreeSlots = true;
    ProcessTx(CreateTxAssignFreeSlots(), ctx);
}

void TTenantSlotBroker::ProcessTx(ITransaction *tx,
                                  const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tx);
    TxQueue.emplace_back(tx);
    ProcessNextTx(ctx);
}

void TTenantSlotBroker::TxCompleted(ITransaction *tx,
                                    const TActorContext &ctx)
{
    Y_ABORT_UNLESS(tx == ActiveTx);
    ActiveTx = nullptr;
    ProcessNextTx(ctx);
}

void TTenantSlotBroker::ProcessNextTx(const TActorContext &ctx)
{
    if (TxQueue.empty() || ActiveTx)
        return;

    ActiveTx = TxQueue.front().Release();
    TxQueue.pop_front();

    Y_ABORT_UNLESS(ActiveTx);
    Execute(ActiveTx, ctx);
}

void TTenantSlotBroker::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                               const TActorContext &ctx)
{
    if (ev->Get()->Record.HasLocal() && ev->Get()->Record.GetLocal()) {
        ProcessTx(CreateTxUpdateConfig(ev), ctx);
    } else {
        // ignore and immediately ack messages from old persistent console subscriptions
        auto response = MakeHolder<TEvConsole::TEvConfigNotificationResponse>();
        response->Record.MutableConfigId()->CopyFrom(ev->Get()->Record.GetConfigId());
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }
}

void TTenantSlotBroker::Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev,
                               const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "Cannot subscribe for config updates: " << rec.GetStatus().GetCode()
                    << " " << rec.GetStatus().GetReason());
        return;
    }

    ConfigSubscriptionId = rec.GetSubscriptionId();

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Got config subscription id=" << ConfigSubscriptionId);
}

void TTenantSlotBroker::Handle(TEvents::TEvUndelivered::TPtr &ev,
                               const TActorContext &ctx)
{
    ui32 nodeId = ev->Sender.NodeId();

    if (ev->Sender != MakeTenantPoolID(nodeId))
        return;

    DisconnectNodeSlots(nodeId, ctx);
}

void TTenantSlotBroker::Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxUpdateNodeLocation(ev), ctx);
}

void TTenantSlotBroker::Handle(TEvPrivate::TEvCheckSlotStatus::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxCheckSlotStatus(ev->Get()->RequestId, ev->Get()->Slot), ctx);
}

void TTenantSlotBroker::Handle(TEvPrivate::TEvCheckAllSlotsStatus::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxCheckSlotStatus(ev->Get()->RequestId), ctx);
}

void TTenantSlotBroker::Handle(TEvTenantPool::TEvLostOwnership::TPtr &ev,
                               const TActorContext &ctx)
{
    ui32 nodeId = ev->Sender.NodeId();
    LOG_CRIT_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
               "Tenant pool ownership is lost on node " << nodeId);

    DisconnectNodeSlots(nodeId, ctx);

    LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Re-taking ownership of tenant pool on node " << nodeId);

    ctx.Send(MakeTenantPoolID(nodeId), new TEvTenantPool::TEvTakeOwnership(Generation()));
}

void TTenantSlotBroker::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev,
                               const TActorContext &ctx)
{
    Y_UNUSED(ev);
    Y_UNUSED(ctx);
}

void TTenantSlotBroker::Handle(TEvTabletPipe::TEvServerDestroyed::TPtr &ev,
                               const TActorContext &ctx)
{
    OnClientDisconnected(ev->Get()->ClientId, ctx);
}

void TTenantSlotBroker::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev,
                               const TActorContext &ctx)
{
    OnClientDisconnected(ev->Get()->ClientId, ctx);
}

void TTenantSlotBroker::Handle(TEvTenantPool::TEvConfigureSlotResult::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxUpdateSlotStatus(ev), ctx);
}

void TTenantSlotBroker::Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxUpdatePoolStatus(ev), ctx);
}

void TTenantSlotBroker::Handle(TEvTenantSlotBroker::TEvAlterTenant::TPtr &ev,
                               const TActorContext &ctx)
{
    ProcessTx(CreateTxAlterTenant(ev), ctx);
}

void TTenantSlotBroker::Handle(TEvTenantSlotBroker::TEvGetSlotStats::TPtr &ev,
                               const TActorContext &ctx)
{
    auto resp = MakeHolder<TEvTenantSlotBroker::TEvSlotStats>();
    Counters->FillSlotStats(resp->Record);

    LOG_TRACE_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Send TEvTenantSlotBroker::TEvSlotStats: " << resp->ToString());

    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TTenantSlotBroker::Handle(TEvTenantSlotBroker::TEvGetTenantState::TPtr &ev,
                               const TActorContext &ctx)
{
    auto resp = MakeHolder<TEvTenantSlotBroker::TEvTenantState>();
    FillTenantState(ev->Get()->Record.GetTenantName(), resp->Record);

    LOG_TRACE_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                "Send TEvTenantSlotBroker::TEvTenantState: " << resp->ToString());

    ctx.Send(ev->Sender, resp.Release());
}

void TTenantSlotBroker::Handle(TEvTenantSlotBroker::TEvListTenants::TPtr &ev,
                               const TActorContext &ctx)
{
    auto resp = MakeHolder<TEvTenantSlotBroker::TEvTenantsList>();
    for (auto &pr : Tenants)
        FillTenantState(pr.first, *resp->Record.AddTenants());
    ctx.Send(ev->Sender, resp.Release());
}


void TTenantSlotBroker::Handle(TEvTenantSlotBroker::TEvRegisterPool::TPtr &ev,
                               const TActorContext &ctx)
{
    const auto &record = ev->Get()->Record;
    auto nodeId = ev->Sender.NodeId();

    KnownPoolPipes.insert(ActorIdFromProto(record.GetClientId()));
    *Counters->ConnectedPools = KnownPoolPipes.size();

    DisconnectNodeSlots(nodeId, ctx);
    ctx.Send(ev->Sender, new TEvTenantPool::TEvTakeOwnership(Generation(), record.GetSeqNo()));
}

IActor *CreateTenantSlotBroker(const TActorId &tablet,
                               TTabletStorageInfo *info)
{
    return new TTenantSlotBroker(tablet, info);
}

} // NTenantSlotBroker
} // namespace NKikimr
