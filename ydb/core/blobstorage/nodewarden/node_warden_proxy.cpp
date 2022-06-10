#include "node_warden_impl.h"

using namespace NKikimr;
using namespace NStorage;

TActorId TNodeWarden::StartEjectedProxy(ui32 groupId) {
    STLOG(PRI_DEBUG, BS_NODE, NW10, "StartErrorProxy", (GroupId, groupId));
    return Register(CreateBlobStorageGroupEjectedProxy(groupId, DsProxyNodeMon), TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
}

void TNodeWarden::StartLocalProxy(ui32 groupId) {
    STLOG(PRI_DEBUG, BS_NODE, NW12, "StartLocalProxy", (GroupId, groupId));

    std::unique_ptr<IActor> proxy;

    if (EnableProxyMock) {
        // create mock proxy
        proxy.reset(CreateBlobStorageGroupProxyMockActor());
    } else if (auto info = NeedGroupInfo(groupId)) {
        // create proxy with configuration
        auto counters = DsProxyPerPoolCounters->GetPoolCounters(info->GetStoragePoolName(), info->GetDeviceType());
        proxy.reset(CreateBlobStorageGroupProxyConfigured(std::move(info), false, DsProxyNodeMon, std::move(counters),
            EnablePutBatching, EnableVPatch));
    } else {
        // create proxy without configuration
        proxy.reset(CreateBlobStorageGroupProxyUnconfigured(groupId, DsProxyNodeMon, EnablePutBatching, EnableVPatch));
    }

    TActorSystem *as = TActivationContext::ActorSystem();
    as->RegisterLocalService(MakeBlobStorageProxyID(groupId), as->Register(proxy.release(), TMailboxType::ReadAsFilled,
        AppData()->SystemPoolId));
}

void TNodeWarden::StartVirtualGroupAgent(ui32 groupId) {
    STLOG(PRI_DEBUG, BS_NODE, NW40, "StartVirtualGroupProxy", (GroupId, groupId));

    TActorSystem *as = TActivationContext::ActorSystem();
    const TActorId actorId = as->Register(NBlobDepot::CreateBlobDepotAgent(groupId), TMailboxType::ReadAsFilled,
        AppData()->SystemPoolId);
    if (auto info = NeedGroupInfo(groupId)) {
        auto counters = DsProxyPerPoolCounters->GetPoolCounters(info->GetStoragePoolName(), info->GetDeviceType());
        Send(actorId, new TEvBlobStorage::TEvConfigureProxy(std::move(info), std::move(counters)));
    }
    as->RegisterLocalService(MakeBlobStorageProxyID(groupId), actorId);
}

void TNodeWarden::StartStaticProxies() {
    for (const auto& group : Cfg->ServiceSet.GetGroups()) {
        StartLocalProxy(group.GetGroupID());
    }
}

void TNodeWarden::HandleForwarded(TAutoPtr<::NActors::IEventHandle> &ev) {
    const TGroupID groupId(GroupIDFromBlobStorageProxyID(ev->GetForwardOnNondeliveryRecipient()));
    const ui32 id = groupId.GetRaw();

    const bool noGroup = (groupId.ConfigurationType() == EGroupConfigurationType::Static && !Groups.count(id)) || EjectedGroups.count(id);
    STLOG(PRI_DEBUG, BS_NODE, NW46, "HandleForwarded", (GroupId, id), (EnableProxyMock, EnableProxyMock), (NoGroup, noGroup));

    if (id == Max<ui32>()) {
        // invalid group; proxy for this group is created at start
    } else if (noGroup) {
        const TActorId errorProxy = StartEjectedProxy(id);
        TActivationContext::Send(ev->Forward(errorProxy));
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, errorProxy, {}, nullptr, 0));
        return;
    } else if (TGroupRecord& group = Groups[id]; !group.ProxyRunning) {
        group.ProxyRunning = true;
        if (TGroupID(id).ConfigurationType() == EGroupConfigurationType::Virtual) {
            StartVirtualGroupAgent(id);
        } else {
            StartLocalProxy(id);
        }
    }

    TActivationContext::Send(ev->Forward(ev->GetForwardOnNondeliveryRecipient()));
}

void TNodeWarden::Handle(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate::TPtr ev) {
    const auto& record = ev->Get()->Record;
    const ui32 groupId = record.GetGroupID();
    if (const auto it = Groups.find(groupId); it != Groups.end() && it->second.ProxyRunning) {
        TActivationContext::Send(ev->Forward(WhiteboardId));
    }
}
