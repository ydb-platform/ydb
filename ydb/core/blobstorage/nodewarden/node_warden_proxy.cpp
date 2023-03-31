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
    TActorSystem *as = TActivationContext::ActorSystem();

    TGroupRecord& group = Groups[groupId];

    auto getCounters = [&](const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
        return DsProxyPerPoolCounters->GetPoolCounters(info->GetStoragePoolName(), info->GetDeviceType());
    };

    if (EnableProxyMock) {
        // create mock proxy
        proxy.reset(CreateBlobStorageGroupProxyMockActor());
    } else if (auto info = NeedGroupInfo(groupId)) {
        if (info->BlobDepotId) {
            TActorId proxyActorId;

            switch (info->DecommitStatus) {
                case NKikimrBlobStorage::TGroupDecommitStatus::NONE:
                case NKikimrBlobStorage::TGroupDecommitStatus::PENDING:
                    Y_FAIL("unexpected DecommitStatus for dynamic group with bound BlobDepotId");

                case NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS:
                    // create proxy that will be used by blob depot agent to fetch underlying data
                    proxyActorId = as->Register(CreateBlobStorageGroupProxyConfigured(
                        TIntrusivePtr<TBlobStorageGroupInfo>(info), false, DsProxyNodeMon,
                        getCounters(info), EnablePutBatching, EnableVPatch), TMailboxType::ReadAsFilled,
                        AppData()->SystemPoolId);
                    [[fallthrough]];
                case NKikimrBlobStorage::TGroupDecommitStatus::DONE:
                    proxy.reset(NBlobDepot::CreateBlobDepotAgent(groupId, info, proxyActorId));
                    group.AgentProxy = true;
                    break;

                case NKikimrBlobStorage::TGroupDecommitStatus_E_TGroupDecommitStatus_E_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBlobStorage::TGroupDecommitStatus_E_TGroupDecommitStatus_E_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_UNREACHABLE();
            }
        } else {
            // create proxy with configuration
            proxy.reset(CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>(info), false, DsProxyNodeMon, getCounters(info),
                EnablePutBatching, EnableVPatch));
        }
    } else {
        // create proxy without configuration
        proxy.reset(CreateBlobStorageGroupProxyUnconfigured(groupId, DsProxyNodeMon, EnablePutBatching, EnableVPatch));
    }

    group.ProxyId = as->Register(proxy.release(), TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
    as->RegisterLocalService(MakeBlobStorageProxyID(groupId), group.ProxyId);
}

void TNodeWarden::StartVirtualGroupAgent(ui32 groupId) {
    STLOG(PRI_DEBUG, BS_NODE, NW40, "StartVirtualGroupProxy", (GroupId, groupId));

    TActorSystem *as = TActivationContext::ActorSystem();
    TGroupRecord& group = Groups[groupId];
    auto info = NeedGroupInfo(groupId);
    group.ProxyId = as->Register(NBlobDepot::CreateBlobDepotAgent(groupId, std::move(info), {}),
        TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
    group.AgentProxy = true;
    as->RegisterLocalService(MakeBlobStorageProxyID(groupId), group.ProxyId);
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
    } else if (TGroupRecord& group = Groups[id]; !group.ProxyId) {
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
    if (const auto it = Groups.find(groupId); it != Groups.end() && it->second.ProxyId) {
        TActivationContext::Send(ev->Forward(WhiteboardId));
    }
}
