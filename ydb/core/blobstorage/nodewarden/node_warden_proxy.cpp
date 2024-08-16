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
        proxy.reset(CreateBlobStorageGroupProxyMockActor(TGroupId::FromValue(groupId)));
    } else if (auto info = NeedGroupInfo(groupId)) {
        if (info->BlobDepotId) {
            TActorId proxyActorId;

            switch (info->DecommitStatus) {
                case NKikimrBlobStorage::TGroupDecommitStatus::NONE:
                case NKikimrBlobStorage::TGroupDecommitStatus::PENDING:
                    Y_ABORT("unexpected DecommitStatus for dynamic group with bound BlobDepotId");

                case NKikimrBlobStorage::TGroupDecommitStatus::IN_PROGRESS:
                    // create proxy that will be used by blob depot agent to fetch underlying data
                    proxyActorId = as->Register(CreateBlobStorageGroupProxyConfigured(
                        TIntrusivePtr<TBlobStorageGroupInfo>(info), false, DsProxyNodeMon, getCounters(info),
                        TBlobStorageProxyParameters{
                            .UseActorSystemTimeInBSQueue = Cfg->UseActorSystemTimeInBSQueue,
                            .EnablePutBatching = EnablePutBatching,
                            .EnableVPatch = EnableVPatch,
                            .SlowDiskThreshold = SlowDiskThreshold,
                            .PredictedDelayMultiplier = PredictedDelayMultiplier,
                        }), TMailboxType::ReadAsFilled, AppData()->SystemPoolId);
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
            proxy.reset(CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>(info), false, 
                DsProxyNodeMon, getCounters(info), TBlobStorageProxyParameters{
                        .UseActorSystemTimeInBSQueue = Cfg->UseActorSystemTimeInBSQueue,
                        .EnablePutBatching = EnablePutBatching,
                        .EnableVPatch = EnableVPatch,
                        .SlowDiskThreshold = SlowDiskThreshold,
                        .PredictedDelayMultiplier = PredictedDelayMultiplier,
                    }
                )
            );
        }
    } else {
        // create proxy without configuration
        proxy.reset(CreateBlobStorageGroupProxyUnconfigured(groupId, DsProxyNodeMon, TBlobStorageProxyParameters{
            .UseActorSystemTimeInBSQueue = Cfg->UseActorSystemTimeInBSQueue,
            .EnablePutBatching = EnablePutBatching,
            .EnableVPatch = EnableVPatch,
            .SlowDiskThreshold = SlowDiskThreshold,
            .PredictedDelayMultiplier = PredictedDelayMultiplier,
        }));
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
    Y_ABORT_UNLESS(Cfg->BlobStorageConfig.HasServiceSet());
    for (const auto& group : Cfg->BlobStorageConfig.GetServiceSet().GetGroups()) {
        StartLocalProxy(group.GetGroupID());
    }
}

void TNodeWarden::HandleForwarded(TAutoPtr<::NActors::IEventHandle> &ev) {
    const TGroupID groupId(GroupIDFromBlobStorageProxyID(ev->GetForwardOnNondeliveryRecipient()));
    const ui32 id = groupId.GetRaw();

    const bool noGroup = EjectedGroups.count(id);
    STLOG(PRI_DEBUG, BS_NODE, NW46, "HandleForwarded", (GroupId, id), (EnableProxyMock, EnableProxyMock), (NoGroup, noGroup));

    if (id == Max<ui32>()) {
        // invalid group; proxy for this group is created at start
    } else if (noGroup) {
        const TActorId errorProxy = StartEjectedProxy(id);
        TActivationContext::Forward(ev, errorProxy);
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, errorProxy, {}, nullptr, 0));
        return;
    } else if (groupId.ConfigurationType() == EGroupConfigurationType::Static && !Groups.count(id)) {
        const auto [it, inserted] = GroupPendingQueue.try_emplace(id);
        auto& queue = it->second;
        TMonotonic expiration = TActivationContext::Monotonic() + TDuration::Seconds(5);
        if (queue.empty()) {
            TimeoutToQueue.emplace(expiration, &*it);
        }
        queue.emplace_back(expiration, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    } else if (TGroupRecord& group = Groups[id]; !group.ProxyId) {
        if (TGroupID(id).ConfigurationType() == EGroupConfigurationType::Virtual) {
            StartVirtualGroupAgent(id);
        } else {
            StartLocalProxy(id);
        }
    }
    TActivationContext::Forward(ev, ev->GetForwardOnNondeliveryRecipient());
}

void TNodeWarden::HandleGroupPendingQueueTick() {
    const TMonotonic now = TActivationContext::Monotonic();

    std::set<std::tuple<TMonotonic, TGroupPendingQueue::value_type*>>::iterator it;
    for (it = TimeoutToQueue.begin(); it != TimeoutToQueue.end(); ++it) {
        const auto& [timestamp, ptr] = *it;
        if (now < timestamp) {
            break;
        }

        auto& [groupId, queue] = *ptr;
        Y_ABORT_UNLESS(!queue.empty());

        const TActorId errorProxy = StartEjectedProxy(groupId);
        for (;;) {
            auto& [timestamp, ev] = queue.front();
            if (now < timestamp) {
                TimeoutToQueue.emplace(timestamp, ptr);
                break;
            } else {
                THolder<IEventHandle> tmp(ev.release());
                TActivationContext::Forward(tmp, errorProxy);
                queue.pop_front();
                if (queue.empty()) {
                    GroupPendingQueue.erase(groupId);
                    break;
                }
            }
        }
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, errorProxy, {}, nullptr, 0));
    }
    TimeoutToQueue.erase(TimeoutToQueue.begin(), it);

    TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvGroupPendingQueueTick, 0,
        SelfId(), {}, nullptr, 0));
}

void TNodeWarden::Handle(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateUpdate::TPtr ev) {
    const auto& record = ev->Get()->Record;
    const ui32 groupId = record.GetGroupID();
    if (const auto it = Groups.find(groupId); it != Groups.end() && it->second.ProxyId) {
        TActivationContext::Send(ev->Forward(WhiteboardId));
    }
}
