#include "dsproxy_impl.h"
#include "dsproxy_monactor.h"

namespace NKikimr {

    void TBlobStorageGroupProxy::Handle5min(TAutoPtr<IEventHandle> ev) {
        if (ev->Cookie == Cookie5min) {
            if (CurrentStateFunc() == &TBlobStorageGroupProxy::StateEstablishingSessionsTimeout) {
                Y_ABORT_UNLESS(!InEstablishingSessionsTimeout5min);
                ++*NodeMon->EstablishingSessionsTimeout5min;
                InEstablishingSessionsTimeout5min = true;
            } else if (CurrentStateFunc() == &TBlobStorageGroupProxy::StateUnconfiguredTimeout) {
                Y_ABORT_UNLESS(!InUnconfiguredTimeout5min);
                ++*NodeMon->UnconfiguredTimeout5min;
                InUnconfiguredTimeout5min = true;
            }
        }
    }

    void TBlobStorageGroupProxy::ClearTimeoutCounters() {
        *NodeMon->EstablishingSessionsTimeout -= std::exchange(InEstablishingSessionsTimeout, false);
        *NodeMon->EstablishingSessionsTimeout5min -= std::exchange(InEstablishingSessionsTimeout5min, false);
        *NodeMon->UnconfiguredTimeout -= std::exchange(InUnconfiguredTimeout, false);
        *NodeMon->UnconfiguredTimeout5min -= std::exchange(InUnconfiguredTimeout5min, false);
    }

    void TBlobStorageGroupProxy::SetStateEstablishingSessions() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " SetStateEstablishingSessions Marker# DSP03");
        EstablishingSessionStartTime = TActivationContext::Now();
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        EstablishingSessionsTimeoutEv = new TEvEstablishingSessionTimeout();
        Become(&TThis::StateEstablishingSessions, ProxyEstablishSessionsTimeout, EstablishingSessionsTimeoutEv);
        SwitchToWorkWhenGoodToGo();
    }

    void TBlobStorageGroupProxy::SetStateEstablishingSessionsTimeout() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " SetStateEstablishingSessionsTimeout Marker# DSP09");
        EstablishingSessionStartTime = TInstant::Zero();
        EstablishingSessionsPutMuteChecker.Unmute();
        EstablishingSessionsTimeoutEv = nullptr;
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        Become(&TThis::StateEstablishingSessionsTimeout);
        ++*NodeMon->EstablishingSessionsTimeout;
        InEstablishingSessionsTimeout = true;
        TActivationContext::Schedule(TDuration::Minutes(5), new IEventHandle(Ev5min, 0, SelfId(), {}, nullptr, ++Cookie5min));
        ProcessInitQueue();
    }

    void TBlobStorageGroupProxy::SetStateUnconfigured() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " SetStateUnconfigured Marker# DSP07");
        ErrorDescription = "StateUnconfigured (DSPE9).";
        EstablishingSessionsTimeoutEv = nullptr;
        ClearTimeoutCounters();
        ConfigureQueryTimeoutEv = new TEvConfigureQueryTimeout();
        Become(&TThis::StateUnconfigured, ProxyConfigurationTimeout, ConfigureQueryTimeoutEv);
    }

    void TBlobStorageGroupProxy::SetStateUnconfiguredTimeout() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " SetStateUnconfiguredTimeout Marker# DSP14");
        EstablishingSessionsTimeoutEv = nullptr;
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        Become(&TThis::StateUnconfiguredTimeout);
        ++*NodeMon->UnconfiguredTimeout;
        InUnconfiguredTimeout = true;
        TActivationContext::Schedule(TDuration::Minutes(5), new IEventHandle(Ev5min, 0, SelfId(), {}, nullptr, ++Cookie5min));
        ProcessInitQueue();
    }

    void TBlobStorageGroupProxy::SetStateWork() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " SetStateWork Marker# DSP15");
        EstablishingSessionsTimeoutEv = nullptr;
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        Become(&TThis::StateWork);
        ProcessInitQueue();
    }

    void TBlobStorageGroupProxy::Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
        auto *msg = ev->Get();
        ApplyGroupInfo(std::move(msg->Info), std::move(msg->StoragePoolCounters));
    }

    void TBlobStorageGroupProxy::ApplyGroupInfo(TIntrusivePtr<TBlobStorageGroupInfo>&& info,
            TIntrusivePtr<TStoragePoolCounters>&& counters) {
        Info = std::move(info);
        if (Info) {
            if (Topology) {
                Y_DEBUG_ABORT_UNLESS(Topology->EqualityCheck(Info->GetTopology()));
            } else {
                Topology = Info->PickTopology();
            }
        }
        NodeLayoutInfo = nullptr;
        Send(MonActor, new TEvBlobStorage::TEvConfigureProxy(Info));
        if (Info) {
            Y_ABORT_UNLESS(!EncryptionMode || *EncryptionMode == Info->GetEncryptionMode());
            Y_ABORT_UNLESS(!LifeCyclePhase || *LifeCyclePhase == Info->GetLifeCyclePhase());
            Y_ABORT_UNLESS(!GroupKeyNonce || *GroupKeyNonce == Info->GetGroupKeyNonce());
            Y_ABORT_UNLESS(!CypherKey || *CypherKey == *Info->GetCypherKey());
            EncryptionMode = Info->GetEncryptionMode();
            LifeCyclePhase = Info->GetLifeCyclePhase();
            GroupKeyNonce = Info->GetGroupKeyNonce();
            CypherKey = *Info->GetCypherKey();
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes(true));
        }
        if (counters) {
            StoragePoolCounters = std::move(counters);
        }
        IsLimitedKeyless = false;
        if (Info && Info->GetEncryptionMode() != TBlobStorageGroupInfo::EEM_NONE) {
            switch (Info->GetLifeCyclePhase()) {
                case TBlobStorageGroupInfo::ELCP_IN_USE:
                    Y_VERIFY_S(Info->GetCypherKey()->GetIsKeySet(), "CypherKey must be set for Group# " << GroupId
                        << " ActorId# " << SelfId() << " EncryptionMode# " << Info->GetEncryptionMode());
                    break;

                case TBlobStorageGroupInfo::ELCP_PROPOSE:
                    Y_VERIFY_DEBUG_S(false, "incorrect LifeCyclePhase# " << Info->GetLifeCyclePhase());
                    [[fallthrough]];
                case TBlobStorageGroupInfo::ELCP_INITIAL:
                case TBlobStorageGroupInfo::ELCP_IN_TRANSITION:
                case TBlobStorageGroupInfo::ELCP_KEY_CRC_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_VERSION_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_ID_ERROR:
                case TBlobStorageGroupInfo::ELCP_KEY_NOT_LOADED:
                    IsLimitedKeyless = true;
                    break;
            }
        }
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
            << " TEvConfigureProxy received"
            << " GroupGeneration# " << (Info ? ToString(Info->GroupGeneration) : "<none>")
            << " IsLimitedKeyless# " << (IsLimitedKeyless ? "true" : "false")
            << " Marker# DSP02");
        if (Info) { // if the new group has arrived
            if (Sessions) { // queues are already created
                for (size_t i = 0; i < Sessions->GroupQueues->DisksByOrderNumber.size(); ++i) {
                    const auto& disk = *Sessions->GroupQueues->DisksByOrderNumber[i];
                    disk.Queues.ForEachQueue([&](const auto& queue) {
                        Send(queue.ActorId, new TEvVGenerationChange(Info->GetVDiskId(i), Info));
                    });
                }
            } else { // this is the first time configuration arrives -- no queues are created yet
                EnsureMonitoring(false);
                Sessions = MakeIntrusive<TGroupSessions>(Info, BSProxyCtx, MonActor, SelfId());
                NumUnconnectedDisks = Sessions->GetNumUnconnectedDisks();
                NodeMon->IncNumUnconnected(NumUnconnectedDisks);
            }
            SetStateEstablishingSessions();
        } else { // configuration has been reset, we have to request it via NodeWarden
            SetStateUnconfigured();
        }
    }

    void TBlobStorageGroupProxy::WakeupUnconfigured(TEvConfigureQueryTimeout::TPtr ev) {
        if (ev && ev->Get() != ConfigureQueryTimeoutEv) {
            return;
        }
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                << " Unconfigured Wakeup TIMEOUT Marker# DSP05");
        ErrorDescription = "Configuration timeout occured (DSPE1).";
        EstablishingSessionsPutMuteChecker.Unmute();
        SetStateUnconfiguredTimeout();
    }

    void TBlobStorageGroupProxy::SwitchToWorkWhenGoodToGo() {
        Y_ABORT_UNLESS(Sessions);
        if (Sessions->GoodToGo(*Topology, ForceWaitAllDrives)) {
            LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                    << " -> StateWork" << " Marker# DSP11");
            ErrorDescription = "StateWork (DSPE3).";
            EstablishingSessionStartTime = TInstant::Zero();
            EstablishingSessionsPutMuteChecker.Unmute();
            ErrorStateMuteChecker.Unmute();
            SetStateWork();
            ScheduleUpdateGroupStat();
        }
    }

    void TBlobStorageGroupProxy::WakeupEstablishingSessions(TEvEstablishingSessionTimeout::TPtr ev) {
        if (ev && ev->Get() != EstablishingSessionsTimeoutEv) {
            return;
        }
        LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                << " StateEstablishingSessions Wakeup TIMEOUT Marker# DSP12");
        ErrorDescription = "Timeout while establishing sessions (DSPE4).";
        SetStateEstablishingSessionsTimeout();
    }

    void TBlobStorageGroupProxy::Handle(TEvProxyQueueState::TPtr& ev) {
        TInstant now = TActivationContext::Now();
        TDuration establishingSessionsDuration = now - EstablishingSessionStartTime;
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                << " Handle TEvProxyQueueState# "<< ev->Get()->ToString()
                << " Duration# " << establishingSessionsDuration
                << " Marker# DSP04");
        Y_ABORT_UNLESS(Sessions);
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(Topology);
        Sessions->QueueConnectUpdate(Topology->GetOrderNumber(msg->VDiskId), msg->QueueId, msg->IsConnected,
            msg->ExtraBlockChecksSupport, msg->CostModel, *Topology);
        MinREALHugeBlobInBytes = Sessions->GetMinREALHugeBlobInBytes();
        if (msg->IsConnected && (CurrentStateFunc() == &TThis::StateEstablishingSessions ||
                CurrentStateFunc() == &TThis::StateEstablishingSessionsTimeout)) {
            SwitchToWorkWhenGoodToGo();
        } else if (!msg->IsConnected && CurrentStateFunc() == &TThis::StateWork && !Sessions->GoodToGo(*Topology, false)) {
            SetStateEstablishingSessions();
        }

        Y_DEBUG_ABORT_UNLESS(CurrentStateFunc() != &TThis::StateWork || MinREALHugeBlobInBytes);

        if (const ui32 prev = std::exchange(NumUnconnectedDisks, Sessions->GetNumUnconnectedDisks()); prev != NumUnconnectedDisks) {
            NodeMon->IncNumUnconnected(NumUnconnectedDisks);
            NodeMon->DecNumUnconnected(prev);
        }
    }

    void TBlobStorageGroupProxy::Bootstrap() {
        if (IsEjected) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "Group# " << GroupId
                    << " HasInvalidGroupId# " << HasInvalidGroupId() << " Bootstrap -> StateEjected Marker# DSP42");
            if (HasInvalidGroupId()) {
                ErrorDescription = "Created as unconfigured in error state (DSPE11). It happens when the request was sent for an invalid groupID";
                ExtraLogInfo = "The request was sent for an invalid groupID ";
            } else {
                ErrorDescription = "Created as unconfigured in error state (DSPE7). It happens when group doesn't present in BSC";
            }
            Become(&TThis::StateEjected);
        } else {
            StopPutBatchingEvent = static_cast<TEventHandle<TEvStopBatchingPutRequests>*>(
                    new IEventHandle(SelfId(), SelfId(), new TEvStopBatchingPutRequests));
            StopGetBatchingEvent = static_cast<TEventHandle<TEvStopBatchingGetRequests>*>(
                    new IEventHandle(SelfId(), SelfId(), new TEvStopBatchingGetRequests));
            ApplyGroupInfo(std::exchange(Info, {}), std::exchange(StoragePoolCounters, {}));
            CheckDeadlines();
        }
    }

    void TBlobStorageGroupProxy::Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        if (Info) {
            std::unordered_map<ui32, TNodeLocation> map;
            for (auto& info : ev->Get()->Nodes) {
                map[info.NodeId] = std::move(info.Location);
            }
            NodeLayoutInfo = MakeIntrusive<TNodeLayoutInfo>(map[TlsActivationContext->ExecutorThread.ActorSystem->NodeId],
                Info, map);
        }
    }

    void TBlobStorageGroupProxy::PassAway() {
        for (const auto& [actorId, _] : ActiveRequests) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, {}, nullptr, 0));
        }
        if (Sessions) { // may be null if not properly configured yet
            Sessions->Poison();
        }
        if (MonActor) {
            Send(MonActor, new NActors::TEvents::TEvPoison());
        }

        // Reply error to all messages in the init queue
        ErrorDescription = "DsProxy got a Poison Pill";
        SetStateEstablishingSessionsTimeout();

        if (NumUnconnectedDisks != Max<ui32>()) {
            NodeMon->DecNumUnconnected(NumUnconnectedDisks);
        }

        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, GetNameserviceActorId(), SelfId(),
            nullptr, 0));

        TActorBootstrapped::PassAway();
        // TODO: Unsubscribe
    }

    void TBlobStorageGroupProxy::ProcessInitQueue() {
        UnconfiguredBufferSize = 0;
        for (std::unique_ptr<IEventHandle>& ev : std::exchange(InitQueue, {})) {
            TAutoPtr<IEventHandle> x(ev.release());
            Receive(x);
        }
    }

    void TBlobStorageGroupProxy::EnsureMonitoring(bool fullIfPossible) {
        if (MonActor && fullIfPossible && !IsFullMonitoring) {
            IsFullMonitoring = true;
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "EnsureMonitoring Group# " << GroupId
                    << " IsLimitedKeyless# " << IsLimitedKeyless << " Marker# DSP57 initialize full monitoring");
            Mon->BecomeFull();
        } else if (!MonActor) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "EnsureMonitoring Group# " << GroupId
                    << " IsLimitedKeyless# " << IsLimitedKeyless << " fullIfPossible# " << fullIfPossible << " Marker# DSP58");

            bool limited = IsLimitedKeyless || !fullIfPossible;
            IsFullMonitoring = IsLimitedKeyless || fullIfPossible;

            TString name = Sprintf("%09" PRIu32, GroupId.GetRawId());
            TIntrusivePtr<::NMonitoring::TDynamicCounters> group = GetServiceCounters(
                    AppData()->Counters, "dsproxy")->GetSubgroup("blobstorageproxy", name);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> percentileGroup = GetServiceCounters(
                    AppData()->Counters, "dsproxy_percentile")->GetSubgroup("blobstorageproxy", name);
            TIntrusivePtr<::NMonitoring::TDynamicCounters> overviewGroup = GetServiceCounters(
                    AppData()->Counters, "dsproxy_overview");

            Mon.Reset(new TBlobStorageGroupProxyMon(group, percentileGroup, overviewGroup, Info, NodeMon, limited));
            BSProxyCtx.Reset(new TBSProxyContext(group->GetSubgroup("subsystem", "memproxy")));
            MonActor = RegisterWithSameMailbox(CreateBlobStorageGroupProxyMon(Mon, GroupId.GetRawId(), Info, SelfId()));
        }
    }

    void TBlobStorageGroupProxy::Handle(TEvRequestProxySessionsState::TPtr &ev) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_PROXY, "RequestProxySessionsState Group# " << GroupId
                << " Marker# DSP59");
        Send(ev->Sender, new TEvProxySessionsState(Sessions ? Sessions->GroupQueues : nullptr));
    }

} // NKikimr
