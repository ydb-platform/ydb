#include "dsproxy_impl.h"
#include "dsproxy_monactor.h"

#include <ydb/core/blobstorage/bridge/proxy/bridge_proxy.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_PROXY

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
        YDB_LOG_INFO("SetStateEstablishingSessions Marker# DSP03",
            {"Group", GroupId});
        EstablishingSessionStartTime = TActivationContext::Now();
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        EstablishingSessionsStateTs = TActivationContext::Monotonic();
        EstablishingSessionsTimeoutEv = new TEvEstablishingSessionTimeout();
        Become(&TThis::StateEstablishingSessions, ProxyEstablishSessionsTimeout, EstablishingSessionsTimeoutEv);
        SwitchToWorkWhenGoodToGo();
    }

    void TBlobStorageGroupProxy::SetStateEstablishingSessionsTimeout() {
        YDB_LOG_INFO("SetStateEstablishingSessionsTimeout Marker# DSP09",
            {"Group", GroupId});
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
        YDB_LOG_INFO("SetStateUnconfigured Marker# DSP07",
            {"Group", GroupId});
        ErrorDescription = "StateUnconfigured (DSPE9).";
        EstablishingSessionsTimeoutEv = nullptr;
        ClearTimeoutCounters();
        UnconfiguredStateTs = TActivationContext::Monotonic();
        ConfigureQueryTimeoutEv = new TEvConfigureQueryTimeout();
        Become(&TThis::StateUnconfigured, ProxyConfigurationTimeout, ConfigureQueryTimeoutEv);
    }

    void TBlobStorageGroupProxy::SetStateUnconfiguredTimeout() {
        YDB_LOG_INFO("SetStateUnconfiguredTimeout Marker# DSP14",
            {"Group", GroupId});
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
        YDB_LOG_INFO("SetStateWork Marker# DSP15",
            {"Group", GroupId});
        EstablishingSessionsTimeoutEv = nullptr;
        ConfigureQueryTimeoutEv = nullptr;
        ClearTimeoutCounters();
        Become(&TThis::StateWork);
        ProcessInitQueue();
    }

    void TBlobStorageGroupProxy::Handle(TEvBlobStorage::TEvConfigureProxy::TPtr ev) {
        auto *msg = ev->Get();
        ApplyGroupInfo(std::move(msg->Info), std::move(msg->NodeLayoutInfo), std::move(msg->StoragePoolCounters));
    }

    void TBlobStorageGroupProxy::ApplyGroupInfo(TIntrusivePtr<TBlobStorageGroupInfo>&& info,
            TNodeLayoutInfoPtr nodeLayoutInfo, TIntrusivePtr<TStoragePoolCounters>&& counters) {
        if (info && info->IsBridged()) {
            const TActorId serviceId = MakeBlobStorageProxyID(info->GroupID);
            BridgeProxyId = RegisterWithSameMailbox(CreateBridgeProxyActor(std::move(info)));
            TActivationContext::ActorSystem()->RegisterLocalService(serviceId, BridgeProxyId);
            Become(&TThis::StateForward);
            ProcessInitQueue();
            return;
        }

        auto prevInfo = std::exchange(Info, std::move(info));
        if (Info) {
            if (Topology) {
                Y_DEBUG_ABORT_UNLESS(Topology->EqualityCheck(Info->GetTopology()));
            } else {
                Topology = Info->PickTopology();
            }
        }
        NodeLayoutInfo = std::move(nodeLayoutInfo);
        if (counters) {
            StoragePoolCounters = std::move(counters);
        }

        if (prevInfo && Info && prevInfo->GroupGeneration == Info->GroupGeneration) {
            return; // group did not actually change
        }

        Send(MonActor, new TEvBlobStorage::TEvConfigureProxy(Info, nullptr));
        if (Info) {
            auto printOptional = [&](const auto& val) -> TString {
                using T = std::decay_t<decltype(*val)>;
                if (!val) {
                    return "<nullopt>";
                } else {
                    if constexpr(std::is_same_v<T, ui64>) {
                        return IntToString<10>(*val);
                    }
                    if constexpr(std::is_same_v<T, TBlobStorageGroupInfo::EEncryptionMode>) {
                        return TBlobStorageGroupInfo::PrintEncryptionMode(*val);
                    }
                    if constexpr(std::is_same_v<T, TBlobStorageGroupInfo::ELifeCyclePhase>) {
                        return TBlobStorageGroupInfo::PrintLifeCyclePhase(*val);
                    }
                    // we don't want to print CypherKey value!
                    return "<value>";
                }
            };

            const bool wasInitial = LifeCyclePhase == TBlobStorageGroupInfo::ELCP_INITIAL;

            Y_VERIFY_S(!EncryptionMode || *EncryptionMode == Info->GetEncryptionMode(),
                    "EncryptionMode# " << printOptional(EncryptionMode) << " Info# " << Info->ToString());
            Y_VERIFY_S(!LifeCyclePhase || *LifeCyclePhase == Info->GetLifeCyclePhase() || wasInitial,
                    "LifeCyclePhase# " << printOptional(LifeCyclePhase) << " Info# " << Info->ToString());
            Y_VERIFY_S(!GroupKeyNonce || *GroupKeyNonce == Info->GetGroupKeyNonce() || wasInitial,
                    "GroupKeyNonce# " << printOptional(GroupKeyNonce) << " Info# " << Info->ToString());
            Y_VERIFY_S(!CypherKey || *CypherKey == *Info->GetCypherKey() || wasInitial,
                    "CypherKey# " << printOptional(CypherKey) << " Info# " << Info->ToString());
            EncryptionMode = Info->GetEncryptionMode();
            LifeCyclePhase = Info->GetLifeCyclePhase();
            GroupKeyNonce = Info->GetGroupKeyNonce();
            CypherKey = *Info->GetCypherKey();
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
        YDB_LOG_INFO("TEvConfigureProxy received Marker# DSP02",
            {"Group", GroupId},
            {"GroupGeneration", (Info ? ToString(Info->GroupGeneration) : "<none>")},
            {"IsLimitedKeyless", (IsLimitedKeyless ? "true" : "false")});
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
                Sessions = MakeIntrusive<TGroupSessions>(Info, BSProxyCtx, MonActor, SelfId(),
                        UseActorSystemTimeInBSQueue);
                NumUnconnectedDisks = Sessions->GetNumUnconnectedDisks();
                NodeMon->IncNumUnconnected(NumUnconnectedDisks);
            }
            SetStateEstablishingSessions();
        } else { // configuration has been reset, we have to request it via NodeWarden
            UnconfiguredStateReason = prevInfo
                    ? EUnconfiguredStateReason::GenerationChanged
                    : EUnconfiguredStateReason::UnknownGroup;
            SetStateUnconfigured();
        }
    }

    void TBlobStorageGroupProxy::WakeupUnconfigured(TEvConfigureQueryTimeout::TPtr ev) {
        if (ev && ev->Get() != ConfigureQueryTimeoutEv) {
            return;
        }

        TString details = TStringBuilder() << " GroupId# " << GroupId
                << " UnconfiguredStateTs# " << UnconfiguredStateTs
                << " UnconfiguredStateReason# " << UnconfiguredStateReasonStr(UnconfiguredStateReason);

        YDB_LOG_ERROR("Unconfigured Wakeup TIMEOUT Marker# DSP05",
            {"details", details});

        ErrorDescription = "Configuration timeout occured (DSPE1). " + details;
        EstablishingSessionsPutMuteChecker.Unmute();
        SetStateUnconfiguredTimeout();
    }

    void TBlobStorageGroupProxy::SwitchToWorkWhenGoodToGo() {
        Y_ABORT_UNLESS(Sessions);
        if (Sessions->GoodToGo(*Topology, ForceWaitAllDrives)) {
            YDB_LOG_INFO("-> StateWork Marker# DSP11",
                {"Group", GroupId});
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
        TString details = TStringBuilder() << " GroupId# " << GroupId
                << " EstablishingSessionsStateTs# " << EstablishingSessionsStateTs
                << " NumUnconnectedDisks# " << NumUnconnectedDisks;
        YDB_LOG_ERROR("StateEstablishingSessions Wakeup TIMEOUT Marker# DSP12",
            {"details", details});
        ErrorDescription = "Timeout while establishing sessions (DSPE4). " + details;
        SetStateEstablishingSessionsTimeout();
    }

    void TBlobStorageGroupProxy::Handle(TEvProxyQueueState::TPtr& ev) {
        TInstant now = TActivationContext::Now();
        TDuration establishingSessionsDuration = now - EstablishingSessionStartTime;
        YDB_LOG_DEBUG("Handle Marker# DSP04",
            {"Group", GroupId},
            {"TEvProxyQueueState", ev->Get()->ToString()},
            {"Duration", establishingSessionsDuration});
        Y_ABORT_UNLESS(Sessions);
        auto *msg = ev->Get();
        Y_ABORT_UNLESS(Topology);
        Sessions->QueueConnectUpdate(Topology->GetOrderNumber(msg->VDiskId), msg->QueueId, msg->IsConnected,
            msg->ExtraBlockChecksSupport, msg->Checksumming, msg->CostModel, *Topology);
        MinHugeBlobInBytes = Sessions->GetMinHugeBlobInBytes();
        if (msg->IsConnected && (CurrentStateFunc() == &TThis::StateEstablishingSessions ||
                CurrentStateFunc() == &TThis::StateEstablishingSessionsTimeout)) {
            SwitchToWorkWhenGoodToGo();
        } else if (!msg->IsConnected && CurrentStateFunc() == &TThis::StateWork && !Sessions->GoodToGo(*Topology, false)) {
            SetStateEstablishingSessions();
        }

        Y_DEBUG_ABORT_UNLESS(CurrentStateFunc() != &TThis::StateWork || MinHugeBlobInBytes);

        if (const ui32 prev = std::exchange(NumUnconnectedDisks, Sessions->GetNumUnconnectedDisks()); prev != NumUnconnectedDisks) {
            NodeMon->IncNumUnconnected(NumUnconnectedDisks);
            NodeMon->DecNumUnconnected(prev);
        }
    }

    void TBlobStorageGroupProxy::Bootstrap() {
        if (IsEjected) {
            YDB_LOG_NOTICE("Bootstrap -> StateEjected Marker# DSP42",
                {"Group", GroupId},
                {"HasInvalidGroupId", HasInvalidGroupId()});
            if (HasInvalidGroupId()) {
                ErrorDescription = "Created as unconfigured in error state (DSPE11). It happens when the request was sent for an invalid groupID";
                ExtraLogInfo = "The request was sent for an invalid groupID ";
            } else {
                ErrorDescription = "Created as unconfigured in error state (DSPE7). It happens when group isn't present in BSC";
            }
            Become(&TThis::StateEjected);
        } else {
            StopPutBatchingEvent = static_cast<TEventHandle<TEvStopBatchingPutRequests>*>(
                    new IEventHandle(SelfId(), SelfId(), new TEvStopBatchingPutRequests));
            StopGetBatchingEvent = static_cast<TEventHandle<TEvStopBatchingGetRequests>*>(
                    new IEventHandle(SelfId(), SelfId(), new TEvStopBatchingGetRequests));
            ApplyGroupInfo(std::exchange(Info, {}), std::exchange(NodeLayoutInfo, {}), std::exchange(StoragePoolCounters, {}));
            CheckDeadlines();
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
            YDB_LOG_NOTICE("EnsureMonitoring Marker# DSP57 initialize full monitoring",
                {"Group", GroupId},
                {"IsLimitedKeyless", IsLimitedKeyless});
            Mon->BecomeFull();
        } else if (!MonActor) {
            YDB_LOG_NOTICE("EnsureMonitoring Marker# DSP58",
                {"Group", GroupId},
                {"IsLimitedKeyless", IsLimitedKeyless},
                {"fullIfPossible", fullIfPossible});

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
        YDB_LOG_DEBUG("RequestProxySessionsState Marker# DSP59",
            {"Group", GroupId});
        Send(ev->Sender, new TEvProxySessionsState(Sessions ? Sessions->GroupQueues : nullptr));
    }

#define SELECT_CONTROL_BY_DEVICE_TYPE(prefix, info) \
([&](NPDisk::EDeviceType deviceType) -> i64 {       \
    TInstant now = TActivationContext::Now();       \
    switch (deviceType) {                           \
    case NPDisk::DEVICE_TYPE_ROT:                   \
        return Controls.prefix##HDD.Update(now);    \
    case NPDisk::DEVICE_TYPE_SSD:                   \
    case NPDisk::DEVICE_TYPE_NVME:                  \
        return Controls.prefix##SSD.Update(now);    \
    default:                                        \
        return Controls.prefix.Update(now);         \
    }                                               \
})(info ? info->GetDeviceType() : NPDisk::DEVICE_TYPE_UNKNOWN)

    TAccelerationParams TBlobStorageGroupProxy::GetAccelerationParams() {
        return TAccelerationParams{
            .SlowDiskThreshold = .001f * SELECT_CONTROL_BY_DEVICE_TYPE(SlowDiskThreshold, Info),
            .PredictedDelayMultiplier = .001f * SELECT_CONTROL_BY_DEVICE_TYPE(PredictedDelayMultiplier, Info),
            .MaxNumOfSlowDisks = static_cast<ui32>(SELECT_CONTROL_BY_DEVICE_TYPE(MaxNumOfSlowDisks, Info)),
        };
    }

#undef SELECT_CONTROL_BY_DEVICE_TYPE

} // NKikimr
