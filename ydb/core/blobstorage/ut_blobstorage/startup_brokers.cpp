#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_operation_broker.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_job_task.h>
#include <ydb/core/blobstorage/vdisk/syncer/syncer_job_actor.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_scheduler.h>

#include <algorithm>
#include <optional>
#include <utility>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace {

    constexpr ui32 NumGroups = 8;
    constexpr ui32 MinExpectedVDisksOnRestartedNode = 6;
    constexpr ui32 StartupBacklogTargetVDisks = 3;
    constexpr ui32 StartupBacklogBlobsPerGroup = 8;
    constexpr ui32 StartupBacklogBlobSize = 512;
    constexpr ui32 WaitIterations = 180;
    const TDuration WaitStep = TDuration::Seconds(1);

    struct TTargetVDisk {
        ui32 GroupId = 0;
        TVDiskID VDiskId;
        TActorId VDiskActorId;
        ui32 PDiskId = 0;
    };

    struct TPerPDiskSelection {
        ui32 FocusPDiskId = 0;
        TVector<TTargetVDisk> FocusTargets;
        TTargetVDisk OtherTarget;
        TVector<TTargetVDisk> StartupTargets;
    };

    struct TBrokerControls {
        ui64 MaxInProgressLocalRecoveryCount = 0;
        ui64 MaxInProgressLocalRecoveryPerPDiskCount = 0;
        ui64 MaxInProgressStartupDataSyncCount = 0;
        ui64 MaxInProgressStartupDataSyncPerPDiskCount = 0;
    };

    void ConfigureBrokerControls(TEnvironmentSetup& env, ui32 nodeId,
            ui64 maxInProgressLocalRecoveryCount, ui64 maxInProgressLocalRecoveryPerPDiskCount,
            ui64 maxInProgressStartupDataSyncCount, ui64 maxInProgressStartupDataSyncPerPDiskCount)
    {
        auto& icb = *env.Runtime->GetNode(nodeId)->AppData->Icb;
        TControlBoard::SetValue(maxInProgressLocalRecoveryCount,
            icb.VDiskControls.MaxInProgressLocalRecoveryCount);
        TControlBoard::SetValue(maxInProgressLocalRecoveryPerPDiskCount,
            icb.VDiskControls.MaxInProgressLocalRecoveryPerPDiskCount);
        TControlBoard::SetValue(maxInProgressStartupDataSyncCount,
            icb.VDiskControls.MaxInProgressStartupDataSyncCount);
        TControlBoard::SetValue(maxInProgressStartupDataSyncPerPDiskCount,
            icb.VDiskControls.MaxInProgressStartupDataSyncPerPDiskCount);
    }

    template<typename TPredicate>
    void WaitUntil(TEnvironmentSetup& env, TPredicate&& predicate, TStringBuf message) {
        for (ui32 i = 0; i < WaitIterations && !predicate(); ++i) {
            env.Sim(WaitStep);
        }
        UNIT_ASSERT_C(predicate(), message);
    }

    TString FormatActorIdList(const TVector<TActorId>& items) {
        TStringBuilder sb;
        sb << "[";
        bool first = true;
        for (const auto& item : items) {
            if (!first) {
                sb << ", ";
            }
            first = false;
            sb << item;
        }
        sb << "]";
        return sb;
    }

    TString FormatActorIdSet(const THashSet<TActorId>& items) {
        TVector<TActorId> ordered(items.begin(), items.end());
        Sort(ordered);
        return FormatActorIdList(ordered);
    }

    void WriteStartupBacklogWhileNodeDown(TEnvironmentSetup& env, const TVector<TTargetVDisk>& targets) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(env.Settings.ControllerNodeId, __FILE__, __LINE__);

        for (size_t targetIdx = 0; targetIdx < targets.size(); ++targetIdx) {
            for (ui32 step = 1; step <= StartupBacklogBlobsPerGroup; ++step) {
                const TLogoBlobID blobId(300'000 + targetIdx, 1, step, 0, StartupBacklogBlobSize, 0);
                TString data = MakeData(StartupBacklogBlobSize, step);

                env.Runtime->WrapInActorContext(edge, [&] {
                    SendToBSProxy(edge, targets[targetIdx].GroupId,
                        new TEvBlobStorage::TEvPut(blobId, std::move(data), TInstant::Max()));
                });

                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false, TInstant::Max());
                UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK,
                    "groupId# " << targets[targetIdx].GroupId
                    << " blobId# " << blobId);
            }
        }

        env.Runtime->DestroyActor(edge);
        env.Sim(TDuration::Seconds(10));
    }

    void SendBrokerAcquire(TEnvironmentSetup& env, const TActorId& ownerActorId,
            const TActorId& brokerServiceId, const TActorId& vdiskServiceId)
    {
        const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdiskServiceId);
        Y_UNUSED(nodeId);
        Y_UNUSED(vslotId);
        env.Runtime->Send(new IEventHandle(brokerServiceId, ownerActorId,
            new TEvAcquireVDiskOperationToken(vdiskServiceId, pdiskId)), ownerActorId.NodeId());
    }

    void SendBrokerRelease(TEnvironmentSetup& env, const TActorId& ownerActorId,
            const TActorId& brokerServiceId, const TActorId& vdiskServiceId)
    {
        const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(vdiskServiceId);
        Y_UNUSED(nodeId);
        Y_UNUSED(vslotId);
        env.Runtime->Send(new IEventHandle(brokerServiceId, ownerActorId,
            new TEvReleaseVDiskOperationToken(vdiskServiceId, pdiskId)), ownerActorId.NodeId());
    }

    void WaitForBrokerToken(TEnvironmentSetup& env, const TActorId& ownerActorId, TStringBuf message) {
        auto* res = env.WaitForEdgeActorEvent<TEvVDiskOperationToken>(ownerActorId, false, env.Now() + TDuration::Seconds(30)).Get();
        UNIT_ASSERT_C(res, message);
    }

    void RestartVDisk(TEnvironmentSetup& env, const TTargetVDisk& target) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(target.VDiskActorId.NodeId(), __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(
            MakeBlobStorageNodeWardenID(target.VDiskActorId.NodeId()),
            edge,
            new TEvBlobStorage::TEvAskRestartVDisk(target.PDiskId, target.VDiskId)),
            target.VDiskActorId.NodeId());
        env.Runtime->DestroyActor(edge);
    }

    bool IsVDiskRunning(TEnvironmentSetup& env, const TTargetVDisk& target) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(target.VDiskActorId.NodeId(), __FILE__, __LINE__);
        env.Runtime->Send(new IEventHandle(
            target.VDiskActorId,
            edge,
            new TEvBlobStorage::TEvVStatus(target.VDiskId),
            IEventHandle::FlagTrackDelivery),
            edge.NodeId());

        auto res = env.Runtime->WaitForEdgeActorEvent({edge});
        env.Runtime->DestroyActor(edge);
        if (auto* msg = res->CastAsLocal<TEvBlobStorage::TEvVStatusResult>()) {
            const auto& record = msg->Record;
            return record.GetStatus() == NKikimrProto::OK && record.GetJoinedGroup() && record.GetReplicated();
        } else if (auto* msg = res->CastAsLocal<TEvents::TEvUndelivered>()) {
            Y_ABORT_UNLESS(msg->SourceType == TEvBlobStorage::EvVStatus);
            return false;
        } else {
            Y_ABORT();
        }
    }

    void WaitForVDisksToGetRunning(TEnvironmentSetup& env, const TVector<TTargetVDisk>& targets) {
        THashSet<TActorId> running;
        WaitUntil(env, [&] {
            for (const auto& target : targets) {
                if (!running.contains(target.VDiskActorId) && IsVDiskRunning(env, target)) {
                    running.insert(target.VDiskActorId);
                }
            }
            return running.size() == targets.size();
        }, "expected VDisks to get running");
    }

    struct TRestartScenario {
        TEnvironmentSetup Env;
        ui32 RestartedNodeId = 0;
        TVector<TTargetVDisk> RestartedNodeVDisks;
        TVector<TTargetVDisk> StartupTargets;
        TPerPDiskSelection PerPDiskSelection;

        TRestartScenario()
            : Env(TEnvironmentSetup::TSettings{
                .NodeCount = 9,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            })
        {
            Env.Runtime->SetLogPriority(NKikimrServices::BS_VDISK_SCRUB, NLog::PRI_ERROR);
            Env.CreateBoxAndPool(0, NumGroups);
            Env.Sim(TDuration::Seconds(30));

            SelectTargets();
        }

        ui32 SelectPeerNodeForGroup(ui32 groupId) {
            THashSet<ui32> nodes;
            const auto groupInfo = Env.GetGroupInfo(groupId);
            for (ui32 i = 0; i < groupInfo->GetTotalVDisksNum(); ++i) {
                const ui32 nodeId = groupInfo->GetActorId(i).NodeId();
                if (nodeId != RestartedNodeId && nodeId != Env.Settings.ControllerNodeId && nodes.emplace(nodeId).second) {
                    return nodeId;
                }
            }

            UNIT_FAIL("failed to choose peer node for group"
                << " groupId# " << groupId);

            return 0;
        }

    private:
        TVector<TTargetVDisk> SelectStartupTargets(const TVector<TTargetVDisk>& vdisks) {
            UNIT_ASSERT_C(vdisks.size() >= StartupBacklogTargetVDisks,
                "expected enough VDisks for startup backlog targets; vdisks# " << vdisks.size());

            TVector<TTargetVDisk> targets;
            targets.reserve(StartupBacklogTargetVDisks);
            for (ui32 i = 0; i < StartupBacklogTargetVDisks; ++i) {
                targets.push_back({
                    .GroupId = vdisks[i].GroupId,
                    .VDiskId = vdisks[i].VDiskId,
                    .VDiskActorId = vdisks[i].VDiskActorId,
                    .PDiskId = vdisks[i].PDiskId,
                });
            }
            return targets;
        }

        TPerPDiskSelection SelectPerPDiskTargets(const TVector<TTargetVDisk>& vdisks) {
            THashMap<ui32, TVector<TTargetVDisk>> byPDisk;
            for (const auto& item : vdisks) {
                byPDisk[item.PDiskId].push_back(item);
            }

            ui32 focusPDiskId = 0;
            size_t maxVDisksOnPDisk = 0;
            for (const auto& [pdiskId, items] : byPDisk) {
                if (items.size() >= 2 && items.size() > maxVDisksOnPDisk) {
                    focusPDiskId = pdiskId;
                    maxVDisksOnPDisk = items.size();
                }
            }

            UNIT_ASSERT_C(focusPDiskId,
                "expected at least two VDisks on the same PDisk to test per-PDisk broker limits");

            TVector<TTargetVDisk> focusTargets = byPDisk[focusPDiskId];
            TTargetVDisk otherTarget;
            bool foundOtherTarget = false;
            for (const auto& item : vdisks) {
                if (item.PDiskId != focusPDiskId) {
                    otherTarget = item;
                    foundOtherTarget = true;
                    break;
                }
            }

            UNIT_ASSERT_C(foundOtherTarget,
                "expected at least one more VDisk on another PDisk to distinguish per-node from per-PDisk limit");

            TPerPDiskSelection selection;
            selection.FocusPDiskId = focusPDiskId;
            selection.FocusTargets.push_back(focusTargets[0]);
            selection.FocusTargets.push_back(focusTargets[1]);
            selection.OtherTarget = otherTarget;
            selection.StartupTargets = {
                {
                    .GroupId = focusTargets[0].GroupId,
                    .VDiskId = focusTargets[0].VDiskId,
                    .VDiskActorId = focusTargets[0].VDiskActorId,
                    .PDiskId = focusTargets[0].PDiskId,
                },
                {
                    .GroupId = focusTargets[1].GroupId,
                    .VDiskId = focusTargets[1].VDiskId,
                    .VDiskActorId = focusTargets[1].VDiskActorId,
                    .PDiskId = focusTargets[1].PDiskId,
                },
                {
                    .GroupId = otherTarget.GroupId,
                    .VDiskId = otherTarget.VDiskId,
                    .VDiskActorId = otherTarget.VDiskActorId,
                    .PDiskId = otherTarget.PDiskId,
                },
            };
            return selection;
        }

        void SelectTargets() {
            NKikimrBlobStorage::TBaseConfig config = Env.FetchBaseConfig();
            ui32 controllerNodeId = Env.Settings.ControllerNodeId;
            THashMap<ui32, TVector<TTargetVDisk>> vdisksPerNode;
            for (const auto& vslot : config.GetVSlot()) {
                const auto& slotId = vslot.GetVSlotId();
                const ui32 nodeId = slotId.GetNodeId();
                if (nodeId == controllerNodeId) {
                    continue;
                }

                vdisksPerNode[nodeId].push_back({
                    .GroupId = vslot.GetGroupId(),
                    .VDiskId = TVDiskID(
                        vslot.GetGroupId(),
                        vslot.GetGroupGeneration(),
                        vslot.GetFailRealmIdx(),
                        vslot.GetFailDomainIdx(),
                        vslot.GetVDiskIdx()),
                    .VDiskActorId = MakeBlobStorageVDiskID(slotId.GetNodeId(), slotId.GetPDiskId(), slotId.GetVSlotId()),
                    .PDiskId = slotId.GetPDiskId(),
                });
            }

            ui32 selectedNodeId = 0;
            size_t maxVDisks = 0;
            for (auto& [nodeId, vdisks] : vdisksPerNode) {
                if (vdisks.size() > maxVDisks) {
                    selectedNodeId = nodeId;
                    maxVDisks = vdisks.size();
                }
            }

            UNIT_ASSERT_C(selectedNodeId, "failed to choose node to restart");
            auto it = vdisksPerNode.find(selectedNodeId);
            UNIT_ASSERT(it != vdisksPerNode.end());

            RestartedNodeId = selectedNodeId;
            TVector<TTargetVDisk>& restartedNodeVDisks = it->second;
            RestartedNodeVDisks = restartedNodeVDisks;

            UNIT_ASSERT_C(restartedNodeVDisks.size() >= MinExpectedVDisksOnRestartedNode,
                "expected many VDisks on restarted node; restartedNodeId# " << RestartedNodeId
                << " vdisks# " << restartedNodeVDisks.size());

            StartupTargets = SelectStartupTargets(restartedNodeVDisks);
            PerPDiskSelection = SelectPerPDiskTargets(restartedNodeVDisks);
        }
    };

    void RestartNodeForBrokerTest(TRestartScenario& scenario, const TBrokerControls& controls,
            const TVector<TTargetVDisk>* startupBacklogTargets = nullptr)
    {
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId,
            controls.MaxInProgressLocalRecoveryCount,
            controls.MaxInProgressLocalRecoveryPerPDiskCount,
            controls.MaxInProgressStartupDataSyncCount,
            controls.MaxInProgressStartupDataSyncPerPDiskCount);
        scenario.Env.StopNode(scenario.RestartedNodeId);
        scenario.Env.Sim(TDuration::Seconds(5));
        if (startupBacklogTargets) {
            WriteStartupBacklogWhileNodeDown(scenario.Env, *startupBacklogTargets);
        }
        scenario.Env.StartNode(scenario.RestartedNodeId);
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId,
            controls.MaxInProgressLocalRecoveryCount,
            controls.MaxInProgressLocalRecoveryPerPDiskCount,
            controls.MaxInProgressStartupDataSyncCount,
            controls.MaxInProgressStartupDataSyncPerPDiskCount);
    }

    struct TVDiskTokenInfo {
        TActorId VDiskActorId;
        ui32 PDiskId = 0;
    };

    enum class ECompletionEventType {
        YardInitResult,
        StartupDataSyncDone,
    };

    struct TBrokerCaptureBase {
        const TActorId BrokerServiceId;
        const ECompletionEventType CompletionEventType;
        THashMap<TActorId, TVDiskTokenInfo> VDiskByOwnerActor;
        THashMap<TActorId, ui32> PDiskIdByVDiskActor;
        THashSet<TActorId> QueriedVDisks;
        TVector<TActorId> GrantedVDisks;
        TVector<TActorId> ReleasedVDisks;
        std::optional<ui32> FocusPDiskId;
        std::optional<TActorId> SelectedOwnerActor;
        bool ShouldStashSelectedCompletionEvent = true;
        std::unique_ptr<IEventHandle> StashedCompletionEvent;

        explicit TBrokerCaptureBase(TActorId brokerServiceId, ECompletionEventType completionEventType,
                std::optional<ui32> focusPDiskId = std::nullopt)
            : BrokerServiceId(brokerServiceId)
            , CompletionEventType(completionEventType)
            , FocusPDiskId(focusPDiskId)
        {}

        void OnQuery(const TActorId& ownerActor, const TActorId& vdiskActorId, ui32 pdiskId) {
            VDiskByOwnerActor[ownerActor] = {vdiskActorId, pdiskId};
            PDiskIdByVDiskActor[vdiskActorId] = pdiskId;
            QueriedVDisks.insert(vdiskActorId);
        }

        void OnGrant(const TActorId& ownerActor) {
            const auto it = VDiskByOwnerActor.find(ownerActor);
            UNIT_ASSERT(it != VDiskByOwnerActor.end());
            GrantedVDisks.push_back(it->second.VDiskActorId);
            // Per-node scenarios may pick the first granted owner arbitrarily.
            // Per-PDisk scenarios must pick an owner on the focus PDisk so that
            // the stashed completion event blocks the specific PDisk lane under test.
            if (!SelectedOwnerActor && (!FocusPDiskId || it->second.PDiskId == *FocusPDiskId)) {
                SelectedOwnerActor = ownerActor;
            }
        }

        void OnRelease(const TActorId& vdiskActorId, ui32 pdiskId) {
            PDiskIdByVDiskActor[vdiskActorId] = pdiskId;
            ReleasedVDisks.push_back(vdiskActorId);
        }

        size_t CountQueriedOnPDisk(ui32 pdiskId) const {
            return std::count_if(QueriedVDisks.begin(), QueriedVDisks.end(), [&](const TActorId& vdiskActorId) {
                const auto it = PDiskIdByVDiskActor.find(vdiskActorId);
                return it != PDiskIdByVDiskActor.end() && it->second == pdiskId;
            });
        }

        size_t CountGrantedOnPDisk(ui32 pdiskId) const {
            return std::count_if(GrantedVDisks.begin(), GrantedVDisks.end(), [&](const TActorId& vdiskActorId) {
                const auto it = PDiskIdByVDiskActor.find(vdiskActorId);
                return it != PDiskIdByVDiskActor.end() && it->second == pdiskId;
            });
        }

        bool HasAnotherGrantedOnPDisk(ui32 pdiskId, const TActorId& excludedVDiskActorId) const {
            return std::any_of(GrantedVDisks.begin(), GrantedVDisks.end(), [&](const TActorId& vdiskActorId) {
                const auto it = PDiskIdByVDiskActor.find(vdiskActorId);
                return vdiskActorId != excludedVDiskActorId && it != PDiskIdByVDiskActor.end() && it->second == pdiskId;
            });
        }

        bool HasReleasedVDisk(const TActorId& vdiskActorId) const {
            return std::find(ReleasedVDisks.begin(), ReleasedVDisks.end(), vdiskActorId) != ReleasedVDisks.end();
        }

        bool HasOneGrantedAndAnotherWaitingPerNode() const {
            return StashedCompletionEvent && SelectedOwnerActor
                && QueriedVDisks.size() >= 2
                && GrantedVDisks.size() == 1;
        }

        bool HasOneGrantedAndAnotherWaitingPerPDisk() const {
            // Besides observing contention on the focus PDisk, require one more grant overall.
            // This proves that only the focus PDisk lane is throttled, while another PDisk can still progress.
            return FocusPDiskId && StashedCompletionEvent && SelectedOwnerActor
                && CountQueriedOnPDisk(*FocusPDiskId) >= 2
                && CountGrantedOnPDisk(*FocusPDiskId) == 1
                && GrantedVDisks.size() >= 2;
        }

        TActorId SelectedVDiskActor() const {
            UNIT_ASSERT(SelectedOwnerActor);
            const auto it = VDiskByOwnerActor.find(*SelectedOwnerActor);
            UNIT_ASSERT(it != VDiskByOwnerActor.end());
            return it->second.VDiskActorId;
        }

        ui32 SelectedPDiskId() const {
            UNIT_ASSERT(SelectedOwnerActor);
            const auto it = VDiskByOwnerActor.find(*SelectedOwnerActor);
            UNIT_ASSERT(it != VDiskByOwnerActor.end());
            return it->second.PDiskId;
        }

        void StashIfSelected(std::unique_ptr<IEventHandle>& ev) {
            if (ShouldStashSelectedCompletionEvent && SelectedOwnerActor && ev->Recipient == *SelectedOwnerActor
                    && !StashedCompletionEvent) {
                StashedCompletionEvent = std::move(ev);
            }
        }

        bool IsSelectedCompletionEvent(ui32 eventType, const TActorId& recipient) const {
            if (!SelectedOwnerActor || recipient != *SelectedOwnerActor || StashedCompletionEvent) {
                return false;
            }

            switch (CompletionEventType) {
                case ECompletionEventType::YardInitResult:
                    return eventType == NPDisk::TEvYardInitResult::EventType;

                case ECompletionEventType::StartupDataSyncDone:
                    return eventType == TEvStartupDataSyncDone::EventType;
            }

            return false;
        }

        bool Handle(std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvAcquireVDiskOperationToken::EventType: {
                    if (ev->Recipient != BrokerServiceId) {
                        break;
                    }
                    auto* msg = ev->Get<TEvAcquireVDiskOperationToken>();
                    OnQuery(ev->Sender, msg->VDiskServiceId, msg->PDiskId);
                    break;
                }

                case TEvVDiskOperationToken::EventType:
                    if (!VDiskByOwnerActor.contains(ev->Recipient)) {
                        break;
                    }
                    OnGrant(ev->Recipient);
                    break;

                case TEvReleaseVDiskOperationToken::EventType: {
                    if (ev->Recipient != BrokerServiceId) {
                        break;
                    }
                    auto* msg = ev->Get<TEvReleaseVDiskOperationToken>();
                    OnRelease(msg->VDiskServiceId, msg->PDiskId);
                    break;
                }
            }

            if (IsSelectedCompletionEvent(ev->GetTypeRewrite(), ev->Recipient)) {
                if (ShouldStashSelectedCompletionEvent) {
                    StashIfSelected(ev);
                    return false;
                }
            }

            return true;
        }

        void ResumeStashedEvent(TEnvironmentSetup& env) {
            UNIT_ASSERT(StashedCompletionEvent);
            ShouldStashSelectedCompletionEvent = false;
            auto* raw = StashedCompletionEvent.release();
            const TActorId recipient = raw->Recipient;
            const bool delivered = env.Runtime->WrapInActorContext(recipient, [&](IActor* actor) {
                TAutoPtr<IEventHandle> ev(raw);
                actor->Receive(ev);
            });
            UNIT_ASSERT_C(delivered, "failed to deliver stashed event to actor; recipient# " << recipient);
        }
    };

    struct TScopedCaptureFilter {
        using TFilterFunction = decltype(std::declval<TTestActorSystem>().FilterFunction);

        TEnvironmentSetup& Env;
        TFilterFunction PrevFilter;

        template<typename TCapture>
        TScopedCaptureFilter(TEnvironmentSetup& env, TCapture& capture)
            : Env(env)
            , PrevFilter(env.Runtime->FilterFunction)
        {
            Env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (capture.Handle(ev)) {
                    return PrevFilter ? PrevFilter(nodeId, ev) : true;
                }
                return false;
            };
        }

        ~TScopedCaptureFilter() {
            Env.Runtime->FilterFunction = PrevFilter;
        }
    };

    struct TLocalRecoveryCapture : TBrokerCaptureBase {
        explicit TLocalRecoveryCapture(std::optional<ui32> focusPDiskId = std::nullopt)
            : TBrokerCaptureBase(MakeBlobStorageLocalRecoveryBrokerID(),
                ECompletionEventType::YardInitResult, focusPDiskId)
        {}

        void FailSelectedYardInit(TEnvironmentSetup& env) {
            UNIT_ASSERT(StashedCompletionEvent);
            ShouldStashSelectedCompletionEvent = false;
            const TActorId recipient = StashedCompletionEvent->Recipient;
            const TActorId sender = StashedCompletionEvent->Sender;
            StashedCompletionEvent.reset();
            const bool delivered = env.Runtime->WrapInActorContext(recipient, [&](IActor* actor) {
                TAutoPtr<IEventHandle> ev(new IEventHandle(recipient, sender,
                    new NPDisk::TEvYardInitResult(NKikimrProto::CORRUPTED, "local recovery broker test error")));
                actor->Receive(ev);
            });
            UNIT_ASSERT_C(delivered, "failed to inject TEvYardInitResult into actor; recipient# " << recipient);
        }
    };

    struct TStartupDataSyncCapture : TBrokerCaptureBase {
        explicit TStartupDataSyncCapture(std::optional<ui32> focusPDiskId = std::nullopt)
            : TBrokerCaptureBase(MakeBlobStorageStartupDataSyncBrokerID(),
                ECompletionEventType::StartupDataSyncDone, focusPDiskId)
        {}

        void PoisonSelectedSyncer(TEnvironmentSetup& env) const {
            UNIT_ASSERT(SelectedOwnerActor);
            env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, *SelectedOwnerActor, {}, nullptr, 0),
                SelectedOwnerActor->NodeId());
        }
    };

    struct TStartupDataSyncJobDoneCapture : TStartupDataSyncCapture {
        std::unique_ptr<IEventHandle> StashedSyncerJobDone;

        TStartupDataSyncJobDoneCapture() {
            ShouldStashSelectedCompletionEvent = false;
        }

        bool Handle(std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvSyncerJobDone::EventType && SelectedOwnerActor && !StashedSyncerJobDone) {
                StashedSyncerJobDone = std::move(ev);
                return false;
            }

            return TStartupDataSyncCapture::Handle(ev);
        }

        void ResumeStashedSyncerJobDone(TEnvironmentSetup& env) {
            UNIT_ASSERT(StashedSyncerJobDone);
            auto* raw = StashedSyncerJobDone.release();
            const TActorId recipient = raw->Recipient;
            const bool delivered = env.Runtime->WrapInActorContext(recipient, [&](IActor* actor) {
                TAutoPtr<IEventHandle> ev(raw);
                actor->Receive(ev);
            });
            UNIT_ASSERT_C(delivered, "failed to deliver stashed TEvSyncerJobDone to actor; recipient# " << recipient);
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(VDiskStartupBrokers) {

    // Active startup data sync owner dies; the broker frees its token and grants a new owner.
    Y_UNIT_TEST(StartupDataSyncBrokerAllowsNewOwnerAfterActiveOwnerDies) {
        TRestartScenario scenario;
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId, 0, 0, 1, 0);

        const TActorId brokerServiceId = MakeBlobStorageStartupDataSyncBrokerID();
        const TActorId vdiskServiceId = scenario.StartupTargets.front().VDiskActorId;
        const TActorId firstOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);
        const TActorId secondOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);

        SendBrokerAcquire(scenario.Env, firstOwnerActorId, brokerServiceId, vdiskServiceId);
        WaitForBrokerToken(scenario.Env, firstOwnerActorId,
            "expected first owner to get startup data sync token");

        scenario.Env.Runtime->DestroyActor(firstOwnerActorId);
        scenario.Env.Sim(TDuration::Seconds(1));

        SendBrokerAcquire(scenario.Env, secondOwnerActorId, brokerServiceId, vdiskServiceId);
        WaitForBrokerToken(scenario.Env, secondOwnerActorId,
            "expected new owner to get startup data sync token after previous owner died");
    }

    // Queued startup data sync owner dies; a replacement waits and receives the token after release.
    Y_UNIT_TEST(StartupDataSyncBrokerAllowsNewOwnerAfterWaitingOwnerDies) {
        TRestartScenario scenario;
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId, 0, 0, 1, 0);

        const TActorId brokerServiceId = MakeBlobStorageStartupDataSyncBrokerID();
        const TActorId holderVDiskServiceId = scenario.StartupTargets[0].VDiskActorId;
        const TActorId queuedVDiskServiceId = scenario.StartupTargets[1].VDiskActorId;
        const TActorId holderActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);
        const TActorId staleWaitingOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);
        const TActorId replacementWaitingOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);

        SendBrokerAcquire(scenario.Env, holderActorId, brokerServiceId, holderVDiskServiceId);
        WaitForBrokerToken(scenario.Env, holderActorId,
            "expected holder actor to get startup data sync token");

        SendBrokerAcquire(scenario.Env, staleWaitingOwnerActorId, brokerServiceId, queuedVDiskServiceId);
        scenario.Env.Sim(TDuration::Seconds(1));
        scenario.Env.Runtime->DestroyActor(staleWaitingOwnerActorId);
        scenario.Env.Sim(TDuration::Seconds(1));

        SendBrokerAcquire(scenario.Env, replacementWaitingOwnerActorId, brokerServiceId, queuedVDiskServiceId);
        SendBrokerRelease(scenario.Env, holderActorId, brokerServiceId, holderVDiskServiceId);
        WaitForBrokerToken(scenario.Env, replacementWaitingOwnerActorId,
            "expected replacement owner to get startup data sync token after stale waiting owner died");
    }

    // Dead queued owner is skipped when activated, so the next queued owner is not stuck.
    Y_UNIT_TEST(StartupDataSyncBrokerDoesNotHangWhenQueuedOwnerDiesBeforeActivation) {
        TRestartScenario scenario;
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId, 0, 0, 1, 0);

        const TActorId brokerServiceId = MakeBlobStorageStartupDataSyncBrokerID();
        const TActorId holderVDiskServiceId = scenario.StartupTargets[0].VDiskActorId;
        const TActorId staleQueuedVDiskServiceId = scenario.StartupTargets[1].VDiskActorId;
        const TActorId nextQueuedVDiskServiceId = scenario.StartupTargets[2].VDiskActorId;
        const TActorId holderActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);
        const TActorId staleQueuedOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);
        const TActorId nextQueuedOwnerActorId = scenario.Env.Runtime->AllocateEdgeActor(scenario.RestartedNodeId, __FILE__, __LINE__);

        SendBrokerAcquire(scenario.Env, holderActorId, brokerServiceId, holderVDiskServiceId);
        WaitForBrokerToken(scenario.Env, holderActorId,
            "expected holder actor to get startup data sync token");

        // Queue one VDisk behind the holder and then kill its owner before the queue reaches it.
        // After the holder releases token, the broker will try to activate this stale queue entry.
        // Without TrackDelivery + Undelivered cleanup this dead entry would keep the only token forever.
        SendBrokerAcquire(scenario.Env, staleQueuedOwnerActorId, brokerServiceId, staleQueuedVDiskServiceId);
        SendBrokerAcquire(scenario.Env, nextQueuedOwnerActorId, brokerServiceId, nextQueuedVDiskServiceId);
        scenario.Env.Sim(TDuration::Seconds(1));
        scenario.Env.Runtime->DestroyActor(staleQueuedOwnerActorId);
        scenario.Env.Sim(TDuration::Seconds(1));

        SendBrokerRelease(scenario.Env, holderActorId, brokerServiceId, holderVDiskServiceId);
        WaitForBrokerToken(scenario.Env, nextQueuedOwnerActorId,
            "expected next queued owner to get startup data sync token after stale queued owner dies");
    }

    // After the initial local-recovery wave is drained, later VDisk restarts still respect broker limits.
    Y_UNIT_TEST(LocalRecoveryBrokerSerializesLateVDiskStartsAfterInitialStartup) {
        TRestartScenario scenario;
        RestartNodeForBrokerTest(scenario, {});
        WaitForVDisksToGetRunning(scenario.Env, scenario.RestartedNodeVDisks);
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId, 1, 0, 0, 0);

        TLocalRecoveryCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);

        RestartVDisk(scenario.Env, scenario.RestartedNodeVDisks[0]);
        RestartVDisk(scenario.Env, scenario.RestartedNodeVDisks[1]);

        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected late VDisk restart to hold local recovery token while another late VDisk is waiting");

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "expected only one local recovery token for late VDisk restart before unblocking first VDisk");

        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] { return capture.GrantedVDisks.size() >= 2; },
            "expected second late VDisk to get local recovery token after the first one proceeds");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected local recovery broker to hand token to another late VDisk after release;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // After the initial startup data sync wave is drained, later VDisk restarts still respect broker limits.
    Y_UNIT_TEST(StartupDataSyncBrokerSerializesLateVDiskStartsAfterInitialStartup) {
        TRestartScenario scenario;
        RestartNodeForBrokerTest(scenario, {}, &scenario.StartupTargets);
        WaitForVDisksToGetRunning(scenario.Env, scenario.RestartedNodeVDisks);
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId, 0, 0, 1, 0);

        TStartupDataSyncCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);

        RestartVDisk(scenario.Env, scenario.RestartedNodeVDisks[0]);
        RestartVDisk(scenario.Env, scenario.RestartedNodeVDisks[1]);

        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected late VDisk restart to hold startup data sync token while another late VDisk is waiting");

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "expected only one startup data sync token for late VDisk restart before unblocking first VDisk");

        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] { return capture.GrantedVDisks.size() >= 2; },
            "expected second late VDisk to get startup data sync token after the first one completes");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected startup data sync broker to hand token to another late VDisk after release;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // Node-wide local recovery limit allows one VDisk at a time and hands off after completion.
    Y_UNIT_TEST(LocalRecoveryBrokerSerializesStartupPerNode) {
        TRestartScenario scenario;
        TLocalRecoveryCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressLocalRecoveryCount = 1});

        // Wait until two VDisks have reached the broker, but only one token is granted.
        // The selected owner's completion event is stashed to keep that state stable.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one VDisk to hold local recovery token while another VDisk is already waiting");
        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "expected only one local recovery token before unblocking first VDisk; queriedVDisks# "
            << capture.QueriedVDisks.size());

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] { return capture.GrantedVDisks.size() >= 2; },
            "expected another VDisk to get local recovery token after the first one proceeds");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected local recovery broker to hand token to another VDisk after release;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // Failed local recovery owner releases its node-wide token for another VDisk.
    Y_UNIT_TEST(LocalRecoveryBrokerReleasesTokenOnStartupFailurePerNode) {
        TRestartScenario scenario;
        TLocalRecoveryCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressLocalRecoveryCount = 1});

        // Observe the throttled state first, then fail the selected owner and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one VDisk to hold local recovery token while another VDisk is waiting");

        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.FailSelectedYardInit(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId) && capture.GrantedVDisks.size() >= 2;
        }, "expected failed local recovery owner to release token and let another VDisk proceed");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != failedVDiskActorId,
            "expected local recovery broker to pass token to another VDisk after startup failure;"
            << " failedVDiskActorId# " << failedVDiskActorId
            << " secondGrantedVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // Per-PDisk local recovery limit blocks one PDisk while another PDisk keeps progressing.
    Y_UNIT_TEST(LocalRecoveryBrokerSerializesStartupPerPDisk) {
        TRestartScenario scenario;
        TLocalRecoveryCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressLocalRecoveryPerPDiskCount = 1});

        // For per-PDisk throttling we require two things:
        // 1. another VDisk on the same PDisk is already waiting;
        // 2. a VDisk on a different PDisk is still allowed to proceed.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one VDisk on focus PDisk to hold local recovery token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        UNIT_ASSERT_VALUES_EQUAL_C(capture.CountGrantedOnPDisk(focusPDiskId), 1,
            "expected exactly one local recovery token on focus PDisk before release");
        UNIT_ASSERT_C(capture.GrantedVDisks.size() >= 2,
            "expected another PDisk to continue while focus PDisk is blocked; focusPDiskId# " << focusPDiskId);

        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] {
            return capture.HasAnotherGrantedOnPDisk(focusPDiskId, firstVDiskActorId);
        }, "expected another VDisk on focus PDisk to get token after the first one proceeds");
    }

    // Failed local recovery owner releases its per-PDisk token for another VDisk on that PDisk.
    Y_UNIT_TEST(LocalRecoveryBrokerReleasesTokenOnStartupFailurePerPDisk) {
        TRestartScenario scenario;
        TLocalRecoveryCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressLocalRecoveryPerPDiskCount = 1});

        // Observe the per-PDisk throttled state first, then fail the selected owner and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one VDisk on focus PDisk to hold local recovery token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.FailSelectedYardInit(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId)
                && capture.HasAnotherGrantedOnPDisk(focusPDiskId, failedVDiskActorId);
        }, "expected failed local recovery owner to release token for another VDisk on the same PDisk");
    }

    // Node-wide startup data sync limit allows one Syncer at a time and hands off after completion.
    Y_UNIT_TEST(StartupDataSyncBrokerSerializesStartupDataSyncPerNode) {
        TRestartScenario scenario;
        TStartupDataSyncCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncCount = 1}, &scenario.StartupTargets);

        // Wait until two Syncers have reached the broker, but only one token is granted.
        // The selected owner's completion event is stashed to keep that state stable.
        for (ui32 i = 0; i < WaitIterations && !capture.HasOneGrantedAndAnotherWaitingPerNode(); ++i) {
            scenario.Env.Sim(WaitStep);
        }
        UNIT_ASSERT_C(capture.HasOneGrantedAndAnotherWaitingPerNode(),
            "expected one Syncer to hold startup data sync token while another VDisk is already waiting"
            << "; queried# " << capture.QueriedVDisks.size()
            << " queried_vdisks# " << FormatActorIdSet(capture.QueriedVDisks)
            << " granted# " << capture.GrantedVDisks.size()
            << " granted_vdisks# " << FormatActorIdList(capture.GrantedVDisks)
            << " released# " << capture.ReleasedVDisks.size()
            << " released_vdisks# " << FormatActorIdList(capture.ReleasedVDisks)
            << " selected_owner# " << (capture.SelectedOwnerActor ? capture.SelectedOwnerActor->ToString() : TString("<none>"))
            << " stashed# " << bool(capture.StashedCompletionEvent));
        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "expected only one startup data sync token before releasing first Syncer; queriedVDisks# "
            << capture.QueriedVDisks.size());

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] { return capture.GrantedVDisks.size() >= 2; },
            "expected another VDisk to get startup data sync token after the first one completes startup wave");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected startup data sync broker to pass token to another VDisk after startup wave completion;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // Dead Syncer releases its node-wide startup data sync token for another VDisk.
    Y_UNIT_TEST(StartupDataSyncBrokerReleasesTokenWhenSyncerDiesPerNode) {
        TRestartScenario scenario;
        TStartupDataSyncCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncCount = 1}, &scenario.StartupTargets);

        // Observe the throttled state first, then kill the selected Syncer and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one Syncer to hold startup data sync token while another VDisk is already waiting");

        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.PoisonSelectedSyncer(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId) && capture.GrantedVDisks.size() >= 2;
        }, "expected startup data sync token to be released when the owning Syncer dies");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != failedVDiskActorId,
            "expected startup data sync broker to pass token to another VDisk after Syncer death;"
            << " failedVDiskActorId# " << failedVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    // Startup data sync token stays held until the initial due sync job result is applied.
    Y_UNIT_TEST(StartupDataSyncBrokerKeepsTokenUntilInitialDueSyncAttemptCompletes) {
        TRestartScenario scenario;
        TStartupDataSyncJobDoneCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncCount = 1});

        WaitUntil(scenario.Env, [&] { return capture.StashedSyncerJobDone != nullptr; },
            "expected first startup sync job to finish while startup data sync token is still held");

        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "startup data sync token must not be released before the initial due sync attempt is applied;"
            << " granted_vdisks# " << FormatActorIdList(capture.GrantedVDisks)
            << " released_vdisks# " << FormatActorIdList(capture.ReleasedVDisks));
        UNIT_ASSERT_C(capture.ReleasedVDisks.empty(),
            "startup data sync token was released before the initial due sync attempt was applied;"
            << " released_vdisks# " << FormatActorIdList(capture.ReleasedVDisks));

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        capture.ResumeStashedSyncerJobDone(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(firstVDiskActorId) && capture.GrantedVDisks.size() >= 2;
        }, "expected startup data sync token to be released after the initial due sync attempt is applied");
    }

    // Offline peer does not keep the node-wide startup data sync token held forever.
    Y_UNIT_TEST(StartupDataSyncBrokerReleasesTokenWhenPeerIsOfflinePerNode) {
        TRestartScenario scenario;
        const ui32 offlinePeerNodeId = scenario.SelectPeerNodeForGroup(scenario.StartupTargets.front().GroupId);
        TStartupDataSyncCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);

        // Keep one startup data sync peer offline and verify that the first Syncer still
        // eventually completes its startup wave, releases the token, and lets another
        // VDisk proceed instead of blocking the broker forever.
        scenario.Env.StopNode(offlinePeerNodeId);
        scenario.Env.Sim(TDuration::Seconds(5));
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncCount = 1}, &scenario.StartupTargets);

        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one Syncer to hold startup data sync token while another VDisk is already waiting");

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        capture.ResumeStashedEvent(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(firstVDiskActorId) && capture.GrantedVDisks.size() >= 2;
        }, "expected startup data sync token to be released even when one peer is offline");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected startup data sync broker to pass token to another VDisk after an offline-peer attempt;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]
            << " offlinePeerNodeId# " << offlinePeerNodeId);
    }

    // Per-PDisk startup data sync limit blocks one PDisk while another PDisk keeps progressing.
    Y_UNIT_TEST(StartupDataSyncBrokerSerializesStartupDataSyncPerPDisk) {
        TRestartScenario scenario;
        TStartupDataSyncCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncPerPDiskCount = 1},
            &scenario.PerPDiskSelection.StartupTargets);

        // For per-PDisk throttling we require two things:
        // 1. another Syncer on the same PDisk is already waiting;
        // 2. a Syncer on a different PDisk is still allowed to proceed.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one Syncer on focus PDisk to hold startup data sync token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        UNIT_ASSERT_VALUES_EQUAL_C(capture.CountGrantedOnPDisk(focusPDiskId), 1,
            "expected exactly one startup data sync token on focus PDisk before release");
        UNIT_ASSERT_C(capture.GrantedVDisks.size() >= 2,
            "expected another PDisk to continue startup data sync while focus PDisk is blocked; focusPDiskId# "
            << focusPDiskId);

        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] {
            return capture.HasAnotherGrantedOnPDisk(focusPDiskId, firstVDiskActorId);
        }, "expected another VDisk on focus PDisk to get startup data sync token after the first one completes");
    }

    // Dead Syncer releases its per-PDisk startup data sync token for another VDisk on that PDisk.
    Y_UNIT_TEST(StartupDataSyncBrokerReleasesTokenWhenSyncerDiesPerPDisk) {
        TRestartScenario scenario;
        TStartupDataSyncCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupDataSyncPerPDiskCount = 1},
            &scenario.PerPDiskSelection.StartupTargets);

        // Observe the per-PDisk throttled state first, then kill the selected Syncer and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one Syncer on focus PDisk to hold startup data sync token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.PoisonSelectedSyncer(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId)
                && capture.HasAnotherGrantedOnPDisk(focusPDiskId, failedVDiskActorId);
        }, "expected startup data sync token to be released for another VDisk on the same PDisk when Syncer dies");
    }

}
