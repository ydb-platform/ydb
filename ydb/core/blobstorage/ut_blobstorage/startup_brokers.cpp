#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_operation_broker.h>
#include <ydb/core/blobstorage/vdisk/syncer/blobstorage_syncer_scheduler.h>

#include <algorithm>
#include <optional>
#include <utility>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace {

    constexpr const char* MaxInProgressLocalRecoveryCountControl =
        "VDiskControls.MaxInProgressLocalRecoveryCount";
    constexpr const char* MaxInProgressLocalRecoveryPerPDiskCountControl =
        "VDiskControls.MaxInProgressLocalRecoveryPerPDiskCount";
    constexpr const char* MaxInProgressStartupCatchupCountControl =
        "VDiskControls.MaxInProgressStartupCatchupCount";
    constexpr const char* MaxInProgressStartupCatchupPerPDiskCountControl =
        "VDiskControls.MaxInProgressStartupCatchupPerPDiskCount";

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
        ui64 MaxInProgressStartupCatchupCount = 0;
        ui64 MaxInProgressStartupCatchupPerPDiskCount = 0;
    };

    void SetNodeControl(TTestActorSystem& runtime, ui32 nodeId, const TString& controlName, i64 value,
            i64 defaultValue = 0, i64 minValue = 0, i64 maxValue = 1'000)
    {
        TAppData* appData = runtime.GetNode(nodeId)->AppData.get();
        TAtomic currentValue = 0;
        bool exists = false;
        appData->Icb->GetValue(controlName, currentValue, exists);

        if (exists) {
            TAtomic prevValue = 0;
            appData->Icb->SetValue(controlName, value, prevValue);
        } else {
            TControlWrapper control(defaultValue, minValue, maxValue);
            appData->Icb->RegisterSharedControl(control, controlName);
            control = value;
        }
    }

    void ConfigureBrokerControls(TEnvironmentSetup& env, ui32 nodeId,
            ui64 maxInProgressLocalRecoveryCount, ui64 maxInProgressLocalRecoveryPerPDiskCount,
            ui64 maxInProgressStartupCatchupCount, ui64 maxInProgressStartupCatchupPerPDiskCount)
    {
        SetNodeControl(*env.Runtime, nodeId, MaxInProgressLocalRecoveryCountControl, maxInProgressLocalRecoveryCount);
        SetNodeControl(*env.Runtime, nodeId, MaxInProgressLocalRecoveryPerPDiskCountControl,
            maxInProgressLocalRecoveryPerPDiskCount);
        SetNodeControl(*env.Runtime, nodeId, MaxInProgressStartupCatchupCountControl,
            maxInProgressStartupCatchupCount);
        SetNodeControl(*env.Runtime, nodeId, MaxInProgressStartupCatchupPerPDiskCountControl,
            maxInProgressStartupCatchupPerPDiskCount);
    }

    template<typename TPredicate>
    void WaitUntil(TEnvironmentSetup& env, TPredicate&& predicate, TStringBuf message) {
        for (ui32 i = 0; i < WaitIterations && !predicate(); ++i) {
            env.Sim(WaitStep);
        }
        UNIT_ASSERT_C(predicate(), message);
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

    struct TRestartScenario {
        TEnvironmentSetup Env;
        ui32 RestartedNodeId = 0;
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
            controls.MaxInProgressStartupCatchupCount,
            controls.MaxInProgressStartupCatchupPerPDiskCount);
        scenario.Env.StopNode(scenario.RestartedNodeId);
        scenario.Env.Sim(TDuration::Seconds(5));
        if (startupBacklogTargets) {
            WriteStartupBacklogWhileNodeDown(scenario.Env, *startupBacklogTargets);
        }
        scenario.Env.StartNode(scenario.RestartedNodeId);
        ConfigureBrokerControls(scenario.Env, scenario.RestartedNodeId,
            controls.MaxInProgressLocalRecoveryCount,
            controls.MaxInProgressLocalRecoveryPerPDiskCount,
            controls.MaxInProgressStartupCatchupCount,
            controls.MaxInProgressStartupCatchupPerPDiskCount);
    }

    struct TVDiskTokenInfo {
        TActorId VDiskActorId;
        ui32 PDiskId = 0;
    };

    enum class ECompletionEventType {
        YardInitResult,
        StartupCatchupDone,
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

                case ECompletionEventType::StartupCatchupDone:
                    return eventType == TEvStartupCatchupDone::EventType;
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
                    const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(msg->VDiskServiceId);
                    Y_UNUSED(nodeId);
                    Y_UNUSED(vslotId);
                    OnQuery(ev->Sender, msg->VDiskServiceId, pdiskId);
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
                    const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(msg->VDiskServiceId);
                    Y_UNUSED(nodeId);
                    Y_UNUSED(vslotId);
                    OnRelease(msg->VDiskServiceId, pdiskId);
                    break;
                }
            }

            if (IsSelectedCompletionEvent(ev->GetTypeRewrite(), ev->Recipient)) {
                StashIfSelected(ev);
                return false;
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

    struct TStartupCatchupCapture : TBrokerCaptureBase {
        explicit TStartupCatchupCapture(std::optional<ui32> focusPDiskId = std::nullopt)
            : TBrokerCaptureBase(MakeBlobStorageStartupCatchupBrokerID(),
                ECompletionEventType::StartupCatchupDone, focusPDiskId)
        {}

        void PoisonSelectedSyncer(TEnvironmentSetup& env) const {
            UNIT_ASSERT(SelectedOwnerActor);
            env.Runtime->Send(new IEventHandle(TEvents::TSystem::PoisonPill, 0, *SelectedOwnerActor, {}, nullptr, 0),
                SelectedOwnerActor->NodeId());
        }
    };

} // anonymous namespace

Y_UNIT_TEST_SUITE(VDiskStartupBrokers) {

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

    Y_UNIT_TEST(StartupCatchupBrokerSerializesStartupCatchupPerNode) {
        TRestartScenario scenario;
        TStartupCatchupCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupCatchupCount = 1}, &scenario.StartupTargets);

        // Wait until two Syncers have reached the broker, but only one token is granted.
        // The selected owner's completion event is stashed to keep that state stable.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one Syncer to hold startup catchup token while another VDisk is already waiting");
        UNIT_ASSERT_VALUES_EQUAL_C(capture.GrantedVDisks.size(), 1,
            "expected only one startup catchup token before releasing first Syncer; queriedVDisks# "
            << capture.QueriedVDisks.size());

        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] { return capture.GrantedVDisks.size() >= 2; },
            "expected another VDisk to get startup catchup token after the first one completes startup wave");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != firstVDiskActorId,
            "expected startup catchup broker to pass token to another VDisk after startup wave completion;"
            << " firstVDiskActorId# " << firstVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    Y_UNIT_TEST(StartupCatchupBrokerReleasesTokenWhenSyncerDiesPerNode) {
        TRestartScenario scenario;
        TStartupCatchupCapture capture;
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupCatchupCount = 1}, &scenario.StartupTargets);

        // Observe the throttled state first, then kill the selected Syncer and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerNode(); },
            "expected one Syncer to hold startup catchup token while another VDisk is already waiting");

        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.PoisonSelectedSyncer(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId) && capture.GrantedVDisks.size() >= 2;
        }, "expected startup catchup token to be released when the owning Syncer dies");

        UNIT_ASSERT_C(capture.GrantedVDisks[1] != failedVDiskActorId,
            "expected startup catchup broker to pass token to another VDisk after Syncer death;"
            << " failedVDiskActorId# " << failedVDiskActorId
            << " secondVDiskActorId# " << capture.GrantedVDisks[1]);
    }

    Y_UNIT_TEST(StartupCatchupBrokerSerializesStartupCatchupPerPDisk) {
        TRestartScenario scenario;
        TStartupCatchupCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupCatchupPerPDiskCount = 1},
            &scenario.PerPDiskSelection.StartupTargets);

        // For per-PDisk throttling we require two things:
        // 1. another Syncer on the same PDisk is already waiting;
        // 2. a Syncer on a different PDisk is still allowed to proceed.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one Syncer on focus PDisk to hold startup catchup token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId firstVDiskActorId = capture.SelectedVDiskActor();
        UNIT_ASSERT_VALUES_EQUAL_C(capture.CountGrantedOnPDisk(focusPDiskId), 1,
            "expected exactly one startup catchup token on focus PDisk before release");
        UNIT_ASSERT_C(capture.GrantedVDisks.size() >= 2,
            "expected another PDisk to continue startup catchup while focus PDisk is blocked; focusPDiskId# "
            << focusPDiskId);

        capture.ResumeStashedEvent(scenario.Env);
        WaitUntil(scenario.Env, [&] {
            return capture.HasAnotherGrantedOnPDisk(focusPDiskId, firstVDiskActorId);
        }, "expected another VDisk on focus PDisk to get startup catchup token after the first one completes");
    }

    Y_UNIT_TEST(StartupCatchupBrokerReleasesTokenWhenSyncerDiesPerPDisk) {
        TRestartScenario scenario;
        TStartupCatchupCapture capture(scenario.PerPDiskSelection.FocusPDiskId);
        TScopedCaptureFilter guard(scenario.Env, capture);
        RestartNodeForBrokerTest(scenario, {.MaxInProgressStartupCatchupPerPDiskCount = 1},
            &scenario.PerPDiskSelection.StartupTargets);

        // Observe the per-PDisk throttled state first, then kill the selected Syncer and verify token handoff.
        WaitUntil(scenario.Env, [&] { return capture.HasOneGrantedAndAnotherWaitingPerPDisk(); },
            "expected one Syncer on focus PDisk to hold startup catchup token while another VDisk on the same PDisk waits");

        const ui32 focusPDiskId = scenario.PerPDiskSelection.FocusPDiskId;
        const TActorId failedVDiskActorId = capture.SelectedVDiskActor();
        capture.PoisonSelectedSyncer(scenario.Env);

        WaitUntil(scenario.Env, [&] {
            return capture.HasReleasedVDisk(failedVDiskActorId)
                && capture.HasAnotherGrantedOnPDisk(focusPDiskId, failedVDiskActorId);
        }, "expected startup catchup token to be released for another VDisk on the same PDisk when Syncer dies");
    }

}
