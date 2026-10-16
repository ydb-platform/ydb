#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/base/blobstorage_database_space_events.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_util_space_color.h>

Y_UNIT_TEST_SUITE(DatabaseSpace) {

    using TColor = NKikimrBlobStorage::TPDiskSpaceColor;
    using TState = NKikimrBlobStorage::TEvControllerDatabaseSpaceState;

    constexpr ui64 DatabaseSchemeShardId = 72075186224037888ull;
    constexpr ui64 DatabasePathId = 42;
    const TPathId DatabaseScope(DatabaseSchemeShardId, DatabasePathId);

    // subscribes through the local NodeWarden and records every state it gets; notifications arrive at any moment,
    // so a regular edge actor can't be used here
    class TStateCollector : public TActorBootstrapped<TStateCollector> {
        std::shared_ptr<std::vector<TState>> States;

    public:
        TStateCollector(std::shared_ptr<std::vector<TState>> states)
            : States(std::move(states))
        {}

        void Bootstrap() {
            auto ev = std::make_unique<TEvBlobStorage::TEvControllerSubscribeDatabaseSpace>();
            DatabaseScope.ToProto(ev->Record.AddSubscribe());
            Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), ev.release());
            Become(&TThis::StateFunc);
        }

        void Handle(TEvBlobStorage::TEvControllerDatabaseSpaceState::TPtr ev) {
            auto& record = ev->Get()->Record;
            Cerr << "Got database space state# " << record.ShortDebugString() << " at# " << SelfId() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(TPathId::FromProto(record.GetScope()), DatabaseScope);
            States->push_back(std::move(record));
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvBlobStorage::TEvControllerDatabaseSpaceState, Handle);
        )
    };

    struct TSubscriber {
        std::shared_ptr<std::vector<TState>> States = std::make_shared<std::vector<TState>>();
        TActorId ActorId;
    };

    struct TTestContext {
        TEnvironmentSetup Env;

        explicit TTestContext(ui32 nodeCount = 8)
            : Env(TEnvironmentSetup::TSettings{
                .NodeCount = nodeCount,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            })
        {
            Env.CreateBoxAndPool(1, 2);
            Env.Sim(TDuration::Seconds(30));
            BindPoolToDatabase();
        }

        void BindPoolToDatabase() {
            // the same way console does it for tenant pools
            NKikimrBlobStorage::TConfigRequest request;
            auto *read = request.AddCommand()->MutableReadStoragePool();
            read->SetBoxId(1);
            read->AddName(Env.StoragePoolName);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
            UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(0).StoragePoolSize(), 1);

            NKikimrBlobStorage::TDefineStoragePool pool = response.GetStatus(0).GetStoragePool(0);
            pool.MutableScopeId()->SetX1(DatabaseSchemeShardId);
            pool.MutableScopeId()->SetX2(DatabasePathId);

            request.Clear();
            request.AddCommand()->MutableDefineStoragePool()->CopyFrom(pool);
            response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        void SetThresholds(TColor::E block, TColor::E unblock) {
            NKikimrBlobStorage::TConfigRequest request;
            auto *us = request.AddCommand()->MutableUpdateSettings();
            us->AddDatabaseSpaceBlockColor(block);
            us->AddDatabaseSpaceUnblockColor(unblock);
            auto response = Env.Invoke(request);
            UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());
        }

        TSubscriber Subscribe(ui32 nodeId) {
            TSubscriber subscriber;
            subscriber.ActorId = Env.Runtime->Register(new TStateCollector(subscriber.States), nodeId);
            return subscriber;
        }

        // wait until the subscriber gets more than `minStates` states and the last one matches
        const TState& WaitState(const TSubscriber& subscriber, bool exhausted, TDuration timeout, size_t minStates = 0) {
            const TInstant deadline = Env.Runtime->GetClock() + timeout;
            auto matches = [&] {
                return subscriber.States->size() > minStates && subscriber.States->back().GetExhausted() == exhausted;
            };
            Env.Runtime->Sim([&] { return !matches() && Env.Runtime->GetClock() < deadline; });
            UNIT_ASSERT_C(matches(), "subscriber " << subscriber.ActorId << " has not got state with Exhausted# "
                << exhausted << " within " << timeout);
            return subscriber.States->back();
        }

        // sequence numbers of injected VDisk reports, above the ones real VDisks of the test use
        ui64 LastSpaceSequence = 1'000'000'000;

        ui64 NextSpaceSequence() {
            return ++LastSpaceSequence;
        }

        static std::unique_ptr<TEvBlobStorage::TEvControllerUpdateDiskStatus> MakeVDiskReport(const TVDiskID& vdiskId,
                const NKikimrBlobStorage::TVSlotId& vslotId, TColor::E color, ui64 sequence) {
            return std::make_unique<TEvBlobStorage::TEvControllerUpdateDiskStatus>(vdiskId, vslotId.GetNodeId(),
                vslotId.GetPDiskId(), vslotId.GetVSlotId(), SpaceColorToStatusFlag(color), sequence);
        }

        static TVDiskID GetVDiskId(const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot) {
            return TVDiskID(TGroupId::FromValue(vslot.GetGroupId()), vslot.GetGroupGeneration(), vslot.GetFailRealmIdx(),
                vslot.GetFailDomainIdx(), vslot.GetVDiskIdx());
        }

        void SendToNodeWarden(std::unique_ptr<TEvBlobStorage::TEvControllerUpdateDiskStatus> ev, ui32 nodeId) {
            Env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(nodeId), TActorId(), ev.release()), nodeId);
        }

        // report VDisk's space color through its NodeWarden the way VDisk does
        void ReportColor(const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot, TColor::E color) {
            SendToNodeWarden(MakeVDiskReport(GetVDiskId(vslot), vslot.GetVSlotId(), color, NextSpaceSequence()),
                vslot.GetVSlotId().GetNodeId());
        }

        // report VDisk's space color the way periodic PDisk metrics do (with no sequence number)
        void ReportColorInPDiskMetrics(const NKikimrBlobStorage::TBaseConfig::TVSlot& vslot, TColor::E color) {
            auto ev = MakeVDiskReport(GetVDiskId(vslot), vslot.GetVSlotId(), color, 0);
            ev->Record.ClearVDiskSpaceSequence();
            SendToNodeWarden(std::move(ev), vslot.GetVSlotId().GetNodeId());
        }

        std::tuple<ui32, ui32> GetSomePDisk() {
            const auto groups = Env.GetGroups();
            UNIT_ASSERT(!groups.empty());
            const auto groupInfo = Env.GetGroupInfo(groups.front());
            ui32 nodeId, pdiskId;
            std::tie(nodeId, pdiskId, std::ignore) = DecomposeVDiskServiceId(groupInfo->GetActorId(0));
            return {nodeId, pdiskId};
        }
    };

    Y_UNIT_TEST(SubscribeBlockAndUnblock) {
        TTestContext ctx;

        // subscribe from two actors on the same node and from another node
        const std::vector<TSubscriber> subscribers{ctx.Subscribe(1), ctx.Subscribe(1), ctx.Subscribe(2)};
        for (const auto& subscriber : subscribers) {
            ctx.WaitState(subscriber, false, TDuration::Seconds(10));
        }

        // PDisk mock does not report status flags in metrics, so BS_CONTROLLER learns the color only from VDisks
        // reporting it through the NodeWarden right away; every group has a VDisk over this PDisk and a group is as
        // bad as its worst VDisk, so the whole pool becomes exhausted
        const auto [nodeId, pdiskId] = ctx.GetSomePDisk();
        ctx.Env.SetPDiskStatusFlags(nodeId, pdiskId, TColor::YELLOW);
        for (const auto& subscriber : subscribers) {
            // must be faster than the regular NodeWarden metrics reporting period (10 seconds)
            ctx.WaitState(subscriber, true, TDuration::Seconds(9));
        }

        // raising the block color unblocks the database, once every VDisk of some group has reported its color (PDisk
        // mock doesn't report colors at all, so do it the way VDisks do)
        const auto baseConfig = ctx.Env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ctx.ReportColor(vslot, TColor::YELLOW);
        }
        ctx.SetThresholds(TColor::RED, TColor::GREEN);
        for (const auto& subscriber : subscribers) {
            ctx.WaitState(subscriber, false, TDuration::Seconds(10));
        }

        // and lowering it back blocks it again
        ctx.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        for (const auto& subscriber : subscribers) {
            ctx.WaitState(subscriber, true, TDuration::Seconds(10));
        }

        // disabling the feature unblocks the database
        ctx.SetThresholds(TColor::GREEN, TColor::GREEN);
        for (const auto& subscriber : subscribers) {
            ctx.WaitState(subscriber, false, TDuration::Seconds(10));
        }
    }

    Y_UNIT_TEST(InvalidThresholds) {
        TTestContext ctx;
        NKikimrBlobStorage::TConfigRequest request;
        auto *us = request.AddCommand()->MutableUpdateSettings();
        us->AddDatabaseSpaceBlockColor(TColor::YELLOW);
        us->AddDatabaseSpaceUnblockColor(TColor::ORANGE);
        auto response = ctx.Env.Invoke(request);
        UNIT_ASSERT(!response.GetSuccess());
    }

    Y_UNIT_TEST(StaleVDiskReportDoesNotUnblock) {
        TTestContext ctx;
        ctx.SetThresholds(TColor::BLACK, TColor::GREEN); // block at BLACK only, no hysteresis
        const TSubscriber subscriber = ctx.Subscribe(1);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));

        // report the first VDisk of every group the way VDisk does; VDisk reports come from different threads, so a
        // newer BLACK report may be delivered before an older RED one; note that group generations have just been
        // bumped by binding the pool to the database, which also checks that NodeWarden reports metrics with the
        // current generation
        auto report = [&](TColor::E color, ui64 sequence) {
            for (const ui32 groupId : ctx.Env.GetGroups()) {
                const auto info = ctx.Env.GetGroupInfo(groupId);
                const auto [nodeId, pdiskId, vslotId] = DecomposeVDiskServiceId(info->GetActorId(0));
                NKikimrBlobStorage::TVSlotId vslot;
                vslot.SetNodeId(nodeId);
                vslot.SetPDiskId(pdiskId);
                vslot.SetVSlotId(vslotId);
                ctx.SendToNodeWarden(TTestContext::MakeVDiskReport(info->GetVDiskId(0), vslot, color, sequence), nodeId);
            }
        };
        const ui64 redSequence = ctx.NextSpaceSequence(); // the RED report happened first...
        const ui64 blackSequence = ctx.NextSpaceSequence();
        report(TColor::BLACK, blackSequence); // ...but the BLACK one is delivered first
        ctx.WaitState(subscriber, true, TDuration::Seconds(9));
        report(TColor::RED, redSequence);

        // the stale report must not unblock the database, even after the regular metrics reporting
        const size_t numStates = subscriber.States->size();
        ctx.Env.Sim(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(subscriber.States->size(), numStates);
        UNIT_ASSERT(subscriber.States->back().GetExhausted());
    }

    Y_UNIT_TEST(PDiskSnapshotDoesNotOverrideVDiskReport) {
        TTestContext ctx;
        ctx.SetThresholds(TColor::YELLOW, TColor::YELLOW);
        const TSubscriber subscriber = ctx.Subscribe(1);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));

        // every VDisk reports YELLOW, so the pool gets exhausted
        const auto baseConfig = ctx.Env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ctx.ReportColor(vslot, TColor::YELLOW);
        }
        ctx.WaitState(subscriber, true, TDuration::Seconds(9));

        // a PDisk metrics snapshot taken before the change is delivered afterwards; VDisks report their colors
        // themselves, so it must be ignored
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ctx.ReportColorInPDiskMetrics(vslot, TColor::GREEN);
        }
        const size_t numStates = subscriber.States->size();
        ctx.Env.Sim(TDuration::Seconds(30)); // longer than the regular metrics reporting period
        UNIT_ASSERT_VALUES_EQUAL(subscriber.States->size(), numStates);
        UNIT_ASSERT(subscriber.States->back().GetExhausted());
    }

    Y_UNIT_TEST(HysteresisSurvivesControllerRestart) {
        TTestContext ctx;
        ctx.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);

        // use a node that is not restarted along with BS_CONTROLLER
        const ui32 nodeId = ctx.Env.Settings.ControllerNodeId + 1;
        const TSubscriber subscriber = ctx.Subscribe(nodeId);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));

        std::vector<NKikimrBlobStorage::TBaseConfig::TVSlot> vslots;
        const auto baseConfig = ctx.Env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetVSlotId().GetNodeId() == nodeId) {
                vslots.push_back(vslot);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(vslots.size(), 2); // one VDisk of every group

        // the PDisk hosts a VDisk of every group, so the pool gets exhausted
        ctx.Env.SetPDiskStatusFlags(nodeId, vslots.front().GetVSlotId().GetPDiskId(), TColor::YELLOW);
        ctx.WaitState(subscriber, true, TDuration::Seconds(9));

        // every VDisk reports LIGHT_YELLOW, which is between the unblock and block colors, so the pool stays exhausted
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ctx.ReportColor(vslot, TColor::LIGHT_YELLOW);
        }
        ctx.Env.Sim(TDuration::Seconds(20)); // longer than the regular metrics reporting period
        const auto updatedBaseConfig = ctx.Env.FetchBaseConfig();
        for (const auto& vslot : updatedBaseConfig.GetVSlot()) {
            if (vslot.GetVSlotId().GetNodeId() == nodeId) {
                UNIT_ASSERT_VALUES_EQUAL(StatusFlagToSpaceColor(vslot.GetVDiskMetrics().GetStatusFlags()),
                    TColor::LIGHT_YELLOW);
            }
        }
        UNIT_ASSERT(subscriber.States->back().GetExhausted());

        // the latch must survive BS_CONTROLLER restart: the database stays blocked all the time
        const size_t numStates = subscriber.States->size();
        ctx.Env.RestartNode(ctx.Env.Settings.ControllerNodeId);
        ctx.WaitState(subscriber, true, TDuration::Seconds(60), numStates);
        ctx.Env.Sim(TDuration::Seconds(20));
        for (size_t i = numStates; i < subscriber.States->size(); ++i) {
            UNIT_ASSERT_C((*subscriber.States)[i].GetExhausted(), "database got unblocked after BS_CONTROLLER restart");
        }
    }

    Y_UNIT_TEST(HysteresisSurvivesReassignmentAndRestart) {
        TTestContext ctx(9); // a spare node for reassigned VDisks
        ctx.SetThresholds(TColor::YELLOW, TColor::LIGHT_YELLOW);
        const ui32 controllerNodeId = ctx.Env.Settings.ControllerNodeId;
        const TSubscriber subscriber = ctx.Subscribe(controllerNodeId + 1);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));

        // in every group, pick the VDisk to be the worst one and a VDisk to reassign, both off the controller node
        std::map<ui32, NKikimrBlobStorage::TBaseConfig::TVSlot> worst, moved;
        const auto baseConfig = ctx.Env.FetchBaseConfig();
        for (const auto& vslot : baseConfig.GetVSlot()) {
            if (vslot.GetVSlotId().GetNodeId() == controllerNodeId) {
                continue;
            } else if (!worst.contains(vslot.GetGroupId())) {
                worst.emplace(vslot.GetGroupId(), vslot);
            } else if (!moved.contains(vslot.GetGroupId())) {
                moved.emplace(vslot.GetGroupId(), vslot);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(worst.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(moved.size(), 2);

        // the pool gets exhausted
        for (const auto& vslot : baseConfig.GetVSlot()) {
            ctx.ReportColor(vslot, TColor::YELLOW);
        }
        ctx.WaitState(subscriber, true, TDuration::Seconds(9));

        // then improves: the worst VDisk of every group is LIGHT_YELLOW, which is within the hysteresis band, and the
        // other ones are CYAN, which is below the unblock color
        for (const auto& vslot : baseConfig.GetVSlot()) {
            const auto& w = worst.at(vslot.GetGroupId());
            const bool isWorst = w.GetVSlotId().ShortDebugString() == vslot.GetVSlotId().ShortDebugString();
            ctx.ReportColor(vslot, isWorst ? TColor::LIGHT_YELLOW : TColor::CYAN);
        }
        ctx.Env.Sim(TDuration::Seconds(20)); // longer than the regular metrics reporting period
        UNIT_ASSERT(subscriber.States->back().GetExhausted());

        // reassignment bumps group generations and drops the persisted metrics of the groups' VDisks; restart
        // BS_CONTROLLER before the metrics get persisted anew, so it starts without knowing the groups' colors
        NKikimrBlobStorage::TConfigRequest request;
        for (const auto& [groupId, vslot] : moved) {
            auto *cmd = request.AddCommand()->MutableReassignGroupDisk();
            cmd->SetGroupId(vslot.GetGroupId());
            cmd->SetGroupGeneration(vslot.GetGroupGeneration());
            cmd->SetFailRealmIdx(vslot.GetFailRealmIdx());
            cmd->SetFailDomainIdx(vslot.GetFailDomainIdx());
            cmd->SetVDiskIdx(vslot.GetVDiskIdx());
        }
        const auto response = ctx.Env.Invoke(request);
        UNIT_ASSERT_C(response.GetSuccess(), response.GetErrorDescription());

        const size_t numStates = subscriber.States->size();
        ctx.Env.RestartNode(controllerNodeId);

        // the latch must be kept while colors are unknown or partially reported (CYAN VDisks may report before the
        // worst one), and after they are refreshed within the hysteresis band
        ctx.WaitState(subscriber, true, TDuration::Seconds(60), numStates);
        ctx.Env.Sim(TDuration::Seconds(20));
        for (size_t i = numStates; i < subscriber.States->size(); ++i) {
            UNIT_ASSERT_C((*subscriber.States)[i].GetExhausted(), "database got unblocked after reassignment and restart");
        }
    }

    Y_UNIT_TEST(ResubscribeAfterControllerRestart) {
        TTestContext ctx;
        ctx.SetThresholds(TColor::YELLOW, TColor::YELLOW);

        // subscribe on a node that does not run BS_CONTROLLER
        const TSubscriber subscriber = ctx.Subscribe(ctx.Env.Settings.ControllerNodeId + 1);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));

        const auto [nodeId, pdiskId] = ctx.GetSomePDisk();
        ctx.Env.SetPDiskStatusFlags(nodeId, pdiskId, TColor::YELLOW);
        ctx.WaitState(subscriber, true, TDuration::Seconds(9));

        // BS_CONTROLLER forgets subscriptions on restart; NodeWarden subscribes again after it registers and gets
        // the state anew
        const size_t numStates = subscriber.States->size();
        ctx.Env.RestartNode(ctx.Env.Settings.ControllerNodeId);
        ctx.WaitState(subscriber, true, TDuration::Seconds(60), numStates);

        // and the restored subscription gets notified about changes
        ctx.SetThresholds(TColor::GREEN, TColor::GREEN);
        ctx.WaitState(subscriber, false, TDuration::Seconds(10));
    }

}
