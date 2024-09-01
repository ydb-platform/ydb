#include "mediator.h"
#include "mediator_impl.h"
#include <ydb/core/tx/coordinator/public/events.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTxMediator {

Y_UNIT_TEST_SUITE(MediatorTest) {

    using namespace Tests;

    enum class EWatcherCmd {
        AddTablet,
        RemoveTablet,
    };

    struct TWatcherCmd {
        ui64 SubscriptionId;
        EWatcherCmd Cmd;
        ui64 TabletId;

        explicit TWatcherCmd(ui64 subscriptionId, EWatcherCmd cmd, ui64 tabletId)
            : SubscriptionId(subscriptionId)
            , Cmd(cmd)
            , TabletId(tabletId)
        {}
    };

    struct TWatchUpdate {
        ui64 SafeStep;

        TWatchUpdate() = default;

        explicit TWatchUpdate(ui64 safeStep)
            : SafeStep(safeStep)
        {}

        explicit TWatchUpdate(const NKikimrTxMediatorTimecast::TEvUpdate& record) {
            SafeStep = record.GetTimeBarrier();
        }

        bool operator==(const TWatchUpdate&) const noexcept = default;

        inline friend IOutputStream& operator<<(IOutputStream& out, const TWatchUpdate& update) {
            return out << "TWatchUpdate{ " << update.SafeStep << " }";
        }
    };

    struct TGranularUpdate {
        ui64 LatestStep;
        THashMap<ui64, ui64> Frozen;
        THashSet<ui64> Unfrozen;

        TGranularUpdate() = default;

        explicit TGranularUpdate(ui64 latestStep, THashMap<ui64, ui64> frozen = {}, THashSet<ui64> unfrozen = {})
            : LatestStep(latestStep)
            , Frozen(std::move(frozen))
            , Unfrozen(std::move(unfrozen))
        {}

        explicit TGranularUpdate(const NKikimrTxMediatorTimecast::TEvGranularUpdate& record) {
            LatestStep = record.GetLatestStep();
            for (size_t i = 0; i < record.FrozenTabletsSize(); ++i) {
                Frozen[record.GetFrozenTablets(i)] = record.GetFrozenSteps(i);
            }
            for (ui64 tabletId : record.GetUnfrozenTablets()) {
                Unfrozen.insert(tabletId);
            }
        }

        bool operator==(const TGranularUpdate&) const noexcept = default;

        inline friend IOutputStream& operator<<(IOutputStream& out, const TGranularUpdate& update) {
            out << "TGranularUpdate{ " << update.LatestStep;
            if (!update.Frozen.empty()) {
                out << ", Frozen# { ";
                bool first = true;
                for (auto& pr : update.Frozen) {
                    if (first) {
                        first = false;
                    } else {
                        out << ", ";
                    }
                    out << pr.first << " -> " << pr.second;
                }
                out << " }";
            }
            if (!update.Unfrozen.empty()) {
                out << ", Unfrozen# { ";
                bool first = true;
                for (ui64 tabletId : update.Unfrozen) {
                    if (first) {
                        first = false;
                    } else {
                        out << ", ";
                    }
                    out << tabletId;
                }
                out << " }";
            }
            return out << " }";
        }
    };

    struct TUpdate : public std::variant<TWatchUpdate, TGranularUpdate> {
        using TBase = std::variant<TWatchUpdate, TGranularUpdate>;
        using TBase::TBase;

        inline friend bool operator==(const TUpdate& a, const TWatchUpdate& b) {
            if (auto* p = std::get_if<TWatchUpdate>(&a)) {
                return *p == b;
            }
            return false;
        }

        inline friend bool operator==(const TUpdate& a, const TGranularUpdate& b) {
            if (auto* p = std::get_if<TGranularUpdate>(&a)) {
                return *p == b;
            }
            return false;
        }

        inline friend IOutputStream& operator<<(IOutputStream& out, const TUpdate& value) {
            auto visitor = [&out](const auto& v) -> IOutputStream& {
                return out << v;
            };
            return std::visit(visitor, value);
        }
    };

    struct TWatcherState : public TThrRefBase {
        using TPtr = TIntrusivePtr<TWatcherState>;

        ui64 SafeStep = 0;
        ui64 LatestStep = 0;
        THashMap<ui64, ui64> Frozen; // TabletId -> Step

        std::deque<TWatcherCmd> Pending; // Pending commands
        THashSet<ui64> Tablets; // Watched TabletIds
        bool Connected = false;
        bool Synchronized = false;

        std::deque<TUpdate> Updates;
    };

    inline IOutputStream& operator<<(IOutputStream& out, const std::deque<TUpdate>& updates) {
        out << "[";
        bool first = true;
        for (const TUpdate& update : updates) {
            if (first) {
                first = false;
                out << " ";
            } else {
                out << ", ";
            }
            out << update;
        }
        out << " ]";
        return out;
    }

    template<class... TArgs>
    std::deque<TUpdate> MakeUpdates(const TArgs&... args) {
        std::deque<TUpdate> updates;
        (updates.push_back(args), ...);
        return updates;
    }

    class TWatcherActor : public TActorBootstrapped<TWatcherActor> {
    public:
        enum EEv {
            EvReconnect = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvAddTablet,
            EvRemoveTablet,
        };

        struct TEvReconnect : public TEventLocal<TEvReconnect, EvReconnect> {
            TEvReconnect() = default;
        };

        struct TEvAddTablet : public TEventLocal<TEvAddTablet, EvAddTablet> {
            const ui64 TabletId;

            explicit TEvAddTablet(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

        struct TEvRemoveTablet : public TEventLocal<TEvRemoveTablet, EvRemoveTablet> {
            const ui64 TabletId;

            explicit TEvRemoveTablet(ui64 tabletId)
                : TabletId(tabletId)
            {}
        };

    public:
        TWatcherActor(TWatcherState::TPtr state, ui64 mediatorId, ui32 bucket)
            : State(std::move(state))
            , MediatorId(mediatorId)
            , Bucket(bucket)
        {}

        void Bootstrap() {
            Become(&TThis::StateWork);
            Reconnect();
        }

    private:
        void Reconnect() {
            if (PipeClient) {
                NTabletPipe::CloseClient(SelfId(), PipeClient);
            }
            PipeClient = RegisterWithSameMailbox(NTabletPipe::CreateClient(SelfId(), MediatorId));

            SubscriptionId = NextSubscriptionId++;
            auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatch>(Bucket, SubscriptionId);

            State->Frozen.clear();

            for (auto& cmd : State->Pending) {
                switch (cmd.Cmd) {
                    case EWatcherCmd::AddTablet:
                        State->Tablets.insert(cmd.TabletId);
                        break;
                    case EWatcherCmd::RemoveTablet:
                        State->Tablets.erase(cmd.TabletId);
                        break;
                }
            }
            State->Pending.clear();

            for (ui64 tabletId : State->Tablets) {
                req->Record.AddTablets(tabletId);
            }

            State->Connected = false;
            State->Synchronized = false;

            NTabletPipe::SendData(SelfId(), PipeClient, req.release(), SubscriptionId);
            NTabletPipe::SendData(SelfId(), PipeClient, new TEvMediatorTimecast::TEvWatch(Bucket));
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvReconnect, Handle);
                hFunc(TEvAddTablet, Handle);
                hFunc(TEvRemoveTablet, Handle);
                hFunc(TEvTabletPipe::TEvClientConnected, Handle);
                hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
                hFunc(TEvMediatorTimecast::TEvUpdate, Handle);
                hFunc(TEvMediatorTimecast::TEvGranularUpdate, Handle);
            }
        }

        void Handle(TEvReconnect::TPtr&) {
            Reconnect();
        }

        void Handle(TEvAddTablet::TPtr& ev) {
            auto* msg = ev->Get();
            ui64 id = NextSubscriptionId++;
            State->Pending.emplace_back(id, EWatcherCmd::AddTablet, msg->TabletId);
            auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatchModify>(Bucket, id);
            req->Record.AddAddTablets(msg->TabletId);
            NTabletPipe::SendData(SelfId(), PipeClient, req.release(), id);
        }

        void Handle(TEvRemoveTablet::TPtr& ev) {
            auto* msg = ev->Get();
            ui64 id = NextSubscriptionId++;
            State->Pending.emplace_back(id, EWatcherCmd::RemoveTablet, msg->TabletId);
            auto req = std::make_unique<TEvMediatorTimecast::TEvGranularWatchModify>(Bucket, id);
            req->Record.AddRemoveTablets(msg->TabletId);
            NTabletPipe::SendData(SelfId(), PipeClient, req.release(), id);
        }

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
            if (ev->Sender != PipeClient) {
                return;
            }
            auto* msg = ev->Get();
            if (msg->Status != NKikimrProto::OK) {
                PipeClient = {};
                Reconnect();
                return;
            }
            State->Connected = true;
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
            if (ev->Sender != PipeClient) {
                return;
            }
            PipeClient = {};
            Reconnect();
        }

        void Handle(TEvMediatorTimecast::TEvUpdate::TPtr& ev) {
            auto* msg = ev->Get();
            auto& update = std::get<TWatchUpdate>(
                State->Updates.emplace_back(std::in_place_type<TWatchUpdate>, msg->Record));
            State->SafeStep = update.SafeStep;
        }

        void Handle(TEvMediatorTimecast::TEvGranularUpdate::TPtr& ev) {
            auto* msg = ev->Get();
            if (msg->Record.GetSubscriptionId() < SubscriptionId) {
                return; // ignore outdated updates
            }

            auto& update = std::get<TGranularUpdate>(
                State->Updates.emplace_back(std::in_place_type<TGranularUpdate>, msg->Record));

            State->LatestStep = update.LatestStep;
            State->Synchronized = true;

            while (!State->Pending.empty()) {
                auto& cmd = State->Pending.front();
                if (msg->Record.GetSubscriptionId() < cmd.SubscriptionId) {
                    break;
                }
                SubscriptionId = cmd.SubscriptionId;
                switch (cmd.Cmd) {
                    case EWatcherCmd::AddTablet:
                        State->Tablets.insert(cmd.TabletId);
                        break;
                    case EWatcherCmd::RemoveTablet:
                        State->Frozen.erase(cmd.TabletId);
                        State->Tablets.erase(cmd.TabletId);
                        break;
                }
                State->Pending.pop_front();
            }

            for (auto& pr : update.Frozen) {
                State->Frozen[pr.first] = pr.second;
            }
            for (ui64 tabletId : update.Unfrozen) {
                State->Frozen.erase(tabletId);
            }
        }

    private:
        const TWatcherState::TPtr State;
        const ui64 MediatorId;
        const ui32 Bucket;
        TActorId PipeClient;
        ui64 SubscriptionId = 0;
        ui64 NextSubscriptionId = 1;
    };

    class TTargetTablet
        : public TActor<TTargetTablet>
        , public NTabletFlatExecutor::TTabletExecutedFlat
    {
    public:
        TTargetTablet(const TActorId& tablet, TTabletStorageInfo* info)
            : TActor(&TThis::StateInit)
            , TTabletExecutedFlat(info, tablet, nullptr)
        {}

    private:
        STFUNC(StateInit) {
            StateInitImpl(ev, SelfId());
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvTxProcessing::TEvPlanStep, Handle);
            default:
                HandleDefaultEvents(ev, SelfId());
            }
        }

        void Handle(TEvTxProcessing::TEvPlanStep::TPtr& ev) {
            ui64 step = ev->Get()->Record.GetStep();
            THashMap<TActorId, TVector<ui64>> acks;
            for (const auto& tx : ev->Get()->Record.GetTransactions()) {
                ui64 txId = tx.GetTxId();
                TActorId owner = ActorIdFromProto(tx.GetAckTo());
                acks[owner].push_back(txId);
            }
            for (auto& pr : acks) {
                auto owner = pr.first;
                Send(owner, new TEvTxProcessing::TEvPlanStepAck(TabletID(), step, pr.second.begin(), pr.second.end()));
            }
            Send(ev->Sender, new TEvTxProcessing::TEvPlanStepAccepted(TabletID(), step));
        }

    private:
        void DefaultSignalTabletActive(const TActorContext&) override {
            // must be empty
        }

        void OnActivateExecutor(const TActorContext& ctx) override {
            Become(&TThis::StateWork);
            SignalTabletActive(ctx);
        }

        void OnDetach(const TActorContext&) override {
            PassAway();
        }

        void OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) override {
            PassAway();
        }
    };

    class TMediatorTestWithWatcher : public NUnitTest::TBaseFixture {
    public:
        void SetUp(NUnitTest::TTestContext&) override {
            auto& pm = PM.emplace();

            TServerSettings serverSettings(pm.GetPort(2134));
            serverSettings.SetDomainName("Root")
                .SetUseRealThreads(false);

            Server = new TServer(serverSettings);
            auto& runtime = GetRuntime();
            Sender = runtime.AllocateEdgeActor();

            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_EXEC_QUEUE, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NLog::PRI_TRACE);
            runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_PRIVATE, NLog::PRI_TRACE);

            CoordinatorId = ChangeStateStorage(TTestTxConfig::Coordinator, Server->GetSettings().Domain);
            MediatorId = ChangeStateStorage(TTestTxConfig::TxTablet0, Server->GetSettings().Domain);

            MediatorBootstrapper = CreateTestBootstrapper(runtime,
                CreateTestTabletInfo(MediatorId, TTabletTypes::Mediator),
                &CreateTxMediator);

            {
                TDispatchOptions options;
                options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
                runtime.DispatchEvents(options);
            }

            {
                auto msg = std::make_unique<TEvSubDomain::TEvConfigure>();
                msg->Record.SetVersion(1);
                msg->Record.SetPlanResolution(500);
                msg->Record.AddCoordinators(CoordinatorId);
                msg->Record.AddMediators(MediatorId);
                msg->Record.SetTimeCastBucketsPerMediator(1);
                runtime.SendToPipe(MediatorId, Sender, msg.release());

                auto ev = runtime.GrabEdgeEvent<TEvSubDomain::TEvConfigureStatus>(Sender);
                UNIT_ASSERT(ev->Get()->Record.GetStatus() == NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS);
            }

            WatcherState = new TWatcherState();
            Watcher = runtime.Register(new TWatcherActor(WatcherState, MediatorId, 0));
            runtime.WaitFor("watcher to connect", [this]{ return WatcherState->Connected; });
        }

    protected:
        TTestActorRuntime& GetRuntime() {
            return *Server->GetRuntime();
        }

        TActorId QueuePipeClient(const TActorId& queue) {
            auto it = PerQueuePipes.find(queue);
            if (it != PerQueuePipes.end()) {
                return it->second;
            }

            auto& runtime = GetRuntime();
            TActorId client = runtime.ConnectToPipe(MediatorId, queue, 0, {});
            PerQueuePipes[queue] = client;
            return client;
        }

        NKikimrTx::TEvCoordinatorSyncResult Sync(const TActorId& queue, ui64 genCookie) {
            auto& runtime = GetRuntime();

            TActorId client = QueuePipeClient(queue);
            runtime.SendToPipe(client, queue, new TEvTxCoordinator::TEvCoordinatorSync(genCookie, MediatorId, CoordinatorId));
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxCoordinator::TEvCoordinatorSyncResult>(queue);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetCookie(), genCookie);
            UNIT_ASSERT_VALUES_EQUAL(msg->Record.GetStatus(), NKikimrProto::OK);
            return std::move(msg->Record);
        }

        void SendStep(const TActorId& queue, ui32 gen, ui64 step, THashMap<ui64, std::vector<ui64>> txs = {}) {
            auto& runtime = GetRuntime();

            TActorId client = QueuePipeClient(queue);
            // Note: prevStep is not actually used by mediator
            auto msg = std::make_unique<TEvTxCoordinator::TEvCoordinatorStep>(
                step, /* prevStep */ 0, MediatorId, CoordinatorId, gen);
            size_t totalAffected = 0;
            for (auto& pr : txs) {
                auto* protoTx = msg->Record.AddTransactions();
                protoTx->SetTxId(pr.first);
                for (ui64 tabletId : pr.second) {
                    protoTx->AddAffectedSet(tabletId);
                    ++totalAffected;
                }
            }
            msg->Record.SetTotalTxAffectedEntries(totalAffected);
            runtime.SendToPipe(client, queue, msg.release());
        }

        ui64 AddTargetTablet() {
            auto& runtime = GetRuntime();

            size_t index = TargetTabletIds.size();
            ui64 tabletId = ChangeStateStorage(TTestTxConfig::TxTablet1 + index, Server->GetSettings().Domain);

            TargetTabletIds.push_back(tabletId);
            TargetBootstrappers.push_back(
                CreateTestBootstrapper(runtime,
                    CreateTestTabletInfo(tabletId, TTabletTypes::Dummy),
                    [](const TActorId& tablet, TTabletStorageInfo* info) {
                        return new TTargetTablet(tablet, info);
                    }));

            {
                TDispatchOptions options;
                options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
                runtime.DispatchEvents(options);
            }

            return tabletId;
        }

        void StopTargetTablet(ui64 tabletId) {
            auto it = std::find(TargetTabletIds.begin(), TargetTabletIds.end(), tabletId);
            if (it != TargetTabletIds.end()) {
                size_t index = it - TargetTabletIds.begin();
                auto bootstrapper = TargetBootstrappers[index];
                GetRuntime().Send(bootstrapper, Sender, new TEvents::TEvPoison);
                TargetBootstrappers[index] = {};
            }
        }

        void ReconnectWatcher() {
            // Note: we use sync event processing here
            GetRuntime().Send(Watcher, Sender, new TWatcherActor::TEvReconnect());
        }

        void AddWatchTablet(ui64 tabletId) {
            // Note: we use sync event processing here
            size_t prevSize = WatcherState->Pending.size();
            GetRuntime().Send(Watcher, Sender, new TWatcherActor::TEvAddTablet(tabletId));
            UNIT_ASSERT_VALUES_EQUAL(WatcherState->Pending.size(), prevSize + 1);
        }

        void RemoveWatchTablet(ui64 tabletId) {
            // Note: we use sync event processing here
            size_t prevSize = WatcherState->Pending.size();
            GetRuntime().Send(Watcher, Sender, new TWatcherActor::TEvRemoveTablet(tabletId));
            UNIT_ASSERT_VALUES_EQUAL(WatcherState->Pending.size(), prevSize + 1);
        }

        void WaitNoPending() {
            GetRuntime().WaitFor("no pending commands", [this]{ return this->WatcherState->Pending.size() == 0; });
        }

    protected:
        std::optional<TPortManager> PM;
        TServer::TPtr Server;
        TActorId Sender;

        ui64 CoordinatorId;

        ui64 MediatorId;
        TActorId MediatorBootstrapper;

        std::vector<ui64> TargetTabletIds;
        std::vector<TActorId> TargetBootstrappers;

        TWatcherState::TPtr WatcherState;
        TActorId Watcher;

        THashMap<TActorId, TActorId> PerQueuePipes; // Queue -> PipeClient
    };

    Y_UNIT_TEST_F(BasicTimecastUpdates, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Connected, true);

        // Mediator must send its current steps to the subscriber
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(0),
            TWatchUpdate(0)));
        WatcherState->Updates.clear();

        auto queue = runtime.AllocateEdgeActor();

        {
            auto sync = Sync(queue, 1);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetCompleteStep(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetLatestKnown(), 0u);
        }

        // Send an empty step, we expect both old and granular updates
        SendStep(queue, /* gen */ 1, /* step */ 1000);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1000),
            TWatchUpdate(1000)));
        WatcherState->Updates.clear();

        ui64 tabletId = AddTargetTablet();

        // Block plan steps and send a step that involves the target tablet
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlanSteps(runtime);
        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tabletId}},
        });
        runtime.WaitFor("blocked plan step", [&]{ return blockedPlanSteps.size() > 0; });
        UNIT_ASSERT_VALUES_EQUAL(blockedPlanSteps.size(), 1u);

        // Check that latest step is updated
        runtime.WaitFor("granular update", [&]{ return WatcherState->Updates.size() > 0; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010)));
        WatcherState->Updates.clear();

        // Subscribe to the target tablet, it must become frozen at a previous step
        AddWatchTablet(tabletId);
        WaitNoPending();
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tabletId, 1009u}}, {})));
        WatcherState->Updates.clear();

        // Unblock the plan step and wait for both watch and granular update
        blockedPlanSteps.Unblock();
        runtime.WaitFor("watch updates", [&]{ return WatcherState->Updates.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {}, {tabletId}),
            TWatchUpdate(1010)));
        WatcherState->Updates.clear();
    }

    Y_UNIT_TEST_F(MultipleTablets, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        ui64 tablet3 = AddTargetTablet();

        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        AddWatchTablet(tablet3);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();

        {
            auto sync = Sync(queue, 1);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetCompleteStep(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetLatestKnown(), 0u);
        }

        SendStep(queue, /* gen */ 1, /* step */ 1000, {
            {1, {tablet1}},
        });
        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {2, {tablet2}},
        });
        SendStep(queue, /* gen */ 1, /* step */ 1020, {
            {3, {tablet3}},
        });

        runtime.WaitFor("step 1020", [&]{ return WatcherState->SafeStep >= 1020 && WatcherState->Frozen.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->SafeStep, 1020u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->LatestStep, 1020u);

        // Block plan steps at tablet 1
        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan1(runtime,
            [&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                return ev->Get()->Record.GetTabletID() == tablet1;
            });

        WatcherState->Updates.clear();

        SendStep(queue, /* gen */ 1, /* step */ 1030, {
            {4, {tablet1, tablet2, tablet3}},
        });

        runtime.WaitFor("granular update", [&]{ return WatcherState->Updates.size() >= 1; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1030, {{tablet1, 1029}, {tablet2, 1029}, {tablet3, 1029}}, {})));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Frozen.size(), 3u);

        runtime.WaitFor("unfrozen 2 tablets", [&]{ return WatcherState->Frozen.size() <= 1; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Frozen.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.size(), 2u);
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->LatestStep, 1030u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->SafeStep, 1020u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());

        blockedPlan1.Unblock();

        runtime.WaitFor("granular and watch update", [&]{ return WatcherState->Updates.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1030, {}, {tablet1}),
            TWatchUpdate(1030)));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Frozen.size(), 0u);
    }

    Y_UNIT_TEST_F(TabletAckBeforePlanComplete, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();

        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();

        {
            auto sync = Sync(queue, 1);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetCompleteStep(), 0u);
            UNIT_ASSERT_VALUES_EQUAL(sync.GetLatestKnown(), 0u);
        }

        SendStep(queue, /* gen */ 1, /* step */ 1000);

        runtime.WaitFor("step 1000", [&]{ return WatcherState->SafeStep >= 1000; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->SafeStep, 1000u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->LatestStep, 1000u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Frozen.size(), 0u);
        WatcherState->Updates.clear();

        TBlockEvents<TEvTxMediator::TEvCommitTabletStep> blockedTabletSteps(runtime);
        TBlockEvents<TEvTxMediator::TEvStepPlanComplete> blockedPlanComplete(runtime);

        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
        });

        runtime.WaitFor("blocked plan", [&]{ return blockedPlanComplete.size() >= 1; });
        UNIT_ASSERT_VALUES_EQUAL(blockedTabletSteps.size(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(blockedPlanComplete.size(), 1u);

        blockedTabletSteps.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());

        blockedTabletSteps.Unblock(1);
        blockedPlanComplete.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet2, 1009}}, {}),
            TGranularUpdate(1010, {}, {tablet2}),
            TWatchUpdate(1010)));
    }

    Y_UNIT_TEST_F(TabletAckWhenDead, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();

        AddWatchTablet(tablet1);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();
        Sync(queue, 1);

        SendStep(queue, /* gen */ 1, /* step */ 1000);

        runtime.WaitFor("step 1000", [&]{ return WatcherState->SafeStep >= 1000; });
        WatcherState->Updates.clear();

        StopTargetTablet(tablet1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1}},
        });

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}}, {})));
        WatcherState->Updates.clear();

        // The target is currently stopped, so after ~10 minutes there will be
        // a hive request that will tell the pipe that this tablet doesn't
        // exist. Even though we technically didn't create this tablet, it will
        // be marked as dead and auto-ack transaction.
        runtime.WaitFor("step 1010", [&]{ return WatcherState->SafeStep >= 1010; });

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {}, {tablet1}),
            TWatchUpdate(1010)));
    }

    Y_UNIT_TEST_F(PlanStepAckToReconnectedMediator, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();

        TBlockEvents<TEvTxProcessing::TEvPlanStepAccepted> blockedAccept(runtime);

        auto queue1 = runtime.AllocateEdgeActor();
        Sync(queue1, 1);
        SendStep(queue1, /* gen */ 1, /* step */ 1000, {
            {1, {tablet1}},
        });

        // Our queue must receive an ack
        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAck>(queue1);
            UNIT_ASSERT(ev);
        }

        // Mediator's ack must be blocked
        runtime.WaitFor("blocked accept", [&]{ return blockedAccept.size() >= 1; });

        auto queue2 = runtime.AllocateEdgeActor();
        Sync(queue2, 1);
        SendStep(queue2, /* gen */ 2, /* step */ 1000, {
            {1, {tablet1}},
        });

        // Give it a chance to settle down
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        // Unblock the mediator ack
        blockedAccept.Unblock();

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAck>(queue2);
            UNIT_ASSERT(ev);
        }

        auto queue3 = runtime.AllocateEdgeActor();
        Sync(queue3, 1);
        SendStep(queue3, /* gen */ 3, /* step */ 1000, {
            {1, {tablet1}},
        });

        {
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAck>(queue3);
            UNIT_ASSERT(ev);
        }
    }

    Y_UNIT_TEST_F(WatcherReconnect, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();
        Sync(queue, 1);
        SendStep(queue, /* gen */ 1, /* step */ 1000);
        runtime.WaitFor("step 1000", [&]{ return WatcherState->SafeStep >= 1000; });
        WatcherState->Updates.clear();

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime);

        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
        });

        // We expect tablet2 to not be reported since it's not watched yet
        runtime.WaitFor("update", [&]{ return WatcherState->Updates.size() >= 1; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}}, {})));
        WatcherState->Updates.clear();

        ReconnectWatcher();

        runtime.WaitFor("updates", [&]{ return WatcherState->Updates.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}}, {}),
            TWatchUpdate(1000)));
        WatcherState->Updates.clear();

        WatcherState->Tablets.insert(tablet2);
        ReconnectWatcher();

        runtime.WaitFor("updates", [&]{ return WatcherState->Updates.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}, {tablet2, 1009}}, {}),
            TWatchUpdate(1000)));
        WatcherState->Updates.clear();

        WatcherState->Tablets.erase(tablet1);
        ReconnectWatcher();

        runtime.WaitFor("updates", [&]{ return WatcherState->Updates.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet2, 1009}}, {}),
            TWatchUpdate(1000)));
        WatcherState->Updates.clear();

        runtime.WaitFor("both plans", [&]{ return blockedPlan.size() >= 2; });
        std::sort(blockedPlan.begin(), blockedPlan.end(), [](auto& a, auto& b) {
            return a->Get()->Record.GetTabletID() < b->Get()->Record.GetTabletID();
        });

        blockedPlan.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());

        blockedPlan.Unblock(2);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {}, {tablet2}),
             TWatchUpdate(1010)));
    }

    Y_UNIT_TEST_F(MultipleSteps, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();
        Sync(queue, 1);
        SendStep(queue, /* gen */ 1, /* step */ 1000);
        runtime.WaitFor("step 1000", [&]{ return WatcherState->SafeStep >= 1000; });
        WatcherState->Updates.clear();

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan1(runtime,
            [&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                return ev->Get()->Record.GetTabletID() == tablet1;
            });

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan2(runtime,
            [&](TEvTxProcessing::TEvPlanStep::TPtr& ev) {
                return ev->Get()->Record.GetTabletID() == tablet2;
            });

        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
        });
        SendStep(queue, /* gen */ 1, /* step */ 1020, {
            {2, {tablet1}},
        });
        SendStep(queue, /* gen */ 1, /* step */ 1030, {
            {3, {tablet2}},
        });
        SendStep(queue, /* gen */ 1, /* step */ 1040, {
            {4, {tablet1, tablet2}},
        });

        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan1.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan2.size(), 3u);

        blockedPlan1.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        blockedPlan2.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        blockedPlan1.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        blockedPlan2.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        blockedPlan1.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        blockedPlan2.Unblock(1);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}, {tablet2, 1009}}, {}),
            TGranularUpdate(1020),
            TGranularUpdate(1030),
            TGranularUpdate(1040),
            TGranularUpdate(1040, {{tablet1, 1019}}, {}),
            TGranularUpdate(1040, {{tablet2, 1029}}, {}),
            TWatchUpdate(1010),
            TGranularUpdate(1040, {{tablet1, 1039}}, {}),
            TWatchUpdate(1020),
            TGranularUpdate(1040, {{tablet2, 1039}}, {}),
            TWatchUpdate(1030),
            TGranularUpdate(1040, {}, {tablet1}),
            TGranularUpdate(1040, {}, {tablet2}),
            TWatchUpdate(1040)));
    }

    Y_UNIT_TEST_F(WatchesBeforeFirstStep, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();

        auto queue = runtime.AllocateEdgeActor();
        Sync(queue, 1);

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(0),
            TWatchUpdate(0),
            TGranularUpdate(0),
            TGranularUpdate(0)));
        WatcherState->Updates.clear();

        TBlockEvents<TEvTxMediator::TEvStepPlanComplete> blockedPlanComplete(runtime);

        SendStep(queue, /* gen */ 1, /* step */ 1000, {
            {1, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());
        UNIT_ASSERT_VALUES_EQUAL(blockedPlanComplete.size(), 1u);

        blockedPlanComplete.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1000, {}, {}),
            TWatchUpdate(1000)));
    }

    Y_UNIT_TEST_F(RebootTargetTablets, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();
        WatcherState->Updates.clear();

        auto queue = runtime.AllocateEdgeActor();
        Sync(queue, 1);

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime);

        SendStep(queue, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}, {tablet2, 1009}})));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 2u);

        blockedPlan.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.back(), TWatchUpdate(1010));
        WatcherState->Updates.clear();

        SendStep(queue, /* gen */ 1, /* step */ 1020, {
            {2, {tablet1}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1020, {{tablet1, 1019}})));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 1u);
        blockedPlan.clear();

        RebootTablet(runtime, tablet1, Sender);
        RebootTablet(runtime, tablet2, Sender);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 1u);

        blockedPlan.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1020, {}, {tablet1}),
            TWatchUpdate(1020)));
    }

    Y_UNIT_TEST_F(ResendSubset, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();
        WatcherState->Updates.clear();

        auto queue1 = runtime.AllocateEdgeActor();
        Sync(queue1, 1);

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime);

        SendStep(queue1, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
            {2, {tablet1}},
            {3, {tablet2}},
            {4, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}, {tablet2, 1009}})));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 2u);
        blockedPlan.clear();

        auto queue2 = runtime.AllocateEdgeActor();
        Sync(queue2, 1);

        // Resend the same plan without tx 2 and 3 (e.g. volatile/lost)
        SendStep(queue2, /* gen */ 2, /* step */ 1010, {
            {1, {tablet1, tablet2}},
            {4, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 0u);

        RebootTablet(runtime, tablet1, Sender);
        RebootTablet(runtime, tablet2, Sender);
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 2u);

        blockedPlan.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.back(), TWatchUpdate(1010));

        for (int i = 0; i < 2; ++i) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAck>(queue2);
            UNIT_ASSERT(ev);
            Cerr << "... ack from " << ev->Get()->Record.GetTabletId() << Endl;
        }
    }

    Y_UNIT_TEST_F(ResendNotSubset, TMediatorTestWithWatcher) {
        auto& runtime = GetRuntime();

        ui64 tablet1 = AddTargetTablet();
        ui64 tablet2 = AddTargetTablet();
        AddWatchTablet(tablet1);
        AddWatchTablet(tablet2);
        WaitNoPending();
        WatcherState->Updates.clear();

        auto queue1 = runtime.AllocateEdgeActor();
        Sync(queue1, 1);

        TBlockEvents<TEvTxProcessing::TEvPlanStep> blockedPlan(runtime);

        SendStep(queue1, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
            {4, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates(
            TGranularUpdate(1010, {{tablet1, 1009}, {tablet2, 1009}})));
        WatcherState->Updates.clear();
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 2u);

        // This should never happen in practice, but mediator shouldn't crash
        SendStep(queue1, /* gen */ 1, /* step */ 1010, {
            {1, {tablet1, tablet2}},
            {2, {tablet1}},
            {3, {tablet2}},
            {4, {tablet1, tablet2}},
        });
        runtime.SimulateSleep(TDuration::MilliSeconds(1));

        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates, MakeUpdates());
        UNIT_ASSERT_VALUES_EQUAL(blockedPlan.size(), 2u);

        blockedPlan.Unblock();
        runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.size(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(WatcherState->Updates.back(), TWatchUpdate(1010));

        // We should receive acks from tablets, plus additional acks from mediator
        for (int i = 0; i < 4; ++i) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvTxProcessing::TEvPlanStepAck>(queue1);
            UNIT_ASSERT(ev);
            Cerr << "... ack from " << ev->Get()->Record.GetTabletId() << Endl;
        }
    }

}

} // namespace NKikimr::NTxMediator
