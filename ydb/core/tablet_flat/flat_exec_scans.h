#pragma once

#include "flat_scan_eggs.h"
#include "flat_scan_actor.h"
#include "flat_exec_broker.h"
#include "util_fmt_line.h"
#include "util_fmt_abort.h"
#include "flat_util_misc.h"
#include <atomic>
#include <util/system/spinlock.h>
#include <util/system/guard.h>
#include "tablet_flat_executor.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TScanOutcome {
        bool System;
        bool Cancelled;
    };

    class TScans { /* NOps state tracker */

        using IScan = NTable::IScan;
        using EStatus = NTable::EStatus;
        using IOps = NActors::IActorOps;
        using ITablet = NFlatExecutorSetup::ITablet;
        using TEvResourceBroker = NResourceBroker::TEvResourceBroker;

        enum class EState {
            None    = 0,
            Task    = 1,    /* Waiting for resource     */
            Ready   = 2,    /* Waiting for Start(..)    */
            Scan    = 3,    /* Scan is in progress      */
            Forget  = 4,    /* Explicit cancellation     */
            Gone    = 5,    /* Internal termination     */
        };

        // Repeated volatile waits must eventually yield a retry. Broker queue
        // time never consumes this budget.
        static constexpr ui32 MaxReadinessChecks = 8;

        enum class EType {
            Client,
            System,
        };

        struct TCookie : public TResource {
            explicit TCookie(ui64 serial)
                : TResource(ESource::Scan)
                , Serial(serial)
            {

            }

            const ui64 Serial;
            bool ManagedLease = false;
        };

        // The scan owns the full working-set reservation. Once it stops, only
        // bytes retained by asynchronous payload owners remain charged, with no
        // CPU slot. The broker cookie never owns this state.
        struct TTaskLease {
            struct TState {
                TActorSystem* const System;
                const TActorId Client;
                const ui64 TaskId;
                const TString Type;
                const ui32 Priority;
                const ui64 Memory;
                TSpinLock Lock;
                ui64 Retained = 0;
                ui64 ChargedMemory;
                ui64 ChargedCpu = 1;
                bool Running = true;
                bool SubmitRejected = false;
                bool WasAcquired = false;
                bool OverrunReported = false;
                std::atomic<bool> Cancel{true};

                TState(TActorSystem* system, TActorId client, ui64 taskId, TString type, ui32 priority, ui64 memory)
                    : System(system), Client(client), TaskId(taskId), Type(std::move(type)), Priority(priority), Memory(memory)
                    , ChargedMemory(memory)
                {}

                ~TState() {
                    if (WasAcquired || !SubmitRejected) {
                        System->Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), Client,
                            new TEvResourceBroker::TEvFinishTask(TaskId, Cancel.load(std::memory_order_acquire))));
                    }
                }

                void UpdateRetained() {
                    const ui64 memory = Running ? Max(Memory, Retained) : Retained;
                    const ui64 cpu = Running ? 1 : 0;
                    if ((WasAcquired || !SubmitRejected) && memory && (memory != ChargedMemory || cpu != ChargedCpu)) {
                        System->Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), Client,
                            new TEvResourceBroker::TEvUpdateTask(TaskId, {{cpu, memory}}, Type, Priority)));
                        ChargedMemory = memory;
                        ChargedCpu = cpu;
                    }
                }
            };

            struct TPayload {
                const std::shared_ptr<TState> State;
                const ui64 Bytes;
                bool Armed = false;

                TPayload(std::shared_ptr<TState> state, ui64 bytes)
                    : State(std::move(state)), Bytes(bytes)
                {}

                ~TPayload() {
                    if (Armed) {
                        with_lock(State->Lock) {
                            Y_ABORT_UNLESS(State->Retained >= Bytes);
                            State->Retained -= Bytes;
                            State->UpdateRetained();
                        }
                    }
                }
            };

            const std::shared_ptr<TState> State;

            TTaskLease(TActorSystem* system, TActorId client, ui64 taskId, TString type, ui32 priority, ui64 memory)
                : State(std::make_shared<TState>(system, client, taskId, std::move(type), priority, memory))
            {}

            ~TTaskLease() {
                with_lock(State->Lock) {
                    State->Running = false;
                    State->UpdateRetained();
                }
            }

            void Acquired() {
                with_lock(State->Lock) {
                    State->WasAcquired = true;
                    State->ChargedMemory = State->Memory;
                    State->ChargedCpu = 1;
                }
            }

            void Rejected() {
                with_lock(State->Lock) {
                    // A rejected submit created no task. A rejected readmission
                    // still owns the task from its earlier grant.
                    State->SubmitRejected = true;
                }
            }

            void Suspend() {
                with_lock(State->Lock) {
                    State->System->Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), State->Client,
                        new TEvResourceBroker::TEvUpdateTask(State->TaskId, {{0, 0}}, State->Type, State->Priority)));
                    State->ChargedMemory = 0;
                    State->ChargedCpu = 0;
                }
            }

            void Readmit() {
                State->System->Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), State->Client,
                    new TEvResourceBroker::TEvUpdateTask(State->TaskId, {{1, State->Memory}}, State->Type,
                        State->Priority, true)));
            }

            static std::shared_ptr<void> Retain(const std::shared_ptr<TState>& state, ui64 bytes) {
                if (!bytes) {
                    return {};
                }
                auto payload = std::make_shared<TPayload>(state, bytes);
                with_lock(state->Lock) {
                    // The payload already exists, including when retained from
                    // Finish(). Charge estimate overruns instead of throwing in
                    // the scan's teardown path or hiding their memory usage.
                    state->Retained += bytes;
                    payload->Armed = true;
                    if (!state->OverrunReported && state->Retained > state->Memory) {
                        state->OverrunReported = true;
                        YDB_LOG_WARN_CTX_COMP(*state->System, NKikimrServices::TABLET_OPS_HOST,
                            "Retained scan payload exceeds admitted memory reservation",
                            {"scanType", state->Type}, {"client", state->Client}, {"taskId", state->TaskId},
                            {"admittedBytes", state->Memory}, {"retainedBytes", state->Retained},
                            {"overrunBytes", state->Retained - state->Memory});
                    }
                    state->UpdateRetained();
                }
                return payload;
            }
        };

        struct TOne : public TIntrusiveListItem<TOne> {

            TOne(ui64 serial, ui32 table, const TScanOptions& options, THolder<TScanSnapshot> snapshot)
                : Serial(serial)
                , Table(table)
                , Options(options)
                , Snapshot(std::move(snapshot))
            {

            }

            // disable copy/move operations
            TOne(const TOne&) = delete;
            TOne(TOne&&) = delete;
            TOne& operator=(const TOne&) = delete;
            TOne& operator=(TOne&&) = delete;

            void Describe(IOutputStream &out, bool full = true) const
            {
                out << "Scan{" << Serial << " on " << Table;

                if (full) {
                    out << ", state " << int(State);
                }

                out << "}";
            }

            const ui64 Serial;
            const ui32 Table;
            std::shared_ptr<TTaskLease> Lease;
            TScanOptions Options;
            THolder<TScanSnapshot> Snapshot;
            std::optional<ui64> PreparedSnapshotId;
            EState State = EState::None;
            bool NeedsReadmission = false;
            // Owns expiry of the readiness state chosen at a broker grant.
            TInstant ReadinessDeadline;
            TInstant BrokerDeadline;
            ui32 ReadinessChecks = 0;
            ui64 TimeoutGeneration = 0;
            TActorId Actor;     /* Valid just after EState::Scan*/
            TAutoPtr<IScan> Scan;   /* Valid before EState::Scan    */
            ui64 Cookie = Max<ui64>();
            ui64 TaskId = 0;    /* Task number in res. broker   */
        };

        struct TTable {
            ui32 Prio = 0;      /* Scan task priotity in broker */
            TString Type;       /* Resource broker task type    */
            ui64 AheadLo = 0;
            ui64 AheadHi = 0;
            TIntrusiveList<TOne> Scans;
        };

        struct TAcquired {
            ui64 Serial = 0;
            ui32 Table = Max<ui32>();

            explicit operator bool() const {
                return Serial != 0;
            }
        };

        struct TCancelled {
            std::shared_ptr<TOne> Scan;
            THolder<TScanSnapshot> Snapshot;

            explicit operator bool() const {
                return bool(Scan);
            }
        };

    public:
        enum class EReadiness { Ready, Waiting, TimedOut, RetryLimitExceeded };

        /**
         * followerId must outlive TScans, it is read on every task submission
         * since executor learns its follower id only during boot.
         */
        TScans(NUtil::ILogger *logger, IOps *ops, TIntrusivePtr<TIdEmitter> emitter,
                    ITablet *owner, const TActorId& ownerActorId,
                    const TActorId& brokerClient, TActorSystem* actorSystem, const ui32& followerId,
                    std::function<void(THolder<TScanSnapshot>)> releaseSnapshot)
            : Logger(logger)
            , Ops(ops)
            , Owner(owner)
            , OwnerActorId(ownerActorId)
            , BrokerClient(brokerClient)
            , ActorSystem(actorSystem)
            , Tablet(Owner->TabletID())
            , FollowerId(followerId)
            , Emitter(std::move(emitter))
            , ReleaseSnapshot(std::move(releaseSnapshot))
        {

        }

        void Describe(IOutputStream &out) const
        {
            out
                << "Scans{serial " << Serial << ", " << Tables.size() << " tbl"
                << ", " << Scans.size() << " ~" <<  CounterAlive << " scn}";
        }

        void Configure(ui32 table, ui64 aLo, ui64 aHi, ui32 prio, TString type)
        {
            Tables[table].AheadLo = aLo;
            Tables[table].AheadHi = aHi;

            Tables[table].Prio = prio;
            Tables[table].Type = std::move(type);

            if (Y_UNLIKELY(NLocalDb::IsLegacyQueueIdTaskName(Tables[table].Type))) {
                // There are a lot of legacy tablets where old config has scans
                // and gen1 compaction on the same queue. It causes delayed
                // compactions in case of many long ReadTables resulting in
                // overloaded shard. Let compaction tasks go first by lowering
                // scans priority.
                ++Tables[table].Prio;
            }
        }

        static void ValidateOptions(const TScanOptions& options) {
            const auto* broker = std::get_if<TScanOptions::TResourceBrokerOptions>(&options.ResourceBroker);
            if (options.Factory) {
                Y_ENSURE(!options.IsResourceBrokerDisabled(), "Deferred scans require broker admission");
                Y_ENSURE(broker && broker->Memory, "Deferred scans must declare memory before construction");
            } else {
                Y_ENSURE(!broker || !broker->Memory || options.OnAdmissionFailure,
                    "Eager scans declaring memory require an admission failure callback");
            }
        }

        ui64 Queue(ui32 table, TAutoPtr<IScan> scan, ui64 cookie, const TScanOptions& options, THolder<TScanSnapshot> snapshot)
        {
            auto &one = Make(table, scan, EType::Client, options, std::move(snapshot));

            one.Cookie = cookie;
            if (const auto* byId = std::get_if<TScanOptions::TSnapshotById>(&options.Snapshot)) {
                one.PreparedSnapshotId = byId->SnapshotId;
            }

            if (options.IsResourceBrokerDisabled()) {
                // A call to Start is expected now
                one.State = EState::Ready;
                one.TaskId = 0;
            } else {
                // A call to Acquired is expected later
                one.State = EState::Task;
                one.TaskId = Emitter->Do();

                const auto* overrides = std::get_if<TScanOptions::TResourceBrokerOptions>(&options.ResourceBroker);

                auto cookie = MakeIntrusive<TCookie>(one.Serial);
                const ui64 memory = overrides ? overrides->Memory : 0;
                if (options.Factory) {
                    one.Lease = std::make_shared<TTaskLease>(ActorSystem, BrokerClient, one.TaskId,
                        overrides ? overrides->Type : Tables[table].Type,
                        overrides ? overrides->Prio : Tables[table].Prio, memory);
                    cookie->ManagedLease = true;
                }
                auto submit = MakeHolder<TEvResourceBroker::TEvSubmitTask>(
                    one.TaskId, MakeLabelFor(one), NResourceBroker::TResourceValues{{1, memory}},
                    overrides ? overrides->Type : Tables[table].Type,
                    overrides ? overrides->Prio : Tables[table].Prio, cookie);
                submit->Task.KeepOnClientDeath = bool(one.Lease);
                if (one.Lease) {
                    submit->Task.OwnerLease = one.Lease;
                }
                submit->SetOwner(Tablet, FollowerId);
                ToBroker(submit.Release());
                BeginBrokerWait(one);
            }

            return one.Serial;
        }

        TAcquired Acquired(ui64 task, TResource *cookie)
        {
            const auto one = Hold(CheckedCast<TCookie*>(cookie)->Serial);

            if (one == nullptr) {
                if (!CheckedCast<TCookie*>(cookie)->ManagedLease) {
                    ToBroker(new TEvResourceBroker::TEvFinishTask(task));
                }

                return { };
            } else if (one->State != EState::Task) {
                Y_TABLET_ERROR(NFmt::Do(*one) << " acquired an unexpected resource");
            } else {
                // An overdue grant must discard the old readiness state
                // before selecting a new one, even if its timer is delayed.
                ExpireReadiness(*one);
                if (!Lookup(one->Serial, false)) {
                    return {};
                }
                // A call to Start is expected now
                one->State = EState::Ready;
                one->BrokerDeadline = {};
                ++one->TimeoutGeneration;
                if (one->Lease) {
                    one->Lease->Acquired();
                }

                return { one->Serial, one->Table };
            }
        }

        EReadiness AwaitReadiness(ui64 serial) {
            const auto owner = Hold(serial);
            auto& one = *owner;
            if (one.ReadinessDeadline && TActivationContext::Now() >= one.ReadinessDeadline) {
                return EReadiness::TimedOut;
            }
            if (!one.Options.OnAdmissionReady) {
                return EReadiness::Ready;
            }
            if (!one.ReadinessDeadline && one.Options.QueueTimeout) {
                one.ReadinessDeadline = TActivationContext::Now() + one.Options.QueueTimeout;
            }
            auto resume = std::make_unique<IEventHandle>(BrokerClient, TActorId(), new NOps::TEvResumeScan(serial));
            const bool ready = one.Options.OnAdmissionReady(one.Options, std::move(resume));
            if (!Lookup(serial, false)) {
                return EReadiness::Waiting;
            }
            if (!ready) {
                if (one.ReadinessChecks++ >= MaxReadinessChecks) {
                    return EReadiness::RetryLimitExceeded;
                }
                if (!one.NeedsReadmission) {
                    // Only the bounded descriptor is needed while waiting.
                    // Transactions being waited on may need broker resources.
                    one.Lease->Suspend();
                    one.NeedsReadmission = true;
                    ScheduleAdmissionTimeout(one);
                }
                return EReadiness::Waiting;
            }
            if (one.NeedsReadmission) {
                // Updating the same task preserves its original submit time,
                // unlike cancelling it and submitting a new request.
                one.Lease->Readmit();
                one.NeedsReadmission = false;
                one.State = EState::Task;
                BeginBrokerWait(one);
                return EReadiness::Waiting;
            }
            return EReadiness::Ready;
        }

        TAcquired Resume(ui64 serial) {
            const auto* one = Lookup(serial, false);
            if (!one || one->State != EState::Ready || !one->Options.Factory) {
                return {};
            }
            return {one->Serial, one->Table};
        }

        bool Initialize(ui64 serial) {
            const auto owner = Hold(serial);
            auto& one = *owner;
            if (one.Options.Factory) {
                auto factory = std::move(one.Options.Factory);
                auto scan = factory(one.Options);
                if (!Lookup(serial, false)) {
                    return false;
                }
                one.Scan = scan;
                one.Options.PendingReservation.reset();
            }
            return bool(one.Scan);
        }

        std::shared_ptr<TScanOptions> HoldOptions(ui64 serial) {
            auto owner = Hold(serial);
            auto* options = &owner->Options;
            return {std::move(owner), options};
        }

        bool Contains(ui64 serial) const {
            return Scans.contains(serial);
        }

        bool NeedsSnapshot(ui64 serial) {
            const auto& one = *Lookup(serial, true);
            if (one.PreparedSnapshotId) {
                const auto* byId = std::get_if<TScanOptions::TSnapshotById>(&one.Options.Snapshot);
                Y_ENSURE(byId && byId->SnapshotId == *one.PreparedSnapshotId,
                    "Cannot replace a prepared scan snapshot during admission");
            }
            return !one.Snapshot;
        }

        void SetSnapshot(ui64 serial, THolder<TScanSnapshot> snapshot) {
            auto& one = *Lookup(serial, true);
            if (const auto* byId = std::get_if<TScanOptions::TSnapshotById>(&one.Options.Snapshot)) {
                one.PreparedSnapshotId = byId->SnapshotId;
            }
            one.Snapshot = std::move(snapshot);
        }

        void AdmissionFailed(ui64 serial, const TString& reason, std::optional<ui64> memoryLimit = {}) {
            auto* one = Lookup(serial, false);
            if (!one || one->State > EState::Ready) {
                return;
            }
            const auto cookie = one->Cookie;
            auto cancelled = Cancel(serial);
            ReleaseSnapshot(std::move(cancelled.Snapshot));
            const auto& options = cancelled.Scan->Options;
            if (memoryLimit && options.OnMemoryLimitExceeded) {
                options.OnMemoryLimitExceeded(*memoryLimit);
            }
            if (options.OnAdmissionFailure) {
                options.OnAdmissionFailure(reason, true);
            } else {
                Owner->ScanComplete(EStatus::Term, {}, cookie, TActivationContext::ActorContextFor(OwnerActorId));
            }
        }

        void AdmissionTimedOut(ui64 serial, ui64 generation) {
            const auto one = Hold(serial);
            if (!one || (one->State != EState::Task && one->State != EState::Ready) ||
                one->TimeoutGeneration != generation) {
                return;
            }
            const auto now = TActivationContext::Now();
            if (one->State == EState::Ready) {
                if (one->ReadinessDeadline && now >= one->ReadinessDeadline) {
                    AdmissionFailed(serial, "Timed out waiting for scan readiness");
                } else {
                    ScheduleAdmissionTimeout(*one);
                }
                return;
            }
            ExpireReadiness(*one);
            if (!Contains(serial)) {
                return;
            }
            if (!one->BrokerDeadline || now < one->BrokerDeadline) {
                ScheduleAdmissionTimeout(*one);
                return;
            }
            const bool keepWaiting = one->Options.OnAdmissionTimeout && one->Options.OnAdmissionTimeout();
            if (!Contains(serial)) {
                return;
            }
            if (keepWaiting) {
                // A heartbeat keeps the same broker task and its queue position.
                BeginBrokerWait(*one);
                return;
            }
            AdmissionFailed(serial, "Timed out waiting for scan admission");
        }

        void Rejected(const TEvResourceBroker::TEvTaskOperationError& error) {
            if (error.Status.Code == TEvResourceBroker::TStatus::UNKNOWN_TASK) {
                // UpdateTask errors have no cookie. Match the live deferred
                // task by id, ignoring errors for running or unrelated work.
                for (const auto& [serial, one] : Scans) {
                    if (one->TaskId == error.TaskId && one->Lease && one->Options.Factory && one->State <= EState::Ready) {
                        // A delayed error may refer to an earlier update, while
                        // the broker still owns a previously acquired task.
                        one->Lease->Rejected();
                        return AdmissionFailed(serial, error.Status.Message);
                    }
                }
            }
            if (error.Status.Code == TEvResourceBroker::TStatus::MEMORY_LIMIT_EXCEEDED && error.Cookie) {
                const auto* resource = CheckedCast<TResource*>(error.Cookie.Get());
                if (resource->Source == TResource::ESource::Scan) {
                    const ui64 serial = CheckedCast<const TCookie*>(resource)->Serial;
                    if (auto* one = Lookup(serial, false)) {
                        if (one->Lease) {
                            one->Lease->Rejected();
                        } else {
                            // The broker already removed the eager task.
                            one->TaskId = 0;
                        }
                    }
                    // Limits may be applied asynchronously. Teach admission the
                    // actual capacity, then retry fitting against that value.
                    return AdmissionFailed(serial, error.Status.Message, error.Status.MemoryLimit);
                }
            }
        }

        void Start(ui64 serial)
        {
            auto &one = *Lookup(serial, true);
            Start(one, { });
            one.Options.OnAdmissionReady = {};
            one.Options.OnReadinessExpired = {};
            one.Options.OnAdmissionFailure = {};
            one.Options.OnAdmissionTimeout = {};
            one.Options.OnMemoryLimitExceeded = {};
        }

        ui64 StartSystem(ui32 table, TAutoPtr<IScan> scan, NOps::TConf conf, THolder<TScanSnapshot> snapshot)
        {
            auto &one = Make(table, scan, EType::System, { }, std::move(snapshot));

            return Start(one, conf);
        }

        void Drop()
        {
            while (Tables) {
                Drop(Tables.begin()->first);
            }
        }

        TVector<THolder<TScanSnapshot>> Drop(ui32 table)
        {
            TVector<THolder<TScanSnapshot>> snapshots;

            if (auto *entry = Tables.FindPtr(table)) {
                while (entry->Scans) {
                    auto *one = entry->Scans.Front();
                    if (one->Snapshot) {
                        snapshots.push_back(std::move(one->Snapshot));
                    }
                    Cancel(*one, EState::Gone);
                }

                Tables.erase(table);
            }

            return snapshots;
        }

        TCancelled Cancel(ui64 serial)
        {
            const auto one = Hold(serial);

            if (serial & 0x1 /* system scan */) {
                Y_TABLET_ERROR(NFmt::If(one.get()) << " is system (" << serial << "), cannot be cancelled this way");
            }

            if (!one) {
                return { };
            }

            TCancelled cancelled{one, std::move(one->Snapshot)};

            if (!Cancel(*one, EState::Forget)) {
                return { };
            }

            return cancelled;
        }

        bool CancelSystem(ui64 serial)
        {
            auto *one = Lookup(serial, false);

            if (!(serial & 0x1 /* system scan */)) {
                Y_TABLET_ERROR(NFmt::If(one) << " is not system (" << serial << "), cannot be cancelled this way");
            }

            if (!one) {
                return false;
            }

            return Cancel(*one, EState::Forget);
        }

        TScanOutcome Release(ui64 serial, EStatus &code, TAutoPtr<IDestructable> &result)
        {
            auto *one = Lookup(serial, true);

            if (one->State < EState::Scan) {
                Y_TABLET_ERROR(NFmt::Do(*one) << " got unexpected scan result");
            } else {
                NUtil::SubSafe(CounterAlive, ui32(1));

                if (one->State == EState::Forget) {
                    code = EStatus::Term;
                }

                return Throw(*one, one->State, code, result);
            }
        }

    private:
        TOne& Make(ui32 table, TAutoPtr<IScan> scan, EType type, const TScanOptions& options, THolder<TScanSnapshot> snapshot)
        {
            /* odd NOps used to mark compactions (system scans) */

            const ui64 token = (Serial += 2) - (type == EType::System ? 1 : 0);

            auto [it, inserted] = Scans.emplace(token, std::make_shared<TOne>(token, table, options, std::move(snapshot)));
            Y_ENSURE(inserted, "Failed to make new scan state entry");

            auto* one = it->second.get();
            one->Scan = scan;
            Tables[table].Scans.PushBack(one);

            return *one;
        }

        ui64 Start(TOne &one, NOps::TConf conf)
        {
            if (one.State != EState::None && one.State != EState::Ready) {
                Y_TABLET_ERROR(NFmt::Do(one) << " is not in start condition");
            }

            switch (one.Options.ReadPrio) {
                case TScanOptions::EReadPrio::Default:
                    break;
                case TScanOptions::EReadPrio::Fast:
                    conf.ReadPrio = NBlockIO::EPriority::Fast;
                    break;
                case TScanOptions::EReadPrio::Bulk:
                    conf.ReadPrio = NBlockIO::EPriority::Bulk;
                    break;
                case TScanOptions::EReadPrio::Low:
                    conf.ReadPrio = NBlockIO::EPriority::Low;
                    break;
            }

            if (const auto* overrides = std::get_if<TScanOptions::TReadAheadOptions>(&one.Options.ReadAhead)) {
                conf.AheadLo = overrides->ReadAheadLo;
                conf.AheadHi = overrides->ReadAheadHi;
            } else {
                conf.AheadLo = Tables[one.Table].AheadLo;
                conf.AheadHi = Tables[one.Table].AheadHi;
            }

            Y_ENSURE(one.Snapshot);
            if (one.Lease) {
                one.Scan->HoldResourceReservation(one.Lease, [state = std::weak_ptr<TTaskLease::TState>(one.Lease->State)](ui64 bytes) {
                    const auto owner = state.lock();
                    Y_ENSURE(owner, "Scan reservation has already been released");
                    return TTaskLease::Retain(owner, bytes);
                });
            }
            auto actor = MakeHolder<NOps::TDriver>(one.Serial, one.Scan, conf, one.Snapshot);

            try {
                if (auto logl = Logger->Log(NUtil::ELnLev::Info))
                    logl << NFmt::Do(*Ops) << " starting " << NFmt::Do(*actor);
            } catch (...) {
                actor->RestoreUnstartedScan(one.Scan, one.Snapshot);
                throw;
            }

            ui32 pool = AppData()->BatchPoolId;
            if (const auto* overrides = std::get_if<TScanOptions::TActorPoolById>(&one.Options.ActorPool)) {
                pool = overrides->PoolId;
            }

            one.Actor = Ops->Register(actor.Release(), TMailboxType::HTSwap, pool);
            one.State = EState::Scan;
            CounterAlive += 1;

            return one.Serial;
        }

        bool Cancel(TOne &one, EState state)
        {
            if (one.State == EState::Task || one.State == EState::Ready) {
                TAutoPtr<IDestructable> result;
                Throw(one, state, EStatus::Term, result);
                return true;
            } else if (one.State == EState::Scan) {
                one.State = state;
                one.Unlink(); /* prevent cancelation on table drop */
                Ops->Send(one.Actor, new TEvents::TEvPoison);
                return true;
            } else if (one.State > EState::Scan) {
                /* Cancel(...) has been already called for this scan */
            }

            return false;
        }

        TScanOutcome Throw(TOne &one, EState last, EStatus status, TAutoPtr<IDestructable> &result)
        {
            if (one.Lease && status == EStatus::Done) {
                one.Lease->State->Cancel.store(false, std::memory_order_release);
            }
            if (auto task = std::exchange(one.TaskId, 0); task && !one.Lease) {
                if (one.State == EState::Task) {
                    ToBroker(new TEvResourceBroker::TEvRemoveTask(task));
                } else if (one.State > EState::Task) {
                    ToBroker(new TEvResourceBroker::TEvFinishTask(task));
                }
            }

            TScanOutcome outcome;
            outcome.System = one.Serial & 0x1;
            outcome.Cancelled = one.State != EState::Scan;

            if (one.State < EState::Scan) {
                /* Emulates legacy behaviour where cancelled and not started
                    scans is not passed to completion callback. This behaviour
                    probably will be changed in next callback iface updates.
                 */
            } else if (outcome.System || (one.State = last) == EState::Forget) {
                /* System scan or explicit cancelation omits callback */
            } else {
                auto ctx = TActivationContext::ActorContextFor(OwnerActorId);

                Owner->ScanComplete(status, result, one.Cookie, ctx);
            }

            const ui64 serial = one.Serial;
            one.Unlink();
            Scans.erase(serial);
            return outcome;
        }

        std::shared_ptr<TOne> Hold(ui64 serial) {
            const auto* entry = Scans.FindPtr(serial);
            return entry ? *entry : nullptr;
        }

        TOne* Lookup(ui64 serial, bool require)
        {
            const auto* entry = Scans.FindPtr(serial);
            auto* one = entry ? entry->get() : nullptr;

            if (require && one == nullptr) {
                Y_TABLET_ERROR("Cannot find scan serial " << serial << " in states");
            }

            return one;
        }

        TString MakeLabelFor(const TOne &one) const
        {
            TStringStream out;

            out << NFmt::Do(one, false) << "::" << Tablet;

            return out.Str();
        }

        void ExpireReadiness(TOne& one) {
            if (one.ReadinessDeadline && TActivationContext::Now() >= one.ReadinessDeadline) {
                one.ReadinessDeadline = {};
                if (one.Options.OnReadinessExpired) {
                    one.Options.OnReadinessExpired();
                }
            }
        }

        void BeginBrokerWait(TOne& one) {
            one.BrokerDeadline = one.Options.QueueTimeout
                ? TActivationContext::Now() + one.Options.QueueTimeout
                : TInstant();
            ScheduleAdmissionTimeout(one);
        }

        void ScheduleAdmissionTimeout(TOne& one) {
            ++one.TimeoutGeneration;
            if (one.Options.Factory) {
                auto deadline = one.BrokerDeadline;
                if (one.ReadinessDeadline) {
                    deadline = deadline ? Min(deadline, one.ReadinessDeadline) : one.ReadinessDeadline;
                }
                if (deadline) {
                    TActivationContext::ActorContextFor(BrokerClient).Schedule(deadline - TActivationContext::Now(),
                        new NOps::TEvAdmissionTimeout(one.Serial, one.TimeoutGeneration));
                }
            }
        }

        void ToBroker(TAutoPtr<IEventBase> event)
        {
            using namespace NResourceBroker;

            Ops->Send(MakeResourceBrokerID(), event.Release(), 0);
        }

    private:
        NUtil::ILogger * const Logger;
        IOps * const Ops;
        ITablet * const Owner;
        const TActorId OwnerActorId;
        const TActorId BrokerClient;
        TActorSystem* const ActorSystem;
        const ui64 Tablet;
        const ui32& FollowerId;

        ui64 Serial = 0;

        const TIntrusivePtr<TIdEmitter> Emitter;
        const std::function<void(THolder<TScanSnapshot>)> ReleaseSnapshot;
        ui32 CounterAlive = 0;
        THashMap<ui64, std::shared_ptr<TOne>> Scans;
        THashMap<ui32, TTable> Tables;  /* only alive scans */
    };
}
}
