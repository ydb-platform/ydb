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
        static constexpr ui32 MaxReadmissionAttempts = 8;

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
                bool OverrunReported = false;
                std::atomic<bool> Cancel{true};

                TState(TActorSystem* system, TActorId client, ui64 taskId, TString type, ui32 priority, ui64 memory)
                    : System(system), Client(client), TaskId(taskId), Type(std::move(type)), Priority(priority), Memory(memory)
                    , ChargedMemory(memory)
                {}

                ~TState() {
                    System->Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), Client,
                        new TEvResourceBroker::TEvFinishTask(TaskId, Cancel.load(std::memory_order_acquire))));
                }

                void UpdateRetained() {
                    const ui64 memory = Running ? Max(Memory, Retained) : Retained;
                    const ui64 cpu = Running ? 1 : 0;
                    if (memory && (memory != ChargedMemory || cpu != ChargedCpu)) {
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
                    State->ChargedMemory = State->Memory;
                    State->ChargedCpu = 1;
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

            std::shared_ptr<void> Retain(ui64 bytes) {
                auto payload = std::make_shared<TPayload>(State, bytes);
                with_lock(State->Lock) {
                    // The payload already exists, including when retained from
                    // Finish(). Charge estimate overruns instead of throwing in
                    // the scan's teardown path or hiding their memory usage.
                    State->Retained += bytes;
                    payload->Armed = true;
                    if (!State->OverrunReported && State->Retained > State->Memory) {
                        State->OverrunReported = true;
                        YDB_LOG_WARN_CTX_COMP(*State->System, NKikimrServices::TABLET_OPS_HOST,
                            "Retained scan payload exceeds admitted memory reservation",
                            {"scanType", State->Type}, {"client", State->Client}, {"taskId", State->TaskId},
                            {"admittedBytes", State->Memory}, {"retainedBytes", State->Retained},
                            {"overrunBytes", State->Retained - State->Memory});
                    }
                    State->UpdateRetained();
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
            EState State = EState::None;
            bool NeedsReadmission = false;
            // Owns expiry of the readiness state chosen at a broker grant.
            TInstant ReadinessDeadline;
            ui32 ReadmissionAttempts = 0;
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
            ui64 Serial = 0;
            ui32 Table = Max<ui32>();
            std::shared_ptr<TTaskLease> Lease;
            TScanOptions Options;
            THolder<TScanSnapshot> Snapshot;

            explicit operator bool() const {
                return Serial != 0;
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
                    const TActorId& brokerClient, TActorSystem* actorSystem, const ui32& followerId)
            : Logger(logger)
            , Ops(ops)
            , Owner(owner)
            , OwnerActorId(ownerActorId)
            , BrokerClient(brokerClient)
            , ActorSystem(actorSystem)
            , Tablet(Owner->TabletID())
            , FollowerId(followerId)
            , Emitter(std::move(emitter))
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

        ui64 Queue(ui32 table, TAutoPtr<IScan> scan, ui64 cookie, const TScanOptions& options, THolder<TScanSnapshot> snapshot)
        {
            auto &one = Make(table, scan, EType::Client, options, std::move(snapshot));

            one.Cookie = cookie;

            if (options.IsResourceBrokerDisabled()) {
                Y_ENSURE(!options.Factory, "Deferred scans require broker admission");
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
                    Y_ENSURE(memory, "Deferred scans must declare memory before construction");
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
                ScheduleAdmissionTimeout(one);
            }

            return one.Serial;
        }

        TAcquired Acquired(ui64 task, TResource *cookie)
        {
            auto *one = Lookup(CheckedCast<TCookie*>(cookie)->Serial, false);

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
                // A call to Start is expected now
                one->State = EState::Ready;
                ++one->TimeoutGeneration;
                if (one->Lease) {
                    one->Lease->Acquired();
                }

                return { one->Serial, one->Table };
            }
        }

        EReadiness AwaitReadiness(ui64 serial) {
            auto& one = *Lookup(serial, true);
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
            if (!one.Options.OnAdmissionReady(one.Options, std::move(resume))) {
                if (!one.NeedsReadmission) {
                    if (one.ReadmissionAttempts++ >= MaxReadmissionAttempts) {
                        return EReadiness::RetryLimitExceeded;
                    }
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
                ScheduleAdmissionTimeout(one);
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
            auto& one = *Lookup(serial, true);
            if (one.Options.Factory) {
                auto factory = std::move(one.Options.Factory);
                one.Scan = factory(one.Options);
                one.Options.PendingReservation.reset();
            }
            return bool(one.Scan);
        }

        TScanOptions& GetOptions(ui64 serial) {
            return Lookup(serial, true)->Options;
        }

        bool NeedsSnapshot(ui64 serial) {
            return !Lookup(serial, true)->Snapshot;
        }

        void SetSnapshot(ui64 serial, THolder<TScanSnapshot> snapshot) {
            Lookup(serial, true)->Snapshot = std::move(snapshot);
        }

        TCancelled AdmissionFailed(ui64 serial, const TString& reason, bool retry = true) {
            auto* one = Lookup(serial, false);
            if (!one || one->State > EState::Ready) {
                return {};
            }
            auto callback = std::move(one->Options.OnAdmissionFailure);
            auto cancelled = Cancel(serial);
            if (callback) {
                callback(reason, retry);
            }
            return cancelled;
        }

        TCancelled AdmissionTimedOut(ui64 serial, ui64 generation) {
            auto* one = Lookup(serial, false);
            if (!one || (one->State != EState::Task && one->State != EState::Ready) ||
                one->TimeoutGeneration != generation) {
                return {};
            }
            if (one->State == EState::Ready && one->ReadinessDeadline &&
                TActivationContext::Now() >= one->ReadinessDeadline) {
                return AdmissionFailed(serial, "Timed out waiting for scan readiness");
            }
            if (one->State == EState::Task) {
                ExpireReadiness(*one);
            }
            if (one->State == EState::Task && one->Options.OnAdmissionTimeout && one->Options.OnAdmissionTimeout()) {
                // A heartbeat must not cancel and reinsert a queued task: that
                // would lose its FIFO position under a sustained backlog.
                ScheduleAdmissionTimeout(*one);
                return {};
            }
            return AdmissionFailed(serial, "Timed out waiting for scan admission");
        }

        TCancelled Rejected(const TEvResourceBroker::TEvTaskOperationError& error) {
            if (error.Status.Code == TEvResourceBroker::TStatus::UNKNOWN_TASK) {
                // UpdateTask errors have no cookie. Match the live deferred
                // task by id, ignoring errors for running or unrelated work.
                for (const auto& [serial, one] : Scans) {
                    if (one.TaskId == error.TaskId && one.Lease && one.Options.Factory && one.State <= EState::Ready) {
                        // A delayed error may refer to an earlier update, while
                        // the broker still owns this task. Always send FinishTask.
                        return AdmissionFailed(serial, error.Status.Message);
                    }
                }
            }
            if (error.Status.Code == TEvResourceBroker::TStatus::MEMORY_LIMIT_EXCEEDED && error.Cookie) {
                const auto* resource = CheckedCast<TResource*>(error.Cookie.Get());
                if (resource->Source == TResource::ESource::Scan) {
                    const ui64 serial = CheckedCast<const TCookie*>(resource)->Serial;
                    if (auto* one = Lookup(serial, false); one && error.Status.MemoryLimit && one->Options.OnMemoryLimitExceeded) {
                        one->Options.OnMemoryLimitExceeded(*error.Status.MemoryLimit);
                    }
                    // Limits may be applied asynchronously. Teach admission the
                    // actual capacity, then retry fitting against that value.
                    // A rejected readmission still owns its broker task, so
                    // keep cleanup armed even when no resources were granted.
                    return AdmissionFailed(serial, error.Status.Message);
                }
            }
            return {};
        }

        void Start(ui64 serial)
        {
            auto &one = *Lookup(serial, true);
            Start(one, { });
            one.Options.OnAdmissionReady = {};
            one.Options.OnReadinessExpired = {};
            one.Options.OnAdmissionFailure = {};
            one.Options.OnAdmissionTimeout = {};
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
            auto *one = Lookup(serial, false);

            if (serial & 0x1 /* system scan */) {
                Y_TABLET_ERROR(NFmt::If(one) << " is system (" << serial << "), cannot be cancelled this way");
            }

            if (!one) {
                return { };
            }

            TCancelled cancelled{ one->Serial, one->Table, one->Lease, std::move(one->Options), std::move(one->Snapshot) };

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

            auto got = Scans.emplace(
                std::piecewise_construct,
                std::make_tuple(token),
                std::make_tuple(token, table, options, std::move(snapshot)));

            Y_ENSURE(got.second, "Failed to make new scan state entry");

            auto *one = &got.first->second;

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
                one.Scan->HoldResourceReservation(one.Lease, [lease = std::weak_ptr<TTaskLease>(one.Lease)](ui64 bytes) {
                    const auto owner = lease.lock();
                    Y_ENSURE(owner, "Scan reservation has already been released");
                    return owner->Retain(bytes);
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
            Scans.erase(serial);
            return outcome;
        }

        TOne* Lookup(ui64 serial, bool require)
        {
            auto *one = Scans.FindPtr(serial);

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

        void ScheduleAdmissionTimeout(TOne& one) {
            ++one.TimeoutGeneration;
            if (one.Options.Factory && one.Options.QueueTimeout) {
                const auto delay = one.ReadinessDeadline
                    ? Min(one.Options.QueueTimeout, one.ReadinessDeadline - TActivationContext::Now())
                    : one.Options.QueueTimeout;
                TActivationContext::ActorContextFor(BrokerClient).Schedule(delay,
                    new NOps::TEvAdmissionTimeout(one.Serial, one.TimeoutGeneration));
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
        ui32 CounterAlive = 0;
        THashMap<ui64, TOne> Scans;
        THashMap<ui32, TTable> Tables;  /* only alive scans */
    };
}
}
