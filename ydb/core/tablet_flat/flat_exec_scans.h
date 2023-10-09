#pragma once

#include "flat_scan_eggs.h"
#include "flat_scan_actor.h"
#include "flat_exec_broker.h"
#include "util_fmt_line.h"
#include "util_fmt_abort.h"
#include "flat_util_misc.h"
#include "tablet_flat_executor.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TScanOutcome {
        bool System;
        bool Cancelled;
    };

    class TScans { /* NOps state tracker */

        using IScan = NTable::IScan;
        using EAbort = NTable::EAbort;
        using IOps = NActors::IActorOps;
        using ITablet = NFlatExecutorSetup::ITablet;
        using TEvResourceBroker = NResourceBroker::TEvResourceBroker;

        enum class EState {
            None    = 0,
            Task    = 1,    /* Waiting for resource     */
            Ready   = 2,    /* Waiting for Start(..)    */
            Scan    = 3,    /* Scan is in progress      */
            Forget  = 4,    /* Explicit cancelation     */
            Gone    = 5,    /* Internal termination     */
        };

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

            void Describe(IOutputStream &out, bool full = true) const noexcept
            {
                out << "Scan{" << Serial << " on " << Table;

                if (full) {
                    out << ", state " << int(State);
                }

                out << "}";
            }

            const ui64 Serial;
            const ui32 Table;
            TScanOptions Options;
            THolder<TScanSnapshot> Snapshot;
            EState State = EState::None;
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
            TScanOptions Options;
            THolder<TScanSnapshot> Snapshot;

            explicit operator bool() const {
                return Serial != 0;
            }
        };

    public:
        TScans(NUtil::ILogger *logger, IOps *ops, TIntrusivePtr<TIdEmitter> emitter,
                    ITablet *owner, const TActorId& ownerActorId)
            : Logger(logger)
            , Ops(ops)
            , Owner(owner)
            , OwnerActorId(ownerActorId)
            , Tablet(Owner->TabletID())
            , Emitter(std::move(emitter))
        {

        }

        void Describe(IOutputStream &out) const noexcept
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
                // A call to Start is expected now
                one.State = EState::Ready;
                one.TaskId = 0;
            } else {
                // A call to Acquired is expected later
                one.State = EState::Task;
                one.TaskId = Emitter->Do();

                const auto* overrides = std::get_if<TScanOptions::TResourceBrokerOptions>(&options.ResourceBroker);

                ToBroker(new TEvResourceBroker::TEvSubmitTask(
                            one.TaskId, MakeLabelFor(one),
                            {{ 1, 0 }},
                            overrides ? overrides->Type : Tables[table].Type,
                            overrides ? overrides->Prio : Tables[table].Prio,
                            new TCookie(one.Serial)));
            }

            return one.Serial;
        }

        TAcquired Acquired(ui64 task, TResource *cookie) noexcept
        {
            auto *one = Lookup(CheckedCast<TCookie*>(cookie)->Serial, false);

            if (one == nullptr) {
                ToBroker(new TEvResourceBroker::TEvFinishTask(task));

                return { };
            } else if (one->State != EState::Task) {
                Y_Fail(NFmt::Do(*one) << " acquired an unexpected resource");
            } else {
                // A call to Start is expected now
                one->State = EState::Ready;

                return { one->Serial, one->Table };
            }
        }

        void Start(ui64 serial)
        {
            auto &one = *Lookup(serial, true);

            Start(one, { });
        }

        ui64 StartSystem(ui32 table, TAutoPtr<IScan> scan, NOps::TConf conf, THolder<TScanSnapshot> snapshot)
        {
            auto &one = Make(table, scan, EType::System, { }, std::move(snapshot));

            return Start(one, conf);
        }

        void Drop() noexcept
        {
            while (Tables) {
                Drop(Tables.begin()->first);
            }
        }

        TVector<THolder<TScanSnapshot>> Drop(ui32 table) noexcept
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

        TCancelled Cancel(ui64 serial) noexcept
        {
            auto *one = Lookup(serial, false);

            if (serial & 0x1 /* system scan */) {
                Y_Fail(NFmt::If(one) << " is system (" << serial << "), cannot be cancelled this way");
            }

            if (!one) {
                return { };
            }

            TCancelled cancelled{ one->Serial, one->Table, std::move(one->Options), std::move(one->Snapshot) };

            if (!Cancel(*one, EState::Forget)) {
                return { };
            }

            return cancelled;
        }

        bool CancelSystem(ui64 serial) noexcept
        {
            auto *one = Lookup(serial, false);

            if (!(serial & 0x1 /* system scan */)) {
                Y_Fail(NFmt::If(one) << " is not system (" << serial << "), cannot be cancelled this way");
            }

            if (!one) {
                return false;
            }

            return Cancel(*one, EState::Forget);
        }

        TScanOutcome Release(ui64 serial, EAbort &code, TAutoPtr<IDestructable> &result) noexcept
        {
            auto *one = Lookup(serial, true);

            if (one->State < EState::Scan) {
                Y_Fail(NFmt::Do(*one) << " got unexpected scan result");
            } else {
                NUtil::SubSafe(CounterAlive, ui32(1));

                if (one->State == EState::Forget) {
                    code = EAbort::Term;
                }

                return Throw(*one, one->State, code, result);
            }
        }

    private:
        TOne& Make(ui32 table, TAutoPtr<IScan> scan, EType type, const TScanOptions& options, THolder<TScanSnapshot> snapshot) noexcept
        {
            /* odd NOps used to mark compactions (system scans) */

            const ui64 token = (Serial += 2) - (type == EType::System ? 1 : 0);

            auto got = Scans.emplace(
                std::piecewise_construct,
                std::make_tuple(token),
                std::make_tuple(token, table, options, std::move(snapshot)));

            Y_ABORT_UNLESS(got.second, "Failed to make new scan state entry");

            auto *one = &got.first->second;

            one->Scan = scan;
            Tables[table].Scans.PushBack(one);

            return *one;
        }

        ui64 Start(TOne &one, NOps::TConf conf)
        {
            if (one.State != EState::None && one.State != EState::Ready) {
                Y_Fail(NFmt::Do(one) << " is not in start condition");
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

            Y_ABORT_UNLESS(one.Snapshot);
            auto *actor = new NOps::TDriver(one.Serial, one.Scan, conf, std::move(one.Snapshot));

            if (auto logl = Logger->Log(NUtil::ELnLev::Info))
                logl << NFmt::Do(*Ops) << " starting " << NFmt::Do(*actor);

            ui32 pool = AppData()->BatchPoolId;
            if (const auto* overrides = std::get_if<TScanOptions::TActorPoolById>(&one.Options.ActorPool)) {
                pool = overrides->PoolId;
            }

            CounterAlive += 1;
            one.State = EState::Scan;
            one.Actor = Ops->Register(actor, TMailboxType::HTSwap, pool);

            return one.Serial;
        }

        bool Cancel(TOne &one, EState state) noexcept
        {
            if (one.State == EState::Task || one.State == EState::Ready) {
                TAutoPtr<IDestructable> result;
                Throw(one, state, EAbort::Term, result);
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

        TScanOutcome Throw(TOne &one, EState last, EAbort status, TAutoPtr<IDestructable> &result)
        {
            if (auto task = std::exchange(one.TaskId, 0)) {
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

        TOne* Lookup(ui64 serial, bool require) noexcept
        {
            auto *one = Scans.FindPtr(serial);

            if (require && one == nullptr) {
                Y_Fail("Cannot find scan serial " << serial << " in states");
            }

            return one;
        }

        TString MakeLabelFor(const TOne &one) const noexcept
        {
            TStringStream out;

            out << NFmt::Do(one, false) << "::" << Tablet;

            return out.Str();
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
        const ui64 Tablet;

        ui64 Serial = 0;

        const TIntrusivePtr<TIdEmitter> Emitter;
        ui32 CounterAlive = 0;
        THashMap<ui64, TOne> Scans;
        THashMap<ui32, TTable> Tables;  /* only alive scans */
    };
}
}
