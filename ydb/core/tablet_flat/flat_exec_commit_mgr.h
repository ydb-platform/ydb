#pragma once

#include "flat_bio_eggs.h"
#include "flat_exec_commit.h"
#include "flat_boot_oven.h"
#include "logic_snap_waste.h"
#include "flat_sausage_slicer.h"
#include "flat_executor_gclogic.h"
#include "flat_executor_counters.h"
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TCommitManager {
        /*_ TExecutor log commits flow controller module

                     Optional synchronous ~ on the current Step0 --.
                                                                    vvvvv
       >---------.-----------.-----------.-----------.-----------.----------.->
                 | Pending.4 | Pending.3 | Detach.2  | Detach.1  |   Sync   |
       >---------'-----------'-----------'-----------'-----------'----------'->

       Confirmed |      Commit(..)-ed    |     Acquired with Begin(..)      |

           .    N-4         N-3         N-2         N-1          N         N+1
      Step | >---|-----------'-----------|-----------'-----------|----------'>
           |     '                       '                       '
           '    Back                    Tail                    Head

            * There may be at most one synchronous ~ on the current Head step;
            * detached ~ acquires step by moving Head forward on Begin(false);
            * synchronus ~ moves Head step forward only on its Commit(...);
            * all Commit(..) calls have to be serialized by commit Step order.

            > CommitManager{ 1:1 | 4 [7, 7], lvl 5982068b }
                             ^^^.  ^.^^^^^^.     ^^^^^^^.
                                 `.  `.     `.           `- Blobs KEEPing by tablet
                                   `.  `.     `-- Current active commits steps span
                                     `.  `------- Pending commit confirmation step
                                       `--------- Tablet:Generation of the tablet

              [7, 7] has one active synchronous commit on the Head
              [7, 8) has one detached commit on past step 7
              [6, 8] has two detached commits on {6, 7} and one sync
              [8, 8) has no any allocated active commits at all
         */

    public:
        using IOps = NActors::IActorOps;
        using ETactic = TEvBlobStorage::TEvPut::ETactic;
        using TMonCo = TExecutorCounters;
        using TGcLogic = TExecutorGCLogic;
        using TEvCommit = TEvTablet::TEvCommit;

        TCommitManager(NBoot::TSteppedCookieAllocatorFactory &steppedCookieAllocatorFactory, TIntrusivePtr<NSnap::TWaste> waste, TGcLogic *logic)
            : Tablet(steppedCookieAllocatorFactory.Tablet)
            , Gen(steppedCookieAllocatorFactory.Gen)
            , Waste(std::move(waste))
            , GcLogic(logic)
            , Turns_(steppedCookieAllocatorFactory.Sys(NBoot::TCookie::EIdx::TurnLz4))
            , Annex(steppedCookieAllocatorFactory.Data())
            , Turns(1, Turns_.Get(), NBlockIO::BlockSize)
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "CommitManager{" << Tablet << ":" << Gen << " | "
                << Min(Back, Tail) << " [" << Tail << ", "
                << Head << (Sync ? "]" : ")")
                << ", lvl " << Waste->Level << "b}";
        }

        void Start(IOps *ops, TActorId owner, ui32 *step0, TMonCo *monCo)
        {
            Y_ABORT_UNLESS(!std::exchange(Ops, ops), "Commit manager is already started");

            Step0 = step0;
            Owner = owner;
            MonCo = monCo;

            *Step0 = Head = Tail = 1;
        }

        void SetTactic(ETactic tactic) noexcept { Tactic = tactic; }

        ui64 Stamp() const noexcept
        {
            return NTable::TTxStamp{ Gen, Head };
        }

        TAutoPtr<TLogCommit> Begin(bool sync, ECommit type, NWilson::TTraceId traceId) noexcept
        {
            const auto step = Head;

            if (Sync && sync) {
                Y_Fail(NFmt::Do(*this) << " tried to start nested commit");
            } else if (Sync && !sync) {
                Y_Fail(NFmt::Do(*this) << " tried to detach sync commit");
            } else if (sync) {
                Sync = true;
            } else {
                Switch(Head += 1); /* detached commits moves head now */
            }

            return new TLogCommit(sync, step, type, std::move(traceId));
        }

        void Commit(TAutoPtr<TLogCommit> commit) noexcept
        {
            if (commit->Step != Tail || (commit->Sync && !Sync)) {
                Y_Fail(
                    NFmt::Do(*this) << " got unordered " << NFmt::Do(*commit));
            } else if (commit->Step == Head) {
                Sync = false, Switch(Head += 1); /* sync ~ moves head forward */
            }

            Tail += 1;
            Back = (Back == Max<ui32>() ? commit->Step : Back);

            StatsAccount(*commit);
            Waste->Account(commit->GcDelta);
            GcLogic->WriteToLog(*commit);
            TrackCommitTxs(*commit);
            SendCommitEv(*commit);
        }

        void Confirm(const ui32 step) noexcept
        {
            if (Back == Max<ui32>() || step != Back || step >= Tail) {
                Y_Fail(NFmt::Do(*this) << " got unexpected confirm " << step);
            } else {
                Back = (Back + 1 == Tail) ? Max<ui32>() : Back + 1;
            }
        }

    private:
        void Switch(ui32 step) noexcept
        {
            *Step0 = step;

            Turns_->Switch(step, true /* require step switch */);
            Annex->Switch(step, true /* require step switch */);
        }

        void StatsAccount(const TLogCommit &commit) noexcept
        {
            ui64 bytes = 0;

            for (const auto &one : commit.Refs)
                bytes += one.Buffer.size();

            MonCo->Cumulative()[TMonCo::LOG_COMMITS].Increment(1);
            MonCo->Cumulative()[TMonCo::LOG_WRITTEN].Increment(bytes);
            MonCo->Cumulative()[TMonCo::LOG_EMBEDDED].Increment(commit.Embedded.size());
        }

        void TrackCommitTxs(TLogCommit &commit) noexcept;

        void SendCommitEv(TLogCommit &commit) noexcept
        {
            const bool snap = (commit.Type == ECommit::Snap);

            auto *ev = new TEvCommit(Tablet, Gen, commit.Step, { commit.Step - 1 }, snap);

            ev->CommitTactic = Tactic;
            ev->References = std::move(commit.Refs);
            ev->EmbeddedLogBody = std::move(commit.Embedded);
            ev->WaitFollowerGcAck = commit.WaitFollowerGcAck;
            ev->FollowerAux = std::move(commit.FollowerAux);
            ev->GcDiscovered = std::move(commit.GcDelta.Created);
            ev->GcLeft = std::move(commit.GcDelta.Deleted);
            ev->EmbeddedMetadata = std::move(commit.Metadata);

            Ops->Send(Owner, ev, 0, ui64(commit.Type), std::move(commit.TraceId));
        }

    public:
        const ui64 Tablet = Max<ui64>();
        const ui32 Gen = Max<ui32>();

    private:
        IOps * Ops = nullptr;
        TActorId Owner;
        ui32 Back = Max<ui32>();/* Commits confirmation edge step   */
        ui32 Tail = 0;          /* Active detached lower Step       */
        ui32 Head = 0;          /* Tablet current step, AKA Step0   */
        ui32 *Step0 = nullptr;  /* Compatability for tablet Step0   */
        bool Sync = false;      /* Synchromous commit in progress   */
        TIntrusivePtr<NSnap::TWaste> Waste;
        ETactic Tactic = ETactic::TacticDefault;
        TGcLogic * const GcLogic = nullptr;
        TMonCo * MonCo = nullptr;
        TAutoPtr<NPageCollection::TSteppedCookieAllocator> Turns_;

    public:
        TAutoPtr<NPageCollection::TSteppedCookieAllocator> Annex;
        NPageCollection::TSlicer Turns;
    };

}
}
