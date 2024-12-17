#pragma once

#include "flat_abi_evol.h"
#include "flat_abi_check.h"
#include "flat_sausage_chop.h"
#include "flat_boot_iface.h"
#include "flat_boot_back.h"
#include "flat_boot_blobs.h"
#include "flat_writer_banks.h"
#include "flat_executor_gclogic.h"
#include "flat_executor_txloglogic.h"

#include <ydb/core/tablet_flat/flat_executor.pb.h>
#include <ydb/core/util/pb.h>
#include <util/string/builder.h>
#include <util/generic/xrange.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    class TSnap final: public NBoot::IStep {
        using EIdx = TCookie::EIdx;
        using TTxStamp = NTable::TTxStamp;

    public:
        TSnap(IStep *owner, TIntrusivePtr<TDependency> deps, TAutoPtr<TBody> snap)
            : IStep(owner, NBoot::EStep::Snap)
            , Codec(NBlockCodecs::Codec("lz4fast"))
            , Deps(std::move(deps))
            , Snap(snap)
        {
            Y_ABORT_UNLESS(!(Deps && Snap), "Need either deps or raw snap");
        }

    private: /* IStep, boot logic DSL actor interface   */
        void Start() noexcept override
        {
            GrabSnapFromDeps();

            if (!Snap) {
                ProcessDeps(), Env->Finish(this);
            } else if (Snap->LargeGlobId.Lead.Step() == 0) {
                Y_Fail("Invalid TLogoBlobID of snaphot: " << Snap->LargeGlobId.Lead);
            } else if (Snap->Body) {
                Apply(Snap->LargeGlobId, Snap->Body);
            } else {
                Pending += Spawn<TLoadBlobs>(Snap->LargeGlobId, 0);
            }
        }

        void HandleStep(TIntrusivePtr<IStep> step) noexcept override
        {
            auto *load = step->ConsumeAs<TLoadBlobs>(Pending);

            Apply(load->LargeGlobId, load->Plain());
        }

    private:
        void Apply(const NPageCollection::TLargeGlobId &snap, TArrayRef<const char> body) noexcept
        {
            if (EIdx::SnapLz4 == TCookie(snap.Lead.Cookie()).Index()) {
                Decode(snap, Codec->Decode(body));
            } else {
                Decode(snap, body);
            }

            ProcessSnap(snap), ProcessDeps(), Env->Finish(this);
        }

        void Decode(const NPageCollection::TLargeGlobId &snap, TArrayRef<const char> body) noexcept
        {
            bool ok = ParseFromStringNoSizeLimit(Proto, body);
            Y_VERIFY_S(ok, "Failed to parse snapshot " << snap.Lead);

            bool huge = (body.size() > 10*1024*1024);

            if (auto logl = Env->Logger()->Log(huge ? ELnLev::Crit : ELnLev::Info)) {
                auto edge = Proto.HasVersion() ? Proto.GetVersion().GetHead():1;
                auto change = Proto.HasSerial() ? Proto.GetSerial() : 0;

                logl
                    << NFmt::Do(*Back) << " snap on "
                    << snap.Lead.Generation() << ":" << snap.Lead.Step()
                    << " change " << change << ", " << body.size() << "b"
                    << ", ABI " << edge << " of "
                        << "[" << ui32(NTable::ECompatibility::Tail)
                        << ", " << ui32(NTable::ECompatibility::Edge) << "]"
                    << ", GC{ +" << Proto.GcSnapDiscoveredSize()
                        << " -" << Proto.GcSnapLeftSize() << " }";
            }

            if (auto *abi = Proto.HasVersion() ? &Proto.GetVersion() : nullptr)
                NTable::TAbi().Check(abi->GetTail(), abi->GetHead(), "snap");
        }

        void ProcessSnap(const NPageCollection::TLargeGlobId &snap) noexcept
        {
            Back->Snap = snap;
            Back->Serial = Proto.GetSerial();

            ReadAlterLog();
            ReadRedoSnap();

            if (Logic->Result().Comp) {
                for (const auto &one : Proto.GetCompactionStates()) {
                    Back->Switches.emplace_back().Init(one);
                }
            }

            for (const auto &one : Proto.GetRowVersionStates()) {
                Back->Switches.emplace_back().Init(one);
            }

            for (const auto &one : Proto.GetDbParts()) {
                Back->Switches.emplace_back();
                Back->Switches.back().Init(one);
            }

            for (const auto &one : Proto.GetTxStatusParts()) {
                Back->Switches.emplace_back();
                Back->Switches.back().Init(one);
            }

            for (const auto &x : Proto.GetTableSnapshoted())
                Back->SetTableEdge(x);

            if (Logic->Result().Loans) {
                for (const auto &one : Proto.GetBorrowInfoIds()) {
                    const auto logo = LogoBlobIDFromLogoBlobID(one);

                    Back->LoansLog.push_back({{ Logic->GetBSGroupFor(logo), logo }, { }});
                }
            }

            ReadGcSnap();
            ReadWaste();
        }

        void ReadAlterLog() noexcept
        {
            TVector<TLogoBlobID> blobs;

            blobs.reserve(Proto.SchemeInfoBodiesSize());

            for (const auto &one : Proto.GetSchemeInfoBodies()) {
                blobs.emplace_back(LogoBlobIDFromLogoBlobID(one));
                const auto& blob = blobs.back();
                const auto* channel = Logic->Info->ChannelInfo(blob.Channel());
                if (channel && blob.Generation() < channel->LatestEntry()->FromGeneration) {
                    Logic->Result().ShouldSnapshotScheme = true;
                }
            }

            NPageCollection::TGroupBlobsByCookie chop(blobs);

            while (auto span = chop.Do()) {
                const auto group = Logic->GetBSGroupFor(span[0]);

                Back->AlterLog.push_back({ NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, group), { } });
            }
        }

        void ReadRedoSnap() noexcept
        {
            /* Merge of two lists with redo log records preferring records with
                embedded bodies. Later merge will be dropped and replaced with
                linear reading of generalized embedded records.
             */

            TVector<TLogoBlobID> logos;

            for (const auto &x : Proto.GetNonSnapLogBodies())
                logos.emplace_back(LogoBlobIDFromLogoBlobID(x));

            NPageCollection::TGroupBlobsByCookie chop(logos);

            size_t offset = 0;
            const size_t size = Proto.EmbeddedLogBodiesSize();

            for (auto span = chop.Do(); span || offset < size;) {
                auto *lx = offset < size ? &Proto.GetEmbeddedLogBodies(offset) : nullptr;
                auto right = lx ? TTxStamp{ lx->GetGeneration(), lx->GetStep() } : TTxStamp{ Max<ui64>() };
                auto left = span ? TTxStamp{ span[0].Generation(), span[0].Step() } : TTxStamp{ Max<ui64>() };

                if (left < right) {
                    auto largeGlobId = NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, Logic->GetBSGroupFor(span[0]));

                    Back->RedoLog.emplace_back(left, largeGlobId);
                } else {
                    Back->RedoLog.emplace_back(right, lx->GetBody());

                    offset++;
                }

                span = left > right ? span : chop.Do();
            }
        }

        void ReadGcSnap() noexcept
        {
            if (auto *logic = Logic->Result().GcLogic.Get()) {
                const auto &lead = Back->Snap.Lead;

                TGCLogEntry entry({ lead.Generation(), lead.Step() - 1 });

                entry.Delta.Created.reserve(Proto.GcSnapDiscoveredSize());
                entry.Delta.Deleted.reserve(Proto.GcSnapLeftSize());

                for (const auto &x : Proto.GetGcSnapDiscovered())
                    entry.Delta.Created.push_back(LogoBlobIDFromLogoBlobID(x));
                for (const auto &x : Proto.GetGcSnapLeft())
                    entry.Delta.Deleted.push_back(LogoBlobIDFromLogoBlobID(x));

                TVector<std::pair<ui32, ui64>> barriers;
                barriers.reserve(Proto.GcBarrierInfoSize());

                for (const auto &x : Proto.GetGcBarrierInfo())
                    barriers.push_back({x.GetChannel(), MakeGenStepPair(x.GetSetToGeneration(), x.GetSetToStep())});

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " process gc snapshot"
                        << ", + " << entry.Delta.Created
                        << ", - " << entry.Delta.Deleted;
                }

                logic->ApplyLogSnapshot(entry, barriers);
            }
        }

        void ReadWaste() const noexcept
        {
            if (auto *waste = Back->Waste.Get()) {
                if (Proto.HasWaste()) {
                    waste->Since = Proto.GetWaste().GetSince();
                    waste->Level = Proto.GetWaste().GetLevel();
                    waste->Keep = Proto.GetWaste().GetKeep();
                    waste->Drop = Proto.GetWaste().GetDrop();
                } else {
                    /* Old snapshot without step pollution accounting,
                        promote start age to the current snapshot since
                        waste, produced until current state, is known. */

                    const auto &lead = Snap->LargeGlobId.Lead;

                    waste->Since = TTxStamp(lead.Generation(), lead.Step());
                }
            }
        }

        void GrabSnapFromDeps() noexcept
        {
            auto *entry = (Deps && Deps->Entries) ? &Deps->Entries[0] : nullptr;

            if (entry && entry->IsSnapshot) {
                TTxStamp stamp{ entry->Id.first, entry->Id.second };

                const auto span = NPageCollection::TGroupBlobsByCookie(entry->References).Do();
                const auto largeGlobId = NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, Logic->GetBSGroupFor(span[0]));

                Y_ABORT_UNLESS(span.size() == entry->References.size());
                Y_ABORT_UNLESS(TCookie(span[0].Cookie()).Type() == TCookie::EType::Log);
                Y_ABORT_UNLESS(largeGlobId, "Cannot make TLargeGlobId for snapshot");

                if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                    logl
                        << NFmt::Do(*Back) << " snap in deps on "
                        << NFmt::TStamp(stamp) << ", " << NFmt::Do(largeGlobId);
                }

                ProcessGcLogEntry(*entry, true);

                Deps->Entries.pop_front(); /* skip record in ProcessDeps() */
                Snap = new TBody{ largeGlobId, { } };
            }
        }

        void ProcessDeps() noexcept
        {
            if (Deps) {
                for (auto &entry : Deps->Entries) {
                    Y_ABORT_UNLESS(!entry.IsSnapshot);

                    TTxStamp stamp{ entry.Id.first, entry.Id.second };

                    if (entry.EmbeddedLogBody) {
                        Y_ABORT_UNLESS(entry.References.empty());
                        Back->RedoLog.emplace_back(stamp, entry.EmbeddedLogBody);
                    } else {
                        NPageCollection::TGroupBlobsByCookie chop(entry.References);

                        while (auto span = chop.Do())
                            SortLogoSpan(stamp, span);
                    }

                    ProcessGcLogEntry(entry, false);
                }
            }
        }

        void ProcessGcLogEntry(TDependency::TEntry &entry, bool snap)
        {
            if (Back->Follower) // do nothing for followers
                return;

            if (auto logl = Env->Logger()->Log(ELnLev::Debug)) {
                logl
                    << NFmt::Do(*Back) << " process " << (snap ? "snap" : "log")
                    << " gc entry, + " << entry.GcDiscovered
                    << ", - " << entry.GcLeft;
            }

            TGCLogEntry gcEntry({ entry.Id.first, entry.Id.second });
            gcEntry.Delta.Created.swap(entry.GcDiscovered);
            gcEntry.Delta.Deleted.swap(entry.GcLeft);
            Back->Waste->Account(gcEntry.Delta);
            Logic->Result().GcLogic->ApplyLogEntry(gcEntry);
        }

        void SortLogoSpan(TTxStamp stamp, NPageCollection::TGroupBlobsByCookie::TArray span)
        {
            Y_ABORT_UNLESS(TCookie(span[0].Cookie()).Type() == TCookie::EType::Log);

            const auto group = Logic->GetBSGroupFor(span[0]);
            const auto index = TCookie(span[0].Cookie()).Index();

            if (index == EIdx::Redo || index == EIdx::RedoLz4) {

                Back->RedoLog.emplace_back(stamp, NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, group));

            } else if (index == EIdx::Alter) {

                Back->AlterLog.push_back({ NPageCollection::TGroupBlobsByCookie::ToLargeGlobId(span, group), { } });

            } else if (index == EIdx::Turn || index == EIdx::TurnLz4) {

                for (auto &one: span) {
                    Back->Switches.push_back(NPageCollection::TLargeGlobId{ group, one });
                }

            } else if (index == EIdx::Loan) {

                for (auto &one: span)
                    Back->LoansLog.push_back({{ group, one }, { }});

            } else if (index == EIdx::GCExt) {
                if (Logic->Result().GcLogic) {
                    for (auto &one: span)
                        Back->GCELog.push_back({ { group, one }, { }});
                }
            } else if (TCookie::CookieRangeRaw().Has(span[0].Cookie())) {
                /* Annex (external blobs for redo log), isn't used here */
            } else {
                Y_Fail(
                    NFmt::Do(*Back) << " got on booting blob " << span[0]
                    << " with unknown TCookie structue, EIdx " << ui32(index));
            }
        }

    private:
        const NBlockCodecs::ICodec *Codec = nullptr;
        TLeft Pending;
        TIntrusivePtr<TDependency> Deps;
        TAutoPtr<TBody> Snap;
        NKikimrExecutorFlat::TLogSnapshot Proto;
    };
}
}
}
