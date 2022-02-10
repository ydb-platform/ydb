#pragma once

#include "util_basics.h"
#include "util_fmt_abort.h"
#include "util_fmt_line.h"
#include "flat_sausage_slicer.h"
#include "flat_bio_eggs.h"
#include "flat_exec_commit.h"
#include "logic_snap_waste.h"

#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TLogicSnap {
    public:
        using TSnap = NKikimrExecutorFlat::TLogSnapshot;

        TLogicSnap(TAutoPtr<NPageCollection::TSteppedCookieAllocator> cookies, TIntrusivePtr<NSnap::TWaste> waste,
                    NPageCollection::TLargeGlobId last)
            : Cookies(cookies)
            , Slicer(1, Cookies.Get(), NBlockIO::BlockSize)
            , Waste_(std::move(waste))
            , Last(last)
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "LSnap{" << Cookies->Tablet << ":" << Cookies->Gen
                << ", on " << Last.Lead.Generation() << ":" << Last.Lead.Step()
                << ", " << Last.Bytes << "b, " << (Pending ? "wait" : "ready")
                << "}";
        }

        ui64 LogBytes() const noexcept { return Last.Bytes; }

        bool MayFlush(bool force) const noexcept
        {
            return
                !Pending
                && (force
                    || Waste_->Trace >= 300
                    || Waste_->Items >= 300
                    || Waste_->Bytes >= 128*1024*1024);
        }

        const NSnap::TWaste& Waste() const noexcept { return *Waste_; }

        void Confirm(ui32 step) noexcept
        {
            if (Pending == 0 || Pending != step)
                Y_Fail(
                    NFmt::Do(*this) << " got unexpected confirm for " << step);

            Pending = 0;
        }

        void MakeSnap(TSnap &snap, TLogCommit &commit, NUtil::ILogger *logger)
        {
            if (Pending != 0) {
                Y_Fail(
                    NFmt::Do(*this) << " cannot make snap on " << commit.Step);
            }

            if (auto *proto = snap.MutableWaste()) {
                proto->SetSince(Waste_->Since);
                proto->SetLevel(Max(Waste_->Level, i64(0)));
                proto->SetKeep(Waste_->Keep);
                proto->SetDrop(Waste_->Drop);
            }

            Pending = commit.Step;
            Cookies->Switch(commit.Step, true /* require step switch */);

            Last = Slicer.Do(commit.Refs, snap.SerializeAsString(), true);

            if (auto logl = logger->Log(NUtil::ELnLev::Info)) {
                logl
                    << NFmt::Do(*this) << " done, " << NFmt::Do(*Waste_);
            }

            Waste_->Flush();
        }

    private:
        ui32 Pending = 0;
        TAutoPtr<NPageCollection::TSteppedCookieAllocator> Cookies;
        NPageCollection::TSlicer Slicer;
        TIntrusivePtr<NSnap::TWaste> Waste_;
        NPageCollection::TLargeGlobId Last;
    };

}
}
