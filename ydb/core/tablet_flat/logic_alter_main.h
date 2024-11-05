#pragma once

#include "flat_exec_commit.h"
#include "flat_sausage_solid.h"
#include "flat_sausage_slicer.h"
#include "flat_bio_eggs.h"

#include <ydb/core/tablet_flat/flat_executor.pb.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

    class TLogicAlter {
    public:
        TLogicAlter(TAutoPtr<NPageCollection::TSteppedCookieAllocator> cookies)
            : Cookies(cookies)
            , Slicer(1, Cookies.Get(), NBlockIO::BlockSize)
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out << "LAlter{log " << Log.size() << ", " << Bytes << "b}";
        }

        ui64 LogBytes() const noexcept { return Bytes; }

        void RestoreLog(const NPageCollection::TLargeGlobId &largeGlobId) noexcept
        {
            largeGlobId.MaterializeTo(Log), Bytes += largeGlobId.Bytes;
        }

        void SnapToLog(NKikimrExecutorFlat::TLogSnapshot &snap) noexcept
        {
            auto items = snap.MutableSchemeInfoBodies();
            for (const auto &logo : Log)
                LogoBlobIDFromLogoBlobID(logo, items->Add());

            auto deleted = snap.MutableGcSnapLeft();
            for (const auto &logo : ObsoleteLog) {
                LogoBlobIDFromLogoBlobID(logo, deleted->Add());
            }
        }

        void WriteLog(TLogCommit &commit, TString alter) noexcept
        {
            Cookies->Switch(commit.Step, true /* require step switch */);

            if (alter) {
                auto largeGlobId = Slicer.Do(commit.Refs, std::move(alter), false);

                largeGlobId.MaterializeTo(commit.GcDelta.Created);
                RestoreLog(largeGlobId);
            }
        }

        void Clear() noexcept
        {
            ObsoleteLog.splice(ObsoleteLog.end(), Log);
            Bytes = 0;
        }

    protected:
        TAutoPtr<NPageCollection::TSteppedCookieAllocator> Cookies;
        NPageCollection::TSlicer Slicer;
        ui64 Bytes = 0;
        TList<TLogoBlobID> Log;
        TList<TLogoBlobID> ObsoleteLog;
    };

}
}
