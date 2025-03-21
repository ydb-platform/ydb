#pragma once

#include "util_basics.h"
#include "util_fmt_flat.h"
#include "flat_exec_commit.h"
#include "flat_boot_cookie.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NSnap {

    struct TWaste : public TSimpleRefCount<TWaste> {
        using EIdx = NBoot::TCookie::EIdx;

        TWaste(ui32 gen) : Since(ui64(gen) << 32) { }

        void Describe(IOutputStream &out, bool full = false) const
        {
            out
                << "Waste{" << NFmt::TStamp(Since) << ", " << Level << "b"
                << " +(" << Items << ", " << Bytes << "b), " << Trace << " trc";

            (full ? (out <<", -" << Drop << "b acc") : out) << "}";
        }

        void Account(const TGCBlobDelta &delta) noexcept
        {
            Trace += 1;

            for (auto &one : delta.Created) {
                Level += one.BlobSize(), Keep += one.BlobSize();
            }

            for (auto &one : delta.Deleted) {
                Level -= one.BlobSize(), Drop += one.BlobSize();

                Items += 1, Bytes += one.BlobSize();
            }
        }

        void Flush() noexcept
        {
            Trace = 0, Items = 0, Bytes = 0;
        }

        ui64 Since = 0; /* stamp of start accounting age    */
        i64 Level = 0;  /* Total bytes kept by commit manager */
        ui64 Trace = 0; /* Redo log commits since last snap */
        ui64 Items = 0;
        ui64 Bytes = 0; /* Waste grown since last snapshot  */

        /* Total KEEPed and DROPed blobs by tablet since birth or pollution
            collection age. Pre-age tablets may have (Keep - Drop) < 0. */

        ui64 Keep = 0;
        ui64 Drop = 0;
    };
}
}
}
