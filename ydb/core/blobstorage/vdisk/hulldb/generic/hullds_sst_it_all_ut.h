#include "hullds_sstvec_it.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    namespace NBlobStorageHullSstItHelpers {
        using TLogoBlobSst = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoBlobSstPtr = TIntrusivePtr<TLogoBlobSst>;
        using TLogoBlobOrderedSsts = TOrderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
        using TLogoBlobOrderedSstsPtr = TIntrusivePtr<TLogoBlobOrderedSsts>;
        using TSegments = TVector<TLogoBlobSstPtr>;

        static const ui32 ChunkSize = 8u << 20u;
        static const ui32 CompWorthReadSize = 2u << 20u;

        TLogoBlobSstPtr GenerateSst(ui32 step, ui32 recs, ui32 plus, ui64 tabletId = 0, ui32 generation = 0,
                                    ui32 channel = 0, ui32 cookie = 0) {
            using TRec = TLogoBlobSst::TRec;
            Y_UNUSED(step);
            TLogoBlobSstPtr ptr(new TLogoBlobSst(TTestContexts().GetVCtx()));
            for (ui32 i = 0; i < recs; i++) {
                TLogoBlobID id(tabletId, generation, step + i * plus, channel, 0, cookie);
                TRec rec {TKeyLogoBlob(id), TMemRecLogoBlob()};
                ptr->LoadedIndex.push_back(rec);
            }
            return ptr;
        }

        TLogoBlobOrderedSstsPtr GenerateOrderedSsts(ui32 step, ui32 recs, ui32 plus, ui32 ssts, ui64 tabletId = 0,
                                                    ui32 generation = 0, ui32 channel = 0, ui32 cookie = 0) {
            TSegments vec;
            for (ui32 i = 0; i < ssts; i++) {
                vec.push_back(GenerateSst(step, recs, plus, tabletId, generation, channel, cookie));
                step += recs * plus;
            }

            return TLogoBlobOrderedSstsPtr(new TLogoBlobOrderedSsts(vec.begin(), vec.end()));

        }

    } // NBlobStorageHullSstItHelpers

} // NKikimr