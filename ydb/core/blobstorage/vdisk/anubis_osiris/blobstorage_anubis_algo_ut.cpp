#include "defs.h"
#include "blobstorage_anubis_algo.h"
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

namespace NKikimr {

#define STR Cnull

    Y_UNIT_TEST_SUITE(TBlobStorageAnubisAlgo) {

        void OutputVector(const TVector<TLogoBlobID> &vec, IOutputStream &str) {
            str << "{";
            bool first = true;
            for (const auto &x : vec) {
                if (first)
                    first = false;
                else
                    str << ", ";
                str << x.ToString();
            }
            str << "}";
        }

        NKikimrBlobStorage::TEvVGetResult BuildResult(const TVDiskID &vd,
                                                      const TVector<TLogoBlobID> &blobs,
                                                      NKikimrProto::EReplyStatus globalStatus,
                                                      const TVector<NKikimrProto::EReplyStatus> &blobsStatus) {
            NKikimrBlobStorage::TEvVGetResult res;
            res.SetStatus(globalStatus);
            VDiskIDFromVDiskID(vd, res.MutableVDiskID());
            Y_ABORT_UNLESS(blobs.size() == blobsStatus.size());
            for (ui32 i = 0; i < blobs.size(); ++i) {
                auto *q = res.AddResult();
                q->SetStatus(blobsStatus[i]);
                LogoBlobIDFromLogoBlobID(blobs[i], q->MutableBlobID());
            }
            return res;
        }


        Y_UNIT_TEST(Mirror3) {
            TBlobStorageGroupInfo info(TErasureType::ErasureMirror3, 1, 4);
            TBlobsStatusMngr mngr(&info.GetTopology());

            TVector<TLogoBlobID> candidates = {
                TLogoBlobID(1, 4, 256, 1, 100, 0),
                TLogoBlobID(1, 4, 257, 1, 100, 0)
            };
            mngr.SetupCandidates(TVector<TLogoBlobID>(candidates));

            // TVDiskRange
            for (const auto &x : info.GetTopology().GetVDisks()) {
                const TVDiskIdShort vd = x.VDiskIdShort;
                Y_UNUSED(vd);

                STR << vd.ToString() << "\n";
                STR << "  candidates: ";
                OutputVector(candidates, STR);
                STR << "\n";
                STR << "  check: ";
                OutputVector(mngr.GetLogoBlobIdsToCheck(vd), STR);
                STR << "\n";
                UNIT_ASSERT_EQUAL(candidates, mngr.GetLogoBlobIdsToCheck(vd));
            }

            // assume we are at VDisk index=0

            {
                // all ok for disk 1
                auto vd = info.GetVDiskId(1);
                NKikimrBlobStorage::TEvVGetResult r = BuildResult(vd, candidates, NKikimrProto::OK,
                                                                  {NKikimrProto::OK, NKikimrProto::NODATA});
                mngr.UpdateStatusForVDisk(TVDiskIdShort(vd), r);
            }

            {
                // all ok for disk 2
                auto vd = info.GetVDiskId(2);
                NKikimrBlobStorage::TEvVGetResult r = BuildResult(vd, candidates, NKikimrProto::OK,
                                                                  {NKikimrProto::OK, NKikimrProto::NODATA});
                mngr.UpdateStatusForVDisk(TVDiskIdShort(vd), r);
            }

            TVector<TLogoBlobID> res = {TLogoBlobID(1, 4, 257, 1, 100, 0)};
            auto blobsToRemove = mngr.BlobsToRemove();
            STR << "blobs to remove: ";
            OutputVector(blobsToRemove, STR);
            STR << "\n";
            UNIT_ASSERT_EQUAL(res, blobsToRemove);
        }
    }

} // NKikimr
