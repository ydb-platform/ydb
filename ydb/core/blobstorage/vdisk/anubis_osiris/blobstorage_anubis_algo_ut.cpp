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
    }

} // NKikimr
