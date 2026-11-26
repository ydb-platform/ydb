#pragma once

#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    inline void MergeVGetKeepFlags(NKikimrBlobStorage::TQueryResult& result, bool keep, bool doNotKeep) {
        if (result.GetDoNotKeep() || doNotKeep) {
            result.ClearKeep();
            result.SetDoNotKeep(true);
        } else if (keep) {
            result.SetKeep(true);
        }
    }

} // NKikimr
