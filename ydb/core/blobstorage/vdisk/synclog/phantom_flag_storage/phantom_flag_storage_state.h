#pragma once

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogformat.h>

#include "phantom_flags.h"
#include "phantom_flag_storage_snapshot.h"
#include "phantom_flag_thresholds.h"

#include <vector>

namespace NKikimr {

namespace NSyncLog {

// Manages Phantom Flag Storage - in-memory (TODO: persistent) storage for DoNotKeep flags
// Stores DoNotKeeps from cut synclog chunks which weren't synced with some VDisks of storage group

class TPhantomFlagStorageState {
public:
    TPhantomFlagStorageState(const TBlobStorageGroupType& gtype);

    void Activate();

    void AddBlobRecord(const TLogoBlobRec& blobRec);
    void AddFlags(TPhantomFlags flags);
    void Clear();

    TPhantomFlagStorageSnapshot GetSnapshot() const;
    bool IsActive() const;

private:
    const TBlobStorageGroupType GType;
    TPhantomFlagThresholds Thresholds;
    TPhantomFlags StoredFlags;
    bool Active = false;
};

} // namespace NSyncLog

} // namespace NKikimr
