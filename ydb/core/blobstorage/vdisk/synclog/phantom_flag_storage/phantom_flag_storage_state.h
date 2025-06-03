#pragma once

#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogmem.h>

#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flags.h>

#include <vector>

namespace NKikimr {

namespace NSyncLog {

// Manages Phantom Flag Storage - in-memory (TODO: persistent) storage for DoNotKeep flags
// Stores DoNotKeeps from cut synclog chunks which weren't synced with some VDisks of storage group

class TPhantomFlagStorageState {
public:
    void Activate();

    void AddFlags(TPhantomFlags flags);
    void Clear();

    TPhantomFlags GetFlags() const;
    bool IsActive() const;

private:
    TPhantomFlags StoredFlags;
    bool Active = false;
};

} // namespace NSyncLog

} // namespace NKikimr
