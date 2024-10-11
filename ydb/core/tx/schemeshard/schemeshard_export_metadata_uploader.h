#pragma once

#include "defs.h"
#include "schemeshard_info_types.h"

namespace NKikimr::NSchemeShard {

namespace NBackupRestore {

using TVirtualTimestamp = TRowVersion;

enum class EStorageType {
    YT,
    S3,
};

struct TLogMetadata : TSimpleRefCount<TLogMetadata> {
    using TPtr = TIntrusivePtr<TLogMetadata>;

    const TVirtualTimestamp StartVts;
    TString ConsistencyKey;
    EStorageType StorageType;
    TString StoragePath;
};

struct TFullBackupMetadata : TSimpleRefCount<TFullBackupMetadata> {
    using TPtr = TIntrusivePtr<TFullBackupMetadata>;

    const TVirtualTimestamp SnapshotVts;
    TString ConsistencyKey;
    TLogMetadata::TPtr FollowingLog;
    EStorageType StorageType;
    TString StoragePath;
};

class TMetadata {
public:
    TMetadata() = default;
    TMetadata(TVector<TFullBackupMetadata::TPtr>&& fullBackups, TVector<TLogMetadata::TPtr>&& logs);

    void AddFullBackup(TFullBackupMetadata::TPtr fullBackup);
    void AddLog(TLogMetadata::TPtr log);
    void SetConsistencyKey(const TString& key);

    TString Serialize() const;
    static TMetadata Deserialize(const TString& metadata);
private:
    TString ConsistencyKey;
    TMap<TVirtualTimestamp, TFullBackupMetadata::TPtr> FullBackups;
    TMap<TVirtualTimestamp, TLogMetadata::TPtr> Logs;
};

} // namespace NBackupRestore

IActor* CreateMetadataUploader(const TActorId& replyTo, TExportInfo::TPtr exportInfo, ui32 itemIdx);

} // namespace NKikimr::NSchemeShard
