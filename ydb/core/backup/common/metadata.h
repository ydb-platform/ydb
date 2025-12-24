#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr::NBackup {

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

struct TIndexMetadata {
    TString ExportPrefix;
    TString ImplTablePrefix;
};

class TMetadata {
public:
    TMetadata() = default;
    TMetadata(TVector<TFullBackupMetadata::TPtr>&& fullBackups, TVector<TLogMetadata::TPtr>&& logs);

    void AddFullBackup(TFullBackupMetadata::TPtr fullBackup);
    void AddLog(TLogMetadata::TPtr log);
    void SetConsistencyKey(const TString& key);
    void SetVersion(ui64 version);
    bool HasVersion() const;
    ui64 GetVersion() const;

    void AddIndex(const TIndexMetadata& index);
    const std::optional<std::vector<TIndexMetadata>>& GetIndexes() const;

    TString Serialize() const;
    static TMetadata Deserialize(const TString& metadata);
private:
    TString ConsistencyKey;
    TMap<TVirtualTimestamp, TFullBackupMetadata::TPtr> FullBackups;
    TMap<TVirtualTimestamp, TLogMetadata::TPtr> Logs;
    TMaybeFail<ui64> Version;

    // Indexes:
    // Undefined (previous versions): we don't know if we see the export with _materialized_ indexes or without them, so list suitable S3 files to find out all materialized indexes
    // []: The export has no materialized indexes
    // [...]: The export must have all materialized indexes listed here
    std::optional<std::vector<TIndexMetadata>> Indexes;
};

} // namespace NKikimr::NBackup
