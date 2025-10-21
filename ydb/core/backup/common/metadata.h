#pragma once
#include "encryption.h"

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

struct TChangefeedMetadata {
    TString ExportPrefix;
    TString Name;
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
    void AddChangefeed(const TChangefeedMetadata& changefeed);
    const std::optional<std::vector<TChangefeedMetadata>>& GetChangefeeds() const;

    void SetEnablePermissions(bool enablePermissions = true);
    bool HasEnablePermissions() const;
    bool GetEnablePermissions() const;

    TString Serialize() const;
    static TMetadata Deserialize(const TString& metadata);

private:
    TString ConsistencyKey;
    TMap<TVirtualTimestamp, TFullBackupMetadata::TPtr> FullBackups;
    TMap<TVirtualTimestamp, TLogMetadata::TPtr> Logs;
    TMaybeFail<ui64> Version;

    // Changefeeds:
    // Undefined (previous versions): we don't know if we see the export with changefeeds or without them, so list suitable S3 files to find out all changefeeds
    // []: The export has no changefeeds
    // [...]: The export must have all changefeeds listed here
    std::optional<std::vector<TChangefeedMetadata>> Changefeeds;

    // EnablePermissions:
    // Undefined (previous versions): we don't know if we see the export with permissions or without them, so check S3 for the permissions file existence
    // 0: The export has no permissions
    // 1: The export must have permissions file
    std::optional<bool> EnablePermissions;
};

TString NormalizeItemPath(const TString& path);
TString NormalizeItemPrefix(TString prefix);
TString NormalizeExportPrefix(TString prefix);

class TSchemaMapping {
public:
    struct TItem {
        TString ExportPrefix;
        TString ObjectPath;
        TMaybe<NBackup::TEncryptionIV> IV;
    };

    TSchemaMapping() = default;

    TString Serialize() const;
    bool Deserialize(const TString& jsonContent, TString& error);

public:
    std::vector<TItem> Items;
};

} // namespace NKikimr::NBackup
