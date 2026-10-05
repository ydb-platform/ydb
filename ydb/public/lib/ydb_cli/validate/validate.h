#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NYdb::NConsoleClient {

// Read-only access to a backup tree. Keys use '/' and are relative to the storage root.
class IBackupStorage {
public:
    virtual ~IBackupStorage() = default;

    virtual bool Exists(const TString& key) const = 0;
    // Keys equal to prefix or starting with prefix + '/'.
    virtual TVector<TString> List(const TString& prefix) const = 0;
    virtual TString Read(const TString& key) const = 0;
    virtual void ReadChunks(const TString& key, const std::function<void(TStringBuf)>& onChunk) const = 0;
};

struct TValidateSettings {
    // Check backup layout and metadata only. Data file bytes are not read.
    bool SchemeOnly = false;
    // Raw encryption key bytes, same encoding as `ydb import`.
    TString EncryptionKey;
    // Object names relative to the validated path. Used for backups created with --item,
    // which have no SchemaMapping. Unset means the list was not provided.
    TMaybe<TVector<TString>> ExpectedObjects;
    // Stop after the first error. By default every independent error is reported.
    // Warnings do not stop the check.
    bool FailFast = false;
};

struct TValidationIssue {
    TString Path;
    TString Message;
};

struct TValidationReport {
    TVector<TString> Checked;
    TVector<TValidationIssue> Issues;
    // Present in the backup, absent from ExpectedObjects. Does not fail Ok().
    TVector<TValidationIssue> Warnings;

    bool Ok() const {
        return Issues.empty();
    }
};

// One object name per line. Empty lines are skipped. Names are not normalized here.
TVector<TString> ParseExpectedObjects(TStringBuf text);

// `path` is a full backup (metadata.json kind SimpleExportV0), a directory of exported
// objects without that metadata (export --item), or one schema object.
TValidationReport ValidateBackup(const IBackupStorage& storage, const TString& path, const TValidateSettings& settings);

// Lowercase hex SHA-256 of data. Used by tests and checksum sidecars.
TString Sha256Hex(TStringBuf data);

// sha256sum sidecar body: "<hex> <file name>\n"
TString MakeChecksumSidecar(TStringBuf data, const TString& fileName);

} // namespace NYdb::NConsoleClient
