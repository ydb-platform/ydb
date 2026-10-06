#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/thread.h>
#include <util/system/types.h>

#include <exception>
#include <functional>
#include <utility>

namespace NYdb::NConsoleClient {

// Read-only access to a backup tree. Keys use '/' and are relative to the storage root.
class IBackupStorage {
public:
    virtual ~IBackupStorage() = default;

    virtual bool Exists(const TString& key) const = 0;
    // Keys equal to prefix or starting with prefix + '/'.
    virtual TVector<TString> List(const TString& prefix) const = 0;
    // Loads the whole object. For metadata and checksum sidecars, which are small.
    // Data files are processed with ReadChunks and must not be loaded here.
    virtual TString Read(const TString& key) const = 0;
    // Streams the object. `beginAttempt` runs before each read, including a retry
    // after an I/O error, so the caller can reset checksum state. `onChunk` then
    // receives successive pieces. A piece is valid only until `onChunk` returns;
    // the storage does not keep the object body.
    virtual void ReadChunks(
        const TString& key,
        const std::function<void()>& beginAttempt,
        const std::function<void(TStringBuf)>& onChunk) const = 0;
};

enum class EValidateFormat {
    Auto = 0,
    Full,
    Item,
};

// Checksums of metadata.json (backup root, SchemaMapping, and each exported object).
// Scheme and data-file checksums are selected separately.
enum class EMetadataChecksumMode {
    // Require a sidecar for every metadata.json. Default.
    Always = 0,
    // Full backup: require them when metadata.json says checksum sha256.
    // Item export: require an object's metadata checksum when version > 0,
    // or when scheme.pb.sha256 or create_view.sql.sha256 is present.
    Auto,
    // Do not require or read metadata checksum sidecars.
    Ignore,
};

struct TValidateSettings {
    // Check backup layout and metadata only. Data file bytes are not read.
    bool SchemeOnly = false;
    // Raw encryption key bytes, same encoding as `ydb import`.
    // Encrypted files are not validated; a supplied key is unused.
    TString EncryptionKey;
    // Object names relative to the validated path. Used for backups created with --item,
    // which have no SchemaMapping. Unset means the list was not provided.
    TMaybe<TVector<TString>> ExpectedObjects;
    // Stop after the first error. By default every independent error is reported.
    // Warnings do not stop the check.
    bool FailFast = false;
    // Maximum number of threads for object and data-file checks.
    // 0 means DefaultValidateThreads(), the same rule as `ydb import file csv`.
    ui64 Threads = 0;
    // auto: full backup if kind is SimpleExportV0, any file remains under SchemaMapping/
    // (including checksum sidecars and encrypted payloads), or metadata.json.sha256
    // remains without metadata.json.
    // full: require a full backup. item: scan schema objects, skip SchemaMapping completeness.
    EValidateFormat Format = EValidateFormat::Auto;
    // always: every metadata.json needs a checksum sidecar.
    // auto: follow the backup's checksum declaration. ignore: skip metadata checksums.
    EMetadataChecksumMode MetadataChecksums = EMetadataChecksumMode::Always;
    // 0: phase changes. 1 (-v): each object's metadata, then its data.
    // 2 (-vv): reads, listings, and checksum files. 3+ (-vvv): exists probes, sizes, and I/O retries.
    // Lines are delivered to Progress. An empty Progress prints nothing.
    ui32 Verbosity = 0;
    std::function<void(TStringBuf)> Progress;
};

// Called from RetryValidateIo. No-op unless a validation with Progress is running on this thread.
void NoteValidateIoRetry(ui32 attempt, const std::exception& ex);

// hardware_concurrency() - 1 when that is positive, otherwise 1.
ui64 DefaultValidateThreads();

// Delay before the retry that follows `failedAttempt` failed tries (1-based).
// 100ms, 200ms, 400ms, ... capped at 2s. Jitter is added by RetryValidateIo.
TDuration ValidateRetryBackoff(ui32 failedAttempt);

// Retries `fn` after an exception. `retries` is the number of attempts (1 means no retry).
template <typename TFn, typename TSleep>
auto RetryValidateIo(ui32 retries, TFn&& fn, TSleep&& sleep) -> decltype(fn()) {
    const ui32 attempts = retries == 0 ? 1 : retries;
    for (ui32 attempt = 1;; ++attempt) {
        try {
            return fn();
        } catch (const std::exception& ex) {
            if (attempt >= attempts) {
                throw;
            }
            NoteValidateIoRetry(attempt, ex);
            const TDuration backoff = ValidateRetryBackoff(attempt);
            ui64 jitterMs = 0;
            if (backoff.MilliSeconds() > 0) {
                jitterMs = RandomNumber<ui64>(backoff.MilliSeconds() / 2 + 1);
            }
            sleep(backoff + TDuration::MilliSeconds(jitterMs));
        }
    }
}

template <typename TFn>
auto RetryValidateIo(ui32 retries, TFn&& fn) -> decltype(fn()) {
    return RetryValidateIo(retries, std::forward<TFn>(fn), [](TDuration delay) {
        Sleep(delay);
    });
}

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

// Checks byte-level integrity and file layout. Success does not mean the backup can be imported.
// `path` is a full backup (metadata.json kind SimpleExportV0, any file under SchemaMapping/,
// or an orphan metadata.json.sha256), a directory of exported objects without those markers
// (export --item), or one schema object.
TValidationReport ValidateBackup(const IBackupStorage& storage, const TString& path, const TValidateSettings& settings);

// Lowercase hex SHA-256 of data. Used by tests and checksum sidecars.
TString Sha256Hex(TStringBuf data);

// sha256sum sidecar body: "<hex> <file name>\n"
TString MakeChecksumSidecar(TStringBuf data, const TString& fileName);

} // namespace NYdb::NConsoleClient
