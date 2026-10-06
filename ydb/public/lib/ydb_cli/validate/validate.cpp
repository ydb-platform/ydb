#include "validate.h"

#include <ydb/library/backup/proto/proto.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <openssl/sha.h>

#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>
#include <vector>

namespace NYdb::NConsoleClient {
namespace {

TString NormalizeKey(TString key) {
    for (auto& ch : key) {
        if (ch == '\\') {
            ch = '/';
        }
    }
    while (key.StartsWith('/')) {
        key.erase(0, 1);
    }
    while (key.EndsWith('/')) {
        key.pop_back();
    }
    TString out;
    out.reserve(key.size());
    bool slash = false;
    for (char ch : key) {
        if (ch == '/') {
            if (!slash && !out.empty()) {
                out.push_back('/');
            }
            slash = true;
        } else {
            out.push_back(ch);
            slash = false;
        }
    }
    return out;
}

TString JoinKey(const TString& prefix, const TString& name) {
    const TString left = NormalizeKey(prefix);
    const TString right = NormalizeKey(name);
    if (!left) {
        return right;
    }
    if (!right) {
        return left;
    }
    return left + "/" + right;
}

TString FileName(const TString& key) {
    const auto pos = key.rfind('/');
    return pos == TString::npos ? key : key.substr(pos + 1);
}

TString ParentKey(const TString& key) {
    const auto pos = key.rfind('/');
    return pos == TString::npos ? TString() : key.substr(0, pos);
}

TString CanonicalObjectName(TString name) {
    name = NormalizeKey(name);
    while (name.StartsWith("./")) {
        name = name.substr(2);
    }
    return name ? name : TString(".");
}

TString ObjectName(const TString& root, const TString& dir) {
    if (dir == root) {
        return ".";
    }
    if (!root) {
        return dir;
    }
    if (dir.StartsWith(root + "/")) {
        return dir.substr(root.size() + 1);
    }
    return dir;
}

bool CoveredByExpected(const TString& name, const THashSet<TString>& expected) {
    if (expected.contains(name)) {
        return true;
    }
    for (const TString& item : expected) {
        // "." is the object at the validated path, not a prefix of every child.
        if (item != "." && name.StartsWith(item + "/")) {
            return true;
        }
    }
    return false;
}

bool IsSchemaObjectFileName(const TString& name) {
    static constexpr TStringBuf Names[] = {
        "scheme.pb",
        "create_view.sql",
        "create_topic.pb",
        "create_async_replication.sql",
        "create_transfer.sql",
        "create_external_data_source.sql",
        "create_external_table.sql",
        "system_view.pb",
    };
    for (const TStringBuf expected : Names) {
        if (name == expected || (name.EndsWith(".enc") && name.StartsWith(expected) && name.size() == expected.size() + 4)) {
            return true;
        }
    }
    return false;
}

bool IsHex(TStringBuf value) {
    if (value.size() != SHA256_DIGEST_LENGTH * 2) {
        return false;
    }
    for (char ch : value) {
        if (!IsAsciiHex(ch)) {
            return false;
        }
    }
    return true;
}

TString ChecksumToken(const TString& body) {
    const TString stripped = StripString(body);
    TStringBuf token(stripped);
    const auto space = token.find_first_of(" \t\r\n");
    if (space != TStringBuf::npos) {
        token = token.substr(0, space);
    }
    return to_lower(TString(token));
}

TString CanonicalDataName(ui32 index, TStringBuf extension) {
    return Sprintf("data_%02u%s", index, extension.data());
}

struct TDataPart {
    ui32 Index = 0;
    TString Key;
    TString PlainName;
    bool Compressed = false;
    bool Encrypted = false;
};

TMaybe<TDataPart> ParseDataPart(const TString& key) {
    const TString name = FileName(key);
    constexpr TStringBuf prefix = "data_";
    if (!name.StartsWith(prefix)) {
        return {};
    }
    size_t i = prefix.size();
    const size_t digitsBegin = i;
    while (i < name.size() && IsAsciiDigit(name[i])) {
        ++i;
    }
    if (i - digitsBegin < 2 || i >= name.size() || name[i] != '.') {
        return {};
    }
    ui32 index = 0;
    if (!TryFromString(name.substr(digitsBegin, i - digitsBegin), index)) {
        return {};
    }
    TStringBuf extension;
    TStringBuf rest(name);
    rest = rest.Tail(i);
    if (rest.SkipPrefix(".csv")) {
        extension = ".csv";
    } else if (rest.SkipPrefix(".parquet")) {
        extension = ".parquet";
    } else {
        return {};
    }
    const TString canonical = CanonicalDataName(index, extension);
    if (!name.StartsWith(canonical)) {
        return {};
    }
    TDataPart part;
    part.Index = index;
    part.Key = key;
    part.PlainName = canonical;
    if (rest.SkipPrefix(".zst")) {
        part.Compressed = true;
    }
    if (rest.SkipPrefix(".enc")) {
        part.Encrypted = true;
    }
    if (rest) {
        return {};
    }
    return part;
}

TString FinishSha256(SHA256_CTX* ctx) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, ctx);
    return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
}

// SHA-256 of the stored object. Bytes are hashed as they arrive.
// Compressed objects are decompressed in the same pass, because export checksums
// are computed from uncompressed plaintext. Neither form is buffered whole.
// onStoredBytes receives the size of each stored chunk, including compressed bytes.
TString HashStoredFile(
    const IBackupStorage& storage,
    const TString& key,
    bool compressed,
    const std::function<void(ui64)>& onStoredBytes)
{
    auto note = [&](TStringBuf chunk) {
        if (chunk && onStoredBytes) {
            onStoredBytes(chunk.size());
        }
    };
    SHA256_CTX ctx;
    if (!compressed) {
        storage.ReadChunks(
            key,
            [&] { SHA256_Init(&ctx); },
            [&](TStringBuf chunk) {
                note(chunk);
                if (chunk) {
                    SHA256_Update(&ctx, chunk.data(), chunk.size());
                }
            });
        return FinishSha256(&ctx);
    }

    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> dctx(ZSTD_createDCtx(), &ZSTD_freeDCtx);
    if (!dctx) {
        ythrow yexception() << "failed to create zstd context";
    }
    size_t lastRet = 1;
    char outBuf[1 << 16];
    storage.ReadChunks(
        key,
        [&] {
            SHA256_Init(&ctx);
            const size_t reset = ZSTD_DCtx_reset(dctx.get(), ZSTD_reset_session_only);
            if (ZSTD_isError(reset)) {
                ythrow yexception() << "zstd: " << ZSTD_getErrorName(reset);
            }
            lastRet = 1;
        },
        [&](TStringBuf chunk) {
            note(chunk);
            if (!chunk) {
                return;
            }
            ZSTD_inBuffer input{chunk.data(), chunk.size(), 0};
            while (input.pos < input.size) {
                ZSTD_outBuffer output{outBuf, sizeof(outBuf), 0};
                lastRet = ZSTD_decompressStream(dctx.get(), &output, &input);
                if (ZSTD_isError(lastRet)) {
                    ythrow yexception() << "zstd: " << ZSTD_getErrorName(lastRet);
                }
                if (output.pos != 0) {
                    SHA256_Update(&ctx, outBuf, output.pos);
                }
            }
        });
    if (lastRet != 0) {
        ythrow yexception() << "truncated zstd frame";
    }
    return FinishSha256(&ctx);
}

bool IsJsonInteger(const NJson::TJsonValue& value) {
    return value.IsInteger() || value.IsUInteger();
}

// GetStringRobust turns a missing field into "null", so only real strings count.
TString JsonString(const NJson::TJsonValue& json, const char* name) {
    if (!json.IsMap() || !json.Has(name) || !json[name].IsString()) {
        return {};
    }
    return json[name].GetString();
}

i64 JsonInteger(const NJson::TJsonValue& value) {
    return value.IsInteger() ? value.GetInteger() : static_cast<i64>(value.GetUInteger());
}

// Nested parallel loops run inline. A worker that is already validating an object
// must not wait for another pool, or the two levels deadlock.
thread_local bool InValidateWorker = false;

class TValidateLog {
public:
    explicit TValidateLog(const TValidateSettings& settings)
        : Verbosity(settings.Verbosity)
        , Sink(settings.Progress)
    {
    }

    void Phase(const TString& text) const {
        Write(0, TStringBuilder() << "phase: " << text);
    }

    void Progress(const TString& text) const {
        Write(0, TStringBuilder() << "progress: " << text);
    }

    void Object(const TString& text) const {
        Write(1, TStringBuilder() << "object: " << text);
    }

    void File(const TString& text) const {
        Write(2, TStringBuilder() << "file: " << text);
    }

    void Trace(const TString& text) const {
        Write(3, TStringBuilder() << "trace: " << text);
    }

private:
    void Write(ui32 level, const TString& text) const {
        if (!Sink || Verbosity < level) {
            return;
        }
        std::lock_guard<std::mutex> lock(Mu);
        Sink(text);
    }

    const ui32 Verbosity = 0;
    const std::function<void(TStringBuf)> Sink;
    mutable std::mutex Mu;
};

thread_local const TValidateLog* TlValidateLog = nullptr;

// Heartbeat while data files are read. wait_for is woken when the stage ends,
// so a short check does not block for the whole period.
constexpr std::chrono::seconds DataProgressPeriod{30};

TString DataFileCount(ui64 count) {
    return TStringBuilder() << count << " data file" << (count == 1 ? "" : "s");
}

TString FormatDataProgress(ui64 checked, ui64 total, ui64 bytesRead) {
    const ui64 remaining = checked < total ? total - checked : 0;
    return TStringBuilder()
        << "checked " << checked << " of " << DataFileCount(total)
        << ", " << remaining << " remaining, "
        << bytesRead << (bytesRead == 1 ? " byte read" : " bytes read");
}

class TDataProgressTicker {
public:
    TDataProgressTicker(
        const TValidateLog& log,
        const std::atomic<ui64>& checked,
        const std::atomic<ui64>& bytesRead,
        ui64 total)
        : Log(log)
        , Checked(checked)
        , BytesRead(bytesRead)
        , Total(total)
    {
    }

    TDataProgressTicker(const TDataProgressTicker&) = delete;
    TDataProgressTicker& operator=(const TDataProgressTicker&) = delete;

    ~TDataProgressTicker() {
        Stop();
    }

    void Start() {
        Report();
        Thread = std::thread([this] {
            std::unique_lock<std::mutex> lock(Mu);
            while (!Done) {
                if (Cv.wait_for(lock, DataProgressPeriod, [this] { return Done; })) {
                    return;
                }
                lock.unlock();
                Report();
                lock.lock();
            }
        });
    }

    void Finish() {
        Stop();
        Report();
    }

private:
    void Stop() {
        {
            std::lock_guard<std::mutex> lock(Mu);
            Done = true;
        }
        Cv.notify_all();
        if (Thread.joinable()) {
            Thread.join();
        }
    }

    void Report() const {
        Log.Progress(FormatDataProgress(
            Checked.load(std::memory_order_relaxed),
            Total,
            BytesRead.load(std::memory_order_relaxed)));
    }

    const TValidateLog& Log;
    const std::atomic<ui64>& Checked;
    const std::atomic<ui64>& BytesRead;
    const ui64 Total = 0;
    std::mutex Mu;
    std::condition_variable Cv;
    bool Done = false;
    std::thread Thread;
};

class TLogScope {
public:
    explicit TLogScope(const TValidateLog* log)
        : Previous(TlValidateLog)
    {
        TlValidateLog = log;
    }

    ~TLogScope() {
        TlValidateLog = Previous;
    }

    TLogScope(const TLogScope&) = delete;
    TLogScope& operator=(const TLogScope&) = delete;

private:
    const TValidateLog* Previous;
};

TString ShownPath(const TString& path) {
    return path.empty() ? TString(".") : path;
}

// Logs backup I/O. Exists probes are trace-only; reads and listings are file traces.
class TLoggingStorage : public IBackupStorage {
public:
    TLoggingStorage(const IBackupStorage& inner, const TValidateLog& log)
        : Inner(inner)
        , Log(log)
    {
    }

    bool Exists(const TString& key) const override {
        const bool found = Inner.Exists(key);
        Log.Trace(TStringBuilder() << "exists " << key << (found ? " yes" : " no"));
        return found;
    }

    TVector<TString> List(const TString& prefix) const override {
        Log.File(TStringBuilder() << "list " << ShownPath(prefix));
        TVector<TString> keys = Inner.List(prefix);
        Log.Trace(TStringBuilder() << "list " << ShownPath(prefix) << " " << keys.size() << " keys");
        return keys;
    }

    TString Read(const TString& key) const override {
        Log.File(TStringBuilder() << "read " << key);
        TString data = Inner.Read(key);
        Log.Trace(TStringBuilder() << "read " << key << " " << data.size() << " bytes");
        return data;
    }

    void ReadChunks(
        const TString& key,
        const std::function<void()>& beginAttempt,
        const std::function<void(TStringBuf)>& onChunk) const override
    {
        Log.File(TStringBuilder() << "read " << key);
        ui64 bytes = 0;
        Inner.ReadChunks(
            key,
            [&] {
                bytes = 0;
                beginAttempt();
            },
            [&](TStringBuf chunk) {
                bytes += chunk.size();
                onChunk(chunk);
            });
        Log.Trace(TStringBuilder() << "read " << key << " " << bytes << " bytes");
    }

private:
    const IBackupStorage& Inner;
    const TValidateLog& Log;
};

class TValidator {
public:
    TValidator(const IBackupStorage& storage, const TValidateSettings& settings, const TValidateLog& log)
        : Storage(storage)
        , Settings(settings)
        , Log(log)
        , Threads(settings.Threads == 0 ? DefaultValidateThreads() : settings.Threads)
    {
    }

    TValidationReport Run(const TString& path) {
        RootPath = NormalizeKey(path);
        // Exists and List outside ParallelFor are not caught by InvokeCheck.
        // Keep the issues already recorded and report the I/O error in the same result.
        try {
            return ValidatePath();
        } catch (const std::exception& ex) {
            Error(RootLabel(), TStringBuilder() << "internal error while validating: " << ex.what());
            return Finish();
        } catch (...) {
            Error(RootLabel(), "internal error while validating");
            return Finish();
        }
    }

    TValidationReport ValidatePath() {
        Log.Phase(TStringBuilder() << "use " << Threads << " worker thread" << (Threads == 1 ? "" : "s"));
        const TString root = RootPath;
        Log.Phase(TStringBuilder() << "detect backup format for " << RootLabel());
        if (Settings.SchemeOnly) {
            Log.Phase("scheme only; data file bytes are not read");
        }
        const TString metadataKey = JoinKey(root, "metadata.json");
        const bool forceFull = Settings.Format == EValidateFormat::Full;
        const bool forceItem = Settings.Format == EValidateFormat::Item;
        const bool schemaMapping = HasSchemaMapping(root);
        // Item exports have no backup-level checksum. A leftover sidecar means the
        // root metadata.json of a full backup was removed.
        const bool orphanMetadataChecksum = Exists(metadataKey + ".sha256") && !Exists(metadataKey);
        const bool fullBackupMarker = schemaMapping || orphanMetadataChecksum;

        const TReadResult metadata = ReadFile(metadataKey);
        if (metadata.Status == EReadStatus::Missing && Exists(metadataKey + ".enc")) {
            RejectEncrypted(metadataKey + ".enc");
            if (forceFull || fullBackupMarker || Stopped()) {
                return Finish();
            }
        }
        if (metadata.Status == EReadStatus::Failed) {
            Error(metadataKey, TStringBuilder() << "failed to read file: " << metadata.Error);
            if (forceFull || fullBackupMarker || Stopped()) {
                return Finish();
            }
        }

        NJson::TJsonValue json;
        bool parsedObject = false;
        bool isSimpleExport = false;
        if (metadata.Status == EReadStatus::Ok) {
            if (!NJson::ReadJsonTree(metadata.Content, &json) || !json.IsMap()) {
                Error(metadataKey, "metadata.json is not a JSON object");
                if (forceFull || fullBackupMarker || Stopped()) {
                    return Finish();
                }
            } else {
                parsedObject = true;
                isSimpleExport = JsonString(json, "kind") == "SimpleExportV0";
                if (json.Has("kind") && !isSimpleExport && !forceItem) {
                    VerifyMetadataChecksum(metadataKey, metadata.Content, Exists(metadataKey + ".sha256"));
                    const TString kind = json["kind"].IsString()
                        ? json["kind"].GetString()
                        : json["kind"].GetStringRobust();
                    Error(metadataKey, TStringBuilder() << "unsupported backup kind \"" << kind << "\"");
                    return Finish();
                }
            }
        }

        const bool looksFull = isSimpleExport || fullBackupMarker;
        const bool validateAsFull = forceFull || (!forceItem && looksFull);
        if (validateAsFull) {
            Log.Phase("full backup");
            if (metadata.Status == EReadStatus::Missing) {
                Error(metadataKey, schemaMapping
                    ? "full backup is missing metadata.json; SchemaMapping is present"
                    : "full backup is missing metadata.json");
                if (Exists(metadataKey + ".sha256")) {
                    Error(metadataKey + ".sha256", "checksum sidecar is present without metadata.json");
                }
                return Finish();
            }
            if (!parsedObject) {
                return Finish();
            }
            if (!isSimpleExport) {
                VerifyMetadataChecksum(metadataKey, metadata.Content, Exists(metadataKey + ".sha256"));
                Error(metadataKey, "full backup metadata.json must have kind SimpleExportV0");
                return Finish();
            }
            if (Settings.ExpectedObjects.Defined()) {
                Error(metadataKey, "--expected-objects applies only to backups created without SchemaMapping");
            }
            if (!Stopped()) {
                ValidateFullBackup(root, metadataKey, metadata.Content, json);
            }
            return Finish();
        }

        if (forceItem && looksFull) {
            Warning(metadataKey, "path looks like a full backup; --format=item skips SchemaMapping completeness checks");
        }

        // Not a full backup. The path may be one schema object, a directory of them, or both.
        Log.Phase(TStringBuilder() << "find schema objects in " << RootLabel());
        const bool self = ValidateObject(root, /*expectChecksums*/ Nothing(), /*expectCompressed*/ Nothing());
        // Exports created with --item have no backup-level metadata.json and no SchemaMapping.
        // The destination prefix is a directory of objects, and index tables may sit under a table
        // even when that table's metadata does not list them.
        bool nested = false;
        if (!Stopped()) {
            nested = ValidateDiscoveredObjects(root);
        }
        if (!self && !nested) {
            if (!Stopped()) {
                Error(root ? root : ".", "path is neither a full backup nor a schema object");
            }
        }
        if (Settings.ExpectedObjects.Defined() && !Stopped()) {
            Log.Phase("check expected objects");
            CheckExpectedObjects(root);
        } else if (!Settings.ExpectedObjects.Defined() && nested && !self && !Stopped()) {
            Warning(root ? root : ".",
                "item export completeness is not checked without --expected-objects; "
                "a successful result means the objects found here are intact");
        }
        return Finish();
    }

private:
    const IBackupStorage& Storage;
    const TValidateSettings& Settings;
    const TValidateLog& Log;
    const ui64 Threads = 1;
    TValidationReport Report;
    THashSet<TString> AllowedObjectDirs;
    THashSet<TString> Visited;
    TString RootPath;
    std::atomic<bool> SawEncryption{false};
    mutable std::mutex Mu;

    // Data bytes are hashed after every object's metadata. Scheme-only checks
    // never queue files: they only read checksum sidecars.
    struct TPendingDataFile {
        TString Dir;
        TString StoredKey;
        TString PlainKey;
        bool Compressed = false;
    };

    TVector<TPendingDataFile> PendingDataFiles;

    void Error(const TString& path, const TString& message) {
        std::lock_guard<std::mutex> lock(Mu);
        Report.Issues.push_back({path, message});
    }

    void Warning(const TString& path, const TString& message) {
        std::lock_guard<std::mutex> lock(Mu);
        Report.Warnings.push_back({path, message});
    }

    TString RootLabel() const {
        return RootPath ? RootPath : TString(".");
    }

    TValidationReport Finish() {
        RunPendingDataChecks();
        if (Settings.EncryptionKey && !SawEncryption.load()) {
            Warning(RootLabel(),
                "encryption key is unused; encrypted backup files are not validated by this command");
        }
        if (Settings.FailFast && !Report.Issues.empty()) {
            Log.Phase("stopped after the first error");
        }
        return Report;
    }

    // Fail-fast stops on errors only. Warnings are still collected until that point.
    bool Stopped() const {
        if (!Settings.FailFast) {
            return false;
        }
        std::lock_guard<std::mutex> lock(Mu);
        return !Report.Issues.empty();
    }

    void Checked(const TString& path) {
        std::lock_guard<std::mutex> lock(Mu);
        Report.Checked.push_back(path);
    }

    bool TryVisit(const TString& dir) {
        std::lock_guard<std::mutex> lock(Mu);
        return Visited.insert(dir).second;
    }

    bool WasVisited(const TString& dir) const {
        std::lock_guard<std::mutex> lock(Mu);
        return Visited.contains(dir);
    }

    void AllowDir(const TString& dir) {
        std::lock_guard<std::mutex> lock(Mu);
        AllowedObjectDirs.insert(dir);
    }

    bool IsAllowed(const TString& dir) const {
        std::lock_guard<std::mutex> lock(Mu);
        return AllowedObjectDirs.contains(dir);
    }

    void InvokeCheck(const std::function<void(size_t)>& fn, size_t index) {
        try {
            fn(index);
        } catch (const std::exception& ex) {
            Error(RootLabel(), TStringBuilder() << "internal error while validating: " << ex.what());
        } catch (...) {
            Error(RootLabel(), "internal error while validating");
        }
    }

    // Runs fn(0) .. fn(count-1). At most Threads calls are in progress.
    // With one thread, calls are in order and stop after the first error when fail-fast is set.
    void ParallelFor(size_t count, const std::function<void(size_t)>& fn) {
        if (count == 0) {
            return;
        }
        if (InValidateWorker || Threads <= 1 || count == 1) {
            for (size_t i = 0; i < count; ++i) {
                if (Stopped()) {
                    return;
                }
                InvokeCheck(fn, i);
            }
            return;
        }
        const size_t workers = std::min(static_cast<size_t>(Threads), count);
        std::atomic<size_t> next{0};
        std::vector<std::thread> pool;
        pool.reserve(workers);
        for (size_t worker = 0; worker < workers; ++worker) {
            pool.emplace_back([&] {
                TLogScope scope(&Log);
                InValidateWorker = true;
                while (!Stopped()) {
                    const size_t index = next.fetch_add(1, std::memory_order_relaxed);
                    if (index >= count) {
                        return;
                    }
                    InvokeCheck(fn, index);
                }
            });
        }
        for (std::thread& thread : pool) {
            thread.join();
        }
    }

    bool Exists(const TString& key) const {
        return Storage.Exists(key);
    }

    // Plaintext mapping.json and metadata.json are the usual markers. A damaged
    // full backup can lose those files and keep only checksum sidecars, encrypted
    // payloads, or any other object under SchemaMapping/.
    bool HasSchemaMapping(const TString& root) const {
        const TString prefix = JoinKey(root, "SchemaMapping");
        if (Exists(prefix + "/mapping.json") || Exists(prefix + "/metadata.json")) {
            return true;
        }
        for (const TString& key : Storage.List(prefix)) {
            if (key == prefix || key.StartsWith(prefix + "/")) {
                return true;
            }
        }
        return false;
    }

    enum class EReadStatus {
        Missing,
        Failed,
        Ok,
    };

    struct TReadResult {
        EReadStatus Status = EReadStatus::Missing;
        TString Content;
        TString Error;
    };

    TReadResult ReadFile(const TString& key) const {
        try {
            if (!Storage.Exists(key)) {
                TReadResult result;
                result.Status = EReadStatus::Missing;
                return result;
            }
        } catch (const std::exception& ex) {
            TReadResult result;
            result.Status = EReadStatus::Failed;
            result.Error = ex.what();
            return result;
        }
        try {
            TReadResult result;
            result.Status = EReadStatus::Ok;
            result.Content = Storage.Read(key);
            return result;
        } catch (const std::exception& ex) {
            TReadResult result;
            result.Status = EReadStatus::Failed;
            result.Error = ex.what();
            return result;
        }
    }

    void RejectEncrypted(const TString& key) {
        SawEncryption.store(true);
        Error(key, "encrypted backup files are not validated by this command");
    }

    TMaybe<TString> ReadPlainFile(const TString& key) {
        if (Exists(key + ".enc") && !Exists(key)) {
            RejectEncrypted(key + ".enc");
            return {};
        }
        const TReadResult content = ReadFile(key);
        if (content.Status == EReadStatus::Failed) {
            Error(key, TStringBuilder() << "failed to read file: " << content.Error);
            return {};
        }
        if (content.Status == EReadStatus::Missing) {
            Error(key, "file is missing");
            return {};
        }
        return content.Content;
    }

    // requiredInAuto is the backup's own declaration (version, checksum field, or a scheme sidecar).
    bool MetadataObjectRequired(bool requiredInAuto) const {
        if (Settings.MetadataChecksums == EMetadataChecksumMode::Always) {
            return true;
        }
        if (Settings.MetadataChecksums == EMetadataChecksumMode::Ignore) {
            return false;
        }
        return requiredInAuto;
    }

    void VerifyMetadataChecksum(const TString& contentKey, const TString& content, bool requiredInAuto) {
        if (Settings.MetadataChecksums == EMetadataChecksumMode::Ignore) {
            return;
        }
        const bool required = Settings.MetadataChecksums == EMetadataChecksumMode::Always || requiredInAuto;
        VerifyChecksum(contentKey, content, required);
    }

    void VerifyChecksum(const TString& contentKey, const TString& content, bool required) {
        const TString sidecar = contentKey + ".sha256";
        Log.File(TStringBuilder() << "checksum " << sidecar);
        if (!Exists(sidecar)) {
            if (required) {
                Error(sidecar, "checksum sidecar is missing");
            }
            return;
        }
        TString body;
        try {
            body = Storage.Read(sidecar);
        } catch (const std::exception& ex) {
            Error(sidecar, TStringBuilder() << "failed to read checksum: " << ex.what());
            return;
        }
        const TString expected = ChecksumToken(body);
        if (!IsHex(expected)) {
            Error(sidecar, "checksum sidecar does not contain a SHA-256 hex digest");
            return;
        }
        const TString got = Sha256Hex(content);
        Log.Trace(TStringBuilder() << "checksum " << contentKey << " expected " << expected << " got " << got);
        if (expected != got) {
            Error(contentKey, TStringBuilder() << "checksum mismatch: expected " << expected << ", got " << got);
        }
    }

    bool ValidateObject(const TString& dir, TMaybe<bool> expectChecksums, TMaybe<bool> expectCompressed) {
        if (Exists(JoinKey(dir, "scheme.pb")) || Exists(JoinKey(dir, "scheme.pb.enc"))) {
            ValidateTable(dir, expectChecksums, expectCompressed, true);
            return true;
        }
        struct TSchemeFile {
            const char* Name;
            void (TValidator::*Check)(const TString& dir, const TString& content, bool expectChecksums);
        };
        const TSchemeFile files[] = {
            {"create_view.sql", &TValidator::CheckView},
            {"create_topic.pb", &TValidator::CheckTopic},
            {"create_async_replication.sql", &TValidator::CheckSql},
            {"create_transfer.sql", &TValidator::CheckSql},
            {"create_external_data_source.sql", &TValidator::CheckSql},
            {"create_external_table.sql", &TValidator::CheckSql},
            {"system_view.pb", &TValidator::CheckSysView},
        };
        for (const auto& file : files) {
            const TString key = JoinKey(dir, file.Name);
            if (Exists(key) || Exists(key + ".enc")) {
                ValidateSchemeObject(dir, file.Name, expectChecksums, file.Check);
                return true;
            }
        }
        return false;
    }

    void ValidateSchemeObject(
        const TString& dir,
        const char* fileName,
        TMaybe<bool> expectChecksums,
        void (TValidator::*check)(const TString& dir, const TString& content, bool expectChecksums))
    {
        if (!TryVisit(dir)) {
            return;
        }
        AllowDir(dir);
        Log.Phase(TStringBuilder() << "check " << ShownPath(dir));
        Log.Object(TStringBuilder() << ShownPath(dir) << ": checking metadata");
        const bool checksums = ResolveChecksums(dir, expectChecksums);
        const TString key = JoinKey(dir, fileName);
        auto content = ReadPlainFile(key);
        if (content) {
            (this->*check)(dir, *content, checksums);
            if (!Stopped()) {
                VerifyChecksum(key, *content, checksums);
            }
        }
        // A missing or unreadable schema file does not make metadata irrelevant.
        if (Stopped()) {
            return;
        }
        ValidateObjectMetadata(dir, checksums, /*table*/ false);
        Checked(dir.empty() ? fileName : dir);
    }

    void CheckView(const TString& dir, const TString& content, bool) {
        if (!StripString(content)) {
            Error(JoinKey(dir, "create_view.sql"), "view query is empty");
        }
    }

    void CheckSql(const TString& dir, const TString& content, bool) {
        const TString text = StripString(content);
        if (!text) {
            Error(dir, "schema file is empty");
            return;
        }
        TString lowered = text;
        for (char& ch : lowered) {
            ch = AsciiToLower(ch);
        }
        if (lowered.find("create") == TString::npos) {
            Error(dir, "schema file does not contain a CREATE statement");
        }
    }

    void CheckTopic(const TString& dir, const TString& content, bool) {
        Ydb::Topic::CreateTopicRequest request;
        const TString key = JoinKey(dir, "create_topic.pb");
        if (!::NYdb::NBackup::ParseProto(content, request)) {
            Error(key, "cannot parse create_topic.pb");
            return;
        }
        if (!request.path() && !request.has_partitioning_settings() && !request.has_retention_period()) {
            Error(key, "topic description has no path, partitioning settings, or retention");
        }
    }

    void CheckSysView(const TString& dir, const TString& content, bool) {
        Ydb::Table::DescribeSystemViewResult result;
        const TString key = JoinKey(dir, "system_view.pb");
        if (!::NYdb::NBackup::ParseProto(content, result)) {
            Error(key, "cannot parse system_view.pb");
            return;
        }
        if (!result.sys_view_name() && result.columns_size() == 0) {
            Error(key, "system view description has no name and no columns");
        }
    }

    bool ResolveChecksums(const TString& dir, TMaybe<bool> parentExpect) const {
        if (parentExpect.Defined()) {
            return *parentExpect;
        }
        const TReadResult metadata = ReadFile(JoinKey(dir, "metadata.json"));
        if (metadata.Status != EReadStatus::Ok) {
            return Exists(JoinKey(dir, "scheme.pb.sha256")) || Exists(JoinKey(dir, "create_view.sql.sha256"));
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(metadata.Content, &json) || !json.IsMap()) {
            return false;
        }
        if (json.Has("version") && IsJsonInteger(json["version"])) {
            return JsonInteger(json["version"]) > 0;
        }
        return Exists(JoinKey(dir, "scheme.pb.sha256"));
    }

    void ValidateObjectMetadata(const TString& dir, bool expectChecksums, bool table) {
        const TString key = JoinKey(dir, "metadata.json");
        const TReadResult content = ReadFile(key);
        if (content.Status == EReadStatus::Failed) {
            Error(key, TStringBuilder() << "failed to read file: " << content.Error);
            return;
        }
        if (content.Status == EReadStatus::Missing) {
            if (MetadataObjectRequired(expectChecksums)) {
                Error(key, "file is missing");
            }
            return;
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(content.Content, &json) || !json.IsMap()) {
            Error(key, "metadata.json is not a JSON object");
            return;
        }
        if (json.Has("kind")) {
            Error(key, "object metadata must not be a backup-level metadata.json");
            return;
        }
        if (json.Has("version") && !IsJsonInteger(json["version"])) {
            Error(key, "metadata version must be an integer");
        }
        if (Stopped()) {
            return;
        }
        if (json.Has("permissions") && !IsJsonInteger(json["permissions"])) {
            Error(key, "metadata permissions flag must be an integer");
        }
        if (Stopped()) {
            return;
        }
        VerifyMetadataChecksum(key, content.Content, expectChecksums);
        if (Stopped()) {
            return;
        }
        const TMaybe<bool> permissions = PermissionsFlag(json);
        ValidatePermissions(dir, permissions, expectChecksums);
        if (Stopped()) {
            return;
        }
        if (table) {
            ValidateChangefeeds(dir, json, expectChecksums);
            if (Stopped()) {
                return;
            }
            ValidateIndexes(dir, json, expectChecksums);
        }
    }

    static TMaybe<bool> PermissionsFlag(const NJson::TJsonValue& json) {
        if (!json.Has("permissions") || !IsJsonInteger(json["permissions"])) {
            return {};
        }
        return JsonInteger(json["permissions"]) != 0;
    }

    void ValidatePermissions(const TString& dir, TMaybe<bool> required, bool expectChecksums) {
        const TString key = JoinKey(dir, "permissions.pb");
        const bool present = Exists(key) || Exists(key + ".enc");
        if (required.Defined() && !*required) {
            if (present) {
                Error(key, "permissions.pb is present while metadata says permissions are disabled");
            }
            return;
        }
        if (required.Defined() && *required && !present) {
            Error(key, "permissions.pb is missing");
            return;
        }
        if (!present) {
            return;
        }
        auto content = ReadPlainFile(key);
        if (!content) {
            return;
        }
        Ydb::Scheme::ModifyPermissionsRequest request;
        if (!::NYdb::NBackup::ParseProto(*content, request)) {
            Error(key, "cannot parse permissions.pb");
            return;
        }
        if (request.actions_size() == 0 && !request.clear_permissions()) {
            Error(key, "permissions.pb has no actions");
        }
        if (Stopped()) {
            return;
        }
        VerifyChecksum(key, *content, expectChecksums);
    }

    void ValidateChangefeeds(const TString& dir, const NJson::TJsonValue& json, bool expectChecksums) {
        if (!json.Has("changefeeds")) {
            return;
        }
        const auto& changefeeds = json["changefeeds"];
        if (!changefeeds.IsArray()) {
            Error(JoinKey(dir, "metadata.json"), "changefeeds must be an array");
            return;
        }
        const auto& changefeedItems = changefeeds.GetArray();
        ParallelFor(changefeedItems.size(), [&](size_t index) {
            const auto& changefeed = changefeedItems[index];
            if (!changefeed.IsMap() || !changefeed["prefix"].IsString() || !changefeed["name"].IsString()
                || !changefeed["prefix"].GetString() || !changefeed["name"].GetString())
            {
                Error(JoinKey(dir, "metadata.json"), "changefeed entry must have prefix and name");
                return;
            }
            const TString prefix = changefeed["prefix"].GetString();
            const TString name = changefeed["name"].GetString();
            const TString cfDir = JoinKey(dir, prefix);
            const TString descKey = JoinKey(cfDir, "changefeed_description.pb");
            auto desc = ReadPlainFile(descKey);
            if (desc) {
                Ydb::Table::ChangefeedDescription proto;
                if (!::NYdb::NBackup::ParseProto(*desc, proto)) {
                    Error(descKey, "cannot parse changefeed_description.pb");
                } else if (proto.name() != name) {
                    Error(descKey, TStringBuilder() << "changefeed name is \"" << proto.name() << "\", metadata says \"" << name << "\"");
                }
                if (!Stopped()) {
                    VerifyChecksum(descKey, *desc, expectChecksums);
                }
            }
            if (Stopped()) {
                return;
            }
            const TString topicKey = JoinKey(cfDir, "topic_description.pb");
            auto topic = ReadPlainFile(topicKey);
            if (topic) {
                Ydb::Topic::DescribeTopicResult proto;
                if (!::NYdb::NBackup::ParseProto(*topic, proto) || proto.ByteSizeLong() == 0) {
                    Error(topicKey, "cannot parse topic_description.pb");
                }
                if (!Stopped()) {
                    VerifyChecksum(topicKey, *topic, expectChecksums);
                }
            }
        });
    }

    void ValidateIndexes(const TString& dir, const NJson::TJsonValue& json, bool expectChecksums) {
        if (!json.Has("indexes")) {
            return;
        }
        const auto& indexes = json["indexes"];
        if (!indexes.IsArray()) {
            Error(JoinKey(dir, "metadata.json"), "indexes must be an array");
            return;
        }
        const auto& indexItems = indexes.GetArray();
        ParallelFor(indexItems.size(), [&](size_t index) {
            const auto& item = indexItems[index];
            if (!item.IsMap() || !item["export_prefix"].IsString() || !item["impl_table_prefix"].IsString()
                || !item["export_prefix"].GetString() || !item["impl_table_prefix"].GetString())
            {
                Error(JoinKey(dir, "metadata.json"), "index entry must have export_prefix and impl_table_prefix");
                return;
            }
            const TString exportPrefix = item["export_prefix"].GetString();
            const TString implPrefix = item["impl_table_prefix"].GetString();
            if (exportPrefix != implPrefix) {
                Warning(JoinKey(dir, "metadata.json"), TStringBuilder()
                    << "index export_prefix \"" << exportPrefix
                    << "\" differs from impl_table_prefix \"" << implPrefix
                    << "\"; they match in an unencrypted backup");
            }
            const TString indexDir = JoinKey(dir, exportPrefix);
            AllowDir(indexDir);
            ValidateTable(indexDir, expectChecksums, Nothing(), false);
        });
    }

    void ValidateTable(const TString& dir, TMaybe<bool> expectChecksums, TMaybe<bool> expectCompressed, bool followIndexes) {
        if (!TryVisit(dir)) {
            return;
        }
        AllowDir(dir);
        if (Stopped()) {
            return;
        }
        Log.Phase(TStringBuilder() << "check " << ShownPath(dir));
        Log.Object(TStringBuilder() << ShownPath(dir) << ": checking metadata");
        const bool checksums = ResolveChecksums(dir, expectChecksums);
        const TString schemeKey = JoinKey(dir, "scheme.pb");
        auto schemeText = ReadPlainFile(schemeKey);
        ui64 partitions = 0;
        if (schemeText) {
            Ydb::Table::CreateTableRequest scheme;
            if (!::NYdb::NBackup::ParseProto(*schemeText, scheme)) {
                Error(schemeKey, "cannot parse scheme.pb");
            } else {
                partitions = ExpectedPartitions(dir, scheme);
                if (!Stopped()) {
                    CheckScheme(schemeKey, scheme);
                }
            }
            if (!Stopped()) {
                VerifyChecksum(schemeKey, *schemeText, checksums);
            }
        }
        if (Stopped()) {
            return;
        }
        if (followIndexes) {
            ValidateObjectMetadata(dir, checksums, true);
        } else {
            const TString metadataKey = JoinKey(dir, "metadata.json");
            const TReadResult metadata = ReadFile(metadataKey);
            if (metadata.Status == EReadStatus::Failed) {
                Error(metadataKey, TStringBuilder() << "failed to read file: " << metadata.Error);
            } else if (metadata.Status == EReadStatus::Ok) {
                VerifyMetadataChecksum(metadataKey, metadata.Content, checksums);
                if (!Stopped()) {
                    NJson::TJsonValue json;
                    if (NJson::ReadJsonTree(metadata.Content, &json) && json.IsMap()) {
                        ValidatePermissions(dir, PermissionsFlag(json), checksums);
                    } else {
                        Error(metadataKey, "metadata.json is not a JSON object");
                    }
                }
            } else if (MetadataObjectRequired(checksums)) {
                Error(metadataKey, "file is missing");
            }
        }
        if (Stopped()) {
            return;
        }
        if (partitions > 0) {
            // Scheme-only reads checksum sidecars here. Full checks queue the files
            // and hash them after every object's metadata, with a progress heartbeat.
            if (Settings.SchemeOnly) {
                Log.Object(TStringBuilder() << ShownPath(dir) << ": checking data");
            }
            CheckDataFiles(dir, partitions, checksums, expectCompressed);
        }
        Checked(dir.empty() ? "scheme.pb" : dir);
    }

    ui64 ExpectedPartitions(const TString& dir, const Ydb::Table::CreateTableRequest& scheme) {
        switch (scheme.partitions_case()) {
            case Ydb::Table::CreateTableRequest::kUniformPartitions:
                if (scheme.uniform_partitions() == 0) {
                    Error(JoinKey(dir, "scheme.pb"), "uniform_partitions must be positive");
                    return 0;
                }
                return scheme.uniform_partitions();
            case Ydb::Table::CreateTableRequest::kPartitionAtKeys:
                return static_cast<ui64>(scheme.partition_at_keys().split_points_size()) + 1;
            default:
                return 1;
        }
    }

    void CheckScheme(const TString& schemeKey, const Ydb::Table::CreateTableRequest& scheme) {
        const bool hasColumns = scheme.columns_size() != 0;
        if (!hasColumns) {
            Error(schemeKey, "scheme has no columns");
        }
        THashSet<TString> columns;
        if (hasColumns) {
            for (const auto& column : scheme.columns()) {
                if (Stopped()) {
                    return;
                }
                if (!column.name()) {
                    Error(schemeKey, "scheme contains a column without a name");
                    continue;
                }
                if (!column.has_type()) {
                    Error(schemeKey, TStringBuilder() << "column \"" << column.name() << "\" has no type");
                }
                columns.insert(column.name());
            }
        }
        if (Stopped()) {
            return;
        }
        if (scheme.primary_key_size() == 0) {
            Error(schemeKey, "scheme has no primary key");
            return;
        }
        // Without a column list, every key would also be reported as missing from that list.
        if (!hasColumns) {
            return;
        }
        for (const auto& key : scheme.primary_key()) {
            if (Stopped()) {
                return;
            }
            if (!columns.contains(key)) {
                Error(schemeKey, TStringBuilder() << "primary key column \"" << key << "\" is not in the column list");
            }
        }
    }

    void CheckDataFiles(const TString& dir, ui64 partitions, bool expectChecksums, TMaybe<bool> expectCompressed) {
        TVector<TDataPart> parts;
        for (const auto& key : Storage.List(dir)) {
            if (ParentKey(key) != dir) {
                continue;
            }
            if (auto part = ParseDataPart(key)) {
                parts.push_back(*part);
            }
        }
        std::sort(parts.begin(), parts.end(), [](const TDataPart& a, const TDataPart& b) {
            return a.Index < b.Index;
        });
        THashSet<ui32> seen;
        TMaybe<bool> compressed;
        TString extension;
        for (const auto& part : parts) {
            if (Stopped()) {
                return;
            }
            if (!seen.insert(part.Index).second) {
                Error(part.Key, TStringBuilder() << "duplicate data file for partition " << part.Index);
            }
            if (part.Index >= partitions) {
                Error(part.Key, TStringBuilder() << "data file partition " << part.Index << " is outside the scheme partition count " << partitions);
            }
            if (part.Encrypted) {
                RejectEncrypted(part.Key);
            }
            if (Stopped()) {
                return;
            }
            const TString partExt = part.PlainName.substr(part.PlainName.rfind('.'));
            if (!compressed.Defined()) {
                compressed = part.Compressed;
                extension = partExt;
            } else {
                if (*compressed != part.Compressed) {
                    Error(part.Key, "data files mix compressed and uncompressed payloads");
                }
                if (extension != partExt) {
                    Error(part.Key, "data files mix csv and parquet payloads");
                }
            }
        }
        if (Stopped()) {
            return;
        }
        if (expectCompressed.Defined() && compressed.Defined() && *expectCompressed != *compressed) {
            Error(dir, *expectCompressed
                ? "backup metadata requests compression, but data files are uncompressed"
                : "backup metadata has no compression, but data files are compressed");
        }
        for (ui64 index = 0; index < partitions; ++index) {
            if (Stopped()) {
                return;
            }
            if (seen.contains(static_cast<ui32>(index))) {
                continue;
            }
            const TString missing = extension
                ? JoinKey(dir, CanonicalDataName(static_cast<ui32>(index), extension))
                : JoinKey(dir, CanonicalDataName(static_cast<ui32>(index), ".csv"));
            Error(missing, TStringBuilder() << "missing data file for partition " << index << " of " << partitions);
        }
        bool verifyChecksums = expectChecksums;
        if (!verifyChecksums) {
            for (const auto& part : parts) {
                if (Exists(JoinKey(dir, part.PlainName) + ".sha256")) {
                    verifyChecksums = true;
                    break;
                }
            }
        }
        if (Stopped()) {
            return;
        }
        if (!Settings.SchemeOnly && !verifyChecksums) {
            Error(dir, "data file checksums are absent; content integrity cannot be verified");
        }
        if (Settings.SchemeOnly) {
            if (!verifyChecksums) {
                return;
            }
            ParallelFor(parts.size(), [&](size_t index) {
                const TDataPart& part = parts[index];
                if (part.Encrypted) {
                    return;
                }
                const TString sidecar = JoinKey(dir, part.PlainName) + ".sha256";
                if (!Exists(sidecar)) {
                    Error(sidecar, "checksum sidecar is missing");
                    return;
                }
                try {
                    const TString expected = ChecksumToken(Storage.Read(sidecar));
                    if (!IsHex(expected)) {
                        Error(sidecar, "checksum sidecar does not contain a SHA-256 hex digest");
                    }
                } catch (const std::exception& ex) {
                    Error(sidecar, TStringBuilder() << "failed to read checksum: " << ex.what());
                }
            });
            return;
        }
        if (!verifyChecksums || Stopped()) {
            return;
        }
        std::lock_guard<std::mutex> lock(Mu);
        for (const auto& part : parts) {
            if (part.Encrypted) {
                continue;
            }
            PendingDataFiles.push_back({
                dir,
                part.Key,
                JoinKey(dir, part.PlainName),
                part.Compressed,
            });
        }
    }

    // Metadata and file layout are already done. Read and hash every queued data file.
    void RunPendingDataChecks() {
        if (Stopped()) {
            return;
        }
        TVector<TPendingDataFile> files;
        {
            std::lock_guard<std::mutex> lock(Mu);
            files.swap(PendingDataFiles);
        }
        if (files.empty()) {
            return;
        }
        Log.Phase(TStringBuilder() << "check " << DataFileCount(files.size()));
        std::atomic<ui64> checked{0};
        std::atomic<ui64> bytesRead{0};
        std::mutex announceMu;
        THashSet<TString> announced;
        TDataProgressTicker ticker(Log, checked, bytesRead, files.size());
        if (Settings.Progress) {
            ticker.Start();
        }
        ParallelFor(files.size(), [&](size_t index) {
            const TPendingDataFile& file = files[index];
            bool firstForDir = false;
            {
                std::lock_guard<std::mutex> lock(announceMu);
                firstForDir = announced.insert(file.Dir).second;
            }
            if (firstForDir) {
                Log.Object(TStringBuilder() << ShownPath(file.Dir) << ": checking data");
            }
            try {
                const TString digest = HashStoredFile(Storage, file.StoredKey, file.Compressed, [&](ui64 n) {
                    bytesRead.fetch_add(n, std::memory_order_relaxed);
                });
                const TString sidecar = file.PlainKey + ".sha256";
                if (!Exists(sidecar)) {
                    Error(sidecar, "checksum sidecar is missing");
                } else {
                    const TString expected = ChecksumToken(Storage.Read(sidecar));
                    if (!IsHex(expected)) {
                        Error(sidecar, "checksum sidecar does not contain a SHA-256 hex digest");
                    } else if (expected != digest) {
                        Error(file.StoredKey, TStringBuilder()
                            << "checksum mismatch: expected " << expected << ", got " << digest);
                    }
                }
            } catch (const std::exception& ex) {
                Error(file.StoredKey, TStringBuilder() << "failed to read data file: " << ex.what());
            }
            checked.fetch_add(1, std::memory_order_relaxed);
        });
        ticker.Finish();
    }

    void ValidateFullBackup(const TString& root, const TString& metadataKey, const TString& metadataText, const NJson::TJsonValue& json) {
        Log.Phase("check backup metadata");
        const TString checksumAlgo = JsonString(json, "checksum");
        if (json.Has("checksum") && checksumAlgo != "sha256") {
            Error(metadataKey, TStringBuilder() << "unsupported checksum algorithm \"" << checksumAlgo << "\"");
        }
        if (Stopped()) {
            return;
        }
        if (JsonString(json, "encryption")) {
            RejectEncrypted(metadataKey);
        }
        const bool checksums = checksumAlgo == "sha256";
        if (!Stopped()) {
            VerifyMetadataChecksum(metadataKey, metadataText, checksums);
        }
        const bool compressed = !JsonString(json, "compression").empty();
        const TMaybe<bool> expectCompressed = compressed;

        if (Stopped()) {
            return;
        }
        Log.Phase("check schema mapping");
        const TString mappingMetaKey = JoinKey(root, "SchemaMapping/metadata.json");
        auto mappingMeta = ReadPlainFile(mappingMetaKey);
        if (mappingMeta) {
            NJson::TJsonValue mappingMetaJson;
            if (!NJson::ReadJsonTree(*mappingMeta, &mappingMetaJson) || mappingMetaJson["kind"].GetStringRobust() != "SchemaMappingV0") {
                Error(mappingMetaKey, "schema mapping metadata kind must be SchemaMappingV0");
            }
            if (!Stopped()) {
                VerifyMetadataChecksum(mappingMetaKey, *mappingMeta, checksums);
            }
        }
        if (Stopped()) {
            return;
        }

        const TString mappingKey = JoinKey(root, "SchemaMapping/mapping.json");
        auto mappingText = ReadPlainFile(mappingKey);
        if (!mappingText) {
            return;
        }
        NJson::TJsonValue mapping;
        if (!NJson::ReadJsonTree(*mappingText, &mapping) || !mapping["exportedObjects"].IsMap()) {
            Error(mappingKey, "SchemaMapping/mapping.json must contain an exportedObjects object");
            return;
        }
        VerifyMetadataChecksum(mappingKey, *mappingText, checksums);
        if (mapping["exportedObjects"].GetMap().empty()) {
            Error(mappingKey, "exportedObjects is empty");
        }
        if (Stopped()) {
            return;
        }
        struct TMappedObject {
            TString Source;
            TString Prefix;
        };
        TVector<TMappedObject> objects;
        for (const auto& [source, info] : mapping["exportedObjects"].GetMap()) {
            if (!info.IsMap() || !info["exportPrefix"].IsString() || !info["exportPrefix"].GetString()) {
                Error(mappingKey, TStringBuilder() << "object \"" << source << "\" has no exportPrefix");
                if (Stopped()) {
                    return;
                }
                continue;
            }
            objects.push_back({TString(source), info["exportPrefix"].GetString()});
        }
        std::sort(objects.begin(), objects.end(), [](const TMappedObject& a, const TMappedObject& b) {
            return a.Prefix < b.Prefix;
        });
        Log.Phase(TStringBuilder() << "check " << objects.size() << " schema object"
            << (objects.size() == 1 ? "" : "s"));
        ParallelFor(objects.size(), [&](size_t index) {
            const TMappedObject& object = objects[index];
            const TString objectDir = JoinKey(root, object.Prefix);
            if (!ValidateObject(objectDir, checksums, expectCompressed)) {
                Error(objectDir, TStringBuilder() << "schema object \"" << object.Source << "\" has no recognized schema file");
            }
        });
        if (Stopped()) {
            return;
        }
        Log.Phase("check unmapped schema files");
        CheckUnexpectedObjects(root);
    }

    void CheckExpectedObjects(const TString& root) {
        THashSet<TString> expected;
        for (const TString& name : *Settings.ExpectedObjects) {
            expected.insert(CanonicalObjectName(name));
        }
        THashSet<TString> found;
        for (const auto& key : Storage.List(root)) {
            if (!IsSchemaObjectFileName(FileName(key))) {
                continue;
            }
            found.insert(ObjectName(root, ParentKey(key)));
        }
        TVector<TString> extras;
        for (const TString& name : found) {
            if (!CoveredByExpected(name, expected)) {
                extras.push_back(name);
            }
        }
        std::sort(extras.begin(), extras.end());
        for (const TString& name : extras) {
            Warning(name, "object is not listed in --expected-objects");
        }
        TVector<TString> missing;
        for (const TString& name : expected) {
            if (!found.contains(name)) {
                missing.push_back(name);
            }
        }
        std::sort(missing.begin(), missing.end());
        for (const TString& name : missing) {
            if (Stopped()) {
                return;
            }
            Error(name, "object listed in --expected-objects was not found in the backup");
        }
    }

    bool ValidateDiscoveredObjects(const TString& root) {
        Log.Phase(TStringBuilder() << "list schema objects in " << ShownPath(root));
        TVector<TString> dirs;
        for (const auto& key : Storage.List(root)) {
            if (!IsSchemaObjectFileName(FileName(key))) {
                continue;
            }
            const TString dir = ParentKey(key);
            if (!WasVisited(dir)) {
                dirs.push_back(dir);
            }
        }
        std::sort(dirs.begin(), dirs.end());
        dirs.erase(std::unique(dirs.begin(), dirs.end()), dirs.end());
        if (!dirs.empty()) {
            Log.Phase(TStringBuilder() << "check " << dirs.size() << " schema object"
                << (dirs.size() == 1 ? "" : "s"));
        }
        std::atomic<bool> found{false};
        ParallelFor(dirs.size(), [&](size_t index) {
            if (WasVisited(dirs[index])) {
                return;
            }
            if (ValidateObject(dirs[index], /*expectChecksums*/ Nothing(), /*expectCompressed*/ Nothing())) {
                found.store(true);
            }
        });
        return found.load();
    }

    void CheckUnexpectedObjects(const TString& root) {
        TVector<TString> unexpected;
        for (const auto& key : Storage.List(root)) {
            if (!IsSchemaObjectFileName(FileName(key))) {
                continue;
            }
            if (!IsAllowed(ParentKey(key))) {
                unexpected.push_back(key);
            }
        }
        std::sort(unexpected.begin(), unexpected.end());
        for (const TString& key : unexpected) {
            if (Stopped()) {
                return;
            }
            Error(key, "file is not listed in SchemaMapping/mapping.json");
        }
    }
};

} // namespace

ui64 DefaultValidateThreads() {
    const unsigned processors = std::thread::hardware_concurrency();
    return processors > 1 ? static_cast<ui64>(processors) - 1 : 1;
}

TDuration ValidateRetryBackoff(ui32 failedAttempt) {
    if (failedAttempt == 0) {
        return TDuration::Zero();
    }
    ui64 ms = 100;
    for (ui32 i = 1; i < failedAttempt; ++i) {
        if (ms >= 2000) {
            return TDuration::MilliSeconds(2000);
        }
        ms *= 2;
    }
    return TDuration::MilliSeconds(std::min<ui64>(ms, 2000));
}

TVector<TString> ParseExpectedObjects(TStringBuf text) {
    TVector<TString> names;
    while (text) {
        const TStringBuf line = text.NextTok('\n');
        TString name = StripString(TString(line));
        if (name) {
            names.push_back(std::move(name));
        }
    }
    return names;
}

TString Sha256Hex(TStringBuf data) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data.data(), data.size());
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &ctx);
    return to_lower(HexEncode(hash, SHA256_DIGEST_LENGTH));
}

TString MakeChecksumSidecar(TStringBuf data, const TString& fileName) {
    return Sha256Hex(data) + " " + fileName + "\n";
}

void NoteValidateIoRetry(ui32 attempt, const std::exception& ex) {
    if (TlValidateLog == nullptr) {
        return;
    }
    TlValidateLog->Trace(TStringBuilder() << "retry " << attempt << ": " << ex.what());
}

TValidationReport ValidateBackup(const IBackupStorage& storage, const TString& path, const TValidateSettings& settings) {
    const TValidateLog log(settings);
    const TLogScope scope(&log);
    const TLoggingStorage logged(storage, log);
    return TValidator(logged, settings, log).Run(path);
}

} // namespace NYdb::NConsoleClient
