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
#include <util/string/ascii.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/hex.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

#include <algorithm>

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

TString HashBuffer(TStringBuf data) {
    return Sha256Hex(data);
}

// SHA-256 of the stored object. Compressed objects are hashed after zstd decompression,
// because export checksums are computed from uncompressed plaintext.
TString HashStoredFile(const IBackupStorage& storage, const TString& key, bool compressed) {
    const TString bytes = storage.Read(key);
    if (!compressed) {
        return HashBuffer(bytes);
    }
    std::unique_ptr<ZSTD_DCtx, decltype(&ZSTD_freeDCtx)> ctx(ZSTD_createDCtx(), &ZSTD_freeDCtx);
    if (!ctx) {
        ythrow yexception() << "failed to create zstd context";
    }
    TString plain;
    char outBuf[1 << 16];
    ZSTD_inBuffer input{bytes.data(), bytes.size(), 0};
    size_t ret = 1;
    while (input.pos < input.size) {
        ZSTD_outBuffer output{outBuf, sizeof(outBuf), 0};
        ret = ZSTD_decompressStream(ctx.get(), &output, &input);
        if (ZSTD_isError(ret)) {
            ythrow yexception() << "zstd: " << ZSTD_getErrorName(ret);
        }
        plain.append(outBuf, output.pos);
    }
    if (ret != 0) {
        ythrow yexception() << "truncated zstd frame";
    }
    return HashBuffer(plain);
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

class TValidator {
public:
    TValidator(const IBackupStorage& storage, const TValidateSettings& settings)
        : Storage(storage)
        , Settings(settings)
    {
    }

    TValidationReport Run(const TString& path) {
        const TString root = NormalizeKey(path);
        const TString metadataKey = JoinKey(root, "metadata.json");
        if (auto metadata = TryRead(metadataKey)) {
            NJson::TJsonValue json;
            if (!NJson::ReadJsonTree(*metadata, &json) || !json.IsMap()) {
                Error(metadataKey, "metadata.json is not a JSON object");
            } else if (json["kind"].GetStringRobust() == "SimpleExportV0") {
                ValidateFullBackup(root, metadataKey, *metadata, json);
                return Report;
            }
        }
        if (!ValidateObject(root, /*expectChecksums*/ Nothing(), /*expectCompressed*/ Nothing())) {
            Error(root ? root : ".", "path is neither a full backup nor a schema object");
        }
        return Report;
    }

private:
    const IBackupStorage& Storage;
    const TValidateSettings& Settings;
    TValidationReport Report;
    THashSet<TString> AllowedObjectDirs;
    THashSet<TString> Visited;

    void Error(const TString& path, const TString& message) {
        Report.Issues.push_back({path, message});
    }

    void Checked(const TString& path) {
        Report.Checked.push_back(path);
    }

    bool Exists(const TString& key) const {
        return Storage.Exists(key);
    }

    TMaybe<TString> TryRead(const TString& key) const {
        if (!Storage.Exists(key)) {
            return {};
        }
        try {
            return Storage.Read(key);
        } catch (const std::exception& ex) {
            return {};
        }
    }

    TString ReadRequired(const TString& key) {
        if (!Storage.Exists(key)) {
            Error(key, "file is missing");
            return {};
        }
        try {
            return Storage.Read(key);
        } catch (const std::exception& ex) {
            Error(key, TStringBuilder() << "failed to read file: " << ex.what());
            return {};
        }
    }

    void RejectEncrypted(const TString& key) {
        if (!Settings.EncryptionKey) {
            Error(key, "backup file is encrypted; pass --encryption-key-file or YDB_ENCRYPTION_KEY");
        } else {
            Error(key, "encrypted backup files are not validated by this command");
        }
    }

    TMaybe<TString> ReadPlainFile(const TString& key) {
        if (Exists(key + ".enc") && !Exists(key)) {
            RejectEncrypted(key + ".enc");
            return {};
        }
        auto content = TryRead(key);
        if (!content) {
            Error(key, "file is missing");
            return {};
        }
        return content;
    }

    void VerifyChecksum(const TString& contentKey, const TString& content, bool required) {
        const TString sidecar = contentKey + ".sha256";
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
        if (!Visited.insert(dir).second) {
            return;
        }
        AllowedObjectDirs.insert(dir);
        const bool checksums = ResolveChecksums(dir, expectChecksums);
        const TString key = JoinKey(dir, fileName);
        auto content = ReadPlainFile(key);
        if (!content) {
            return;
        }
        (this->*check)(dir, *content, checksums);
        VerifyChecksum(key, *content, checksums);
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
        const auto metadata = TryRead(JoinKey(dir, "metadata.json"));
        if (!metadata) {
            return Exists(JoinKey(dir, "scheme.pb.sha256")) || Exists(JoinKey(dir, "create_view.sql.sha256"));
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(*metadata, &json) || !json.IsMap()) {
            return false;
        }
        if (json.Has("version") && IsJsonInteger(json["version"])) {
            return JsonInteger(json["version"]) > 0;
        }
        return Exists(JoinKey(dir, "scheme.pb.sha256"));
    }

    void ValidateObjectMetadata(const TString& dir, bool expectChecksums, bool table) {
        const TString key = JoinKey(dir, "metadata.json");
        auto content = TryRead(key);
        if (!content) {
            if (expectChecksums) {
                Error(key, "file is missing");
            }
            return;
        }
        NJson::TJsonValue json;
        if (!NJson::ReadJsonTree(*content, &json) || !json.IsMap()) {
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
        if (json.Has("permissions") && !IsJsonInteger(json["permissions"])) {
            Error(key, "metadata permissions flag must be an integer");
        }
        VerifyChecksum(key, *content, expectChecksums);
        const TMaybe<bool> permissions = PermissionsFlag(json);
        ValidatePermissions(dir, permissions, expectChecksums);
        if (table) {
            ValidateChangefeeds(dir, json, expectChecksums);
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
        for (const auto& changefeed : changefeeds.GetArray()) {
            if (!changefeed.IsMap() || !changefeed["prefix"].IsString() || !changefeed["name"].IsString()
                || !changefeed["prefix"].GetString() || !changefeed["name"].GetString())
            {
                Error(JoinKey(dir, "metadata.json"), "changefeed entry must have prefix and name");
                continue;
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
                VerifyChecksum(descKey, *desc, expectChecksums);
            }
            const TString topicKey = JoinKey(cfDir, "topic_description.pb");
            auto topic = ReadPlainFile(topicKey);
            if (topic) {
                Ydb::Topic::DescribeTopicResult proto;
                if (!::NYdb::NBackup::ParseProto(*topic, proto) || proto.ByteSizeLong() == 0) {
                    Error(topicKey, "cannot parse topic_description.pb");
                }
                VerifyChecksum(topicKey, *topic, expectChecksums);
            }
        }
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
        for (const auto& index : indexes.GetArray()) {
            if (!index.IsMap() || !index["export_prefix"].IsString() || !index["impl_table_prefix"].IsString()
                || !index["export_prefix"].GetString() || !index["impl_table_prefix"].GetString())
            {
                Error(JoinKey(dir, "metadata.json"), "index entry must have export_prefix and impl_table_prefix");
                continue;
            }
            const TString indexDir = JoinKey(dir, index["export_prefix"].GetString());
            AllowedObjectDirs.insert(indexDir);
            ValidateTable(indexDir, expectChecksums, Nothing(), false);
        }
    }

    void ValidateTable(const TString& dir, TMaybe<bool> expectChecksums, TMaybe<bool> expectCompressed, bool followIndexes) {
        if (!Visited.insert(dir).second) {
            return;
        }
        AllowedObjectDirs.insert(dir);
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
                CheckScheme(schemeKey, scheme);
            }
            VerifyChecksum(schemeKey, *schemeText, checksums);
        }
        if (followIndexes) {
            ValidateObjectMetadata(dir, checksums, true);
        } else {
            const TString metadataKey = JoinKey(dir, "metadata.json");
            if (auto metadata = TryRead(metadataKey)) {
                VerifyChecksum(metadataKey, *metadata, checksums);
                NJson::TJsonValue json;
                if (NJson::ReadJsonTree(*metadata, &json) && json.IsMap()) {
                    ValidatePermissions(dir, PermissionsFlag(json), checksums);
                } else {
                    Error(metadataKey, "metadata.json is not a JSON object");
                }
            } else if (checksums) {
                Error(metadataKey, "file is missing");
            }
        }
        if (partitions > 0) {
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
        if (scheme.columns_size() == 0) {
            Error(schemeKey, "scheme has no columns");
            return;
        }
        if (scheme.primary_key_size() == 0) {
            Error(schemeKey, "scheme has no primary key");
            return;
        }
        THashSet<TString> columns;
        for (const auto& column : scheme.columns()) {
            if (!column.name()) {
                Error(schemeKey, "scheme contains a column without a name");
                continue;
            }
            if (!column.has_type()) {
                Error(schemeKey, TStringBuilder() << "column \"" << column.name() << "\" has no type");
            }
            columns.insert(column.name());
        }
        for (const auto& key : scheme.primary_key()) {
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
            if (!seen.insert(part.Index).second) {
                Error(part.Key, TStringBuilder() << "duplicate data file for partition " << part.Index);
            }
            if (part.Index >= partitions) {
                Error(part.Key, TStringBuilder() << "data file partition " << part.Index << " is outside the scheme partition count " << partitions);
            }
            if (part.Encrypted) {
                RejectEncrypted(part.Key);
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
        if (expectCompressed.Defined() && compressed.Defined() && *expectCompressed != *compressed) {
            Error(dir, *expectCompressed
                ? "backup metadata requests compression, but data files are uncompressed"
                : "backup metadata has no compression, but data files are compressed");
        }
        for (ui64 index = 0; index < partitions; ++index) {
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
        if (!Settings.SchemeOnly && !verifyChecksums) {
            Error(dir, "data file checksums are absent; content integrity cannot be verified");
        }
        for (const auto& part : parts) {
            if (part.Encrypted) {
                continue;
            }
            const TString plainKey = JoinKey(dir, part.PlainName);
            if (Settings.SchemeOnly) {
                if (!verifyChecksums) {
                    continue;
                }
                const TString sidecar = plainKey + ".sha256";
                if (!Exists(sidecar)) {
                    Error(sidecar, "checksum sidecar is missing");
                    continue;
                }
                try {
                    const TString expected = ChecksumToken(Storage.Read(sidecar));
                    if (!IsHex(expected)) {
                        Error(sidecar, "checksum sidecar does not contain a SHA-256 hex digest");
                    }
                } catch (const std::exception& ex) {
                    Error(sidecar, TStringBuilder() << "failed to read checksum: " << ex.what());
                }
                continue;
            }
            if (!verifyChecksums) {
                continue;
            }
            try {
                const TString digest = HashStoredFile(Storage, part.Key, part.Compressed);
                const TString sidecar = plainKey + ".sha256";
                if (!Exists(sidecar)) {
                    Error(sidecar, "checksum sidecar is missing");
                    continue;
                }
                const TString expected = ChecksumToken(Storage.Read(sidecar));
                if (!IsHex(expected)) {
                    Error(sidecar, "checksum sidecar does not contain a SHA-256 hex digest");
                } else if (expected != digest) {
                    Error(part.Key, TStringBuilder() << "checksum mismatch: expected " << expected << ", got " << digest);
                }
            } catch (const std::exception& ex) {
                Error(part.Key, TStringBuilder() << "failed to read data file: " << ex.what());
            }
        }
    }

    void ValidateFullBackup(const TString& root, const TString& metadataKey, const TString& metadataText, const NJson::TJsonValue& json) {
        const TString checksumAlgo = JsonString(json, "checksum");
        if (json.Has("checksum") && checksumAlgo != "sha256") {
            Error(metadataKey, TStringBuilder() << "unsupported checksum algorithm \"" << checksumAlgo << "\"");
        }
        if (JsonString(json, "encryption")) {
            RejectEncrypted(metadataKey);
        }
        const bool checksums = checksumAlgo == "sha256";
        VerifyChecksum(metadataKey, metadataText, checksums);
        const bool compressed = !JsonString(json, "compression").empty();
        const TMaybe<bool> expectCompressed = compressed;

        const TString mappingMetaKey = JoinKey(root, "SchemaMapping/metadata.json");
        auto mappingMeta = ReadPlainFile(mappingMetaKey);
        if (mappingMeta) {
            NJson::TJsonValue mappingMetaJson;
            if (!NJson::ReadJsonTree(*mappingMeta, &mappingMetaJson) || mappingMetaJson["kind"].GetStringRobust() != "SchemaMappingV0") {
                Error(mappingMetaKey, "schema mapping metadata kind must be SchemaMappingV0");
            }
            VerifyChecksum(mappingMetaKey, *mappingMeta, checksums);
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
        VerifyChecksum(mappingKey, *mappingText, checksums);
        if (mapping["exportedObjects"].GetMap().empty()) {
            Error(mappingKey, "exportedObjects is empty");
        }
        for (const auto& [source, info] : mapping["exportedObjects"].GetMap()) {
            if (!info.IsMap() || !info["exportPrefix"].IsString() || !info["exportPrefix"].GetString()) {
                Error(mappingKey, TStringBuilder() << "object \"" << source << "\" has no exportPrefix");
                continue;
            }
            const TString objectDir = JoinKey(root, info["exportPrefix"].GetString());
            if (!ValidateObject(objectDir, checksums, expectCompressed)) {
                Error(objectDir, TStringBuilder() << "schema object \"" << source << "\" has no recognized schema file");
            }
        }
        CheckUnexpectedObjects(root);
        Checked(root.empty() ? "backup" : root);
    }

    void CheckUnexpectedObjects(const TString& root) {
        static constexpr const char* Names[] = {
            "scheme.pb",
            "create_view.sql",
            "create_topic.pb",
            "create_async_replication.sql",
            "create_transfer.sql",
            "create_external_data_source.sql",
            "create_external_table.sql",
            "system_view.pb",
        };
        for (const auto& key : Storage.List(root)) {
            const TString name = FileName(key);
            bool objectFile = false;
            for (const char* expected : Names) {
                if (name == expected) {
                    objectFile = true;
                    break;
                }
            }
            if (!objectFile) {
                continue;
            }
            if (!AllowedObjectDirs.contains(ParentKey(key))) {
                Error(key, "file is not listed in SchemaMapping/mapping.json");
            }
        }
    }
};

} // namespace

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

TValidationReport ValidateBackup(const IBackupStorage& storage, const TString& path, const TValidateSettings& settings) {
    return TValidator(storage, settings).Run(path);
}

} // namespace NYdb::NConsoleClient
