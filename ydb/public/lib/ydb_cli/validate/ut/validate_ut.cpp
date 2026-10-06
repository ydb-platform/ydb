#include <ydb/public/lib/ydb_cli/validate/validate.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace NYdb::NConsoleClient {
namespace {

class TMemoryStorage : public IBackupStorage {
public:
    void Put(const TString& key, const TString& data) {
        std::lock_guard<std::mutex> lock(Mu);
        Files[key] = data;
    }

    void PutChecked(const TString& key, const TString& data) {
        Put(key, data);
        const auto slash = key.rfind('/');
        const TString name = slash == TString::npos ? key : key.substr(slash + 1);
        Put(key + ".sha256", MakeChecksumSidecar(data, name));
    }

    bool Exists(const TString& key) const override {
        std::lock_guard<std::mutex> lock(Mu);
        return Files.contains(key);
    }

    TVector<TString> List(const TString& prefix) const override {
        std::lock_guard<std::mutex> lock(Mu);
        TVector<TString> keys;
        for (const auto& [key, data] : Files) {
            Y_UNUSED(data);
            if (prefix.empty() || key == prefix || key.StartsWith(prefix + "/")) {
                keys.push_back(key);
            }
        }
        return keys;
    }

    TString Read(const TString& key) const override {
        std::lock_guard<std::mutex> lock(Mu);
        const auto it = Files.find(key);
        if (it == Files.end()) {
            ythrow yexception() << "missing " << key;
        }
        return it->second;
    }

    void ReadChunks(
        const TString& key,
        const std::function<void()>& beginAttempt,
        const std::function<void(TStringBuf)>& onChunk) const override
    {
        beginAttempt();
        const TString data = Read(key);
        if (data) {
            onChunk(data);
        }
    }

    THashMap<TString, TString> Files;
    mutable std::mutex Mu;
};

class TSchemeOnlyGuard : public TMemoryStorage {
public:
    TString Read(const TString& key) const override {
        if (key.Contains("data_") && !key.EndsWith(".sha256")) {
            ythrow yexception() << "data file must not be read in scheme-only mode: " << key;
        }
        return TMemoryStorage::Read(key);
    }
};

TString Issues(const TValidationReport& report) {
    TStringBuilder out;
    for (const TValidationIssue& issue : report.Issues) {
        out << issue.Path << ": " << issue.Message << "\n";
    }
    return out;
}

bool HasIssue(const TValidationReport& report, TStringBuf pathPart, TStringBuf messagePart) {
    for (const TValidationIssue& issue : report.Issues) {
        if (issue.Path.Contains(pathPart) && issue.Message.Contains(messagePart)) {
            return true;
        }
    }
    return false;
}

TString Scheme(ui64 partitions, bool withPrimaryKey = true) {
    TStringBuilder text;
    text << "columns {\n  name: \"id\"\n  type { type_id: UINT64 }\n}\n";
    if (withPrimaryKey) {
        text << "primary_key: \"id\"\n";
    }
    if (partitions > 1) {
        text << "uniform_partitions: " << partitions << "\n";
    }
    return text;
}

TString Zstd(TStringBuf plain) {
    TString out;
    out.resize(ZSTD_compressBound(plain.size()));
    const size_t written = ZSTD_compress(out.begin(), out.size(), plain.data(), plain.size(), 1);
    UNIT_ASSERT_C(!ZSTD_isError(written), ZSTD_getErrorName(written));
    out.resize(written);
    return out;
}

TString DataName(ui32 index) {
    return Sprintf("data_%02u.csv", index);
}

TString Key(const TString& dir, const TString& name) {
    return dir ? dir + "/" + name : name;
}

TString TableMetadata(bool checksums, const TString& extra = {}) {
    TStringBuilder json;
    json << "{\"version\":" << (checksums ? 1 : 0) << ",\"permissions\":0,\"changefeeds\":[],\"indexes\":[]";
    if (extra) {
        json << "," << extra;
    }
    json << "}";
    return json;
}

void AddTable(TMemoryStorage& storage, const TString& dir, ui64 partitions, const TString& payload, bool checksums, bool compressed = false) {
    auto put = [&](const TString& key, const TString& data) {
        if (checksums) {
            storage.PutChecked(key, data);
        } else {
            storage.Put(key, data);
        }
    };
    put(Key(dir, "scheme.pb"), Scheme(partitions));
    put(Key(dir, "metadata.json"), TableMetadata(checksums));
    for (ui64 index = 0; index < partitions; ++index) {
        const TString name = DataName(static_cast<ui32>(index));
        const TString key = Key(dir, name);
        if (compressed) {
            storage.Put(key + ".zst", Zstd(payload));
            if (checksums) {
                storage.Put(key + ".sha256", MakeChecksumSidecar(payload, name));
            }
        } else {
            put(key, payload);
        }
    }
}

TValidationReport Run(const IBackupStorage& storage, const TString& path, bool schemeOnly = false, const TString& key = {}) {
    TValidateSettings settings;
    settings.SchemeOnly = schemeOnly;
    settings.EncryptionKey = key;
    return ValidateBackup(storage, path, settings);
}

TValidationReport RunFast(const IBackupStorage& storage, const TString& path, bool schemeOnly = false) {
    TValidateSettings settings;
    settings.SchemeOnly = schemeOnly;
    settings.FailFast = true;
    settings.Threads = 1;
    return ValidateBackup(storage, path, settings);
}

} // namespace

Y_UNIT_TEST_SUITE(ValidateBackup) {

Y_UNIT_TEST(Sha256Empty) {
    UNIT_ASSERT_VALUES_EQUAL(
        Sha256Hex(""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    UNIT_ASSERT_VALUES_EQUAL(
        MakeChecksumSidecar("abc", "data_00.csv"),
        Sha256Hex("abc") + " data_00.csv\n");
}

Y_UNIT_TEST(DataFilesAreHashedInChunks) {
    class TChunkStorage : public TMemoryStorage {
    public:
        TString Read(const TString& key) const override {
            if (key.Contains("data_") && !key.EndsWith(".sha256")) {
                ythrow yexception() << "data file must be streamed: " << key;
            }
            return TMemoryStorage::Read(key);
        }

        void ReadChunks(
            const TString& key,
            const std::function<void()>& beginAttempt,
            const std::function<void(TStringBuf)>& onChunk) const override
        {
            beginAttempt();
            const TString data = TMemoryStorage::Read(key);
            for (size_t offset = 0; offset < data.size(); ++offset) {
                onChunk(TStringBuf(data.data() + offset, 1));
            }
        }
    };

    TChunkStorage plain;
    AddTable(plain, "", 1, "row\n", true);
    UNIT_ASSERT_C(Run(plain, "").Ok(), Issues(Run(plain, "")));

    TChunkStorage compressed;
    AddTable(compressed, "t", 1, "row\n", true, true);
    UNIT_ASSERT_C(Run(compressed, "t").Ok(), Issues(Run(compressed, "t")));
}

Y_UNIT_TEST(DefaultThreadsMatchImportFileCsv) {
    const unsigned processors = std::thread::hardware_concurrency();
    const ui64 expected = processors > 1 ? static_cast<ui64>(processors) - 1 : 1;
    UNIT_ASSERT_VALUES_EQUAL(DefaultValidateThreads(), expected);
}

Y_UNIT_TEST(ThreadsCheckObjectsAndDataFilesConcurrently) {
    class TOverlapStorage : public TMemoryStorage {
    public:
        bool DataFiles = false;
        mutable std::mutex WaitMu;
        mutable std::condition_variable Cv;
        mutable int Active = 0;
        mutable int MaxActive = 0;

        bool Match(const TString& key) const {
            if (DataFiles) {
                return key.Contains("data_") && !key.EndsWith(".sha256");
            }
            return key.EndsWith("/scheme.pb");
        }

        TString Read(const TString& key) const override {
            if (!Match(key)) {
                return TMemoryStorage::Read(key);
            }
            {
                std::unique_lock<std::mutex> lock(WaitMu);
                ++Active;
                if (Active > MaxActive) {
                    MaxActive = Active;
                }
                Cv.notify_all();
                Cv.wait_for(lock, std::chrono::seconds(2), [&] { return Active >= 2; });
            }
            TString data = TMemoryStorage::Read(key);
            {
                std::lock_guard<std::mutex> lock(WaitMu);
                --Active;
            }
            return data;
        }
    };

    TOverlapStorage data;
    data.DataFiles = true;
    AddTable(data, "t", 2, "row\n", true);
    TValidateSettings dataSettings;
    dataSettings.Threads = 2;
    const TValidationReport dataReport = ValidateBackup(data, "t", dataSettings);
    UNIT_ASSERT_C(dataReport.Ok(), Issues(dataReport));
    UNIT_ASSERT_VALUES_EQUAL(data.MaxActive, 2);

    TOverlapStorage metadata;
    AddTable(metadata, "a", 1, "a\n", true);
    AddTable(metadata, "b", 1, "b\n", true);
    TValidateSettings metadataSettings;
    metadataSettings.Threads = 2;
    const TValidationReport metadataReport = ValidateBackup(metadata, "", metadataSettings);
    UNIT_ASSERT_C(metadataReport.Ok(), Issues(metadataReport));
    UNIT_ASSERT_VALUES_EQUAL(metadata.MaxActive, 2);
}

Y_UNIT_TEST(TableFullAndSchemeOnly) {
    TMemoryStorage storage;
    AddTable(storage, "", 2, "row\n", true);

    UNIT_ASSERT_C(Run(storage, "").Ok(), Issues(Run(storage, "")));
    UNIT_ASSERT_C(Run(storage, "", true).Ok(), Issues(Run(storage, "", true)));
}

Y_UNIT_TEST(SchemeOnlyDoesNotReadDataBytes) {
    TSchemeOnlyGuard storage;
    AddTable(storage, "table", 2, "row\n", true);
    const TValidationReport report = Run(storage, "table", true);
    UNIT_ASSERT_C(report.Ok(), Issues(report));
}

Y_UNIT_TEST(MissingAndExtraDataFiles) {
    TMemoryStorage missing;
    AddTable(missing, "t", 2, "row\n", true);
    missing.Files.erase("t/data_01.csv");
    missing.Files.erase("t/data_01.csv.sha256");
    const TValidationReport missingReport = Run(missing, "t");
    UNIT_ASSERT(HasIssue(missingReport, "t/data_01.csv", "missing data file"));
    UNIT_ASSERT(!Run(missing, "t", true).Ok());

    TMemoryStorage extra;
    AddTable(extra, "t", 2, "row\n", true);
    extra.PutChecked("t/data_02.csv", "extra\n");
    const TValidationReport extraReport = Run(extra, "t");
    UNIT_ASSERT(HasIssue(extraReport, "t/data_02.csv", "outside the scheme"));
}

Y_UNIT_TEST(DataChecksumMismatchIsIgnoredInSchemeOnly) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Put("t/data_00.csv.sha256", TString(64, 'a') + " data_00.csv\n");

    UNIT_ASSERT(HasIssue(Run(storage, "t"), "t/data_00.csv", "checksum mismatch"));
    UNIT_ASSERT_C(Run(storage, "t", true).Ok(), Issues(Run(storage, "t", true)));
}

Y_UNIT_TEST(SchemeChecksumMismatchFailsBothModes) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Put("t/scheme.pb.sha256", TString(64, 'b') + " scheme.pb\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t"), "t/scheme.pb", "checksum mismatch"));
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "t/scheme.pb", "checksum mismatch"));
}

Y_UNIT_TEST(FullModeRequiresDataChecksums) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", false);
    TValidateSettings settings;
    settings.MetadataChecksums = EMetadataChecksumMode::Auto;
    UNIT_ASSERT(HasIssue(ValidateBackup(storage, "t", settings), "t", "checksums are absent"));
    settings.SchemeOnly = true;
    const TValidationReport schemeOnly = ValidateBackup(storage, "t", settings);
    UNIT_ASSERT_C(schemeOnly.Ok(), Issues(schemeOnly));
}

Y_UNIT_TEST(MalformedDataChecksumSidecar) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Put("t/data_00.csv.sha256", "not-a-digest\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "t/data_00.csv.sha256", "SHA-256"));
}

Y_UNIT_TEST(PartitionAtKeysMatchesDataFiles) {
    TMemoryStorage storage;
    TString scheme = Scheme(1);
    scheme += "partition_at_keys {\n"
              "  split_points {\n"
              "    type { type_id: UINT64 }\n"
              "    value { uint64_value: 10 }\n"
              "  }\n"
              "}\n";
    storage.PutChecked("scheme.pb", scheme);
    storage.PutChecked("metadata.json", TableMetadata(true));
    storage.PutChecked("data_00.csv", "a\n");
    const TValidationReport one = Run(storage, "");
    UNIT_ASSERT(HasIssue(one, "data_01.csv", "missing data file"));

    storage.PutChecked("data_01.csv", "b\n");
    UNIT_ASSERT_C(Run(storage, "").Ok(), Issues(Run(storage, "")));
}

Y_UNIT_TEST(SchemeMinimalFields) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/scheme.pb", "columns {\n  name: \"id\"\n}\nprimary_key: \"id\"\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "t/scheme.pb", "no type"));

    storage.PutChecked("t/scheme.pb", Scheme(1, false));
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "t/scheme.pb", "no primary key"));

    storage.PutChecked("t/scheme.pb", TStringBuilder() << Scheme(1) << "primary_key: \"missing\"\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "t/scheme.pb", "not in the column list"));
}

Y_UNIT_TEST(CompressedDataIsHashedUncompressed) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true, true);
    UNIT_ASSERT_C(Run(storage, "t").Ok(), Issues(Run(storage, "t")));

    storage.Put("t/data_00.csv.sha256", MakeChecksumSidecar(storage.Files["t/data_00.csv.zst"], "data_00.csv"));
    UNIT_ASSERT(HasIssue(Run(storage, "t"), "t/data_00.csv.zst", "checksum mismatch"));
}

Y_UNIT_TEST(FullBackupMappingAndUnexpectedObject) {
    TMemoryStorage storage;
    storage.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    storage.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    storage.PutChecked("SchemaMapping/mapping.json",
        "{\"exportedObjects\":{\"/t1\":{\"exportPrefix\":\"t1\"},\"/t2\":{\"exportPrefix\":\"t2\"}}}");
    AddTable(storage, "t1", 1, "a\n", true);
    AddTable(storage, "t2", 2, "b\n", true);
    UNIT_ASSERT_C(Run(storage, "").Ok(), Issues(Run(storage, "")));

    storage.PutChecked("t3/scheme.pb", Scheme(1));
    storage.PutChecked("t3/metadata.json", TableMetadata(true));
    storage.PutChecked("t3/data_00.csv", "c\n");
    UNIT_ASSERT(HasIssue(Run(storage, ""), "t3/scheme.pb", "not listed"));
}

Y_UNIT_TEST(SingleTablePathInsideBackup) {
    TMemoryStorage storage;
    storage.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    storage.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    storage.PutChecked("SchemaMapping/mapping.json", "{\"exportedObjects\":{\"/t1\":{\"exportPrefix\":\"t1\"}}}");
    AddTable(storage, "t1", 1, "a\n", true);
    AddTable(storage, "t2", 1, "b\n", true);
    storage.Files["t2/data_00.csv"] = "changed\n";
    UNIT_ASSERT_C(Run(storage, "t1").Ok(), Issues(Run(storage, "t1")));
    UNIT_ASSERT(!Run(storage, "t2").Ok());
}

Y_UNIT_TEST(ChangefeedAndIndex) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json",
        "{\"version\":1,\"permissions\":0,\"changefeeds\":[{\"prefix\":\"updates\",\"name\":\"updates\"}],"
        "\"indexes\":[{\"export_prefix\":\"idx/indexImplTable\",\"impl_table_prefix\":\"idx/indexImplTable\"}]}");
    storage.PutChecked("t/updates/changefeed_description.pb", "name: \"updates\"\n");
    storage.PutChecked("t/updates/topic_description.pb", "self { name: \"updates\" }\n");
    AddTable(storage, "t/idx/indexImplTable", 1, "idx\n", true);
    UNIT_ASSERT_C(Run(storage, "t").Ok(), Issues(Run(storage, "t")));

    storage.Files.erase("t/updates/changefeed_description.pb");
    storage.Files.erase("t/updates/changefeed_description.pb.sha256");
    UNIT_ASSERT(HasIssue(Run(storage, "t"), "changefeed_description.pb", "missing"));
}

Y_UNIT_TEST(ChangefeedNameMismatch) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json",
        "{\"version\":1,\"permissions\":0,\"changefeeds\":[{\"prefix\":\"cf\",\"name\":\"expected\"}],\"indexes\":[]}");
    storage.PutChecked("t/cf/changefeed_description.pb", "name: \"other\"\n");
    storage.PutChecked("t/cf/topic_description.pb", "self { name: \"cf\" }\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "changefeed_description.pb", "metadata says"));
}

Y_UNIT_TEST(PermissionsFlag) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json", "{\"version\":1,\"permissions\":0,\"changefeeds\":[],\"indexes\":[]}");
    storage.PutChecked("t/permissions.pb", "clear_permissions: true\n");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "permissions.pb", "disabled"));

    storage.PutChecked("t/metadata.json", "{\"version\":1,\"permissions\":1,\"changefeeds\":[],\"indexes\":[]}");
    storage.Files.erase("t/permissions.pb");
    storage.Files.erase("t/permissions.pb.sha256");
    UNIT_ASSERT(HasIssue(Run(storage, "t", true), "permissions.pb", "missing"));

    storage.PutChecked("t/permissions.pb", "clear_permissions: true\n");
    UNIT_ASSERT_C(Run(storage, "t", true).Ok(), Issues(Run(storage, "t", true)));
}

Y_UNIT_TEST(ViewTopicAndSql) {
    TMemoryStorage view;
    view.PutChecked("create_view.sql", "CREATE VIEW v AS SELECT 1");
    view.PutChecked("metadata.json", TableMetadata(true));
    UNIT_ASSERT_C(Run(view, "").Ok(), Issues(Run(view, "")));

    view.PutChecked("create_view.sql", "select 1");
    UNIT_ASSERT_C(Run(view, "", true).Ok(), Issues(Run(view, "", true)));

    TMemoryStorage emptyView;
    emptyView.PutChecked("create_view.sql", "  \n");
    emptyView.PutChecked("metadata.json", TableMetadata(true));
    UNIT_ASSERT(HasIssue(Run(emptyView, "", true), "create_view.sql", "empty"));

    TMemoryStorage topic;
    topic.PutChecked("create_topic.pb", "path: \"/db/topic\"\n");
    topic.PutChecked("metadata.json", TableMetadata(true));
    UNIT_ASSERT_C(Run(topic, "").Ok(), Issues(Run(topic, "")));

    TMemoryStorage sql;
    sql.PutChecked("create_external_table.sql", "select 1");
    sql.PutChecked("metadata.json", TableMetadata(true));
    UNIT_ASSERT(HasIssue(Run(sql, "", true), "", "CREATE"));
}

Y_UNIT_TEST(EncryptedFilesAreRejected) {
    TMemoryStorage storage;
    storage.Put("scheme.pb.enc", "cipher");
    const TValidationReport noKey = Run(storage, "");
    UNIT_ASSERT(HasIssue(noKey, "scheme.pb.enc", "not validated"));

    const TValidationReport withKey = Run(storage, "", false, "key-bytes");
    UNIT_ASSERT(HasIssue(withKey, "scheme.pb.enc", "not validated"));
}

Y_UNIT_TEST(OldItemExportWithoutSchemaMapping) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 2, "a\n", true);
    AddTable(storage, "dir/t2", 1, "b\n", true);
    UNIT_ASSERT_C(Run(storage, "dir").Ok(), Issues(Run(storage, "dir")));

    storage.Files["dir/t2/data_00.csv"] = "changed\n";
    UNIT_ASSERT_C(Run(storage, "dir/t1").Ok(), Issues(Run(storage, "dir/t1")));
    UNIT_ASSERT(HasIssue(Run(storage, "dir"), "dir/t2/data_00.csv", "checksum mismatch"));
}

Y_UNIT_TEST(IndexFilesWithoutMetadataEntry) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json", "{\"version\":1,\"permissions\":0}");
    AddTable(storage, "t/idx/indexImplTable", 1, "idx\n", true);
    UNIT_ASSERT_C(Run(storage, "t").Ok(), Issues(Run(storage, "t")));

    storage.Files["t/idx/indexImplTable/data_00.csv"] = "bad\n";
    UNIT_ASSERT(HasIssue(Run(storage, "t"), "t/idx/indexImplTable/data_00.csv", "checksum mismatch"));
}

TValidationReport RunExpected(const IBackupStorage& storage, const TString& path, std::initializer_list<TString> names) {
    TValidateSettings settings;
    settings.ExpectedObjects = TVector<TString>(names);
    return ValidateBackup(storage, path, settings);
}

bool HasWarning(const TValidationReport& report, TStringBuf pathPart, TStringBuf messagePart) {
    for (const TValidationIssue& issue : report.Warnings) {
        if (issue.Path.Contains(pathPart) && issue.Message.Contains(messagePart)) {
            return true;
        }
    }
    return false;
}

Y_UNIT_TEST(ParseExpectedObjectsSkipsEmptyLines) {
    const TVector<TString> names = ParseExpectedObjects("dir/t1\n\n  \n./dir/t2/ \n");
    UNIT_ASSERT_VALUES_EQUAL(names.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(names[0], "dir/t1");
    UNIT_ASSERT_VALUES_EQUAL(names[1], "./dir/t2/");
}

Y_UNIT_TEST(ExpectedObjectsMatch) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 1, "a\n", true);
    AddTable(storage, "dir/t2", 1, "b\n", true);
    const TValidationReport report = RunExpected(storage, "dir", {"t1", "./t2/"});
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(report.Warnings.empty());
}

Y_UNIT_TEST(ExpectedObjectsExtraIsWarning) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 1, "a\n", true);
    AddTable(storage, "dir/t2", 1, "b\n", true);
    const TValidationReport report = RunExpected(storage, "dir", {"t1"});
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(HasWarning(report, "t2", "not listed"));
    UNIT_ASSERT(!HasWarning(report, "t1", "not listed"));
}

Y_UNIT_TEST(ExpectedObjectsMissingIsError) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 1, "a\n", true);
    const TValidationReport report = RunExpected(storage, "dir", {"t1", "t2"});
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(HasIssue(report, "t2", "was not found"));
    UNIT_ASSERT(report.Warnings.empty());
}

Y_UNIT_TEST(ExpectedObjectsCoversNestedIndex) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json", "{\"version\":1,\"permissions\":0}");
    AddTable(storage, "t/idx/indexImplTable", 1, "idx\n", true);
    AddTable(storage, "other", 1, "x\n", true);
    const TValidationReport report = RunExpected(storage, "", {"t"});
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(HasWarning(report, "other", "not listed"));
    UNIT_ASSERT(!HasWarning(report, "idx", "not listed"));
}

Y_UNIT_TEST(ExpectedObjectsDoNotApplyToSchemaMapping) {
    TMemoryStorage storage;
    storage.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    storage.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    storage.PutChecked("SchemaMapping/mapping.json", "{\"exportedObjects\":{\"/t\":{\"exportPrefix\":\"t\"}}}");
    AddTable(storage, "t", 1, "a\n", true);
    const TValidationReport report = RunExpected(storage, "", {"t"});
    UNIT_ASSERT(HasIssue(report, "metadata.json", "SchemaMapping"));
}

Y_UNIT_TEST(UnknownPath) {
    TMemoryStorage storage;
    storage.Put("readme.txt", "hello");
    UNIT_ASSERT(HasIssue(Run(storage, ""), ".", "neither"));
}

Y_UNIT_TEST(ReportsEveryIndependentError) {
    TMemoryStorage tables;
    AddTable(tables, "a", 1, "a\n", true);
    AddTable(tables, "b", 1, "b\n", true);
    tables.Files["a/data_00.csv"] = "changed-a\n";
    tables.Files["b/data_00.csv"] = "changed-b\n";
    const TValidationReport both = Run(tables, "");
    UNIT_ASSERT(HasIssue(both, "a/data_00.csv", "checksum mismatch"));
    UNIT_ASSERT(HasIssue(both, "b/data_00.csv", "checksum mismatch"));

    TMemoryStorage scheme;
    AddTable(scheme, "t", 1, "row\n", true);
    scheme.PutChecked("t/scheme.pb", "columns {\n  name: \"id\"\n}\n");
    const TValidationReport schemeReport = Run(scheme, "t", true);
    UNIT_ASSERT(HasIssue(schemeReport, "t/scheme.pb", "no type"));
    UNIT_ASSERT(HasIssue(schemeReport, "t/scheme.pb", "no primary key"));

    TMemoryStorage missing;
    AddTable(missing, "t", 2, "row\n", true);
    missing.PutChecked("t/metadata.json", TableMetadata(false));
    missing.Files.erase("t/data_00.csv");
    missing.Files.erase("t/data_00.csv.sha256");
    missing.Files.erase("t/data_01.csv");
    missing.Files.erase("t/data_01.csv.sha256");
    const TValidationReport missingReport = Run(missing, "t");
    UNIT_ASSERT(HasIssue(missingReport, "t/data_00.csv", "missing data file"));
    UNIT_ASSERT(HasIssue(missingReport, "t/data_01.csv", "missing data file"));
    UNIT_ASSERT(HasIssue(missingReport, "t", "checksums are absent"));
}

Y_UNIT_TEST(FailFastStopsAtFirstError) {
    // Metadata of later objects is read before any data file. Fail-fast still
    // skips data bytes that follow the first error.
    class TGuard : public TMemoryStorage {
    public:
        void ReadChunks(
            const TString& key,
            const std::function<void()>& beginAttempt,
            const std::function<void(TStringBuf)>& onChunk) const override
        {
            if (key.StartsWith("b/")) {
                ythrow yexception() << "read past the first error: " << key;
            }
            TMemoryStorage::ReadChunks(key, beginAttempt, onChunk);
        }
    };

    TGuard tables;
    AddTable(tables, "a", 1, "a\n", true);
    AddTable(tables, "b", 1, "b\n", true);
    tables.Files["a/data_00.csv"] = "changed-a\n";
    tables.Files["b/data_00.csv"] = "changed-b\n";
    const TValidationReport firstTable = RunFast(tables, "");
    UNIT_ASSERT_VALUES_EQUAL(firstTable.Issues.size(), 1);
    UNIT_ASSERT(HasIssue(firstTable, "a/data_00.csv", "checksum mismatch"));

    TMemoryStorage scheme;
    AddTable(scheme, "t", 1, "row\n", true);
    scheme.PutChecked("t/scheme.pb", "columns {\n  name: \"id\"\n}\n");
    const TValidationReport schemeReport = RunFast(scheme, "t", true);
    UNIT_ASSERT_VALUES_EQUAL(schemeReport.Issues.size(), 1);
    UNIT_ASSERT(HasIssue(schemeReport, "t/scheme.pb", "no type"));

    TMemoryStorage missing;
    AddTable(missing, "t", 2, "row\n", true);
    missing.PutChecked("t/metadata.json", TableMetadata(false));
    missing.Files.erase("t/data_00.csv");
    missing.Files.erase("t/data_00.csv.sha256");
    missing.Files.erase("t/data_01.csv");
    missing.Files.erase("t/data_01.csv.sha256");
    const TValidationReport missingReport = RunFast(missing, "t");
    UNIT_ASSERT_VALUES_EQUAL(missingReport.Issues.size(), 1);
    UNIT_ASSERT(HasIssue(missingReport, "t/data_00.csv", "partition 0"));
    UNIT_ASSERT(!HasIssue(missingReport, "t/data_01.csv", "missing data file"));
    UNIT_ASSERT(!HasIssue(missingReport, "t", "checksums are absent"));
}

Y_UNIT_TEST(FailFastSkipsChecksAfterChangefeedError) {
    class TGuard : public TMemoryStorage {
    public:
        TString Read(const TString& key) const override {
            if (key.Contains("indexImplTable")) {
                ythrow yexception() << "index must not be read after the first error: " << key;
            }
            return TMemoryStorage::Read(key);
        }

        void ReadChunks(
            const TString& key,
            const std::function<void()>& beginAttempt,
            const std::function<void(TStringBuf)>& onChunk) const override
        {
            if (key.Contains("indexImplTable")) {
                ythrow yexception() << "index must not be read after the first error: " << key;
            }
            TMemoryStorage::ReadChunks(key, beginAttempt, onChunk);
        }
    };

    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json",
        "{\"version\":1,\"permissions\":0,\"changefeeds\":[{\"prefix\":\"updates\",\"name\":\"updates\"}],"
        "\"indexes\":[{\"export_prefix\":\"idx/indexImplTable\",\"impl_table_prefix\":\"idx/indexImplTable\"}]}");
    storage.PutChecked("t/updates/topic_description.pb", "self { name: \"updates\" }\n");
    AddTable(storage, "t/idx/indexImplTable", 1, "idx\n", true);
    storage.Files["t/idx/indexImplTable/data_00.csv"] = "changed\n";
    const TValidationReport all = Run(storage, "t");
    UNIT_ASSERT(HasIssue(all, "changefeed_description.pb", "missing"));
    UNIT_ASSERT(HasIssue(all, "t/idx/indexImplTable/data_00.csv", "checksum mismatch"));

    TGuard guarded;
    for (const auto& [key, data] : storage.Files) {
        guarded.Put(key, data);
    }
    const TValidationReport fast = RunFast(guarded, "t");
    UNIT_ASSERT_VALUES_EQUAL(fast.Issues.size(), 1);
    UNIT_ASSERT(HasIssue(fast, "changefeed_description.pb", "missing"));
}

Y_UNIT_TEST(FailFastKeepsWarningsAndStopsOnFirstMissingObject) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 1, "a\n", true);
    AddTable(storage, "dir/extra", 1, "b\n", true);
    TValidateSettings settings;
    settings.ExpectedObjects = TVector<TString>{"t1", "missing-b", "missing-a"};
    const TValidationReport all = ValidateBackup(storage, "dir", settings);
    UNIT_ASSERT(HasWarning(all, "extra", "not listed"));
    UNIT_ASSERT(HasIssue(all, "missing-a", "was not found"));
    UNIT_ASSERT(HasIssue(all, "missing-b", "was not found"));

    settings.FailFast = true;
    settings.Threads = 1;
    const TValidationReport fast = ValidateBackup(storage, "dir", settings);
    UNIT_ASSERT(HasWarning(fast, "extra", "not listed"));
    UNIT_ASSERT_VALUES_EQUAL(fast.Issues.size(), 1);
    UNIT_ASSERT(HasIssue(fast, "missing-a", "was not found"));
}

Y_UNIT_TEST(CompressionFlagMustMatchDataFiles) {
    TMemoryStorage storage;
    storage.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\",\"compression\":\"zstd\"}");
    storage.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    storage.PutChecked("SchemaMapping/mapping.json", "{\"exportedObjects\":{\"/t\":{\"exportPrefix\":\"t\"}}}");
    AddTable(storage, "t", 1, "row\n", true, false);
    UNIT_ASSERT(HasIssue(Run(storage, ""), "t", "requests compression"));
}

void PutFullBackup(TMemoryStorage& storage) {
    storage.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    storage.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    storage.PutChecked("SchemaMapping/mapping.json", "{\"exportedObjects\":{\"/t\":{\"exportPrefix\":\"t\"}}}");
    AddTable(storage, "t", 1, "a\n", true);
}

Y_UNIT_TEST(MissingRootMetadataWithSchemaMappingIsError) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    storage.Files.erase("metadata.json");
    const TValidationReport report = Run(storage, "");
    UNIT_ASSERT(HasIssue(report, "metadata.json", "SchemaMapping"));
    UNIT_ASSERT(HasIssue(report, "metadata.json.sha256", "without metadata.json"));
    UNIT_ASSERT(!report.Ok());
}

void DropFullBackupPlaintext(TMemoryStorage& storage) {
    storage.Files.erase("metadata.json");
    storage.Files.erase("SchemaMapping/mapping.json");
    storage.Files.erase("SchemaMapping/metadata.json");
}

Y_UNIT_TEST(SchemaMappingChecksumSidecarsAreNotItemExport) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    DropFullBackupPlaintext(storage);
    const TValidationReport report = Run(storage, "");
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(HasIssue(report, "metadata.json", "SchemaMapping"));
    UNIT_ASSERT(HasIssue(report, "metadata.json.sha256", "without metadata.json"));
    UNIT_ASSERT(!HasWarning(report, "", "completeness is not checked"));

    TValidateSettings fullSettings;
    fullSettings.Format = EValidateFormat::Full;
    const TValidationReport asFull = ValidateBackup(storage, "", fullSettings);
    UNIT_ASSERT(!asFull.Ok());
    UNIT_ASSERT(HasIssue(asFull, "metadata.json", "missing metadata.json"));
}

Y_UNIT_TEST(SchemaMappingEncryptedRemnantsAreNotItemExport) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    DropFullBackupPlaintext(storage);
    storage.Files.erase("metadata.json.sha256");
    storage.Files.erase("SchemaMapping/mapping.json.sha256");
    storage.Files.erase("SchemaMapping/metadata.json.sha256");
    storage.Put("SchemaMapping/mapping.json.enc", "cipher");
    storage.Put("SchemaMapping/metadata.json.enc", "cipher");
    const TValidationReport report = Run(storage, "");
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(HasIssue(report, "metadata.json", "SchemaMapping"));
    UNIT_ASSERT(!HasWarning(report, "", "completeness is not checked"));
}

Y_UNIT_TEST(OrphanRootMetadataChecksumIsNotItemExport) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    DropFullBackupPlaintext(storage);
    storage.Files.erase("SchemaMapping/mapping.json.sha256");
    storage.Files.erase("SchemaMapping/metadata.json.sha256");
    const TValidationReport report = Run(storage, "");
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(HasIssue(report, "metadata.json", "missing metadata.json"));
    UNIT_ASSERT(!HasIssue(report, "metadata.json", "SchemaMapping is present"));
    UNIT_ASSERT(HasIssue(report, "metadata.json.sha256", "without metadata.json"));
    UNIT_ASSERT(!HasWarning(report, "", "completeness is not checked"));
}

Y_UNIT_TEST(AnySchemaMappingFileIsFullBackup) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    DropFullBackupPlaintext(storage);
    storage.Files.erase("metadata.json.sha256");
    storage.Files.erase("SchemaMapping/mapping.json.sha256");
    storage.Files.erase("SchemaMapping/metadata.json.sha256");
    storage.Put("SchemaMapping/leftover.txt", "not a marker");
    const TValidationReport report = Run(storage, "");
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(HasIssue(report, "metadata.json", "SchemaMapping"));
    UNIT_ASSERT(!HasWarning(report, "", "completeness is not checked"));
}

Y_UNIT_TEST(UnknownBackupKindIsError) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    storage.PutChecked("metadata.json", "{\"kind\":\"OtherExport\",\"checksum\":\"sha256\"}");
    const TValidationReport withMapping = Run(storage, "");
    UNIT_ASSERT(HasIssue(withMapping, "metadata.json", "unsupported backup kind"));
    UNIT_ASSERT(!withMapping.Ok());

    TMemoryStorage noMapping;
    noMapping.PutChecked("metadata.json", "{\"kind\":\"Nope\"}");
    AddTable(noMapping, "t", 1, "a\n", true);
    const TValidationReport report = Run(noMapping, "");
    UNIT_ASSERT(HasIssue(report, "metadata.json", "unsupported backup kind"));
    UNIT_ASSERT(!report.Ok());
}

Y_UNIT_TEST(RootMetadataReadFailureIsNotAbsence) {
    class TReadFailsStorage : public TMemoryStorage {
    public:
        TString Read(const TString& key) const override {
            if (key == "metadata.json" || key.EndsWith("/metadata.json")) {
                ythrow yexception() << "injected read failure";
            }
            return TMemoryStorage::Read(key);
        }
    };

    TReadFailsStorage storage;
    PutFullBackup(storage);
    const TValidationReport full = Run(storage, "");
    UNIT_ASSERT(HasIssue(full, "metadata.json", "failed to read file"));
    UNIT_ASSERT(HasIssue(full, "metadata.json", "injected read failure"));
    UNIT_ASSERT(!full.Ok());

    TReadFailsStorage table;
    AddTable(table, "t", 1, "row\n", true);
    const TValidationReport object = Run(table, "t");
    UNIT_ASSERT(HasIssue(object, "t/metadata.json", "failed to read file"));
    UNIT_ASSERT(!HasIssue(object, "t/metadata.json", "file is missing"));
    UNIT_ASSERT(!object.Ok());
}

Y_UNIT_TEST(FormatFullAndItem) {
    TMemoryStorage item;
    AddTable(item, "t", 1, "a\n", true);
    TValidateSettings fullSettings;
    fullSettings.Format = EValidateFormat::Full;
    UNIT_ASSERT(HasIssue(ValidateBackup(item, "", fullSettings), "metadata.json", "missing metadata.json"));

    TMemoryStorage full;
    PutFullBackup(full);
    TValidateSettings itemSettings;
    itemSettings.Format = EValidateFormat::Item;
    const TValidationReport asItem = ValidateBackup(full, "", itemSettings);
    UNIT_ASSERT_C(asItem.Ok(), Issues(asItem));
    UNIT_ASSERT(HasWarning(asItem, "metadata.json", "--format=item"));
}

Y_UNIT_TEST(ItemExportWarnsWithoutExpectedObjects) {
    TMemoryStorage storage;
    AddTable(storage, "dir/t1", 1, "a\n", true);
    AddTable(storage, "dir/t2", 1, "b\n", true);
    const TValidationReport report = Run(storage, "dir");
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(HasWarning(report, "dir", "--expected-objects"));

    const TValidationReport one = Run(storage, "dir/t1");
    UNIT_ASSERT_C(one.Ok(), Issues(one));
    UNIT_ASSERT(!HasWarning(one, "dir/t1", "--expected-objects"));
}

Y_UNIT_TEST(UnusedEncryptionKeyIsWarning) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    TValidateSettings settings;
    settings.EncryptionKey = "key-bytes";
    const TValidationReport report = ValidateBackup(storage, "t", settings);
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(HasWarning(report, "t", "unused"));
}

Y_UNIT_TEST(IndexPrefixMismatchIsWarning) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.PutChecked("t/metadata.json",
        "{\"version\":1,\"permissions\":0,\"changefeeds\":[],"
        "\"indexes\":[{\"export_prefix\":\"000\",\"impl_table_prefix\":\"idx/indexImplTable\"}]}");
    AddTable(storage, "t/000", 1, "idx\n", true);
    const TValidationReport report = Run(storage, "t");
    UNIT_ASSERT_C(report.Ok(), Issues(report));
    UNIT_ASSERT(HasWarning(report, "t/metadata.json", "impl_table_prefix"));
}

Y_UNIT_TEST(ParquetDataFileChecksum) {
    TMemoryStorage storage;
    storage.PutChecked("scheme.pb", Scheme(1));
    storage.PutChecked("metadata.json", TableMetadata(true));
    const TString payload = "parquet-bytes";
    storage.Put("data_00.parquet", payload);
    storage.Put("data_00.parquet.sha256", MakeChecksumSidecar(payload, "data_00.parquet"));
    UNIT_ASSERT_C(Run(storage, "").Ok(), Issues(Run(storage, "")));

    storage.Files["data_00.parquet"] = "changed";
    UNIT_ASSERT(HasIssue(Run(storage, ""), "data_00.parquet", "checksum mismatch"));
}

Y_UNIT_TEST(FailFastWithMultipleThreadsReportsError) {
    TMemoryStorage storage;
    AddTable(storage, "a", 1, "a\n", true);
    AddTable(storage, "b", 1, "b\n", true);
    storage.Files["a/data_00.csv"] = "changed-a\n";
    storage.Files["b/data_00.csv"] = "changed-b\n";
    TValidateSettings settings;
    settings.FailFast = true;
    settings.Threads = 2;
    const TValidationReport report = ValidateBackup(storage, "", settings);
    UNIT_ASSERT(!report.Ok());
    UNIT_ASSERT(report.Issues.size() >= 1);
}

Y_UNIT_TEST(WorkerExceptionBecomesIssue) {
    class TThrowExists : public TMemoryStorage {
    public:
        bool Exists(const TString& key) const override {
            if (key.EndsWith("/scheme.pb") && (key.StartsWith("a/") || key.StartsWith("b/"))) {
                ythrow yexception() << "exists boom";
            }
            return TMemoryStorage::Exists(key);
        }
    };

    TThrowExists storage;
    AddTable(storage, "a", 1, "a\n", true);
    AddTable(storage, "b", 1, "b\n", true);
    TValidateSettings settings;
    settings.Threads = 2;
    const TValidationReport report = ValidateBackup(storage, "", settings);
    UNIT_ASSERT(HasIssue(report, ".", "internal error while validating"));
    UNIT_ASSERT(HasIssue(report, ".", "exists boom"));
}

Y_UNIT_TEST(IoFailureOutsideWorkerKeepsReport) {
    class TThrowExists : public TMemoryStorage {
    public:
        bool Exists(const TString& key) const override {
            ythrow yexception() << "exists boom " << key;
        }
    };

    TThrowExists early;
    bool thrown = false;
    TValidationReport earlyReport;
    try {
        earlyReport = Run(early, "");
    } catch (const std::exception&) {
        thrown = true;
    }
    UNIT_ASSERT(!thrown);
    UNIT_ASSERT(HasIssue(earlyReport, ".", "internal error while validating"));
    UNIT_ASSERT(HasIssue(earlyReport, ".", "exists boom"));

    class TThrowDataList : public TMemoryStorage {
    public:
        TVector<TString> List(const TString& prefix) const override {
            if (prefix.Contains("SchemaMapping")) {
                return {};
            }
            ythrow yexception() << "list boom " << prefix;
        }
    };

    TThrowDataList storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Put("t/scheme.pb.sha256", TString(64, 'b') + " scheme.pb\n");
    thrown = false;
    TValidationReport report;
    try {
        report = Run(storage, "t");
    } catch (const std::exception&) {
        thrown = true;
    }
    UNIT_ASSERT(!thrown);
    UNIT_ASSERT(HasIssue(report, "t/scheme.pb", "checksum mismatch"));
    UNIT_ASSERT(HasIssue(report, "t", "list boom"));
    UNIT_ASSERT(HasIssue(report, "t", "internal error while validating"));
}

TValidationReport RunMetadataChecksums(
    const IBackupStorage& storage,
    const TString& path,
    EMetadataChecksumMode mode,
    bool schemeOnly = false)
{
    TValidateSettings settings;
    settings.MetadataChecksums = mode;
    settings.SchemeOnly = schemeOnly;
    return ValidateBackup(storage, path, settings);
}

Y_UNIT_TEST(MetadataChecksumAlwaysRequiresSidecar) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", false);
    const TValidationReport always = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Always, true);
    UNIT_ASSERT(HasIssue(always, "t/metadata.json.sha256", "checksum sidecar is missing"));
    UNIT_ASSERT(!always.Ok());

    const TValidationReport automatic = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Auto, true);
    UNIT_ASSERT_C(automatic.Ok(), Issues(automatic));

    const TValidationReport ignored = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Ignore, true);
    UNIT_ASSERT_C(ignored.Ok(), Issues(ignored));
}

Y_UNIT_TEST(MetadataChecksumIgnoreSkipsMismatch) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Put("t/metadata.json.sha256", TString(64, 'c') + " metadata.json\n");

    const TValidationReport always = Run(storage, "t");
    UNIT_ASSERT(HasIssue(always, "t/metadata.json", "checksum mismatch"));

    const TValidationReport ignored = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Ignore);
    UNIT_ASSERT_C(ignored.Ok(), Issues(ignored));
    UNIT_ASSERT(!HasIssue(ignored, "t/metadata.json", "checksum mismatch"));
}

Y_UNIT_TEST(MetadataChecksumAlwaysRequiresMetadataFile) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);
    storage.Files.erase("t/metadata.json");
    storage.Files.erase("t/metadata.json.sha256");

    const TValidationReport always = Run(storage, "t");
    UNIT_ASSERT(HasIssue(always, "t/metadata.json", "file is missing"));
    UNIT_ASSERT(!always.Ok());

    const TValidationReport automatic = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Auto);
    UNIT_ASSERT(HasIssue(automatic, "t/metadata.json", "file is missing"));

    const TValidationReport ignored = RunMetadataChecksums(storage, "t", EMetadataChecksumMode::Ignore);
    UNIT_ASSERT_C(ignored.Ok(), Issues(ignored));
}

Y_UNIT_TEST(MetadataChecksumAlwaysRequiresFullBackupSidecars) {
    TMemoryStorage storage;
    PutFullBackup(storage);
    storage.Put("metadata.json", "{\"kind\":\"SimpleExportV0\"}");
    storage.Files.erase("metadata.json.sha256");
    storage.Files.erase("SchemaMapping/metadata.json.sha256");

    const TValidationReport always = Run(storage, "");
    bool rootSidecar = false;
    for (const TValidationIssue& issue : always.Issues) {
        if (issue.Path == "metadata.json.sha256" && issue.Message.Contains("checksum sidecar is missing")) {
            rootSidecar = true;
        }
    }
    UNIT_ASSERT(rootSidecar);
    UNIT_ASSERT(HasIssue(always, "SchemaMapping/metadata.json.sha256", "checksum sidecar is missing"));
    UNIT_ASSERT(!always.Ok());

    const TValidationReport automatic = RunMetadataChecksums(storage, "", EMetadataChecksumMode::Auto);
    UNIT_ASSERT_C(automatic.Ok(), Issues(automatic));
}

Y_UNIT_TEST(RetryValidateIoRetriesThenSucceeds) {
    UNIT_ASSERT_VALUES_EQUAL(ValidateRetryBackoff(1).MilliSeconds(), 100);
    UNIT_ASSERT_VALUES_EQUAL(ValidateRetryBackoff(2).MilliSeconds(), 200);
    UNIT_ASSERT_VALUES_EQUAL(ValidateRetryBackoff(5).MilliSeconds(), 1600);
    UNIT_ASSERT_VALUES_EQUAL(ValidateRetryBackoff(6).MilliSeconds(), 2000);
    UNIT_ASSERT_VALUES_EQUAL(ValidateRetryBackoff(10).MilliSeconds(), 2000);

    ui32 calls = 0;
    ui32 sleeps = 0;
    const bool ok = RetryValidateIo(3, [&] {
        ++calls;
        if (calls < 3) {
            ythrow yexception() << "transient";
        }
        return true;
    }, [&](TDuration) {
        ++sleeps;
    });
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(calls, 3);
    UNIT_ASSERT_VALUES_EQUAL(sleeps, 2);

    ui32 failedCalls = 0;
    bool thrown = false;
    try {
        RetryValidateIo(2, [&] {
            ++failedCalls;
            ythrow yexception() << "still failing";
            return true;
        }, [&](TDuration) {});
    } catch (const yexception& ex) {
        thrown = true;
        UNIT_ASSERT_STRING_CONTAINS(ex.what(), "still failing");
    }
    UNIT_ASSERT(thrown);
    UNIT_ASSERT_VALUES_EQUAL(failedCalls, 2);
}

TVector<TString> ProgressOf(
    const IBackupStorage& storage,
    const TString& path,
    ui32 verbosity,
    bool schemeOnly = false,
    bool failFast = false)
{
    TValidateSettings settings;
    settings.SchemeOnly = schemeOnly;
    settings.FailFast = failFast;
    settings.Threads = 1;
    settings.Verbosity = verbosity;
    TVector<TString> lines;
    settings.Progress = [&lines](TStringBuf line) {
        lines.emplace_back(line);
    };
    ValidateBackup(storage, path, settings);
    return lines;
}

bool ContainsLine(const TVector<TString>& lines, TStringBuf part) {
    for (const TString& line : lines) {
        if (line.Contains(part)) {
            return true;
        }
    }
    return false;
}

size_t LineIndex(const TVector<TString>& lines, TStringBuf exact) {
    for (size_t index = 0; index < lines.size(); ++index) {
        if (lines[index] == exact) {
            return index;
        }
    }
    return lines.size();
}

Y_UNIT_TEST(CheckedCountDoesNotIncludeTheContainingDirectory) {
    TMemoryStorage one;
    AddTable(one, "t", 1, "row\n", true);
    UNIT_ASSERT_VALUES_EQUAL(Run(one, "t").Checked.size(), 1);

    TMemoryStorage item;
    AddTable(item, "db/customer", 1, "a\n", true);
    AddTable(item, "db/customer/idx_customer_name/indexImplTable", 1, "b\n", true);
    const TValidationReport itemReport = Run(item, "db");
    UNIT_ASSERT_C(itemReport.Ok(), Issues(itemReport));
    UNIT_ASSERT_VALUES_EQUAL(itemReport.Checked.size(), 2);

    TMemoryStorage full;
    full.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    full.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    full.PutChecked("SchemaMapping/mapping.json",
        "{\"exportedObjects\":{\"/t1\":{\"exportPrefix\":\"t1\"},\"/t2\":{\"exportPrefix\":\"t2\"}}}");
    AddTable(full, "t1", 1, "a\n", true);
    AddTable(full, "t2", 1, "b\n", true);
    const TValidationReport fullReport = Run(full, "");
    UNIT_ASSERT_C(fullReport.Ok(), Issues(fullReport));
    UNIT_ASSERT_VALUES_EQUAL(fullReport.Checked.size(), 2);
}

Y_UNIT_TEST(ProgressFollowsVerbosity) {
    TMemoryStorage storage;
    AddTable(storage, "t", 1, "row\n", true);

    const TVector<TString> phases = ProgressOf(storage, "t", 0);
    UNIT_ASSERT(ContainsLine(phases, "phase: detect backup format for t"));
    const size_t findObjects = LineIndex(phases, "phase: find schema objects in t");
    const size_t checkTable = LineIndex(phases, "phase: check t");
    const size_t listObjects = LineIndex(phases, "phase: list schema objects in t");
    const size_t dataPhase = LineIndex(phases, "phase: check 1 data file");
    const size_t dataStarted = LineIndex(phases, "progress: checked 0 of 1 data file, 1 remaining, 0 bytes read");
    const size_t dataDone = LineIndex(phases, "progress: checked 1 of 1 data file, 0 remaining, 4 bytes read");
    UNIT_ASSERT(findObjects < checkTable);
    UNIT_ASSERT(checkTable < listObjects);
    UNIT_ASSERT(listObjects < dataPhase);
    UNIT_ASSERT(dataPhase < dataStarted);
    UNIT_ASSERT(dataStarted < dataDone);
    UNIT_ASSERT(!ContainsLine(phases, "phase: exported schema objects"));
    UNIT_ASSERT(!ContainsLine(phases, "object:"));
    UNIT_ASSERT(!ContainsLine(phases, "file:"));
    UNIT_ASSERT(!ContainsLine(phases, "trace:"));
    UNIT_ASSERT(!ContainsLine(phases, "phase: scheme only"));

    const TVector<TString> objects = ProgressOf(storage, "t", 1);
    const size_t metadata = LineIndex(objects, "object: t: checking metadata");
    const size_t objectDataPhase = LineIndex(objects, "phase: check 1 data file");
    const size_t data = LineIndex(objects, "object: t: checking data");
    UNIT_ASSERT(LineIndex(objects, "phase: check t") < metadata);
    UNIT_ASSERT(metadata < objectDataPhase);
    UNIT_ASSERT(objectDataPhase < LineIndex(objects, "progress: checked 0 of 1 data file, 1 remaining, 0 bytes read"));
    UNIT_ASSERT(LineIndex(objects, "progress: checked 0 of 1 data file, 1 remaining, 0 bytes read") < data);
    UNIT_ASSERT(data < LineIndex(objects, "progress: checked 1 of 1 data file, 0 remaining, 4 bytes read"));
    UNIT_ASSERT(!ContainsLine(objects, "file:"));
    UNIT_ASSERT(!ContainsLine(objects, "trace:"));

    const TVector<TString> files = ProgressOf(storage, "t", 2);
    UNIT_ASSERT_LT(LineIndex(files, "file: read t/scheme.pb"), files.size());
    UNIT_ASSERT_LT(LineIndex(files, "file: checksum t/scheme.pb.sha256"), files.size());
    UNIT_ASSERT_LT(LineIndex(files, "file: read t/data_00.csv"), files.size());
    UNIT_ASSERT(!ContainsLine(files, "trace:"));

    const TVector<TString> trace = ProgressOf(storage, "t", 3);
    UNIT_ASSERT(ContainsLine(trace, "trace: exists t/scheme.pb yes"));
    UNIT_ASSERT(ContainsLine(trace, "trace: read t/data_00.csv "));
    UNIT_ASSERT(ContainsLine(trace, "trace: checksum t/scheme.pb expected "));

    const TVector<TString> schemeOnly = ProgressOf(storage, "t", 2, true);
    UNIT_ASSERT(ContainsLine(schemeOnly, "phase: scheme only; data file bytes are not read"));
    UNIT_ASSERT(ContainsLine(schemeOnly, "object: t: checking data"));
    UNIT_ASSERT(!ContainsLine(schemeOnly, "phase: check 1 data file"));
    UNIT_ASSERT(!ContainsLine(schemeOnly, "progress:"));
    UNIT_ASSERT_EQUAL(LineIndex(schemeOnly, "file: read t/data_00.csv"), schemeOnly.size());
    UNIT_ASSERT_LT(LineIndex(schemeOnly, "file: read t/data_00.csv.sha256"), schemeOnly.size());

    TMemoryStorage full;
    full.PutChecked("metadata.json", "{\"kind\":\"SimpleExportV0\",\"checksum\":\"sha256\"}");
    full.PutChecked("SchemaMapping/metadata.json", "{\"kind\":\"SchemaMappingV0\"}");
    full.PutChecked("SchemaMapping/mapping.json",
        "{\"exportedObjects\":{\"/t1\":{\"exportPrefix\":\"t1\"},\"/t2\":{\"exportPrefix\":\"t2\"}}}");
    AddTable(full, "t1", 1, "a\n", true);
    AddTable(full, "t2", 1, "b\n", true);
    const TVector<TString> fullPhases = ProgressOf(full, "", 0);
    const size_t detected = LineIndex(fullPhases, "phase: detect backup format for .");
    const size_t fullBackup = LineIndex(fullPhases, "phase: full backup");
    const size_t backupMeta = LineIndex(fullPhases, "phase: check backup metadata");
    const size_t mapping = LineIndex(fullPhases, "phase: check schema mapping");
    const size_t schemaObjects = LineIndex(fullPhases, "phase: check 2 schema objects");
    const size_t unmapped = LineIndex(fullPhases, "phase: check unmapped schema files");
    const size_t fullData = LineIndex(fullPhases, "phase: check 2 data files");
    UNIT_ASSERT(detected < fullBackup);
    UNIT_ASSERT(fullBackup < backupMeta);
    UNIT_ASSERT(backupMeta < mapping);
    UNIT_ASSERT(mapping < schemaObjects);
    UNIT_ASSERT(schemaObjects < unmapped);
    UNIT_ASSERT(unmapped < fullData);
    UNIT_ASSERT(fullData < LineIndex(fullPhases, "progress: checked 0 of 2 data files, 2 remaining, 0 bytes read"));
    UNIT_ASSERT(LineIndex(fullPhases, "progress: checked 0 of 2 data files, 2 remaining, 0 bytes read")
        < LineIndex(fullPhases, "progress: checked 2 of 2 data files, 0 remaining, 4 bytes read"));

    const TVector<TString> fullObjects = ProgressOf(full, "", 1);
    const size_t t1Meta = LineIndex(fullObjects, "object: t1: checking metadata");
    const size_t t2Meta = LineIndex(fullObjects, "object: t2: checking metadata");
    const size_t t1Data = LineIndex(fullObjects, "object: t1: checking data");
    const size_t t2Data = LineIndex(fullObjects, "object: t2: checking data");
    UNIT_ASSERT(LineIndex(fullObjects, "phase: check t1") < t1Meta);
    UNIT_ASSERT(t1Meta < LineIndex(fullObjects, "phase: check t2"));
    UNIT_ASSERT(LineIndex(fullObjects, "phase: check t2") < t2Meta);
    UNIT_ASSERT(t2Meta < LineIndex(fullObjects, "phase: check unmapped schema files"));
    UNIT_ASSERT(LineIndex(fullObjects, "phase: check unmapped schema files") < LineIndex(fullObjects, "phase: check 2 data files"));
    UNIT_ASSERT(LineIndex(fullObjects, "phase: check 2 data files") < t1Data);
    UNIT_ASSERT(t1Data < t2Data);
    UNIT_ASSERT(t2Data < LineIndex(fullObjects, "progress: checked 2 of 2 data files, 0 remaining, 4 bytes read"));

    TMemoryStorage item;
    AddTable(item, "t1", 1, "a\n", true);
    AddTable(item, "t2", 1, "b\n", true);
    const TVector<TString> itemPhases = ProgressOf(item, "", 0);
    const size_t itemFind = LineIndex(itemPhases, "phase: find schema objects in .");
    const size_t itemList = LineIndex(itemPhases, "phase: list schema objects in .");
    const size_t itemCount = LineIndex(itemPhases, "phase: check 2 schema objects");
    const size_t itemT1 = LineIndex(itemPhases, "phase: check t1");
    const size_t itemT2 = LineIndex(itemPhases, "phase: check t2");
    UNIT_ASSERT(itemFind < itemList);
    UNIT_ASSERT(itemList < itemCount);
    const size_t itemData = LineIndex(itemPhases, "phase: check 2 data files");
    UNIT_ASSERT(itemCount < itemT1);
    UNIT_ASSERT(itemT1 < itemT2);
    UNIT_ASSERT(itemT2 < itemData);
    UNIT_ASSERT(ContainsLine(itemPhases, "progress: checked 2 of 2 data files, 0 remaining, 4 bytes read"));

    TMemoryStorage damaged;
    damaged.Put("SchemaMapping/mapping.json", "{}");
    const TVector<TString> stopped = ProgressOf(damaged, "", 0, false, true);
    UNIT_ASSERT(ContainsLine(stopped, "phase: full backup"));
    UNIT_ASSERT(ContainsLine(stopped, "phase: stopped after the first error"));
}

} // Y_UNIT_TEST_SUITE(ValidateBackup)

} // namespace NYdb::NConsoleClient
