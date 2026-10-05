#include <ydb/public/lib/ydb_cli/validate/validate.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NYdb::NConsoleClient {
namespace {

class TMemoryStorage : public IBackupStorage {
public:
    void Put(const TString& key, const TString& data) {
        Files[key] = data;
    }

    void PutChecked(const TString& key, const TString& data) {
        Put(key, data);
        const auto slash = key.rfind('/');
        const TString name = slash == TString::npos ? key : key.substr(slash + 1);
        Put(key + ".sha256", MakeChecksumSidecar(data, name));
    }

    bool Exists(const TString& key) const override {
        return Files.contains(key);
    }

    TVector<TString> List(const TString& prefix) const override {
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
        const auto it = Files.find(key);
        if (it == Files.end()) {
            ythrow yexception() << "missing " << key;
        }
        return it->second;
    }

    void ReadChunks(const TString& key, const std::function<void(TStringBuf)>& onChunk) const override {
        const TString data = Read(key);
        if (data) {
            onChunk(data);
        }
    }

    THashMap<TString, TString> Files;
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
    UNIT_ASSERT(HasIssue(Run(storage, "t"), "t", "checksums are absent"));
    UNIT_ASSERT_C(Run(storage, "t", true).Ok(), Issues(Run(storage, "t", true)));
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
    UNIT_ASSERT(HasIssue(noKey, "scheme.pb.enc", "--encryption-key-file"));

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
    missing.Put("t/metadata.json", TableMetadata(false));
    missing.Files.erase("t/metadata.json.sha256");
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
    class TGuard : public TMemoryStorage {
    public:
        bool Armed = false;

        TString Read(const TString& key) const override {
            if (Armed && key.StartsWith("b/")) {
                ythrow yexception() << "read past the first error: " << key;
            }
            return TMemoryStorage::Read(key);
        }

        void ReadChunks(const TString& key, const std::function<void(TStringBuf)>& onChunk) const override {
            if (Armed && key.StartsWith("b/")) {
                ythrow yexception() << "read past the first error: " << key;
            }
            TMemoryStorage::ReadChunks(key, onChunk);
        }
    };

    TGuard tables;
    AddTable(tables, "a", 1, "a\n", true);
    AddTable(tables, "b", 1, "b\n", true);
    tables.Files["a/data_00.csv"] = "changed-a\n";
    tables.Files["b/data_00.csv"] = "changed-b\n";
    tables.Armed = true;
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
    missing.Put("t/metadata.json", TableMetadata(false));
    missing.Files.erase("t/metadata.json.sha256");
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

        void ReadChunks(const TString& key, const std::function<void(TStringBuf)>& onChunk) const override {
            if (key.Contains("indexImplTable")) {
                ythrow yexception() << "index must not be read after the first error: " << key;
            }
            TMemoryStorage::ReadChunks(key, onChunk);
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

} // Y_UNIT_TEST_SUITE(ValidateBackup)

} // namespace NYdb::NConsoleClient
