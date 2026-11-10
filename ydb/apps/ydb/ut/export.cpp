#include "mock_env.h"

#include <ydb/public/api/grpc/ydb_export_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <type_traits>

const TString TEST_BUCKET = "test-bucket";
const TString TEST_S3_ENDPOINT = "https://ydb.s3.local";
const ui32 DEFAULT_RETRIES = 10;

class TExportImpl : public TMockGrpcServiceBase<Ydb::Export::V1::ExportService::Service> {
public:
    grpc::Status ExportToS3(grpc::ServerContext* context, const Ydb::Export::ExportToS3Request* request, Ydb::Export::ExportToS3Response* response) override {
        Y_UNUSED(context);
        ++ExportCalls;
        CheckS3Params(request);
        CheckItems(request);
        auto status = FillResponse<Ydb::Export::ExportToS3Result>(response, "s3");
        return status;
    }

    grpc::Status ExportToFs(grpc::ServerContext*, const Ydb::Export::ExportToFsRequest* request, Ydb::Export::ExportToFsResponse* response) override {
        ++ExportCalls;
        CHECK_EXP(request->settings().base_path() == CommonDstPrefix, "Incorrect FS base path");
        CheckItems(request);
        return FillResponse<Ydb::Export::ExportToFsResult>(response, "fs");
    }

    void CheckS3Params(const Ydb::Export::ExportToS3Request* request) {
        const auto& settings = request->settings();
        CHECK_EXP(settings.bucket() == Bucket, "Incorrect bucket: \"" << settings.bucket() << "\" instead of \"" << Bucket << "\"");
        CHECK_EXP(settings.endpoint() == S3Endpoint, "Incorrect S3 endpoint: \"" << settings.endpoint() << "\" instead of \"" << S3Endpoint << "\"");
        CHECK_EXP(settings.scheme() == Scheme, "Incorrect scheme: \"" << Ydb::Export::ExportToS3Settings::Scheme_Name(settings.scheme()) << "\" instead of \"" << Ydb::Export::ExportToS3Settings::Scheme_Name(Scheme) << "\"");
        CHECK_EXP(settings.access_key() == S3AccessKey, "Incorrect S3 access key: \"" << settings.access_key() << "\" instead of \"" << S3AccessKey << "\"");
        CHECK_EXP(settings.secret_key() == S3SecretKey, "Incorrect S3 secret key: \"" << settings.secret_key() << "\" instead of \"" << S3SecretKey << "\"");
        CHECK_EXP(settings.description() == Description, "Incorrect description: \"" << settings.description() << "\" instead of \"" << Description << "\"");
        CHECK_EXP(settings.number_of_retries() == NumberOfRetries, "Incorrect number of retries: " << settings.number_of_retries() << " instead of " << NumberOfRetries);
        CHECK_EXP(settings.storage_class() == StorageClass, "Incorrect storage class: \"" << Ydb::Export::ExportToS3Settings::StorageClass_Name(settings.storage_class()) << "\" instead of \"" << Ydb::Export::ExportToS3Settings::StorageClass_Name(StorageClass) << "\"");
        CHECK_EXP(settings.compression() == Compression, "Incorrect compression: \"" << settings.compression() << "\" instead of \"" << Compression << "\"");
        CHECK_EXP(settings.region() == Region, "Incorrect region: \"" << settings.region() << "\" instead of \"" << Region << "\"");
        CHECK_EXP(settings.disable_virtual_addressing() == DisableVirtualAddressing, "Incorrect disable virtual addressing: " << settings.disable_virtual_addressing() << " instead of " << DisableVirtualAddressing);
        CHECK_EXP(settings.source_path() == CommonSourcePrefix, "Incorrect common source prefix: \"" << settings.source_path() << "\" instead of \"" << CommonSourcePrefix << "\"");
        CHECK_EXP(settings.destination_prefix() == CommonDstPrefix, "Incorrect common destination prefix: \"" << settings.destination_prefix() << "\" instead of \"" << CommonDstPrefix << "\"");
        CHECK_EXP(settings.include_index_data() == IncludeIndexData, "Incorrect include index data: " << settings.include_index_data() << " instead of " << IncludeIndexData);
        CheckEncryptionSettings(settings);
        CheckExcludeRegexps(settings);
    }

    template <typename TRequest>
    void CheckItems(const TRequest* request) {
        const auto& settings = request->settings();
        CHECK_EXP(settings.items_size() == static_cast<int>(Items.size()),
            "Items size does not match. settings.items_size(): " << settings.items_size() << ". ExpectedItems.size(): " << Items.size());
        for (int i = 0; i < settings.items_size() && i < static_cast<int>(Items.size()); ++i) {
            CHECK_EXP(settings.items(i).source_path() == Items[i].first,
                "Incorrect item source path at " << i << ": \"" << settings.items(i).source_path() << "\" instead of \"" << Items[i].first << "\"");
            const auto& destination = [&]() -> const auto& {
                if constexpr (std::is_same_v<TRequest, Ydb::Export::ExportToS3Request>) {
                    return settings.items(i).destination_prefix();
                } else {
                    return settings.items(i).destination_path();
                }
            }();
            CHECK_EXP(destination == Items[i].second,
                "Incorrect item destination at " << i << ": \"" << destination << "\" instead of \"" << Items[i].second << "\"");
        }
    }

    void CheckEncryptionSettings(const Ydb::Export::ExportToS3Settings& settings) {
        const auto& encryptionSettings = settings.encryption_settings();
        CHECK_EXP(encryptionSettings.encryption_algorithm() == EncryptionAlgorithm,
            "Incorrect encryption algorithm: \"" << encryptionSettings.encryption_algorithm() << "\" instead of \"" << EncryptionAlgorithm << "\"");
        CHECK_EXP(encryptionSettings.symmetric_key().key() == SymmetricKey,
            "Incorrect symmetric encryption key: \"" << encryptionSettings.symmetric_key().key() << "\" instead of \"" << SymmetricKey << "\"");
    }

    void CheckExcludeRegexps(const Ydb::Export::ExportToS3Settings& settings) {
        CHECK_EXP(settings.exclude_regexps_size() == static_cast<int>(ExcludeRegexps.size()),
            "Exclude regexps size does not match. settings.exclude_regexps_size(): " << settings.exclude_regexps_size() << ". ExcludeRegexps.size(): " << ExcludeRegexps.size());
        for (int i = 0; i < settings.exclude_regexps_size() && i < static_cast<int>(ExcludeRegexps.size()); ++i) {
            CHECK_EXP(settings.exclude_regexps(i) == ExcludeRegexps[i],
                "Incorrect exclude regexp at " << i << ": \"" << settings.exclude_regexps(i) << "\" instead of \"" << ExcludeRegexps[i] << "\"");
        }
    }

    template <typename TResult, typename TResponse>
    grpc::Status FillResponse(TResponse* response, TStringBuf kind) {
        TResult res;
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        if (BadRequestMessage) {
            operation->set_status(Ydb::StatusIds::BAD_REQUEST);
            operation->add_issues()->set_message(BadRequestMessage);
            return grpc::Status();
        }
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->set_id(TStringBuilder() << "ydb://export/6?id=1&kind=" << kind);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    void CheckExpectations() override {
        CHECK_EXP(ExportCalls == ExpectedExportCalls, "Incorrect export calls count: " << ExportCalls << " instead of " << ExpectedExportCalls);
        TChecker::CheckExpectations();
    }

    void ClearExpectations() override {
        Items.clear();
        Bucket.clear();
        S3Endpoint.clear();
        S3AccessKey.clear();
        S3SecretKey.clear();
        Scheme = Ydb::Export::ExportToS3Settings::HTTPS;
        Description.clear();
        NumberOfRetries = DEFAULT_RETRIES;
        StorageClass = Ydb::Export::ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED;
        Compression.clear();
        Region.clear();
        DisableVirtualAddressing = false;
        CommonSourcePrefix.clear();
        CommonDstPrefix.clear();
        EncryptionAlgorithm.clear();
        SymmetricKey.clear();
        IncludeIndexData = false;
        ExcludeRegexps.clear();
        ExpectedExportCalls = 1;
        ExportCalls = 0;
        BadRequestMessage.clear();
    }

    TExportImpl& ExpectItem(TString srcPath, TString dstPrefix) {
        Items.emplace_back(std::move(srcPath), std::move(dstPrefix));
        return *this;
    }

    TExportImpl& ExpectNoExportCall() {
        ExpectedExportCalls = 0;
        return *this;
    }

    TExportImpl& ReplyWithBadRequest(TString message) {
        BadRequestMessage = std::move(message);
        return *this;
    }

    TExportImpl& ExpectBucket(TString bucket) {
        Bucket = std::move(bucket);
        return *this;
    }

    TExportImpl& ExpectS3Endpoint(TString s3Endpoint) {
        S3Endpoint = std::move(s3Endpoint);
        return *this;
    }

    TExportImpl& ExpectScheme(Ydb::Export::ExportToS3Settings::Scheme scheme) {
        Scheme = scheme;
        return *this;
    }

    TExportImpl& ExpectS3AccessKey(TString s3AccessKey) {
        S3AccessKey = std::move(s3AccessKey);
        return *this;
    }

    TExportImpl& ExpectS3SecretKey(TString s3SecretKey) {
        S3SecretKey = std::move(s3SecretKey);
        return *this;
    }

    TExportImpl& ExpectDescription(TString description) {
        Description = std::move(description);
        return *this;
    }

    TExportImpl& ExpectNumberOfRetries(ui32 numberOfRetries) {
        NumberOfRetries = numberOfRetries;
        return *this;
    }

    TExportImpl& ExpectStorageClass(Ydb::Export::ExportToS3Settings::StorageClass storageClass) {
        StorageClass = storageClass;
        return *this;
    }

    TExportImpl& ExpectCompression(TString compression) {
        Compression = std::move(compression);
        return *this;
    }

    TExportImpl& ExpectRegion(TString region) {
        Region = std::move(region);
        return *this;
    }

    TExportImpl& ExpectDisableVirtualAddressing(bool disableVirtualAddressing) {
        DisableVirtualAddressing = disableVirtualAddressing;
        return *this;
    }

    TExportImpl& ExpectCommonSourcePrefix(TString commonSourcePrefix) {
        CommonSourcePrefix = std::move(commonSourcePrefix);
        return *this;
    }

    TExportImpl& ExpectCommonDstPrefix(TString commonDstPrefix) {
        CommonDstPrefix = std::move(commonDstPrefix);
        return *this;
    }

    TExportImpl& ExpectSymmetricEncryption(TString encryptionAlgorithm, TString symmetricKey) {
        EncryptionAlgorithm = std::move(encryptionAlgorithm);
        SymmetricKey = std::move(symmetricKey);
        return *this;
    }

    TExportImpl& ExpectIncludeIndexData(bool includeIndexData) {
        IncludeIndexData = includeIndexData;
        return *this;
    }

    TExportImpl& ExpectExcludeRegexp(TString excludeRegexp) {
        ExcludeRegexps.emplace_back(std::move(excludeRegexp));
        return *this;
    }

    std::vector<std::pair<TString, TString>> Items;
    TString Bucket;
    TString S3Endpoint;
    Ydb::Export::ExportToS3Settings::Scheme Scheme = Ydb::Export::ExportToS3Settings::HTTPS;
    TString S3AccessKey;
    TString S3SecretKey;
    TString Description;
    ui32 NumberOfRetries = DEFAULT_RETRIES;
    Ydb::Export::ExportToS3Settings::StorageClass StorageClass = Ydb::Export::ExportToS3Settings::STORAGE_CLASS_UNSPECIFIED;
    TString Compression;
    TString Region;
    bool DisableVirtualAddressing = false;
    TString CommonSourcePrefix;
    TString CommonDstPrefix;
    TString EncryptionAlgorithm;
    TString SymmetricKey;
    bool IncludeIndexData = false;
    std::vector<TString> ExcludeRegexps;
    int ExpectedExportCalls = 1;
    int ExportCalls = 0;
    TString BadRequestMessage;
};

class TTableImpl : public TMockGrpcServiceBase<Ydb::Table::V1::TableService::Service> {
public:
    grpc::Status CreateSession(grpc::ServerContext* context, const Ydb::Table::CreateSessionRequest* request, Ydb::Table::CreateSessionResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        Ydb::Table::CreateSessionResult res;
        res.set_session_id("test-session-id");
        FillOperation(response, res);
        return grpc::Status();
    }

    grpc::Status DeleteSession(grpc::ServerContext* context, const Ydb::Table::DeleteSessionRequest* request, Ydb::Table::DeleteSessionResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status();
    }

    grpc::Status KeepAlive(grpc::ServerContext* context, const Ydb::Table::KeepAliveRequest* request, Ydb::Table::KeepAliveResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
        Ydb::Table::KeepAliveResult res;
        res.set_session_status(Ydb::Table::KeepAliveResult::SESSION_STATUS_READY);
        FillOperation(response, res);
        return grpc::Status();
    }

    grpc::Status DescribeTable(grpc::ServerContext* context, const Ydb::Table::DescribeTableRequest* request, Ydb::Table::DescribeTableResponse* response) override {
        Y_UNUSED(context);
        Ydb::Table::DescribeTableResult res;
        res.mutable_self()->set_name(TStringBuf(request->path()).RNextTok('/'));
        res.mutable_self()->set_type(Ydb::Scheme::Entry::TABLE);
        if (const auto it = TableAttributes.find(request->path()); it != TableAttributes.end()) {
            for (const auto& [name, value] : it->second) {
                (*res.mutable_attributes())[name] = value;
            }
        }
        FillOperation(response, res);
        return grpc::Status();
    }

    TTableImpl& SetTableAttribute(TString path, TString name, TString value) {
        TableAttributes[std::move(path)][std::move(name)] = std::move(value);
        return *this;
    }

    void ClearExpectations() override {
        TableAttributes.clear();
    }

private:
    THashMap<TString, THashMap<TString, TString>> TableAttributes;

    template <typename TResponse, typename TResult>
    void FillOperation(TResponse* response, const TResult& result) {
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
    }
};

class TExportFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TExportImpl>();
        AddService<TTableImpl>();
    }
};

Y_UNIT_TEST_SUITE(ExportTest) {
    Y_UNIT_TEST_F(BadRequestDoesNotRetryWithTablesOnly, TExportFixture) {
        for (const bool nfs : {false, true}) {
            for (const bool includeTable : {false, true}) {
                for (const auto* message : {"Nothing to export", "Invalid export settings"}) {
                    Service<TSchemeImpl>().ExpectListDirectory(GetDatabase() + "/view", Ydb::Scheme::Entry::VIEW);
                    auto& service = Service<TExportImpl>();
                    service.ExpectCommonDstPrefix("/dst")
                        .ExpectItem(GetDatabase() + "/view", "")
                        .ReplyWithBadRequest(message);
                    TList<TString> args = {"-e", GetEndpoint(), "-d", GetDatabase(), "export"};
                    if (nfs) {
                        args.insert(args.end(), {"nfs", "--fs-path", "/dst"});
                    } else {
                        service.ExpectBucket(TEST_BUCKET).ExpectS3Endpoint(TEST_S3_ENDPOINT)
                            .ExpectS3AccessKey("test-key").ExpectS3SecretKey("test-access-key");
                        args.insert(args.end(), {"s3", "--destination-prefix", "/dst",
                            "--bucket", TEST_BUCKET, "--s3-endpoint", TEST_S3_ENDPOINT,
                            "--access-key", "test-key", "--secret-key", "test-access-key"});
                    }
                    args.insert(args.end(), {"--include", "view", "--exclude", "^never$"});
                    if (includeTable) {
                        Service<TSchemeImpl>().ExpectListDirectory(GetDatabase() + "/keep", Ydb::Scheme::Entry::TABLE);
                        service.ExpectItem(GetDatabase() + "/keep", "");
                        args.insert(args.end(), {"--include", "keep"});
                    }
                    ExpectFail();
                    RunCli(std::move(args));
                }
            }
        }
    }

    Y_UNIT_TEST_F(ExcludedNfsItemsDoNotStartExport, TExportFixture) {
        Service<TSchemeImpl>().ExpectListDirectory(GetDatabase())
            .ExpectChild(GetDatabase(), "skip", Ydb::Scheme::Entry::TABLE);
        Service<TExportImpl>().ExpectNoExportCall();
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "export", "nfs",
            "--fs-path", "/dst", "--exclude", "skip"});
    }

    Y_UNIT_TEST_F(PassS3Parameters, TExportFixture) {
        const TString encryptionKey = "test-encryption-key";
        const TString encryptionKeyFile = EnvFile(encryptionKey, "export_encryption.key");

        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectScheme(Ydb::Export::ExportToS3Settings::HTTP)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectDescription("test export")
            .ExpectNumberOfRetries(42)
            .ExpectStorageClass(Ydb::Export::ExportToS3Settings::STANDARD_IA)
            .ExpectCompression("zstd-3")
            .ExpectDisableVirtualAddressing(true)
            .ExpectCommonSourcePrefix("/test_database/export-root")
            .ExpectCommonDstPrefix("common/prefix")
            .ExpectSymmetricEncryption("AES-128-GCM", encryptionKey)
            .ExpectIncludeIndexData(true);

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--scheme", "http",
                "--storage-class", "STANDARD_IA",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--destination-prefix", "common/prefix",
                "--description", "test export",
                "--retries", "42",
                "--compression", "zstd-3",
                "--use-virtual-addressing", "false",
                "--root-path", "/test_database/export-root",
                "--encryption-algorithm", "AES-128-GCM",
                "--encryption-key-file", encryptionKeyFile,
                "--include-index-data", "true",
            }
        );
    }

    Y_UNIT_TEST_F(PassItems, TExportFixture) {
        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("root/path")
            .ExpectCommonDstPrefix("dest/prefix")
            .ExpectItem("/test_database/root/path/path", "")
            .ExpectItem("/test_database/root/path/srcpath", "dstpath");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--root-path", "root/path",
                "--destination-prefix", "dest/prefix",
                "--item", "src=srcpath,dst=dstpath",
                "--include", "path",
            }
        );
    }

    Y_UNIT_TEST_F(CommonDestinationPrefixWithoutItems, TExportFixture) {
        // The main use case: export the whole database recursively
        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("dst");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--destination-prefix", "dst",
                }
            );
        }

        const TString encryptionKey = "test-encryption-key";
        const TString encryptionKeyFile = EnvFile(encryptionKey, "encryption.key");

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("dst")
                .ExpectSymmetricEncryption("AES-128-GCM", encryptionKey);

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--destination-prefix", "dst",
                    "--encryption-algorithm", "AES-128-GCM",
                    "--encryption-key-file", encryptionKeyFile,
                }
            );
        }
    }

    Y_UNIT_TEST_F(ApplyExcludeClientFilter, TExportFixture) {
        Service<TSchemeImpl>()
            .ExpectListDirectory("/test_database/export-root")
            .ExpectChild("/test_database/export-root", "keep", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "skip", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "nested", Ydb::Scheme::Entry::DIRECTORY)
            .ExpectListDirectory("/test_database/export-root/nested")
            .ExpectChild("/test_database/export-root/nested", "keep_nested", Ydb::Scheme::Entry::TABLE);

        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("/test_database/export-root")
            .ExpectCommonDstPrefix("dst")
            .ExpectItem("/test_database/export-root/keep", "dst/keep")
            .ExpectItem("/test_database/export-root/nested/keep_nested", "dst/nested/keep_nested");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--root-path", "/test_database/export-root",
                "--destination-prefix", "dst",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(SkipNotExportableItems, TExportFixture) {
        Service<TSchemeImpl>()
            .ExpectListDirectory("/test_database/export-root")
            .ExpectChild("/test_database/export-root", "keep", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "omit", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "omitted_column", Ydb::Scheme::Entry::COLUMN_TABLE)
            .ExpectChild("/test_database/export-root", "kept_column", Ydb::Scheme::Entry::COLUMN_TABLE)
            .ExpectChild("/test_database/export-root", "replica", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "both", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "false_attributes", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "topic", Ydb::Scheme::Entry::TOPIC)
            .ExpectChild("/test_database/export-root", "omitted_view", Ydb::Scheme::Entry::VIEW)
            .ExpectChild("/test_database/export-root", "kept_view", Ydb::Scheme::Entry::VIEW);

        Service<TTableImpl>()
            .SetTableAttribute("/test_database/export-root/omit", "__omit_backup", "true")
            .SetTableAttribute("/test_database/export-root/omitted_column", "__omit_backup", "true")
            .SetTableAttribute("/test_database/export-root/replica", "__async_replica", "true")
            .SetTableAttribute("/test_database/export-root/both", "__async_replica", "true")
            .SetTableAttribute("/test_database/export-root/both", "__omit_backup", "true")
            .SetTableAttribute("/test_database/export-root/false_attributes", "__async_replica", "false")
            .SetTableAttribute("/test_database/export-root/false_attributes", "__omit_backup", "false");

        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectItem("/test_database/export-root/keep", "dst/keep")
            .ExpectItem("/test_database/export-root/kept_column", "dst/kept_column")
            .ExpectItem("/test_database/export-root/false_attributes", "dst/false_attributes")
            .ExpectItem("/test_database/export-root/topic", "dst/topic")
            .ExpectItem("/test_database/export-root/omitted_view", "dst/omitted_view")
            .ExpectItem("/test_database/export-root/kept_view", "dst/kept_view");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--item", "src=export-root,dst=dst",
            }
        );
    }

    Y_UNIT_TEST_F(ExcludeClientFilterFailsWhenAllItemsFilteredOut, TExportFixture) {
        Service<TSchemeImpl>()
            .ExpectListDirectory("/test_database/export-root")
            .ExpectChild("/test_database/export-root", "skip1", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "skip2", Ydb::Scheme::Entry::TABLE);
        Service<TExportImpl>().ExpectNoExportCall();

        ExpectFail();
        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--root-path", "/test_database/export-root",
                "--destination-prefix", "dst",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(ExcludeClientFilterUsesDatabaseRootWithoutRootPathAndItems, TExportFixture) {
        Service<TSchemeImpl>()
            .ExpectListDirectory(GetDatabase())
            .ExpectChild(GetDatabase(), "keep", Ydb::Scheme::Entry::TABLE)
            .ExpectChild(GetDatabase(), "skip", Ydb::Scheme::Entry::TABLE);

        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonDstPrefix("dst")
            .ExpectItem(GetDatabase() + "/keep", "dst/keep");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--destination-prefix", "dst",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(ExcludeClientFilterSucceedsWithRootPathAndNoItems, TExportFixture) {
        Service<TSchemeImpl>()
            .ExpectListDirectory("/test_database/export-root")
            .ExpectChild("/test_database/export-root", "keep", Ydb::Scheme::Entry::TABLE)
            .ExpectChild("/test_database/export-root", "skip", Ydb::Scheme::Entry::TABLE);

        Service<TExportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("/test_database/export-root")
            .ExpectCommonDstPrefix("dst")
            .ExpectItem("/test_database/export-root/keep", "dst/keep");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "export", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--root-path", "/test_database/export-root",
                "--destination-prefix", "dst",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(PassEncryptionKey, TExportFixture) {
        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("dest/prefix")
                .ExpectSymmetricEncryption("ChaCha20-Poly1305", "1234567890");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--destination-prefix", "dest/prefix",
                    "--encryption-algorithm", "ChaCha20-Poly1305",
                },
                {
                    {"YDB_ENCRYPTION_KEY", "31323334353637383930"} // Hex
                }
            );
        }

        const TString keyFile = EnvFile("encryption-key", "key");

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("dest/prefix")
                .ExpectSymmetricEncryption("AES-256-GCM", "encryption-key");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--destination-prefix", "dest/prefix",
                    "--encryption-algorithm", "AES-256-GCM",
                },
                {
                    {"YDB_ENCRYPTION_KEY_FILE", keyFile}
                }
            );
        }

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("dest/prefix")
                .ExpectSymmetricEncryption("AES-128-GCM", "encryption-key");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--destination-prefix", "dest/prefix",
                    "--encryption-algorithm", "AES-128-GCM",
                    "--encryption-key-file", keyFile,
                }
            );
        }
    }

    Y_UNIT_TEST_F(SelectAwsParameters, TExportFixture) {
        EnvHomeFile(".aws/credentials", R"(
            [default]
            aws_access_key_id = default-test-key
            aws_secret_access_key = default-test-access-key

            [test-profile]
            aws_access_key_id = test-key
            aws_secret_access_key = test-access-key
        )");

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("default-test-key")
                .ExpectS3SecretKey("default-test-access-key")
                .ExpectCommonDstPrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--destination-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--aws-profile", "test-profile",
                    "--destination-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonDstPrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--destination-prefix", "common/prefix",
                },
                {
                    {"AWS_PROFILE", "test-profile"},
                }
            );
        }

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("opt-test-key")
                .ExpectS3SecretKey("opt-test-access-key")
                .ExpectCommonDstPrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "opt-test-key",
                    "--secret-key", "opt-test-access-key",
                    "--destination-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TExportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("env-test-key")
                .ExpectS3SecretKey("env-test-access-key")
                .ExpectCommonDstPrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "export", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--destination-prefix", "common/prefix",
                },
                {
                    {"AWS_ACCESS_KEY_ID", "env-test-key"},
                    {"AWS_SECRET_ACCESS_KEY", "env-test-access-key"},
                }
            );
        }
    }
}
