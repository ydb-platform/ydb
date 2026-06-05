#include "mock_env.h"

#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>

const TString TEST_BUCKET = "test-bucket";
const TString TEST_S3_ENDPOINT = "https://ydb.s3.local";
const ui32 DEFAULT_RETRIES = 10;

class TImportImpl : public TMockGrpcServiceBase<Ydb::Import::V1::ImportService::Service> {
public:
    struct TExpectedItem {
        TString SourcePrefix;
        TString DestinationPath;
        TString SourcePath;
    };

    struct TExpectedListResultItem {
        TString Prefix;
        TString Path;
    };

    grpc::Status ImportFromS3(grpc::ServerContext* context, const Ydb::Import::ImportFromS3Request* request, Ydb::Import::ImportFromS3Response* response) override {
        Y_UNUSED(context);
        ++ImportCalls;
        CheckS3Params(request);
        CheckItems(request);
        return FillImportResponse(response);
    }

    grpc::Status ListObjectsInS3Export(grpc::ServerContext* context, const Ydb::Import::ListObjectsInS3ExportRequest* request, Ydb::Import::ListObjectsInS3ExportResponse* response) override {
        Y_UNUSED(context);
        ++ListCalls;
        CheckListS3Params(request);
        CheckListItems(request);
        return FillListResponse(response);
    }

    grpc::Status FillImportResponse(Ydb::Import::ImportFromS3Response* response) {
        Ydb::Import::ImportFromS3Result res;
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->set_id("ydb://import/6?id=1&kind=s3");
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    grpc::Status FillListResponse(Ydb::Import::ListObjectsInS3ExportResponse* response) {
        Ydb::Import::ListObjectsInS3ExportResult res;
        for (const auto& item : ListResultItems) {
            auto* resultItem = res.add_items();
            resultItem->set_prefix(item.Prefix);
            resultItem->set_path(item.Path);
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    void CheckS3Params(const Ydb::Import::ImportFromS3Request* request) {
        const auto& settings = request->settings();
        CheckCommonS3Params(settings);
        CHECK_EXP(settings.description() == Description, "Incorrect description: \"" << settings.description() << "\" instead of \"" << Description << "\"");
        CHECK_EXP(settings.source_prefix() == CommonSourcePrefix, "Incorrect common source prefix: \"" << settings.source_prefix() << "\" instead of \"" << CommonSourcePrefix << "\"");
        CHECK_EXP(settings.destination_path() == CommonDestinationPath, "Incorrect common destination path: \"" << settings.destination_path() << "\" instead of \"" << CommonDestinationPath << "\"");
        CHECK_EXP(settings.no_acl() == NoACL, "Incorrect no acl: " << settings.no_acl() << " instead of " << NoACL);
        CHECK_EXP(settings.skip_checksum_validation() == SkipChecksumValidation, "Incorrect skip checksum validation: " << settings.skip_checksum_validation() << " instead of " << SkipChecksumValidation);
        CHECK_EXP(settings.index_population_mode() == IndexPopulationMode, "Incorrect index population mode: \"" << Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(settings.index_population_mode()) << "\" instead of \"" << Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(IndexPopulationMode) << "\"");
    }

    void CheckListS3Params(const Ydb::Import::ListObjectsInS3ExportRequest* request) {
        const auto& settings = request->settings();
        CheckCommonS3Params(settings);
        CHECK_EXP(settings.prefix() == CommonSourcePrefix, "Incorrect common source prefix: \"" << settings.prefix() << "\" instead of \"" << CommonSourcePrefix << "\"");
    }

    template <typename TSettings>
    void CheckCommonS3Params(const TSettings& settings) {
        CHECK_EXP(settings.bucket() == Bucket, "Incorrect bucket: \"" << settings.bucket() << "\" instead of \"" << Bucket << "\"");
        CHECK_EXP(settings.endpoint() == S3Endpoint, "Incorrect S3 endpoint: \"" << settings.endpoint() << "\" instead of \"" << S3Endpoint << "\"");
        CHECK_EXP(settings.scheme() == Scheme, "Incorrect scheme: \"" << Ydb::Import::ImportFromS3Settings::Scheme_Name(settings.scheme()) << "\" instead of \"" << Ydb::Import::ImportFromS3Settings::Scheme_Name(Scheme) << "\"");
        CHECK_EXP(settings.access_key() == S3AccessKey, "Incorrect S3 access key: \"" << settings.access_key() << "\" instead of \"" << S3AccessKey << "\"");
        CHECK_EXP(settings.secret_key() == S3SecretKey, "Incorrect S3 secret key: \"" << settings.secret_key() << "\" instead of \"" << S3SecretKey << "\"");
        CHECK_EXP(settings.number_of_retries() == NumberOfRetries, "Incorrect number of retries: " << settings.number_of_retries() << " instead of " << NumberOfRetries);
        CHECK_EXP(settings.disable_virtual_addressing() == DisableVirtualAddressing, "Incorrect disable virtual addressing: " << settings.disable_virtual_addressing() << " instead of " << DisableVirtualAddressing);
        CheckEncryptionSettings(settings);
        CheckExcludeRegexps(settings);
    }

    void CheckItems(const Ydb::Import::ImportFromS3Request* request) {
        const auto& settings = request->settings();
        CHECK_EXP(settings.items_size() == static_cast<int>(Items.size()),
            "Items size does not match. settings.items_size(): " << settings.items_size() << ". Items.size(): " << Items.size());
        for (int i = 0; i < settings.items_size() && i < static_cast<int>(Items.size()); ++i) {
            const auto& item = settings.items(i);
            const auto& expected = Items[i];
            CHECK_EXP(item.source_prefix() == expected.SourcePrefix,
                "Incorrect item source prefix at " << i << ": \"" << item.source_prefix() << "\" instead of \"" << expected.SourcePrefix << "\"");
            CHECK_EXP(item.destination_path() == expected.DestinationPath,
                "Incorrect item destination path at " << i << ": \"" << item.destination_path() << "\" instead of \"" << expected.DestinationPath << "\"");
            CHECK_EXP(item.source_path() == expected.SourcePath,
                "Incorrect item source path at " << i << ": \"" << item.source_path() << "\" instead of \"" << expected.SourcePath << "\"");
        }
    }

    void CheckListItems(const Ydb::Import::ListObjectsInS3ExportRequest* request) {
        const auto& settings = request->settings();
        CHECK_EXP(settings.items_size() == static_cast<int>(ListItems.size()),
            "List items size does not match. settings.items_size(): " << settings.items_size() << ". ListItems.size(): " << ListItems.size());
        for (int i = 0; i < settings.items_size() && i < static_cast<int>(ListItems.size()); ++i) {
            CHECK_EXP(settings.items(i).path() == ListItems[i],
                "Incorrect list item path at " << i << ": \"" << settings.items(i).path() << "\" instead of \"" << ListItems[i] << "\"");
        }
    }

    template <typename TSettings>
    void CheckEncryptionSettings(const TSettings& settings) {
        const auto& encryptionSettings = settings.encryption_settings();
        CHECK_EXP(encryptionSettings.symmetric_key().key() == SymmetricKey,
            "Incorrect symmetric encryption key: \"" << encryptionSettings.symmetric_key().key() << "\" instead of \"" << SymmetricKey << "\"");
    }

    template <typename TSettings>
    void CheckExcludeRegexps(const TSettings& settings) {
        CHECK_EXP(settings.exclude_regexps_size() == static_cast<int>(ExcludeRegexps.size()),
            "Exclude regexps size does not match. settings.exclude_regexps_size(): " << settings.exclude_regexps_size() << ". ExcludeRegexps.size(): " << ExcludeRegexps.size());
        for (int i = 0; i < settings.exclude_regexps_size() && i < static_cast<int>(ExcludeRegexps.size()); ++i) {
            CHECK_EXP(settings.exclude_regexps(i) == ExcludeRegexps[i],
                "Incorrect exclude regexp at " << i << ": \"" << settings.exclude_regexps(i) << "\" instead of \"" << ExcludeRegexps[i] << "\"");
        }
    }

    void CheckExpectations() override {
        CHECK_EXP(ImportCalls == ExpectedImportCalls, "Incorrect ImportFromS3 calls count: " << ImportCalls << " instead of " << ExpectedImportCalls);
        CHECK_EXP(ListCalls == ExpectedListCalls, "Incorrect ListObjectsInS3Export calls count: " << ListCalls << " instead of " << ExpectedListCalls);
        TChecker::CheckExpectations();
    }

    void ClearExpectations() override {
        Items.clear();
        ListItems.clear();
        ListResultItems.clear();
        Bucket.clear();
        S3Endpoint.clear();
        Scheme = Ydb::Import::ImportFromS3Settings::HTTPS;
        S3AccessKey.clear();
        S3SecretKey.clear();
        Description.clear();
        NumberOfRetries = DEFAULT_RETRIES;
        DisableVirtualAddressing = false;
        CommonSourcePrefix.clear();
        CommonDestinationPath.clear();
        SymmetricKey.clear();
        NoACL = false;
        SkipChecksumValidation = false;
        IndexPopulationMode = Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD;
        ExcludeRegexps.clear();
        ExpectedImportCalls = 1;
        ExpectedListCalls = 0;
        ImportCalls = 0;
        ListCalls = 0;
    }

    TImportImpl& ExpectItem(TString sourcePrefix, TString destinationPath, TString sourcePath = {}) {
        Items.push_back({
            .SourcePrefix = std::move(sourcePrefix),
            .DestinationPath = std::move(destinationPath),
            .SourcePath = std::move(sourcePath),
        });
        return *this;
    }

    TImportImpl& ExpectListItem(TString path) {
        ListItems.emplace_back(std::move(path));
        return *this;
    }

    TImportImpl& AddListResultItem(TString prefix, TString path) {
        ListResultItems.push_back({
            .Prefix = std::move(prefix),
            .Path = std::move(path),
        });
        return *this;
    }

    TImportImpl& ExpectListCall() {
        ExpectedImportCalls = 0;
        ExpectedListCalls = 1;
        return *this;
    }

    TImportImpl& ExpectNoImportCall() {
        ExpectedImportCalls = 0;
        return *this;
    }

    TImportImpl& ExpectBucket(TString bucket) {
        Bucket = std::move(bucket);
        return *this;
    }

    TImportImpl& ExpectS3Endpoint(TString s3Endpoint) {
        S3Endpoint = std::move(s3Endpoint);
        return *this;
    }

    TImportImpl& ExpectScheme(Ydb::Import::ImportFromS3Settings::Scheme scheme) {
        Scheme = scheme;
        return *this;
    }

    TImportImpl& ExpectS3AccessKey(TString s3AccessKey) {
        S3AccessKey = std::move(s3AccessKey);
        return *this;
    }

    TImportImpl& ExpectS3SecretKey(TString s3SecretKey) {
        S3SecretKey = std::move(s3SecretKey);
        return *this;
    }

    TImportImpl& ExpectDescription(TString description) {
        Description = std::move(description);
        return *this;
    }

    TImportImpl& ExpectNumberOfRetries(ui32 numberOfRetries) {
        NumberOfRetries = numberOfRetries;
        return *this;
    }

    TImportImpl& ExpectDisableVirtualAddressing(bool disableVirtualAddressing) {
        DisableVirtualAddressing = disableVirtualAddressing;
        return *this;
    }

    TImportImpl& ExpectCommonSourcePrefix(TString commonSourcePrefix) {
        CommonSourcePrefix = std::move(commonSourcePrefix);
        return *this;
    }

    TImportImpl& ExpectCommonDestinationPath(TString commonDestinationPath) {
        CommonDestinationPath = std::move(commonDestinationPath);
        return *this;
    }

    TImportImpl& ExpectSymmetricKey(TString symmetricKey) {
        SymmetricKey = std::move(symmetricKey);
        return *this;
    }

    TImportImpl& ExpectNoACL(bool noACL) {
        NoACL = noACL;
        return *this;
    }

    TImportImpl& ExpectSkipChecksumValidation(bool skipChecksumValidation) {
        SkipChecksumValidation = skipChecksumValidation;
        return *this;
    }

    TImportImpl& ExpectIndexPopulationMode(Ydb::Import::ImportFromS3Settings::IndexPopulationMode indexPopulationMode) {
        IndexPopulationMode = indexPopulationMode;
        return *this;
    }

    TImportImpl& ExpectExcludeRegexp(TString excludeRegexp) {
        ExcludeRegexps.emplace_back(std::move(excludeRegexp));
        return *this;
    }

    std::vector<TExpectedItem> Items;
    std::vector<TString> ListItems;
    std::vector<TExpectedListResultItem> ListResultItems;
    TString Bucket;
    TString S3Endpoint;
    Ydb::Import::ImportFromS3Settings::Scheme Scheme = Ydb::Import::ImportFromS3Settings::HTTPS;
    TString S3AccessKey;
    TString S3SecretKey;
    TString Description;
    ui32 NumberOfRetries = DEFAULT_RETRIES;
    bool DisableVirtualAddressing = false;
    TString CommonSourcePrefix;
    TString CommonDestinationPath;
    TString SymmetricKey;
    bool NoACL = false;
    bool SkipChecksumValidation = false;
    Ydb::Import::ImportFromS3Settings::IndexPopulationMode IndexPopulationMode = Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_BUILD;
    std::vector<TString> ExcludeRegexps;
    int ExpectedImportCalls = 1;
    int ExpectedListCalls = 0;
    int ImportCalls = 0;
    int ListCalls = 0;
};

class TImportFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TImportImpl>();
    }

public:
    NKikimr::NWrappers::NTestHelpers::TS3Mock& S3Mock() {
        if (!S3Mock_) {
            S3Port_ = GetPortManager().GetPort();
            S3Mock_.ConstructInPlace(NKikimr::NWrappers::NTestHelpers::TS3Mock::TSettings(S3Port_));
            UNIT_ASSERT_C(S3Mock_->Start(), S3Mock_->GetError());
        }
        return *S3Mock_;
    }

    TString GetS3Endpoint() {
        S3Mock();
        return TStringBuilder() << "localhost:" << S3Port_;
    }

    void PutS3Object(const TString& key, const TString& value = {}) {
        S3Mock().GetData()[key] = value;
    }

    void PutS3TableExport(const TString& prefix) {
        PutS3Object("/" + TEST_BUCKET + "/" + prefix + "/scheme.pb");
    }

private:
    TMaybe<NKikimr::NWrappers::NTestHelpers::TS3Mock> S3Mock_;
    ui16 S3Port_ = 0;
};

Y_UNIT_TEST_SUITE(ImportTest) {
    Y_UNIT_TEST_F(PassS3Parameters, TImportFixture) {
        const TString encryptionKey = "test-encryption-key";
        const TString encryptionKeyFile = EnvFile(encryptionKey, "import_encryption.key");

        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectScheme(Ydb::Import::ImportFromS3Settings::HTTP)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectDescription("test import")
            .ExpectNumberOfRetries(42)
            .ExpectDisableVirtualAddressing(true)
            .ExpectCommonSourcePrefix("common/prefix")
            .ExpectCommonDestinationPath("/test_database/import-root")
            .ExpectSymmetricKey(encryptionKey)
            .ExpectNoACL(true)
            .ExpectSkipChecksumValidation(true)
            .ExpectIndexPopulationMode(Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT);
            // Exclude regexps are not yet passed to the server since it is a new feature that can be absent on the server.
            // .ExpectExcludeRegexp("^logs/")
            // .ExpectExcludeRegexp("^tmp/")

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--source-prefix", "common/prefix",
                "--destination-path", "/test_database/import-root",
                "--description", "test import",
                "--retries", "42",
                "--use-virtual-addressing", "false",
                "--encryption-key-file", encryptionKeyFile,
                "--no-acl",
                "--skip-checksum-validation",
                "--index-population-mode", "Import",
                "--exclude", "^logs/",
                "--exclude", "^tmp/",
            }
        );
    }

    Y_UNIT_TEST_F(PassIncludeItems, TImportFixture) {
        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("source/prefix")
            .ExpectItem("", "", "src/path")
            .ExpectItem("", "", "another/path");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--source-prefix", "source/prefix",
                "--include", "src/path",
                "--include", "another/path",
            }
        );
    }

    Y_UNIT_TEST_F(CommonSourcePrefixWithoutItems, TImportFixture) {
        // The main use case: import the whole database recursively
        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("source/prefix");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--source-prefix", "source/prefix",
            }
        );

        const TString encryptionKey = "test-encryption-key";
        const TString encryptionKeyFile = EnvFile(encryptionKey, "import_whole_database_encryption.key");

        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectCommonSourcePrefix("source/prefix")
            .ExpectSymmetricKey(encryptionKey);

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--source-prefix", "source/prefix",
                "--encryption-key-file", encryptionKeyFile,
            }
        );
    }

    Y_UNIT_TEST_F(ApplyExcludeClientFilter, TImportFixture) {
        PutS3TableExport("source/prefix/keep");
        PutS3TableExport("source/prefix/skip");

        const TString s3Endpoint = GetS3Endpoint();

        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(s3Endpoint)
            .ExpectScheme(Ydb::Import::ImportFromS3Settings::HTTP)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectDisableVirtualAddressing(true)
            .ExpectItem("source/prefix/keep", "/test_database/import-root/keep");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", s3Endpoint,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--use-virtual-addressing", "false",
                "--item", "src=source/prefix,dst=/test_database/import-root",
                "--exclude", "skip",
            }
        );

        Service<TImportImpl>()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(s3Endpoint)
            .ExpectScheme(Ydb::Import::ImportFromS3Settings::HTTP)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectDisableVirtualAddressing(true)
            .ExpectCommonSourcePrefix("source/prefix")
            .ExpectItem("", "", "keep");

        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", s3Endpoint,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--use-virtual-addressing", "false",
                "--source-prefix", "source/prefix",
                "--include", "keep",
                "--include", "skip",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(ExcludeClientFilterFailsWhenAllItemsFilteredOut, TImportFixture) {
        PutS3TableExport("source/prefix/skip1");
        PutS3TableExport("source/prefix/skip2");

        const TString s3Endpoint = GetS3Endpoint();

        Service<TImportImpl>()
            .ExpectNoImportCall();

        ExpectFail();
        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", s3Endpoint,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--use-virtual-addressing", "false",
                "--item", "src=source/prefix,dst=/test_database/import-root",
                "--exclude", "skip",
            }
        );

        Service<TImportImpl>()
            .ExpectNoImportCall();

        ExpectFail();
        RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", s3Endpoint,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--use-virtual-addressing", "false",
                "--source-prefix", "source/prefix",
                "--include", "skip1",
                "--include", "skip2",
                "--exclude", "skip",
            }
        );
    }

    Y_UNIT_TEST_F(ListObjects, TImportFixture) {
        const TString encryptionKey = "test-encryption-key";
        const TString encryptionKeyFile = EnvFile(encryptionKey, "import_list_encryption.key");

        Service<TImportImpl>()
            .ExpectListCall()
            .ExpectBucket(TEST_BUCKET)
            .ExpectS3Endpoint(TEST_S3_ENDPOINT)
            .ExpectScheme(Ydb::Import::ImportFromS3Settings::HTTP)
            .ExpectS3AccessKey("test-key")
            .ExpectS3SecretKey("test-access-key")
            .ExpectNumberOfRetries(42)
            .ExpectDisableVirtualAddressing(true)
            .ExpectCommonSourcePrefix("source/prefix")
            .ExpectSymmetricKey(encryptionKey)
            .ExpectListItem("src/path")
            .ExpectListItem("another/path")
            .AddListResultItem("source/prefix/src/path", "src/path")
            .AddListResultItem("source/prefix/another/path", "another/path");

        const TString output = RunCli(
            {
                "-v",
                "-e", GetEndpoint(),
                "-d", GetDatabase(),
                "import", "s3",
                "--list",
                "--bucket", TEST_BUCKET,
                "--s3-endpoint", TEST_S3_ENDPOINT,
                "--scheme", "http",
                "--access-key", "test-key",
                "--secret-key", "test-access-key",
                "--source-prefix", "source/prefix",
                "--include", "src/path",
                "--include", "another/path",
                "--retries", "42",
                "--use-virtual-addressing", "false",
                "--encryption-key-file", encryptionKeyFile,
            }
        );

        UNIT_ASSERT_STRING_CONTAINS(output, "src/path");
        UNIT_ASSERT_STRING_CONTAINS(output, "source/prefix/src/path");
        UNIT_ASSERT_STRING_CONTAINS(output, "another/path");
        UNIT_ASSERT_STRING_CONTAINS(output, "source/prefix/another/path");
    }

    Y_UNIT_TEST_F(PassEncryptionKey, TImportFixture) {
        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonSourcePrefix("source/prefix")
                .ExpectSymmetricKey("1234567890");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--source-prefix", "source/prefix",
                },
                {
                    {"YDB_ENCRYPTION_KEY", "31323334353637383930"} // Hex
                }
            );
        }

        const TString keyFile = EnvFile("encryption-key", "key");

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonSourcePrefix("source/prefix")
                .ExpectSymmetricKey("encryption-key");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--source-prefix", "source/prefix",
                },
                {
                    {"YDB_ENCRYPTION_KEY_FILE", keyFile}
                }
            );
        }

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonSourcePrefix("source/prefix")
                .ExpectSymmetricKey("encryption-key");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "test-key",
                    "--secret-key", "test-access-key",
                    "--source-prefix", "source/prefix",
                    "--encryption-key-file", keyFile,
                }
            );
        }
    }

    Y_UNIT_TEST_F(SelectAwsParameters, TImportFixture) {
        EnvHomeFile(".aws/credentials", R"(
            [default]
            aws_access_key_id = default-test-key
            aws_secret_access_key = default-test-access-key

            [test-profile]
            aws_access_key_id = test-key
            aws_secret_access_key = test-access-key
        )");

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("default-test-key")
                .ExpectS3SecretKey("default-test-access-key")
                .ExpectCommonSourcePrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--source-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonSourcePrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--aws-profile", "test-profile",
                    "--source-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("test-key")
                .ExpectS3SecretKey("test-access-key")
                .ExpectCommonSourcePrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--source-prefix", "common/prefix",
                },
                {
                    {"AWS_PROFILE", "test-profile"},
                }
            );
        }

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("opt-test-key")
                .ExpectS3SecretKey("opt-test-access-key")
                .ExpectCommonSourcePrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--access-key", "opt-test-key",
                    "--secret-key", "opt-test-access-key",
                    "--source-prefix", "common/prefix",
                }
            );
        }

        {
            Service<TImportImpl>()
                .ExpectBucket(TEST_BUCKET)
                .ExpectS3Endpoint(TEST_S3_ENDPOINT)
                .ExpectS3AccessKey("env-test-key")
                .ExpectS3SecretKey("env-test-access-key")
                .ExpectCommonSourcePrefix("common/prefix");

            RunCli(
                {
                    "-v",
                    "-e", GetEndpoint(),
                    "-d", GetDatabase(),
                    "import", "s3",
                    "--bucket", TEST_BUCKET,
                    "--s3-endpoint", TEST_S3_ENDPOINT,
                    "--source-prefix", "common/prefix",
                },
                {
                    {"AWS_ACCESS_KEY_ID", "env-test-key"},
                    {"AWS_SECRET_ACCESS_KEY", "env-test-access-key"},
                }
            );
        }
    }
}
