#include "mock_env.h"

#include <ydb/public/api/grpc/ydb_import_v1.grpc.pb.h>

const TString TEST_BUCKET = "test-bucket";
const TString TEST_S3_ENDPOINT = "https://ydb.s3.local";
const ui32 DEFAULT_RETRIES = 10;

class TImportImpl : public TMockGrpcServiceBase<Ydb::Import::V1::ImportService::Service> {
public:
    grpc::Status ImportFromS3(grpc::ServerContext* context, const Ydb::Import::ImportFromS3Request* request, Ydb::Import::ImportFromS3Response* response) override {
        Y_UNUSED(context);
        CheckS3Params(request);
        return FillImportResponse(response);
    }

    grpc::Status ListObjectsInS3Export(grpc::ServerContext* context, const Ydb::Import::ListObjectsInS3ExportRequest* request, Ydb::Import::ListObjectsInS3ExportResponse* response) override {
        Y_UNUSED(context);
        Y_UNUSED(request);
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
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(res);
        return grpc::Status();
    }

    void CheckS3Params(const Ydb::Import::ImportFromS3Request* request) {
        const auto& settings = request->settings();
        CHECK_EXP(settings.bucket() == Bucket, "Incorrect bucket: \"" << settings.bucket() << "\" instead of \"" << Bucket << "\"");
        CHECK_EXP(settings.endpoint() == S3Endpoint, "Incorrect S3 endpoint: \"" << settings.endpoint() << "\" instead of \"" << S3Endpoint << "\"");
        CHECK_EXP(settings.scheme() == Scheme, "Incorrect scheme: \"" << Ydb::Import::ImportFromS3Settings::Scheme_Name(settings.scheme()) << "\" instead of \"" << Ydb::Import::ImportFromS3Settings::Scheme_Name(Scheme) << "\"");
        CHECK_EXP(settings.access_key() == S3AccessKey, "Incorrect S3 access key: \"" << settings.access_key() << "\" instead of \"" << S3AccessKey << "\"");
        CHECK_EXP(settings.secret_key() == S3SecretKey, "Incorrect S3 secret key: \"" << settings.secret_key() << "\" instead of \"" << S3SecretKey << "\"");
        CHECK_EXP(settings.description() == Description, "Incorrect description: \"" << settings.description() << "\" instead of \"" << Description << "\"");
        CHECK_EXP(settings.number_of_retries() == NumberOfRetries, "Incorrect number of retries: " << settings.number_of_retries() << " instead of " << NumberOfRetries);
        CHECK_EXP(settings.disable_virtual_addressing() == DisableVirtualAddressing, "Incorrect disable virtual addressing: " << settings.disable_virtual_addressing() << " instead of " << DisableVirtualAddressing);
        CHECK_EXP(settings.source_prefix() == CommonSourcePrefix, "Incorrect common source prefix: \"" << settings.source_prefix() << "\" instead of \"" << CommonSourcePrefix << "\"");
        CHECK_EXP(settings.destination_path() == CommonDestinationPath, "Incorrect common destination path: \"" << settings.destination_path() << "\" instead of \"" << CommonDestinationPath << "\"");
        CHECK_EXP(settings.no_acl() == NoACL, "Incorrect no acl: " << settings.no_acl() << " instead of " << NoACL);
        CHECK_EXP(settings.skip_checksum_validation() == SkipChecksumValidation, "Incorrect skip checksum validation: " << settings.skip_checksum_validation() << " instead of " << SkipChecksumValidation);
        CHECK_EXP(settings.index_population_mode() == IndexPopulationMode, "Incorrect index population mode: \"" << Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(settings.index_population_mode()) << "\" instead of \"" << Ydb::Import::ImportFromS3Settings::IndexPopulationMode_Name(IndexPopulationMode) << "\"");
        CheckEncryptionSettings(settings);
        CheckExcludeRegexps(settings);
    }

    void CheckEncryptionSettings(const Ydb::Import::ImportFromS3Settings& settings) {
        const auto& encryptionSettings = settings.encryption_settings();
        CHECK_EXP(encryptionSettings.symmetric_key().key() == SymmetricKey,
            "Incorrect symmetric encryption key: \"" << encryptionSettings.symmetric_key().key() << "\" instead of \"" << SymmetricKey << "\"");
    }

    void CheckExcludeRegexps(const Ydb::Import::ImportFromS3Settings& settings) {
        CHECK_EXP(settings.exclude_regexps_size() == static_cast<int>(ExcludeRegexps.size()),
            "Exclude regexps size does not match. settings.exclude_regexps_size(): " << settings.exclude_regexps_size() << ". ExcludeRegexps.size(): " << ExcludeRegexps.size());
        for (int i = 0; i < settings.exclude_regexps_size() && i < static_cast<int>(ExcludeRegexps.size()); ++i) {
            CHECK_EXP(settings.exclude_regexps(i) == ExcludeRegexps[i],
                "Incorrect exclude regexp at " << i << ": \"" << settings.exclude_regexps(i) << "\" instead of \"" << ExcludeRegexps[i] << "\"");
        }
    }

    void ClearExpectations() override {
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
};

class TImportFixture : public TCliTestFixture {
    void AddServices() override {
        TCliTestFixture::AddServices();
        AddService<TImportImpl>();
    }
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
}
