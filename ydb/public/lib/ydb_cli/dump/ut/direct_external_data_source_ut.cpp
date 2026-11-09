#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <atomic>

namespace NYdb::NDump {
namespace {

enum class EInstallationResponse {
    Missing,
    Empty,
    Cloud,
};

class TSourceTableService : public Ydb::Table::V1::TableService::Service {
public:
    std::atomic<EInstallationResponse> Installation = EInstallationResponse::Missing;
    std::atomic_uint DescribeCalls = 0;
    std::atomic_bool IncludeInstallation = false;

    grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Table::CreateSessionRequest*,
        Ydb::Table::CreateSessionResponse* response) override {
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("fake-session-id");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status DescribeExternalDataSource(grpc::ServerContext*, const Ydb::Table::DescribeExternalDataSourceRequest* request,
        Ydb::Table::DescribeExternalDataSourceResponse* response) override {
        ++DescribeCalls;
        IncludeInstallation.store(request->include_installation());
        Ydb::Table::DescribeExternalDataSourceResult result;
        result.mutable_self()->set_name("source");
        result.mutable_self()->set_type(Ydb::Scheme::Entry::EXTERNAL_DATA_SOURCE);
        result.mutable_self()->set_owner("root@builtin");
        result.set_source_type("ObjectStorage");
        result.set_location("https://bucket.example.test/data/prefix/");
        (*result.mutable_properties())["AUTH_METHOD"] = "NONE";
        const auto installation = Installation.load();
        if (request->include_installation() && installation != EInstallationResponse::Missing) {
            result.set_installation(installation == EInstallationResponse::Cloud ? "cloud" : "");
        }
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }
};

class TSourceSchemeService : public Ydb::Scheme::V1::SchemeService::Service {
public:
    std::atomic_uint Listings = 0;
    std::atomic_uint Writes = 0;

    grpc::Status ListDirectory(grpc::ServerContext*, const Ydb::Scheme::ListDirectoryRequest*,
        Ydb::Scheme::ListDirectoryResponse* response) override {
        ++Listings;
        Ydb::Scheme::ListDirectoryResult result;
        result.mutable_self()->set_name("source");
        result.mutable_self()->set_type(Ydb::Scheme::Entry::EXTERNAL_DATA_SOURCE);
        result.mutable_self()->set_owner("root@builtin");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status DescribePath(grpc::ServerContext*, const Ydb::Scheme::DescribePathRequest*,
        Ydb::Scheme::DescribePathResponse* response) override {
        Ydb::Scheme::DescribePathResult result;
        result.mutable_self()->set_name("source");
        result.mutable_self()->set_type(Ydb::Scheme::Entry::EXTERNAL_DATA_SOURCE);
        result.mutable_self()->set_owner("root@builtin");
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        operation->set_status(Ydb::StatusIds::SUCCESS);
        operation->mutable_result()->PackFrom(result);
        return grpc::Status::OK;
    }

    grpc::Status MakeDirectory(grpc::ServerContext*, const Ydb::Scheme::MakeDirectoryRequest*,
        Ydb::Scheme::MakeDirectoryResponse* response) override {
        ++Writes;
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::UNAUTHORIZED);
        return grpc::Status::OK;
    }
};

class TSourceServer {
public:
    TSourceTableService Table;
    TSourceSchemeService Scheme;
    std::unique_ptr<grpc::Server> Server;
    std::unique_ptr<TDriver> Driver;

    TSourceServer() {
        int port = 0;
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&Table);
        builder.RegisterService(&Scheme);
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
        Driver = std::make_unique<TDriver>(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << port)
            .SetDiscoveryMode(EDiscoveryMode::Off).SetDatabase("/Root"));
    }

    ~TSourceServer() {
        Driver->Stop(true);
        Server->Shutdown();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(DirectExternalDataSourceBackup) {
    Y_UNIT_TEST_TWIN(OldServerMustNotProduceACompletedArchive, SavePartialResult) {
        TSourceServer server;
        TTempDir tempDir;
        const auto archive = tempDir.Path().Child("dump");
        TClient client(*server.Driver);
        const auto result = client.Dump("/Root/source", archive, TDumpSettings().Database("/Root")
            .EnableDirectExternalDataSource(true).SavePartialResult(SavePartialResult));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "IncludeInstallation");
        UNIT_ASSERT(server.Table.IncludeInstallation.load());
        UNIT_ASSERT_VALUES_EQUAL(server.Scheme.Writes.load(), 0);
        UNIT_ASSERT(!archive.Child("source").Child(NFiles::CreateExternalDataSource().FileName).Exists());
        if (SavePartialResult) {
            UNIT_ASSERT(archive.Child(NFiles::Incomplete().FileName).Exists());
            UNIT_ASSERT(archive.Child("source").Child(NFiles::Incomplete().FileName).Exists());
        } else {
            UNIT_ASSERT(!archive.Exists());
        }
    }

    Y_UNIT_TEST(DisabledGateDoesNotRequestExpandedMetadata) {
        TSourceServer server;
        TTempDir tempDir;
        const auto archive = tempDir.Path().Child("dump");
        TClient client(*server.Driver);
        const auto result = client.Dump("/Root/source", archive, TDumpSettings().Database("/Root").SavePartialResult(true));
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(server.Table.DescribeCalls.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(server.Scheme.Listings.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(server.Scheme.Writes.load(), 0);
        UNIT_ASSERT(!archive.Exists());
    }

    Y_UNIT_TEST_TWIN(PresentEmptyAndCloudInstallationAreSerialized, NonEmpty) {
        TSourceServer server;
        server.Table.Installation = NonEmpty ? EInstallationResponse::Cloud : EInstallationResponse::Empty;
        TTempDir tempDir;
        const auto archive = tempDir.Path().Child("dump");
        TClient client(*server.Driver);
        const auto result = client.Dump("/Root/source", archive,
            TDumpSettings().Database("/Root").EnableDirectExternalDataSource(true));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(server.Table.IncludeInstallation.load());
        UNIT_ASSERT_VALUES_EQUAL(server.Scheme.Writes.load(), 0);
        const auto sql = TFileInput(archive.Child("source").Child(NFiles::CreateExternalDataSource().FileName)).ReadAll();
        UNIT_ASSERT_STRING_CONTAINS(sql, "LOCATION = 'https://bucket.example.test/data/prefix/'");
        if (NonEmpty) {
            UNIT_ASSERT_STRING_CONTAINS(sql, "INSTALLATION = 'cloud'");
        } else {
            UNIT_ASSERT(!sql.Contains("INSTALLATION"));
        }
    }
}

} // namespace NYdb::NDump
