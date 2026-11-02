#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <google/protobuf/util/message_differencer.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <string>
#include <utility>

namespace NKikimr::NGRpcService {
namespace {

using TTable = Ydb::Table::V1::TableService::Stub;

template <class TResult, class TRequest, class TResponse>
TResult Describe(
    TTable& stub,
    grpc::Status (TTable::*method)(grpc::ClientContext*, const TRequest&, TResponse*),
    TStringBuf database,
    TStringBuf path)
{
    grpc::ClientContext context;
    context.AddMetadata("x-ydb-database", std::string(database.data(), database.size()));
    context.AddMetadata("x-ydb-auth-ticket", "root@builtin");
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    TRequest request;
    request.set_path(std::string(path.data(), path.size()));
    request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
    TResponse response;
    const auto status = (stub.*method)(&context, request, &response);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    UNIT_ASSERT_C(response.operation().ready(), response.DebugString());
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.DebugString());

    TResult result;
    UNIT_ASSERT_C(response.operation().result().UnpackTo(&result), response.DebugString());
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeDatabaseDescribe) {
    Y_UNIT_TEST_TWIN(ExternalTableDataSourceUsesLogicalDatabase, enableRelativePaths) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableRelativePaths(enableRelativePaths);
        config.MutableFeatureFlags()->SetEnableExternalDataSources(true);
        auto* rule = config.MutableResourcePathPrefixMapping()->AddRules();
        rule->SetSrc("/Root/logical");
        rule->SetDst("/Root/physical");

        NYdb::TKikimrWithGrpcAndRootSchema server(config, {}, {}, false, nullptr, [](Tests::TServerSettings& settings) {
            settings.StoragePoolTypes.clear();
            settings.AddStoragePool("hdd");
            settings.StoragePoolTypes.at("hdd").SetStoragePoolId(0);
        });
        Ydb::Cms::CreateDatabaseRequest create;
        create.set_path("/Root/physical");
        auto* storage = create.mutable_resources()->add_storage_units();
        storage->set_unit_kind("hdd");
        storage->set_count(1);
        server.Tenants_->CreateTenant(std::move(create));
        for (const auto node : server.Tenants_->List("/Root/physical")) {
            server.GetServer().EnableGRpc(server.GetPortManager().GetPort(), node, "/Root/physical");
        }

        const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
        NYdb::TDriver driver(NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase("/Root/physical")
            .SetAuthToken("root@builtin"));
        NYdb::NQuery::TQueryClient queryClient(driver);
        const auto sessionResult = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        const auto created = session.ExecuteQuery(R"(
            CREATE EXTERNAL DATA SOURCE `/Root/physical/DataSource` WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "https://storage.example/Root/physical/bucket/",
                AUTH_METHOD = "NONE"
            );
            CREATE EXTERNAL TABLE `/Root/physical/ExternalTable` (
                key Utf8 NOT NULL,
                value Utf8 NOT NULL
            ) WITH (
                DATA_SOURCE = "/Root/physical/DataSource",
                LOCATION = "/Root/physical/objects/",
                FORMAT = "csv_with_names",
                COMPRESSION = "gzip"
            );
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), created.GetIssues().ToString());

        auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        const auto physical = Describe<Ydb::Table::DescribeExternalTableResult>(
            *stub, &TTable::DescribeExternalTable, "/Root/physical", "/Root/physical/ExternalTable");
        UNIT_ASSERT_VALUES_EQUAL(physical.data_source_path(), "/Root/physical/DataSource");
        UNIT_ASSERT_VALUES_EQUAL(physical.source_type(), "ObjectStorage");
        UNIT_ASSERT_VALUES_EQUAL(physical.location(), "/Root/physical/objects/");
        UNIT_ASSERT_VALUES_EQUAL(physical.content().at("FORMAT"), "[\"csv_with_names\"]");
        UNIT_ASSERT_VALUES_EQUAL(physical.content().at("COMPRESSION"), "[\"gzip\"]");

        auto expected = physical;
        expected.set_data_source_path("/Root/logical/DataSource");
        const auto absolute = Describe<Ydb::Table::DescribeExternalTableResult>(
            *stub, &TTable::DescribeExternalTable, "/Root/logical", "/Root/logical/ExternalTable");
        UNIT_ASSERT_C(google::protobuf::util::MessageDifferencer::Equals(expected, absolute),
            absolute.DebugString());

        const auto physicalSource = Describe<Ydb::Table::DescribeExternalDataSourceResult>(
            *stub, &TTable::DescribeExternalDataSource, "/Root/physical", "/Root/physical/DataSource");
        UNIT_ASSERT_VALUES_EQUAL(physicalSource.location(), "https://storage.example/Root/physical/bucket/");
        const auto absoluteSource = Describe<Ydb::Table::DescribeExternalDataSourceResult>(
            *stub, &TTable::DescribeExternalDataSource, "/Root/logical", "/Root/logical/DataSource");
        UNIT_ASSERT_C(google::protobuf::util::MessageDifferencer::Equals(physicalSource, absoluteSource),
            absoluteSource.DebugString());

        if (enableRelativePaths) {
            // Only the database header is relative; schema object paths remain absolute.
            // Without resolving the original header before response rewriting, this
            // succeeds but returns /Root/physical/DataSource instead of the alias.
            const auto relative = Describe<Ydb::Table::DescribeExternalTableResult>(
                *stub, &TTable::DescribeExternalTable, "logical", "/Root/logical/ExternalTable");
            UNIT_ASSERT_C(google::protobuf::util::MessageDifferencer::Equals(absolute, relative),
                relative.DebugString());
            const auto relativeSource = Describe<Ydb::Table::DescribeExternalDataSourceResult>(
                *stub, &TTable::DescribeExternalDataSource, "logical", "/Root/logical/DataSource");
            UNIT_ASSERT_C(google::protobuf::util::MessageDifferencer::Equals(physicalSource, relativeSource),
                relativeSource.DebugString());
        }
    }
}

} // namespace NKikimr::NGRpcService
