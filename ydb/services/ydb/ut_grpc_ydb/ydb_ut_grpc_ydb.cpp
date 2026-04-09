#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYdb {

Ydb::StatusIds::StatusCode WaitForStatus(
    std::shared_ptr<grpc::Channel> channel, const TString& opId, TString* error,
    const TString& database, int retries, TDuration sleepDuration
) {
    std::unique_ptr<Ydb::Operation::V1::OperationService::Stub> stub;
    stub = Ydb::Operation::V1::OperationService::NewStub(channel);
    Ydb::Operations::GetOperationRequest request;
    request.set_id(opId);
    Ydb::Operations::GetOperationResponse response;
    for (int retry = 0; retry <= retries; ++retry) {
        grpc::ClientContext context;
        context.AddMetadata("x-ydb-database", database);
        auto grpcStatus = stub->GetOperation(&context, request, &response);
        UNIT_ASSERT_C(grpcStatus.ok(), grpcStatus.error_message());
        if (response.operation().ready()) {
            break;
        }
        Sleep(sleepDuration *= 2);
    }
    if (error && response.operation().issues_size() > 0) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(response.operation().issues(), issues);
        *error = issues.ToString();
    }
    return response.operation().status();
}

}
}

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

static bool HasIssue(const NYql::TIssues& issues, ui32 code,
    std::function<bool(const NYql::TIssue& issue)> predicate = {})
{
    bool hasIssue = false;

    for (auto& issue : issues) {
        WalkThroughIssues(issue, false, [code, predicate, &hasIssue] (const NYql::TIssue& issue, int level) {
            Y_UNUSED(level);
            if (issue.GetCode() == code) {
                hasIssue = predicate
                    ? predicate(issue)
                    : true;
            }
        });
    }

    return hasIssue;
}

Ydb::Table::DescribeTableResult DescribeTable(std::shared_ptr<grpc::Channel> channel, const TString& sessionId, const TString& path)
{
    Ydb::Table::DescribeTableRequest request;
    request.set_session_id(sessionId);
    request.set_path(path);
    request.set_include_shard_key_bounds(true);

    Ydb::Table::DescribeTableResponse response;

    std::unique_ptr<Ydb::Table::V1::TableService::Stub> stub;
    stub = Ydb::Table::V1::TableService::NewStub(channel);
    grpc::ClientContext context;
    auto status = stub->DescribeTable(&context, request, &response);
    UNIT_ASSERT(status.ok());
    auto deferred = response.operation();
    UNIT_ASSERT(deferred.ready() == true);
    NYql::TIssues issues;
    NYql::IssuesFromMessage(deferred.issues(), issues);
    issues.PrintTo(Cerr);

    UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

    Ydb::Table::DescribeTableResult result;
    Y_ABORT_UNLESS(deferred.result().UnpackTo(&result));
    return result;
}


Y_UNIT_TEST_SUITE(TGRpcYdbTest) {
    Y_UNIT_TEST(RemoveNotExistedDirectory) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::RemoveDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheNotExistedDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::RemoveDirectoryResponse response;

            auto status = Stub_->RemoveDirectory(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            TString tmp = issues.ToString();
            TString expected = "<main>: Error: Path does not exist, code: 200200\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
    }

    Y_UNIT_TEST(MakeListRemoveDirectory) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::MakeDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::MakeDirectoryResponse response;

            auto status = Stub_->MakeDirectory(&context, request, &response);
            auto operation = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            //UNIT_ASSERT(operation.ready() == false); //Not finished yet
            //id = operation.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::ListDirectoryRequest request;
            TString scheme(
                "path: \"/Roo\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::ListDirectoryResponse response;

            auto status = Stub_->ListDirectory(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SCHEME_ERROR);
        }
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::ListDirectoryRequest request;
            TString scheme(
                "path: \"/Root\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::ListDirectoryResponse response;

            auto status = Stub_->ListDirectory(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Scheme::ListDirectoryResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            for (auto& child : *result.mutable_children()) {
                child.clear_created_at();
            }
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = "self {\n"
                "  name: \"Root\"\n"
                "  owner: \"root@builtin\"\n"
                "  type: DIRECTORY\n"
                "}\n"
                "children {\n"
                "  name: \".sys\"\n"
                "  owner: \"metadata@system\"\n"
                "  type: DIRECTORY\n"
                "}\n"
                "children {\n"
                "  name: \"TheDirectory\"\n"
                "  owner: \"root@builtin\"\n"
                "  type: DIRECTORY\n"
                "}\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Scheme::RemoveDirectoryRequest request;
            TString scheme(
                "path: \"/Root/TheDirectory\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::RemoveDirectoryResponse response;

            auto status = Stub_->RemoveDirectory(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
    }

    Y_UNIT_TEST(GetOperationBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            auto status = WaitForStatus(Channel_, "");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://...");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=42");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=aaa&sstid=bbbb");
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }

    }
/*
    Y_UNIT_TEST(GetOperationUnknownId) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            auto status = WaitForStatus(Channel_, "ydb://operation/1?txid=42&sstid=66");
            UNIT_ASSERT(status == Ydb::StatusIds::BAD_REQUEST);
        }
    }
*/
    Y_UNIT_TEST(CreateTableBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::CreateTableRequest request;
        Ydb::Table::CreateTableResponse response;

        auto status = Stub_->CreateTable(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok()); //GRpc layer - OK
        UNIT_ASSERT(deferred.ready() == true); //Ready to get status
        UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST); //But with error
    }

    static void CreateTableBadRequest(const TString& scheme,
        const TString& expectedMsg, const Ydb::StatusIds::StatusCode expectedStatus)
    {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        grpc::ClientContext context;
        Ydb::Table::CreateTableRequest request;
        ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

        Ydb::Table::CreateTableResponse response;

        auto status = Stub_->CreateTable(&context, request, &response);
        auto deferred = response.operation();
        UNIT_ASSERT(status.ok()); //GRpc layer - OK
        UNIT_ASSERT(deferred.ready() == true); //Ready to get status

        NYql::TIssues issues;
        NYql::IssuesFromMessage(deferred.issues(), issues);
        TString tmp = issues.ToString();

        UNIT_ASSERT_NO_DIFF(tmp, expectedMsg);
        UNIT_ASSERT_VALUES_EQUAL(deferred.status(), expectedStatus); //But with error
    }

    Y_UNIT_TEST(CreateTableBadRequest2) {
        TString scheme(R"___(
            path: "/Root/TheTable"
            columns { name: "Key"             type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "Value"           type: { optional_type { item { type_id: UTF8   } } } }
            primary_key: ["BlaBla"]
        )___");

        TString expected("<main>: Error: Unknown column 'BlaBla' specified in key column list\n");

        CreateTableBadRequest(scheme, expected, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(CreateTableBadRequest3) {
        TString scheme(R"___(
            path: "/Root/TheTable"
            columns { name: "Key"             type: { optional_type { item { type_id: UINT64 } } } }
            columns { name: "Value"           type: { optional_type { item { type_id: UTF8   } } } }
        )___");

        TString expected("<main>: Error: At least one primary key should be specified\n");

        CreateTableBadRequest(scheme, expected, Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(DropTableBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::DropTableRequest request;
            Ydb::Table::DropTableResponse response;

            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok()); //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Ready to get status
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST); //But with error
        }
        {
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::DropTableRequest request;
            TString scheme(
                "path: \"/Root/NotExists\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::DropTableResponse response;

            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok()); //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Ready to get status
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR); //But with error
        }

    }

    Y_UNIT_TEST(AlterTableAddIndexBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            context.AddMetadata("x-ydb-database", "/Root");
            Ydb::Table::AlterTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "add_indexes { name: \"ByValue\" index_columns: \"Value\" }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::AlterTableResponse response;

            auto status = Stub_->AlterTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST);
            NYdb::NIssue::TIssues issues;
            NYdb::NIssue::IssuesFromMessage(deferred.issues(), issues);
            UNIT_ASSERT(issues.ToString().contains("Invalid or unset index type"));
        }
    }

    Y_UNIT_TEST(CreateAlterCopyAndDropTable) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DescribeTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DescribeTableResponse response;

            auto status = Stub_->DescribeTable(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::DescribeTableResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = R"___(self {
  name: "TheTable"
  owner: "root@builtin"
  type: TABLE
}
columns {
  name: "Key"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
columns {
  name: "Value"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
primary_key: "Key"
partitioning_settings {
  partitioning_by_size: DISABLED
  partitioning_by_load: DISABLED
  min_partitions_count: 1
}
)___";
           UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            std::unique_ptr<Ydb::Scheme::V1::SchemeService::Stub> Stub_;
            Stub_ = Ydb::Scheme::V1::SchemeService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Scheme::DescribePathRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Scheme::DescribePathResponse response;

            auto status = Stub_->DescribePath(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Scheme::DescribePathResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = "self {\n"
            "  name: \"TheTable\"\n"
            "  owner: \"root@builtin\"\n"
            "  type: TABLE\n"
            "}\n";
            UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::AlterTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "add_columns { name: \"Value2\"           type: { optional_type { item { type_id: UTF8 } } } }"
                "drop_columns: [\"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::AlterTableResponse response;

            auto status = Stub_->AlterTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CopyTableRequest request;
            TString scheme(
                "source_path: \"/Root/TheTable\""
                "destination_path: \"/Root/TheTable2\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CopyTableResponse response;
            auto status = Stub_->CopyTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CopyTablesRequest request;
            TString scheme(R"(
                tables {
                    source_path: "/Root/TheTable"
                    destination_path: "/Root/TheTable3"
                }
                tables {
                    source_path: "/Root/TheTable2"
                    destination_path: "/Root/TheTable4"
                }
            )");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CopyTablesResponse response;
            auto status = Stub_->CopyTables(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        id.clear();
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DropTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DropTableResponse response;
            auto status = Stub_->DropTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
    }
    Y_UNIT_TEST(CreateTableWithIndex) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->GetAppData().AllowPrivateTableDescribeForTest = true;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
/*
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { table_profile { partitioning_policy { uniform_partitions: 16 } } } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::GENERIC_ERROR);
        }
*/
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable1\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::TypedValue point;
            auto &keyType = *point.mutable_type()->mutable_tuple_type();
            keyType.add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
            auto &keyVal = *point.mutable_value();
            keyVal.add_items()->set_text_value("q");
            auto index = request.add_indexes();
            index->set_name("IndexedValue");
            index->add_index_columns("IValue");
//            auto points = index->mutable_global_index()->mutable_table_profile()->mutable_partitioning_policy()->mutable_explicit_partitions();
//            points->add_split_points()->CopyFrom(point);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
        }
/*
        {
            TString sessionId = CreateSession(channel);
            auto result = DescribeTable(channel, sessionId, "/Root/TheTable1/IndexedValue/indexImplTable");
            const TString expected = R"__(type {
  tuple_type {
    elements {
      optional_type {
        item {
          type_id: UTF8
        }
      }
    }
    elements {
      optional_type {
        item {
          type_id: UINT64
        }
      }
    }
  }
}
value {
  items {
    text_value: "q"
  }
  items {
    null_flag_value: NULL_VALUE
  }
}
)__";
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result.shard_key_bounds(0), &tmp);
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(tmp, expected);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable2\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UINT32 } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { table_profile { partitioning_policy { uniform_partitions: 16 } } } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
        }

        {
            TString sessionId = CreateSession(channel);
            auto result = DescribeTable(channel, sessionId, "/Root/TheTable2/IndexedValue/indexImplTable");
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds().size(), 15);
            UNIT_ASSERT_VALUES_EQUAL(result.shard_key_bounds(0).value().items(0).uint32_value(), ((1ull << 32) - 1) / 16);
        }
*/

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"IValue\"          type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]"
                "indexes { name: \"IndexedValue\"    index_columns:   [\"IValue\"]        global_index { } }"
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);

            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::DescribeTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::DescribeTableResponse response;

            auto status = Stub_->DescribeTable(&context, request, &response);
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::DescribeTableResult result;
            response.operation().result().UnpackTo(&result);
            result.mutable_self()->clear_created_at(); // variadic part
            TString tmp;
            google::protobuf::TextFormat::PrintToString(result, &tmp);
            const TString expected = R"___(self {
  name: "TheTable"
  owner: "root@builtin"
  type: TABLE
}
columns {
  name: "Key"
  type {
    optional_type {
      item {
        type_id: UINT64
      }
    }
  }
}
columns {
  name: "IValue"
  type {
    optional_type {
      item {
        type_id: UTF8
      }
    }
  }
}
primary_key: "Key"
indexes {
  name: "IndexedValue"
  index_columns: "IValue"
  global_index {
    settings {
      partitioning_settings {
        partitioning_by_size: ENABLED
        partition_size_mb: 2048
        partitioning_by_load: DISABLED
        min_partitions_count: 1
      }
    }
  }
  status: STATUS_READY
}
partitioning_settings {
  partitioning_by_size: DISABLED
  partitioning_by_load: DISABLED
  min_partitions_count: 1
}
)___";
           UNIT_ASSERT_NO_DIFF(tmp, expected);
        }
        {
            TString sessionId = CreateSession(channel);
            const TString query1(R"(
            UPSERT INTO `/Root/TheTable` (Key, IValue) VALUES
                (1, "Secondary1"),
                (2, "Secondary2"),
                (3, "Secondary3");
            )");
            ExecYql(channel, sessionId, query1);
        }

        {
            TString sessionId = CreateSession(channel);
            ExecYql(channel, sessionId,
                "SELECT * FROM `/Root/TheTable`;");
        }

    }

    Y_UNIT_TEST(CreateYqlSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
        }
    }

    Y_UNIT_TEST(CreateDeleteYqlSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::DeleteSessionRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::DeleteSessionResponse response;

            auto status = Stub_->DeleteSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
        }

    }

    Y_UNIT_TEST(ExecuteQueryBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryImplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("SELECT 1 as a, 'qwerty' as b, 43.5 as c UNION ALL SELECT 11 as a, 'asdfgg' as b, Null as c;");
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryExplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = Stub_->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT 1 as a, 'qwerty' as b, 43.5 as c UNION ALL SELECT 11 as a, 'asdfgg' as b, Null as c;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"a\"\n"
                    "    type {\n"
                    "      type_id: INT32\n"
                    "    }\n"
                    "  }\n"
                    "  columns {\n"
                    "    name: \"b\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  columns {\n"
                    "    name: \"c\"\n"
                    "    type {\n"
                    "      optional_type {\n"
                    "        item {\n"
                    "          type_id: DOUBLE\n"
                    "        }\n"
                    "      }\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      int32_value: 1\n"
                    "    }\n"
                    "    items {\n"
                    "      bytes_value: \"qwerty\"\n"
                    "    }\n"
                    "    items {\n"
                    "      double_value: 43.5\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      int32_value: 11\n"
                    "    }\n"
                    "    items {\n"
                    "      bytes_value: \"asdfgg\"\n"
                    "    }\n"
                    "    items {\n"
                    "      null_flag_value: NULL_VALUE\n"
                    "    }\n"
                    "  }\n"
                    "  format: FORMAT_VALUE\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
                TResultSet resultSet(result.result_sets(0));
                UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 3);

                int row = 0;
                TResultSetParser rsParser(resultSet);
                while (rsParser.TryNextRow()) {
                    switch (row) {
                        case 0: {
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 1);
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetString(), "qwerty");
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(2).GetOptionalDouble(), 43.5);
                        }
                        break;
                        case 1: {
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetInt32(), 11);
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(1).GetString(), "asdfgg");
                            rsParser.ColumnParser(2).OpenOptional();
                            UNIT_ASSERT_EQUAL(rsParser.ColumnParser(2).IsNull(), true);
                        }
                        break;
                        default: {
                            UNIT_ASSERT(false);
                        }
                    }
                    row++;
                }
            }
        }
    }

    Y_UNIT_TEST(ExecuteQueryWithUuid) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"___(SELECT CAST("5ca32c22-841b-11e8-adc0-fa7ae01bbebc" AS Uuid);)___");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString expected = R"___(result_sets {
  columns {
    name: "column0"
    type {
      optional_type {
        item {
          type_id: UUID
        }
      }
    }
  }
  rows {
    items {
      low_128: 1290426546294828066
      high_128: 13600338575655354541
    }
  }
  format: FORMAT_VALUE
}
tx_meta {
}
)___";
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
    }

    Y_UNIT_TEST(SdkUuid) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT CAST("5ca32c22-841b-11e8-adc0-fa7ae01bbebc" AS Uuid);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT(result.IsSuccess());

        TString expectedJson = R"({"column0":"5ca32c22-841b-11e8-adc0-fa7ae01bbebc"}
)";
        UNIT_ASSERT_VALUES_EQUAL(expectedJson, NYdb::FormatResultSetJson(result.GetResultSet(0), NYdb::EBinaryStringEncoding::Base64));

        UNIT_ASSERT_VALUES_EQUAL(R"([[["5ca32c22-841b-11e8-adc0-fa7ae01bbebc"]]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(SdkUuidViaParams) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NTable::TTableClient(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto param = client.GetParamsBuilder()
            .AddParam("$in")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("u").Uuid(TUuidValue("5ca32c22-841b-11e8-adc0-fa7ae01bbebc"))
                    .EndStruct()
                .EndList()
                .Build()
            .Build();
        auto result = session.ExecuteDataQuery(R"(
            DECLARE $in AS List<Struct<u: Uuid>>;
            SELECT * FROM AS_TABLE($in);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), param).ExtractValueSync();

        UNIT_ASSERT(result.IsSuccess());

        TString expectedJson = R"({"u":"5ca32c22-841b-11e8-adc0-fa7ae01bbebc"}
)";
        UNIT_ASSERT_VALUES_EQUAL(expectedJson, NYdb::FormatResultSetJson(result.GetResultSet(0), NYdb::EBinaryStringEncoding::Base64));

        UNIT_ASSERT_VALUES_EQUAL(R"([["5ca32c22-841b-11e8-adc0-fa7ae01bbebc"]])", NYdb::FormatResultSetYson(result.GetResultSet(0)));
    }


    Y_UNIT_TEST(ExecuteQueryWithParametersBadRequest) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
        Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
        Ydb::Table::ExecuteDataQueryRequest request;
        request.set_session_id(sessionId);
        request.mutable_query()->set_yql_text("DECLARE $param1 AS Tuple<Int32,Bool>; SELECT $param1 AS Tuple;");
        request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        request.mutable_tx_control()->set_commit_tx(true);

        {
            // Bad type Kind
            ::google::protobuf::Map<TString, Ydb::TypedValue> parameters;

            const TString type = R"(
                type_id: TYPE_UNDEFINED
            )";
            google::protobuf::TextFormat::ParseFromString(type, parameters["$param1"].mutable_type());

            const TString value = R"(
                int32_value: 10
            )";
            google::protobuf::TextFormat::ParseFromString(value, parameters["$param1"].mutable_value());

            *request.mutable_parameters() = parameters;
            Ydb::Table::ExecuteDataQueryResponse response;
            grpc::ClientContext context;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }

        {
            // Value mismatch
            ::google::protobuf::Map<TString, Ydb::TypedValue> parameters;

            const TString type = R"(
                tuple_type {
                    elements {
                        type_id: Int32
                    }
                    elements {
                        type_id: Bool
                    }
                }
            )";
            google::protobuf::TextFormat::ParseFromString(type, parameters["$param1"].mutable_type());

            const TString value = R"(
                int32_value: 10
            )";
            google::protobuf::TextFormat::ParseFromString(value, parameters["$param1"].mutable_value());

            *request.mutable_parameters() = parameters;
            Ydb::Table::ExecuteDataQueryResponse response;
            grpc::ClientContext context;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(ExecuteQueryWithParametersExplicitSession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: STRING\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "bytes_value: \"Paul\"\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(request, &tmp);
                const TString expected =
                    "tx_control {\n"
                    "  begin_tx {\n"
                    "    serializable_read_write {\n"
                    "    }\n"
                    "  }\n"
                    "  commit_tx: true\n"
                    "}\n"
                    "query {\n"
                    "  yql_text: \"DECLARE $paramName AS String; SELECT $paramName;\"\n"
                    "}\n"
                    "parameters {\n"
                    "  key: \"$paramName\"\n"
                    "  value {\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "    value {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"column0\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "  format: FORMAT_VALUE\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
        // Check Uuid protos as parametr
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.mutable_query()->set_yql_text("DECLARE $paramName AS Uuid; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: UUID\n";
                UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type()));
                const TString value = R"(low_128: 1290426546294828066
                                         high_128: 13600338575655354541)";
                UNIT_ASSERT(google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value()));
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            request.set_session_id(sessionId);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected = R"___(result_sets {
  columns {
    name: "column0"
    type {
      type_id: UUID
    }
  }
  rows {
    items {
      low_128: 1290426546294828066
      high_128: 13600338575655354541
    }
  }
  format: FORMAT_VALUE
}
tx_meta {
}
)___";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }

    }

    Y_UNIT_TEST(ExecuteDmlQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteSchemeQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                CREATE TABLE `Root/TheTable` (
                    Key UINT64,
                    Value UTF8,
                    PRIMARY KEY (Key)
                );
            )");

            Ydb::Table::ExecuteSchemeQueryResponse response;
            auto status = Stub_->ExecuteSchemeQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExplainDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (1, "One");
            )");

            Ydb::Table::ExplainDataQueryResponse response;
            auto status = Stub_->ExplainDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        TString txId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (1, "One");
            )");

            auto& txControl = *request.mutable_tx_control();
            txControl.mutable_begin_tx()->mutable_serializable_read_write();

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);

            txId = result.tx_meta().id();
            UNIT_ASSERT(!txId.empty());
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text(R"(
                UPSERT INTO `Root/TheTable` (Key, Value)
                VALUES (2, "Two");
            )");

            auto& txControl = *request.mutable_tx_control();
            txControl.set_tx_id(txId);
            txControl.set_commit_tx(true);

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        TString queryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::PrepareDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text(R"(
                DECLARE $Key AS Uint64;
                SELECT * FROM `Root/TheTable` WHERE Key < $Key;
            )");

            Ydb::Table::PrepareDataQueryResponse response;
            auto status = Stub_->PrepareDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());

            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::PrepareQueryResult result;
            deferred.result().UnpackTo(&result);
            queryId = result.query_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;

            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(queryId);

            auto& txControl = *request.mutable_tx_control();
            txControl.mutable_begin_tx()->mutable_online_read_only();
            txControl.set_commit_tx(true);

            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: UINT64\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "uint64_value: 5\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$Key"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();

            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);

            UNIT_ASSERT(result.tx_meta().id().empty());
            UNIT_ASSERT_VALUES_EQUAL(result.result_sets(0).rows_size(), 2);
        }
    }

    Y_UNIT_TEST(CreateYqlSessionExecuteQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT 1, \"qq\"; SELECT 2;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("SELECT * from `Root/NotFound`");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SCHEME_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            UNIT_ASSERT(HasIssue(issues, NYql::TIssuesIds::KIKIMR_SCHEME_ERROR));
        }

    }

    Y_UNIT_TEST(ExecutePreparedQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        TString preparedQueryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::PrepareDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            Ydb::Table::PrepareDataQueryResponse response;
            auto status = Stub_->PrepareDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::PrepareQueryResult result;

            deferred.result().UnpackTo(&result);
            preparedQueryId = result.query_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(preparedQueryId);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::TypedValue parameter;
            {
                const TString type =
                    "type_id: STRING\n";
                google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
                const TString value = "bytes_value: \"Paul\"\n";
                google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
            }

            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            {
                TString tmp;
                google::protobuf::TextFormat::PrintToString(result, &tmp);
                const TString expected =
                    "result_sets {\n"
                    "  columns {\n"
                    "    name: \"column0\"\n"
                    "    type {\n"
                    "      type_id: STRING\n"
                    "    }\n"
                    "  }\n"
                    "  rows {\n"
                    "    items {\n"
                    "      bytes_value: \"Paul\"\n"
                    "    }\n"
                    "  }\n"
                    "  format: FORMAT_VALUE\n"
                    "}\n"
                    "tx_meta {\n"
                    "}\n";
                UNIT_ASSERT_NO_DIFF(tmp, expected);
            }
        }
    }

    Y_UNIT_TEST(ExecuteQueryCache) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        Ydb::TypedValue parameter;
        {
            const TString type =
                "type_id: STRING\n";
            google::protobuf::TextFormat::ParseFromString(type, parameter.mutable_type());
            const TString value = "bytes_value: \"Paul\"\n";
            google::protobuf::TextFormat::ParseFromString(value, parameter.mutable_value());
        }

        TString queryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DECLARE $paramName AS String; SELECT $paramName;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            UNIT_ASSERT(result.has_query_meta());
            queryId = result.query_meta().id();
            UNIT_ASSERT(!queryId.empty());
            UNIT_ASSERT(!result.query_meta().parameters_types().empty());
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id(queryId);
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            request.mutable_query_cache_policy()->set_keep_in_cache(true);
            auto& map = *request.mutable_parameters();
            map["$paramName"] = parameter;

            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::ExecuteQueryResult result;
            deferred.result().UnpackTo(&result);
            UNIT_ASSERT(!result.has_query_meta());
        }
    }

    Y_UNIT_TEST(ExplainQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type: { item: { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type: { item: { type_id: UTF8   } } } }"
                "primary_key: [\"Key\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            TString tmp = issues.ToString();
            Cerr << tmp << Endl;

            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */

        TString sessionId;
        TString preparedQueryId;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("UPSERT INTO `Root/TheTable` (Key, Value) VALUES (42, \"data\");");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExplainDataQueryRequest request;
            request.set_session_id(sessionId);
            request.set_yql_text("SELECT COUNT(*) FROM `Root/TheTable`;");
            Ydb::Table::ExplainDataQueryResponse response;
            auto status = Stub_->ExplainDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(DeleteFromAfterCreate) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;

        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\", \"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable2\""
                "columns {\n"
                "  name: \"id_a\"\n"
                "  type: { optional_type { item { type_id: INT32 } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_b\"\n"
                "  type: { optional_type { item { type_id: INT64 } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_c\"\n"
                "  type: { optional_type { item { type_id: STRING } } }"
                "}\n"
                "columns {\n"
                "  name: \"id_d\"\n"
                "  type: { optional_type { item { type_id: STRING } } }"
                "}\n"
                "primary_key: \"id_a\"\n"
                "primary_key: \"id_b\"\n"
                "primary_key: \"id_c\"\n"
                "primary_key: \"id_d\"");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            //id = deferred.id();
        }
        /*
        {
            auto status = WaitForStatus(Channel_, id);
            UNIT_ASSERT(status == Ydb::StatusIds::SUCCESS);
        }
        */
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DELETE FROM `Root/TheTable`;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_yql_text("DELETE FROM `Root/TheTable2`;");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(ReadTable) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;

        TVector<std::tuple<ui64, TString>> data = {
            {42, "data42"},
            {43, "data43"},
            {44, "data44"},
            {45, "data45"},
            {46, "data46"},
            {47, "data47"},
            {48, "data48"},
            {49, "data49"},
            {50, "data50"},
            {51, "data51"},
            {52, "data52"}
        };
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { optional_type { item { type_id: UINT64 } } } }"
                "columns { name: \"Value\"           type: { optional_type { item { type_id: UTF8   } } } }"
                "primary_key: [\"Key\", \"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); //Not finished yet
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            TStringBuilder requestBuilder;
            requestBuilder << "UPSERT INTO `Root/TheTable` (Key, Value) VALUES";
            for (auto pair : data) {
                requestBuilder << "(" << std::get<0>(pair) << ", \"" << std::get<1>(pair) << "\"),";
            }
            TString req(requestBuilder);
            req.back() = ';';
            request.mutable_query()->set_yql_text(req);

            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            // Empty request - we expect to get BAD_REQUEST response
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::BAD_REQUEST);
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                // Expect all data in first response message
                if (res) {
                    UNIT_ASSERT_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 0;
                        UNIT_ASSERT_EQUAL((size_t)response.result().result_set().rows_size(), data.size());
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto greater = keyRange->mutable_greater();
            greater->mutable_type()->mutable_tuple_type()->add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            greater->mutable_value()->add_items()->set_uint64_value(50);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 9;
                        UNIT_ASSERT(response.result().result_set().rows_size() == 2);
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto less = keyRange->mutable_less_or_equal();
            less->mutable_type()->mutable_tuple_type()->add_elements()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);
            less->mutable_value()->add_items()->set_uint64_value(50);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT(response.status() == Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        UNIT_ASSERT(response.result().result_set().rows_size() == 9);
                        size_t i = 0;
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_EQUAL(std::get<0>(pair), row.items(0).uint64_value());
                            UNIT_ASSERT_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ReadTablePg) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetEnableTablePgTypes(true);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        TString sessionId;

        TVector<std::tuple<ui64, TString>> data = {
            {42, "data42"},
            {43, "data43"},
            {44, "data44"},
            {45, "data45"},
            {46, "data46"},
            {47, "data47"},
            {48, "data48"},
            {49, "data49"},
            {50, "data50"},
            {51, "data51"},
            {52, "data52"}
        };
        TString id;
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateTableRequest request;
            TString scheme(
                "path: \"/Root/TheTable\""
                "columns { name: \"Key\"             type: { pg_type { type_name: \"pgint8\" } } }"
                "columns { name: \"Value\"           type: { pg_type { type_name: \"pgtext\" } } }"
                "primary_key: [\"Key\", \"Value\"]");
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            Ydb::Table::CreateTableResponse response;

            auto status = Stub_->CreateTable(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  // GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true); // Not finished yet
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::CreateSessionRequest request;
            Ydb::Table::CreateSessionResponse response;

            auto status = Stub_->CreateSession(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            Ydb::Table::CreateSessionResult result;

            deferred.result().UnpackTo(&result);
            sessionId = result.session_id();
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            TStringBuilder requestBuilder;
            requestBuilder << "UPSERT INTO `Root/TheTable` (Key, Value) VALUES";
            for (auto pair : data) {
                requestBuilder << "(" << std::get<0>(pair) << "pi, \"" << std::get<1>(pair) << "\"pt),";
            }
            TString req(requestBuilder);
            req.back() = ';';
            request.mutable_query()->set_yql_text(req);

            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_tx_control()->set_commit_tx(true);
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::SUCCESS);
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            // Empty request - we expect to get BAD_REQUEST response
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::BAD_REQUEST);
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                // Expect all data in first response message
                if (res) {
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 0;
                        UNIT_ASSERT_VALUES_EQUAL((size_t)response.result().result_set().rows_size(), data.size());
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_VALUES_EQUAL(ToString(std::get<0>(pair)), row.items(0).text_value());
                            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto greater = keyRange->mutable_greater();
            greater->mutable_type()->mutable_tuple_type()->add_elements()->mutable_pg_type()->set_type_name("pgint8");
            greater->mutable_value()->add_items()->set_text_value("50");
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        size_t i = 9;
                        UNIT_ASSERT_VALUES_EQUAL(response.result().result_set().rows_size(), 2);
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_VALUES_EQUAL(ToString(std::get<0>(pair)), row.items(0).text_value());
                            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(Channel_);
            grpc::ClientContext context;
            Ydb::Table::ReadTableRequest request;
            Ydb::Table::ReadTableResponse response;

            TString scheme(
                "path: \"/Root/TheTable\""
            );
            ::google::protobuf::TextFormat::ParseFromString(scheme, &request);
            auto keyRange = request.mutable_key_range();
            auto less = keyRange->mutable_less_or_equal();
            less->mutable_type()->mutable_tuple_type()->add_elements()->mutable_pg_type()->set_type_name("pgint8");
            less->mutable_value()->add_items()->set_text_value("50");
            auto reader = Stub_->StreamReadTable(&context, request);
            bool res = true;
            while (res) {
                res = reader->Read(&response);
                if (res) {
                    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
                    if (response.result().has_result_set()) {
                        UNIT_ASSERT_VALUES_EQUAL(response.result().result_set().rows_size(), 9);
                        size_t i = 0;
                        for (const auto& row : response.result().result_set().rows()) {
                            const auto& pair = data[i++];
                            UNIT_ASSERT_VALUES_EQUAL(ToString(std::get<0>(pair)), row.items(0).text_value());
                            UNIT_ASSERT_VALUES_EQUAL(std::get<1>(pair), row.items(1).text_value());
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(OperationTimeout) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Draft::Dummy::DummyService::Stub> Stub_;
            Stub_ = Draft::Dummy::DummyService::NewStub(Channel_);
            grpc::ClientContext context;

            Draft::Dummy::InfiniteRequest request;
            Draft::Dummy::InfiniteResponse response;

            request.mutable_operation_params()->mutable_operation_timeout()->set_nanos(100000000); // 100ms

            auto status = Stub_->Infinite(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::TIMEOUT);
        }
    }

    Y_UNIT_TEST(OperationCancelAfter) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());
        {
            std::unique_ptr<Draft::Dummy::DummyService::Stub> Stub_;
            Stub_ = Draft::Dummy::DummyService::NewStub(Channel_);
            grpc::ClientContext context;

            Draft::Dummy::InfiniteRequest request;
            Draft::Dummy::InfiniteResponse response;

            request.mutable_operation_params()->mutable_cancel_after()->set_nanos(100000000); // 100ms

            auto status = Stub_->Infinite(&context, request, &response);
            auto deferred = response.operation();
            UNIT_ASSERT(status.ok());  //GRpc layer - OK
            UNIT_ASSERT(deferred.ready() == true);

            NYql::TIssues issues;
            NYql::IssuesFromMessage(deferred.issues(), issues);
            issues.PrintTo(Cerr);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::CANCELLED);
        }
    }

    Y_UNIT_TEST(KeepAlive) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId = CreateSession(channel);

        {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::KeepAliveRequest request;
            request.set_session_id(sessionId);
            Ydb::Table::KeepAliveResponse response;
            auto status = Stub_->KeepAlive(&context, request, &response);
            UNIT_ASSERT(status.ok());
            auto deferred = response.operation();
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT(deferred.status() == Ydb::StatusIds::SUCCESS);

            Ydb::Table::KeepAliveResult result;
            deferred.result().UnpackTo(&result);

            UNIT_ASSERT(result.session_status() == Ydb::Table::KeepAliveResult::SESSION_STATUS_READY);
        }
    }

    Y_UNIT_TEST(BeginTxRequestError) {
        TVector<NKikimrKqp::TKqpSetting> settings;
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpMaxActiveTxPerSession");
        setting.SetValue("2");
        settings.push_back(setting);

        TKikimrWithGrpcAndRootSchema server(NKikimrConfig::TAppConfig(), settings);
        ui16 grpc = server.GetPort();

        std::shared_ptr<grpc::Channel> channel;
        channel = grpc::CreateChannel("localhost:" + ToString(grpc), grpc::InsecureChannelCredentials());

        TString sessionId = CreateSession(channel);

        for (ui32 i = 0; i < 3; ++i) {
            std::unique_ptr<Ydb::Table::V1::TableService::Stub> Stub_;
            Stub_ = Ydb::Table::V1::TableService::NewStub(channel);
            grpc::ClientContext context;
            Ydb::Table::ExecuteDataQueryRequest request;
            request.set_session_id(sessionId);
            request.mutable_query()->set_id("ydb://preparedqueryid/0?id=bad_query");
            request.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
            Ydb::Table::ExecuteDataQueryResponse response;
            auto status = Stub_->ExecuteDataQuery(&context, request, &response);
            auto deferred = response.operation();

            UNIT_ASSERT(status.ok());
            UNIT_ASSERT(deferred.ready() == true);
            UNIT_ASSERT_VALUES_EQUAL(deferred.status(), Ydb::StatusIds::NOT_FOUND);
        }
    }
}

} // namespace NKikimr

