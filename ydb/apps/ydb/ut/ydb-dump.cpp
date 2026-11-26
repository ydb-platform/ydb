#include "mock_env.h"
#include "mock_table.h"

#include <util/string/split.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/lib/ydb_cli/dump/dump.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>

#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <google/protobuf/text_format.h>

#include <atomic>

namespace {

class TDatabaseRestoreScheme : public TSchemeImpl {
public:
    grpc::Status DescribePath(grpc::ServerContext*, const Ydb::Scheme::DescribePathRequest* request, Ydb::Scheme::DescribePathResponse* response) override {
        CHECK_EXP(request->path() == Target + "/dest", "Incorrect restore destination: " << request->path());
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::UNAUTHORIZED);
        return grpc::Status::OK;
    }

    grpc::Status ListDirectory(grpc::ServerContext* context, const Ydb::Scheme::ListDirectoryRequest* request, Ydb::Scheme::ListDirectoryResponse* response) override {
        ++RootLookups;
        return TSchemeImpl::ListDirectory(context, request, response);
    }

    grpc::Status ModifyPermissions(grpc::ServerContext* context, const Ydb::Scheme::ModifyPermissionsRequest* request, Ydb::Scheme::ModifyPermissionsResponse* response) override {
        ++PermissionsCalls;
        CHECK_EXP(request->path() == Target, "Incorrect ACL target: " << request->path());
        CheckClientMetadata(context, "x-ydb-database", Target);
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }

    grpc::Status MakeDirectory(grpc::ServerContext* context, const Ydb::Scheme::MakeDirectoryRequest* request, Ydb::Scheme::MakeDirectoryResponse* response) override {
        ++ContentCalls;
        CHECK_EXP(request->path() == Target + "/orders", "Incorrect content target: " << request->path());
        CheckClientMetadata(context, "x-ydb-database", Target);
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }

    TString Target = TEST_DATABASE + "/tenant";
    std::atomic<size_t> RootLookups{0};
    std::atomic<size_t> PermissionsCalls{0};
    std::atomic<size_t> ContentCalls{0};
};

class TDatabaseRestoreCms : public TMockGrpcServiceBase<Ydb::Cms::V1::CmsService::Service> {
public:
    grpc::Status CreateDatabase(grpc::ServerContext*, const Ydb::Cms::CreateDatabaseRequest* request, Ydb::Cms::CreateDatabaseResponse* response) override {
        CHECK_EXP(request->path() == TEST_DATABASE + "/tenant", "Incorrect database target: " << request->path());
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::ALREADY_EXISTS);
        return grpc::Status::OK;
    }
};

class TDatabaseRestoreTable : public TMockTableService {
public:
    grpc::Status ExecuteSchemeQuery(grpc::ServerContext*, const Ydb::Table::ExecuteSchemeQueryRequest*, Ydb::Table::ExecuteSchemeQueryResponse* response) override {
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }
};

class TDatabaseRestoreFixture : public TCliTestFixture {
    void AddServices() override {
        AddService<TDiscoveryImpl>("localhost", FromString<ui16>(TStringBuf(GetAddress()).RNextTok(':')));
        AddService<TDatabaseRestoreScheme>();
        AddService<TDatabaseRestoreCms>();
        AddService<TDatabaseRestoreTable>();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(YdbDump) {

Y_UNIT_TEST_F(RelativeDestinationUsesTheConfiguredDatabase, TDatabaseRestoreFixture) {
    for (const bool relative : {false, true}) {
        auto& scheme = Service<TDatabaseRestoreScheme>();
        scheme.ClearExpectations();
        scheme.RootLookups = 0;
        const TString database = relative ? "tenant" : scheme.Target;
        scheme.ExpectedDatabase = database;
        scheme.ExpectListDirectory("/").ExpectChild("/", TString(TStringBuf(TEST_DATABASE).SubStr(1)), Ydb::Scheme::Entry::DATABASE);
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetAddress()).SetDatabase(database));
        NYdb::NDump::TClient client(driver);
        UNIT_ASSERT_VALUES_EQUAL(client.Restore("unused", "dest").GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(scheme.RootLookups.load(), relative ? 1 : 0);
        CheckExpectations();
    }
}

Y_UNIT_TEST_F(DatabaseRestoreUsesOneTarget, TDatabaseRestoreFixture) {
    using namespace NYdb::NDump;
    TTempDir dump;
    const TFsPath path(dump.Name());
    for (const auto* file : {&NFiles::CreateUser(), &NFiles::CreateGroup(), &NFiles::AlterGroup(), &NFiles::Permissions()}) {
        TFileOutput(path.Child(file->FileName)).Write("");
    }
    TFileOutput(path.Child(NFiles::Database().FileName)).Write(Sprintf("path: \"%s/tenant\" resources {}", TEST_DATABASE.c_str()));
    path.Child("orders").MkDirs();
    TFileOutput(path.Child("orders").Child(NFiles::Empty().FileName)).Write("");

    for (const bool relative : {false, true}) {
        for (const bool withContent : {false, true}) {
            auto& scheme = Service<TDatabaseRestoreScheme>();
            scheme.ClearExpectations();
            scheme.RootLookups = scheme.PermissionsCalls = scheme.ContentCalls = 0;
            const TString database = relative ? "tenant" : scheme.Target;
            const TString sourceDatabase = TEST_DATABASE + "/source";
            scheme.ExpectedDatabase = sourceDatabase;
            scheme.ExpectListDirectory("/").ExpectChild("/", TString(TStringBuf(TEST_DATABASE).SubStr(1)), Ydb::Scheme::Entry::DATABASE);
            NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetAddress()).SetDatabase(sourceDatabase));
            TClient client(driver);
            const auto result = client.RestoreDatabase(path.GetPath(), TRestoreDatabaseSettings().Database(database).WithContent(withContent));
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(scheme.RootLookups.load(), relative ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(scheme.PermissionsCalls.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(scheme.ContentCalls.load(), withContent ? 1 : 0);
            CheckExpectations();
        }
    }
}

Y_UNIT_TEST_F(RestorePropagatesRootLookupError, TCliTestFixture) {
    Service<TSchemeImpl>().ExpectedDatabase = "tenant";
    Service<TSchemeImpl>().ListDirectoryStatus = Ydb::StatusIds::UNAUTHORIZED;
    Service<TSchemeImpl>().ExpectListDirectory("/");
    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetAddress()).SetDatabase("tenant"));
    NYdb::NDump::TClient client(driver);
    UNIT_ASSERT_VALUES_EQUAL(client.Restore("unused", "relative").GetStatus(), NYdb::EStatus::UNAUTHORIZED);
    CheckExpectations();
}

Y_UNIT_TEST(NotNullTypeDump) {
    const char* tableName = "TableWithNotNullTypeForDump";
    RunYdb({"-v", "yql", "-s",
        Sprintf(R"(CREATE TABLE %s (
            k Uint32 NOT NULL,
            v String NOT NULL,
            ov String,
            PRIMARY KEY(k));
        )", tableName)},
        TList<TString>());

    const auto dumpPath = GetOutputPath() / "dump";
    RunYdb({"-v", "tools", "dump", "--scheme-only", "--output", dumpPath.GetPath()}, TList<TString>());

    struct TFlags {
        const bool HasNullFlag;
        const bool IsOptionalType;
    };

    TVector<TFlags> column_flags;
    auto fillFlag = [&column_flags](const TString& str) {
        Ydb::Table::ColumnMeta meta;
        google::protobuf::TextFormat::ParseFromString(str, &meta);
        column_flags.emplace_back(TFlags{meta.has_not_null(), meta.type().has_optional_type()});
    };

    const auto output = TFileInput(dumpPath / tableName / "scheme.pb").ReadAll();
    const TString token = "columns {";
    size_t start = 0;
    while (true) {
        start = output.find(token, start);
        if (start != TString::npos) {
            int scope = 1;
            start += token.size();
            size_t pos = start;
            while (pos < output.size() && scope != 0) {
                if (output[pos] == '{') {
                    scope++;
                } else if (output[pos] == '}') {
                    scope--;
                }
                pos++;
            }
            Y_ABORT_UNLESS(pos > start);
            fillFlag(output.substr(start, pos - start - 1));
            start = pos;
        } else {
            break;
        }
    }

    // For compatibility reason we do not show not null flag
    UNIT_ASSERT_VALUES_EQUAL(column_flags.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[0].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[0].IsOptionalType, false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[1].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[1].IsOptionalType, false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[2].HasNullFlag,    false);
    UNIT_ASSERT_VALUES_EQUAL(column_flags[2].IsOptionalType, true);

    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetYdbEndpoint()).SetDatabase(GetYdbDatabase()));
    NYdb::NDump::TClient client(driver);
    UNIT_ASSERT_VALUES_EQUAL(client.Dump("/", (GetOutputPath() / "outside").GetPath(),
        NYdb::NDump::TDumpSettings().Database(GetYdbDatabase())).GetStatus(), NYdb::EStatus::INTERNAL_ERROR);
    RunYdb({"yql", "-s", Sprintf("UPSERT INTO %s (k, v, ov) VALUES (1u, 'value', NULL);", tableName)}, {});
    const auto expected = RunYdb({"yql", "-s", Sprintf("SELECT k, v, ov FROM %s;", tableName)}, {});
    for (const bool absolute : {false, true}) {
        const TString name = absolute ? "DumpAbsolute" : "DumpRelative";
        const auto tableDumpPath = GetOutputPath() / name;
        const TString source = absolute ? GetYdbDatabase() + "/" + tableName : TString(tableName);
        auto dumpResult = client.Dump(source, tableDumpPath.GetPath(),
            NYdb::NDump::TDumpSettings().Database(GetYdbDatabase()));
        UNIT_ASSERT_C(dumpResult.IsSuccess(), dumpResult.GetIssues().ToString());
        const TString destination = name + "Restored" + (absolute ? "/." : "/");
        auto restoreResult = client.Restore(tableDumpPath.GetPath(), destination);
        UNIT_ASSERT_C(restoreResult.IsSuccess(), restoreResult.GetIssues().ToString());
        auto schemaResult = client.Restore(tableDumpPath.GetPath(), destination, NYdb::NDump::TRestoreSettings().DryRun(true));
        UNIT_ASSERT_C(schemaResult.IsSuccess(), schemaResult.GetIssues().ToString());
        auto replaceResult = client.Restore(tableDumpPath.GetPath(), destination,
            NYdb::NDump::TRestoreSettings().Replace(true).VerifyExistence(true));
        UNIT_ASSERT_C(replaceResult.IsSuccess(), replaceResult.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(client.Restore(tableDumpPath.GetPath(), name + "Restored/../" + name + "Restored",
            NYdb::NDump::TRestoreSettings().Replace(true).VerifyExistence(true)).GetStatus(), NYdb::EStatus::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(
            RunYdb({"yql", "-s", Sprintf("SELECT k, v, ov FROM \\`%s/%sRestored/%s\\`;",
                GetYdbDatabase().c_str(), name.c_str(), tableName)}, {}),
            expected);
    }
    for (const TString destination : {"", "/"}) {
        UNIT_ASSERT_VALUES_EQUAL(client.Restore(dumpPath.GetPath(), destination).GetStatus(), NYdb::EStatus::BAD_REQUEST);
    }
}

}
