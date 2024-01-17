#include "ydb_common_ut.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_long_tx.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/long_tx_service/public/types.h>
#include <ydb/core/tx/sharding/sharding.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/formats/arrow/serializer/full.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>

using namespace NYdb;

namespace
{

static const constexpr char* TestTablePath = TTestOlap::TablePath;

TString TestBlob() {
    auto batch = TTestOlap::SampleBatch();
    return NArrow::NSerialization::TFullDataSerializer(arrow::ipc::IpcWriteOptions::Defaults()).Serialize(batch);
}

}


Y_UNIT_TEST_SUITE(YdbLongTx) {

    Y_UNIT_TEST(BeginWriteCommit) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings);

        NYdb::NLongTx::TClient client(connection);

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

        auto txId = resBeginTx.GetResult().tx_id();
        TString data = TestBlob();

        NLongTx::TLongTxWriteResult resWrite =
            client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::SUCCESS);

        NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resCommitTx.Status().GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(BeginWriteRollback) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings);

        NYdb::NLongTx::TClient client(connection);

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

        auto txId = resBeginTx.GetResult().tx_id();
        TString data = TestBlob();

        NLongTx::TLongTxWriteResult resWrite =
            client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::SUCCESS);

        NLongTx::TLongTxRollbackResult resRollbackTx = client.RollbackTx(txId).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resRollbackTx.Status().GetStatus(), EStatus::SUCCESS);
    }


    Y_UNIT_TEST(WriteAclChecks) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection1 = NYdb::TDriver(TDriverConfig()
            .SetEndpoint(location)
            .SetDatabase("/Root")
            .SetAuthToken("user1@builtin"));
        auto connection2 = NYdb::TDriver(TDriverConfig()
            .SetEndpoint(location)
            .SetDatabase("/Root")
            .SetAuthToken("user2@builtin"));

        TTestOlap::CreateTable(*server.ServerSettings);
        {
            TClient annoyingClient(*server.ServerSettings);
            annoyingClient.SetSecurityToken("root@builtin");
            NACLib::TDiffACL diff;
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "user1@builtin");
            annoyingClient.ModifyACL("/Root/OlapStore", "OlapTable", diff.SerializeAsString());
        }

        // try user1 first
        {
            NYdb::NLongTx::TClient client(connection1);

            NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();
            TString data = TestBlob();

            NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::SUCCESS);

            NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resCommitTx.Status().GetStatus(), EStatus::SUCCESS);
        }

        // try user2 next
        {
            NYdb::NLongTx::TClient client(connection2);

            NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();
            TString data = TestBlob();

            NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::UNAUTHORIZED);

            NLongTx::TLongTxRollbackResult resRollbackTx = client.RollbackTx(txId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRollbackTx.Status().GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(CreateOlapWithDirs) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings, 1, "DirA/OlapStore", "DirB/OlapTable");
    }

}
