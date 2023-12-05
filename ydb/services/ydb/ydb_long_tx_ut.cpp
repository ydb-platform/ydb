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

TVector<std::shared_ptr<arrow::RecordBatch>> SplitData(const TString& data, ui32 numBatches) {
    auto batch = NArrow::NSerialization::TFullDataDeserializer().Deserialize(data);
    UNIT_ASSERT(batch.ok());

    NSharding::TLogsSharding sharding(numBatches, { "timestamp", "uid" }, numBatches);
    std::vector<ui32> rowSharding = sharding.MakeSharding(*batch);
    Y_ABORT_UNLESS(rowSharding.size() == (size_t)batch->get()->num_rows());

    std::vector<std::shared_ptr<arrow::RecordBatch>> sharded = NArrow::ShardingSplit(*batch, rowSharding, numBatches);
    Y_ABORT_UNLESS(sharded.size() == numBatches);

    TVector<std::shared_ptr<arrow::RecordBatch>> out;
    for (size_t i = 0; i < numBatches; ++i) {
        if (sharded[i]) {
            Y_ABORT_UNLESS(sharded[i]->ValidateFull().ok());
            out.emplace_back(sharded[i]);
        }
    }

    return out;
}

bool EqualBatches(const TString& x, const TString& y) {
    auto schema = TTestOlap::ArrowSchema();
    std::shared_ptr<arrow::RecordBatch> batchX = NArrow::DeserializeBatch(x, schema);
    std::shared_ptr<arrow::RecordBatch> batchY = NArrow::DeserializeBatch(y, schema);
    Y_ABORT_UNLESS(batchX && batchY);
    if ((batchX->num_columns() != batchY->num_columns()) ||
        (batchX->num_rows() != batchY->num_rows())) {
        Cerr << __FILE__ << ':' << __LINE__ << " "
            << batchX->num_columns() << ':' << batchX->num_rows() << " vs "
            << batchY->num_columns() << ':' << batchY->num_rows() << "\n";
        return false;
    }

    for (auto& column : schema->field_names()) {
        auto filedX = batchX->schema()->GetFieldByName(column);
        auto filedY = batchY->schema()->GetFieldByName(column);
        Y_ABORT_UNLESS(filedX->type()->id() == filedY->type()->id());

        auto arrX = batchX->GetColumnByName(column);
        auto arrY = batchY->GetColumnByName(column);

        switch (filedX->type()->id()) {
            case arrow::Type::INT32:
                if (!NArrow::ArrayEqualValue<arrow::Int32Array>(arrX, arrY)) {
                    Cerr << __FILE__ << ':' << __LINE__ << " " << column << "\n";
                    return false;
                }
                break;
            case arrow::Type::UINT64:
                if (!NArrow::ArrayEqualValue<arrow::UInt64Array>(arrX, arrY)) {
                    Cerr << __FILE__ << ':' << __LINE__ << " " << column << "\n";
                    return false;
                }
                break;
            case arrow::Type::TIMESTAMP:
                if (!NArrow::ArrayEqualValue<arrow::TimestampArray>(arrX, arrY)) {
                    Cerr << __FILE__ << ':' << __LINE__ << " " << column << "\n";
                    return false;
                }
                break;
            case arrow::Type::BINARY:
                if (!NArrow::ArrayEqualView<arrow::BinaryArray>(arrX, arrY)) {
                    Cerr << __FILE__ << ':' << __LINE__ << " " << column << "\n";
                    return false;
                }
                break;
            case arrow::Type::STRING:
                if (!NArrow::ArrayEqualView<arrow::StringArray>(arrX, arrY)) {
                    Cerr << __FILE__ << ':' << __LINE__ << " " << column << "\n";
                    return false;
                }
                break;

            default:
                Cerr << __FILE__ << ':' << __LINE__ << "\n";
                return false;
        }
    }

    return true;
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

    Y_UNIT_TEST(BeginRead) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings);

        NYdb::NLongTx::TClient client(connection);

        NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

        auto txId = resBeginTx.GetResult().tx_id();

        NLongTx::TLongTxReadResult resRead = client.Read(txId, TestTablePath).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(resRead.GetResult().data().data(), "");
    }

    Y_UNIT_TEST(WriteThenRead) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings);

        NYdb::NLongTx::TClient client(connection);

        // Read before write
        TString beforeWriteTxId;
        {
            NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            beforeWriteTxId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxReadResult resRead = client.Read(beforeWriteTxId, TestTablePath).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(resRead.GetResult().data().data(), "");
        }

        // Write
        auto batch = TTestOlap::SampleBatch(false, 10000);
        const TString data = NArrow::NSerialization::TFullDataSerializer(arrow::ipc::IpcWriteOptions::Defaults()).Serialize(batch);

        {
            NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::SUCCESS);

            NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resCommitTx.Status().GetStatus(), EStatus::SUCCESS);
        }

        // Read after write
        auto sharded = SplitData(data, 2);
        UNIT_ASSERT_VALUES_EQUAL(sharded.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(sharded[0]->num_rows(), 4990);
        UNIT_ASSERT_VALUES_EQUAL(sharded[1]->num_rows(), 5010);

        TVector<TString> expected;
        for (auto batch : sharded) {
            expected.push_back(NArrow::SerializeBatchNoCompression(batch));
        }

        TVector<TString> returned;
        {
            NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxReadResult resRead = client.Read(txId, TestTablePath).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);
            returned.push_back(resRead.GetResult().data().data());
            // TODO: read both

            UNIT_ASSERT(EqualBatches(expected[0], returned[0]) ||
                        EqualBatches(expected[1], returned[0]));
        }

        // Read before write again
        {
            NLongTx::TLongTxReadResult resRead = client.Read(beforeWriteTxId, TestTablePath).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(resRead.GetResult().data().data(), "");
        }
    }

    Y_UNIT_TEST(ReadFutureSnapshot) {
        NKikimrConfig::TAppConfig appConfig;
        TKikimrWithGrpcAndRootSchema server(appConfig);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(TDriverConfig().SetEndpoint(location));

        TTestOlap::CreateTable(*server.ServerSettings, 1);

        NYdb::NLongTx::TClient client(connection);

        TString futureTxId;
        NYdb::NLongTx::TClient::TAsyncReadResult futureRead;
        {
            NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            TString txId = resBeginTx.GetResult().tx_id();
            NLongTxService::TLongTxId parsed;
            Y_ABORT_UNLESS(parsed.ParseString(txId));
            Y_ABORT_UNLESS(parsed.Snapshot.Step > 0);
            parsed.Snapshot.Step += 2000; // 2 seconds in the future
            futureTxId = parsed.ToString();
            // Cerr << "Future txId " << futureTxId << Endl;

            futureRead = client.Read(futureTxId, TestTablePath);
        }

        // Write
        TString data = TestBlob();
        {
            NLongTx::TLongTxBeginResult resBeginTx = client.BeginWriteTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxWriteResult resWrite =
                client.Write(txId, TestTablePath, "0", data, Ydb::LongTx::Data::APACHE_ARROW).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resWrite.Status().GetStatus(), EStatus::SUCCESS);

            NLongTx::TLongTxCommitResult resCommitTx = client.CommitTx(txId).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resCommitTx.Status().GetStatus(), EStatus::SUCCESS);
        }

        // Await read
        {
            NLongTx::TLongTxReadResult resRead = futureRead.GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);

            auto inputBatch = NArrow::NSerialization::TFullDataDeserializer().Deserialize(data);
            UNIT_ASSERT(inputBatch.ok());
            auto readBatch = NArrow::NSerialization::TBatchPayloadDeserializer(inputBatch->get()->schema()).Deserialize(resRead.GetResult().data().data());
            UNIT_ASSERT(readBatch.ok());
            UNIT_ASSERT_VALUES_EQUAL(readBatch->get()->ToString(), inputBatch->get()->ToString());
        }
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

    Y_UNIT_TEST(ReadAclChecks) {
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
            diff.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@builtin");
            annoyingClient.ModifyACL("/Root/OlapStore", "OlapTable", diff.SerializeAsString());
        }

        // try user1 first
        {
            NYdb::NLongTx::TClient client(connection1);

            NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxReadResult resRead = client.Read(txId, TestTablePath).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::SUCCESS);
        }

        // try user2 next
        {
            NYdb::NLongTx::TClient client(connection2);

            NLongTx::TLongTxBeginResult resBeginTx = client.BeginReadTx().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resBeginTx.Status().GetStatus(), EStatus::SUCCESS);

            auto txId = resBeginTx.GetResult().tx_id();

            NLongTx::TLongTxReadResult resRead = client.Read(txId, TestTablePath).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(resRead.Status().GetStatus(), EStatus::UNAUTHORIZED);
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
