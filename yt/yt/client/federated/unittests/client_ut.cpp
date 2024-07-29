#include <yt/yt/client/federated/client.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/connection.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/net/local_address.h>

#include <util/datetime/base.h>


namespace NYT::NClient::NFederated {
namespace {

using namespace NYT::NApi;

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<NApi::TMockClient>;
using TStrictMockConnection = StrictMock<NApi::TMockConnection>;
using TStrictMockTransaction = StrictMock<NApi::TMockTransaction>;

////////////////////////////////////////////////////////////////////////////////

struct TTestDataStorage
{
    TTestDataStorage()
    {
        LookupResult1 = [&] {
            auto rowBuffer = NYT::New<NTableClient::TRowBuffer>();
            std::vector<NTableClient::TUnversionedRow> rows;
            for (ui64 key : {10, 11}) {
                auto row = rowBuffer->AllocateUnversioned(2);
                row[0] = rowBuffer->CaptureValue(NTableClient::MakeUnversionedUint64Value(key, TableSchema->GetColumnIndex(KeyColumn)));
                row[1] = rowBuffer->CaptureValue(NTableClient::MakeUnversionedUint64Value(key + 10, TableSchema->GetColumnIndex(ValueColumn)));
                rows.push_back(NTableClient::TUnversionedRow{row});
            }
            return TUnversionedLookupRowsResult{
                .Rowset = NApi::CreateRowset(TableSchema, MakeSharedRange(rows, std::move(rowBuffer))),
            };
        } ();

        LookupResult2 = [&] {
            auto rowBuffer = NYT::New<NTableClient::TRowBuffer>();
            std::vector<NTableClient::TUnversionedRow> rows;
            for (ui64 key : {12, 13}) {
                auto row = rowBuffer->AllocateUnversioned(2);
                row[0] = rowBuffer->CaptureValue(NTableClient::MakeUnversionedUint64Value(key, TableSchema->GetColumnIndex(KeyColumn)));
                row[1] = rowBuffer->CaptureValue(NTableClient::MakeUnversionedUint64Value(key + 10, TableSchema->GetColumnIndex(ValueColumn)));
                rows.push_back(NTableClient::TUnversionedRow{row});
            }
            return TUnversionedLookupRowsResult{
                .Rowset = NApi::CreateRowset(TableSchema, MakeSharedRange(rows, std::move(rowBuffer))),
            };
        } ();

        NameTable = NYT::New<NTableClient::TNameTable>();

        i32 keyField = NameTable->GetIdOrRegisterName(KeyColumn);
        auto keys = [&] {
            auto rowBuffer = NYT::New<NTableClient::TRowBuffer>();
            std::vector<NTableClient::TUnversionedRow> keysVector;
            for (ui64 key : {10, 11}) {
                NTableClient::TUnversionedRowBuilder builder;
                builder.AddValue(NTableClient::MakeUnversionedUint64Value(key, keyField));
                keysVector.push_back(rowBuffer->CaptureRow(builder.GetRow()));
            }
            return NYT::MakeSharedRange(std::move(keysVector), std::move(rowBuffer));
        } ();
    }

    const NYPath::TYPath Path = "/test/table";
    const TString KeyColumn = "key";
    const TString ValueColumn = "value";

    const NTableClient::TColumnSchema KeyColumnSchema = NTableClient::TColumnSchema(KeyColumn, NTableClient::EValueType::Uint64);
    const NTableClient::TColumnSchema ValueColumnSchema = NTableClient::TColumnSchema(ValueColumn, NTableClient::EValueType::Uint64);
    NTableClient::TTableSchemaPtr TableSchema = New<NTableClient::TTableSchema>(std::vector{KeyColumnSchema, ValueColumnSchema});

    NApi::TUnversionedLookupRowsResult LookupResult1;
    NApi::TUnversionedLookupRowsResult LookupResult2;
    NTableClient::TNameTablePtr NameTable;
    TSharedRange<NTableClient::TUnversionedRow> Keys;
};

TEST(TFederatedClientTest, Basic)
{
    TTestDataStorage data;

    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    // To identify best (closest) cluster.
    NYson::TYsonString listResult1(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult1)));

    NYson::TYsonString listResult2(TStringBuf(R"(["b-rpc-proxy-b.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult2)));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("a-rpc-proxy.vla.yp-c.yandex.net");

    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillOnce(Return(VoidFuture))
        .WillRepeatedly(Return(MakeFuture(TError("Failure"))));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    // Creation of federated client.
    std::vector<IClientPtr> clients{mockClientSas, mockClientVla};
    auto config = New<TFederationConfig>();
    config->ClusterHealthCheckPeriod = TDuration::Seconds(5);
    config->ClusterRetryAttempts = 1;
    auto federatedClient = CreateClient(clients, config);

    // 1. `vla` client should be used as closest cluster.
    // 2. error from `vla` cluster should be received.
    // 3. `sas` client should be used as other cluster.

    EXPECT_CALL(*mockClientVla, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult1)))
        .WillOnce(Return(MakeFuture<TUnversionedLookupRowsResult>(TError(NRpc::EErrorCode::Unavailable, "Failure"))));

    EXPECT_CALL(*mockClientSas, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult2)));

    // Wait for the first execution of CheckClustersHealth.
    Sleep(TDuration::Seconds(2));

    // From `vla`.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();
        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);
        ASSERT_EQ("[0#10u, 1#20u]", actualFirstRow);
    }

    // Error from `vla`.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        ASSERT_ANY_THROW(result.Get().ValueOrThrow());
    }

    // From `sas`.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();

        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);

        ASSERT_EQ("[0#12u, 1#22u]", actualFirstRow);
    }
}

TEST(TFederatedClientTest, CheckHealth)
{
    TTestDataStorage data;

    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    // To identify best (closest) cluster.
    NYson::TYsonString listResult1(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult1)));

    NYson::TYsonString listResult2(TStringBuf(R"(["b-rpc-proxy-b.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult2)));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("a-rpc-proxy.vla.yp-c.yandex.net");

    std::vector<IClientPtr> clients{mockClientSas, mockClientVla};
    auto config = New<TFederationConfig>();
    config->ClusterHealthCheckPeriod = TDuration::Seconds(5);
    config->ClusterRetryAttempts = 1;
    config->BundleName = "my_bundle";

    TCheckClusterLivenessOptions checkLivenessOptions;
    checkLivenessOptions.CheckCypressRoot = true;
    checkLivenessOptions.CheckTabletCellBundle = config->BundleName;
    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(checkLivenessOptions))
        .WillOnce(Return(VoidFuture))
        .WillOnce(Return(MakeFuture(TError("Failure"))))
        .WillOnce(Return(VoidFuture));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(checkLivenessOptions))
        .WillRepeatedly(Return(VoidFuture));

    auto federatedClient = CreateClient(clients, config);

    EXPECT_CALL(*mockClientVla, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult1)))
        .WillOnce(Return(MakeFuture(data.LookupResult1)));

    EXPECT_CALL(*mockClientSas, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult2)));

    // From `vla`.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();
        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);
        ASSERT_EQ("[0#10u, 1#20u]", actualFirstRow);
    }

    // Wait for the check of cluster liveness.
    Sleep(TDuration::Seconds(6));

    // From `sas` because `vla` was marked as unhealthy after CheckClustersHealth.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();

        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);

        ASSERT_EQ("[0#12u, 1#22u]", actualFirstRow);
    }

    // Wait for the next check of cluster liveness, `vla` cluster will become current again.
    Sleep(TDuration::Seconds(5));

    // From `vla` because it became ok again.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();
        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);
        ASSERT_EQ("[0#10u, 1#20u]", actualFirstRow);
    }
}

TEST(TFederatedClientTest, Transactions)
{
    TTestDataStorage data;

    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    // To identify best (closest) cluster.
    NYson::TYsonString listResult1(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult1)));

    NYson::TYsonString listResult2(TStringBuf(R"(["b-rpc-proxy-b.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult2)));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("a-rpc-proxy.vla.yp-c.yandex.net");

    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillOnce(Return(VoidFuture))
        .WillRepeatedly(Return(MakeFuture(TError("Failure"))));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    // Creation of federated client.
    std::vector<IClientPtr> clients{mockClientSas, mockClientVla};
    auto config = New<TFederationConfig>();
    config->ClusterHealthCheckPeriod = TDuration::Seconds(5);
    config->ClusterRetryAttempts = 1;
    auto federatedClient = CreateClient(clients, config);

    auto mockTransactionVla = New<TStrictMockTransaction>();

    EXPECT_CALL(*mockClientVla, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture(NApi::ITransactionPtr{mockTransactionVla})));

    EXPECT_CALL(*mockTransactionVla, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult1)))
        .WillOnce(Return(MakeFuture<TUnversionedLookupRowsResult>(TError(NRpc::EErrorCode::Unavailable, "Failure"))));

    // Wait for the first check of clusters healths.
    Sleep(TDuration::Seconds(2));

    auto transaction = federatedClient->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get().Value();

    // Check mock transaction doesn't work with sticky proxy address.
    {
        auto* derived = transaction->As<IFederatedClientTransactionMixin>();
        ASSERT_EQ(std::nullopt, derived->TryGetStickyProxyAddress());
    }

    // From `vla`.
    {
        auto result = transaction->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();
        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);
        ASSERT_EQ("[0#10u, 1#20u]", actualFirstRow);
    }

    // Error from `vla`.
    {
        auto result = transaction->LookupRows(data.Path, data.NameTable, data.Keys);
        ASSERT_ANY_THROW(result.Get().ValueOrThrow());
    }

    auto mockTransactionSas = New<TStrictMockTransaction>();
    EXPECT_CALL(*mockClientSas, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture(NApi::ITransactionPtr{mockTransactionSas})));

    EXPECT_CALL(*mockTransactionSas, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult2)));

    // Creating next transaction in `sas`.
    {
        transaction = federatedClient->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get().Value();

        auto result = transaction->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();

        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);

        ASSERT_EQ("[0#12u, 1#22u]", actualFirstRow);
    }
}

TEST(TFederatedClientTest, RetryWithoutTransaction)
{
    TTestDataStorage data;

    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    // To identify best (closest) cluster.
    NYson::TYsonString listResult1(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult1)));

    NYson::TYsonString listResult2(TStringBuf(R"(["b-rpc-proxy-b.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult2)));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("a-rpc-proxy.vla.yp-c.yandex.net");

    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillOnce(Return(VoidFuture))
        .WillOnce(Return(VoidFuture))
        .WillRepeatedly(Return(MakeFuture(TError("Failure"))));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    // Creation of federated client.
    std::vector<IClientPtr> clients{mockClientSas, mockClientVla};
    auto config = New<TFederationConfig>();
    config->ClusterHealthCheckPeriod = TDuration::Seconds(5);
    auto federatedClient = CreateClient(clients, config);

    // 1. `vla` client should be used as closest cluster.
    // 2. error from `vla` cluster should be received.
    // 3. `sas` client should be used as other cluster.

    EXPECT_CALL(*mockClientVla, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture<TUnversionedLookupRowsResult>(TError(NRpc::EErrorCode::Unavailable, "Failure"))));

    EXPECT_CALL(*mockClientSas, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult2)));

    // Wait for the first execution of CheckClustersHealth.
    Sleep(TDuration::Seconds(2));

    // Go to `vla`, getting error, retry via `sas` and getting response from `sas`.
    {
        auto result = federatedClient->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();

        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);

        ASSERT_EQ("[0#12u, 1#22u]", actualFirstRow);
    }

    // Wait for the next check, `vla` is current cluster again.
    Sleep(TDuration::Seconds(5));

    auto mockTransactionVla = New<TStrictMockTransaction>();
    EXPECT_CALL(*mockClientVla, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture<NApi::ITransactionPtr>(TError(NRpc::EErrorCode::Unavailable, "Failure"))));

    auto mockTransactionSas = New<TStrictMockTransaction>();
    EXPECT_CALL(*mockClientSas, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture(NApi::ITransactionPtr(mockTransactionSas))));
    EXPECT_CALL(*mockTransactionSas, LookupRows(data.Path, _, _, _))
        .WillOnce(Return(MakeFuture(data.LookupResult2)));

    // Try to start transaction in `vla`, getting error, retry via `sas`, creating transaction and getting response from `sas`.
    {
        auto transaction = federatedClient->StartTransaction(NTransactionClient::ETransactionType::Tablet).Get().Value();

        auto result = transaction->LookupRows(data.Path, data.NameTable, data.Keys);
        auto rows = result.Get().Value().Rowset->GetRows();

        ASSERT_EQ(2u, rows.Size());
        auto actualFirstRow = ToString(rows[0]);

        ASSERT_EQ("[0#12u, 1#22u]", actualFirstRow);
    }
}

TEST(TFederatedClientTest, AttachTransaction)
{
    TTestDataStorage data;

    auto mockClientSas = New<TStrictMockClient>();
    auto mockClientVla = New<TStrictMockClient>();

    // To identify best (closest) cluster.
    NYson::TYsonString listResult1(TStringBuf(R"(["a-rpc-proxy-a.sas.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientSas, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult1)));

    NYson::TYsonString listResult2(TStringBuf(R"(["b-rpc-proxy-b.vla.yp-c.yandex.net:9013"])"));
    EXPECT_CALL(*mockClientVla, ListNode("//sys/rpc_proxies", _))
        .WillOnce(Return(MakeFuture(listResult2)));

    auto finally = Finally([oldLocalHostName = NNet::GetLocalHostName()] {
        NNet::WriteLocalHostName(oldLocalHostName);
    });
    NNet::WriteLocalHostName("b-rpc-proxy.vla.yp-c.yandex.net");

    EXPECT_CALL(*mockClientVla, CheckClusterLiveness(_))
        .WillRepeatedly(Return(MakeFuture(TError("Failure"))));

    EXPECT_CALL(*mockClientSas, CheckClusterLiveness(_))
        .WillRepeatedly(Return(VoidFuture));

    auto mockConnectionSas = New<TStrictMockConnection>();
    EXPECT_CALL(*mockConnectionSas, GetClusterTag())
        .WillRepeatedly(Return(NObjectClient::TCellTag(123)));
    EXPECT_CALL(*mockClientSas, GetConnection())
        .WillOnce(Return(mockConnectionSas));

    auto mockConnectionVla = New<TStrictMockConnection>();
    EXPECT_CALL(*mockConnectionVla, GetClusterTag())
        .WillRepeatedly(Return(NObjectClient::TCellTag(456)));
    EXPECT_CALL(*mockClientVla, GetConnection())
        .WillOnce(Return(mockConnectionVla));

    // Creation of federated client.
    std::vector<IClientPtr> clients{mockClientSas, mockClientVla};
    auto config = New<TFederationConfig>();
    config->ClusterHealthCheckPeriod = TDuration::Seconds(5);
    auto federatedClient = CreateClient(clients, config);

    auto mockTransactionSas = New<TStrictMockTransaction>();
    auto transactionId = TGuid(0, 123 << 16, 0, 0);
    EXPECT_CALL(*mockTransactionSas, GetId())
        .WillRepeatedly(Return(transactionId));

    EXPECT_CALL(*mockClientSas, AttachTransaction(transactionId, _))
        .WillOnce(Return(mockTransactionSas));

    auto transaction = federatedClient->AttachTransaction(transactionId);
    ASSERT_EQ(transaction->GetId(), transactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClient::NFederated
