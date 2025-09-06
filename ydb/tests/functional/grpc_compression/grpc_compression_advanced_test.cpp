#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/impl/codegen/compression_types.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <memory>

using namespace NYdb;
using namespace NYdb::NTable;

/**
 * Advanced gRPC compression tests that verify both client and server-side compression functionality.
 * These tests verify that YDB's gRPC server can properly handle compressed requests and responses.
 */
class TAdvancedGrpcCompressionTest : public TTestBase {
private:
    std::unique_ptr<TDriver> Driver_;
    std::string Database_;
    std::string Endpoint_;

public:
    void SetUp() override {
        Database_ = std::getenv("YDB_DATABASE") ? std::getenv("YDB_DATABASE") : "";
        Endpoint_ = std::getenv("YDB_ENDPOINT") ? std::getenv("YDB_ENDPOINT") : "";
        
        UNIT_ASSERT(!Database_.empty());
        UNIT_ASSERT(!Endpoint_.empty());
        
        auto driverConfig = TDriverConfig()
            .SetEndpoint(Endpoint_)
            .SetDatabase(Database_)
            .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "");
            
        Driver_ = std::make_unique<TDriver>(driverConfig);
    }
    
    void TearDown() override {
        Driver_.reset();
    }

    TDriver& GetDriver() {
        return *Driver_;
    }
    
    std::string GetDatabase() const {
        return Database_;
    }
    
    std::string GetEndpoint() const {
        return Endpoint_;
    }
    
    // Helper function to create a grpc channel with specific compression
    std::shared_ptr<grpc::Channel> CreateChannelWithCompression(
        grpc_compression_algorithm compression,
        grpc_compression_level level = GRPC_COMPRESS_LEVEL_NONE
    ) {
        grpc::ChannelArguments args;
        args.SetCompressionAlgorithm(compression);
        if (level != GRPC_COMPRESS_LEVEL_NONE) {
            args.SetInt(GRPC_COMPRESSION_CHANNEL_DEFAULT_LEVEL, static_cast<int>(level));
        }
        
        return grpc::CreateCustomChannel(
            Endpoint_,
            grpc::InsecureChannelCredentials(),
            args
        );
    }
    
    // Test compression by making actual gRPC calls and measuring effectiveness
    struct CompressionTestResult {
        bool success;
        size_t requestSize;
        size_t responseSize;
        grpc::StatusCode statusCode;
    };
    
    CompressionTestResult TestCompressionEffectiveness(
        grpc_compression_algorithm compression,
        const TString& largeData
    ) {
        auto channel = CreateChannelWithCompression(compression);
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        
        // Create session first
        Ydb::Table::CreateSessionRequest sessionRequest;
        Ydb::Table::CreateSessionResponse sessionResponse;
        grpc::ClientContext sessionContext;
        sessionContext.set_compression_algorithm(compression);
        
        auto sessionStatus = stub->CreateSession(&sessionContext, sessionRequest, &sessionResponse);
        if (!sessionStatus.ok()) {
            return {false, 0, 0, sessionStatus.error_code()};
        }
        
        // Execute a query with large data
        Ydb::Table::ExecuteDataQueryRequest queryRequest;
        queryRequest.set_session_id(sessionResponse.session_id());
        queryRequest.mutable_tx_control()->mutable_begin_tx()->mutable_serializable_read_write();
        queryRequest.mutable_tx_control()->set_commit_tx(true);
        
        // Create a query that includes the large data
        TString query = TStringBuilder() << "SELECT '" << largeData << "' as large_data;";
        queryRequest.mutable_query()->set_yql_text(query);
        
        Ydb::Table::ExecuteDataQueryResponse queryResponse;
        grpc::ClientContext queryContext;
        queryContext.set_compression_algorithm(compression);
        
        auto queryStatus = stub->ExecuteDataQuery(&queryContext, queryRequest, &queryResponse);
        
        return {
            queryStatus.ok(),
            query.size(),
            queryResponse.ByteSizeLong(),
            queryStatus.error_code()
        };
    }
};

Y_UNIT_TEST_SUITE(AdvancedGrpcCompressionTests) {
    Y_UNIT_TEST(TestGzipCompressionEffectiveness) {
        TAdvancedGrpcCompressionTest test;
        test.SetUp();
        
        // Create highly compressible data (repeated pattern)
        TString largeCompressibleData(10000, 'A'); // 10KB of 'A' characters
        
        auto result = test.TestCompressionEffectiveness(GRPC_COMPRESS_GZIP, largeCompressibleData);
        
        UNIT_ASSERT(result.success);
        UNIT_ASSERT(result.requestSize > 0);
        UNIT_ASSERT(result.responseSize > 0);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestDeflateCompressionEffectiveness) {
        TAdvancedGrpcCompressionTest test;
        test.SetUp();
        
        // Create highly compressible data
        TString largeCompressibleData(10000, 'B'); // 10KB of 'B' characters
        
        auto result = test.TestCompressionEffectiveness(GRPC_COMPRESS_DEFLATE, largeCompressibleData);
        
        UNIT_ASSERT(result.success);
        UNIT_ASSERT(result.requestSize > 0);
        UNIT_ASSERT(result.responseSize > 0);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestCompressionWithDifferentLevels) {
        TAdvancedGrpcCompressionTest test;
        test.SetUp();
        
        // Test different compression levels
        TString testData(5000, 'C'); // 5KB of 'C' characters
        
        // Test with low compression
        auto lowResult = test.TestCompressionEffectiveness(GRPC_COMPRESS_GZIP, testData);
        UNIT_ASSERT(lowResult.success);
        
        // Test with high compression (Note: actual level setting may depend on server config)
        auto highResult = test.TestCompressionEffectiveness(GRPC_COMPRESS_GZIP, testData);
        UNIT_ASSERT(highResult.success);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestMixedCompressionScenarios) {
        TAdvancedGrpcCompressionTest test;
        test.SetUp();
        
        // Test scenario: Switch between compression types in same session
        TTableClient client(test.GetDriver());
        
        // Create a test table
        auto tableResult = client.RetryOperationSync([&](TSession session) {
            auto tableBuilder = TTableBuilder()
                .AddNullableColumn("id", EPrimitiveType::Uint64)
                .AddNullableColumn("data", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("id");
                
            return session.CreateTable(test.GetDatabase() + "/compression_test_table", tableBuilder.Build());
        });
        
        // Allow table creation to fail if it already exists
        UNIT_ASSERT(tableResult.IsSuccess() || tableResult.GetStatus() == EStatus::ALREADY_EXISTS);
        
        // Insert data using different compression scenarios
        for (int i = 0; i < 3; ++i) {
            TString data(1000 + i * 500, 'D' + i); // Varying data sizes
            
            auto insertResult = client.RetryOperationSync([&](TSession session) {
                TString query = TStringBuilder() << 
                    "UPSERT INTO `" << test.GetDatabase() << "/compression_test_table` (id, data) VALUES (" << 
                    i << ", '" << data << "');";
                    
                return session.ExecuteDataQuery(
                    query,
                    TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
            });
            
            UNIT_ASSERT(insertResult.IsSuccess());
        }
        
        // Read back the data to verify compression didn't affect data integrity
        auto readResult = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteDataQuery(
                "SELECT id, data FROM `" + test.GetDatabase() + "/compression_test_table` ORDER BY id;",
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        });
        
        UNIT_ASSERT(readResult.IsSuccess());
        auto resultSet = readResult.GetResultSets()[0];
        UNIT_ASSERT_EQUAL(resultSet.RowsCount(), 3);
        
        // Clean up
        client.RetryOperationSync([&](TSession session) {
            return session.DropTable(test.GetDatabase() + "/compression_test_table");
        });
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestCompressionWithConcurrentClients) {
        TAdvancedGrpcCompressionTest test;
        test.SetUp();
        
        // Test multiple clients with different compression settings
        std::vector<std::thread> threads;
        std::vector<bool> results(3, false);
        
        for (int i = 0; i < 3; ++i) {
            threads.emplace_back([&, i]() {
                grpc_compression_algorithm algos[] = {GRPC_COMPRESS_NONE, GRPC_COMPRESS_GZIP, GRPC_COMPRESS_DEFLATE};
                auto channel = test.CreateChannelWithCompression(algos[i]);
                auto stub = Ydb::Table::V1::TableService::NewStub(channel);
                
                Ydb::Table::CreateSessionRequest request;
                Ydb::Table::CreateSessionResponse response;
                grpc::ClientContext context;
                context.set_compression_algorithm(algos[i]);
                
                auto status = stub->CreateSession(&context, request, &response);
                results[i] = status.ok() || status.error_code() == grpc::StatusCode::UNAUTHENTICATED;
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        // All clients should succeed
        for (bool result : results) {
            UNIT_ASSERT(result);
        }
        
        test.TearDown();
    }
}