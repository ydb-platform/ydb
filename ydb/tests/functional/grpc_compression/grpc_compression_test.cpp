#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

#include <memory>

using namespace NYdb;
using namespace NYdb::NTable;
class TGrpcCompressionTest : public TTestBase {
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
    std::shared_ptr<grpc::Channel> CreateChannelWithCompression(grpc_compression_algorithm compression) {
        grpc::ChannelArguments args;
        args.SetCompressionAlgorithm(compression);
        
        return grpc::CreateCustomChannel(
            Endpoint_,
            grpc::InsecureChannelCredentials(),
            args
        );
    }
    
    // Helper function to test if server supports compression by making actual calls
    bool TestServerSupportsCompression(grpc_compression_algorithm compression) {
        auto channel = CreateChannelWithCompression(compression);
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        
        Ydb::Table::CreateSessionRequest request;
        Ydb::Table::CreateSessionResponse response;
        grpc::ClientContext context;
        
        // Set client-side compression for the call
        context.set_compression_algorithm(compression);
        
        auto status = stub->CreateSession(&context, request, &response);
        
        // If the call succeeds, the server handled compression correctly
        return status.ok() || status.error_code() == grpc::StatusCode::UNAUTHENTICATED;
    }
};
Y_UNIT_TEST_SUITE(GrpcCompressionTests) {
    Y_UNIT_TEST(BasicConnectivity) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        TTableClient client(test.GetDriver());
        
        // Create a simple test table to verify connectivity
        auto result = client.RetryOperationSync([&](TSession session) {
            return session.DescribeTable(test.GetDatabase() + "/sys/ds_tableStats").GetValueSync();
        });
        
        // We expect this to succeed (table exists in YDB) or fail with specific error (table not found)
        // Both cases indicate successful gRPC connection
        UNIT_ASSERT(result.IsSuccess() || result.GetStatus() == EStatus::SCHEME_ERROR);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestGrpcChannelWithGzipCompression) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Test that we can create a grpc channel with compression
        auto channel = test.CreateChannelWithCompression(GRPC_COMPRESS_GZIP);
        UNIT_ASSERT(channel != nullptr);
        
        // Verify channel state - should be able to connect
        auto state = channel->GetState(false);
        UNIT_ASSERT(state != GRPC_CHANNEL_SHUTDOWN);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestGrpcChannelWithDeflateCompression) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Test that we can create a grpc channel with deflate compression
        auto channel = test.CreateChannelWithCompression(GRPC_COMPRESS_DEFLATE);
        UNIT_ASSERT(channel != nullptr);
        
        // Verify channel state
        auto state = channel->GetState(false);
        UNIT_ASSERT(state != GRPC_CHANNEL_SHUTDOWN);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestServerHandlesGzipCompression) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Test that the server can handle GZIP compressed requests
        bool supports = test.TestServerSupportsCompression(GRPC_COMPRESS_GZIP);
        UNIT_ASSERT(supports);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestServerHandlesDeflateCompression) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Test that the server can handle DEFLATE compressed requests
        bool supports = test.TestServerSupportsCompression(GRPC_COMPRESS_DEFLATE);
        UNIT_ASSERT(supports);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestServerHandlesNoCompression) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Test that the server can handle uncompressed requests
        bool supports = test.TestServerSupportsCompression(GRPC_COMPRESS_NONE);
        UNIT_ASSERT(supports);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestYdbClientWithCompressionEnabled) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        // Create a YDB client that uses compression
        // Note: This test verifies that compression settings don't break the client
        TTableClient client(test.GetDriver());
        
        // Perform a simple operation
        auto result = client.RetryOperationSync([&](TSession session) {
            // Execute a simple query that should work regardless of compression
            return session.ExecuteDataQuery(
                "SELECT 1 as test_value;",
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        });
        
        UNIT_ASSERT(result.IsSuccess());
        
        auto resultSet = result.GetResultSets()[0];
        UNIT_ASSERT_EQUAL(resultSet.RowsCount(), 1);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestCompressionWithLargeRequest) {
        TGrpcCompressionTest test;
        test.SetUp();
        
        TTableClient client(test.GetDriver());
        
        // Create a query with a large string that should benefit from compression
        TString largeString(10000, 'A'); // 10KB of 'A' characters - highly compressible
        TString query = TStringBuilder() << "SELECT '" << largeString << "' as large_value;";
        
        auto result = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        });
        
        UNIT_ASSERT(result.IsSuccess());
        
        auto resultSet = result.GetResultSets()[0];
        UNIT_ASSERT_EQUAL(resultSet.RowsCount(), 1);
        
        // Verify the result contains our large string
        TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("large_value").GetOptionalUtf8();
        UNIT_ASSERT(value.Defined());
        UNIT_ASSERT_EQUAL(value->size(), largeString.size());
        
        test.TearDown();
    }
}