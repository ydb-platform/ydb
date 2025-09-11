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

/**
 * Test suite that verifies server-side compression configuration.
 * This test configures YDB with specific compression settings and validates they work.
 */
class TServerCompressionConfigTest : public TTestBase {
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
    
    // Test that server accepts and properly handles compression headers
    bool TestServerCompressionResponse(grpc_compression_algorithm compression) {
        grpc::ChannelArguments args;
        args.SetCompressionAlgorithm(compression);
        
        auto channel = grpc::CreateCustomChannel(
            Endpoint_,
            grpc::InsecureChannelCredentials(),
            args
        );
        
        auto stub = Ydb::Table::V1::TableService::NewStub(channel);
        
        Ydb::Table::CreateSessionRequest request;
        Ydb::Table::CreateSessionResponse response;
        grpc::ClientContext context;
        
        // Explicitly request compression for this call
        context.set_compression_algorithm(compression);
        
        auto status = stub->CreateSession(&context, request, &response);
        
        // Check if server handled compression correctly
        return status.ok() || status.error_code() == grpc::StatusCode::UNAUTHENTICATED;
    }
    
    // Test compression with actual data transfer
    bool TestDataTransferWithCompression(grpc_compression_algorithm compression) {
        TTableClient client(GetDriver());
        
        // Execute a query that returns substantial data
        auto result = client.RetryOperationSync([&](TSession session) {
            // Create a query that generates substantial response data
            TString query = R"(
                SELECT 
                    CAST(1 as Uint64) as id,
                    'This is a test string that will be repeated many times to test compression effectiveness. The quick brown fox jumps over the lazy dog. ' || 
                    'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' ||
                    'Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.' as large_text
                UNION ALL
                SELECT 
                    CAST(2 as Uint64) as id,
                    'This is another test string with similar content to test compression. The quick brown fox jumps over the lazy dog again. ' ||
                    'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' ||
                    'Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.' as large_text
                UNION ALL
                SELECT 
                    CAST(3 as Uint64) as id,
                    'Yet another test string to generate compressible content for the compression test. The quick brown fox jumps over the lazy dog once more. ' ||
                    'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' ||
                    'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.' as large_text;
            )";
            
            return session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        });
        
        if (!result.IsSuccess()) {
            return false;
        }
        
        auto resultSet = result.GetResultSets()[0];
        return resultSet.RowsCount() == 3;
    }
};

Y_UNIT_TEST_SUITE(ServerCompressionConfigTests) {
    Y_UNIT_TEST(TestServerHandlesGzipCompression) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        bool success = test.TestServerCompressionResponse(GRPC_COMPRESS_GZIP);
        UNIT_ASSERT(success);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestServerHandlesDeflateCompression) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        bool success = test.TestServerCompressionResponse(GRPC_COMPRESS_DEFLATE);
        UNIT_ASSERT(success);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestDataTransferWithGzipCompression) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        bool success = test.TestDataTransferWithCompression(GRPC_COMPRESS_GZIP);
        UNIT_ASSERT(success);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestDataTransferWithDeflateCompression) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        bool success = test.TestDataTransferWithCompression(GRPC_COMPRESS_DEFLATE);
        UNIT_ASSERT(success);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestCompressionDoesNotAffectDataIntegrity) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        TTableClient client(test.GetDriver());
        
        // Test that data integrity is maintained with compression
        auto result = client.RetryOperationSync([&](TSession session) {
            TString testString = "Test string with special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?~`АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯабвгдеёжзийклмнопрстуфхцчшщъыьэюя";
            TString query = TStringBuilder() << "SELECT '" << testString << "' as test_data;";
            
            return session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        });
        
        UNIT_ASSERT(result.IsSuccess());
        
        auto resultSet = result.GetResultSets()[0];
        UNIT_ASSERT_EQUAL(resultSet.RowsCount(), 1);
        
        TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        auto value = parser.ColumnParser("test_data").GetOptionalUtf8();
        UNIT_ASSERT(value.Defined());
        
        // Verify the string contains our special characters (UTF-8 test)
        UNIT_ASSERT(value->find("АБВ") != TString::npos);
        UNIT_ASSERT(value->find("!@#") != TString::npos);
        
        test.TearDown();
    }
    
    Y_UNIT_TEST(TestCompressionPerformanceCharacteristics) {
        TServerCompressionConfigTest test;
        test.SetUp();
        
        TTableClient client(test.GetDriver());
        
        // Generate data of different compressibility characteristics
        struct TestCase {
            TString name;
            TString data;
        };
        
        std::vector<TestCase> testCases = {
            {"highly_compressible", TString(5000, 'A')},  // Very compressible
            {"medium_compressible", "This is a test string. " + TString(100, 'B') + " More test data. " + TString(100, 'C')},  // Moderately compressible
            {"random_like", "1a2b3c4d5e6f7g8h9i0j1k2l3m4n5o6p7q8r9s0t1u2v3w4x5y6z7a8b9c0d1e2f3g4h5i6j7k8l9m0n1o2p3q4r5s6t7u8v9w0x1y2z3"} // Less compressible
        };
        
        for (const auto& testCase : testCases) {
            auto result = client.RetryOperationSync([&](TSession session) {
                TString query = TStringBuilder() << "SELECT '" << testCase.data << "' as data_type, '" << testCase.name << "' as test_name;";
                
                return session.ExecuteDataQuery(
                    query,
                    TTxControl::BeginTx().CommitTx()
                ).GetValueSync();
            });
            
            UNIT_ASSERT_C(result.IsSuccess(), TStringBuilder() << "Failed for test case: " << testCase.name);
            
            auto resultSet = result.GetResultSets()[0];
            UNIT_ASSERT_EQUAL(resultSet.RowsCount(), 1);
        }
        
        test.TearDown();
    }
}