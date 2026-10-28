#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/event.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include <utility>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

    class TMockQueryService final: public Ydb::Query::V1::QueryService::Service {
    public:
        TManualEvent Release;

        grpc::Status ExecuteQuery(
            grpc::ServerContext*,
            const Ydb::Query::ExecuteQueryRequest*,
            grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart>* writer) override {
            Ydb::Query::ExecuteQueryResponsePart part;
            part.set_status(Ydb::StatusIds::SUCCESS);
            if (!writer->Write(part)) {
                return grpc::Status::CANCELLED;
            }
            if (!Release.WaitT(TDuration::Seconds(30))) {
                return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Test release timed out");
            }
            return grpc::Status::OK;
        }
    };

    template <typename TCallback>
    void WithQueryIterator(TCallback callback) {
        NTesting::InitPortManagerFromEnv();
        const auto port = NTesting::GetFreePort();
        const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

        TMockQueryService service;
        auto server = grpc::ServerBuilder()
                          .AddListeningPort(endpoint, grpc::InsecureServerCredentials())
                          .RegisterService(&service)
                          .BuildAndStart();
        UNIT_ASSERT(server);

        TDriver driver(TDriverConfig()
                           .SetEndpoint(endpoint)
                           .SetDiscoveryMode(EDiscoveryMode::Off)
                           .SetDatabase("/Root/My/DB"));
        TQueryClient client(driver);
        Y_DEFER {
            service.Release.Signal();
        };

        auto iteratorFuture = client.StreamExecuteQuery("SELECT 1", TTxControl::NoTx());
        UNIT_ASSERT(iteratorFuture.Wait(TDuration::Seconds(10)));
        auto iterator = iteratorFuture.ExtractValueSync();
        UNIT_ASSERT(iterator.IsSuccess());

        auto firstPart = iterator.ReadNext();
        UNIT_ASSERT(firstPart.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(firstPart.ExtractValueSync().IsSuccess());

        callback(iterator);
    }

    void AssertCancelled(TAsyncExecuteQueryPart part) {
        UNIT_ASSERT(part.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(part.ExtractValueSync().GetStatus(), EStatus::CLIENT_CANCELLED);
    }

} // namespace

Y_UNIT_TEST_SUITE(ExecuteQueryCancellation) {
    Y_UNIT_TEST(CancelCompletesPendingRead) {
        WithQueryIterator([](TExecuteQueryIterator& iterator) {
            auto part = iterator.ReadNext();
            UNIT_ASSERT(!part.HasValue());

            auto copy = iterator;
            copy.Cancel();
            iterator.Cancel();

            AssertCancelled(part);
            iterator.Cancel();
        });
    }

    Y_UNIT_TEST(CancelBetweenReads) {
        WithQueryIterator([](TExecuteQueryIterator& iterator) {
            iterator.Cancel();
            AssertCancelled(iterator.ReadNext());
        });
    }

    Y_UNIT_TEST(CancelMovedFromIterator) {
        WithQueryIterator([](TExecuteQueryIterator& iterator) {
            auto moved = std::move(iterator);
            auto part = moved.ReadNext();

            iterator.Cancel();
            UNIT_ASSERT(!part.HasValue());

            moved.Cancel();
            AssertCancelled(part);
        });
    }
} // Y_UNIT_TEST_SUITE(ExecuteQueryCancellation)
