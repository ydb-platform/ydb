#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/grpcpp.h>

#include <util/string/cast.h>

#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;

using namespace std::chrono_literals;

class TDiscoveryProxy final : public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    TDiscoveryProxy(std::atomic_bool& paused)
        :  Paused_(paused)
    {
    }

    grpc::Status ListEndpoints([[maybe_unused]] grpc::ServerContext* context,
                               [[maybe_unused]] const Ydb::Discovery::ListEndpointsRequest* request,
                               Ydb::Discovery::ListEndpointsResponse* response) override {
        if (Paused_.load()) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
        }

        Ydb::Discovery::ListEndpointsResult result;
        auto info = result.add_endpoints();
        info->set_address("localhost");
        info->set_port(Port_);
        info->set_node_id(1);

        response->mutable_operation()->mutable_result()->PackFrom(result);
        response->mutable_operation()->set_id("ydb://operation/1");
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS);
        return grpc::Status::OK;
    }

    void SetPort(int port) {
        Port_ = port;
    }

private:
    int Port_;
    std::atomic_bool& Paused_;
};

class TQueryProxy final : public Ydb::Query::V1::QueryService::Service {
public:
    TQueryProxy(std::shared_ptr<grpc::Channel> channel, std::atomic_bool& paused)
        : Stub_(channel)
        , Paused_(paused)
    {
    }

    template <typename TRequest, typename TResponse>
    using TGrpcCall =
        grpc::Status(Ydb::Query::V1::QueryService::Stub::*)(grpc::ClientContext*, const TRequest& request, TResponse* response);

    template <typename TRequest, typename TResponse>
    using TGrpcStreamCall =
        std::unique_ptr<grpc::ClientReader<TResponse>>(Ydb::Query::V1::QueryService::Stub::*)(grpc::ClientContext*, const TRequest& request);

    template <typename TRequest, typename TResponse>
    grpc::Status Run(TGrpcCall<TRequest, TResponse> call, grpc::ServerContext *context,
                     const TRequest* request, TResponse* response) {
        if (Paused_.load()) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
        }

        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        return (Stub_.*call)(clientContext.get(), *request, response);
    }

    template <typename TRequest, typename TResponse>
    grpc::Status RunStream(TGrpcStreamCall<TRequest, TResponse> call, grpc::ServerContext *context,
                     const TRequest* request, grpc::ServerWriter<TResponse>* writer) {
        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        auto reader = (Stub_.*call)(clientContext.get(), *request);

        TResponse state;

        while (reader->Read(&state)) {
            if (Paused_.load()) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
            }
            writer->Write(state);
        }

        return reader->Finish();
    }

    grpc::Status CreateSession(grpc::ServerContext *context, const Ydb::Query::CreateSessionRequest* request,
                               Ydb::Query::CreateSessionResponse* response) override {
        return Run(&Ydb::Query::V1::QueryService::Stub::CreateSession, context, request, response);
    }

    grpc::Status DeleteSession(grpc::ServerContext *context, const Ydb::Query::DeleteSessionRequest *request,
                               Ydb::Query::DeleteSessionResponse *response) override {
        return Run(&Ydb::Query::V1::QueryService::Stub::DeleteSession, context, request, response);
    }

    grpc::Status AttachSession(grpc::ServerContext *context, const Ydb::Query::AttachSessionRequest *request,
                               grpc::ServerWriter<Ydb::Query::SessionState> *writer) override {
        return RunStream(&Ydb::Query::V1::QueryService::Stub::AttachSession, context, request, writer);
    }

    grpc::Status ExecuteQuery(grpc::ServerContext *context, const Ydb::Query::ExecuteQueryRequest *request,
                              grpc::ServerWriter<Ydb::Query::ExecuteQueryResponsePart> *writer) override {
        return RunStream(&Ydb::Query::V1::QueryService::Stub::ExecuteQuery, context, request, writer);
    }

private:
    Ydb::Query::V1::QueryService::Stub Stub_;
    std::atomic_bool& Paused_;
};

class ServerRestartTest : public testing::Test {
protected:
    ServerRestartTest() {
        std::string endpoint = std::getenv("YDB_ENDPOINT");
        std::string database = std::getenv("YDB_DATABASE");
        Channel_ = grpc::CreateChannel(grpc::string{endpoint}, grpc::InsecureChannelCredentials());

        DisoveryService_ = std::make_unique<TDiscoveryProxy>(Paused_);
        QueryService_ = std::make_unique<TQueryProxy>(Channel_, Paused_);

        int port = 0;

        Server_ = grpc::ServerBuilder()
            .AddListeningPort("0.0.0.0:0", grpc::InsecureServerCredentials(), &port)
            .RegisterService(DisoveryService_.get())
            .RegisterService(QueryService_.get())
            .BuildAndStart();
        
        DisoveryService_->SetPort(port);

        Driver_ = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint("localhost:" + std::to_string(port))
            .SetDatabase(database)
            .SetDiscoveryMode(EDiscoveryMode::Async)
        );
    }

    TDriver GetDriver() {
        return *Driver_;
    }

    void PauseServer() {
        Paused_.store(true);
    }

    void UnpauseServer() {
        Paused_.store(false);
    }

private:
    std::atomic_bool Paused_{false};

    std::shared_ptr<grpc::Channel> Channel_;

    std::unique_ptr<TDiscoveryProxy> DisoveryService_;
    std::unique_ptr<TQueryProxy> QueryService_;
    std::unique_ptr<grpc::Server> Server_;

    std::unique_ptr<TDriver> Driver_;
};


TEST_F(ServerRestartTest, RestartOnGetSession) {
    TQueryClient client(GetDriver());
    std::atomic_bool closed(false);

    auto thread = std::thread([&client, &closed]() {
        std::optional<TStatus> status;
        while (!closed.load()) {
            status = client.RetryQuerySync([](NYdb::NQuery::TSession session) {
                return session.ExecuteQuery("SELECT 1", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            });

            ASSERT_LE(client.GetActiveSessionCount(), 1);

            std::this_thread::sleep_for(100ms);
        }

        ASSERT_TRUE(status.has_value());
        ASSERT_TRUE(status->IsSuccess()) << ToString(*status);
    });

    std::this_thread::sleep_for(1s);
    PauseServer();
    std::this_thread::sleep_for(10s);
    UnpauseServer();
    std::this_thread::sleep_for(1s);

    closed.store(true);
    thread.join();

    ASSERT_EQ(client.GetActiveSessionCount(), 0);
}
