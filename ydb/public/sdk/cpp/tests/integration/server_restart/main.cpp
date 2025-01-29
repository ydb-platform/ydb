#include <ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <grpcpp/grpcpp.h>

#include <thread>

using namespace NYdb;
using namespace NYdb::NQuery;

using namespace std::chrono_literals;

class TDiscoveryProxy final : public Ydb::Discovery::V1::DiscoveryService::Service {
public:
    TDiscoveryProxy(std::shared_ptr<grpc::Channel> channel, std::atomic_bool& paused)
        : Stub_(channel)
        , Paused_(paused)
    {}

    grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request,
                               Ydb::Discovery::ListEndpointsResponse* response) override {
        if (Paused_.load()) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
        }

        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        return Stub_.ListEndpoints(clientContext.get(), *request, response);
    }

private:
    Ydb::Discovery::V1::DiscoveryService::Stub Stub_;
    std::atomic_bool& Paused_;
};

class TQueryProxy final : public Ydb::Query::V1::QueryService::Service {
public:
    TQueryProxy(std::shared_ptr<grpc::Channel> channel, std::atomic_bool& paused)
        : Stub_(channel)
        , Paused_(paused)
    {}

    grpc::Status CreateSession(grpc::ServerContext *context, const Ydb::Query::CreateSessionRequest *request,
                               Ydb::Query::CreateSessionResponse *response) override {
        if (Paused_.load()) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
        }

        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        return Stub_.CreateSession(clientContext.get(), *request, response);
    }

    grpc::Status DeleteSession(grpc::ServerContext *context, const Ydb::Query::DeleteSessionRequest *request,
                               Ydb::Query::DeleteSessionResponse *response) override {
        if (Paused_.load()) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
        }

        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        return Stub_.DeleteSession(clientContext.get(), *request, response);
    }

    grpc::Status AttachSession(grpc::ServerContext *context, const Ydb::Query::AttachSessionRequest *request,
                               grpc::ServerWriter<Ydb::Query::SessionState> *writer) override {
        auto clientContext = grpc::ClientContext::FromServerContext(*context);
        auto reader = Stub_.AttachSession(clientContext.get(), *request);

        Ydb::Query::SessionState state;

        while (reader->Read(&state)) {
            if (Paused_.load()) {
                return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Server is paused");
            }
            writer->Write(state);
        }
        
        return reader->Finish();
    }

private:
    Ydb::Query::V1::QueryService::Stub Stub_;
    std::atomic_bool& Paused_;
};

class ServerRestartTest : public testing::Test {
protected:
    ServerRestartTest() {
        Endpoint_ = std::getenv("YDB_ENDPOINT");
        Database_ = std::getenv("YDB_DATABASE");
        Channel_ = grpc::CreateChannel(grpc::string{Endpoint_}, grpc::InsecureChannelCredentials());

        DisoveryService_ = std::make_unique<TDiscoveryProxy>(Channel_, Paused_);
        QueryService_ = std::make_unique<TQueryProxy>(Channel_, Paused_);

        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(DisoveryService_.get());
        builder.RegisterService(QueryService_.get());

        Server_ = builder.BuildAndStart();

        Driver_ = std::make_unique<TDriver>(TDriverConfig()
            .SetEndpoint("localhost:" + std::to_string(port))
            .SetDatabase(Database_)
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
    std::string Endpoint_;
    std::string Database_;
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
            auto settings = TCreateSessionSettings().ClientTimeout(TDuration::MilliSeconds(100));
            auto sessionResult = client.GetSession(settings).ExtractValueSync();

            status = sessionResult;

            ASSERT_EQ(client.GetActiveSessionCount(), 1);
            ASSERT_EQ(client.GetCurrentPoolSize(), 0);

            std::this_thread::sleep_for(100ms);
        }

        ASSERT_TRUE(status.has_value());
        ASSERT_TRUE(status->IsSuccess()) << ToString(*status);
    });

    std::this_thread::sleep_for(1s);
    PauseServer();
    std::this_thread::sleep_for(5s);
    UnpauseServer();
    std::this_thread::sleep_for(1s);

    closed.store(true);
    thread.join();

    ASSERT_EQ(client.GetActiveSessionCount(), 0);
}
