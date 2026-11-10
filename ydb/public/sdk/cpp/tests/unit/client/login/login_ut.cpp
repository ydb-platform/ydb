#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <jwt-cpp/jwt.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/scope.h>
#include <util/system/event.h>

#include <atomic>
#include <chrono>

using namespace NYdb;

namespace {

    class TDummyClient {
        class TImpl: public TClientImplCommon<TImpl> {
        public:
            explicit TImpl(TDriver& driver)
                : TClientImplCommon(CreateInternalInterface(driver), TCommonClientSettings())
            {
            }

            std::shared_ptr<ICoreFacility> GetCoreFacility() const {
                return DbDriverState_;
            }
        };

    public:
        explicit TDummyClient(TDriver& driver)
            : Impl_(std::make_shared<TImpl>(driver))
        {
        }

        std::shared_ptr<ICoreFacility> GetCoreFacility() const {
            return Impl_->GetCoreFacility();
        }

    private:
        std::shared_ptr<TImpl> Impl_;
    };

    class TLoginService final: public Ydb::Auth::V1::AuthService::Service {
    public:
        explicit TLoginService(Ydb::StatusIds::StatusCode firstStatus,
                               grpc::StatusCode firstTransportStatus = grpc::StatusCode::OK)
            : FirstStatus(firstStatus)
            , FirstTransportStatus(firstTransportStatus)
            , Token(jwt::create()
                        .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours(1))
                        .sign(jwt::algorithm::none{}))
        {
        }

        grpc::Status Login(grpc::ServerContext*, const Ydb::Auth::LoginRequest* request,
                           Ydb::Auth::LoginResponse* response) override {
            if (request->user() != "login-retry-user" || request->password() != "login-retry-password") {
                CredentialsMatch = false;
            }
            const auto attempt = ++Requests;
            if (attempt == 1 && !ReleaseFirst.WaitT(TDuration::Seconds(10))) {
                return grpc::Status(grpc::StatusCode::DEADLINE_EXCEEDED, "Test response release timed out");
            }
            if (attempt == 1 && FirstTransportStatus != grpc::StatusCode::OK) {
                return grpc::Status(FirstTransportStatus, "Test transport failure");
            }
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(attempt == 1 ? FirstStatus : Ydb::StatusIds::SUCCESS);
            if (attempt > 1) {
                Ydb::Auth::LoginResult result;
                result.set_token(Token);
                operation->mutable_result()->PackFrom(result);
            }
            return grpc::Status::OK;
        }

        const Ydb::StatusIds::StatusCode FirstStatus;
        const grpc::StatusCode FirstTransportStatus;
        const std::string Token;
        TManualEvent ReleaseFirst;
        std::atomic_uint Requests{0};
        std::atomic_bool CredentialsMatch{true};
    };

    template <typename TCallback>
    void WithProvider(Ydb::StatusIds::StatusCode firstStatus, TCallback callback,
                      grpc::StatusCode firstTransportStatus = grpc::StatusCode::OK) {
        NTesting::InitPortManagerFromEnv();
        const auto port = NTesting::GetFreePort();
        const std::string endpoint = "127.0.0.1:" + std::to_string(port);
        TLoginService service(firstStatus, firstTransportStatus);
        auto server = grpc::ServerBuilder()
                          .AddListeningPort(TStringType{endpoint}, grpc::InsecureServerCredentials())
                          .RegisterService(&service)
                          .BuildAndStart();
        UNIT_ASSERT(server);
        Y_DEFER {
            service.ReleaseFirst.Signal();
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(5));
            server->Wait();
        };

        TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Off));
        TDummyClient client(driver);
        auto provider = CreateLoginCredentialsProviderFactory({.User = "login-retry-user", .Password = "login-retry-password"})->CreateProvider(client.GetCoreFacility());
        Y_DEFER {
            service.ReleaseFirst.Signal();
            provider.reset();
            driver.Stop(true);
        };

        // Capture this future before either response can complete it. Retrying by
        // obtaining a new future/provider would hide a permanently failed first login.
        const auto future = provider->GetAuthInfoAsync();
        service.ReleaseFirst.Signal();
        callback(service, future);
    }

} // namespace

Y_UNIT_TEST_SUITE(Login) {
    Y_UNIT_TEST(InitialTransientOperationRetries) {
        WithProvider(Ydb::StatusIds::UNAVAILABLE, [](const auto& service, const auto& future) {
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), "Initial login did not finish after a transient operation failure");
            UNIT_ASSERT_C(!future.HasException(), "Cold Login operation UNAVAILABLE permanently failed the original auth future");
            UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), service.Token);
            UNIT_ASSERT(service.Requests.load() >= 2);
            UNIT_ASSERT(service.CredentialsMatch.load());
        });
    }

    Y_UNIT_TEST(InitialUnauthorizedFails) {
        WithProvider(Ydb::StatusIds::UNAUTHORIZED, [](const auto& service, const auto& future) {
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), "Rejected credentials left the initial auth future pending");
            UNIT_ASSERT(future.HasException());
            UNIT_ASSERT(service.Requests.load() >= 1);
            UNIT_ASSERT(service.CredentialsMatch.load());
        });
    }

    Y_UNIT_TEST(InitialTransportUnauthenticatedFails) {
        WithProvider(Ydb::StatusIds::SUCCESS, [](const auto& service, const auto& future) {
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), "Transport rejection left the initial auth future pending");
            UNIT_ASSERT(future.HasException());
            UNIT_ASSERT_VALUES_EQUAL(service.Requests.load(), 1);
            UNIT_ASSERT(service.CredentialsMatch.load());
        }, grpc::StatusCode::UNAUTHENTICATED);
    }
} // Y_UNIT_TEST_SUITE(Login)
