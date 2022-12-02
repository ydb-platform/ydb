#include <ydb/services/ydb/ydb_common_ut.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/core_facility/core_facility.h>

#define INCLUDE_YDB_INTERNAL_H

/// !!!! JUST FOR UT, DO NOT COPY-PASTE !!! ///
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/driver/constants.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

using namespace NYdb;

namespace {

class TExampleDummyProviderFactory : public ICredentialsProviderFactory {
    class TExampleDummyProvider : public ICredentialsProvider {
    private:
        TPeriodicCb CreatePingPongTask(std::weak_ptr<ICoreFacility> facility) {
            auto periodicCb = [this, facility](NYql::TIssues&&, EStatus status) {
                if (status != EStatus::SUCCESS) {
                    return false;
                }

                auto strong = facility.lock();
                if (!strong)
                    return false;

                auto responseCb = [this](Draft::Dummy::PingResponse* resp, TPlainStatus status) -> void {
                    UNIT_ASSERT(status.Ok());
                    UNIT_ASSERT_VALUES_EQUAL(resp->payload(), "abc");
                    (*RunCnt)++;
                };

                Draft::Dummy::PingRequest request;
                request.set_copy(true);
                request.set_payload("abc");

                TRpcRequestSettings rpcSettings;
                rpcSettings.ClientTimeout = TDuration::Seconds(1);

                TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Draft::Dummy::DummyService, Draft::Dummy::PingRequest, Draft::Dummy::PingResponse>(
                    strong, std::move(request), std::move(responseCb), &Draft::Dummy::DummyService::Stub::AsyncPing,
                    rpcSettings);
                return true;
            };
            return periodicCb;
        }
    public:
        TExampleDummyProvider(std::weak_ptr<ICoreFacility> facility, int* runCnt)
            : RunCnt(runCnt)
        {
            auto strong = facility.lock();
            // must be able to promote in ctor
            Y_VERIFY(strong);
            strong->AddPeriodicTask(CreatePingPongTask(facility), TDuration::Seconds(1));
        }

        TString GetAuthInfo() const override {
            return "";
        }

        bool IsValid() const override {
            return true;
        }
    private:
        int* RunCnt;
    };

public:
    TExampleDummyProviderFactory(int* runCnt)
        : RunCnt(runCnt)
    {}

    std::shared_ptr<ICredentialsProvider> CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        return std::make_shared<TExampleDummyProvider>(std::move(facility), RunCnt);
    }

    // Just for compatibility with old interface
    std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        Y_FAIL();
        return {};
    }

private:
    int* RunCnt;

};

void RunTest(bool sync) {
    TKikimrWithGrpcAndRootSchema server;
    ui16 grpc = server.GetPort();

    TString location = TStringBuilder() << "localhost:" << grpc;

    int runCnt = 0;

    {
        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location)
                .SetDiscoveryMode(sync ? EDiscoveryMode::Sync : EDiscoveryMode::Async)
                .SetCredentialsProviderFactory(std::make_shared<TExampleDummyProviderFactory>(&runCnt)));
        // Creates DbDriverState in case of empty database
        auto client = NYdb::NTable::TTableClient(driver);

        Y_UNUSED(client);
        Sleep(TDuration::Seconds(5));
    }

    Cerr << runCnt << Endl;
    UNIT_ASSERT_C(runCnt > 1, runCnt);
}

}

Y_UNIT_TEST_SUITE(SdkCredProvider) {
    Y_UNIT_TEST(PingFromProviderSyncDiscovery) {
        RunTest(true);
    }

    Y_UNIT_TEST(PingFromProviderAsyncDiscovery) {
        RunTest(false);
    }
}

