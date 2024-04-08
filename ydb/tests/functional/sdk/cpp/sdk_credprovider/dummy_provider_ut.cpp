#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>

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

                auto responseCb = [this](Ydb::Table::CreateSessionResponse* resp, TPlainStatus status) -> void {
                    if (status.Status == EStatus::CLIENT_CANCELLED)
                        return;

                    UNIT_ASSERT_C(status.Ok(), status.Status);
                    UNIT_ASSERT(resp->operation().ready());
                    (*RunCnt)++;
                };

                // use CreateSession as ping pong
                Ydb::Table::CreateSessionRequest request;

                TRpcRequestSettings rpcSettings;
                rpcSettings.ClientTimeout = TDuration::Seconds(1);

                TGRpcConnectionsImpl::RunOnDiscoveryEndpoint<Ydb::Table::V1::TableService,
                    Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>
                (
                    strong, std::move(request), std::move(responseCb), &Ydb::Table::V1::TableService::Stub::AsyncCreateSession,
                    rpcSettings
                );
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
            Y_ABORT_UNLESS(strong);
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
        Y_ABORT();
        return {};
    }

private:
    int* RunCnt;

};

void RunTest(bool sync) {
    int runCnt = 0;
    int maxWait = 10;

    TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");

    auto driver = NYdb::TDriver(
        TDriverConfig(connectionString)
            .SetDiscoveryMode(sync ? EDiscoveryMode::Sync : EDiscoveryMode::Async)
            .SetCredentialsProviderFactory(std::make_shared<TExampleDummyProviderFactory>(&runCnt)));
    // Creates DbDriverState in case of empty database
    auto client = NYdb::NTable::TTableClient(driver);
    Y_UNUSED(client);

    while (runCnt < 2 && maxWait--)
        Sleep(TDuration::Seconds(1));

    Cerr << runCnt << Endl;
    UNIT_ASSERT_C(runCnt > 1, runCnt);
    driver.Stop(true);
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
