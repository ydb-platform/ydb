#include "client.h"

#include "config.h"

#include <ydb/core/nbs/cloud/blockstore/compat/libs/diagnostics/server_stats.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/compat/libs/service/service_test.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler_test.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/timer.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/monitoring.h>
#include <ydb/core/nbs/cloud/storage/core/libs/grpc/tls_certificate_provider.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

using NYdb::NBS::CreateCertificateProviderStub;
using NYdb::NBS::CreateLoggingService;
using NYdb::NBS::CreateMonitoringServiceStub;
using NYdb::NBS::CreateSchedulerStub;
using NYdb::NBS::CreateWallClockTimer;
using NYdb::NBS::E_NOT_IMPLEMENTED;
using NYdb::NBS::FormatError;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TClientTest)
{
    Y_UNIT_TEST(
        ShouldReturnNotImplementedErrorStatusCodeForUnsupportedRequestsFromDataEndpoint)
    {
        TPortManager portManager;
        ui16 dataPort = portManager.GetPort(9001);

        NProto::TClientAppConfig clientAppConfig;
        auto& clientConfig = *clientAppConfig.MutableClientConfig();
        clientConfig.SetPort(dataPort);
        clientConfig.SetClientId(CreateGuidAsString());

        auto result = CreateClient(
            std::make_shared<TClientAppConfig>(clientAppConfig),
            CreateWallClockTimer(),
            CreateSchedulerStub(),
            CreateLoggingService("console"),
            CreateMonitoringServiceStub(),
            CreateServerStatsStub(),
            CreateCertificateProviderStub());

        auto client = result.ExtractResult();
        UNIT_ASSERT_C(client, FormatError(result.GetError()));

        auto clientEndpoint = client->CreateDataEndpoint();

        client->Start();
        clientEndpoint->Start();
        Y_DEFER
        {
            clientEndpoint->Stop();
            client->Stop();
        };

        auto futurePing = clientEndpoint->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>());
        UNIT_ASSERT(futurePing.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_IMPLEMENTED,
            futurePing.GetValue().GetError().GetCode());
    }
}

}   // namespace NCloud::NBlockStore::NClient
