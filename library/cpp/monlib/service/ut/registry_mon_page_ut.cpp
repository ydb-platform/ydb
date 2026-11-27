#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/monlib/consumers/collecting_consumer.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/registry_mon_page.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/str.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TMetricRegistryPageTest) {
    Y_UNIT_TEST(SpackVersionQueryParameter) {
        TPortManager portManager;
        const ui16 port = portManager.GetPort();
        const TLabels labels{{"sensor", "requests"}};
        TMetricRegistry registry;
        registry.Counter(labels)->Inc();
        TMonService2 server(port, 1, "test");
        server.Register(new TMetricRegistryPage("metrics", "Metrics", &registry));
        server.StartOrThrow();

        TSimpleHttpClient client("localhost", port);
        auto checkVersion = [&](TStringBuf query, ui16 expectedVersion) {
            TStringStream body;
            client.DoGet(TString::Join("/metrics/spack", query), &body, {{"Accept-Encoding", "zstd"}});

            TSpackHeader header;
            TStringInput(body.Str()).LoadOrFail(&header, sizeof(header));
            UNIT_ASSERT_VALUES_EQUAL_C(header.Version, expectedVersion, query);
            UNIT_ASSERT_VALUES_EQUAL(header.Compression, EncodeCompression(ECompression::ZSTD));

            TCollectingConsumer consumer;
            DecodeSpackV1(&body, &consumer, "sensor");
            UNIT_ASSERT_VALUES_EQUAL(consumer.Metrics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(consumer.Metrics.front().Labels, labels);
        };

        checkVersion("?spackVersion=1.2", SV1_02);
        checkVersion("?spackVersion=v1_3", SV1_03);
        checkVersion("?spackVersion=SPACK_1.4&spackVersion=1.2", SV1_04);
        checkVersion("", SV1_01);
        checkVersion("?spackVersion=", SV1_01);
        checkVersion("?spackVersion=1.0", SV1_01);
        checkVersion("?spackVersion=1.9", SV1_01);
    }
} // Y_UNIT_TEST_SUITE(TMetricRegistryPageTest)
