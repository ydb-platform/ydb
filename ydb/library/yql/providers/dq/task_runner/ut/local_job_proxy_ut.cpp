#include <ydb/library/yql/providers/dq/task_runner/local_job_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NTaskRunnerProxy {

    Y_UNIT_TEST_SUITE(TChildLocalJobProxyTest) {
        Y_UNIT_TEST(EnablesLocalRoutingWithoutNestedPorto) {
            THashMap<TString, TString> environment;
            auto settings = ConfigureChildLocalJobProxy(
                &environment, /*useLocalJobProxy*/ true, "http://freud.yt.yandex.net:80", "/yt/job/proxy.sock");

            UNIT_ASSERT(settings);
            UNIT_ASSERT_VALUES_EQUAL(environment.at("YQL_YT_JOB_PROXY_ADDRESS"), "http://freud.yt.yandex.net:80");
            UNIT_ASSERT_VALUES_EQUAL(environment.at("YT_JOB_PROXY_SOCKET_PATH"), "/yt/job/proxy.sock");
        }

        Y_UNIT_TEST(BindsLocalProxyInNestedPorto) {
            THashMap<TString, TString> environment{
                {"OTHER", "preserved"},
            };
            auto settings = ConfigureChildLocalJobProxy(
                &environment, /*useLocalJobProxy*/ true, "http://freud.yt.yandex.net:80", "/yt/job/proxy.sock");

            UNIT_ASSERT(settings);
            settings->ConfigureEnvironment(environment, /*porto*/ true);
            UNIT_ASSERT_VALUES_EQUAL(environment.at("YQL_YT_JOB_PROXY_ADDRESS"), "http://freud.yt.yandex.net:80");
            UNIT_ASSERT_VALUES_EQUAL(environment.at("YT_JOB_PROXY_SOCKET_PATH"), "/tmp/yt-job-proxy.sock");
            UNIT_ASSERT_VALUES_EQUAL(settings->GetPortoBind(), "/yt/job/proxy.sock /tmp/yt-job-proxy.sock ro;");
            UNIT_ASSERT_VALUES_EQUAL(environment.at("OTHER"), "preserved");

            // Per-task settings may disable Porto in the same worker.
            settings->ConfigureEnvironment(environment, /*porto*/ false);
            UNIT_ASSERT_VALUES_EQUAL(environment.at("YT_JOB_PROXY_SOCKET_PATH"), "/yt/job/proxy.sock");
        }

        Y_UNIT_TEST(EscapesPortoBindSource) {
            TLocalJobProxySettings settings{"http://freud.yt.yandex.net:80", "/yt/job dir/pipe;1\\proxy.sock"};
            UNIT_ASSERT_VALUES_EQUAL(settings.GetPortoBind(), "/yt/job\\ dir/pipe\\;1\\\\proxy.sock /tmp/yt-job-proxy.sock ro;");
        }

        Y_UNIT_TEST(RequiresFlagProxyAddressAndSocket) {
            for (bool enabled : {false, true}) {
                for (TStringBuf proxyAddress : {TStringBuf(), TStringBuf("http://freud.yt.yandex.net:80")}) {
                    for (TStringBuf socket : {TStringBuf(), TStringBuf("/yt/job/proxy.sock")}) {
                        THashMap<TString, TString> environment;
                        auto settings = ConfigureChildLocalJobProxy(&environment, enabled, proxyAddress, socket);
                        const bool expected = enabled && !proxyAddress.empty() && !socket.empty();
                        UNIT_ASSERT_VALUES_EQUAL(settings.Defined(), expected);
                        UNIT_ASSERT_VALUES_EQUAL(environment.contains("YQL_YT_JOB_PROXY_ADDRESS"), expected);
                        UNIT_ASSERT_VALUES_EQUAL(environment.contains("YT_JOB_PROXY_SOCKET_PATH"), expected);
                    }
                }
            }
        }
    } // Y_UNIT_TEST_SUITE(TChildLocalJobProxyTest)

} // namespace NYql::NTaskRunnerProxy
