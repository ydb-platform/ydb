#include "yaml_config_helpers.h"

#include <ydb/library/yaml_config/ut/protos/test_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NYaml;

Y_UNIT_TEST_SUITE(DefaultOpaqueConfigParser) {

    Y_UNIT_TEST(ParsesFieldsCorrectly) {
        const TString yamlTemplate = R"(
service_name: my-service
max_connections: 128
tls_enabled: true
endpoints:
- host: node1.example.com
  port: 8080
- host: node2.example.com
  port: 9090
)";
        TString yaml = "---\n" + yamlTemplate;
        // Checks for YAML doc and YAML subfield
        for ( int i = 0; i < 2; i++ ) {
            auto msg = DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(yaml, true);
            UNIT_ASSERT(msg);
            // dynamic_cast is to verify that DefaultOpaqueConfigParser<TTestServiceConfig>
            // actually returns a TTestServiceConfig inside the shared_ptr<Message>.
            // CopyFrom() would silently succeed even if the function returned a wrong proto type
            // (as long as fields happened to match), masking a real bug.
            // CopyFrom() is correct choise for production code where you already trust the source type
            // and just need to extract the data.
            auto* cfg = dynamic_cast<const NYamlConfigTest::TTestServiceConfig*>(msg.get());
            UNIT_ASSERT(cfg != nullptr);

            UNIT_ASSERT(cfg->HasServiceName());
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetServiceName(), "my-service");

            UNIT_ASSERT(cfg->HasMaxConnections());
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetMaxConnections(), 128u);

            UNIT_ASSERT(cfg->HasTlsEnabled());
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetTlsEnabled(), true);

            UNIT_ASSERT_VALUES_EQUAL(cfg->EndpointsSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetEndpoints(0).GetHost(), "node1.example.com");
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetEndpoints(0).GetPort(), 8080u);
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetEndpoints(1).GetHost(), "node2.example.com");
            UNIT_ASSERT_VALUES_EQUAL(cfg->GetEndpoints(1).GetPort(), 9090u);

            yaml = yamlTemplate;
        }
    }

    Y_UNIT_TEST(AbsentFieldsAreAbsent) {
        const TString yaml = R"(---
service_name: minimal
)";
        auto msg = DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(yaml, true);
        UNIT_ASSERT(msg);
        NYamlConfigTest::TTestServiceConfig cfg;
        cfg.CopyFrom(*msg);

        UNIT_ASSERT(cfg.HasServiceName());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetServiceName(), "minimal");

        UNIT_ASSERT(!cfg.HasMaxConnections());
        UNIT_ASSERT(!cfg.HasTlsEnabled());
        UNIT_ASSERT_VALUES_EQUAL(cfg.EndpointsSize(), 0);
    }

    Y_UNIT_TEST(AllowsUnknownFieldsWhenEnabled) {
        const TString yaml = R"(
service_name: my-service
unknown_field_xyz: some_value
)";
        auto msg = DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(yaml, true);
        UNIT_ASSERT(msg);
        NYamlConfigTest::TTestServiceConfig cfg;
        cfg.CopyFrom(*msg);

        UNIT_ASSERT(cfg.HasServiceName());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetServiceName(), "my-service");
    }

    Y_UNIT_TEST(ThrowsOnUnknownFieldsWhenDisabled) {
        const TString yaml = R"(
service_name: my-service
unknown_field_xyz: some_value
)";
        UNIT_CHECK_GENERATED_EXCEPTION(
            DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(yaml, false),
            std::exception
        );
    }

    Y_UNIT_TEST(EmptyInputProducesEmptyProto) {
        const TString blanks[] = {"", " ", "\t", "\n", "\r\n", "  \t\n  \r\n  "};

        TString prefix = "---";
        // Checks for YAML doc and YAML subfield
        for ( int i = 0; i < 2; i++ ) {
            for (const auto& input : blanks) {
                auto msg = DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(prefix + input, true);
                UNIT_ASSERT(!msg);
            }

            prefix = "";
        }
    }

    Y_UNIT_TEST(ThrowsOnMalformedYaml) {
        TString prefix = "---\n";
        // Checks for YAML doc and YAML subfield
        for ( int i = 0; i < 2; i++ ) {
            // Completely invalid YAML: bad indentation
            UNIT_CHECK_GENERATED_EXCEPTION(
                DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(
                    prefix + "some random text", true),
                std::exception
            );

            // Unclosed quote
            UNIT_CHECK_GENERATED_EXCEPTION(
                DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(
                    prefix + "service_name: \"unclosed\n", true),
                std::exception
            );

            // Duplicate key (strict YAML parsers reject this)
            UNIT_CHECK_GENERATED_EXCEPTION(
                DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(
                    prefix + "service_name: first\nservice_name: second\n", true),
                std::exception
            );

            // Tabs in indentation (forbidden by YAML spec)
            UNIT_CHECK_GENERATED_EXCEPTION(
                DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>(
                    prefix + "endpoints:\n\t- host: bad\n", true),
                std::exception
            );

            prefix = "";
        }
    }
    Y_UNIT_TEST(CheckParserCreation) {
        const TString yaml = R"(
service_name: my-service
unknown_field_xyz: some_value
)";
        auto parser = std::bind(NKikimr::NYaml::DefaultOpaqueConfigParser<NYamlConfigTest::TTestServiceConfig>, std::placeholders::_1, true);

        auto msg = parser(yaml);
        UNIT_ASSERT(msg);
        NYamlConfigTest::TTestServiceConfig cfg;
        cfg.CopyFrom(*msg);

        UNIT_ASSERT(cfg.HasServiceName());
        UNIT_ASSERT_VALUES_EQUAL(cfg.GetServiceName(), "my-service");
    }
}
