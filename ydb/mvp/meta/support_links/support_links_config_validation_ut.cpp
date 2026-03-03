#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/meta/support_links/support_links_config_validation.h>

Y_UNIT_TEST_SUITE(SupportLinksConfigValidation) {
    Y_UNIT_TEST(RequiresSource) {
        const auto& validation = NMVP::NSupportLinks::TConfigValidation::Default();

        NMVP::TSupportLinkEntryConfig linkConfig;
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            validation.Validate(linkConfig, NMVP::TGrafanaSupportConfig{}, "support_links.cluster[0]"),
            yexception,
            "source is required");
    }

}
