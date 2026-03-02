#include <library/cpp/testing/unittest/registar.h>

#include <ydb/mvp/meta/support_links/resolver_factory.h>

#include <memory>

Y_UNIT_TEST_SUITE(SupportLinksSourceFactory) {
    Y_UNIT_TEST(BuildsHandlerForGrafanaDashboardSource) {
        NMVP::NSupportLinks::TLinkResolveContext context;
        context.LinkConfig.Source = "grafana/dashboard";
        std::unique_ptr<NActors::IActor> actor(NMVP::NSupportLinks::BuildSourceHandler(std::move(context)));
        UNIT_ASSERT(actor != nullptr);
    }

    Y_UNIT_TEST(BuildsHandlerForGrafanaDashboardSearchSource) {
        NMVP::NSupportLinks::TLinkResolveContext context;
        context.LinkConfig.Source = "grafana/dashboard/search";
        std::unique_ptr<NActors::IActor> actor(NMVP::NSupportLinks::BuildSourceHandler(std::move(context)));
        UNIT_ASSERT(actor != nullptr);
    }

    Y_UNIT_TEST(IgnoresContextSourceNameAndBuildsByLinkConfigSource) {
        NMVP::NSupportLinks::TLinkResolveContext context;
        context.SourceName = "unknown/source";
        context.LinkConfig.Source = "grafana/dashboard";

        std::unique_ptr<NActors::IActor> actor(NMVP::NSupportLinks::BuildSourceHandler(std::move(context)));
        UNIT_ASSERT(actor != nullptr);
    }

    Y_UNIT_TEST(ThrowsOnUnsupportedSource) {
        NMVP::NSupportLinks::TLinkResolveContext context;
        context.LinkConfig.Source = "unknown/source";

        bool thrown = false;
        try {
            [[maybe_unused]] auto* actor = NMVP::NSupportLinks::BuildSourceHandler(std::move(context));
        } catch (const yexception&) {
            thrown = true;
        }

        UNIT_ASSERT(thrown);
    }
}
