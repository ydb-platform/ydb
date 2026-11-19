#include "mock_env.h"

Y_UNIT_TEST_SUITE(SchemeCompletionTest) {
    Y_UNIT_TEST_F(RelativeDatabaseRequestsHaveClientDeadlines, TCliTestFixture) {
        auto& scheme = Service<TSchemeImpl>();
        scheme.Database = "tenant";
        scheme.MaxClientTimeout = TDuration::Seconds(3);
        scheme.ExpectChild("data", "orders", Ydb::Scheme::Entry::TABLE);

        const auto output = RunCli({
            "-e", GetEndpoint(), "-d", "tenant", "scheme", "ls", "data/",
            "---CUSTOM-COMPLETION---", "SchemePathCompleterTable", "7", "data/", "",
        });

        UNIT_ASSERT_VALUES_EQUAL(output, "data/\n/\ndata/orders\n");
    }

    Y_UNIT_TEST_F(RelativeDatabaseCompletionListsDatabaseRoot, TCliTestFixture) {
        auto& scheme = Service<TSchemeImpl>();
        scheme.Database = "tenant";
        scheme.ExpectChild(".", "orders", Ydb::Scheme::Entry::TABLE);

        const auto output = RunCli({
            "-e", GetEndpoint(), "-d", "tenant", "scheme", "ls", ".",
            "---CUSTOM-COMPLETION---", "SchemePathCompleterTable", "7", "", "",
        });

        UNIT_ASSERT_VALUES_EQUAL(output, "\n/\norders\n");
    }
}
