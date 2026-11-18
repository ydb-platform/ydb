#include "mock_env.h"

Y_UNIT_TEST_SUITE(SchemeCompletionTest) {
    Y_UNIT_TEST_F(RelativeDatabaseRequestsHaveClientDeadlines, TCliTestFixture) {
        auto& scheme = Service<TSchemeImpl>();
        const TString database = GetDatabase() + "/tenant";
        scheme.BootstrapDatabase = "tenant";
        scheme.Database = database;
        scheme.MaxClientTimeout = TDuration::Seconds(3);
        scheme.ExpectChild("/", "test_database", Ydb::Scheme::Entry::DIRECTORY)
            .ExpectChild(database + "/data", "orders", Ydb::Scheme::Entry::TABLE);

        const auto output = RunCli({
            "-e", GetEndpoint(), "-d", "tenant", "scheme", "ls", "data/",
            "---CUSTOM-COMPLETION---", "SchemePathCompleterTable", "7", "data/", "",
        });

        UNIT_ASSERT_VALUES_EQUAL(output, "data/\n/\ndata/orders\n");
    }
}
