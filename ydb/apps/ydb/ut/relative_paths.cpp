#include "mock_env.h"

Y_UNIT_TEST_SUITE(RelativePaths) {
    Y_UNIT_TEST_F(CompletionAndRootRemoval, TCliTestFixture) {
        auto& scheme = Service<TSchemeImpl>();
        scheme.Database = "tenant";
        for (const TString prefix : {"", "data/"}) {
            scheme.ExpectChild(prefix.empty() ? "." : "data", "orders", Ydb::Scheme::Entry::TABLE);
            const auto output = RunCli({
                "-e", GetEndpoint(), "-d", "tenant", "scheme", "ls", ".",
                "---CUSTOM-COMPLETION---", "SchemePathCompleterTable", "7", prefix, "",
            });
            UNIT_ASSERT_VALUES_EQUAL(output, prefix + "\n/\n" + prefix + "orders\n");
        }
        for (const TString path : {"/", "/./"}) {
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", "tenant", "scheme", "rmdir", "--force", path});
        }
    }
}
