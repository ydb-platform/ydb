#include <ydb/library/workload/tpch/tpch.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/regex/pcre/regexp.h>

using namespace NYdbWorkload;

Y_UNIT_TEST_SUITE(TpchQueries) {
    Y_UNIT_TEST(ScaleFactor) {
        TTpchWorkloadParams params;
        NLastGetopt::TOpts opts;
        params.ConfigureOpts(opts, TWorkloadParams::ECommandType::Run, 0);
        const char* args[] = {"ut", "--scale", "10"};
        for (auto parser = NLastGetopt::TOptsParser(&opts, sizeof(args)/sizeof(*args), args); parser.Next();) {
        }
        auto generator = params.CreateGenerator();
        ui32 query_num = 0;
        for (const auto& query: generator->GetWorkload(0)) {
            if (query_num == 11) {
                UNIT_ASSERT_C(TRegExMatch("\\$scale_factor\\s*=\\s*10\\s*;").Match(query.Query.c_str()),
                    "Scale factor is wrong for q11:" << Endl << query.Query);
                break;
            }
            ++query_num;
        }
    }
};
