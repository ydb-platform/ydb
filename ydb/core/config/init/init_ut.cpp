#include "init_impl.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NConfig;

Y_UNIT_TEST_SUITE(Init) {
    Y_UNIT_TEST(TWithDefaultParser) {
        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd"};
            NLastGetopt::TOptsParseResult res(&opts, args.size(), args.data());

            UNIT_ASSERT(!commonOpts.ClusterName);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.ClusterName, "unknown");

            UNIT_ASSERT(!commonOpts.LogSamplingLevel);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.LogSamplingLevel, NActors::NLog::PRI_DEBUG);
        }

        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd", "--cluster-name", "unknown", "--log-sampling-level", "7"};
            NLastGetopt::TOptsParseResult res(&opts, args.size(), args.data());

            UNIT_ASSERT(commonOpts.ClusterName);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.ClusterName, "unknown");

            UNIT_ASSERT(commonOpts.LogSamplingLevel);
            UNIT_ASSERT_VALUES_EQUAL(*commonOpts.LogSamplingLevel, NActors::NLog::PRI_DEBUG);
        }

        {
            NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
            TCommonAppOptions commonOpts;
            commonOpts.RegisterCliOptions(opts);
            TVector<const char*> args = {"ydbd", "--log-sampling-level", "string"};

            auto parse = [&]() {
                NLastGetopt::TOptsParseResultException res(&opts, args.size(), args.data());
            };

            UNIT_ASSERT_EXCEPTION(parse(), NLastGetopt::TUsageException);
        }
    }
}
