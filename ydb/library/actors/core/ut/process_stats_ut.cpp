#include <process_stats.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TProcStat) {
    Y_UNIT_TEST(Fill) {
        TProcStat procStat;

        // Note: doesn't work in CI
        Cerr << "Fill = " << procStat.Fill(getpid()) << Endl;
        Cerr << "AnonRss = " << procStat.AnonRss << Endl;
        Cerr << "CGroupMemLim = " << procStat.CGroupMemLim << Endl;
        Cerr << "MemTotal = " << procStat.MemTotal << Endl;
        Cerr << "MemAvailable = " << procStat.MemAvailable << Endl;
    }
}
