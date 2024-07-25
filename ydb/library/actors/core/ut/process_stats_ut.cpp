#include <process_stats.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TProcStat) {
    Y_UNIT_TEST(Fill) {
        TProcStat procStat;
        UNIT_ASSERT(procStat.Fill(getpid()));

        Cerr << "AnonRss = " << procStat.AnonRss << Endl;
        Cerr << "CGroupMemLim = " << procStat.CGroupMemLim << Endl;
        Cerr << "MemTotal = " << procStat.MemTotal << Endl;
        Cerr << "MemAvailable = " << procStat.MemAvailable << Endl;
    }
}
