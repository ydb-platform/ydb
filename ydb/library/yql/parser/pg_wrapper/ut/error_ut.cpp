#include "../pg_compat.h"

#include <library/cpp/testing/unittest/registar.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/postgres.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/utils/elog.h>
}

#include <util/datetime/cputimer.h>

#if defined(NDEBUG) && !defined(_san_enabled_)
constexpr ui32 IterationsCount = 1000000000;
#else
constexpr ui32 IterationsCount = 100000000;
#endif

Y_UNIT_TEST_SUITE(TErrorTests) {
    Y_UNIT_TEST(TestPgTry) {
        Cout << "begin...\n";
        TSimpleTimer timer;
        volatile ui32 x = 0;
        for (ui32 i = 0; i < IterationsCount; ++i) {
            PG_TRY();
            {
                x += 1;
            }
            PG_CATCH();
            {
            }
            PG_END_TRY();
        }

        Cout << "done, elapsed: " << timer.Get() << "\n";
    }

    Y_UNIT_TEST(TestCppTry) {
        Cout << "begin...\n";
        TSimpleTimer timer;
        volatile ui32 x = 0;
        for (ui32 i = 0; i < IterationsCount; ++i) {
            try {
                x += 1;
            } catch (...) {
            }
        }

        Cout << "done, elapsed: " << timer.Get() << "\n";
    }
}
