#include "backtrace.h"
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <library/cpp/testing/unittest/registar.h>
namespace {
    Y_NO_INLINE void TestTrace394() {
        TStringStream ss;
        NYql::NBacktrace::KikimrBackTraceFormatImpl(&ss);
#if !defined(_hardening_enabled_) && !defined(_win_)
        UNIT_ASSERT_STRING_CONTAINS(ss.Str(), "(anonymous namespace)::TestTrace394");
#endif
    }
    Y_NO_INLINE void TestTrace39114() {
        TStringStream ss;
        NYql::NBacktrace::KikimrBackTraceFormatImpl(&ss);
#if !defined(_hardening_enabled_) && !defined(_win_)
        UNIT_ASSERT_STRING_CONTAINS(ss.Str(), "(anonymous namespace)::TestTrace39114");
#endif
    }
}

Y_UNIT_TEST_SUITE(TEST_BACKTRACE_AND_SYMBOLIZE) {
    Y_UNIT_TEST(TEST_NO_KIKIMR) {
        NYql::NBacktrace::EnableKikimrSymbolize();
        TestTrace394();
        TestTrace39114();
    }
}
