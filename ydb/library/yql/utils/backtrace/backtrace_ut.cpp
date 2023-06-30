#include "backtrace.h"
#include "symbolizer.h"
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <library/cpp/testing/unittest/registar.h>
namespace {
    Y_NO_INLINE void TestTrace() {
        void* array[300];
        const size_t s = BackTrace(array, Y_ARRAY_SIZE(array));
        auto symbolizer = BuildSymbolizer(false);
        TStringBuilder output;
        for (size_t i = 0; i < s; ++i) {
            output << symbolizer->SymbolizeFrame(array[i]);
        }
#ifndef _hardening_enabled_
        UNIT_ASSERT(NYql::NBacktrace::Symbolize(output, {}).find("(anonymous namespace)::TestTrace() at") != TString::npos);
#endif
    }
}

Y_UNIT_TEST_SUITE(TEST_BACKTRACE_AND_SYMBOLIZE) {
    Y_UNIT_TEST(TEST_NO_KIKIMR) {
        TestTrace();
    }
}
