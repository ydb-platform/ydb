#include "test_utils.h"

#if defined(__x86_64__)
#include <immintrin.h>
#endif

namespace {

template <bool Supported>
bool CpuSupported() {
    return Supported;
}

template <bool Supported>
class TTestSuiteProbe: public NSimd::NTest::TCpuTestSuite<CpuSupported<Supported>> {
    using TBase = NSimd::NTest::TCpuTestSuite<CpuSupported<Supported>>;

protected:
    void Run(std::function<void()> test, const TString& suite, const char* name, bool forceFork) {
        bool executed = false;
        TBase::Run([&] {
            executed = true;
            test();
        }, suite, name, forceFork);

        if (!(this->GetForkTests() || forceFork) || this->GetIsForked()) {
            UNIT_ASSERT_VALUES_EQUAL(executed, Supported);
        }
    }
};

using TSupportedTestSuite = TTestSuiteProbe<true>;
using TUnsupportedTestSuite = TTestSuiteProbe<false>;

struct TUnsupportedFixture: public NUnitTest::TBaseFixture {
    void SetUp(NUnitTest::TTestContext&) override {
        UNIT_FAIL("The fixture must not run without the required CPU instructions");
    }

    void TearDown(NUnitTest::TTestContext&) override {
        UNIT_FAIL("The fixture must not run without the required CPU instructions");
    }
};

} // namespace

Y_UNIT_TEST_SUITE_IMPL(TSupportedCpuTest, TSupportedTestSuite) {
    Y_UNIT_TEST(ExecutesBody) {
        UNIT_ASSERT(CpuSupported<true>());
    }

    SIMPLE_UNIT_FORKED_TEST(ExecutesForkedBody) {
        UNIT_ASSERT(CpuSupported<true>());
    }
}

Y_UNIT_TEST_SUITE_IMPL_F(TUnsupportedCpuTest, TUnsupportedTestSuite, TUnsupportedFixture) {
    Y_UNIT_TEST(SkipsBodyAndFixture) {
        UNIT_FAIL("The body must not run without the required CPU instructions");
    }

    SIMPLE_UNIT_FORKED_TEST(SkipsForkedBodyAndFixture) {
        UNIT_FAIL("The forked body must not run without the required CPU instructions");
    }
}

#if defined(__x86_64__)
namespace {

bool SimdBodyExecuted = false;

template <bool (*IsSupported)()>
class TSimdMacroProbe: public NUnitTest::TTestBase {
protected:
    void Run(std::function<void()> test, const TString& suite, const char* name, bool forceFork) {
        SimdBodyExecuted = false;
        NUnitTest::TTestBase::Run(test, suite, name, forceFork);
        if (!(GetForkTests() || forceFork) || GetIsForked()) {
            UNIT_ASSERT_VALUES_EQUAL(SimdBodyExecuted, IsSupported());
        }
    }
};

using TAvx2MacroProbe = TSimdMacroProbe<NX86::HaveAVX2>;
using TSse42MacroProbe = TSimdMacroProbe<NX86::HaveSSE42>;

} // namespace

Y_UNIT_TEST_SUITE_IMPL(TAvx2MacroTest, TAvx2MacroProbe) {
    Y_SIMD_UNIT_TEST(ExecutesOnlyWhenSupported, AVX2) {
        SimdBodyExecuted = true;
        volatile int value = 42;
        volatile __m256i data = _mm256_set1_epi32(value);
        UNIT_ASSERT_VALUES_EQUAL(_mm256_extract_epi32(data, 7), value);
    }
}

Y_UNIT_TEST_SUITE_IMPL(TSse42MacroTest, TSse42MacroProbe) {
    Y_SIMD_UNIT_TEST(ExecutesOnlyWhenSupported, SSE42) {
        SimdBodyExecuted = true;
        volatile ui32 value = 42;
        UNIT_ASSERT_VALUES_EQUAL(_mm_crc32_u32(0, value), 2651215084u);
    }
}
#endif
