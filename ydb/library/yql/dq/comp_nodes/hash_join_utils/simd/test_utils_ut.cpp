#include "test_utils.h"
#include "ut/test_utils_enums.h"

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

Y_CPU_UNIT_TEST_SUITE_IMPL(TSupportedCpuTest, TSupportedTestSuite, NUnitTest::TBaseTestCase, "sse2", {
    Y_CPU_UNIT_TEST(ExecutesBody) {
        UNIT_ASSERT(CpuSupported<true>());
    }

    Y_CPU_FORKED_UNIT_TEST(ExecutesForkedBody) {
        UNIT_ASSERT(CpuSupported<true>());
    }

    Y_CPU_UNIT_TEST_TWIN(ExecutesTwinBody, Enabled) {
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(Name_).EndsWith("+Enabled"), Enabled);
    }

    Y_CPU_UNIT_TEST(ExecutesParametrizedBody, NSimd::NTest::ETestFirst, NSimd::NTest::ETestSecond) {
        UNIT_ASSERT(Arg<0>() == NSimd::NTest::ETestFirst::First || Arg<0>() == NSimd::NTest::ETestFirst::Second);
        UNIT_ASSERT(Arg<1>() == NSimd::NTest::ETestSecond::First || Arg<1>() == NSimd::NTest::ETestSecond::Second);
        UNIT_ASSERT(TStringBuf(Name_).StartsWith("ExecutesParametrizedBody-"));
    }
})

Y_CPU_UNIT_TEST_SUITE_IMPL(TUnsupportedCpuTest, TUnsupportedTestSuite, TUnsupportedFixture, "sse2", {
    Y_CPU_UNIT_TEST(SkipsBodyAndFixture) {
        UNIT_FAIL("The body must not run without the required CPU instructions");
    }

    Y_CPU_FORKED_UNIT_TEST(SkipsForkedBodyAndFixture) {
        UNIT_FAIL("The forked body must not run without the required CPU instructions");
    }

    Y_CPU_UNIT_TEST_TWIN(SkipsTwinBodyAndFixture, Enabled) {
        UNIT_FAIL("The twin body must not run without the required CPU instructions");
    }

    Y_CPU_UNIT_TEST(SkipsParametrizedBodyAndFixture, NSimd::NTest::ETestFirst, NSimd::NTest::ETestSecond) {
        UNIT_FAIL("The parametrized body must not run without the required CPU instructions");
    }
})

#if defined(__x86_64__)
namespace {

bool SimdBodyExecuted = false;

template <bool (*IsSupported)()>
class TSimdMacroProbe: public NSimd::NTest::TCpuTestSuite<IsSupported> {
    using TBase = NSimd::NTest::TCpuTestSuite<IsSupported>;

protected:
    void Run(std::function<void()> test, const TString& suite, const char* name, bool forceFork) {
        SimdBodyExecuted = false;
        TBase::Run(test, suite, name, forceFork);
        if (!(this->GetForkTests() || forceFork) || this->GetIsForked()) {
            UNIT_ASSERT_VALUES_EQUAL(SimdBodyExecuted, IsSupported());
        }
    }
};

using TAvx2MacroProbe = TSimdMacroProbe<NX86::HaveAVX2>;
using TSse42MacroProbe = TSimdMacroProbe<NX86::HaveSSE42>;

} // namespace

Y_CPU_UNIT_TEST_SUITE_IMPL(TAvx2MacroTest, TAvx2MacroProbe, NUnitTest::TBaseTestCase, Y_CPU_TEST_TARGET_AVX2, {
    Y_CPU_UNIT_TEST(ExecutesOnlyWhenSupported) {
        SimdBodyExecuted = true;
        volatile int value = 42;
        volatile __m256i data = _mm256_set1_epi32(value);
        UNIT_ASSERT_VALUES_EQUAL(_mm256_extract_epi32(data, 7), value);
    }
})

Y_CPU_UNIT_TEST_SUITE_IMPL(TSse42MacroTest, TSse42MacroProbe, NUnitTest::TBaseTestCase, Y_CPU_TEST_TARGET_SSE42, {
    Y_CPU_UNIT_TEST(ExecutesOnlyWhenSupported) {
        SimdBodyExecuted = true;
        volatile ui32 value = 42;
        UNIT_ASSERT_VALUES_EQUAL(_mm_crc32_u32(0, value), 2651215084u);
    }
})
#endif
