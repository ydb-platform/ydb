#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/system/cpu_id.h>

namespace NSimd::NTest {

template <bool (*IsSupported)(), typename TTest>
void RunCpuTest(TTest&& test, const TString& suite, const char* name) {
    if (IsSupported()) {
        test();
    } else {
        Cerr << "Skipped " << suite << "::" << name << ": CPU instruction set is unavailable" << Endl;
    }
}

// Keep the CPU check outside functions compiled for the required instruction set.
template <bool (*IsSupported)()>
class TCpuTestSuite: public NUnitTest::TTestBase {
protected:
    void Run(std::function<void()> test, const TString& suite, const char* name, bool forceFork) {
        NUnitTest::TTestBase::Run([&] {
            RunCpuTest<IsSupported>(test, suite, name);
        }, suite, name, forceFork);
    }
};

using TAvx2TestSuite = TCpuTestSuite<NX86::HaveAVX2>;
using TSse42TestSuite = TCpuTestSuite<NX86::HaveSSE42>;

} // namespace NSimd::NTest

#define Y_SIMD_TARGET_AVX2 "avx2"
#define Y_SIMD_TARGET_SSE42 "sse4.2"

// Registration and the CPU check use baseline instructions. Only the separate
// body is compiled for ISA, which also selects the runtime CPU check.
#define Y_SIMD_UNIT_TEST(N, ISA) \
    static void SimdBody##N(NUnitTest::TTestContext&) __attribute__((target(Y_SIMD_TARGET_##ISA))); \
    Y_UNIT_TEST(N) { \
        NSimd::NTest::RunCpuTest<NX86::Have##ISA>([&] { \
            SimdBody##N(ut_context); \
        }, TCurrentTest::StaticName(), #N); \
    } \
    static void SimdBody##N(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)
