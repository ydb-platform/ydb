#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/system/cpu_id.h>

namespace NSimd::NTest {

// The dispatcher and registration stay outside the target-specific region.
template <bool (*IsSupported)()>
class TCpuTestSuite: public NUnitTest::TTestBase {
protected:
    void Run(std::function<void()> test, const TString& suite, const char* name, bool forceFork) {
        NUnitTest::TTestBase::Run([&] {
            if (IsSupported()) {
                test();
            } else {
                Cerr << "Skipped " << suite << "::" << name << ": CPU instruction set is unavailable" << Endl;
            }
        }, suite, name, forceFork);
    }
};

template <class TBody>
class TTestCase: public TBody {
public:
    TTestCase(const char* name, const char* file, int line, bool forceFork) {
        this->Name_ = name;
        this->File_ = file;
        this->Line_ = line;
        this->ForceFork_ = forceFork;
    }

    void Execute_(NUnitTest::TTestContext& context) override {
        this->ExecuteBody(context);
    }
};

template <class TSuite, class TBody>
struct TTestRegistration {
    TTestRegistration(const char* name, const char* file, int line, bool forceFork = false) {
        TSuite::AddTest([=]() -> THolder<NUnitTest::TBaseTestCase> {
            return MakeHolder<TTestCase<TBody>>(name, file, line, forceFork);
        });
    }
};

} // namespace NSimd::NTest

#define Y_CPU_TEST_STRINGIFY_IMPL(...) #__VA_ARGS__
#define Y_CPU_TEST_PRAGMA(...) _Pragma(Y_CPU_TEST_STRINGIFY_IMPL(__VA_ARGS__))
#define Y_CPU_TEST_TARGET_AVX2 "avx2"
#define Y_CPU_TEST_TARGET_SSE42 "sse4.2"

#if defined(__x86_64__) || defined(__i386__)
#define Y_CPU_TEST_TARGET_PUSH(TARGET) \
    Y_CPU_TEST_PRAGMA(clang attribute push(__attribute__((target(TARGET))), apply_to=function(is_member)))
#define Y_CPU_TEST_TARGET_POP() Y_CPU_TEST_PRAGMA(clang attribute pop)
#else
#define Y_CPU_TEST_TARGET_PUSH(TARGET)
#define Y_CPU_TEST_TARGET_POP()
#endif

// Only explicitly declared test-body methods receive the target attribute.
// Their constructors, factories and execution wrappers are defined above.
#define Y_CPU_UNIT_TEST_SUITE_IMPL(N, BASE, FIXTURE, TARGET, ...) \
    Y_UNIT_TEST_SUITE_IMPL_F(N, BASE, FIXTURE) {} \
    Y_CPU_TEST_TARGET_PUSH(TARGET) \
    namespace NTestSuite##N __VA_ARGS__ \
    Y_CPU_TEST_TARGET_POP()

#define Y_CPU_UNIT_TEST_SUITE(N, ISA, ...) \
    Y_CPU_UNIT_TEST_SUITE_IMPL(N, NSimd::NTest::TCpuTestSuite<NX86::Have##ISA>, \
        NUnitTest::TBaseTestCase, Y_CPU_TEST_TARGET_##ISA, __VA_ARGS__)

#define Y_CPU_UNIT_TEST_IMPL(N, FORK) \
    struct TBody##N: public TCurrentTestCase { \
        void ExecuteBody(NUnitTest::TTestContext&); \
    }; \
    static const NSimd::NTest::TTestRegistration<TCurrentTest, TBody##N> \
        Registration##N(#N, __FILE__, __LINE__, FORK); \
    void TBody##N::ExecuteBody(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_CPU_UNIT_TEST(N) Y_CPU_UNIT_TEST_IMPL(N, false)
#define Y_CPU_FORKED_UNIT_TEST(N) Y_CPU_UNIT_TEST_IMPL(N, true)
