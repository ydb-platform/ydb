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

template <class TFixture, class... TEnums>
struct TTestBody: public TFixture {
    using TArgs = std::tuple<TEnums...>;
    TArgs Args;

    template <size_t I>
    decltype(auto) Arg() const {
        return std::get<I>(Args);
    }
};

template <class TBody>
class TTestCase: public TBody {
public:
    TTestCase(const TString& name, typename TBody::TArgs args, const char* file, int line, bool forceFork)
        : NameStorage_(NUnitTest::BuildParamTestNameFromTuple(name.c_str(), args))
    {
        this->Args = std::move(args);
        this->Name_ = NameStorage_.c_str();
        this->File_ = file;
        this->Line_ = line;
        this->ForceFork_ = forceFork;
    }

    void Execute_(NUnitTest::TTestContext& context) override {
        this->ExecuteBody(context);
    }

private:
    TString NameStorage_;
};

template <class TSuite, class TBody>
struct TTestRegistration {
    TTestRegistration(const TString& name, const char* file, int line, bool forceFork = false) {
        Register(name, file, line, forceFork, typename TBody::TArgs{});
    }

private:
    template <class... TEnums>
    static void Register(const TString& name, const char* file, int line, bool forceFork, std::tuple<TEnums...>) {
        NUnitTest::ForEachEnums<TEnums...>([=](auto... values) {
            auto args = std::make_tuple(values...);
            TSuite::AddTest([=]() -> THolder<NUnitTest::TBaseTestCase> {
                return MakeHolder<TTestCase<TBody>>(name, args, file, line, forceFork);
            });
        });
    }
};

template <class TSuite, template <bool> class TBody>
struct TTwinTestRegistration {
    TTwinTestRegistration(const char* name, const char* option, const char* file, int line) {
        TTestRegistration<TSuite, TBody<true>>(TString(name) + "+" + option, file, line);
        TTestRegistration<TSuite, TBody<false>>(TString(name) + "-" + option, file, line);
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

#define Y_CPU_UNIT_TEST_IMPL(N, FORK, ...) \
    struct TBody##N: public NSimd::NTest::TTestBody<TCurrentTestCase __VA_OPT__(,) __VA_ARGS__> { \
        void ExecuteBody(NUnitTest::TTestContext&); \
    }; \
    static const NSimd::NTest::TTestRegistration<TCurrentTest, TBody##N> \
        Registration##N(#N, __FILE__, __LINE__, FORK); \
    void TBody##N::ExecuteBody(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_CPU_UNIT_TEST(N, ...) Y_CPU_UNIT_TEST_IMPL(N, false __VA_OPT__(,) __VA_ARGS__)
#define Y_CPU_FORKED_UNIT_TEST(N) Y_CPU_UNIT_TEST_IMPL(N, true)

#define Y_CPU_UNIT_TEST_TWIN(N, OPTION) \
    template <bool OPTION> \
    struct TBody##N: public NSimd::NTest::TTestBody<TCurrentTestCase> { \
        void ExecuteBody(NUnitTest::TTestContext&); \
    }; \
    static const NSimd::NTest::TTwinTestRegistration<TCurrentTest, TBody##N> \
        Registration##N(#N, #OPTION, __FILE__, __LINE__); \
    template <bool OPTION> \
    void TBody##N<OPTION>::ExecuteBody(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)
