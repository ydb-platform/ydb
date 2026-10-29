#include "test_utils.h"

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
