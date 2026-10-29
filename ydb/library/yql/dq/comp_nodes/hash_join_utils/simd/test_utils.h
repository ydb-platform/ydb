#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/system/cpu_id.h>

namespace NSimd::NTest {

// Keep the CPU check outside functions compiled for the required instruction set.
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

using TAvx2TestSuite = TCpuTestSuite<NX86::HaveAVX2>;
using TSse42TestSuite = TCpuTestSuite<NX86::HaveSSE42>;

} // namespace NSimd::NTest
