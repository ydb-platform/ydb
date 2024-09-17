#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/global/access.h>

#include <library/cpp/yt/global/mock_modules/module1_defs/direct_access.h>
#include <library/cpp/yt/global/mock_modules/module1_public/test_tag.h>

#include <library/cpp/yt/global/mock_modules/module2_defs/direct_access.h>
#include <library/cpp/yt/global/mock_modules/module2_public/test_tag.h>

#include <optional>
#include <thread>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// NB: Test is identical to just_works_ut.
// We check that linking non-conflicting module has no interference.
TEST(TGlobalVariableTest, JustWorks)
{
    auto erasedVar = NGlobal::GetErasedVariable(TestTag1);
    EXPECT_TRUE(erasedVar.has_value());

    auto concreteVar = erasedVar->AsConcrete<int>();
    EXPECT_EQ(concreteVar, GetTestVariable1());

    SetTestVariable1(12344);

    // NB: We copied variable, not a reference to it!
    EXPECT_EQ(concreteVar, erasedVar->AsConcrete<int>());

    EXPECT_EQ(NGlobal::GetErasedVariable(TestTag1)->AsConcrete<int>(), 12344);
}

TEST(TGlobalVariableTest, MissingTag)
{
    static constexpr NGlobal::TVariableTag MissingTag = {};
    EXPECT_FALSE(NGlobal::GetErasedVariable(MissingTag));
}

TEST(TGlobalVariableTest, ThreadLocal)
{
    auto ensureConstructed = [] {
        // NB: tls variable is constructed only after
        // being referred to for the first time.
        auto val = GetTlsVariable();
        ++val;
        Y_UNUSED(val);
    };

    auto checkTls = [&ensureConstructed] (int val) {
        ensureConstructed();

        auto erasedVar = NGlobal::GetErasedVariable(ThreadLocalTag);
        EXPECT_TRUE(erasedVar);
        EXPECT_EQ(erasedVar->AsConcrete<int>(), 0);

        EXPECT_EQ(GetTlsVariable(), 0);
        SetTlsVariable(val);

        EXPECT_EQ(erasedVar->AsConcrete<int>(), 0);
        EXPECT_EQ(NGlobal::GetErasedVariable(ThreadLocalTag)->AsConcrete<int>(), val);
    };

    checkTls(42);

    for (int idx = 0; idx < 42; ++idx) {
        auto thread = std::thread(std::bind(checkTls, idx << 2));
        thread.join();
    }
}

TEST(TGlobalVariableTest, JustWorksAnotherModule)
{
    auto erasedVar = NGlobal::GetErasedVariable(TestTag2);
    EXPECT_TRUE(erasedVar.has_value());

    EXPECT_EQ(erasedVar->AsConcrete<TGuid>(), GetTestVariable2());

    auto val = TGuid{1, 2, 3, 4};
    SetTestVariable2(val);
    EXPECT_EQ(NGlobal::GetErasedVariable(TestTag2)->AsConcrete<TGuid>(), val);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
