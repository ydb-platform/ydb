#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/global/access.h>

#include <library/cpp/yt/global/mock_modules/module1_public/test_tag.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

// NB: Module containing global variable definition is not
// included in peerdirs. We expect it to be invisible from
// here.
TEST(TGlobalVariableTest, MissingModule)
{
    EXPECT_FALSE(NGlobal::GetErasedVariable(TestTag1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
