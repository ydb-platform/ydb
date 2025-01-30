#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/global/access.h>

#include <library/cpp/yt/global/mock_modules/module3_defs/direct_access.h>
#include <library/cpp/yt/global/mock_modules/module3_public/test_tag.h>

#include <array>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t N>
bool Equal(const std::array<T, N>& left, const std::array<T, N>& right)
{
    return [&] <size_t... Idx> (std::index_sequence<Idx...>) {
        return ([&] {
            return left[Idx] == right[Idx];
        } () && ...);
    } (std::make_index_sequence<N>());
}

TEST(TGlobalVariableTest, NoOdrViolation)
{
    auto erasedVar = NGlobal::GetErasedVariable(TestTag3);
    EXPECT_TRUE(erasedVar);

    EXPECT_TRUE(Equal(erasedVar->AsConcrete<std::array<int, 2>>(), std::array<int, 2>{11, 22}));

    SetTestVariable3(std::array<int, 2>{44, 55});
    EXPECT_TRUE(Equal(erasedVar->AsConcrete<std::array<int, 2>>(), std::array<int, 2>{11, 22}));

    EXPECT_TRUE(Equal(NGlobal::GetErasedVariable(TestTag3)->AsConcrete<std::array<int, 2>>(), std::array<int, 2>{44, 55}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
