#include <library/cpp/testing/unittest/gtest.h>

#include <library/cpp/type_info/type_complexity.h>
#include <library/cpp/type_info/type_constructors.h>

using namespace NTi;

TEST(TypeComplexity, Test)
{
    EXPECT_EQ(ComputeTypeComplexity(Int64()), 1);
    EXPECT_EQ(ComputeTypeComplexity(String()), 1);
    EXPECT_EQ(ComputeTypeComplexity(Null()), 1);
    EXPECT_EQ(ComputeTypeComplexity(Decimal(4, 2)), 1);

    EXPECT_EQ(ComputeTypeComplexity(Optional(Utf8())), 2);
    EXPECT_EQ(ComputeTypeComplexity(List(Json())), 2);
    EXPECT_EQ(ComputeTypeComplexity(Tagged(String(), "jpeg")), 2);
    EXPECT_EQ(ComputeTypeComplexity(Dict(String(), Optional(Int64()))), 4);
    EXPECT_EQ(ComputeTypeComplexity(Struct({
        {"a", String()},
        {"b", List(Optional(Int64()))},
    })), 5);
    EXPECT_EQ(ComputeTypeComplexity(Tuple({{Float()}, {Float()}})), 3);

    EXPECT_EQ(ComputeTypeComplexity(Variant(Struct({
        {"a", String()},
        {"b", Int64()},
    }))), 3);
    EXPECT_EQ(ComputeTypeComplexity(Tuple({
        {String()},
        {Int64()},
    })), 3);
}