#include <library/cpp/testing/gtest/gtest.h>

#include <yt/yt/library/numeric/serialize/double_array.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDoubleArrayTest
    : public testing::Test
{
protected:
    TDoubleArrayTest() = default;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDoubleArrayTest, TestToString)
{
    TDoubleArray<5> arr = {0, 1, 2, 3, 4};
    TString result = "[0.000000 1.000000 2.000000 3.000000 4.000000]";
    TString resultSmallPrecision = "[0.00 1.00 2.00 3.00 4.00]";
    TString resultHighPrecision = "[0.0000000000 1.0000000000 2.0000000000 3.0000000000 4.0000000000]";

    // TODO(ignat)
    // EXPECT_EQ(result, ToString(arr));
    EXPECT_EQ(result, Format("%v", arr));
    EXPECT_EQ(resultSmallPrecision, Format("%.2v", arr));
    EXPECT_EQ(resultHighPrecision, Format("%.10v", arr));

    std::stringstream ss1;
    ss1 << arr;
    EXPECT_EQ(result, TString(ss1.str()));

    // TODO(ignat)
    // TStringStream ss2;
    // ss2 << arr;
    // EXPECT_EQ(result, ss2.Str());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
