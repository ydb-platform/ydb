#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/mpl.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMetaProgrammingTest, IsPod)
{
    EXPECT_TRUE (( NMpl::TIsPod<char>::value ));
    EXPECT_TRUE (( NMpl::TIsPod<int>::value ));
    EXPECT_TRUE (( NMpl::TIsPod<short>::value ));
    EXPECT_TRUE (( NMpl::TIsPod<long>::value ));
    EXPECT_TRUE (( NMpl::TIsPod<float>::value ));
    EXPECT_TRUE (( NMpl::TIsPod<double>::value ));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
