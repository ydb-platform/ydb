#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/client/node_tracker_client/node_directory.h>

namespace NYT::NNodeTrackerClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TGetAddress, Test)
{
    const TAddressMap map{{"ipv4", "127.0.0.1:8081"}, {"ipv6", "::1:8081"}, {"default", "localhost:8081"}};

    EXPECT_EQ("::1:8081", GetAddressOrThrow(map, { "ipv6", "ipv4" }));
    EXPECT_EQ("127.0.0.1:8081", GetAddressOrThrow(map, { "ipv4", "ipv6" }));
    EXPECT_THROW(GetAddressOrThrow(map, { "wrong" }), TErrorException);
}

TEST(TFindAddress, Test)
{
    const TAddressMap map{{"ipv4", "127.0.0.1:8081"}, {"ipv6", "::1:8081"}, {"default", "localhost:8081"}};

    EXPECT_EQ("::1:8081", *FindAddress(map, {"ipv6", "ipv4"}));
    EXPECT_EQ("127.0.0.1:8081", *FindAddress(map, {"ipv4", "ipv6"}));
    EXPECT_FALSE(FindAddress(map, {"wrong"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNodeTrackerClient

