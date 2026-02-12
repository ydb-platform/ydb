#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NQueueClient {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using ClusterPathString = std::tuple<std::string, std::string, std::string>;

class TCrossClusterReferenceFromStringTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<ClusterPathString>
{ };

TEST_P(TCrossClusterReferenceFromStringTest, TestFromString)
{
    auto [cluster, path, str] = GetParam();
    EXPECT_EQ((TCrossClusterReference{.Cluster = cluster, .Path = path.data()}), TCrossClusterReference::FromString(str));
};

INSTANTIATE_TEST_SUITE_P(
    TCrossClusterReferenceFromStringTest,
    TCrossClusterReferenceFromStringTest,
    ::testing::Values(
        ClusterPathString{"secondary", "//keker", "secondary://keker"},
        ClusterPathString{"secondary", "/keker", "secondary:/keker"},
        ClusterPathString{"primary", "//haha:haha:", "primary://haha:haha:"},
        ClusterPathString{"primary", "//some/path", "<cluster=primary>//some/path"},
        ClusterPathString{"primary", "//some/path", "<cluster=primary;queue_consumer_id=my_consumer>//some/path"},
        ClusterPathString{"primary", "//some/path", "<cluster=primary;queue_consumer_id=my_consumer;any_attribute=is_ok>//some/path"},
        ClusterPathString{"primary", "//cluster/with/dot/is/ok", "<queue_consumer_id=my_consumer;any_attribute=is_ok>primary://cluster/with/dot/is/ok"}
));

////////////////////////////////////////////////////////////////////////////////

class TCrossClusterReferenceFromBadStringTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::string>
{ };

TEST_P(TCrossClusterReferenceFromBadStringTest, TestFromString)
{
    EXPECT_THROW(TCrossClusterReference::FromString(GetParam()), TErrorException);
};

INSTANTIATE_TEST_SUITE_P(
    TCrossClusterReferenceFromBadStringTest,
    TCrossClusterReferenceFromBadStringTest,
    ::testing::Values(
        "no_cluster",
        "primary:path_without_slash"
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueClient
