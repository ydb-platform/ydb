#include <yt/yt/client/api/client_cache.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NApi {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TClientAuthenticationIdentityTest, ServiceTicketParticipatesInEquality)
{
    const TClientAuthenticationIdentity first("user", "tag", "ticket-1");
    const TClientAuthenticationIdentity same("user", "tag", "ticket-1");
    const TClientAuthenticationIdentity differentTicket("user", "tag", "ticket-2");

    EXPECT_EQ(first, same);
    EXPECT_NE(first, differentTicket);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi
