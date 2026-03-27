#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/authentication_identity.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TAuthenticationIdentityGuardTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TAuthenticationIdentityGuardTest, Simple)
{
    EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "root");
    {
        TAuthenticationIdentity userBob("bob");
        TCurrentAuthenticationIdentityGuard guard1(&userBob);
        EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "bob");
    }
    EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "root");
}

TEST_F(TAuthenticationIdentityGuardTest, Inclusion)
{
    TAuthenticationIdentity userBob("bob"), userAlice("alice"), userDave("dave");
    TCurrentAuthenticationIdentityGuard guard1(&userDave);
    EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "dave");
    {
        TCurrentAuthenticationIdentityGuard guard2(&userBob);
        EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "bob");
        {
            TCurrentAuthenticationIdentityGuard guard3(&userAlice);
            EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "alice");
        }
        EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "bob");
    }
    EXPECT_EQ(GetCurrentAuthenticationIdentity().User, "dave");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
