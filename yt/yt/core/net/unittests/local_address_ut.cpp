#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NNet {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLocalYPClusterTest, Basic)
{
    TString ypHostNameMan = "noqpmfiudzbb4hvs.man.yp-c.yandex.net";
    TString ypHostNameSas = "hellodarknessmyoldfriend.sas.yp-c.yandex.net";

    SetLocalHostName(ypHostNameMan);
    TStringBuf localYPCluster1{GetLocalYPClusterRaw()};
    EXPECT_EQ(localYPCluster1, "man");
    EXPECT_EQ(GetLocalYPCluster(), "man");

    SetLocalHostName(ypHostNameSas);
    TStringBuf localYPCluster2{GetLocalYPClusterRaw()};
    EXPECT_EQ(localYPCluster1, "man");
    EXPECT_EQ(localYPCluster2, "sas");
    EXPECT_EQ(GetLocalYPCluster(), "sas");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNet
