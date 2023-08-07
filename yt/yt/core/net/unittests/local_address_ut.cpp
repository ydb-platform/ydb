#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NNet {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLocalYPClusterTest, Basic)
{
    TString ypHostNameMan = "noqpmfiudzbb4hvs.man.yp-c.yandex.net";
    TString ypHostNameSas = "hellodarknessmyoldfriend.sas.yp-c.yandex.net";

    WriteLocalHostName(ypHostNameMan);
    TStringBuf localYPCluster1{ReadLocalYPCluster()};
    EXPECT_EQ(localYPCluster1, "man");
    EXPECT_EQ(GetLocalYPCluster(), "man");

    WriteLocalHostName(ypHostNameSas);
    TStringBuf localYPCluster2{ReadLocalYPCluster()};
    EXPECT_EQ(localYPCluster1, "man");
    EXPECT_EQ(localYPCluster2, "sas");
    EXPECT_EQ(GetLocalYPCluster(), "sas");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NNet
