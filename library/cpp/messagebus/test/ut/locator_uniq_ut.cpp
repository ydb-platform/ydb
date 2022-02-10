#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/messagebus/test_utils.h>
#include <library/cpp/messagebus/ybus.h>

class TLocatorRegisterUniqTest: public TTestBase {
    UNIT_TEST_SUITE(TLocatorRegisterUniqTest);
    UNIT_TEST(TestRegister);
    UNIT_TEST_SUITE_END();

protected:
    void TestRegister();
};

UNIT_TEST_SUITE_REGISTRATION(TLocatorRegisterUniqTest);

void TLocatorRegisterUniqTest::TestRegister() {
    ASSUME_IP_V4_ENABLED;

    NBus::TBusLocator locator;
    const char* serviceName = "TestService";
    const char* hostName = "192.168.0.42";
    int port = 31337;

    NBus::TBusKeyVec keys;
    locator.LocateKeys(serviceName, keys);
    UNIT_ASSERT(keys.size() == 0);

    locator.Register(serviceName, hostName, port);
    locator.LocateKeys(serviceName, keys);
    /// YBUS_KEYMIN YBUS_KEYMAX range
    UNIT_ASSERT(keys.size() == 1);

    TVector<NBus::TNetAddr> hosts;
    UNIT_ASSERT(locator.LocateAll(serviceName, NBus::YBUS_KEYMIN, hosts) == 1);

    locator.Register(serviceName, hostName, port);
    hosts.clear();
    UNIT_ASSERT(locator.LocateAll(serviceName, NBus::YBUS_KEYMIN, hosts) == 1);
}
