#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>

Y_UNIT_TEST_SUITE(Www) {
    Y_UNIT_TEST(messagebus_js) {
       auto str = NResource::Find("/messagebus.js");
       UNIT_ASSERT(str.Contains("logTransform"));
       UNIT_ASSERT(str.Contains("plotHist"));
    }

    Y_UNIT_TEST(busico_png) {
       auto str = NResource::Find("/bus-ico.png");
       UNIT_ASSERT(str.Contains("PNG")); //header
    }

    Y_UNIT_TEST(not_exist) {
       UNIT_ASSERT_EXCEPTION(NResource::Find("/not_exist"), yexception);
    }
}
