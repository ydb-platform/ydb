#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/ibdrv/include/infiniband/verbs.h>
#include <util/system/env.h>
#include <errno.h>

static bool IsRdmaTestDisabled() {
    return GetEnv("TEST_ICRDMA").empty();
}

Y_UNIT_TEST_SUITE(Ibv) {
    Y_UNIT_TEST(ListDevice) {
        if (IsRdmaTestDisabled()) {
            Cerr << "RDMA test skipped" << Endl;
            return;
        }
        int numDevices = 0;

        ibv_device** deviceList  = ibv_get_device_list(&numDevices);

        UNIT_ASSERT_C(deviceList, "unable to get device list");

        for (int i = 0; i < numDevices; i++) {
            ibv_device* dev = deviceList[i];
            Cerr << "found: " << dev->dev_name << " " << dev->name << " " << dev->dev_path << Endl;
            ibv_context* ctx = ibv_open_device(dev);
            UNIT_ASSERT_C(ctx, "unable to open device");
            ibv_device_attr devAttrs;
            UNIT_ASSERT_C(ibv_query_device(ctx, &devAttrs) == 0, strerror(errno));

            for (int port = 1; port <= devAttrs.phys_port_cnt; ++port) {
                ibv_port_attr portAttrs;
                UNIT_ASSERT_C(ibv_query_port(ctx, port, &portAttrs) == 0, strerror(errno));
                Cerr << "port " << port << " speed: " << (int)portAttrs.active_speed << Endl;
            }

            UNIT_ASSERT(ibv_close_device(ctx) == 0);
        }

        ibv_free_device_list(deviceList);
    }
}
