#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/ibdrv/include/infiniband/verbs.h>

Y_UNIT_TEST_SUITE(Ibv) {
    Y_UNIT_TEST(ListDevice) {
        int numDevices = 0;
        int err;

        ibv_device** deviceList  = ibv_get_device_list(&numDevices);

        UNIT_ASSERT_C(deviceList, "unable to get device list");

        for (int i = 0; i < numDevices; i++) {
            ibv_device* dev = deviceList[i];
            Cerr << "found: " << dev->dev_name << " " << dev->name << " " << dev->dev_path << Endl;
            ibv_context* ctx = ibv_open_device(dev);
            UNIT_ASSERT_C(ctx, "unable to open device");
            ibv_device_attr devAttrs;
            err = ibv_query_device(ctx, &devAttrs);
            UNIT_ASSERT(err == 0);

            for (int port = 1; port <= devAttrs.phys_port_cnt; ++port) {
                ibv_port_attr portAttrs;
                err = ibv_query_port(ctx, port, &portAttrs);
                UNIT_ASSERT(err == 0);
                Cerr << "port " << port << " speed: " << (int)portAttrs.active_speed << Endl;
            }
            ibv_close_device(ctx);
        }

        ibv_free_device_list(deviceList);
    }
}
