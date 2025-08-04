#include <library/cpp/testing/gtest/gtest.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>


    TEST(Ibv, ListDevice) {
        int numDevices;
        int err;
        ibv_device** deviceList = ibv_get_device_list(&numDevices);
        ASSERT_GT(numDevices, 0) << "no devices found";
        for (int i = 0; i < numDevices; i++) {
            ibv_device* dev = deviceList[i];
            Cerr << "found: " << dev->dev_name << " " << dev->name << " " << dev->dev_path << Endl; 
            ibv_context* ctx = ibv_open_device(dev);
            ASSERT_TRUE(ctx) << "unable to open device";
            ibv_device_attr devAttrs;
            err = ibv_query_device(ctx, &devAttrs);
            ASSERT_EQ(err, 0);

            for (int port = 1; port <= devAttrs.phys_port_cnt; ++port) {
                ibv_port_attr portAttrs;
                err = ibv_query_port(ctx, port, &portAttrs);
                ASSERT_EQ(err, 0);
                Cerr << "port " << port << " speed: " << (int)portAttrs.active_speed << Endl;
            }
            ibv_close_device(ctx);
        }
        ibv_free_device_list(deviceList);
    }
