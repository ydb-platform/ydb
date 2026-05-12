#include "utils.h"
#include <contrib/libs/ibdrv/include/infiniband/verbs.h>
#include <ydb/library/testlib/unittest_gtest_macro_subst.h>
#include <errno.h>

class TIbvSuite : public TSkipFixture {};

TEST_F(TIbvSuite, ListDevice) {
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

        {
            std::array<ibv_gid_entry, 10> entries;
            ssize_t rv = ibv_query_gid_table(ctx, entries.data(), entries.size(), 0);
            UNIT_ASSERT(rv > 0);
            bool roceV2Found = false;
            for (ssize_t i = 0; i < rv; i++) {
                roceV2Found |= (entries[i].gid_type == IBV_GID_TYPE_ROCE_V2);
            }
            UNIT_ASSERT(roceV2Found);
        }

        for (int port = 1; port <= devAttrs.phys_port_cnt; ++port) {
            ibv_port_attr portAttrs;
            UNIT_ASSERT_C(ibv_query_port(ctx, port, &portAttrs) == 0, strerror(errno));
            Cerr << "port " << port << " speed: " << (int)portAttrs.active_speed << Endl;

            bool roceV2Found = false;
            for (int gidIndex = 0; gidIndex < portAttrs.gid_tbl_len; gidIndex++ ) {
                struct ibv_gid_entry entry;
                Cerr << "port:  " << port << " " << gidIndex << Endl;
                int rv = ibv_query_gid_ex(ctx, port, gidIndex, &entry, 0);
                if (rv < 0) {
                    continue;
                }
                roceV2Found |= (entry.gid_type == IBV_GID_TYPE_ROCE_V2);
            }
            UNIT_ASSERT(roceV2Found);
        }

        UNIT_ASSERT(ibv_close_device(ctx) == 0);
    }

    ibv_free_device_list(deviceList);
}
