RECURSE(
    json
    yaml
)

RECURSE_FOR_TESTS(
    ut
    tests
)

LIBRARY()

IF(NOT OS_WINDOWS)
IF(BUILD_TYPE == RELEASE OR BUILD_TYPE == RELWITHDEBINFO)
    CXXFLAGS(-Oz)
ENDIF()
ENDIF()

SRCS(
    browse_db.h
    browse_events.h
    browse.h
    browse_pq.h
    counters_hosts.h
    healthcheck_record.h
    json_handlers.cpp
    json_handlers.h
    json_handlers_browse.cpp
    json_handlers_operation.cpp
    json_handlers_query.cpp
    json_handlers_pdisk.cpp
    json_handlers_scheme.cpp
    json_handlers_storage.cpp
    json_handlers_vdisk.cpp
    json_handlers_viewer.cpp
    json_handlers_pq.cpp
    json_local_rpc.h
    json_pipe_req.cpp
    json_pipe_req.h
    json_storage_base.h
    json_vdisk_req.h
    json_wb_req.cpp
    json_wb_req.h
    log.h
    operation_cancel.h
    operation_forget.h
    operation_get.h
    operation_list.h
    pdisk_info.h
    pdisk_restart.h
    pdisk_status.h
    query_autocomplete_helper.h
    scheme_directory.h
    storage_groups.h
    vdisk_blobindexstat.h
    vdisk_evict.h
    vdisk_getblob.h
    vdisk_vdiskstat.h
    viewer_acl.h
    viewer_autocomplete.h
    viewer_browse.h
    viewer_bscontrollerinfo.h
    viewer_bsgroupinfo.h
    viewer_capabilities.h
    viewer_check_access.h
    viewer_cluster.h
    viewer_compute.h
    viewer_config.h
    viewer_content.h
    viewer_counters.h
    viewer_describe_consumer.h
    viewer_describe.h
    viewer_describe_topic.h
    viewer_feature_flags.h
    viewer_topic_data.cpp
    viewer_graph.h
    viewer_healthcheck.h
    viewer_helper.h
    viewer_hiveinfo.h
    viewer_hivestats.h
    viewer_hotkeys.h
    viewer_labeled_counters.h
    viewer_metainfo.h
    viewer_netinfo.h
    viewer_nodeinfo.h
    viewer_nodelist.h
    viewer_nodes.h
    viewer_pdiskinfo.h
    viewer_pqconsumerinfo.h
    viewer_query.h
    viewer_render.h
    viewer_request.cpp
    viewer_request.h
    viewer_storage.h
    viewer_storage_usage.h
    viewer_sysinfo.h
    viewer_tabletcounters.h
    viewer_tabletinfo.h
    viewer_tenantinfo.h
    viewer_tenants.h
    viewer_topicinfo.h
    viewer_vdiskinfo.h
    viewer_whoami.h
    viewer.h
    viewer.cpp
    wb_aggregate.cpp
    wb_aggregate.h
    wb_filter.cpp
    wb_filter.h
    wb_group.h
    wb_merge.cpp
    wb_merge.h
    wb_req.h
)

IF (NOT EXPORT_CMAKE)
    # GENERATED MONITORING RESOURCES START
    RESOURCE(
        monitoring/CHANGELOG.md monitoring/CHANGELOG.md
        monitoring/asset-manifest.json monitoring/asset-manifest.json
        monitoring/css.worker.js monitoring/css.worker.js
        monitoring/css.worker.js.LICENSE.txt monitoring/css.worker.js.LICENSE.txt
        monitoring/editor.worker.js monitoring/editor.worker.js
        monitoring/html.worker.js monitoring/html.worker.js
        monitoring/index.html monitoring/index.html
        monitoring/json.worker.js monitoring/json.worker.js
        monitoring/json.worker.js.LICENSE.txt monitoring/json.worker.js.LICENSE.txt
        monitoring/static/css/18676.4dd5da63.chunk.css monitoring/static/css/18676.4dd5da63.chunk.css
        monitoring/static/css/26158.cf5d9065.chunk.css monitoring/static/css/26158.cf5d9065.chunk.css
        monitoring/static/css/28520.66487eef.chunk.css monitoring/static/css/28520.66487eef.chunk.css
        monitoring/static/css/31538.036250d1.chunk.css monitoring/static/css/31538.036250d1.chunk.css
        monitoring/static/css/37020.ff84ef2e.chunk.css monitoring/static/css/37020.ff84ef2e.chunk.css
        monitoring/static/css/38527.aaaa5492.chunk.css monitoring/static/css/38527.aaaa5492.chunk.css
        monitoring/static/css/39422.3578208f.chunk.css monitoring/static/css/39422.3578208f.chunk.css
        monitoring/static/css/48593.7fdf5ffc.chunk.css monitoring/static/css/48593.7fdf5ffc.chunk.css
        monitoring/static/css/51819.b1faff6b.chunk.css monitoring/static/css/51819.b1faff6b.chunk.css
        monitoring/static/css/62377.1d68f96c.chunk.css monitoring/static/css/62377.1d68f96c.chunk.css
        monitoring/static/css/82960.6657c65a.chunk.css monitoring/static/css/82960.6657c65a.chunk.css
        monitoring/static/css/85169.20cf78df.chunk.css monitoring/static/css/85169.20cf78df.chunk.css
        monitoring/static/css/86299.7cffb936.chunk.css monitoring/static/css/86299.7cffb936.chunk.css
        monitoring/static/css/94695.b2628977.chunk.css monitoring/static/css/94695.b2628977.chunk.css
        monitoring/static/css/main.9e1c1712.css monitoring/static/css/main.9e1c1712.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/10064.5442bcf6.chunk.js monitoring/static/js/10064.5442bcf6.chunk.js
        monitoring/static/js/10132.c1a19fa8.chunk.js monitoring/static/js/10132.c1a19fa8.chunk.js
        monitoring/static/js/10242.86faa17f.chunk.js monitoring/static/js/10242.86faa17f.chunk.js
        monitoring/static/js/10246.ee23b775.chunk.js monitoring/static/js/10246.ee23b775.chunk.js
        monitoring/static/js/10246.ee23b775.chunk.js.LICENSE.txt monitoring/static/js/10246.ee23b775.chunk.js.LICENSE.txt
        monitoring/static/js/10310.a0a2e8d7.chunk.js monitoring/static/js/10310.a0a2e8d7.chunk.js
        monitoring/static/js/10525.b02faf58.chunk.js monitoring/static/js/10525.b02faf58.chunk.js
        monitoring/static/js/1073.da2a8c8a.chunk.js monitoring/static/js/1073.da2a8c8a.chunk.js
        monitoring/static/js/10815.03541c68.chunk.js monitoring/static/js/10815.03541c68.chunk.js
        monitoring/static/js/10902.cd23357e.chunk.js monitoring/static/js/10902.cd23357e.chunk.js
        monitoring/static/js/10902.cd23357e.chunk.js.LICENSE.txt monitoring/static/js/10902.cd23357e.chunk.js.LICENSE.txt
        monitoring/static/js/11192.56c4d6e0.chunk.js monitoring/static/js/11192.56c4d6e0.chunk.js
        monitoring/static/js/11278.aab5c12c.chunk.js monitoring/static/js/11278.aab5c12c.chunk.js
        monitoring/static/js/11278.aab5c12c.chunk.js.LICENSE.txt monitoring/static/js/11278.aab5c12c.chunk.js.LICENSE.txt
        monitoring/static/js/11338.6bb2b16a.chunk.js monitoring/static/js/11338.6bb2b16a.chunk.js
        monitoring/static/js/1146.65a37bc6.chunk.js monitoring/static/js/1146.65a37bc6.chunk.js
        monitoring/static/js/1187.c5435886.chunk.js monitoring/static/js/1187.c5435886.chunk.js
        monitoring/static/js/1237.328d0241.chunk.js monitoring/static/js/1237.328d0241.chunk.js
        monitoring/static/js/12776.d400f028.chunk.js monitoring/static/js/12776.d400f028.chunk.js
        monitoring/static/js/13470.82cfe328.chunk.js monitoring/static/js/13470.82cfe328.chunk.js
        monitoring/static/js/13638.e739b34f.chunk.js monitoring/static/js/13638.e739b34f.chunk.js
        monitoring/static/js/13638.e739b34f.chunk.js.LICENSE.txt monitoring/static/js/13638.e739b34f.chunk.js.LICENSE.txt
        monitoring/static/js/1431.2bb62d12.chunk.js monitoring/static/js/1431.2bb62d12.chunk.js
        monitoring/static/js/14382.a8e3e5fd.chunk.js monitoring/static/js/14382.a8e3e5fd.chunk.js
        monitoring/static/js/14542.fea67816.chunk.js monitoring/static/js/14542.fea67816.chunk.js
        monitoring/static/js/14542.fea67816.chunk.js.LICENSE.txt monitoring/static/js/14542.fea67816.chunk.js.LICENSE.txt
        monitoring/static/js/15418.978d5fff.chunk.js monitoring/static/js/15418.978d5fff.chunk.js
        monitoring/static/js/15521.a8f9fb42.chunk.js monitoring/static/js/15521.a8f9fb42.chunk.js
        monitoring/static/js/15542.e6a4dbf6.chunk.js monitoring/static/js/15542.e6a4dbf6.chunk.js
        monitoring/static/js/15542.e6a4dbf6.chunk.js.LICENSE.txt monitoring/static/js/15542.e6a4dbf6.chunk.js.LICENSE.txt
        monitoring/static/js/15931.ad3d689f.chunk.js monitoring/static/js/15931.ad3d689f.chunk.js
        monitoring/static/js/16038.8c61a9b6.chunk.js monitoring/static/js/16038.8c61a9b6.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js monitoring/static/js/1606.0041ed7d.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt
        monitoring/static/js/16097.4303c083.chunk.js monitoring/static/js/16097.4303c083.chunk.js
        monitoring/static/js/16210.cfacbd9b.chunk.js monitoring/static/js/16210.cfacbd9b.chunk.js
        monitoring/static/js/16210.cfacbd9b.chunk.js.LICENSE.txt monitoring/static/js/16210.cfacbd9b.chunk.js.LICENSE.txt
        monitoring/static/js/16242.ba0392be.chunk.js monitoring/static/js/16242.ba0392be.chunk.js
        monitoring/static/js/16664.195e9acf.chunk.js monitoring/static/js/16664.195e9acf.chunk.js
        monitoring/static/js/16758.3630f667.chunk.js monitoring/static/js/16758.3630f667.chunk.js
        monitoring/static/js/16795.c5c2f8ec.chunk.js monitoring/static/js/16795.c5c2f8ec.chunk.js
        monitoring/static/js/17240.74653f76.chunk.js monitoring/static/js/17240.74653f76.chunk.js
        monitoring/static/js/173.7f4af5fa.chunk.js monitoring/static/js/173.7f4af5fa.chunk.js
        monitoring/static/js/17500.d2b3273a.chunk.js monitoring/static/js/17500.d2b3273a.chunk.js
        monitoring/static/js/17830.763df207.chunk.js monitoring/static/js/17830.763df207.chunk.js
        monitoring/static/js/17880.ed6168a4.chunk.js monitoring/static/js/17880.ed6168a4.chunk.js
        monitoring/static/js/17981.5fd12b3d.chunk.js monitoring/static/js/17981.5fd12b3d.chunk.js
        monitoring/static/js/18676.3f21f5ed.chunk.js monitoring/static/js/18676.3f21f5ed.chunk.js
        monitoring/static/js/18676.3f21f5ed.chunk.js.LICENSE.txt monitoring/static/js/18676.3f21f5ed.chunk.js.LICENSE.txt
        monitoring/static/js/19026.f05aa9b2.chunk.js monitoring/static/js/19026.f05aa9b2.chunk.js
        monitoring/static/js/19233.b4069ac2.chunk.js monitoring/static/js/19233.b4069ac2.chunk.js
        monitoring/static/js/19507.0512979b.chunk.js monitoring/static/js/19507.0512979b.chunk.js
        monitoring/static/js/19702.cd1b5e06.chunk.js monitoring/static/js/19702.cd1b5e06.chunk.js
        monitoring/static/js/19728.daf9b9f8.chunk.js monitoring/static/js/19728.daf9b9f8.chunk.js
        monitoring/static/js/19728.daf9b9f8.chunk.js.LICENSE.txt monitoring/static/js/19728.daf9b9f8.chunk.js.LICENSE.txt
        monitoring/static/js/19791.feabd3fc.chunk.js monitoring/static/js/19791.feabd3fc.chunk.js
        monitoring/static/js/20053.925f8331.chunk.js monitoring/static/js/20053.925f8331.chunk.js
        monitoring/static/js/20535.b2fdb3bf.chunk.js monitoring/static/js/20535.b2fdb3bf.chunk.js
        monitoring/static/js/20600.6e1ccb0d.chunk.js monitoring/static/js/20600.6e1ccb0d.chunk.js
        monitoring/static/js/20654.f715fac2.chunk.js monitoring/static/js/20654.f715fac2.chunk.js
        monitoring/static/js/20654.f715fac2.chunk.js.LICENSE.txt monitoring/static/js/20654.f715fac2.chunk.js.LICENSE.txt
        monitoring/static/js/21053.3d2e8f28.chunk.js monitoring/static/js/21053.3d2e8f28.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js monitoring/static/js/2162.cca3e026.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt
        monitoring/static/js/21657.6821035c.chunk.js monitoring/static/js/21657.6821035c.chunk.js
        monitoring/static/js/21742.5a360b56.chunk.js monitoring/static/js/21742.5a360b56.chunk.js
        monitoring/static/js/21956.3e818e6c.chunk.js monitoring/static/js/21956.3e818e6c.chunk.js
        monitoring/static/js/21989.76579159.chunk.js monitoring/static/js/21989.76579159.chunk.js
        monitoring/static/js/21996.6a6996bf.chunk.js monitoring/static/js/21996.6a6996bf.chunk.js
        monitoring/static/js/22372.9c5be99f.chunk.js monitoring/static/js/22372.9c5be99f.chunk.js
        monitoring/static/js/22372.9c5be99f.chunk.js.LICENSE.txt monitoring/static/js/22372.9c5be99f.chunk.js.LICENSE.txt
        monitoring/static/js/2251.651c81f8.chunk.js monitoring/static/js/2251.651c81f8.chunk.js
        monitoring/static/js/22609.bdebad49.chunk.js monitoring/static/js/22609.bdebad49.chunk.js
        monitoring/static/js/22626.2495b693.chunk.js monitoring/static/js/22626.2495b693.chunk.js
        monitoring/static/js/23039.f34c5f78.chunk.js monitoring/static/js/23039.f34c5f78.chunk.js
        monitoring/static/js/23158.a522a83d.chunk.js monitoring/static/js/23158.a522a83d.chunk.js
        monitoring/static/js/23158.a522a83d.chunk.js.LICENSE.txt monitoring/static/js/23158.a522a83d.chunk.js.LICENSE.txt
        monitoring/static/js/23321.4a32d0c2.chunk.js monitoring/static/js/23321.4a32d0c2.chunk.js
        monitoring/static/js/23702.887336e8.chunk.js monitoring/static/js/23702.887336e8.chunk.js
        monitoring/static/js/23702.887336e8.chunk.js.LICENSE.txt monitoring/static/js/23702.887336e8.chunk.js.LICENSE.txt
        monitoring/static/js/23882.3b39c413.chunk.js monitoring/static/js/23882.3b39c413.chunk.js
        monitoring/static/js/24349.ff3855f6.chunk.js monitoring/static/js/24349.ff3855f6.chunk.js
        monitoring/static/js/24458.50eb1325.chunk.js monitoring/static/js/24458.50eb1325.chunk.js
        monitoring/static/js/24534.3cac028e.chunk.js monitoring/static/js/24534.3cac028e.chunk.js
        monitoring/static/js/24534.3cac028e.chunk.js.LICENSE.txt monitoring/static/js/24534.3cac028e.chunk.js.LICENSE.txt
        monitoring/static/js/24615.12b53f26.chunk.js monitoring/static/js/24615.12b53f26.chunk.js
        monitoring/static/js/24662.5e8417ae.chunk.js monitoring/static/js/24662.5e8417ae.chunk.js
        monitoring/static/js/24662.5e8417ae.chunk.js.LICENSE.txt monitoring/static/js/24662.5e8417ae.chunk.js.LICENSE.txt
        monitoring/static/js/25007.2ae44a13.chunk.js monitoring/static/js/25007.2ae44a13.chunk.js
        monitoring/static/js/25101.06af1360.chunk.js monitoring/static/js/25101.06af1360.chunk.js
        monitoring/static/js/25453.c8bfcbe1.chunk.js monitoring/static/js/25453.c8bfcbe1.chunk.js
        monitoring/static/js/25604.54cb85d0.chunk.js monitoring/static/js/25604.54cb85d0.chunk.js
        monitoring/static/js/25636.358f92b4.chunk.js monitoring/static/js/25636.358f92b4.chunk.js
        monitoring/static/js/25636.358f92b4.chunk.js.LICENSE.txt monitoring/static/js/25636.358f92b4.chunk.js.LICENSE.txt
        monitoring/static/js/26158.d6c53b48.chunk.js monitoring/static/js/26158.d6c53b48.chunk.js
        monitoring/static/js/26214.42be0c73.chunk.js monitoring/static/js/26214.42be0c73.chunk.js
        monitoring/static/js/26214.42be0c73.chunk.js.LICENSE.txt monitoring/static/js/26214.42be0c73.chunk.js.LICENSE.txt
        monitoring/static/js/26324.10b41523.chunk.js monitoring/static/js/26324.10b41523.chunk.js
        monitoring/static/js/26327.62bdac9a.chunk.js monitoring/static/js/26327.62bdac9a.chunk.js
        monitoring/static/js/26358.23555994.chunk.js monitoring/static/js/26358.23555994.chunk.js
        monitoring/static/js/26411.07f03301.chunk.js monitoring/static/js/26411.07f03301.chunk.js
        monitoring/static/js/2656.6cdcd805.chunk.js monitoring/static/js/2656.6cdcd805.chunk.js
        monitoring/static/js/26625.d5154eea.chunk.js monitoring/static/js/26625.d5154eea.chunk.js
        monitoring/static/js/26798.ded35add.chunk.js monitoring/static/js/26798.ded35add.chunk.js
        monitoring/static/js/26833.d6c6c578.chunk.js monitoring/static/js/26833.d6c6c578.chunk.js
        monitoring/static/js/27148.5289994c.chunk.js monitoring/static/js/27148.5289994c.chunk.js
        monitoring/static/js/27148.5289994c.chunk.js.LICENSE.txt monitoring/static/js/27148.5289994c.chunk.js.LICENSE.txt
        monitoring/static/js/2726.abc3a0c2.chunk.js monitoring/static/js/2726.abc3a0c2.chunk.js
        monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt
        monitoring/static/js/27950.acee5eec.chunk.js monitoring/static/js/27950.acee5eec.chunk.js
        monitoring/static/js/28125.0776827a.chunk.js monitoring/static/js/28125.0776827a.chunk.js
        monitoring/static/js/28256.20804e43.chunk.js monitoring/static/js/28256.20804e43.chunk.js
        monitoring/static/js/28520.1f1834e8.chunk.js monitoring/static/js/28520.1f1834e8.chunk.js
        monitoring/static/js/28868.3c0ecf71.chunk.js monitoring/static/js/28868.3c0ecf71.chunk.js
        monitoring/static/js/29043.f0a51584.chunk.js monitoring/static/js/29043.f0a51584.chunk.js
        monitoring/static/js/29193.43e37031.chunk.js monitoring/static/js/29193.43e37031.chunk.js
        monitoring/static/js/29394.e027e9c7.chunk.js monitoring/static/js/29394.e027e9c7.chunk.js
        monitoring/static/js/29394.e027e9c7.chunk.js.LICENSE.txt monitoring/static/js/29394.e027e9c7.chunk.js.LICENSE.txt
        monitoring/static/js/29461.74898902.chunk.js monitoring/static/js/29461.74898902.chunk.js
        monitoring/static/js/29769.21fd5384.chunk.js monitoring/static/js/29769.21fd5384.chunk.js
        monitoring/static/js/29866.16645b0d.chunk.js monitoring/static/js/29866.16645b0d.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js monitoring/static/js/2994.6c6016a8.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt
        monitoring/static/js/30226.e28d8c14.chunk.js monitoring/static/js/30226.e28d8c14.chunk.js
        monitoring/static/js/30229.9a64e8c6.chunk.js monitoring/static/js/30229.9a64e8c6.chunk.js
        monitoring/static/js/30249.b5966caf.chunk.js monitoring/static/js/30249.b5966caf.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js monitoring/static/js/3048.692b5966.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt
        monitoring/static/js/30499.28982bd3.chunk.js monitoring/static/js/30499.28982bd3.chunk.js
        monitoring/static/js/30523.726a6c98.chunk.js monitoring/static/js/30523.726a6c98.chunk.js
        monitoring/static/js/30573.df897e71.chunk.js monitoring/static/js/30573.df897e71.chunk.js
        monitoring/static/js/3061.c2a39676.chunk.js monitoring/static/js/3061.c2a39676.chunk.js
        monitoring/static/js/30811.b4fed9a8.chunk.js monitoring/static/js/30811.b4fed9a8.chunk.js
        monitoring/static/js/30850.aef4d4ce.chunk.js monitoring/static/js/30850.aef4d4ce.chunk.js
        monitoring/static/js/31177.d892dd40.chunk.js monitoring/static/js/31177.d892dd40.chunk.js
        monitoring/static/js/3121.74e9e7dc.chunk.js monitoring/static/js/3121.74e9e7dc.chunk.js
        monitoring/static/js/31423.b7296b03.chunk.js monitoring/static/js/31423.b7296b03.chunk.js
        monitoring/static/js/3149.0c1f7eee.chunk.js monitoring/static/js/3149.0c1f7eee.chunk.js
        monitoring/static/js/31538.69a42535.chunk.js monitoring/static/js/31538.69a42535.chunk.js
        monitoring/static/js/31584.6ddd5f13.chunk.js monitoring/static/js/31584.6ddd5f13.chunk.js
        monitoring/static/js/31975.54132ada.chunk.js monitoring/static/js/31975.54132ada.chunk.js
        monitoring/static/js/32166.8e70395f.chunk.js monitoring/static/js/32166.8e70395f.chunk.js
        monitoring/static/js/32286.23bbbad8.chunk.js monitoring/static/js/32286.23bbbad8.chunk.js
        monitoring/static/js/32670.1e209a72.chunk.js monitoring/static/js/32670.1e209a72.chunk.js
        monitoring/static/js/32742.d4094814.chunk.js monitoring/static/js/32742.d4094814.chunk.js
        monitoring/static/js/32742.d4094814.chunk.js.LICENSE.txt monitoring/static/js/32742.d4094814.chunk.js.LICENSE.txt
        monitoring/static/js/32854.6c426003.chunk.js monitoring/static/js/32854.6c426003.chunk.js
        monitoring/static/js/32854.6c426003.chunk.js.LICENSE.txt monitoring/static/js/32854.6c426003.chunk.js.LICENSE.txt
        monitoring/static/js/33338.c39231db.chunk.js monitoring/static/js/33338.c39231db.chunk.js
        monitoring/static/js/33338.c39231db.chunk.js.LICENSE.txt monitoring/static/js/33338.c39231db.chunk.js.LICENSE.txt
        monitoring/static/js/33436.e8e4435f.chunk.js monitoring/static/js/33436.e8e4435f.chunk.js
        monitoring/static/js/33521.11130f2d.chunk.js monitoring/static/js/33521.11130f2d.chunk.js
        monitoring/static/js/33822.3b7da7cd.chunk.js monitoring/static/js/33822.3b7da7cd.chunk.js
        monitoring/static/js/33822.3b7da7cd.chunk.js.LICENSE.txt monitoring/static/js/33822.3b7da7cd.chunk.js.LICENSE.txt
        monitoring/static/js/3410.6391755f.chunk.js monitoring/static/js/3410.6391755f.chunk.js
        monitoring/static/js/34119.377036f7.chunk.js monitoring/static/js/34119.377036f7.chunk.js
        monitoring/static/js/34156.b749f595.chunk.js monitoring/static/js/34156.b749f595.chunk.js
        monitoring/static/js/34169.9a22281f.chunk.js monitoring/static/js/34169.9a22281f.chunk.js
        monitoring/static/js/34542.6199c708.chunk.js monitoring/static/js/34542.6199c708.chunk.js
        monitoring/static/js/34693.09c32626.chunk.js monitoring/static/js/34693.09c32626.chunk.js
        monitoring/static/js/34847.ec23543c.chunk.js monitoring/static/js/34847.ec23543c.chunk.js
        monitoring/static/js/35161.d7f85805.chunk.js monitoring/static/js/35161.d7f85805.chunk.js
        monitoring/static/js/3534.d7d57e03.chunk.js monitoring/static/js/3534.d7d57e03.chunk.js
        monitoring/static/js/35382.6471a2ea.chunk.js monitoring/static/js/35382.6471a2ea.chunk.js
        monitoring/static/js/35382.6471a2ea.chunk.js.LICENSE.txt monitoring/static/js/35382.6471a2ea.chunk.js.LICENSE.txt
        monitoring/static/js/35569.8ceadf10.chunk.js monitoring/static/js/35569.8ceadf10.chunk.js
        monitoring/static/js/35596.d9a26c66.chunk.js monitoring/static/js/35596.d9a26c66.chunk.js
        monitoring/static/js/35803.0104654b.chunk.js monitoring/static/js/35803.0104654b.chunk.js
        monitoring/static/js/35888.e5062b23.chunk.js monitoring/static/js/35888.e5062b23.chunk.js
        monitoring/static/js/35888.e5062b23.chunk.js.LICENSE.txt monitoring/static/js/35888.e5062b23.chunk.js.LICENSE.txt
        monitoring/static/js/35958.73cbdbca.chunk.js monitoring/static/js/35958.73cbdbca.chunk.js
        monitoring/static/js/36374.16f4dcdb.chunk.js monitoring/static/js/36374.16f4dcdb.chunk.js
        monitoring/static/js/36374.16f4dcdb.chunk.js.LICENSE.txt monitoring/static/js/36374.16f4dcdb.chunk.js.LICENSE.txt
        monitoring/static/js/3648.3a72999a.chunk.js monitoring/static/js/3648.3a72999a.chunk.js
        monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt
        monitoring/static/js/36754.f9faf9f5.chunk.js monitoring/static/js/36754.f9faf9f5.chunk.js
        monitoring/static/js/36786.ca3962c1.chunk.js monitoring/static/js/36786.ca3962c1.chunk.js
        monitoring/static/js/37020.2839cab0.chunk.js monitoring/static/js/37020.2839cab0.chunk.js
        monitoring/static/js/3738.bec1d482.chunk.js monitoring/static/js/3738.bec1d482.chunk.js
        monitoring/static/js/37380.7c50d99e.chunk.js monitoring/static/js/37380.7c50d99e.chunk.js
        monitoring/static/js/37385.c32c6c83.chunk.js monitoring/static/js/37385.c32c6c83.chunk.js
        monitoring/static/js/37579.aa311c74.chunk.js monitoring/static/js/37579.aa311c74.chunk.js
        monitoring/static/js/37605.270ebf37.chunk.js monitoring/static/js/37605.270ebf37.chunk.js
        monitoring/static/js/37677.c7cb500e.chunk.js monitoring/static/js/37677.c7cb500e.chunk.js
        monitoring/static/js/37747.ab1590d9.chunk.js monitoring/static/js/37747.ab1590d9.chunk.js
        monitoring/static/js/37963.55ad78e4.chunk.js monitoring/static/js/37963.55ad78e4.chunk.js
        monitoring/static/js/38103.a27aa378.chunk.js monitoring/static/js/38103.a27aa378.chunk.js
        monitoring/static/js/38527.d916241a.chunk.js monitoring/static/js/38527.d916241a.chunk.js
        monitoring/static/js/38718.bf415be7.chunk.js monitoring/static/js/38718.bf415be7.chunk.js
        monitoring/static/js/3872.a25d87b5.chunk.js monitoring/static/js/3872.a25d87b5.chunk.js
        monitoring/static/js/3902.973b73c6.chunk.js monitoring/static/js/3902.973b73c6.chunk.js
        monitoring/static/js/3952.ce1b4fad.chunk.js monitoring/static/js/3952.ce1b4fad.chunk.js
        monitoring/static/js/39705.257f0583.chunk.js monitoring/static/js/39705.257f0583.chunk.js
        monitoring/static/js/3980.f3083535.chunk.js monitoring/static/js/3980.f3083535.chunk.js
        monitoring/static/js/40047.1e272b92.chunk.js monitoring/static/js/40047.1e272b92.chunk.js
        monitoring/static/js/40060.5f9cbddd.chunk.js monitoring/static/js/40060.5f9cbddd.chunk.js
        monitoring/static/js/40132.8f54cbd2.chunk.js monitoring/static/js/40132.8f54cbd2.chunk.js
        monitoring/static/js/40132.8f54cbd2.chunk.js.LICENSE.txt monitoring/static/js/40132.8f54cbd2.chunk.js.LICENSE.txt
        monitoring/static/js/4018.f6c8e2ef.chunk.js monitoring/static/js/4018.f6c8e2ef.chunk.js
        monitoring/static/js/40388.be25f07a.chunk.js monitoring/static/js/40388.be25f07a.chunk.js
        monitoring/static/js/40710.a00731c6.chunk.js monitoring/static/js/40710.a00731c6.chunk.js
        monitoring/static/js/40730.5e1bc3d1.chunk.js monitoring/static/js/40730.5e1bc3d1.chunk.js
        monitoring/static/js/41696.f9f2ec5d.chunk.js monitoring/static/js/41696.f9f2ec5d.chunk.js
        monitoring/static/js/4180.1de6c8ba.chunk.js monitoring/static/js/4180.1de6c8ba.chunk.js
        monitoring/static/js/42111.2a4b8434.chunk.js monitoring/static/js/42111.2a4b8434.chunk.js
        monitoring/static/js/42182.a71d5155.chunk.js monitoring/static/js/42182.a71d5155.chunk.js
        monitoring/static/js/42182.a71d5155.chunk.js.LICENSE.txt monitoring/static/js/42182.a71d5155.chunk.js.LICENSE.txt
        monitoring/static/js/42384.403ac671.chunk.js monitoring/static/js/42384.403ac671.chunk.js
        monitoring/static/js/42396.9af219b0.chunk.js monitoring/static/js/42396.9af219b0.chunk.js
        monitoring/static/js/4243.697ce022.chunk.js monitoring/static/js/4243.697ce022.chunk.js
        monitoring/static/js/42612.7c3ffc1d.chunk.js monitoring/static/js/42612.7c3ffc1d.chunk.js
        monitoring/static/js/42615.93e0f1f6.chunk.js monitoring/static/js/42615.93e0f1f6.chunk.js
        monitoring/static/js/42791.26100ebe.chunk.js monitoring/static/js/42791.26100ebe.chunk.js
        monitoring/static/js/42912.7ab36c78.chunk.js monitoring/static/js/42912.7ab36c78.chunk.js
        monitoring/static/js/43028.3817b0d7.chunk.js monitoring/static/js/43028.3817b0d7.chunk.js
        monitoring/static/js/43702.745d5072.chunk.js monitoring/static/js/43702.745d5072.chunk.js
        monitoring/static/js/43702.745d5072.chunk.js.LICENSE.txt monitoring/static/js/43702.745d5072.chunk.js.LICENSE.txt
        monitoring/static/js/43761.7a876492.chunk.js monitoring/static/js/43761.7a876492.chunk.js
        monitoring/static/js/43979.9ed5260f.chunk.js monitoring/static/js/43979.9ed5260f.chunk.js
        monitoring/static/js/44096.4faf307e.chunk.js monitoring/static/js/44096.4faf307e.chunk.js
        monitoring/static/js/44391.7bf4eade.chunk.js monitoring/static/js/44391.7bf4eade.chunk.js
        monitoring/static/js/4441.2b4963cf.chunk.js monitoring/static/js/4441.2b4963cf.chunk.js
        monitoring/static/js/44601.53b5fa99.chunk.js monitoring/static/js/44601.53b5fa99.chunk.js
        monitoring/static/js/44846.352e01be.chunk.js monitoring/static/js/44846.352e01be.chunk.js
        monitoring/static/js/44846.352e01be.chunk.js.LICENSE.txt monitoring/static/js/44846.352e01be.chunk.js.LICENSE.txt
        monitoring/static/js/44866.fde9a535.chunk.js monitoring/static/js/44866.fde9a535.chunk.js
        monitoring/static/js/45517.dd0696d8.chunk.js monitoring/static/js/45517.dd0696d8.chunk.js
        monitoring/static/js/4554.8b82bb25.chunk.js monitoring/static/js/4554.8b82bb25.chunk.js
        monitoring/static/js/45685.47ca075a.chunk.js monitoring/static/js/45685.47ca075a.chunk.js
        monitoring/static/js/45759.cb764ce8.chunk.js monitoring/static/js/45759.cb764ce8.chunk.js
        monitoring/static/js/46012.36fc4080.chunk.js monitoring/static/js/46012.36fc4080.chunk.js
        monitoring/static/js/46012.36fc4080.chunk.js.LICENSE.txt monitoring/static/js/46012.36fc4080.chunk.js.LICENSE.txt
        monitoring/static/js/46047.240cef79.chunk.js monitoring/static/js/46047.240cef79.chunk.js
        monitoring/static/js/46134.708fa2c1.chunk.js monitoring/static/js/46134.708fa2c1.chunk.js
        monitoring/static/js/4617.be8c65cd.chunk.js monitoring/static/js/4617.be8c65cd.chunk.js
        monitoring/static/js/46306.d3a5a75d.chunk.js monitoring/static/js/46306.d3a5a75d.chunk.js
        monitoring/static/js/46541.3c0665eb.chunk.js monitoring/static/js/46541.3c0665eb.chunk.js
        monitoring/static/js/4664.972299e2.chunk.js monitoring/static/js/4664.972299e2.chunk.js
        monitoring/static/js/47108.d6adff77.chunk.js monitoring/static/js/47108.d6adff77.chunk.js
        monitoring/static/js/47153.2c051af0.chunk.js monitoring/static/js/47153.2c051af0.chunk.js
        monitoring/static/js/4730.78e66e9a.chunk.js monitoring/static/js/4730.78e66e9a.chunk.js
        monitoring/static/js/47472.10032073.chunk.js monitoring/static/js/47472.10032073.chunk.js
        monitoring/static/js/47614.68df3ac9.chunk.js monitoring/static/js/47614.68df3ac9.chunk.js
        monitoring/static/js/47660.d58e412b.chunk.js monitoring/static/js/47660.d58e412b.chunk.js
        monitoring/static/js/47692.c0ce8e67.chunk.js monitoring/static/js/47692.c0ce8e67.chunk.js
        monitoring/static/js/47692.c0ce8e67.chunk.js.LICENSE.txt monitoring/static/js/47692.c0ce8e67.chunk.js.LICENSE.txt
        monitoring/static/js/47878.706bd425.chunk.js monitoring/static/js/47878.706bd425.chunk.js
        monitoring/static/js/48008.f90295f8.chunk.js monitoring/static/js/48008.f90295f8.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js monitoring/static/js/4818.6beda30c.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt
        monitoring/static/js/48633.41f9d3a3.chunk.js monitoring/static/js/48633.41f9d3a3.chunk.js
        monitoring/static/js/4887.f016c3bb.chunk.js monitoring/static/js/4887.f016c3bb.chunk.js
        monitoring/static/js/48914.0bb2f1c2.chunk.js monitoring/static/js/48914.0bb2f1c2.chunk.js
        monitoring/static/js/49067.2e09b756.chunk.js monitoring/static/js/49067.2e09b756.chunk.js
        monitoring/static/js/49523.654b328e.chunk.js monitoring/static/js/49523.654b328e.chunk.js
        monitoring/static/js/49582.c5a749cc.chunk.js monitoring/static/js/49582.c5a749cc.chunk.js
        monitoring/static/js/49582.c5a749cc.chunk.js.LICENSE.txt monitoring/static/js/49582.c5a749cc.chunk.js.LICENSE.txt
        monitoring/static/js/49725.3529a00c.chunk.js monitoring/static/js/49725.3529a00c.chunk.js
        monitoring/static/js/49778.b9d397f4.chunk.js monitoring/static/js/49778.b9d397f4.chunk.js
        monitoring/static/js/49778.b9d397f4.chunk.js.LICENSE.txt monitoring/static/js/49778.b9d397f4.chunk.js.LICENSE.txt
        monitoring/static/js/49788.12d03dd2.chunk.js monitoring/static/js/49788.12d03dd2.chunk.js
        monitoring/static/js/50045.c8e44e5c.chunk.js monitoring/static/js/50045.c8e44e5c.chunk.js
        monitoring/static/js/50245.1623217b.chunk.js monitoring/static/js/50245.1623217b.chunk.js
        monitoring/static/js/50875.c6afaf0d.chunk.js monitoring/static/js/50875.c6afaf0d.chunk.js
        monitoring/static/js/51094.f421b808.chunk.js monitoring/static/js/51094.f421b808.chunk.js
        monitoring/static/js/51094.f421b808.chunk.js.LICENSE.txt monitoring/static/js/51094.f421b808.chunk.js.LICENSE.txt
        monitoring/static/js/51159.314cef1d.chunk.js monitoring/static/js/51159.314cef1d.chunk.js
        monitoring/static/js/51255.beb93f73.chunk.js monitoring/static/js/51255.beb93f73.chunk.js
        monitoring/static/js/51400.767e472a.chunk.js monitoring/static/js/51400.767e472a.chunk.js
        monitoring/static/js/51414.1b5a0681.chunk.js monitoring/static/js/51414.1b5a0681.chunk.js
        monitoring/static/js/51414.1b5a0681.chunk.js.LICENSE.txt monitoring/static/js/51414.1b5a0681.chunk.js.LICENSE.txt
        monitoring/static/js/51496.423aebfa.chunk.js monitoring/static/js/51496.423aebfa.chunk.js
        monitoring/static/js/51627.9762f671.chunk.js monitoring/static/js/51627.9762f671.chunk.js
        monitoring/static/js/51819.ac8257ca.chunk.js monitoring/static/js/51819.ac8257ca.chunk.js
        monitoring/static/js/52036.0bcd45d5.chunk.js monitoring/static/js/52036.0bcd45d5.chunk.js
        monitoring/static/js/52182.735ff091.chunk.js monitoring/static/js/52182.735ff091.chunk.js
        monitoring/static/js/52518.2a3ff21a.chunk.js monitoring/static/js/52518.2a3ff21a.chunk.js
        monitoring/static/js/52518.2a3ff21a.chunk.js.LICENSE.txt monitoring/static/js/52518.2a3ff21a.chunk.js.LICENSE.txt
        monitoring/static/js/52527.57100447.chunk.js monitoring/static/js/52527.57100447.chunk.js
        monitoring/static/js/5254.ef9c1c59.chunk.js monitoring/static/js/5254.ef9c1c59.chunk.js
        monitoring/static/js/52541.7c3f886c.chunk.js monitoring/static/js/52541.7c3f886c.chunk.js
        monitoring/static/js/5282.37c7be51.chunk.js monitoring/static/js/5282.37c7be51.chunk.js
        monitoring/static/js/53338.161bc4dd.chunk.js monitoring/static/js/53338.161bc4dd.chunk.js
        monitoring/static/js/53338.161bc4dd.chunk.js.LICENSE.txt monitoring/static/js/53338.161bc4dd.chunk.js.LICENSE.txt
        monitoring/static/js/53672.a70ebf8e.chunk.js monitoring/static/js/53672.a70ebf8e.chunk.js
        monitoring/static/js/5393.cb636c81.chunk.js monitoring/static/js/5393.cb636c81.chunk.js
        monitoring/static/js/5411.001c1dc7.chunk.js monitoring/static/js/5411.001c1dc7.chunk.js
        monitoring/static/js/54520.c2776d7f.chunk.js monitoring/static/js/54520.c2776d7f.chunk.js
        monitoring/static/js/54597.f81d6f07.chunk.js monitoring/static/js/54597.f81d6f07.chunk.js
        monitoring/static/js/54651.40344b3d.chunk.js monitoring/static/js/54651.40344b3d.chunk.js
        monitoring/static/js/54678.5a7b7b35.chunk.js monitoring/static/js/54678.5a7b7b35.chunk.js
        monitoring/static/js/54678.5a7b7b35.chunk.js.LICENSE.txt monitoring/static/js/54678.5a7b7b35.chunk.js.LICENSE.txt
        monitoring/static/js/5475.20ce4f75.chunk.js monitoring/static/js/5475.20ce4f75.chunk.js
        monitoring/static/js/54781.9ee0aecd.chunk.js monitoring/static/js/54781.9ee0aecd.chunk.js
        monitoring/static/js/54861.f927c937.chunk.js monitoring/static/js/54861.f927c937.chunk.js
        monitoring/static/js/5530.da339b78.chunk.js monitoring/static/js/5530.da339b78.chunk.js
        monitoring/static/js/55454.4f52583e.chunk.js monitoring/static/js/55454.4f52583e.chunk.js
        monitoring/static/js/55454.4f52583e.chunk.js.LICENSE.txt monitoring/static/js/55454.4f52583e.chunk.js.LICENSE.txt
        monitoring/static/js/55528.e1e10ce0.chunk.js monitoring/static/js/55528.e1e10ce0.chunk.js
        monitoring/static/js/55534.43828e20.chunk.js monitoring/static/js/55534.43828e20.chunk.js
        monitoring/static/js/55651.bd57c77c.chunk.js monitoring/static/js/55651.bd57c77c.chunk.js
        monitoring/static/js/55816.ceb201d4.chunk.js monitoring/static/js/55816.ceb201d4.chunk.js
        monitoring/static/js/55853.2ca378d0.chunk.js monitoring/static/js/55853.2ca378d0.chunk.js
        monitoring/static/js/55990.c86b7669.chunk.js monitoring/static/js/55990.c86b7669.chunk.js
        monitoring/static/js/56026.85d58e2b.chunk.js monitoring/static/js/56026.85d58e2b.chunk.js
        monitoring/static/js/56054.9d70a2ed.chunk.js monitoring/static/js/56054.9d70a2ed.chunk.js
        monitoring/static/js/56174.562b7d92.chunk.js monitoring/static/js/56174.562b7d92.chunk.js
        monitoring/static/js/5634.0a0bddae.chunk.js monitoring/static/js/5634.0a0bddae.chunk.js
        monitoring/static/js/56358.3a141569.chunk.js monitoring/static/js/56358.3a141569.chunk.js
        monitoring/static/js/56358.3a141569.chunk.js.LICENSE.txt monitoring/static/js/56358.3a141569.chunk.js.LICENSE.txt
        monitoring/static/js/56405.5fa107d3.chunk.js monitoring/static/js/56405.5fa107d3.chunk.js
        monitoring/static/js/56421.a250ca1b.chunk.js monitoring/static/js/56421.a250ca1b.chunk.js
        monitoring/static/js/5647.0920ef73.chunk.js monitoring/static/js/5647.0920ef73.chunk.js
        monitoring/static/js/56761.638c1141.chunk.js monitoring/static/js/56761.638c1141.chunk.js
        monitoring/static/js/56990.be6d200f.chunk.js monitoring/static/js/56990.be6d200f.chunk.js
        monitoring/static/js/57016.ccc30938.chunk.js monitoring/static/js/57016.ccc30938.chunk.js
        monitoring/static/js/57118.e38774e7.chunk.js monitoring/static/js/57118.e38774e7.chunk.js
        monitoring/static/js/57118.e38774e7.chunk.js.LICENSE.txt monitoring/static/js/57118.e38774e7.chunk.js.LICENSE.txt
        monitoring/static/js/57206.7544a09d.chunk.js monitoring/static/js/57206.7544a09d.chunk.js
        monitoring/static/js/57206.7544a09d.chunk.js.LICENSE.txt monitoring/static/js/57206.7544a09d.chunk.js.LICENSE.txt
        monitoring/static/js/57320.74fc8316.chunk.js monitoring/static/js/57320.74fc8316.chunk.js
        monitoring/static/js/57469.85faf9a5.chunk.js monitoring/static/js/57469.85faf9a5.chunk.js
        monitoring/static/js/57708.c2b518ee.chunk.js monitoring/static/js/57708.c2b518ee.chunk.js
        monitoring/static/js/57946.31f41343.chunk.js monitoring/static/js/57946.31f41343.chunk.js
        monitoring/static/js/57946.31f41343.chunk.js.LICENSE.txt monitoring/static/js/57946.31f41343.chunk.js.LICENSE.txt
        monitoring/static/js/57970.67e88902.chunk.js monitoring/static/js/57970.67e88902.chunk.js
        monitoring/static/js/58457.708b5a15.chunk.js monitoring/static/js/58457.708b5a15.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js monitoring/static/js/5866.14d27c8c.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt
        monitoring/static/js/58666.91b79adf.chunk.js monitoring/static/js/58666.91b79adf.chunk.js
        monitoring/static/js/58840.529e7b9b.chunk.js monitoring/static/js/58840.529e7b9b.chunk.js
        monitoring/static/js/58890.cf2d0d90.chunk.js monitoring/static/js/58890.cf2d0d90.chunk.js
        monitoring/static/js/58986.472d72cc.chunk.js monitoring/static/js/58986.472d72cc.chunk.js
        monitoring/static/js/58986.472d72cc.chunk.js.LICENSE.txt monitoring/static/js/58986.472d72cc.chunk.js.LICENSE.txt
        monitoring/static/js/59243.5de594f4.chunk.js monitoring/static/js/59243.5de594f4.chunk.js
        monitoring/static/js/59605.a2f7e321.chunk.js monitoring/static/js/59605.a2f7e321.chunk.js
        monitoring/static/js/59748.4682a957.chunk.js monitoring/static/js/59748.4682a957.chunk.js
        monitoring/static/js/59748.4682a957.chunk.js.LICENSE.txt monitoring/static/js/59748.4682a957.chunk.js.LICENSE.txt
        monitoring/static/js/59786.9a9eda90.chunk.js monitoring/static/js/59786.9a9eda90.chunk.js
        monitoring/static/js/59882.b6def2ca.chunk.js monitoring/static/js/59882.b6def2ca.chunk.js
        monitoring/static/js/59908.4f5fa1f6.chunk.js monitoring/static/js/59908.4f5fa1f6.chunk.js
        monitoring/static/js/60110.448cdddf.chunk.js monitoring/static/js/60110.448cdddf.chunk.js
        monitoring/static/js/60110.448cdddf.chunk.js.LICENSE.txt monitoring/static/js/60110.448cdddf.chunk.js.LICENSE.txt
        monitoring/static/js/60221.8d560e16.chunk.js monitoring/static/js/60221.8d560e16.chunk.js
        monitoring/static/js/60464.c820a295.chunk.js monitoring/static/js/60464.c820a295.chunk.js
        monitoring/static/js/60949.c74a3708.chunk.js monitoring/static/js/60949.c74a3708.chunk.js
        monitoring/static/js/61088.c55195af.chunk.js monitoring/static/js/61088.c55195af.chunk.js
        monitoring/static/js/61250.2b3f06a3.chunk.js monitoring/static/js/61250.2b3f06a3.chunk.js
        monitoring/static/js/61387.f19330bb.chunk.js monitoring/static/js/61387.f19330bb.chunk.js
        monitoring/static/js/61741.c551cb8f.chunk.js monitoring/static/js/61741.c551cb8f.chunk.js
        monitoring/static/js/61747.0c4ed2d6.chunk.js monitoring/static/js/61747.0c4ed2d6.chunk.js
        monitoring/static/js/61865.21725853.chunk.js monitoring/static/js/61865.21725853.chunk.js
        monitoring/static/js/61917.92d39b4c.chunk.js monitoring/static/js/61917.92d39b4c.chunk.js
        monitoring/static/js/6197.acb1fd7c.chunk.js monitoring/static/js/6197.acb1fd7c.chunk.js
        monitoring/static/js/62042.e21d383b.chunk.js monitoring/static/js/62042.e21d383b.chunk.js
        monitoring/static/js/62042.e21d383b.chunk.js.LICENSE.txt monitoring/static/js/62042.e21d383b.chunk.js.LICENSE.txt
        monitoring/static/js/62308.fe05af2f.chunk.js monitoring/static/js/62308.fe05af2f.chunk.js
        monitoring/static/js/62350.07b0039d.chunk.js monitoring/static/js/62350.07b0039d.chunk.js
        monitoring/static/js/62350.07b0039d.chunk.js.LICENSE.txt monitoring/static/js/62350.07b0039d.chunk.js.LICENSE.txt
        monitoring/static/js/62377.cfab7757.chunk.js monitoring/static/js/62377.cfab7757.chunk.js
        monitoring/static/js/62595.0c9bd5a0.chunk.js monitoring/static/js/62595.0c9bd5a0.chunk.js
        monitoring/static/js/6261.78de43a8.chunk.js monitoring/static/js/6261.78de43a8.chunk.js
        monitoring/static/js/62888.e3af7359.chunk.js monitoring/static/js/62888.e3af7359.chunk.js
        monitoring/static/js/63008.97387142.chunk.js monitoring/static/js/63008.97387142.chunk.js
        monitoring/static/js/63653.c2f7dcde.chunk.js monitoring/static/js/63653.c2f7dcde.chunk.js
        monitoring/static/js/63679.05a63e19.chunk.js monitoring/static/js/63679.05a63e19.chunk.js
        monitoring/static/js/63769.731ffb68.chunk.js monitoring/static/js/63769.731ffb68.chunk.js
        monitoring/static/js/63782.48301ab7.chunk.js monitoring/static/js/63782.48301ab7.chunk.js
        monitoring/static/js/65206.7f46c107.chunk.js monitoring/static/js/65206.7f46c107.chunk.js
        monitoring/static/js/65252.2655458e.chunk.js monitoring/static/js/65252.2655458e.chunk.js
        monitoring/static/js/65252.2655458e.chunk.js.LICENSE.txt monitoring/static/js/65252.2655458e.chunk.js.LICENSE.txt
        monitoring/static/js/6528.77d69abb.chunk.js monitoring/static/js/6528.77d69abb.chunk.js
        monitoring/static/js/65401.e76db1a2.chunk.js monitoring/static/js/65401.e76db1a2.chunk.js
        monitoring/static/js/65579.1ec2325b.chunk.js monitoring/static/js/65579.1ec2325b.chunk.js
        monitoring/static/js/65633.b6bc2f47.chunk.js monitoring/static/js/65633.b6bc2f47.chunk.js
        monitoring/static/js/65915.935a70d0.chunk.js monitoring/static/js/65915.935a70d0.chunk.js
        monitoring/static/js/65988.11e4149b.chunk.js monitoring/static/js/65988.11e4149b.chunk.js
        monitoring/static/js/65988.11e4149b.chunk.js.LICENSE.txt monitoring/static/js/65988.11e4149b.chunk.js.LICENSE.txt
        monitoring/static/js/66262.b361ce28.chunk.js monitoring/static/js/66262.b361ce28.chunk.js
        monitoring/static/js/66262.b361ce28.chunk.js.LICENSE.txt monitoring/static/js/66262.b361ce28.chunk.js.LICENSE.txt
        monitoring/static/js/66397.32c2f9da.chunk.js monitoring/static/js/66397.32c2f9da.chunk.js
        monitoring/static/js/66447.716a34f7.chunk.js monitoring/static/js/66447.716a34f7.chunk.js
        monitoring/static/js/66447.716a34f7.chunk.js.LICENSE.txt monitoring/static/js/66447.716a34f7.chunk.js.LICENSE.txt
        monitoring/static/js/66593.94c01a99.chunk.js monitoring/static/js/66593.94c01a99.chunk.js
        monitoring/static/js/66809.a4c3fdb1.chunk.js monitoring/static/js/66809.a4c3fdb1.chunk.js
        monitoring/static/js/66820.ec86ae7a.chunk.js monitoring/static/js/66820.ec86ae7a.chunk.js
        monitoring/static/js/66820.ec86ae7a.chunk.js.LICENSE.txt monitoring/static/js/66820.ec86ae7a.chunk.js.LICENSE.txt
        monitoring/static/js/66824.abfa3f22.chunk.js monitoring/static/js/66824.abfa3f22.chunk.js
        monitoring/static/js/67105.3413451f.chunk.js monitoring/static/js/67105.3413451f.chunk.js
        monitoring/static/js/67191.46b77437.chunk.js monitoring/static/js/67191.46b77437.chunk.js
        monitoring/static/js/67329.08db90c1.chunk.js monitoring/static/js/67329.08db90c1.chunk.js
        monitoring/static/js/67348.c1ed85fa.chunk.js monitoring/static/js/67348.c1ed85fa.chunk.js
        monitoring/static/js/67357.dd9aa014.chunk.js monitoring/static/js/67357.dd9aa014.chunk.js
        monitoring/static/js/67574.31643beb.chunk.js monitoring/static/js/67574.31643beb.chunk.js
        monitoring/static/js/67574.31643beb.chunk.js.LICENSE.txt monitoring/static/js/67574.31643beb.chunk.js.LICENSE.txt
        monitoring/static/js/67605.6cd42d90.chunk.js monitoring/static/js/67605.6cd42d90.chunk.js
        monitoring/static/js/6785.f25ed122.chunk.js monitoring/static/js/6785.f25ed122.chunk.js
        monitoring/static/js/68220.ece8573d.chunk.js monitoring/static/js/68220.ece8573d.chunk.js
        monitoring/static/js/68377.f73a91b7.chunk.js monitoring/static/js/68377.f73a91b7.chunk.js
        monitoring/static/js/68527.1f687bcf.chunk.js monitoring/static/js/68527.1f687bcf.chunk.js
        monitoring/static/js/6881.7e6434c9.chunk.js monitoring/static/js/6881.7e6434c9.chunk.js
        monitoring/static/js/68818.0e24392e.chunk.js monitoring/static/js/68818.0e24392e.chunk.js
        monitoring/static/js/68821.a96b8277.chunk.js monitoring/static/js/68821.a96b8277.chunk.js
        monitoring/static/js/68821.a96b8277.chunk.js.LICENSE.txt monitoring/static/js/68821.a96b8277.chunk.js.LICENSE.txt
        monitoring/static/js/68990.59087cc5.chunk.js monitoring/static/js/68990.59087cc5.chunk.js
        monitoring/static/js/69220.f0f0b57b.chunk.js monitoring/static/js/69220.f0f0b57b.chunk.js
        monitoring/static/js/69500.7fbd370c.chunk.js monitoring/static/js/69500.7fbd370c.chunk.js
        monitoring/static/js/69712.983d0bad.chunk.js monitoring/static/js/69712.983d0bad.chunk.js
        monitoring/static/js/69854.1159f91a.chunk.js monitoring/static/js/69854.1159f91a.chunk.js
        monitoring/static/js/69997.3bda423b.chunk.js monitoring/static/js/69997.3bda423b.chunk.js
        monitoring/static/js/70190.e3137ef3.chunk.js monitoring/static/js/70190.e3137ef3.chunk.js
        monitoring/static/js/70225.f064b5ad.chunk.js monitoring/static/js/70225.f064b5ad.chunk.js
        monitoring/static/js/70225.f064b5ad.chunk.js.LICENSE.txt monitoring/static/js/70225.f064b5ad.chunk.js.LICENSE.txt
        monitoring/static/js/70289.b63d5fb8.chunk.js monitoring/static/js/70289.b63d5fb8.chunk.js
        monitoring/static/js/70695.3af812a3.chunk.js monitoring/static/js/70695.3af812a3.chunk.js
        monitoring/static/js/70858.35d686d1.chunk.js monitoring/static/js/70858.35d686d1.chunk.js
        monitoring/static/js/71107.d2d26409.chunk.js monitoring/static/js/71107.d2d26409.chunk.js
        monitoring/static/js/71266.6ba99b0a.chunk.js monitoring/static/js/71266.6ba99b0a.chunk.js
        monitoring/static/js/71486.3e01f058.chunk.js monitoring/static/js/71486.3e01f058.chunk.js
        monitoring/static/js/71515.2280d42d.chunk.js monitoring/static/js/71515.2280d42d.chunk.js
        monitoring/static/js/71588.5c21e822.chunk.js monitoring/static/js/71588.5c21e822.chunk.js
        monitoring/static/js/71672.9d9a091b.chunk.js monitoring/static/js/71672.9d9a091b.chunk.js
        monitoring/static/js/71756.324c49c8.chunk.js monitoring/static/js/71756.324c49c8.chunk.js
        monitoring/static/js/72020.e0c38d22.chunk.js monitoring/static/js/72020.e0c38d22.chunk.js
        monitoring/static/js/72188.5b48f0f8.chunk.js monitoring/static/js/72188.5b48f0f8.chunk.js
        monitoring/static/js/72401.bef7ab50.chunk.js monitoring/static/js/72401.bef7ab50.chunk.js
        monitoring/static/js/72568.9f8b7a4b.chunk.js monitoring/static/js/72568.9f8b7a4b.chunk.js
        monitoring/static/js/72737.e79b7900.chunk.js monitoring/static/js/72737.e79b7900.chunk.js
        monitoring/static/js/72775.0e6824d4.chunk.js monitoring/static/js/72775.0e6824d4.chunk.js
        monitoring/static/js/72788.d00f6565.chunk.js monitoring/static/js/72788.d00f6565.chunk.js
        monitoring/static/js/73026.ba38cd34.chunk.js monitoring/static/js/73026.ba38cd34.chunk.js
        monitoring/static/js/73064.b0c26084.chunk.js monitoring/static/js/73064.b0c26084.chunk.js
        monitoring/static/js/73238.abca2b52.chunk.js monitoring/static/js/73238.abca2b52.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js monitoring/static/js/734.3fe325e9.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt
        monitoring/static/js/73442.0bbc74fd.chunk.js monitoring/static/js/73442.0bbc74fd.chunk.js
        monitoring/static/js/73478.353da8fe.chunk.js monitoring/static/js/73478.353da8fe.chunk.js
        monitoring/static/js/73478.353da8fe.chunk.js.LICENSE.txt monitoring/static/js/73478.353da8fe.chunk.js.LICENSE.txt
        monitoring/static/js/73534.e63e8bd4.chunk.js monitoring/static/js/73534.e63e8bd4.chunk.js
        monitoring/static/js/73534.e63e8bd4.chunk.js.LICENSE.txt monitoring/static/js/73534.e63e8bd4.chunk.js.LICENSE.txt
        monitoring/static/js/73863.6655927e.chunk.js monitoring/static/js/73863.6655927e.chunk.js
        monitoring/static/js/73879.9dc10432.chunk.js monitoring/static/js/73879.9dc10432.chunk.js
        monitoring/static/js/74324.03761f87.chunk.js monitoring/static/js/74324.03761f87.chunk.js
        monitoring/static/js/74394.3bb376a8.chunk.js monitoring/static/js/74394.3bb376a8.chunk.js
        monitoring/static/js/74729.fa6e7280.chunk.js monitoring/static/js/74729.fa6e7280.chunk.js
        monitoring/static/js/74891.fa3d6f11.chunk.js monitoring/static/js/74891.fa3d6f11.chunk.js
        monitoring/static/js/7548.fd5d2b6c.chunk.js monitoring/static/js/7548.fd5d2b6c.chunk.js
        monitoring/static/js/75523.d65a825f.chunk.js monitoring/static/js/75523.d65a825f.chunk.js
        monitoring/static/js/76603.db4ff761.chunk.js monitoring/static/js/76603.db4ff761.chunk.js
        monitoring/static/js/76879.ff0dd32c.chunk.js monitoring/static/js/76879.ff0dd32c.chunk.js
        monitoring/static/js/77642.1a976f0c.chunk.js monitoring/static/js/77642.1a976f0c.chunk.js
        monitoring/static/js/77642.1a976f0c.chunk.js.LICENSE.txt monitoring/static/js/77642.1a976f0c.chunk.js.LICENSE.txt
        monitoring/static/js/77697.23abc7db.chunk.js monitoring/static/js/77697.23abc7db.chunk.js
        monitoring/static/js/77718.38572b28.chunk.js monitoring/static/js/77718.38572b28.chunk.js
        monitoring/static/js/77718.38572b28.chunk.js.LICENSE.txt monitoring/static/js/77718.38572b28.chunk.js.LICENSE.txt
        monitoring/static/js/7773.b2d5a51a.chunk.js monitoring/static/js/7773.b2d5a51a.chunk.js
        monitoring/static/js/78053.1b2b1602.chunk.js monitoring/static/js/78053.1b2b1602.chunk.js
        monitoring/static/js/78112.671df87e.chunk.js monitoring/static/js/78112.671df87e.chunk.js
        monitoring/static/js/78517.bd6413c4.chunk.js monitoring/static/js/78517.bd6413c4.chunk.js
        monitoring/static/js/78710.673d31cf.chunk.js monitoring/static/js/78710.673d31cf.chunk.js
        monitoring/static/js/78979.e31ea57f.chunk.js monitoring/static/js/78979.e31ea57f.chunk.js
        monitoring/static/js/79204.b8bc5268.chunk.js monitoring/static/js/79204.b8bc5268.chunk.js
        monitoring/static/js/79247.1bc3dc95.chunk.js monitoring/static/js/79247.1bc3dc95.chunk.js
        monitoring/static/js/79312.17a58c6f.chunk.js monitoring/static/js/79312.17a58c6f.chunk.js
        monitoring/static/js/79312.17a58c6f.chunk.js.LICENSE.txt monitoring/static/js/79312.17a58c6f.chunk.js.LICENSE.txt
        monitoring/static/js/79433.b3b128c9.chunk.js monitoring/static/js/79433.b3b128c9.chunk.js
        monitoring/static/js/79707.def8f77e.chunk.js monitoring/static/js/79707.def8f77e.chunk.js
        monitoring/static/js/79842.ef61156d.chunk.js monitoring/static/js/79842.ef61156d.chunk.js
        monitoring/static/js/79842.ef61156d.chunk.js.LICENSE.txt monitoring/static/js/79842.ef61156d.chunk.js.LICENSE.txt
        monitoring/static/js/79972.b465d16f.chunk.js monitoring/static/js/79972.b465d16f.chunk.js
        monitoring/static/js/80017.210d20b7.chunk.js monitoring/static/js/80017.210d20b7.chunk.js
        monitoring/static/js/80030.4efa59e1.chunk.js monitoring/static/js/80030.4efa59e1.chunk.js
        monitoring/static/js/80030.4efa59e1.chunk.js.LICENSE.txt monitoring/static/js/80030.4efa59e1.chunk.js.LICENSE.txt
        monitoring/static/js/80067.910ba368.chunk.js monitoring/static/js/80067.910ba368.chunk.js
        monitoring/static/js/80108.0334ef65.chunk.js monitoring/static/js/80108.0334ef65.chunk.js
        monitoring/static/js/80397.e9187aee.chunk.js monitoring/static/js/80397.e9187aee.chunk.js
        monitoring/static/js/80555.c5181f9f.chunk.js monitoring/static/js/80555.c5181f9f.chunk.js
        monitoring/static/js/80719.840e5448.chunk.js monitoring/static/js/80719.840e5448.chunk.js
        monitoring/static/js/80921.252f76ca.chunk.js monitoring/static/js/80921.252f76ca.chunk.js
        monitoring/static/js/81014.351ef346.chunk.js monitoring/static/js/81014.351ef346.chunk.js
        monitoring/static/js/81243.4d5129fa.chunk.js monitoring/static/js/81243.4d5129fa.chunk.js
        monitoring/static/js/81299.b1fcb7d9.chunk.js monitoring/static/js/81299.b1fcb7d9.chunk.js
        monitoring/static/js/81327.e08f6d45.chunk.js monitoring/static/js/81327.e08f6d45.chunk.js
        monitoring/static/js/81571.71d0a13e.chunk.js monitoring/static/js/81571.71d0a13e.chunk.js
        monitoring/static/js/81747.65a6a7f3.chunk.js monitoring/static/js/81747.65a6a7f3.chunk.js
        monitoring/static/js/81836.c32c7c12.chunk.js monitoring/static/js/81836.c32c7c12.chunk.js
        monitoring/static/js/81869.a0a15184.chunk.js monitoring/static/js/81869.a0a15184.chunk.js
        monitoring/static/js/81940.05d638c7.chunk.js monitoring/static/js/81940.05d638c7.chunk.js
        monitoring/static/js/82053.2a21538f.chunk.js monitoring/static/js/82053.2a21538f.chunk.js
        monitoring/static/js/82066.573664a4.chunk.js monitoring/static/js/82066.573664a4.chunk.js
        monitoring/static/js/8215.36727d1f.chunk.js monitoring/static/js/8215.36727d1f.chunk.js
        monitoring/static/js/82315.a87de6b3.chunk.js monitoring/static/js/82315.a87de6b3.chunk.js
        monitoring/static/js/82399.80864f92.chunk.js monitoring/static/js/82399.80864f92.chunk.js
        monitoring/static/js/82505.405051e6.chunk.js monitoring/static/js/82505.405051e6.chunk.js
        monitoring/static/js/82714.46d13b51.chunk.js monitoring/static/js/82714.46d13b51.chunk.js
        monitoring/static/js/82960.aae45b92.chunk.js monitoring/static/js/82960.aae45b92.chunk.js
        monitoring/static/js/83005.08f70905.chunk.js monitoring/static/js/83005.08f70905.chunk.js
        monitoring/static/js/83012.103c3f36.chunk.js monitoring/static/js/83012.103c3f36.chunk.js
        monitoring/static/js/83075.aa140970.chunk.js monitoring/static/js/83075.aa140970.chunk.js
        monitoring/static/js/83333.65b07752.chunk.js monitoring/static/js/83333.65b07752.chunk.js
        monitoring/static/js/83510.ed31c9f8.chunk.js monitoring/static/js/83510.ed31c9f8.chunk.js
        monitoring/static/js/83896.d4b0cab7.chunk.js monitoring/static/js/83896.d4b0cab7.chunk.js
        monitoring/static/js/84027.c693664b.chunk.js monitoring/static/js/84027.c693664b.chunk.js
        monitoring/static/js/84307.59d32ce3.chunk.js monitoring/static/js/84307.59d32ce3.chunk.js
        monitoring/static/js/84401.8a09e9ae.chunk.js monitoring/static/js/84401.8a09e9ae.chunk.js
        monitoring/static/js/84578.9e6910e9.chunk.js monitoring/static/js/84578.9e6910e9.chunk.js
        monitoring/static/js/84582.caa06f15.chunk.js monitoring/static/js/84582.caa06f15.chunk.js
        monitoring/static/js/84582.caa06f15.chunk.js.LICENSE.txt monitoring/static/js/84582.caa06f15.chunk.js.LICENSE.txt
        monitoring/static/js/84587.961ae27a.chunk.js monitoring/static/js/84587.961ae27a.chunk.js
        monitoring/static/js/84652.5dfa8103.chunk.js monitoring/static/js/84652.5dfa8103.chunk.js
        monitoring/static/js/84745.60da2449.chunk.js monitoring/static/js/84745.60da2449.chunk.js
        monitoring/static/js/84870.ab138a59.chunk.js monitoring/static/js/84870.ab138a59.chunk.js
        monitoring/static/js/84870.ab138a59.chunk.js.LICENSE.txt monitoring/static/js/84870.ab138a59.chunk.js.LICENSE.txt
        monitoring/static/js/84960.05121e37.chunk.js monitoring/static/js/84960.05121e37.chunk.js
        monitoring/static/js/85027.462a7fdc.chunk.js monitoring/static/js/85027.462a7fdc.chunk.js
        monitoring/static/js/85047.23b0ab8e.chunk.js monitoring/static/js/85047.23b0ab8e.chunk.js
        monitoring/static/js/85169.2c7d5d73.chunk.js monitoring/static/js/85169.2c7d5d73.chunk.js
        monitoring/static/js/8534.f7aec532.chunk.js monitoring/static/js/8534.f7aec532.chunk.js
        monitoring/static/js/85393.341703ec.chunk.js monitoring/static/js/85393.341703ec.chunk.js
        monitoring/static/js/85406.ba8965be.chunk.js monitoring/static/js/85406.ba8965be.chunk.js
        monitoring/static/js/85595.262d8065.chunk.js monitoring/static/js/85595.262d8065.chunk.js
        monitoring/static/js/85622.18615d00.chunk.js monitoring/static/js/85622.18615d00.chunk.js
        monitoring/static/js/85623.d896063a.chunk.js monitoring/static/js/85623.d896063a.chunk.js
        monitoring/static/js/85664.569016a5.chunk.js monitoring/static/js/85664.569016a5.chunk.js
        monitoring/static/js/85776.0f7f2e5d.chunk.js monitoring/static/js/85776.0f7f2e5d.chunk.js
        monitoring/static/js/85950.98e6e44a.chunk.js monitoring/static/js/85950.98e6e44a.chunk.js
        monitoring/static/js/85953.ea1ee23e.chunk.js monitoring/static/js/85953.ea1ee23e.chunk.js
        monitoring/static/js/86114.0fc0acbb.chunk.js monitoring/static/js/86114.0fc0acbb.chunk.js
        monitoring/static/js/86299.c5989de8.chunk.js monitoring/static/js/86299.c5989de8.chunk.js
        monitoring/static/js/86342.528e5efc.chunk.js monitoring/static/js/86342.528e5efc.chunk.js
        monitoring/static/js/86342.528e5efc.chunk.js.LICENSE.txt monitoring/static/js/86342.528e5efc.chunk.js.LICENSE.txt
        monitoring/static/js/86472.57a5a1cc.chunk.js monitoring/static/js/86472.57a5a1cc.chunk.js
        monitoring/static/js/86658.6a6caa74.chunk.js monitoring/static/js/86658.6a6caa74.chunk.js
        monitoring/static/js/86658.6a6caa74.chunk.js.LICENSE.txt monitoring/static/js/86658.6a6caa74.chunk.js.LICENSE.txt
        monitoring/static/js/86814.c1a521f5.chunk.js monitoring/static/js/86814.c1a521f5.chunk.js
        monitoring/static/js/8704.87492da1.chunk.js monitoring/static/js/8704.87492da1.chunk.js
        monitoring/static/js/87138.cf3f482b.chunk.js monitoring/static/js/87138.cf3f482b.chunk.js
        monitoring/static/js/87233.c8b49edb.chunk.js monitoring/static/js/87233.c8b49edb.chunk.js
        monitoring/static/js/87357.d4031306.chunk.js monitoring/static/js/87357.d4031306.chunk.js
        monitoring/static/js/87429.3792c589.chunk.js monitoring/static/js/87429.3792c589.chunk.js
        monitoring/static/js/87962.2094d7c1.chunk.js monitoring/static/js/87962.2094d7c1.chunk.js
        monitoring/static/js/88081.e48ec099.chunk.js monitoring/static/js/88081.e48ec099.chunk.js
        monitoring/static/js/88119.041d294e.chunk.js monitoring/static/js/88119.041d294e.chunk.js
        monitoring/static/js/88269.9b813297.chunk.js monitoring/static/js/88269.9b813297.chunk.js
        monitoring/static/js/88432.95ced9fa.chunk.js monitoring/static/js/88432.95ced9fa.chunk.js
        monitoring/static/js/88706.b895fbe4.chunk.js monitoring/static/js/88706.b895fbe4.chunk.js
        monitoring/static/js/88810.cb646554.chunk.js monitoring/static/js/88810.cb646554.chunk.js
        monitoring/static/js/88987.f221b1c0.chunk.js monitoring/static/js/88987.f221b1c0.chunk.js
        monitoring/static/js/89015.1865c336.chunk.js monitoring/static/js/89015.1865c336.chunk.js
        monitoring/static/js/89025.e20277a3.chunk.js monitoring/static/js/89025.e20277a3.chunk.js
        monitoring/static/js/89033.56232409.chunk.js monitoring/static/js/89033.56232409.chunk.js
        monitoring/static/js/89177.f2ac8235.chunk.js monitoring/static/js/89177.f2ac8235.chunk.js
        monitoring/static/js/89222.d941dfbd.chunk.js monitoring/static/js/89222.d941dfbd.chunk.js
        monitoring/static/js/89697.31847e00.chunk.js monitoring/static/js/89697.31847e00.chunk.js
        monitoring/static/js/89922.e5924c1e.chunk.js monitoring/static/js/89922.e5924c1e.chunk.js
        monitoring/static/js/90118.4fc97e01.chunk.js monitoring/static/js/90118.4fc97e01.chunk.js
        monitoring/static/js/90118.4fc97e01.chunk.js.LICENSE.txt monitoring/static/js/90118.4fc97e01.chunk.js.LICENSE.txt
        monitoring/static/js/90290.a7f1549c.chunk.js monitoring/static/js/90290.a7f1549c.chunk.js
        monitoring/static/js/90330.7878a0d4.chunk.js monitoring/static/js/90330.7878a0d4.chunk.js
        monitoring/static/js/90367.afe12186.chunk.js monitoring/static/js/90367.afe12186.chunk.js
        monitoring/static/js/90504.d340d9cc.chunk.js monitoring/static/js/90504.d340d9cc.chunk.js
        monitoring/static/js/90513.c6053ab5.chunk.js monitoring/static/js/90513.c6053ab5.chunk.js
        monitoring/static/js/90529.112c30e3.chunk.js monitoring/static/js/90529.112c30e3.chunk.js
        monitoring/static/js/90561.e3f79915.chunk.js monitoring/static/js/90561.e3f79915.chunk.js
        monitoring/static/js/90561.e3f79915.chunk.js.LICENSE.txt monitoring/static/js/90561.e3f79915.chunk.js.LICENSE.txt
        monitoring/static/js/90628.e6a33d41.chunk.js monitoring/static/js/90628.e6a33d41.chunk.js
        monitoring/static/js/91087.3a776c06.chunk.js monitoring/static/js/91087.3a776c06.chunk.js
        monitoring/static/js/91249.38e22793.chunk.js monitoring/static/js/91249.38e22793.chunk.js
        monitoring/static/js/91545.080bf65f.chunk.js monitoring/static/js/91545.080bf65f.chunk.js
        monitoring/static/js/9174.ae7682da.chunk.js monitoring/static/js/9174.ae7682da.chunk.js
        monitoring/static/js/9177.3b6d6de8.chunk.js monitoring/static/js/9177.3b6d6de8.chunk.js
        monitoring/static/js/91880.1a716d56.chunk.js monitoring/static/js/91880.1a716d56.chunk.js
        monitoring/static/js/92008.7d46cb66.chunk.js monitoring/static/js/92008.7d46cb66.chunk.js
        monitoring/static/js/92016.1c9c5217.chunk.js monitoring/static/js/92016.1c9c5217.chunk.js
        monitoring/static/js/92016.1c9c5217.chunk.js.LICENSE.txt monitoring/static/js/92016.1c9c5217.chunk.js.LICENSE.txt
        monitoring/static/js/92466.f38204fe.chunk.js monitoring/static/js/92466.f38204fe.chunk.js
        monitoring/static/js/92551.088c5eb2.chunk.js monitoring/static/js/92551.088c5eb2.chunk.js
        monitoring/static/js/92816.26e23640.chunk.js monitoring/static/js/92816.26e23640.chunk.js
        monitoring/static/js/92868.fbd23d48.chunk.js monitoring/static/js/92868.fbd23d48.chunk.js
        monitoring/static/js/92984.bc3d29a6.chunk.js monitoring/static/js/92984.bc3d29a6.chunk.js
        monitoring/static/js/93033.55bd21d1.chunk.js monitoring/static/js/93033.55bd21d1.chunk.js
        monitoring/static/js/93672.030a2ec6.chunk.js monitoring/static/js/93672.030a2ec6.chunk.js
        monitoring/static/js/93691.0298f4d1.chunk.js monitoring/static/js/93691.0298f4d1.chunk.js
        monitoring/static/js/93696.ebf5e5d2.chunk.js monitoring/static/js/93696.ebf5e5d2.chunk.js
        monitoring/static/js/93756.c3c309ab.chunk.js monitoring/static/js/93756.c3c309ab.chunk.js
        monitoring/static/js/93771.f96f428a.chunk.js monitoring/static/js/93771.f96f428a.chunk.js
        monitoring/static/js/93890.f4cf2ce4.chunk.js monitoring/static/js/93890.f4cf2ce4.chunk.js
        monitoring/static/js/94102.c7e5199b.chunk.js monitoring/static/js/94102.c7e5199b.chunk.js
        monitoring/static/js/94129.b1cdd95d.chunk.js monitoring/static/js/94129.b1cdd95d.chunk.js
        monitoring/static/js/9426.8c0cade1.chunk.js monitoring/static/js/9426.8c0cade1.chunk.js
        monitoring/static/js/94695.a8cc2910.chunk.js monitoring/static/js/94695.a8cc2910.chunk.js
        monitoring/static/js/94810.4ac18bf1.chunk.js monitoring/static/js/94810.4ac18bf1.chunk.js
        monitoring/static/js/9493.f7e806b7.chunk.js monitoring/static/js/9493.f7e806b7.chunk.js
        monitoring/static/js/95050.8313ef1e.chunk.js monitoring/static/js/95050.8313ef1e.chunk.js
        monitoring/static/js/95050.8313ef1e.chunk.js.LICENSE.txt monitoring/static/js/95050.8313ef1e.chunk.js.LICENSE.txt
        monitoring/static/js/9511.aceb118a.chunk.js monitoring/static/js/9511.aceb118a.chunk.js
        monitoring/static/js/9518.3fffdd45.chunk.js monitoring/static/js/9518.3fffdd45.chunk.js
        monitoring/static/js/95264.612bf2f8.chunk.js monitoring/static/js/95264.612bf2f8.chunk.js
        monitoring/static/js/95819.5ebcb088.chunk.js monitoring/static/js/95819.5ebcb088.chunk.js
        monitoring/static/js/95924.d3281fc7.chunk.js monitoring/static/js/95924.d3281fc7.chunk.js
        monitoring/static/js/96017.b8da505b.chunk.js monitoring/static/js/96017.b8da505b.chunk.js
        monitoring/static/js/9606.fc9247cb.chunk.js monitoring/static/js/9606.fc9247cb.chunk.js
        monitoring/static/js/9614.71b68927.chunk.js monitoring/static/js/9614.71b68927.chunk.js
        monitoring/static/js/96410.55c52134.chunk.js monitoring/static/js/96410.55c52134.chunk.js
        monitoring/static/js/96554.bff09e47.chunk.js monitoring/static/js/96554.bff09e47.chunk.js
        monitoring/static/js/96554.bff09e47.chunk.js.LICENSE.txt monitoring/static/js/96554.bff09e47.chunk.js.LICENSE.txt
        monitoring/static/js/97420.08729928.chunk.js monitoring/static/js/97420.08729928.chunk.js
        monitoring/static/js/97440.3db00b72.chunk.js monitoring/static/js/97440.3db00b72.chunk.js
        monitoring/static/js/97638.a9af06da.chunk.js monitoring/static/js/97638.a9af06da.chunk.js
        monitoring/static/js/97638.a9af06da.chunk.js.LICENSE.txt monitoring/static/js/97638.a9af06da.chunk.js.LICENSE.txt
        monitoring/static/js/97748.5df46a4a.chunk.js monitoring/static/js/97748.5df46a4a.chunk.js
        monitoring/static/js/98014.a4ad6ba5.chunk.js monitoring/static/js/98014.a4ad6ba5.chunk.js
        monitoring/static/js/98014.a4ad6ba5.chunk.js.LICENSE.txt monitoring/static/js/98014.a4ad6ba5.chunk.js.LICENSE.txt
        monitoring/static/js/98234.c2934def.chunk.js monitoring/static/js/98234.c2934def.chunk.js
        monitoring/static/js/98234.c2934def.chunk.js.LICENSE.txt monitoring/static/js/98234.c2934def.chunk.js.LICENSE.txt
        monitoring/static/js/98256.8a661541.chunk.js monitoring/static/js/98256.8a661541.chunk.js
        monitoring/static/js/98268.2e2a9c9c.chunk.js monitoring/static/js/98268.2e2a9c9c.chunk.js
        monitoring/static/js/98559.7d826ee7.chunk.js monitoring/static/js/98559.7d826ee7.chunk.js
        monitoring/static/js/98958.21184356.chunk.js monitoring/static/js/98958.21184356.chunk.js
        monitoring/static/js/99010.0141f0a0.chunk.js monitoring/static/js/99010.0141f0a0.chunk.js
        monitoring/static/js/99010.0141f0a0.chunk.js.LICENSE.txt monitoring/static/js/99010.0141f0a0.chunk.js.LICENSE.txt
        monitoring/static/js/99176.319e969d.chunk.js monitoring/static/js/99176.319e969d.chunk.js
        monitoring/static/js/99176.319e969d.chunk.js.LICENSE.txt monitoring/static/js/99176.319e969d.chunk.js.LICENSE.txt
        monitoring/static/js/9930.e56b072a.chunk.js monitoring/static/js/9930.e56b072a.chunk.js
        monitoring/static/js/99341.613b8fce.chunk.js monitoring/static/js/99341.613b8fce.chunk.js
        monitoring/static/js/99466.a47d41f5.chunk.js monitoring/static/js/99466.a47d41f5.chunk.js
        monitoring/static/js/99753.25160545.chunk.js monitoring/static/js/99753.25160545.chunk.js
        monitoring/static/js/99872.c308e65f.chunk.js monitoring/static/js/99872.c308e65f.chunk.js
        monitoring/static/js/99872.c308e65f.chunk.js.LICENSE.txt monitoring/static/js/99872.c308e65f.chunk.js.LICENSE.txt
        monitoring/static/js/main.583d67b6.js monitoring/static/js/main.583d67b6.js
        monitoring/static/js/main.583d67b6.js.LICENSE.txt monitoring/static/js/main.583d67b6.js.LICENSE.txt
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.f6283f7ccaed1249d9eb.ttf monitoring/static/media/codicon.f6283f7ccaed1249d9eb.ttf
        monitoring/static/media/error.9bbd075178a739dcc30f2a7a3e2a3249.svg monitoring/static/media/error.9bbd075178a739dcc30f2a7a3e2a3249.svg
        monitoring/static/media/error.ca9e31d5d3dc34da07e11a00f7af0842.svg monitoring/static/media/error.ca9e31d5d3dc34da07e11a00f7af0842.svg
        monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg
        monitoring/ts.worker.js monitoring/ts.worker.js
        monitoring/ts.worker.js.LICENSE.txt monitoring/ts.worker.js.LICENSE.txt
    )
    # GENERATED MONITORING RESOURCES END
ENDIF()

RESOURCE(
    content/index.html viewer/index.html
    content/viewer.js viewer/viewer.js
    content/jstree.min.js viewer/jstree.min.js
    content/style.min.css viewer/style.min.css
    content/throbber.gif viewer/throbber.gif
    content/32px.png viewer/32px.png
    content/40px.png viewer/40px.png
    content/v2/cpu viewer/v2/cpu
    content/v2/cpu_view.js viewer/v2/cpu_view.js
    content/v2/disk_cell.js viewer/v2/disk_cell.js
    content/v2/disk_map.js viewer/v2/disk_map.js
    content/v2/index.html viewer/v2/index.html
    content/v2/man-green.png viewer/v2/man-green.png
    content/v2/man-orange.png viewer/v2/man-orange.png
    content/v2/man-red.png viewer/v2/man-red.png
    content/v2/man-yellow.png viewer/v2/man-yellow.png
    content/v2/net_view.js viewer/v2/net_view.js
    content/v2/network viewer/v2/network
    content/v2/node_group.js viewer/v2/node_group.js
    content/v2/node.js viewer/v2/node.js
    content/v2/node_map.js viewer/v2/node_map.js
    content/v2/nodes viewer/v2/nodes
    content/v2/node_view.js viewer/v2/node_view.js
    content/v2/overview viewer/v2/overview
    content/v2/overview.js viewer/v2/overview.js
    content/v2/pdisk.js viewer/v2/pdisk.js
    content/v2/pool_block.js viewer/v2/pool_block.js
    content/v2/pool_map.js viewer/v2/pool_map.js
    content/v2/runner.html viewer/v2/runner.html
    content/v2/stats.js viewer/v2/stats.js
    content/v2/storage viewer/v2/storage
    content/v2/storage_group.js viewer/v2/storage_group.js
    content/v2/storage.js viewer/v2/storage.js
    content/v2/storage_view.js viewer/v2/storage_view.js
    content/v2/tablet_cell.js viewer/v2/tablet_cell.js
    content/v2/tablet_map.js viewer/v2/tablet_map.js
    content/v2/tenant.js viewer/v2/tenant.js
    content/v2/tenants viewer/v2/tenants
    content/v2/tenant_view.js viewer/v2/tenant_view.js
    content/v2/throbber.gif viewer/v2/throbber.gif
    content/v2/util.js viewer/v2/util.js
    content/v2/vdisk.js viewer/v2/vdisk.js
    content/v2/viewer.css viewer/v2/viewer.css
    content/v2/viewer.js viewer/v2/viewer.js
    content/api/favicon-16x16.png viewer/api/favicon-16x16.png
    content/api/favicon-32x32.png viewer/api/favicon-32x32.png
    content/api/index.css viewer/api/index.css
    content/api/index.html viewer/api/index.html
    content/api/swagger-initializer.js viewer/api/swagger-initializer.js
    content/api/swagger-ui-bundle.js viewer/api/swagger-ui-bundle.js
    content/api/swagger-ui.css viewer/api/swagger-ui.css
    content/api/swagger-ui-standalone-preset.js viewer/api/swagger-ui-standalone-preset.js
)

PEERDIR(
    ydb/library/actors/core
    ydb/library/actors/helpers
    library/cpp/archive
    library/cpp/mime/types
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/client/server
    ydb/core/external_sources
    ydb/core/graph/api
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/core/health_check
    ydb/core/mon
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/scheme
    ydb/core/sys_view/common
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/core/util
    ydb/core/viewer/json
    ydb/core/viewer/yaml
    ydb/core/viewer/protos
    ydb/library/persqueue/topic_parser
    ydb/library/yaml_config
    ydb/public/api/protos
    ydb/public/lib/deprecated/kicli
    ydb/public/lib/json_value
    ydb/public/lib/ydb_cli/common
    ydb/public/api/grpc
    ydb/public/sdk/cpp/adapters/issue
    ydb/public/sdk/cpp/src/client/types
    ydb/services/lib/auth
    contrib/libs/yaml-cpp
)

YQL_LAST_ABI_VERSION()

END()
