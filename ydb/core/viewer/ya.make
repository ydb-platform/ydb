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
    viewer_commit_offset.h
    viewer_compute.h
    viewer_config.h
    viewer_content.h
    viewer_counters.h
    viewer_database_stats.h
    viewer_describe_consumer.h
    viewer_describe.h
    viewer_describe_topic.h
    viewer_feature_flags.h
    viewer_topic_data.cpp
    viewer_graph.h
    viewer_groups.h
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
    viewer_peers.h
    viewer_pqconsumerinfo.h
    viewer_put_record.h
    viewer_query.h
    viewer_render.h
    viewer_request.cpp
    viewer_request.h
    viewer_storage.h
    viewer_storage_stats.h
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
        monitoring/editor.worker.js monitoring/editor.worker.js
        monitoring/index.html monitoring/index.html
        monitoring/static/css/70776.fef33a69.css monitoring/static/css/70776.fef33a69.css
        monitoring/static/css/async/15145.cf1dcc4c.css monitoring/static/css/async/15145.cf1dcc4c.css
        monitoring/static/css/async/2026.2c4e6ced.css monitoring/static/css/async/2026.2c4e6ced.css
        monitoring/static/css/async/21239.1b7d3003.css monitoring/static/css/async/21239.1b7d3003.css
        monitoring/static/css/async/21678.633b5b03.css monitoring/static/css/async/21678.633b5b03.css
        monitoring/static/css/async/24712.e1ca7d5e.css monitoring/static/css/async/24712.e1ca7d5e.css
        monitoring/static/css/async/27518.8d5be8b9.css monitoring/static/css/async/27518.8d5be8b9.css
        monitoring/static/css/async/30713.ad03fb9c.css monitoring/static/css/async/30713.ad03fb9c.css
        monitoring/static/css/async/35254.7628be1b.css monitoring/static/css/async/35254.7628be1b.css
        monitoring/static/css/async/42042.8890ba8b.css monitoring/static/css/async/42042.8890ba8b.css
        monitoring/static/css/async/55262.539b4093.css monitoring/static/css/async/55262.539b4093.css
        monitoring/static/css/async/67226.afc402c1.css monitoring/static/css/async/67226.afc402c1.css
        monitoring/static/css/async/70950.acec7c88.css monitoring/static/css/async/70950.acec7c88.css
        monitoring/static/css/async/77099.8cbc2f23.css monitoring/static/css/async/77099.8cbc2f23.css
        monitoring/static/css/async/77406.285d38e8.css monitoring/static/css/async/77406.285d38e8.css
        monitoring/static/css/async/81047.371b1d67.css monitoring/static/css/async/81047.371b1d67.css
        monitoring/static/css/async/86222.b4d7cefa.css monitoring/static/css/async/86222.b4d7cefa.css
        monitoring/static/css/index.7b9a53d3.css monitoring/static/css/index.7b9a53d3.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/70776.33ec89ee.js monitoring/static/js/70776.33ec89ee.js
        monitoring/static/js/70776.33ec89ee.js.LICENSE.txt monitoring/static/js/70776.33ec89ee.js.LICENSE.txt
        monitoring/static/js/async/10103.2b987b59.js monitoring/static/js/async/10103.2b987b59.js
        monitoring/static/js/async/10183.8c539f92.js monitoring/static/js/async/10183.8c539f92.js
        monitoring/static/js/async/10273.40deb6ab.js monitoring/static/js/async/10273.40deb6ab.js
        monitoring/static/js/async/1063.90800bdd.js monitoring/static/js/async/1063.90800bdd.js
        monitoring/static/js/async/10650.9dad8a42.js monitoring/static/js/async/10650.9dad8a42.js
        monitoring/static/js/async/10708.bc2661b7.js monitoring/static/js/async/10708.bc2661b7.js
        monitoring/static/js/async/10800.017514a5.js monitoring/static/js/async/10800.017514a5.js
        monitoring/static/js/async/10883.d3d9e388.js monitoring/static/js/async/10883.d3d9e388.js
        monitoring/static/js/async/10883.d3d9e388.js.LICENSE.txt monitoring/static/js/async/10883.d3d9e388.js.LICENSE.txt
        monitoring/static/js/async/11101.9f5d896a.js monitoring/static/js/async/11101.9f5d896a.js
        monitoring/static/js/async/11563.094c639c.js monitoring/static/js/async/11563.094c639c.js
        monitoring/static/js/async/11755.d6879b9d.js monitoring/static/js/async/11755.d6879b9d.js
        monitoring/static/js/async/11755.d6879b9d.js.LICENSE.txt monitoring/static/js/async/11755.d6879b9d.js.LICENSE.txt
        monitoring/static/js/async/1179.8ed56da0.js monitoring/static/js/async/1179.8ed56da0.js
        monitoring/static/js/async/11930.eb008365.js monitoring/static/js/async/11930.eb008365.js
        monitoring/static/js/async/12190.7acb500e.js monitoring/static/js/async/12190.7acb500e.js
        monitoring/static/js/async/12753.2b75a04f.js monitoring/static/js/async/12753.2b75a04f.js
        monitoring/static/js/async/12765.7f44f7d7.js monitoring/static/js/async/12765.7f44f7d7.js
        monitoring/static/js/async/12984.cfb4b165.js monitoring/static/js/async/12984.cfb4b165.js
        monitoring/static/js/async/13677.7595b2ae.js monitoring/static/js/async/13677.7595b2ae.js
        monitoring/static/js/async/13903.503e04f9.js monitoring/static/js/async/13903.503e04f9.js
        monitoring/static/js/async/13907.5419f8ae.js monitoring/static/js/async/13907.5419f8ae.js
        monitoring/static/js/async/13979.350001bb.js monitoring/static/js/async/13979.350001bb.js
        monitoring/static/js/async/13979.350001bb.js.LICENSE.txt monitoring/static/js/async/13979.350001bb.js.LICENSE.txt
        monitoring/static/js/async/14095.e0c884f1.js monitoring/static/js/async/14095.e0c884f1.js
        monitoring/static/js/async/14095.e0c884f1.js.LICENSE.txt monitoring/static/js/async/14095.e0c884f1.js.LICENSE.txt
        monitoring/static/js/async/14323.2c384ec1.js monitoring/static/js/async/14323.2c384ec1.js
        monitoring/static/js/async/14323.2c384ec1.js.LICENSE.txt monitoring/static/js/async/14323.2c384ec1.js.LICENSE.txt
        monitoring/static/js/async/14509.d66ac416.js monitoring/static/js/async/14509.d66ac416.js
        monitoring/static/js/async/14598.3674ebc4.js monitoring/static/js/async/14598.3674ebc4.js
        monitoring/static/js/async/14627.815dfc8f.js monitoring/static/js/async/14627.815dfc8f.js
        monitoring/static/js/async/14627.815dfc8f.js.LICENSE.txt monitoring/static/js/async/14627.815dfc8f.js.LICENSE.txt
        monitoring/static/js/async/1469.9b21d86d.js monitoring/static/js/async/1469.9b21d86d.js
        monitoring/static/js/async/15087.4a4178fb.js monitoring/static/js/async/15087.4a4178fb.js
        monitoring/static/js/async/15139.ff494037.js monitoring/static/js/async/15139.ff494037.js
        monitoring/static/js/async/1514.f953c72e.js monitoring/static/js/async/1514.f953c72e.js
        monitoring/static/js/async/15145.dd7db38c.js monitoring/static/js/async/15145.dd7db38c.js
        monitoring/static/js/async/15367.d8f40316.js monitoring/static/js/async/15367.d8f40316.js
        monitoring/static/js/async/15408.7e573b4b.js monitoring/static/js/async/15408.7e573b4b.js
        monitoring/static/js/async/1557.20ebf8fa.js monitoring/static/js/async/1557.20ebf8fa.js
        monitoring/static/js/async/15640.7d6f2319.js monitoring/static/js/async/15640.7d6f2319.js
        monitoring/static/js/async/15736.36200b96.js monitoring/static/js/async/15736.36200b96.js
        monitoring/static/js/async/15809.69c1aa15.js monitoring/static/js/async/15809.69c1aa15.js
        monitoring/static/js/async/15819.064fb058.js monitoring/static/js/async/15819.064fb058.js
        monitoring/static/js/async/16069.11e227e6.js monitoring/static/js/async/16069.11e227e6.js
        monitoring/static/js/async/16919.e262ebda.js monitoring/static/js/async/16919.e262ebda.js
        monitoring/static/js/async/17161.abdb152f.js monitoring/static/js/async/17161.abdb152f.js
        monitoring/static/js/async/17386.3665b6fe.js monitoring/static/js/async/17386.3665b6fe.js
        monitoring/static/js/async/17533.b5e66525.js monitoring/static/js/async/17533.b5e66525.js
        monitoring/static/js/async/17663.5d7c935e.js monitoring/static/js/async/17663.5d7c935e.js
        monitoring/static/js/async/17663.5d7c935e.js.LICENSE.txt monitoring/static/js/async/17663.5d7c935e.js.LICENSE.txt
        monitoring/static/js/async/18327.5941e8fa.js monitoring/static/js/async/18327.5941e8fa.js
        monitoring/static/js/async/18729.15e40cf1.js monitoring/static/js/async/18729.15e40cf1.js
        monitoring/static/js/async/18842.35d398ab.js monitoring/static/js/async/18842.35d398ab.js
        monitoring/static/js/async/18977.613fadf6.js monitoring/static/js/async/18977.613fadf6.js
        monitoring/static/js/async/18977.613fadf6.js.LICENSE.txt monitoring/static/js/async/18977.613fadf6.js.LICENSE.txt
        monitoring/static/js/async/19133.7e5bb031.js monitoring/static/js/async/19133.7e5bb031.js
        monitoring/static/js/async/19133.7e5bb031.js.LICENSE.txt monitoring/static/js/async/19133.7e5bb031.js.LICENSE.txt
        monitoring/static/js/async/19312.650eeacf.js monitoring/static/js/async/19312.650eeacf.js
        monitoring/static/js/async/19357.567404ca.js monitoring/static/js/async/19357.567404ca.js
        monitoring/static/js/async/19357.567404ca.js.LICENSE.txt monitoring/static/js/async/19357.567404ca.js.LICENSE.txt
        monitoring/static/js/async/19376.ad9c8f70.js monitoring/static/js/async/19376.ad9c8f70.js
        monitoring/static/js/async/19423.b37c77d5.js monitoring/static/js/async/19423.b37c77d5.js
        monitoring/static/js/async/19734.ee4a4a97.js monitoring/static/js/async/19734.ee4a4a97.js
        monitoring/static/js/async/19739.5ffa4e61.js monitoring/static/js/async/19739.5ffa4e61.js
        monitoring/static/js/async/1979.8b95763b.js monitoring/static/js/async/1979.8b95763b.js
        monitoring/static/js/async/19847.322cca42.js monitoring/static/js/async/19847.322cca42.js
        monitoring/static/js/async/20030.b9677ec8.js monitoring/static/js/async/20030.b9677ec8.js
        monitoring/static/js/async/20056.1c5beb03.js monitoring/static/js/async/20056.1c5beb03.js
        monitoring/static/js/async/20300.309c73f1.js monitoring/static/js/async/20300.309c73f1.js
        monitoring/static/js/async/20335.c3f1ed10.js monitoring/static/js/async/20335.c3f1ed10.js
        monitoring/static/js/async/20557.d27e1639.js monitoring/static/js/async/20557.d27e1639.js
        monitoring/static/js/async/20947.2e439a99.js monitoring/static/js/async/20947.2e439a99.js
        monitoring/static/js/async/20983.d7626dd1.js monitoring/static/js/async/20983.d7626dd1.js
        monitoring/static/js/async/21172.904bd1bf.js monitoring/static/js/async/21172.904bd1bf.js
        monitoring/static/js/async/21239.3bd4f2f9.js monitoring/static/js/async/21239.3bd4f2f9.js
        monitoring/static/js/async/21276.27186f77.js monitoring/static/js/async/21276.27186f77.js
        monitoring/static/js/async/21299.922b26e3.js monitoring/static/js/async/21299.922b26e3.js
        monitoring/static/js/async/21299.922b26e3.js.LICENSE.txt monitoring/static/js/async/21299.922b26e3.js.LICENSE.txt
        monitoring/static/js/async/21550.0f2771f4.js monitoring/static/js/async/21550.0f2771f4.js
        monitoring/static/js/async/21678.6cd1c2e8.js monitoring/static/js/async/21678.6cd1c2e8.js
        monitoring/static/js/async/2174.094938cb.js monitoring/static/js/async/2174.094938cb.js
        monitoring/static/js/async/21863.c34f1918.js monitoring/static/js/async/21863.c34f1918.js
        monitoring/static/js/async/21864.036be292.js monitoring/static/js/async/21864.036be292.js
        monitoring/static/js/async/21864.036be292.js.LICENSE.txt monitoring/static/js/async/21864.036be292.js.LICENSE.txt
        monitoring/static/js/async/2199.90b1c925.js monitoring/static/js/async/2199.90b1c925.js
        monitoring/static/js/async/22322.ff416d52.js monitoring/static/js/async/22322.ff416d52.js
        monitoring/static/js/async/22385.1af7fcef.js monitoring/static/js/async/22385.1af7fcef.js
        monitoring/static/js/async/22622.0176d828.js monitoring/static/js/async/22622.0176d828.js
        monitoring/static/js/async/22933.299444fa.js monitoring/static/js/async/22933.299444fa.js
        monitoring/static/js/async/23039.3a4ba7c7.js monitoring/static/js/async/23039.3a4ba7c7.js
        monitoring/static/js/async/23143.cbd4e5f7.js monitoring/static/js/async/23143.cbd4e5f7.js
        monitoring/static/js/async/23267.0cff0c0c.js monitoring/static/js/async/23267.0cff0c0c.js
        monitoring/static/js/async/23267.0cff0c0c.js.LICENSE.txt monitoring/static/js/async/23267.0cff0c0c.js.LICENSE.txt
        monitoring/static/js/async/2339.f0b01444.js monitoring/static/js/async/2339.f0b01444.js
        monitoring/static/js/async/2339.f0b01444.js.LICENSE.txt monitoring/static/js/async/2339.f0b01444.js.LICENSE.txt
        monitoring/static/js/async/23458.f964a304.js monitoring/static/js/async/23458.f964a304.js
        monitoring/static/js/async/24158.36a7595d.js monitoring/static/js/async/24158.36a7595d.js
        monitoring/static/js/async/24358.9dbaf936.js monitoring/static/js/async/24358.9dbaf936.js
        monitoring/static/js/async/24359.6dc92317.js monitoring/static/js/async/24359.6dc92317.js
        monitoring/static/js/async/2440.bc49c576.js monitoring/static/js/async/2440.bc49c576.js
        monitoring/static/js/async/24672.9bb47bd1.js monitoring/static/js/async/24672.9bb47bd1.js
        monitoring/static/js/async/25019.7f036768.js monitoring/static/js/async/25019.7f036768.js
        monitoring/static/js/async/25019.7f036768.js.LICENSE.txt monitoring/static/js/async/25019.7f036768.js.LICENSE.txt
        monitoring/static/js/async/25108.0c4a4735.js monitoring/static/js/async/25108.0c4a4735.js
        monitoring/static/js/async/25316.5f89e6dc.js monitoring/static/js/async/25316.5f89e6dc.js
        monitoring/static/js/async/25391.9433b0a6.js monitoring/static/js/async/25391.9433b0a6.js
        monitoring/static/js/async/25391.9433b0a6.js.LICENSE.txt monitoring/static/js/async/25391.9433b0a6.js.LICENSE.txt
        monitoring/static/js/async/25418.258f5a79.js monitoring/static/js/async/25418.258f5a79.js
        monitoring/static/js/async/25674.2e4fd52b.js monitoring/static/js/async/25674.2e4fd52b.js
        monitoring/static/js/async/2571.663e2569.js monitoring/static/js/async/2571.663e2569.js
        monitoring/static/js/async/2571.663e2569.js.LICENSE.txt monitoring/static/js/async/2571.663e2569.js.LICENSE.txt
        monitoring/static/js/async/25731.d051576e.js monitoring/static/js/async/25731.d051576e.js
        monitoring/static/js/async/26008.4b346d6c.js monitoring/static/js/async/26008.4b346d6c.js
        monitoring/static/js/async/26123.036e8143.js monitoring/static/js/async/26123.036e8143.js
        monitoring/static/js/async/26123.036e8143.js.LICENSE.txt monitoring/static/js/async/26123.036e8143.js.LICENSE.txt
        monitoring/static/js/async/26438.a0f8078a.js monitoring/static/js/async/26438.a0f8078a.js
        monitoring/static/js/async/26497.10975979.js monitoring/static/js/async/26497.10975979.js
        monitoring/static/js/async/26526.ed8a7abe.js monitoring/static/js/async/26526.ed8a7abe.js
        monitoring/static/js/async/26534.2e69e4cc.js monitoring/static/js/async/26534.2e69e4cc.js
        monitoring/static/js/async/26621.b5ce50bf.js monitoring/static/js/async/26621.b5ce50bf.js
        monitoring/static/js/async/26621.b5ce50bf.js.LICENSE.txt monitoring/static/js/async/26621.b5ce50bf.js.LICENSE.txt
        monitoring/static/js/async/26938.0313f03b.js monitoring/static/js/async/26938.0313f03b.js
        monitoring/static/js/async/27012.0060616c.js monitoring/static/js/async/27012.0060616c.js
        monitoring/static/js/async/2749.0e1a8baa.js monitoring/static/js/async/2749.0e1a8baa.js
        monitoring/static/js/async/27518.0551d7a9.js monitoring/static/js/async/27518.0551d7a9.js
        monitoring/static/js/async/27518.0551d7a9.js.LICENSE.txt monitoring/static/js/async/27518.0551d7a9.js.LICENSE.txt
        monitoring/static/js/async/2765.c74e6135.js monitoring/static/js/async/2765.c74e6135.js
        monitoring/static/js/async/27900.607f63b4.js monitoring/static/js/async/27900.607f63b4.js
        monitoring/static/js/async/27980.da2d0100.js monitoring/static/js/async/27980.da2d0100.js
        monitoring/static/js/async/28128.bf88942c.js monitoring/static/js/async/28128.bf88942c.js
        monitoring/static/js/async/2817.9edae78a.js monitoring/static/js/async/2817.9edae78a.js
        monitoring/static/js/async/2817.9edae78a.js.LICENSE.txt monitoring/static/js/async/2817.9edae78a.js.LICENSE.txt
        monitoring/static/js/async/28242.9d6f9d6e.js monitoring/static/js/async/28242.9d6f9d6e.js
        monitoring/static/js/async/28331.297da9fb.js monitoring/static/js/async/28331.297da9fb.js
        monitoring/static/js/async/28442.16288050.js monitoring/static/js/async/28442.16288050.js
        monitoring/static/js/async/28498.d69b2c05.js monitoring/static/js/async/28498.d69b2c05.js
        monitoring/static/js/async/28531.c5c4cec7.js monitoring/static/js/async/28531.c5c4cec7.js
        monitoring/static/js/async/28592.acfa7a9e.js monitoring/static/js/async/28592.acfa7a9e.js
        monitoring/static/js/async/28942.d530bb0e.js monitoring/static/js/async/28942.d530bb0e.js
        monitoring/static/js/async/28959.60bdb2bd.js monitoring/static/js/async/28959.60bdb2bd.js
        monitoring/static/js/async/29032.c5f46b05.js monitoring/static/js/async/29032.c5f46b05.js
        monitoring/static/js/async/29044.c2a4e044.js monitoring/static/js/async/29044.c2a4e044.js
        monitoring/static/js/async/29170.f25e2085.js monitoring/static/js/async/29170.f25e2085.js
        monitoring/static/js/async/29217.eb9f87b6.js monitoring/static/js/async/29217.eb9f87b6.js
        monitoring/static/js/async/29485.78fe5382.js monitoring/static/js/async/29485.78fe5382.js
        monitoring/static/js/async/29546.d66a47cf.js monitoring/static/js/async/29546.d66a47cf.js
        monitoring/static/js/async/3005.d3b3a3b0.js monitoring/static/js/async/3005.d3b3a3b0.js
        monitoring/static/js/async/3005.d3b3a3b0.js.LICENSE.txt monitoring/static/js/async/3005.d3b3a3b0.js.LICENSE.txt
        monitoring/static/js/async/30232.8c42342a.js monitoring/static/js/async/30232.8c42342a.js
        monitoring/static/js/async/30540.531cf8c7.js monitoring/static/js/async/30540.531cf8c7.js
        monitoring/static/js/async/30540.531cf8c7.js.LICENSE.txt monitoring/static/js/async/30540.531cf8c7.js.LICENSE.txt
        monitoring/static/js/async/30713.cf5befe4.js monitoring/static/js/async/30713.cf5befe4.js
        monitoring/static/js/async/30828.5f44c874.js monitoring/static/js/async/30828.5f44c874.js
        monitoring/static/js/async/30889.802b27d9.js monitoring/static/js/async/30889.802b27d9.js
        monitoring/static/js/async/30893.b2788818.js monitoring/static/js/async/30893.b2788818.js
        monitoring/static/js/async/31039.f6eda8d2.js monitoring/static/js/async/31039.f6eda8d2.js
        monitoring/static/js/async/31039.f6eda8d2.js.LICENSE.txt monitoring/static/js/async/31039.f6eda8d2.js.LICENSE.txt
        monitoring/static/js/async/31195.b9680a57.js monitoring/static/js/async/31195.b9680a57.js
        monitoring/static/js/async/31195.b9680a57.js.LICENSE.txt monitoring/static/js/async/31195.b9680a57.js.LICENSE.txt
        monitoring/static/js/async/31229.1d00ec9c.js monitoring/static/js/async/31229.1d00ec9c.js
        monitoring/static/js/async/31516.9e5523cd.js monitoring/static/js/async/31516.9e5523cd.js
        monitoring/static/js/async/31931.c755ba19.js monitoring/static/js/async/31931.c755ba19.js
        monitoring/static/js/async/32239.dd580435.js monitoring/static/js/async/32239.dd580435.js
        monitoring/static/js/async/32276.6abd0548.js monitoring/static/js/async/32276.6abd0548.js
        monitoring/static/js/async/32606.432e0aa7.js monitoring/static/js/async/32606.432e0aa7.js
        monitoring/static/js/async/33071.4560a0ae.js monitoring/static/js/async/33071.4560a0ae.js
        monitoring/static/js/async/33071.4560a0ae.js.LICENSE.txt monitoring/static/js/async/33071.4560a0ae.js.LICENSE.txt
        monitoring/static/js/async/33103.f4f1bc6e.js monitoring/static/js/async/33103.f4f1bc6e.js
        monitoring/static/js/async/33281.f735cff8.js monitoring/static/js/async/33281.f735cff8.js
        monitoring/static/js/async/33382.b60741b2.js monitoring/static/js/async/33382.b60741b2.js
        monitoring/static/js/async/34135.747ad134.js monitoring/static/js/async/34135.747ad134.js
        monitoring/static/js/async/34135.747ad134.js.LICENSE.txt monitoring/static/js/async/34135.747ad134.js.LICENSE.txt
        monitoring/static/js/async/34195.c6de7051.js monitoring/static/js/async/34195.c6de7051.js
        monitoring/static/js/async/34801.bd23a818.js monitoring/static/js/async/34801.bd23a818.js
        monitoring/static/js/async/34930.34eac0f6.js monitoring/static/js/async/34930.34eac0f6.js
        monitoring/static/js/async/35254.5ce3c56a.js monitoring/static/js/async/35254.5ce3c56a.js
        monitoring/static/js/async/35540.b7a99ad9.js monitoring/static/js/async/35540.b7a99ad9.js
        monitoring/static/js/async/35548.84577723.js monitoring/static/js/async/35548.84577723.js
        monitoring/static/js/async/35659.b0672f99.js monitoring/static/js/async/35659.b0672f99.js
        monitoring/static/js/async/35659.b0672f99.js.LICENSE.txt monitoring/static/js/async/35659.b0672f99.js.LICENSE.txt
        monitoring/static/js/async/35773.4887629e.js monitoring/static/js/async/35773.4887629e.js
        monitoring/static/js/async/35863.9bb4e636.js monitoring/static/js/async/35863.9bb4e636.js
        monitoring/static/js/async/35959.e0a5f202.js monitoring/static/js/async/35959.e0a5f202.js
        monitoring/static/js/async/3600.90798a79.js monitoring/static/js/async/3600.90798a79.js
        monitoring/static/js/async/36345.3c2b095b.js monitoring/static/js/async/36345.3c2b095b.js
        monitoring/static/js/async/36765.ac1619d4.js monitoring/static/js/async/36765.ac1619d4.js
        monitoring/static/js/async/36870.7857b32a.js monitoring/static/js/async/36870.7857b32a.js
        monitoring/static/js/async/37164.bed8782b.js monitoring/static/js/async/37164.bed8782b.js
        monitoring/static/js/async/37211.f4d7f789.js monitoring/static/js/async/37211.f4d7f789.js
        monitoring/static/js/async/37272.65bec297.js monitoring/static/js/async/37272.65bec297.js
        monitoring/static/js/async/37459.af5ffddc.js monitoring/static/js/async/37459.af5ffddc.js
        monitoring/static/js/async/37459.af5ffddc.js.LICENSE.txt monitoring/static/js/async/37459.af5ffddc.js.LICENSE.txt
        monitoring/static/js/async/37468.fe9f414b.js monitoring/static/js/async/37468.fe9f414b.js
        monitoring/static/js/async/37851.e936a6b8.js monitoring/static/js/async/37851.e936a6b8.js
        monitoring/static/js/async/38536.b24a052f.js monitoring/static/js/async/38536.b24a052f.js
        monitoring/static/js/async/38749.f5d8b8d3.js monitoring/static/js/async/38749.f5d8b8d3.js
        monitoring/static/js/async/38749.f5d8b8d3.js.LICENSE.txt monitoring/static/js/async/38749.f5d8b8d3.js.LICENSE.txt
        monitoring/static/js/async/38838.ddf5d771.js monitoring/static/js/async/38838.ddf5d771.js
        monitoring/static/js/async/39274.6b808300.js monitoring/static/js/async/39274.6b808300.js
        monitoring/static/js/async/3977.715587c0.js monitoring/static/js/async/3977.715587c0.js
        monitoring/static/js/async/40015.357eeb66.js monitoring/static/js/async/40015.357eeb66.js
        monitoring/static/js/async/40084.37130d0e.js monitoring/static/js/async/40084.37130d0e.js
        monitoring/static/js/async/40115.61cd056e.js monitoring/static/js/async/40115.61cd056e.js
        monitoring/static/js/async/40115.61cd056e.js.LICENSE.txt monitoring/static/js/async/40115.61cd056e.js.LICENSE.txt
        monitoring/static/js/async/40248.5dae1b4b.js monitoring/static/js/async/40248.5dae1b4b.js
        monitoring/static/js/async/40492.fb065c08.js monitoring/static/js/async/40492.fb065c08.js
        monitoring/static/js/async/40637.fad211d6.js monitoring/static/js/async/40637.fad211d6.js
        monitoring/static/js/async/40842.5a2fd3b1.js monitoring/static/js/async/40842.5a2fd3b1.js
        monitoring/static/js/async/41169.4f955ae5.js monitoring/static/js/async/41169.4f955ae5.js
        monitoring/static/js/async/41212.41bde9b1.js monitoring/static/js/async/41212.41bde9b1.js
        monitoring/static/js/async/41329.b1196c30.js monitoring/static/js/async/41329.b1196c30.js
        monitoring/static/js/async/41434.53cfdc71.js monitoring/static/js/async/41434.53cfdc71.js
        monitoring/static/js/async/4145.44c1b7a9.js monitoring/static/js/async/4145.44c1b7a9.js
        monitoring/static/js/async/41463.94a62fae.js monitoring/static/js/async/41463.94a62fae.js
        monitoring/static/js/async/41489.67b4e319.js monitoring/static/js/async/41489.67b4e319.js
        monitoring/static/js/async/41982.53bad7ec.js monitoring/static/js/async/41982.53bad7ec.js
        monitoring/static/js/async/41982.53bad7ec.js.LICENSE.txt monitoring/static/js/async/41982.53bad7ec.js.LICENSE.txt
        monitoring/static/js/async/42042.2877fa32.js monitoring/static/js/async/42042.2877fa32.js
        monitoring/static/js/async/42100.a5f10855.js monitoring/static/js/async/42100.a5f10855.js
        monitoring/static/js/async/42104.50c384c9.js monitoring/static/js/async/42104.50c384c9.js
        monitoring/static/js/async/42446.0087fe16.js monitoring/static/js/async/42446.0087fe16.js
        monitoring/static/js/async/42489.e5ca84d7.js monitoring/static/js/async/42489.e5ca84d7.js
        monitoring/static/js/async/42564.d4fb1f84.js monitoring/static/js/async/42564.d4fb1f84.js
        monitoring/static/js/async/42613.a6551665.js monitoring/static/js/async/42613.a6551665.js
        monitoring/static/js/async/42694.2283704e.js monitoring/static/js/async/42694.2283704e.js
        monitoring/static/js/async/42914.86418ece.js monitoring/static/js/async/42914.86418ece.js
        monitoring/static/js/async/43031.410e5f31.js monitoring/static/js/async/43031.410e5f31.js
        monitoring/static/js/async/43335.34771c33.js monitoring/static/js/async/43335.34771c33.js
        monitoring/static/js/async/43479.3881af6f.js monitoring/static/js/async/43479.3881af6f.js
        monitoring/static/js/async/43646.9b890c13.js monitoring/static/js/async/43646.9b890c13.js
        monitoring/static/js/async/43749.3865f3cc.js monitoring/static/js/async/43749.3865f3cc.js
        monitoring/static/js/async/43815.21902b4b.js monitoring/static/js/async/43815.21902b4b.js
        monitoring/static/js/async/43815.21902b4b.js.LICENSE.txt monitoring/static/js/async/43815.21902b4b.js.LICENSE.txt
        monitoring/static/js/async/4382.1ed00c29.js monitoring/static/js/async/4382.1ed00c29.js
        monitoring/static/js/async/43859.2d562724.js monitoring/static/js/async/43859.2d562724.js
        monitoring/static/js/async/44349.a45e7e92.js monitoring/static/js/async/44349.a45e7e92.js
        monitoring/static/js/async/44473.a478059d.js monitoring/static/js/async/44473.a478059d.js
        monitoring/static/js/async/44594.5145ad9e.js monitoring/static/js/async/44594.5145ad9e.js
        monitoring/static/js/async/45306.6d981d1d.js monitoring/static/js/async/45306.6d981d1d.js
        monitoring/static/js/async/45488.7382d15e.js monitoring/static/js/async/45488.7382d15e.js
        monitoring/static/js/async/45565.f11494bf.js monitoring/static/js/async/45565.f11494bf.js
        monitoring/static/js/async/45565.f11494bf.js.LICENSE.txt monitoring/static/js/async/45565.f11494bf.js.LICENSE.txt
        monitoring/static/js/async/45723.3986a49e.js monitoring/static/js/async/45723.3986a49e.js
        monitoring/static/js/async/45723.3986a49e.js.LICENSE.txt monitoring/static/js/async/45723.3986a49e.js.LICENSE.txt
        monitoring/static/js/async/45850.7c1e2dd1.js monitoring/static/js/async/45850.7c1e2dd1.js
        monitoring/static/js/async/46023.594375ac.js monitoring/static/js/async/46023.594375ac.js
        monitoring/static/js/async/46023.594375ac.js.LICENSE.txt monitoring/static/js/async/46023.594375ac.js.LICENSE.txt
        monitoring/static/js/async/46130.f0b196d5.js monitoring/static/js/async/46130.f0b196d5.js
        monitoring/static/js/async/46332.7a6adbb7.js monitoring/static/js/async/46332.7a6adbb7.js
        monitoring/static/js/async/46346.f3f233ed.js monitoring/static/js/async/46346.f3f233ed.js
        monitoring/static/js/async/46403.9d6b4fbb.js monitoring/static/js/async/46403.9d6b4fbb.js
        monitoring/static/js/async/46542.2816095b.js monitoring/static/js/async/46542.2816095b.js
        monitoring/static/js/async/46635.f4fb6d47.js monitoring/static/js/async/46635.f4fb6d47.js
        monitoring/static/js/async/46704.4a97a70a.js monitoring/static/js/async/46704.4a97a70a.js
        monitoring/static/js/async/46815.9dfac84e.js monitoring/static/js/async/46815.9dfac84e.js
        monitoring/static/js/async/47171.fc127ed1.js monitoring/static/js/async/47171.fc127ed1.js
        monitoring/static/js/async/47173.b189e37f.js monitoring/static/js/async/47173.b189e37f.js
        monitoring/static/js/async/47379.e846e3bd.js monitoring/static/js/async/47379.e846e3bd.js
        monitoring/static/js/async/47379.e846e3bd.js.LICENSE.txt monitoring/static/js/async/47379.e846e3bd.js.LICENSE.txt
        monitoring/static/js/async/47744.ef299837.js monitoring/static/js/async/47744.ef299837.js
        monitoring/static/js/async/48053.67f5901e.js monitoring/static/js/async/48053.67f5901e.js
        monitoring/static/js/async/48269.545773c0.js monitoring/static/js/async/48269.545773c0.js
        monitoring/static/js/async/48349.bf8ea4f9.js monitoring/static/js/async/48349.bf8ea4f9.js
        monitoring/static/js/async/48399.626a5af0.js monitoring/static/js/async/48399.626a5af0.js
        monitoring/static/js/async/48635.b797ede9.js monitoring/static/js/async/48635.b797ede9.js
        monitoring/static/js/async/48736.3e8afbe1.js monitoring/static/js/async/48736.3e8afbe1.js
        monitoring/static/js/async/48924.24cca422.js monitoring/static/js/async/48924.24cca422.js
        monitoring/static/js/async/4907.f7e6a01e.js monitoring/static/js/async/4907.f7e6a01e.js
        monitoring/static/js/async/50192.107430ca.js monitoring/static/js/async/50192.107430ca.js
        monitoring/static/js/async/50242.f84220da.js monitoring/static/js/async/50242.f84220da.js
        monitoring/static/js/async/50331.e6277586.js monitoring/static/js/async/50331.e6277586.js
        monitoring/static/js/async/50375.d70f0853.js monitoring/static/js/async/50375.d70f0853.js
        monitoring/static/js/async/50499.91fd09bd.js monitoring/static/js/async/50499.91fd09bd.js
        monitoring/static/js/async/50499.91fd09bd.js.LICENSE.txt monitoring/static/js/async/50499.91fd09bd.js.LICENSE.txt
        monitoring/static/js/async/5087.b316f527.js monitoring/static/js/async/5087.b316f527.js
        monitoring/static/js/async/5122.e2ac3506.js monitoring/static/js/async/5122.e2ac3506.js
        monitoring/static/js/async/51898.235b8f9f.js monitoring/static/js/async/51898.235b8f9f.js
        monitoring/static/js/async/51906.05bffa52.js monitoring/static/js/async/51906.05bffa52.js
        monitoring/static/js/async/51915.19c88301.js monitoring/static/js/async/51915.19c88301.js
        monitoring/static/js/async/51941.3a3d6dc7.js monitoring/static/js/async/51941.3a3d6dc7.js
        monitoring/static/js/async/51941.3a3d6dc7.js.LICENSE.txt monitoring/static/js/async/51941.3a3d6dc7.js.LICENSE.txt
        monitoring/static/js/async/52078.33bb7736.js monitoring/static/js/async/52078.33bb7736.js
        monitoring/static/js/async/52110.ebae4907.js monitoring/static/js/async/52110.ebae4907.js
        monitoring/static/js/async/52257.aa53f23d.js monitoring/static/js/async/52257.aa53f23d.js
        monitoring/static/js/async/52257.aa53f23d.js.LICENSE.txt monitoring/static/js/async/52257.aa53f23d.js.LICENSE.txt
        monitoring/static/js/async/52270.9fe60058.js monitoring/static/js/async/52270.9fe60058.js
        monitoring/static/js/async/52295.dc5370f7.js monitoring/static/js/async/52295.dc5370f7.js
        monitoring/static/js/async/52388.76402988.js monitoring/static/js/async/52388.76402988.js
        monitoring/static/js/async/52389.a5e6348f.js monitoring/static/js/async/52389.a5e6348f.js
        monitoring/static/js/async/52497.28d97464.js monitoring/static/js/async/52497.28d97464.js
        monitoring/static/js/async/52528.a4b357ed.js monitoring/static/js/async/52528.a4b357ed.js
        monitoring/static/js/async/52567.1e171b98.js monitoring/static/js/async/52567.1e171b98.js
        monitoring/static/js/async/52622.cdced710.js monitoring/static/js/async/52622.cdced710.js
        monitoring/static/js/async/52754.87e260b9.js monitoring/static/js/async/52754.87e260b9.js
        monitoring/static/js/async/52817.3a133d80.js monitoring/static/js/async/52817.3a133d80.js
        monitoring/static/js/async/52817.3a133d80.js.LICENSE.txt monitoring/static/js/async/52817.3a133d80.js.LICENSE.txt
        monitoring/static/js/async/53102.aeee3b6a.js monitoring/static/js/async/53102.aeee3b6a.js
        monitoring/static/js/async/53107.fbf233b2.js monitoring/static/js/async/53107.fbf233b2.js
        monitoring/static/js/async/53107.fbf233b2.js.LICENSE.txt monitoring/static/js/async/53107.fbf233b2.js.LICENSE.txt
        monitoring/static/js/async/53134.19b03022.js monitoring/static/js/async/53134.19b03022.js
        monitoring/static/js/async/5317.8641eb54.js monitoring/static/js/async/5317.8641eb54.js
        monitoring/static/js/async/53260.53a412c0.js monitoring/static/js/async/53260.53a412c0.js
        monitoring/static/js/async/53283.2414009e.js monitoring/static/js/async/53283.2414009e.js
        monitoring/static/js/async/53507.60d069b5.js monitoring/static/js/async/53507.60d069b5.js
        monitoring/static/js/async/53507.60d069b5.js.LICENSE.txt monitoring/static/js/async/53507.60d069b5.js.LICENSE.txt
        monitoring/static/js/async/53523.9a7d5bc5.js monitoring/static/js/async/53523.9a7d5bc5.js
        monitoring/static/js/async/53988.3505f78d.js monitoring/static/js/async/53988.3505f78d.js
        monitoring/static/js/async/54046.eee6769c.js monitoring/static/js/async/54046.eee6769c.js
        monitoring/static/js/async/54096.603d3e27.js monitoring/static/js/async/54096.603d3e27.js
        monitoring/static/js/async/54151.fb125fe8.js monitoring/static/js/async/54151.fb125fe8.js
        monitoring/static/js/async/54223.a5db91db.js monitoring/static/js/async/54223.a5db91db.js
        monitoring/static/js/async/5445.bcb1a399.js monitoring/static/js/async/5445.bcb1a399.js
        monitoring/static/js/async/54767.46034a20.js monitoring/static/js/async/54767.46034a20.js
        monitoring/static/js/async/54867.425cd7f4.js monitoring/static/js/async/54867.425cd7f4.js
        monitoring/static/js/async/54867.425cd7f4.js.LICENSE.txt monitoring/static/js/async/54867.425cd7f4.js.LICENSE.txt
        monitoring/static/js/async/5506.901df532.js monitoring/static/js/async/5506.901df532.js
        monitoring/static/js/async/55262.3286aae1.js monitoring/static/js/async/55262.3286aae1.js
        monitoring/static/js/async/55420.e534c48e.js monitoring/static/js/async/55420.e534c48e.js
        monitoring/static/js/async/55701.8be32335.js monitoring/static/js/async/55701.8be32335.js
        monitoring/static/js/async/55835.b5569471.js monitoring/static/js/async/55835.b5569471.js
        monitoring/static/js/async/56063.d5c563f8.js monitoring/static/js/async/56063.d5c563f8.js
        monitoring/static/js/async/56312.ba126fcd.js monitoring/static/js/async/56312.ba126fcd.js
        monitoring/static/js/async/56420.5d52c507.js monitoring/static/js/async/56420.5d52c507.js
        monitoring/static/js/async/5645.fd8cb458.js monitoring/static/js/async/5645.fd8cb458.js
        monitoring/static/js/async/56660.c45674cb.js monitoring/static/js/async/56660.c45674cb.js
        monitoring/static/js/async/56905.8be0525c.js monitoring/static/js/async/56905.8be0525c.js
        monitoring/static/js/async/57043.6f4239a8.js monitoring/static/js/async/57043.6f4239a8.js
        monitoring/static/js/async/57043.6f4239a8.js.LICENSE.txt monitoring/static/js/async/57043.6f4239a8.js.LICENSE.txt
        monitoring/static/js/async/57566.5920620d.js monitoring/static/js/async/57566.5920620d.js
        monitoring/static/js/async/57615.142f709c.js monitoring/static/js/async/57615.142f709c.js
        monitoring/static/js/async/57865.36e5d43c.js monitoring/static/js/async/57865.36e5d43c.js
        monitoring/static/js/async/58232.ab2b75d8.js monitoring/static/js/async/58232.ab2b75d8.js
        monitoring/static/js/async/58294.afdef38d.js monitoring/static/js/async/58294.afdef38d.js
        monitoring/static/js/async/58516.8d1da251.js monitoring/static/js/async/58516.8d1da251.js
        monitoring/static/js/async/58759.a9d1c206.js monitoring/static/js/async/58759.a9d1c206.js
        monitoring/static/js/async/58935.d654b184.js monitoring/static/js/async/58935.d654b184.js
        monitoring/static/js/async/58935.d654b184.js.LICENSE.txt monitoring/static/js/async/58935.d654b184.js.LICENSE.txt
        monitoring/static/js/async/5903.c7cb0be6.js monitoring/static/js/async/5903.c7cb0be6.js
        monitoring/static/js/async/59043.04cb3fa8.js monitoring/static/js/async/59043.04cb3fa8.js
        monitoring/static/js/async/59043.04cb3fa8.js.LICENSE.txt monitoring/static/js/async/59043.04cb3fa8.js.LICENSE.txt
        monitoring/static/js/async/59046.4f52bf18.js monitoring/static/js/async/59046.4f52bf18.js
        monitoring/static/js/async/59324.2544ef63.js monitoring/static/js/async/59324.2544ef63.js
        monitoring/static/js/async/59411.cae373dc.js monitoring/static/js/async/59411.cae373dc.js
        monitoring/static/js/async/59411.cae373dc.js.LICENSE.txt monitoring/static/js/async/59411.cae373dc.js.LICENSE.txt
        monitoring/static/js/async/59703.73354731.js monitoring/static/js/async/59703.73354731.js
        monitoring/static/js/async/59752.e2b32059.js monitoring/static/js/async/59752.e2b32059.js
        monitoring/static/js/async/5989.b322ecd3.js monitoring/static/js/async/5989.b322ecd3.js
        monitoring/static/js/async/60103.fc41375a.js monitoring/static/js/async/60103.fc41375a.js
        monitoring/static/js/async/60362.8462872f.js monitoring/static/js/async/60362.8462872f.js
        monitoring/static/js/async/60448.c6666ead.js monitoring/static/js/async/60448.c6666ead.js
        monitoring/static/js/async/60474.69d002aa.js monitoring/static/js/async/60474.69d002aa.js
        monitoring/static/js/async/60553.766a1dc8.js monitoring/static/js/async/60553.766a1dc8.js
        monitoring/static/js/async/60635.147b2ff3.js monitoring/static/js/async/60635.147b2ff3.js
        monitoring/static/js/async/60789.21b3410d.js monitoring/static/js/async/60789.21b3410d.js
        monitoring/static/js/async/60791.d9406276.js monitoring/static/js/async/60791.d9406276.js
        monitoring/static/js/async/60919.b81270e6.js monitoring/static/js/async/60919.b81270e6.js
        monitoring/static/js/async/61020.f5011c4d.js monitoring/static/js/async/61020.f5011c4d.js
        monitoring/static/js/async/61052.d8d3d3de.js monitoring/static/js/async/61052.d8d3d3de.js
        monitoring/static/js/async/61436.43591ddd.js monitoring/static/js/async/61436.43591ddd.js
        monitoring/static/js/async/61621.6899e037.js monitoring/static/js/async/61621.6899e037.js
        monitoring/static/js/async/61887.073b4f5d.js monitoring/static/js/async/61887.073b4f5d.js
        monitoring/static/js/async/62258.16f55c87.js monitoring/static/js/async/62258.16f55c87.js
        monitoring/static/js/async/62565.d30d75e9.js monitoring/static/js/async/62565.d30d75e9.js
        monitoring/static/js/async/63309.dd08a775.js monitoring/static/js/async/63309.dd08a775.js
        monitoring/static/js/async/63359.f6fef534.js monitoring/static/js/async/63359.f6fef534.js
        monitoring/static/js/async/6361.e055a317.js monitoring/static/js/async/6361.e055a317.js
        monitoring/static/js/async/63632.0a829520.js monitoring/static/js/async/63632.0a829520.js
        monitoring/static/js/async/63858.217bcf75.js monitoring/static/js/async/63858.217bcf75.js
        monitoring/static/js/async/63858.217bcf75.js.LICENSE.txt monitoring/static/js/async/63858.217bcf75.js.LICENSE.txt
        monitoring/static/js/async/64140.fdafc7c8.js monitoring/static/js/async/64140.fdafc7c8.js
        monitoring/static/js/async/64366.c0fb6f6c.js monitoring/static/js/async/64366.c0fb6f6c.js
        monitoring/static/js/async/64419.c3053aa2.js monitoring/static/js/async/64419.c3053aa2.js
        monitoring/static/js/async/647.fe976119.js monitoring/static/js/async/647.fe976119.js
        monitoring/static/js/async/65104.444c98bb.js monitoring/static/js/async/65104.444c98bb.js
        monitoring/static/js/async/65104.444c98bb.js.LICENSE.txt monitoring/static/js/async/65104.444c98bb.js.LICENSE.txt
        monitoring/static/js/async/65311.e2206300.js monitoring/static/js/async/65311.e2206300.js
        monitoring/static/js/async/6547.9823596d.js monitoring/static/js/async/6547.9823596d.js
        monitoring/static/js/async/6547.9823596d.js.LICENSE.txt monitoring/static/js/async/6547.9823596d.js.LICENSE.txt
        monitoring/static/js/async/65530.7e81e92d.js monitoring/static/js/async/65530.7e81e92d.js
        monitoring/static/js/async/65642.0b92eccd.js monitoring/static/js/async/65642.0b92eccd.js
        monitoring/static/js/async/6579.bb18f608.js monitoring/static/js/async/6579.bb18f608.js
        monitoring/static/js/async/65791.2c942c3f.js monitoring/static/js/async/65791.2c942c3f.js
        monitoring/static/js/async/6580.a8c690a8.js monitoring/static/js/async/6580.a8c690a8.js
        monitoring/static/js/async/65870.124185ad.js monitoring/static/js/async/65870.124185ad.js
        monitoring/static/js/async/65891.0250795a.js monitoring/static/js/async/65891.0250795a.js
        monitoring/static/js/async/65891.0250795a.js.LICENSE.txt monitoring/static/js/async/65891.0250795a.js.LICENSE.txt
        monitoring/static/js/async/66247.dafb143a.js monitoring/static/js/async/66247.dafb143a.js
        monitoring/static/js/async/66247.dafb143a.js.LICENSE.txt monitoring/static/js/async/66247.dafb143a.js.LICENSE.txt
        monitoring/static/js/async/66266.d565870b.js monitoring/static/js/async/66266.d565870b.js
        monitoring/static/js/async/66367.41dac86a.js monitoring/static/js/async/66367.41dac86a.js
        monitoring/static/js/async/66367.41dac86a.js.LICENSE.txt monitoring/static/js/async/66367.41dac86a.js.LICENSE.txt
        monitoring/static/js/async/66579.5d36c151.js monitoring/static/js/async/66579.5d36c151.js
        monitoring/static/js/async/66579.5d36c151.js.LICENSE.txt monitoring/static/js/async/66579.5d36c151.js.LICENSE.txt
        monitoring/static/js/async/66690.ff1f9e29.js monitoring/static/js/async/66690.ff1f9e29.js
        monitoring/static/js/async/66914.2769541b.js monitoring/static/js/async/66914.2769541b.js
        monitoring/static/js/async/67226.7d1b705a.js monitoring/static/js/async/67226.7d1b705a.js
        monitoring/static/js/async/67226.7d1b705a.js.LICENSE.txt monitoring/static/js/async/67226.7d1b705a.js.LICENSE.txt
        monitoring/static/js/async/67264.49065ecb.js monitoring/static/js/async/67264.49065ecb.js
        monitoring/static/js/async/67359.5d50b788.js monitoring/static/js/async/67359.5d50b788.js
        monitoring/static/js/async/67523.947f464c.js monitoring/static/js/async/67523.947f464c.js
        monitoring/static/js/async/67523.947f464c.js.LICENSE.txt monitoring/static/js/async/67523.947f464c.js.LICENSE.txt
        monitoring/static/js/async/67555.982f1075.js monitoring/static/js/async/67555.982f1075.js
        monitoring/static/js/async/67818.0e1c3f30.js monitoring/static/js/async/67818.0e1c3f30.js
        monitoring/static/js/async/68295.c7005d6a.js monitoring/static/js/async/68295.c7005d6a.js
        monitoring/static/js/async/68411.06e40c2b.js monitoring/static/js/async/68411.06e40c2b.js
        monitoring/static/js/async/68411.06e40c2b.js.LICENSE.txt monitoring/static/js/async/68411.06e40c2b.js.LICENSE.txt
        monitoring/static/js/async/68447.67de615f.js monitoring/static/js/async/68447.67de615f.js
        monitoring/static/js/async/68449.d3bad723.js monitoring/static/js/async/68449.d3bad723.js
        monitoring/static/js/async/68624.a12ce77b.js monitoring/static/js/async/68624.a12ce77b.js
        monitoring/static/js/async/68628.814b0145.js monitoring/static/js/async/68628.814b0145.js
        monitoring/static/js/async/68781.77b226ec.js monitoring/static/js/async/68781.77b226ec.js
        monitoring/static/js/async/68851.9485a372.js monitoring/static/js/async/68851.9485a372.js
        monitoring/static/js/async/68851.9485a372.js.LICENSE.txt monitoring/static/js/async/68851.9485a372.js.LICENSE.txt
        monitoring/static/js/async/69057.ea2fd72d.js monitoring/static/js/async/69057.ea2fd72d.js
        monitoring/static/js/async/69075.eaa9b03d.js monitoring/static/js/async/69075.eaa9b03d.js
        monitoring/static/js/async/69075.eaa9b03d.js.LICENSE.txt monitoring/static/js/async/69075.eaa9b03d.js.LICENSE.txt
        monitoring/static/js/async/69208.a750736f.js monitoring/static/js/async/69208.a750736f.js
        monitoring/static/js/async/69295.59f0f84a.js monitoring/static/js/async/69295.59f0f84a.js
        monitoring/static/js/async/70104.720ad971.js monitoring/static/js/async/70104.720ad971.js
        monitoring/static/js/async/70193.6b7f7235.js monitoring/static/js/async/70193.6b7f7235.js
        monitoring/static/js/async/70389.dc6417bf.js monitoring/static/js/async/70389.dc6417bf.js
        monitoring/static/js/async/70630.0cec19cd.js monitoring/static/js/async/70630.0cec19cd.js
        monitoring/static/js/async/70896.7942cdd2.js monitoring/static/js/async/70896.7942cdd2.js
        monitoring/static/js/async/70950.fc97cb30.js monitoring/static/js/async/70950.fc97cb30.js
        monitoring/static/js/async/70968.a3779b98.js monitoring/static/js/async/70968.a3779b98.js
        monitoring/static/js/async/71182.0d89d016.js monitoring/static/js/async/71182.0d89d016.js
        monitoring/static/js/async/7141.ad8aad88.js monitoring/static/js/async/7141.ad8aad88.js
        monitoring/static/js/async/71613.05db1b0c.js monitoring/static/js/async/71613.05db1b0c.js
        monitoring/static/js/async/71667.30d83f89.js monitoring/static/js/async/71667.30d83f89.js
        monitoring/static/js/async/71675.a0fc41d6.js monitoring/static/js/async/71675.a0fc41d6.js
        monitoring/static/js/async/71846.390464d0.js monitoring/static/js/async/71846.390464d0.js
        monitoring/static/js/async/71986.88c6bf1c.js monitoring/static/js/async/71986.88c6bf1c.js
        monitoring/static/js/async/72131.b75aab2f.js monitoring/static/js/async/72131.b75aab2f.js
        monitoring/static/js/async/72239.7907ae58.js monitoring/static/js/async/72239.7907ae58.js
        monitoring/static/js/async/72443.5ec7104c.js monitoring/static/js/async/72443.5ec7104c.js
        monitoring/static/js/async/7245.9b1f9221.js monitoring/static/js/async/7245.9b1f9221.js
        monitoring/static/js/async/73028.72d2ec43.js monitoring/static/js/async/73028.72d2ec43.js
        monitoring/static/js/async/73235.626e20b4.js monitoring/static/js/async/73235.626e20b4.js
        monitoring/static/js/async/73235.626e20b4.js.LICENSE.txt monitoring/static/js/async/73235.626e20b4.js.LICENSE.txt
        monitoring/static/js/async/73431.db032007.js monitoring/static/js/async/73431.db032007.js
        monitoring/static/js/async/73526.8bbc0ffb.js monitoring/static/js/async/73526.8bbc0ffb.js
        monitoring/static/js/async/7362.8d93311e.js monitoring/static/js/async/7362.8d93311e.js
        monitoring/static/js/async/73877.7b0863ae.js monitoring/static/js/async/73877.7b0863ae.js
        monitoring/static/js/async/73895.622eebd9.js monitoring/static/js/async/73895.622eebd9.js
        monitoring/static/js/async/73978.e588176c.js monitoring/static/js/async/73978.e588176c.js
        monitoring/static/js/async/73983.79adefe7.js monitoring/static/js/async/73983.79adefe7.js
        monitoring/static/js/async/73983.79adefe7.js.LICENSE.txt monitoring/static/js/async/73983.79adefe7.js.LICENSE.txt
        monitoring/static/js/async/74120.d3f0c9d2.js monitoring/static/js/async/74120.d3f0c9d2.js
        monitoring/static/js/async/7413.467b0bf9.js monitoring/static/js/async/7413.467b0bf9.js
        monitoring/static/js/async/74587.7242d7e6.js monitoring/static/js/async/74587.7242d7e6.js
        monitoring/static/js/async/74707.a818f306.js monitoring/static/js/async/74707.a818f306.js
        monitoring/static/js/async/74707.a818f306.js.LICENSE.txt monitoring/static/js/async/74707.a818f306.js.LICENSE.txt
        monitoring/static/js/async/74742.3c873292.js monitoring/static/js/async/74742.3c873292.js
        monitoring/static/js/async/74784.c2248024.js monitoring/static/js/async/74784.c2248024.js
        monitoring/static/js/async/75308.3f92647d.js monitoring/static/js/async/75308.3f92647d.js
        monitoring/static/js/async/75768.91febbfa.js monitoring/static/js/async/75768.91febbfa.js
        monitoring/static/js/async/76241.3297360c.js monitoring/static/js/async/76241.3297360c.js
        monitoring/static/js/async/76306.e28a1353.js monitoring/static/js/async/76306.e28a1353.js
        monitoring/static/js/async/76436.c4a5ca76.js monitoring/static/js/async/76436.c4a5ca76.js
        monitoring/static/js/async/76935.7721ea01.js monitoring/static/js/async/76935.7721ea01.js
        monitoring/static/js/async/77031.e7afa867.js monitoring/static/js/async/77031.e7afa867.js
        monitoring/static/js/async/77095.515bbab2.js monitoring/static/js/async/77095.515bbab2.js
        monitoring/static/js/async/77099.694f54e7.js monitoring/static/js/async/77099.694f54e7.js
        monitoring/static/js/async/77219.7650ddb8.js monitoring/static/js/async/77219.7650ddb8.js
        monitoring/static/js/async/77219.7650ddb8.js.LICENSE.txt monitoring/static/js/async/77219.7650ddb8.js.LICENSE.txt
        monitoring/static/js/async/77344.c0d0140a.js monitoring/static/js/async/77344.c0d0140a.js
        monitoring/static/js/async/77406.14f2d45d.js monitoring/static/js/async/77406.14f2d45d.js
        monitoring/static/js/async/77669.146129f1.js monitoring/static/js/async/77669.146129f1.js
        monitoring/static/js/async/7791.37de197d.js monitoring/static/js/async/7791.37de197d.js
        monitoring/static/js/async/7791.37de197d.js.LICENSE.txt monitoring/static/js/async/7791.37de197d.js.LICENSE.txt
        monitoring/static/js/async/77981.fd128195.js monitoring/static/js/async/77981.fd128195.js
        monitoring/static/js/async/78087.5a62946b.js monitoring/static/js/async/78087.5a62946b.js
        monitoring/static/js/async/78138.7e0c74ee.js monitoring/static/js/async/78138.7e0c74ee.js
        monitoring/static/js/async/78250.43b6f66f.js monitoring/static/js/async/78250.43b6f66f.js
        monitoring/static/js/async/78252.44b63dec.js monitoring/static/js/async/78252.44b63dec.js
        monitoring/static/js/async/78499.928be91f.js monitoring/static/js/async/78499.928be91f.js
        monitoring/static/js/async/78499.928be91f.js.LICENSE.txt monitoring/static/js/async/78499.928be91f.js.LICENSE.txt
        monitoring/static/js/async/78631.a6db412b.js monitoring/static/js/async/78631.a6db412b.js
        monitoring/static/js/async/78919.ead306b9.js monitoring/static/js/async/78919.ead306b9.js
        monitoring/static/js/async/78919.ead306b9.js.LICENSE.txt monitoring/static/js/async/78919.ead306b9.js.LICENSE.txt
        monitoring/static/js/async/79363.1e6a2c77.js monitoring/static/js/async/79363.1e6a2c77.js
        monitoring/static/js/async/79363.1e6a2c77.js.LICENSE.txt monitoring/static/js/async/79363.1e6a2c77.js.LICENSE.txt
        monitoring/static/js/async/79482.be608073.js monitoring/static/js/async/79482.be608073.js
        monitoring/static/js/async/79517.69d19eb4.js monitoring/static/js/async/79517.69d19eb4.js
        monitoring/static/js/async/79953.2dfe0612.js monitoring/static/js/async/79953.2dfe0612.js
        monitoring/static/js/async/80125.9d5b97ba.js monitoring/static/js/async/80125.9d5b97ba.js
        monitoring/static/js/async/80332.4c1d6b66.js monitoring/static/js/async/80332.4c1d6b66.js
        monitoring/static/js/async/80360.67b32b8a.js monitoring/static/js/async/80360.67b32b8a.js
        monitoring/static/js/async/80401.5925925c.js monitoring/static/js/async/80401.5925925c.js
        monitoring/static/js/async/80406.c6bbf2ee.js monitoring/static/js/async/80406.c6bbf2ee.js
        monitoring/static/js/async/80565.441480f1.js monitoring/static/js/async/80565.441480f1.js
        monitoring/static/js/async/80774.15816915.js monitoring/static/js/async/80774.15816915.js
        monitoring/static/js/async/81021.670346c8.js monitoring/static/js/async/81021.670346c8.js
        monitoring/static/js/async/81047.88ae886c.js monitoring/static/js/async/81047.88ae886c.js
        monitoring/static/js/async/81350.4eb89a4a.js monitoring/static/js/async/81350.4eb89a4a.js
        monitoring/static/js/async/81387.e9f3296b.js monitoring/static/js/async/81387.e9f3296b.js
        monitoring/static/js/async/81387.e9f3296b.js.LICENSE.txt monitoring/static/js/async/81387.e9f3296b.js.LICENSE.txt
        monitoring/static/js/async/81481.da5e152a.js monitoring/static/js/async/81481.da5e152a.js
        monitoring/static/js/async/81586.e29cd057.js monitoring/static/js/async/81586.e29cd057.js
        monitoring/static/js/async/81708.92582b94.js monitoring/static/js/async/81708.92582b94.js
        monitoring/static/js/async/82004.472b5b11.js monitoring/static/js/async/82004.472b5b11.js
        monitoring/static/js/async/82357.7ed19ece.js monitoring/static/js/async/82357.7ed19ece.js
        monitoring/static/js/async/82415.5dfd0267.js monitoring/static/js/async/82415.5dfd0267.js
        monitoring/static/js/async/82601.2b25facb.js monitoring/static/js/async/82601.2b25facb.js
        monitoring/static/js/async/82601.2b25facb.js.LICENSE.txt monitoring/static/js/async/82601.2b25facb.js.LICENSE.txt
        monitoring/static/js/async/82913.0c9bc5f3.js monitoring/static/js/async/82913.0c9bc5f3.js
        monitoring/static/js/async/83093.56c32821.js monitoring/static/js/async/83093.56c32821.js
        monitoring/static/js/async/8316.df6e9fdb.js monitoring/static/js/async/8316.df6e9fdb.js
        monitoring/static/js/async/8339.7e97c3ae.js monitoring/static/js/async/8339.7e97c3ae.js
        monitoring/static/js/async/8339.7e97c3ae.js.LICENSE.txt monitoring/static/js/async/8339.7e97c3ae.js.LICENSE.txt
        monitoring/static/js/async/83396.ceae836d.js monitoring/static/js/async/83396.ceae836d.js
        monitoring/static/js/async/8368.2fb3ddce.js monitoring/static/js/async/8368.2fb3ddce.js
        monitoring/static/js/async/83898.c572cdc9.js monitoring/static/js/async/83898.c572cdc9.js
        monitoring/static/js/async/84199.c57a7c9d.js monitoring/static/js/async/84199.c57a7c9d.js
        monitoring/static/js/async/8451.2c2bb692.js monitoring/static/js/async/8451.2c2bb692.js
        monitoring/static/js/async/8473.f9d59fd9.js monitoring/static/js/async/8473.f9d59fd9.js
        monitoring/static/js/async/8489.2d70b07b.js monitoring/static/js/async/8489.2d70b07b.js
        monitoring/static/js/async/85179.64625fbe.js monitoring/static/js/async/85179.64625fbe.js
        monitoring/static/js/async/85179.64625fbe.js.LICENSE.txt monitoring/static/js/async/85179.64625fbe.js.LICENSE.txt
        monitoring/static/js/async/85459.a0b3fb2e.js monitoring/static/js/async/85459.a0b3fb2e.js
        monitoring/static/js/async/85532.f69c0ada.js monitoring/static/js/async/85532.f69c0ada.js
        monitoring/static/js/async/85567.fe117e2b.js monitoring/static/js/async/85567.fe117e2b.js
        monitoring/static/js/async/85658.cd0fb0ba.js monitoring/static/js/async/85658.cd0fb0ba.js
        monitoring/static/js/async/85671.75179534.js monitoring/static/js/async/85671.75179534.js
        monitoring/static/js/async/85813.ed4f1a89.js monitoring/static/js/async/85813.ed4f1a89.js
        monitoring/static/js/async/85813.ed4f1a89.js.LICENSE.txt monitoring/static/js/async/85813.ed4f1a89.js.LICENSE.txt
        monitoring/static/js/async/85881.628d3965.js monitoring/static/js/async/85881.628d3965.js
        monitoring/static/js/async/85905.2c5158b9.js monitoring/static/js/async/85905.2c5158b9.js
        monitoring/static/js/async/86127.55074c80.js monitoring/static/js/async/86127.55074c80.js
        monitoring/static/js/async/8619.59a7ffaa.js monitoring/static/js/async/8619.59a7ffaa.js
        monitoring/static/js/async/8619.59a7ffaa.js.LICENSE.txt monitoring/static/js/async/8619.59a7ffaa.js.LICENSE.txt
        monitoring/static/js/async/86222.8243d105.js monitoring/static/js/async/86222.8243d105.js
        monitoring/static/js/async/8627.800cb5f4.js monitoring/static/js/async/8627.800cb5f4.js
        monitoring/static/js/async/86617.a0bfb2b2.js monitoring/static/js/async/86617.a0bfb2b2.js
        monitoring/static/js/async/8708.ab8ff876.js monitoring/static/js/async/8708.ab8ff876.js
        monitoring/static/js/async/87453.ce7bf7b2.js monitoring/static/js/async/87453.ce7bf7b2.js
        monitoring/static/js/async/87507.9e8775bd.js monitoring/static/js/async/87507.9e8775bd.js
        monitoring/static/js/async/87571.71a52701.js monitoring/static/js/async/87571.71a52701.js
        monitoring/static/js/async/87571.71a52701.js.LICENSE.txt monitoring/static/js/async/87571.71a52701.js.LICENSE.txt
        monitoring/static/js/async/87625.9835cdab.js monitoring/static/js/async/87625.9835cdab.js
        monitoring/static/js/async/87709.3c74a28e.js monitoring/static/js/async/87709.3c74a28e.js
        monitoring/static/js/async/87841.aeeb44fd.js monitoring/static/js/async/87841.aeeb44fd.js
        monitoring/static/js/async/87841.aeeb44fd.js.LICENSE.txt monitoring/static/js/async/87841.aeeb44fd.js.LICENSE.txt
        monitoring/static/js/async/87924.954e5553.js monitoring/static/js/async/87924.954e5553.js
        monitoring/static/js/async/88217.7f3ac612.js monitoring/static/js/async/88217.7f3ac612.js
        monitoring/static/js/async/88295.58ec3e18.js monitoring/static/js/async/88295.58ec3e18.js
        monitoring/static/js/async/88331.33d54523.js monitoring/static/js/async/88331.33d54523.js
        monitoring/static/js/async/88331.33d54523.js.LICENSE.txt monitoring/static/js/async/88331.33d54523.js.LICENSE.txt
        monitoring/static/js/async/88342.5a802834.js monitoring/static/js/async/88342.5a802834.js
        monitoring/static/js/async/88366.919e9b7d.js monitoring/static/js/async/88366.919e9b7d.js
        monitoring/static/js/async/88403.2879da05.js monitoring/static/js/async/88403.2879da05.js
        monitoring/static/js/async/8853.d085c1d9.js monitoring/static/js/async/8853.d085c1d9.js
        monitoring/static/js/async/88676.d6172fe7.js monitoring/static/js/async/88676.d6172fe7.js
        monitoring/static/js/async/88748.95778572.js monitoring/static/js/async/88748.95778572.js
        monitoring/static/js/async/88887.15d853c1.js monitoring/static/js/async/88887.15d853c1.js
        monitoring/static/js/async/88887.15d853c1.js.LICENSE.txt monitoring/static/js/async/88887.15d853c1.js.LICENSE.txt
        monitoring/static/js/async/89004.2da9a234.js monitoring/static/js/async/89004.2da9a234.js
        monitoring/static/js/async/89011.1409f7e5.js monitoring/static/js/async/89011.1409f7e5.js
        monitoring/static/js/async/89203.34787c62.js monitoring/static/js/async/89203.34787c62.js
        monitoring/static/js/async/89245.ce0f653c.js monitoring/static/js/async/89245.ce0f653c.js
        monitoring/static/js/async/89289.15879cb9.js monitoring/static/js/async/89289.15879cb9.js
        monitoring/static/js/async/89289.15879cb9.js.LICENSE.txt monitoring/static/js/async/89289.15879cb9.js.LICENSE.txt
        monitoring/static/js/async/89565.8d112c2a.js monitoring/static/js/async/89565.8d112c2a.js
        monitoring/static/js/async/89738.c33dc904.js monitoring/static/js/async/89738.c33dc904.js
        monitoring/static/js/async/89918.b058fd85.js monitoring/static/js/async/89918.b058fd85.js
        monitoring/static/js/async/90058.a8513089.js monitoring/static/js/async/90058.a8513089.js
        monitoring/static/js/async/90070.dac17c83.js monitoring/static/js/async/90070.dac17c83.js
        monitoring/static/js/async/90093.5cbde80d.js monitoring/static/js/async/90093.5cbde80d.js
        monitoring/static/js/async/9021.c20c7dd7.js monitoring/static/js/async/9021.c20c7dd7.js
        monitoring/static/js/async/90219.557a2382.js monitoring/static/js/async/90219.557a2382.js
        monitoring/static/js/async/9065.36448dc7.js monitoring/static/js/async/9065.36448dc7.js
        monitoring/static/js/async/90783.8aa44e6e.js monitoring/static/js/async/90783.8aa44e6e.js
        monitoring/static/js/async/90783.8aa44e6e.js.LICENSE.txt monitoring/static/js/async/90783.8aa44e6e.js.LICENSE.txt
        monitoring/static/js/async/90958.d5e2826a.js monitoring/static/js/async/90958.d5e2826a.js
        monitoring/static/js/async/91087.f4f6d9ab.js monitoring/static/js/async/91087.f4f6d9ab.js
        monitoring/static/js/async/91255.07bd817d.js monitoring/static/js/async/91255.07bd817d.js
        monitoring/static/js/async/91798.f0fc299d.js monitoring/static/js/async/91798.f0fc299d.js
        monitoring/static/js/async/91848.263708b3.js monitoring/static/js/async/91848.263708b3.js
        monitoring/static/js/async/92011.60786522.js monitoring/static/js/async/92011.60786522.js
        monitoring/static/js/async/92013.0ba738d1.js monitoring/static/js/async/92013.0ba738d1.js
        monitoring/static/js/async/92301.f7a5cfc6.js monitoring/static/js/async/92301.f7a5cfc6.js
        monitoring/static/js/async/92470.706d9c61.js monitoring/static/js/async/92470.706d9c61.js
        monitoring/static/js/async/9248.d343ad7b.js monitoring/static/js/async/9248.d343ad7b.js
        monitoring/static/js/async/93059.8ab8500d.js monitoring/static/js/async/93059.8ab8500d.js
        monitoring/static/js/async/93641.f5ea8a3d.js monitoring/static/js/async/93641.f5ea8a3d.js
        monitoring/static/js/async/93647.e7b80a1f.js monitoring/static/js/async/93647.e7b80a1f.js
        monitoring/static/js/async/93647.e7b80a1f.js.LICENSE.txt monitoring/static/js/async/93647.e7b80a1f.js.LICENSE.txt
        monitoring/static/js/async/94056.fbb7f497.js monitoring/static/js/async/94056.fbb7f497.js
        monitoring/static/js/async/94076.490b9f13.js monitoring/static/js/async/94076.490b9f13.js
        monitoring/static/js/async/94101.4c74aa2d.js monitoring/static/js/async/94101.4c74aa2d.js
        monitoring/static/js/async/94160.f059c8b3.js monitoring/static/js/async/94160.f059c8b3.js
        monitoring/static/js/async/94299.480dd293.js monitoring/static/js/async/94299.480dd293.js
        monitoring/static/js/async/94324.eb6b29d8.js monitoring/static/js/async/94324.eb6b29d8.js
        monitoring/static/js/async/94456.b9b1b5e8.js monitoring/static/js/async/94456.b9b1b5e8.js
        monitoring/static/js/async/94467.406289f2.js monitoring/static/js/async/94467.406289f2.js
        monitoring/static/js/async/94467.406289f2.js.LICENSE.txt monitoring/static/js/async/94467.406289f2.js.LICENSE.txt
        monitoring/static/js/async/94686.93a7d6b3.js monitoring/static/js/async/94686.93a7d6b3.js
        monitoring/static/js/async/9475.29254d18.js monitoring/static/js/async/9475.29254d18.js
        monitoring/static/js/async/9475.29254d18.js.LICENSE.txt monitoring/static/js/async/9475.29254d18.js.LICENSE.txt
        monitoring/static/js/async/9485.0b93e3c2.js monitoring/static/js/async/9485.0b93e3c2.js
        monitoring/static/js/async/94985.70b3f8c6.js monitoring/static/js/async/94985.70b3f8c6.js
        monitoring/static/js/async/95123.dce72fdf.js monitoring/static/js/async/95123.dce72fdf.js
        monitoring/static/js/async/95135.80580f3c.js monitoring/static/js/async/95135.80580f3c.js
        monitoring/static/js/async/95178.bc4b3415.js monitoring/static/js/async/95178.bc4b3415.js
        monitoring/static/js/async/9518.93b9e24a.js monitoring/static/js/async/9518.93b9e24a.js
        monitoring/static/js/async/95218.6ad3da80.js monitoring/static/js/async/95218.6ad3da80.js
        monitoring/static/js/async/95325.93077db6.js monitoring/static/js/async/95325.93077db6.js
        monitoring/static/js/async/95495.9f9f2b6a.js monitoring/static/js/async/95495.9f9f2b6a.js
        monitoring/static/js/async/95495.9f9f2b6a.js.LICENSE.txt monitoring/static/js/async/95495.9f9f2b6a.js.LICENSE.txt
        monitoring/static/js/async/95591.a0708881.js monitoring/static/js/async/95591.a0708881.js
        monitoring/static/js/async/95591.a0708881.js.LICENSE.txt monitoring/static/js/async/95591.a0708881.js.LICENSE.txt
        monitoring/static/js/async/9578.4ca38be7.js monitoring/static/js/async/9578.4ca38be7.js
        monitoring/static/js/async/95843.44399653.js monitoring/static/js/async/95843.44399653.js
        monitoring/static/js/async/95843.44399653.js.LICENSE.txt monitoring/static/js/async/95843.44399653.js.LICENSE.txt
        monitoring/static/js/async/95933.bf052b36.js monitoring/static/js/async/95933.bf052b36.js
        monitoring/static/js/async/96008.9f775b10.js monitoring/static/js/async/96008.9f775b10.js
        monitoring/static/js/async/9605.04c0b98a.js monitoring/static/js/async/9605.04c0b98a.js
        monitoring/static/js/async/96062.7e446830.js monitoring/static/js/async/96062.7e446830.js
        monitoring/static/js/async/96093.8660117a.js monitoring/static/js/async/96093.8660117a.js
        monitoring/static/js/async/96239.5249a026.js monitoring/static/js/async/96239.5249a026.js
        monitoring/static/js/async/96459.938d6174.js monitoring/static/js/async/96459.938d6174.js
        monitoring/static/js/async/96955.eb2fe38f.js monitoring/static/js/async/96955.eb2fe38f.js
        monitoring/static/js/async/96981.46663e95.js monitoring/static/js/async/96981.46663e95.js
        monitoring/static/js/async/97132.dc0c126f.js monitoring/static/js/async/97132.dc0c126f.js
        monitoring/static/js/async/97137.6782a53e.js monitoring/static/js/async/97137.6782a53e.js
        monitoring/static/js/async/97137.6782a53e.js.LICENSE.txt monitoring/static/js/async/97137.6782a53e.js.LICENSE.txt
        monitoring/static/js/async/97263.e3e753e9.js monitoring/static/js/async/97263.e3e753e9.js
        monitoring/static/js/async/97318.89b5213a.js monitoring/static/js/async/97318.89b5213a.js
        monitoring/static/js/async/97429.65a2eb89.js monitoring/static/js/async/97429.65a2eb89.js
        monitoring/static/js/async/97429.65a2eb89.js.LICENSE.txt monitoring/static/js/async/97429.65a2eb89.js.LICENSE.txt
        monitoring/static/js/async/97448.5d447d18.js monitoring/static/js/async/97448.5d447d18.js
        monitoring/static/js/async/97608.23491b05.js monitoring/static/js/async/97608.23491b05.js
        monitoring/static/js/async/97915.1bb9d6e9.js monitoring/static/js/async/97915.1bb9d6e9.js
        monitoring/static/js/async/98480.a919da49.js monitoring/static/js/async/98480.a919da49.js
        monitoring/static/js/async/98865.10f55171.js monitoring/static/js/async/98865.10f55171.js
        monitoring/static/js/async/98908.c4d5f683.js monitoring/static/js/async/98908.c4d5f683.js
        monitoring/static/js/async/99036.e9df39dd.js monitoring/static/js/async/99036.e9df39dd.js
        monitoring/static/js/async/99204.9f838adb.js monitoring/static/js/async/99204.9f838adb.js
        monitoring/static/js/async/99584.30e1ba68.js monitoring/static/js/async/99584.30e1ba68.js
        monitoring/static/js/index.739dd56a.js monitoring/static/js/index.739dd56a.js
        monitoring/static/js/lib-axios.3ce68a69.js monitoring/static/js/lib-axios.3ce68a69.js
        monitoring/static/js/lib-react.4a71e642.js monitoring/static/js/lib-react.4a71e642.js
        monitoring/static/js/lib-react.4a71e642.js.LICENSE.txt monitoring/static/js/lib-react.4a71e642.js.LICENSE.txt
        monitoring/static/js/lib-router.2d396e8c.js monitoring/static/js/lib-router.2d396e8c.js
        monitoring/static/js/lib-router.2d396e8c.js.LICENSE.txt monitoring/static/js/lib-router.2d396e8c.js.LICENSE.txt
        monitoring/static/media/codicon.dfc1b1db.ttf monitoring/static/media/codicon.dfc1b1db.ttf
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
