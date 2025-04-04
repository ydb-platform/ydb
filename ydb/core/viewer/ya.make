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
    viewer_query_old.h
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
        monitoring/asset-manifest.json monitoring/asset-manifest.json
        monitoring/css.worker.js monitoring/css.worker.js
        monitoring/css.worker.js.LICENSE.txt monitoring/css.worker.js.LICENSE.txt
        monitoring/editor.worker.js monitoring/editor.worker.js
        monitoring/html.worker.js monitoring/html.worker.js
        monitoring/index.html monitoring/index.html
        monitoring/json.worker.js monitoring/json.worker.js
        monitoring/json.worker.js.LICENSE.txt monitoring/json.worker.js.LICENSE.txt
        monitoring/static/css/2031.1e565ac5.chunk.css monitoring/static/css/2031.1e565ac5.chunk.css
        monitoring/static/css/3779.66c0ef83.chunk.css monitoring/static/css/3779.66c0ef83.chunk.css
        monitoring/static/css/3812.440ff245.chunk.css monitoring/static/css/3812.440ff245.chunk.css
        monitoring/static/css/431.3826ea2f.chunk.css monitoring/static/css/431.3826ea2f.chunk.css
        monitoring/static/css/5438.615bd68a.chunk.css monitoring/static/css/5438.615bd68a.chunk.css
        monitoring/static/css/5652.261c3a7a.chunk.css monitoring/static/css/5652.261c3a7a.chunk.css
        monitoring/static/css/6030.05f92b81.chunk.css monitoring/static/css/6030.05f92b81.chunk.css
        monitoring/static/css/684.a4d29f42.chunk.css monitoring/static/css/684.a4d29f42.chunk.css
        monitoring/static/css/8393.09a78946.chunk.css monitoring/static/css/8393.09a78946.chunk.css
        monitoring/static/css/8593.95df9723.chunk.css monitoring/static/css/8593.95df9723.chunk.css
        monitoring/static/css/9393.c6fb8cd9.chunk.css monitoring/static/css/9393.c6fb8cd9.chunk.css
        monitoring/static/css/9796.828f7385.chunk.css monitoring/static/css/9796.828f7385.chunk.css
        monitoring/static/css/9802.d30d0ff3.chunk.css monitoring/static/css/9802.d30d0ff3.chunk.css
        monitoring/static/css/99.7cffb936.chunk.css monitoring/static/css/99.7cffb936.chunk.css
        monitoring/static/css/main.8f03af14.css monitoring/static/css/main.8f03af14.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1053.f976b474.chunk.js monitoring/static/js/1053.f976b474.chunk.js
        monitoring/static/js/108.d2d9c180.chunk.js monitoring/static/js/108.d2d9c180.chunk.js
        monitoring/static/js/1088.40c16ea2.chunk.js monitoring/static/js/1088.40c16ea2.chunk.js
        monitoring/static/js/1094.b5bb2475.chunk.js monitoring/static/js/1094.b5bb2475.chunk.js
        monitoring/static/js/1094.b5bb2475.chunk.js.LICENSE.txt monitoring/static/js/1094.b5bb2475.chunk.js.LICENSE.txt
        monitoring/static/js/110.2c798565.chunk.js monitoring/static/js/110.2c798565.chunk.js
        monitoring/static/js/110.2c798565.chunk.js.LICENSE.txt monitoring/static/js/110.2c798565.chunk.js.LICENSE.txt
        monitoring/static/js/118.112f7e2f.chunk.js monitoring/static/js/118.112f7e2f.chunk.js
        monitoring/static/js/118.112f7e2f.chunk.js.LICENSE.txt monitoring/static/js/118.112f7e2f.chunk.js.LICENSE.txt
        monitoring/static/js/1187.c5435886.chunk.js monitoring/static/js/1187.c5435886.chunk.js
        monitoring/static/js/1237.328d0241.chunk.js monitoring/static/js/1237.328d0241.chunk.js
        monitoring/static/js/1278.d5c24e15.chunk.js monitoring/static/js/1278.d5c24e15.chunk.js
        monitoring/static/js/1278.d5c24e15.chunk.js.LICENSE.txt monitoring/static/js/1278.d5c24e15.chunk.js.LICENSE.txt
        monitoring/static/js/1299.aaedb42e.chunk.js monitoring/static/js/1299.aaedb42e.chunk.js
        monitoring/static/js/132.cf00f1e7.chunk.js monitoring/static/js/132.cf00f1e7.chunk.js
        monitoring/static/js/132.cf00f1e7.chunk.js.LICENSE.txt monitoring/static/js/132.cf00f1e7.chunk.js.LICENSE.txt
        monitoring/static/js/1321.401aa9b8.chunk.js monitoring/static/js/1321.401aa9b8.chunk.js
        monitoring/static/js/1321.401aa9b8.chunk.js.LICENSE.txt monitoring/static/js/1321.401aa9b8.chunk.js.LICENSE.txt
        monitoring/static/js/1329.d6e31925.chunk.js monitoring/static/js/1329.d6e31925.chunk.js
        monitoring/static/js/1414.4cfda0bc.chunk.js monitoring/static/js/1414.4cfda0bc.chunk.js
        monitoring/static/js/1414.4cfda0bc.chunk.js.LICENSE.txt monitoring/static/js/1414.4cfda0bc.chunk.js.LICENSE.txt
        monitoring/static/js/1486.8a488ae4.chunk.js monitoring/static/js/1486.8a488ae4.chunk.js
        monitoring/static/js/1496.18b1eb19.chunk.js monitoring/static/js/1496.18b1eb19.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js monitoring/static/js/1606.0041ed7d.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt
        monitoring/static/js/1627.80c94ebf.chunk.js monitoring/static/js/1627.80c94ebf.chunk.js
        monitoring/static/js/1657.d6f0b340.chunk.js monitoring/static/js/1657.d6f0b340.chunk.js
        monitoring/static/js/1696.6120f4a8.chunk.js monitoring/static/js/1696.6120f4a8.chunk.js
        monitoring/static/js/1747.f01c9fd8.chunk.js monitoring/static/js/1747.f01c9fd8.chunk.js
        monitoring/static/js/1756.b612458a.chunk.js monitoring/static/js/1756.b612458a.chunk.js
        monitoring/static/js/1836.1a1c6d4b.chunk.js monitoring/static/js/1836.1a1c6d4b.chunk.js
        monitoring/static/js/1865.72c933c8.chunk.js monitoring/static/js/1865.72c933c8.chunk.js
        monitoring/static/js/1917.e3ac9c6d.chunk.js monitoring/static/js/1917.e3ac9c6d.chunk.js
        monitoring/static/js/1956.c11f9b8a.chunk.js monitoring/static/js/1956.c11f9b8a.chunk.js
        monitoring/static/js/1975.e3066826.chunk.js monitoring/static/js/1975.e3066826.chunk.js
        monitoring/static/js/2016.397296b7.chunk.js monitoring/static/js/2016.397296b7.chunk.js
        monitoring/static/js/2016.397296b7.chunk.js.LICENSE.txt monitoring/static/js/2016.397296b7.chunk.js.LICENSE.txt
        monitoring/static/js/2031.d8f098f9.chunk.js monitoring/static/js/2031.d8f098f9.chunk.js
        monitoring/static/js/2031.d8f098f9.chunk.js.LICENSE.txt monitoring/static/js/2031.d8f098f9.chunk.js.LICENSE.txt
        monitoring/static/js/2042.81e83eeb.chunk.js monitoring/static/js/2042.81e83eeb.chunk.js
        monitoring/static/js/2042.81e83eeb.chunk.js.LICENSE.txt monitoring/static/js/2042.81e83eeb.chunk.js.LICENSE.txt
        monitoring/static/js/2053.b4294b46.chunk.js monitoring/static/js/2053.b4294b46.chunk.js
        monitoring/static/js/2161.865bcd48.chunk.js monitoring/static/js/2161.865bcd48.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js monitoring/static/js/2162.cca3e026.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt
        monitoring/static/js/2166.e382ccb1.chunk.js monitoring/static/js/2166.e382ccb1.chunk.js
        monitoring/static/js/2182.1e53f745.chunk.js monitoring/static/js/2182.1e53f745.chunk.js
        monitoring/static/js/2182.1e53f745.chunk.js.LICENSE.txt monitoring/static/js/2182.1e53f745.chunk.js.LICENSE.txt
        monitoring/static/js/221.b740df48.chunk.js monitoring/static/js/221.b740df48.chunk.js
        monitoring/static/js/225.cf362439.chunk.js monitoring/static/js/225.cf362439.chunk.js
        monitoring/static/js/225.cf362439.chunk.js.LICENSE.txt monitoring/static/js/225.cf362439.chunk.js.LICENSE.txt
        monitoring/static/js/2286.e992ffd4.chunk.js monitoring/static/js/2286.e992ffd4.chunk.js
        monitoring/static/js/2308.9e3a980c.chunk.js monitoring/static/js/2308.9e3a980c.chunk.js
        monitoring/static/js/2350.a7af0a2e.chunk.js monitoring/static/js/2350.a7af0a2e.chunk.js
        monitoring/static/js/2350.a7af0a2e.chunk.js.LICENSE.txt monitoring/static/js/2350.a7af0a2e.chunk.js.LICENSE.txt
        monitoring/static/js/2372.880a31f6.chunk.js monitoring/static/js/2372.880a31f6.chunk.js
        monitoring/static/js/2372.880a31f6.chunk.js.LICENSE.txt monitoring/static/js/2372.880a31f6.chunk.js.LICENSE.txt
        monitoring/static/js/245.041a06df.chunk.js monitoring/static/js/245.041a06df.chunk.js
        monitoring/static/js/246.13bb9db2.chunk.js monitoring/static/js/246.13bb9db2.chunk.js
        monitoring/static/js/246.13bb9db2.chunk.js.LICENSE.txt monitoring/static/js/246.13bb9db2.chunk.js.LICENSE.txt
        monitoring/static/js/2466.b8b05e16.chunk.js monitoring/static/js/2466.b8b05e16.chunk.js
        monitoring/static/js/2518.ac97d255.chunk.js monitoring/static/js/2518.ac97d255.chunk.js
        monitoring/static/js/2518.ac97d255.chunk.js.LICENSE.txt monitoring/static/js/2518.ac97d255.chunk.js.LICENSE.txt
        monitoring/static/js/2568.5bec7af3.chunk.js monitoring/static/js/2568.5bec7af3.chunk.js
        monitoring/static/js/2612.55127fa0.chunk.js monitoring/static/js/2612.55127fa0.chunk.js
        monitoring/static/js/2626.a5b0d58c.chunk.js monitoring/static/js/2626.a5b0d58c.chunk.js
        monitoring/static/js/2726.abc3a0c2.chunk.js monitoring/static/js/2726.abc3a0c2.chunk.js
        monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt
        monitoring/static/js/2742.d5c8fae8.chunk.js monitoring/static/js/2742.d5c8fae8.chunk.js
        monitoring/static/js/2742.d5c8fae8.chunk.js.LICENSE.txt monitoring/static/js/2742.d5c8fae8.chunk.js.LICENSE.txt
        monitoring/static/js/2775.9105769d.chunk.js monitoring/static/js/2775.9105769d.chunk.js
        monitoring/static/js/2776.0f5e87f5.chunk.js monitoring/static/js/2776.0f5e87f5.chunk.js
        monitoring/static/js/2854.48cc58dc.chunk.js monitoring/static/js/2854.48cc58dc.chunk.js
        monitoring/static/js/2854.48cc58dc.chunk.js.LICENSE.txt monitoring/static/js/2854.48cc58dc.chunk.js.LICENSE.txt
        monitoring/static/js/290.b4f1e118.chunk.js monitoring/static/js/290.b4f1e118.chunk.js
        monitoring/static/js/2984.df749ebe.chunk.js monitoring/static/js/2984.df749ebe.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js monitoring/static/js/2994.6c6016a8.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt
        monitoring/static/js/30.69969059.chunk.js monitoring/static/js/30.69969059.chunk.js
        monitoring/static/js/30.69969059.chunk.js.LICENSE.txt monitoring/static/js/30.69969059.chunk.js.LICENSE.txt
        monitoring/static/js/3008.c8aa656b.chunk.js monitoring/static/js/3008.c8aa656b.chunk.js
        monitoring/static/js/3033.06c03554.chunk.js monitoring/static/js/3033.06c03554.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js monitoring/static/js/3048.692b5966.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt
        monitoring/static/js/310.b23bf6b2.chunk.js monitoring/static/js/310.b23bf6b2.chunk.js
        monitoring/static/js/3121.74e9e7dc.chunk.js monitoring/static/js/3121.74e9e7dc.chunk.js
        monitoring/static/js/3158.3f2d4e5f.chunk.js monitoring/static/js/3158.3f2d4e5f.chunk.js
        monitoring/static/js/3158.3f2d4e5f.chunk.js.LICENSE.txt monitoring/static/js/3158.3f2d4e5f.chunk.js.LICENSE.txt
        monitoring/static/js/3238.a9505f7f.chunk.js monitoring/static/js/3238.a9505f7f.chunk.js
        monitoring/static/js/3321.3370cf83.chunk.js monitoring/static/js/3321.3370cf83.chunk.js
        monitoring/static/js/3333.eb9ec516.chunk.js monitoring/static/js/3333.eb9ec516.chunk.js
        monitoring/static/js/3338.83fe4e63.chunk.js monitoring/static/js/3338.83fe4e63.chunk.js
        monitoring/static/js/3338.83fe4e63.chunk.js.LICENSE.txt monitoring/static/js/3338.83fe4e63.chunk.js.LICENSE.txt
        monitoring/static/js/3410.6391755f.chunk.js monitoring/static/js/3410.6391755f.chunk.js
        monitoring/static/js/3478.480dafa6.chunk.js monitoring/static/js/3478.480dafa6.chunk.js
        monitoring/static/js/3478.480dafa6.chunk.js.LICENSE.txt monitoring/static/js/3478.480dafa6.chunk.js.LICENSE.txt
        monitoring/static/js/3510.0740f36d.chunk.js monitoring/static/js/3510.0740f36d.chunk.js
        monitoring/static/js/3521.775b3981.chunk.js monitoring/static/js/3521.775b3981.chunk.js
        monitoring/static/js/3534.3f09429a.chunk.js monitoring/static/js/3534.3f09429a.chunk.js
        monitoring/static/js/3534.3f09429a.chunk.js.LICENSE.txt monitoring/static/js/3534.3f09429a.chunk.js.LICENSE.txt
        monitoring/static/js/3638.ee3efb24.chunk.js monitoring/static/js/3638.ee3efb24.chunk.js
        monitoring/static/js/3638.ee3efb24.chunk.js.LICENSE.txt monitoring/static/js/3638.ee3efb24.chunk.js.LICENSE.txt
        monitoring/static/js/3648.3a72999a.chunk.js monitoring/static/js/3648.3a72999a.chunk.js
        monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt
        monitoring/static/js/3653.06b5272c.chunk.js monitoring/static/js/3653.06b5272c.chunk.js
        monitoring/static/js/3672.12436dd6.chunk.js monitoring/static/js/3672.12436dd6.chunk.js
        monitoring/static/js/3679.e293221d.chunk.js monitoring/static/js/3679.e293221d.chunk.js
        monitoring/static/js/3702.778880f9.chunk.js monitoring/static/js/3702.778880f9.chunk.js
        monitoring/static/js/3702.778880f9.chunk.js.LICENSE.txt monitoring/static/js/3702.778880f9.chunk.js.LICENSE.txt
        monitoring/static/js/3756.9a440b73.chunk.js monitoring/static/js/3756.9a440b73.chunk.js
        monitoring/static/js/3761.dd957fd1.chunk.js monitoring/static/js/3761.dd957fd1.chunk.js
        monitoring/static/js/3769.860e8d58.chunk.js monitoring/static/js/3769.860e8d58.chunk.js
        monitoring/static/js/3779.1d869a05.chunk.js monitoring/static/js/3779.1d869a05.chunk.js
        monitoring/static/js/3812.368eb751.chunk.js monitoring/static/js/3812.368eb751.chunk.js
        monitoring/static/js/3822.00ab6aaa.chunk.js monitoring/static/js/3822.00ab6aaa.chunk.js
        monitoring/static/js/3822.00ab6aaa.chunk.js.LICENSE.txt monitoring/static/js/3822.00ab6aaa.chunk.js.LICENSE.txt
        monitoring/static/js/3872.a25d87b5.chunk.js monitoring/static/js/3872.a25d87b5.chunk.js
        monitoring/static/js/3879.17f211ad.chunk.js monitoring/static/js/3879.17f211ad.chunk.js
        monitoring/static/js/3902.973b73c6.chunk.js monitoring/static/js/3902.973b73c6.chunk.js
        monitoring/static/js/3952.ce1b4fad.chunk.js monitoring/static/js/3952.ce1b4fad.chunk.js
        monitoring/static/js/3977.4c33ec16.chunk.js monitoring/static/js/3977.4c33ec16.chunk.js
        monitoring/static/js/3977.4c33ec16.chunk.js.LICENSE.txt monitoring/static/js/3977.4c33ec16.chunk.js.LICENSE.txt
        monitoring/static/js/4096.33f702a0.chunk.js monitoring/static/js/4096.33f702a0.chunk.js
        monitoring/static/js/4130.4d4b9fad.chunk.js monitoring/static/js/4130.4d4b9fad.chunk.js
        monitoring/static/js/4169.f2243012.chunk.js monitoring/static/js/4169.f2243012.chunk.js
        monitoring/static/js/4180.1de6c8ba.chunk.js monitoring/static/js/4180.1de6c8ba.chunk.js
        monitoring/static/js/431.f23349ec.chunk.js monitoring/static/js/431.f23349ec.chunk.js
        monitoring/static/js/4324.c69948f7.chunk.js monitoring/static/js/4324.c69948f7.chunk.js
        monitoring/static/js/4349.9c8d20fd.chunk.js monitoring/static/js/4349.9c8d20fd.chunk.js
        monitoring/static/js/4401.f46d19f6.chunk.js monitoring/static/js/4401.f46d19f6.chunk.js
        monitoring/static/js/4441.2b4963cf.chunk.js monitoring/static/js/4441.2b4963cf.chunk.js
        monitoring/static/js/4534.b98f1389.chunk.js monitoring/static/js/4534.b98f1389.chunk.js
        monitoring/static/js/4534.b98f1389.chunk.js.LICENSE.txt monitoring/static/js/4534.b98f1389.chunk.js.LICENSE.txt
        monitoring/static/js/4542.18433910.chunk.js monitoring/static/js/4542.18433910.chunk.js
        monitoring/static/js/4542.18433910.chunk.js.LICENSE.txt monitoring/static/js/4542.18433910.chunk.js.LICENSE.txt
        monitoring/static/js/4582.5bf174ff.chunk.js monitoring/static/js/4582.5bf174ff.chunk.js
        monitoring/static/js/4582.5bf174ff.chunk.js.LICENSE.txt monitoring/static/js/4582.5bf174ff.chunk.js.LICENSE.txt
        monitoring/static/js/4601.39745c4e.chunk.js monitoring/static/js/4601.39745c4e.chunk.js
        monitoring/static/js/4652.9a5d2242.chunk.js monitoring/static/js/4652.9a5d2242.chunk.js
        monitoring/static/js/4662.1c10232d.chunk.js monitoring/static/js/4662.1c10232d.chunk.js
        monitoring/static/js/4662.1c10232d.chunk.js.LICENSE.txt monitoring/static/js/4662.1c10232d.chunk.js.LICENSE.txt
        monitoring/static/js/4678.4e2f4af4.chunk.js monitoring/static/js/4678.4e2f4af4.chunk.js
        monitoring/static/js/4678.4e2f4af4.chunk.js.LICENSE.txt monitoring/static/js/4678.4e2f4af4.chunk.js.LICENSE.txt
        monitoring/static/js/47.54dd12ac.chunk.js monitoring/static/js/47.54dd12ac.chunk.js
        monitoring/static/js/4730.78e66e9a.chunk.js monitoring/static/js/4730.78e66e9a.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js monitoring/static/js/4818.6beda30c.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt
        monitoring/static/js/4847.63c73f0a.chunk.js monitoring/static/js/4847.63c73f0a.chunk.js
        monitoring/static/js/4870.1916a88d.chunk.js monitoring/static/js/4870.1916a88d.chunk.js
        monitoring/static/js/4870.1916a88d.chunk.js.LICENSE.txt monitoring/static/js/4870.1916a88d.chunk.js.LICENSE.txt
        monitoring/static/js/4872.bbf7ab34.chunk.js monitoring/static/js/4872.bbf7ab34.chunk.js
        monitoring/static/js/4872.bbf7ab34.chunk.js.LICENSE.txt monitoring/static/js/4872.bbf7ab34.chunk.js.LICENSE.txt
        monitoring/static/js/4891.c441be74.chunk.js monitoring/static/js/4891.c441be74.chunk.js
        monitoring/static/js/5027.9e6325ef.chunk.js monitoring/static/js/5027.9e6325ef.chunk.js
        monitoring/static/js/5047.ebc9f1b2.chunk.js monitoring/static/js/5047.ebc9f1b2.chunk.js
        monitoring/static/js/5050.ffa3921f.chunk.js monitoring/static/js/5050.ffa3921f.chunk.js
        monitoring/static/js/5050.ffa3921f.chunk.js.LICENSE.txt monitoring/static/js/5050.ffa3921f.chunk.js.LICENSE.txt
        monitoring/static/js/513.46a664ad.chunk.js monitoring/static/js/513.46a664ad.chunk.js
        monitoring/static/js/5161.20e37d15.chunk.js monitoring/static/js/5161.20e37d15.chunk.js
        monitoring/static/js/523.17013d4e.chunk.js monitoring/static/js/523.17013d4e.chunk.js
        monitoring/static/js/5252.991dcab8.chunk.js monitoring/static/js/5252.991dcab8.chunk.js
        monitoring/static/js/5252.991dcab8.chunk.js.LICENSE.txt monitoring/static/js/5252.991dcab8.chunk.js.LICENSE.txt
        monitoring/static/js/5382.3a2e6ac6.chunk.js monitoring/static/js/5382.3a2e6ac6.chunk.js
        monitoring/static/js/5382.3a2e6ac6.chunk.js.LICENSE.txt monitoring/static/js/5382.3a2e6ac6.chunk.js.LICENSE.txt
        monitoring/static/js/5401.dfb63825.chunk.js monitoring/static/js/5401.dfb63825.chunk.js
        monitoring/static/js/5418.00d0c5d1.chunk.js monitoring/static/js/5418.00d0c5d1.chunk.js
        monitoring/static/js/5438.91fe1c76.chunk.js monitoring/static/js/5438.91fe1c76.chunk.js
        monitoring/static/js/5453.921caa82.chunk.js monitoring/static/js/5453.921caa82.chunk.js
        monitoring/static/js/5454.e8f64f1a.chunk.js monitoring/static/js/5454.e8f64f1a.chunk.js
        monitoring/static/js/5454.e8f64f1a.chunk.js.LICENSE.txt monitoring/static/js/5454.e8f64f1a.chunk.js.LICENSE.txt
        monitoring/static/js/5475.20ce4f75.chunk.js monitoring/static/js/5475.20ce4f75.chunk.js
        monitoring/static/js/5542.7c13d444.chunk.js monitoring/static/js/5542.7c13d444.chunk.js
        monitoring/static/js/5542.7c13d444.chunk.js.LICENSE.txt monitoring/static/js/5542.7c13d444.chunk.js.LICENSE.txt
        monitoring/static/js/5579.a2c9515c.chunk.js monitoring/static/js/5579.a2c9515c.chunk.js
        monitoring/static/js/5634.0a0bddae.chunk.js monitoring/static/js/5634.0a0bddae.chunk.js
        monitoring/static/js/5636.da9c4c85.chunk.js monitoring/static/js/5636.da9c4c85.chunk.js
        monitoring/static/js/5636.da9c4c85.chunk.js.LICENSE.txt monitoring/static/js/5636.da9c4c85.chunk.js.LICENSE.txt
        monitoring/static/js/5647.0920ef73.chunk.js monitoring/static/js/5647.0920ef73.chunk.js
        monitoring/static/js/5652.40b9a7d3.chunk.js monitoring/static/js/5652.40b9a7d3.chunk.js
        monitoring/static/js/5685.c0a21a10.chunk.js monitoring/static/js/5685.c0a21a10.chunk.js
        monitoring/static/js/5759.52418ea5.chunk.js monitoring/static/js/5759.52418ea5.chunk.js
        monitoring/static/js/5816.144b5755.chunk.js monitoring/static/js/5816.144b5755.chunk.js
        monitoring/static/js/5819.0ae2eb3d.chunk.js monitoring/static/js/5819.0ae2eb3d.chunk.js
        monitoring/static/js/5853.0c5ec1d1.chunk.js monitoring/static/js/5853.0c5ec1d1.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js monitoring/static/js/5866.14d27c8c.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt
        monitoring/static/js/5875.f8a190bf.chunk.js monitoring/static/js/5875.f8a190bf.chunk.js
        monitoring/static/js/5888.4fa64369.chunk.js monitoring/static/js/5888.4fa64369.chunk.js
        monitoring/static/js/5888.4fa64369.chunk.js.LICENSE.txt monitoring/static/js/5888.4fa64369.chunk.js.LICENSE.txt
        monitoring/static/js/5924.53ba4f49.chunk.js monitoring/static/js/5924.53ba4f49.chunk.js
        monitoring/static/js/5950.28656717.chunk.js monitoring/static/js/5950.28656717.chunk.js
        monitoring/static/js/5953.cb95c45e.chunk.js monitoring/static/js/5953.cb95c45e.chunk.js
        monitoring/static/js/5988.38ef363d.chunk.js monitoring/static/js/5988.38ef363d.chunk.js
        monitoring/static/js/5988.38ef363d.chunk.js.LICENSE.txt monitoring/static/js/5988.38ef363d.chunk.js.LICENSE.txt
        monitoring/static/js/60.85d957cd.chunk.js monitoring/static/js/60.85d957cd.chunk.js
        monitoring/static/js/6012.aac08e72.chunk.js monitoring/static/js/6012.aac08e72.chunk.js
        monitoring/static/js/6012.aac08e72.chunk.js.LICENSE.txt monitoring/static/js/6012.aac08e72.chunk.js.LICENSE.txt
        monitoring/static/js/6030.b30fca36.chunk.js monitoring/static/js/6030.b30fca36.chunk.js
        monitoring/static/js/6047.328b41a5.chunk.js monitoring/static/js/6047.328b41a5.chunk.js
        monitoring/static/js/6114.c74edf11.chunk.js monitoring/static/js/6114.c74edf11.chunk.js
        monitoring/static/js/6210.69d6a30a.chunk.js monitoring/static/js/6210.69d6a30a.chunk.js
        monitoring/static/js/6210.69d6a30a.chunk.js.LICENSE.txt monitoring/static/js/6210.69d6a30a.chunk.js.LICENSE.txt
        monitoring/static/js/6214.a9a481a7.chunk.js monitoring/static/js/6214.a9a481a7.chunk.js
        monitoring/static/js/6214.a9a481a7.chunk.js.LICENSE.txt monitoring/static/js/6214.a9a481a7.chunk.js.LICENSE.txt
        monitoring/static/js/6261.78de43a8.chunk.js monitoring/static/js/6261.78de43a8.chunk.js
        monitoring/static/js/6262.44dba84f.chunk.js monitoring/static/js/6262.44dba84f.chunk.js
        monitoring/static/js/6262.44dba84f.chunk.js.LICENSE.txt monitoring/static/js/6262.44dba84f.chunk.js.LICENSE.txt
        monitoring/static/js/628.70d08de9.chunk.js monitoring/static/js/628.70d08de9.chunk.js
        monitoring/static/js/6324.de01edfb.chunk.js monitoring/static/js/6324.de01edfb.chunk.js
        monitoring/static/js/6342.a2819c87.chunk.js monitoring/static/js/6342.a2819c87.chunk.js
        monitoring/static/js/6342.a2819c87.chunk.js.LICENSE.txt monitoring/static/js/6342.a2819c87.chunk.js.LICENSE.txt
        monitoring/static/js/6358.2997762b.chunk.js monitoring/static/js/6358.2997762b.chunk.js
        monitoring/static/js/6374.be0c5879.chunk.js monitoring/static/js/6374.be0c5879.chunk.js
        monitoring/static/js/6374.be0c5879.chunk.js.LICENSE.txt monitoring/static/js/6374.be0c5879.chunk.js.LICENSE.txt
        monitoring/static/js/6397.b8cf6fae.chunk.js monitoring/static/js/6397.b8cf6fae.chunk.js
        monitoring/static/js/6405.b0dd94a9.chunk.js monitoring/static/js/6405.b0dd94a9.chunk.js
        monitoring/static/js/6447.2c0d9bda.chunk.js monitoring/static/js/6447.2c0d9bda.chunk.js
        monitoring/static/js/6447.2c0d9bda.chunk.js.LICENSE.txt monitoring/static/js/6447.2c0d9bda.chunk.js.LICENSE.txt
        monitoring/static/js/654.863ea445.chunk.js monitoring/static/js/654.863ea445.chunk.js
        monitoring/static/js/654.863ea445.chunk.js.LICENSE.txt monitoring/static/js/654.863ea445.chunk.js.LICENSE.txt
        monitoring/static/js/6541.a39e9d6a.chunk.js monitoring/static/js/6541.a39e9d6a.chunk.js
        monitoring/static/js/6554.6dfab136.chunk.js monitoring/static/js/6554.6dfab136.chunk.js
        monitoring/static/js/6554.6dfab136.chunk.js.LICENSE.txt monitoring/static/js/6554.6dfab136.chunk.js.LICENSE.txt
        monitoring/static/js/6625.a8d44d36.chunk.js monitoring/static/js/6625.a8d44d36.chunk.js
        monitoring/static/js/6658.b22172da.chunk.js monitoring/static/js/6658.b22172da.chunk.js
        monitoring/static/js/6658.b22172da.chunk.js.LICENSE.txt monitoring/static/js/6658.b22172da.chunk.js.LICENSE.txt
        monitoring/static/js/6664.b4dbf019.chunk.js monitoring/static/js/6664.b4dbf019.chunk.js
        monitoring/static/js/6786.28af14f6.chunk.js monitoring/static/js/6786.28af14f6.chunk.js
        monitoring/static/js/6820.bff2520f.chunk.js monitoring/static/js/6820.bff2520f.chunk.js
        monitoring/static/js/6820.bff2520f.chunk.js.LICENSE.txt monitoring/static/js/6820.bff2520f.chunk.js.LICENSE.txt
        monitoring/static/js/6833.584b7806.chunk.js monitoring/static/js/6833.584b7806.chunk.js
        monitoring/static/js/684.9346e985.chunk.js monitoring/static/js/684.9346e985.chunk.js
        monitoring/static/js/6879.2965a366.chunk.js monitoring/static/js/6879.2965a366.chunk.js
        monitoring/static/js/6881.7e6434c9.chunk.js monitoring/static/js/6881.7e6434c9.chunk.js
        monitoring/static/js/6990.70257b9b.chunk.js monitoring/static/js/6990.70257b9b.chunk.js
        monitoring/static/js/710.87e9f2e0.chunk.js monitoring/static/js/710.87e9f2e0.chunk.js
        monitoring/static/js/7118.ce0cd05f.chunk.js monitoring/static/js/7118.ce0cd05f.chunk.js
        monitoring/static/js/7118.ce0cd05f.chunk.js.LICENSE.txt monitoring/static/js/7118.ce0cd05f.chunk.js.LICENSE.txt
        monitoring/static/js/7148.ef54cd41.chunk.js monitoring/static/js/7148.ef54cd41.chunk.js
        monitoring/static/js/7148.ef54cd41.chunk.js.LICENSE.txt monitoring/static/js/7148.ef54cd41.chunk.js.LICENSE.txt
        monitoring/static/js/7206.6b7278f5.chunk.js monitoring/static/js/7206.6b7278f5.chunk.js
        monitoring/static/js/7206.6b7278f5.chunk.js.LICENSE.txt monitoring/static/js/7206.6b7278f5.chunk.js.LICENSE.txt
        monitoring/static/js/7240.a674bc94.chunk.js monitoring/static/js/7240.a674bc94.chunk.js
        monitoring/static/js/730.a22f6f5f.chunk.js monitoring/static/js/730.a22f6f5f.chunk.js
        monitoring/static/js/7329.514640a4.chunk.js monitoring/static/js/7329.514640a4.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js monitoring/static/js/734.3fe325e9.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt
        monitoring/static/js/7348.eac33db2.chunk.js monitoring/static/js/7348.eac33db2.chunk.js
        monitoring/static/js/7357.a518ad9e.chunk.js monitoring/static/js/7357.a518ad9e.chunk.js
        monitoring/static/js/7380.38a8eedf.chunk.js monitoring/static/js/7380.38a8eedf.chunk.js
        monitoring/static/js/7420.d0c66c34.chunk.js monitoring/static/js/7420.d0c66c34.chunk.js
        monitoring/static/js/7548.fd5d2b6c.chunk.js monitoring/static/js/7548.fd5d2b6c.chunk.js
        monitoring/static/js/7574.8ac9803d.chunk.js monitoring/static/js/7574.8ac9803d.chunk.js
        monitoring/static/js/7574.8ac9803d.chunk.js.LICENSE.txt monitoring/static/js/7574.8ac9803d.chunk.js.LICENSE.txt
        monitoring/static/js/7579.079e5569.chunk.js monitoring/static/js/7579.079e5569.chunk.js
        monitoring/static/js/7605.cdc8f605.chunk.js monitoring/static/js/7605.cdc8f605.chunk.js
        monitoring/static/js/7614.00d00ded.chunk.js monitoring/static/js/7614.00d00ded.chunk.js
        monitoring/static/js/7638.5489d672.chunk.js monitoring/static/js/7638.5489d672.chunk.js
        monitoring/static/js/7638.5489d672.chunk.js.LICENSE.txt monitoring/static/js/7638.5489d672.chunk.js.LICENSE.txt
        monitoring/static/js/7642.39707d60.chunk.js monitoring/static/js/7642.39707d60.chunk.js
        monitoring/static/js/7642.39707d60.chunk.js.LICENSE.txt monitoring/static/js/7642.39707d60.chunk.js.LICENSE.txt
        monitoring/static/js/7692.af7181c9.chunk.js monitoring/static/js/7692.af7181c9.chunk.js
        monitoring/static/js/7692.af7181c9.chunk.js.LICENSE.txt monitoring/static/js/7692.af7181c9.chunk.js.LICENSE.txt
        monitoring/static/js/7697.44af783d.chunk.js monitoring/static/js/7697.44af783d.chunk.js
        monitoring/static/js/7718.f897f8ca.chunk.js monitoring/static/js/7718.f897f8ca.chunk.js
        monitoring/static/js/7718.f897f8ca.chunk.js.LICENSE.txt monitoring/static/js/7718.f897f8ca.chunk.js.LICENSE.txt
        monitoring/static/js/7748.3ec14243.chunk.js monitoring/static/js/7748.3ec14243.chunk.js
        monitoring/static/js/7878.1f9512c4.chunk.js monitoring/static/js/7878.1f9512c4.chunk.js
        monitoring/static/js/7946.3b4cf6fd.chunk.js monitoring/static/js/7946.3b4cf6fd.chunk.js
        monitoring/static/js/7946.3b4cf6fd.chunk.js.LICENSE.txt monitoring/static/js/7946.3b4cf6fd.chunk.js.LICENSE.txt
        monitoring/static/js/7962.43ffbad7.chunk.js monitoring/static/js/7962.43ffbad7.chunk.js
        monitoring/static/js/7970.80caf61d.chunk.js monitoring/static/js/7970.80caf61d.chunk.js
        monitoring/static/js/8008.da36f479.chunk.js monitoring/static/js/8008.da36f479.chunk.js
        monitoring/static/js/8014.eb9f97f2.chunk.js monitoring/static/js/8014.eb9f97f2.chunk.js
        monitoring/static/js/8014.eb9f97f2.chunk.js.LICENSE.txt monitoring/static/js/8014.eb9f97f2.chunk.js.LICENSE.txt
        monitoring/static/js/8053.71be175a.chunk.js monitoring/static/js/8053.71be175a.chunk.js
        monitoring/static/js/8081.5fafc7dc.chunk.js monitoring/static/js/8081.5fafc7dc.chunk.js
        monitoring/static/js/8103.00c9e9c1.chunk.js monitoring/static/js/8103.00c9e9c1.chunk.js
        monitoring/static/js/811.a0c1e1ce.chunk.js monitoring/static/js/811.a0c1e1ce.chunk.js
        monitoring/static/js/8119.196e82ef.chunk.js monitoring/static/js/8119.196e82ef.chunk.js
        monitoring/static/js/8234.7512920e.chunk.js monitoring/static/js/8234.7512920e.chunk.js
        monitoring/static/js/8234.7512920e.chunk.js.LICENSE.txt monitoring/static/js/8234.7512920e.chunk.js.LICENSE.txt
        monitoring/static/js/8256.a2ce240a.chunk.js monitoring/static/js/8256.a2ce240a.chunk.js
        monitoring/static/js/8393.2e643149.chunk.js monitoring/static/js/8393.2e643149.chunk.js
        monitoring/static/js/8452.3bfa9018.chunk.js monitoring/static/js/8452.3bfa9018.chunk.js
        monitoring/static/js/8517.cf981031.chunk.js monitoring/static/js/8517.cf981031.chunk.js
        monitoring/static/js/8534.f7aec532.chunk.js monitoring/static/js/8534.f7aec532.chunk.js
        monitoring/static/js/856.36f195a5.chunk.js monitoring/static/js/856.36f195a5.chunk.js
        monitoring/static/js/8633.da605a09.chunk.js monitoring/static/js/8633.da605a09.chunk.js
        monitoring/static/js/8704.87492da1.chunk.js monitoring/static/js/8704.87492da1.chunk.js
        monitoring/static/js/8706.755fcb81.chunk.js monitoring/static/js/8706.755fcb81.chunk.js
        monitoring/static/js/8821.96eeccd6.chunk.js monitoring/static/js/8821.96eeccd6.chunk.js
        monitoring/static/js/8821.96eeccd6.chunk.js.LICENSE.txt monitoring/static/js/8821.96eeccd6.chunk.js.LICENSE.txt
        monitoring/static/js/8840.5eb376ca.chunk.js monitoring/static/js/8840.5eb376ca.chunk.js
        monitoring/static/js/8868.a9031705.chunk.js monitoring/static/js/8868.a9031705.chunk.js
        monitoring/static/js/8890.9f3d8f08.chunk.js monitoring/static/js/8890.9f3d8f08.chunk.js
        monitoring/static/js/8979.0c0acc31.chunk.js monitoring/static/js/8979.0c0acc31.chunk.js
        monitoring/static/js/8986.de287636.chunk.js monitoring/static/js/8986.de287636.chunk.js
        monitoring/static/js/8986.de287636.chunk.js.LICENSE.txt monitoring/static/js/8986.de287636.chunk.js.LICENSE.txt
        monitoring/static/js/9010.4bfaf5fa.chunk.js monitoring/static/js/9010.4bfaf5fa.chunk.js
        monitoring/static/js/9010.4bfaf5fa.chunk.js.LICENSE.txt monitoring/static/js/9010.4bfaf5fa.chunk.js.LICENSE.txt
        monitoring/static/js/902.a1b90b1b.chunk.js monitoring/static/js/902.a1b90b1b.chunk.js
        monitoring/static/js/902.a1b90b1b.chunk.js.LICENSE.txt monitoring/static/js/902.a1b90b1b.chunk.js.LICENSE.txt
        monitoring/static/js/9025.6ae28867.chunk.js monitoring/static/js/9025.6ae28867.chunk.js
        monitoring/static/js/9067.254af4a9.chunk.js monitoring/static/js/9067.254af4a9.chunk.js
        monitoring/static/js/9172.f332051f.chunk.js monitoring/static/js/9172.f332051f.chunk.js
        monitoring/static/js/9172.f332051f.chunk.js.LICENSE.txt monitoring/static/js/9172.f332051f.chunk.js.LICENSE.txt
        monitoring/static/js/9174.ae7682da.chunk.js monitoring/static/js/9174.ae7682da.chunk.js
        monitoring/static/js/9176.3f08336f.chunk.js monitoring/static/js/9176.3f08336f.chunk.js
        monitoring/static/js/9176.3f08336f.chunk.js.LICENSE.txt monitoring/static/js/9176.3f08336f.chunk.js.LICENSE.txt
        monitoring/static/js/921.0402e36c.chunk.js monitoring/static/js/921.0402e36c.chunk.js
        monitoring/static/js/9220.a9f48eb9.chunk.js monitoring/static/js/9220.a9f48eb9.chunk.js
        monitoring/static/js/9222.a1913f85.chunk.js monitoring/static/js/9222.a1913f85.chunk.js
        monitoring/static/js/9243.cb95c73b.chunk.js monitoring/static/js/9243.cb95c73b.chunk.js
        monitoring/static/js/9300.89daa9ec.chunk.js monitoring/static/js/9300.89daa9ec.chunk.js
        monitoring/static/js/9312.5eb8d4b1.chunk.js monitoring/static/js/9312.5eb8d4b1.chunk.js
        monitoring/static/js/9312.5eb8d4b1.chunk.js.LICENSE.txt monitoring/static/js/9312.5eb8d4b1.chunk.js.LICENSE.txt
        monitoring/static/js/9393.b0a1bf0a.chunk.js monitoring/static/js/9393.b0a1bf0a.chunk.js
        monitoring/static/js/9394.ca56f408.chunk.js monitoring/static/js/9394.ca56f408.chunk.js
        monitoring/static/js/9394.ca56f408.chunk.js.LICENSE.txt monitoring/static/js/9394.ca56f408.chunk.js.LICENSE.txt
        monitoring/static/js/9433.4cf14d1c.chunk.js monitoring/static/js/9433.4cf14d1c.chunk.js
        monitoring/static/js/9466.0afe6e6e.chunk.js monitoring/static/js/9466.0afe6e6e.chunk.js
        monitoring/static/js/949.12b4714d.chunk.js monitoring/static/js/949.12b4714d.chunk.js
        monitoring/static/js/9507.14872b99.chunk.js monitoring/static/js/9507.14872b99.chunk.js
        monitoring/static/js/9518.3fffdd45.chunk.js monitoring/static/js/9518.3fffdd45.chunk.js
        monitoring/static/js/9523.ac019963.chunk.js monitoring/static/js/9523.ac019963.chunk.js
        monitoring/static/js/957.08a1c505.chunk.js monitoring/static/js/957.08a1c505.chunk.js
        monitoring/static/js/957.08a1c505.chunk.js.LICENSE.txt monitoring/static/js/957.08a1c505.chunk.js.LICENSE.txt
        monitoring/static/js/9582.c09a3623.chunk.js monitoring/static/js/9582.c09a3623.chunk.js
        monitoring/static/js/9582.c09a3623.chunk.js.LICENSE.txt monitoring/static/js/9582.c09a3623.chunk.js.LICENSE.txt
        monitoring/static/js/9606.fc9247cb.chunk.js monitoring/static/js/9606.fc9247cb.chunk.js
        monitoring/static/js/9697.bed5988b.chunk.js monitoring/static/js/9697.bed5988b.chunk.js
        monitoring/static/js/9707.72e68790.chunk.js monitoring/static/js/9707.72e68790.chunk.js
        monitoring/static/js/9725.a94823a0.chunk.js monitoring/static/js/9725.a94823a0.chunk.js
        monitoring/static/js/9728.7cc957e4.chunk.js monitoring/static/js/9728.7cc957e4.chunk.js
        monitoring/static/js/9728.7cc957e4.chunk.js.LICENSE.txt monitoring/static/js/9728.7cc957e4.chunk.js.LICENSE.txt
        monitoring/static/js/9748.e711e962.chunk.js monitoring/static/js/9748.e711e962.chunk.js
        monitoring/static/js/9748.e711e962.chunk.js.LICENSE.txt monitoring/static/js/9748.e711e962.chunk.js.LICENSE.txt
        monitoring/static/js/9778.52ad76ce.chunk.js monitoring/static/js/9778.52ad76ce.chunk.js
        monitoring/static/js/9778.52ad76ce.chunk.js.LICENSE.txt monitoring/static/js/9778.52ad76ce.chunk.js.LICENSE.txt
        monitoring/static/js/9796.23c68f38.chunk.js monitoring/static/js/9796.23c68f38.chunk.js
        monitoring/static/js/9801.b9143d43.chunk.js monitoring/static/js/9801.b9143d43.chunk.js
        monitoring/static/js/9802.b15025e5.chunk.js monitoring/static/js/9802.b15025e5.chunk.js
        monitoring/static/js/9842.b8ba19ad.chunk.js monitoring/static/js/9842.b8ba19ad.chunk.js
        monitoring/static/js/9842.b8ba19ad.chunk.js.LICENSE.txt monitoring/static/js/9842.b8ba19ad.chunk.js.LICENSE.txt
        monitoring/static/js/9872.65a6fae7.chunk.js monitoring/static/js/9872.65a6fae7.chunk.js
        monitoring/static/js/9872.65a6fae7.chunk.js.LICENSE.txt monitoring/static/js/9872.65a6fae7.chunk.js.LICENSE.txt
        monitoring/static/js/9882.b983931a.chunk.js monitoring/static/js/9882.b983931a.chunk.js
        monitoring/static/js/99.73d10ff1.chunk.js monitoring/static/js/99.73d10ff1.chunk.js
        monitoring/static/js/9908.4bd3acb1.chunk.js monitoring/static/js/9908.4bd3acb1.chunk.js
        monitoring/static/js/9922.367b63b0.chunk.js monitoring/static/js/9922.367b63b0.chunk.js
        monitoring/static/js/main.806ca619.js monitoring/static/js/main.806ca619.js
        monitoring/static/js/main.806ca619.js.LICENSE.txt monitoring/static/js/main.806ca619.js.LICENSE.txt
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
