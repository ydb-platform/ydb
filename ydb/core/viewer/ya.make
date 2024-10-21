RECURSE_FOR_TESTS(
    ut
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
        monitoring/html.worker.js.LICENSE.txt monitoring/html.worker.js.LICENSE.txt
        monitoring/index.html monitoring/index.html
        monitoring/json.worker.js monitoring/json.worker.js
        monitoring/json.worker.js.LICENSE.txt monitoring/json.worker.js.LICENSE.txt
        monitoring/static/css/1001.d55e6f5e.chunk.css monitoring/static/css/1001.d55e6f5e.chunk.css
        monitoring/static/css/1276.f723033a.chunk.css monitoring/static/css/1276.f723033a.chunk.css
        monitoring/static/css/3209.28cb5984.chunk.css monitoring/static/css/3209.28cb5984.chunk.css
        monitoring/static/css/3295.8975fa00.chunk.css monitoring/static/css/3295.8975fa00.chunk.css
        monitoring/static/css/3812.b1faff6b.chunk.css monitoring/static/css/3812.b1faff6b.chunk.css
        monitoring/static/css/4808.146b1de9.chunk.css monitoring/static/css/4808.146b1de9.chunk.css
        monitoring/static/css/511.0a2e691a.chunk.css monitoring/static/css/511.0a2e691a.chunk.css
        monitoring/static/css/5118.ac57fa01.chunk.css monitoring/static/css/5118.ac57fa01.chunk.css
        monitoring/static/css/5426.8f2389ae.chunk.css monitoring/static/css/5426.8f2389ae.chunk.css
        monitoring/static/css/5573.3ce054a3.chunk.css monitoring/static/css/5573.3ce054a3.chunk.css
        monitoring/static/css/6866.3cc21bd9.chunk.css monitoring/static/css/6866.3cc21bd9.chunk.css
        monitoring/static/css/6869.1d0912d9.chunk.css monitoring/static/css/6869.1d0912d9.chunk.css
        monitoring/static/css/7054.44580026.chunk.css monitoring/static/css/7054.44580026.chunk.css
        monitoring/static/css/7512.b17f7aaf.chunk.css monitoring/static/css/7512.b17f7aaf.chunk.css
        monitoring/static/css/main.0bf24cd8.css monitoring/static/css/main.0bf24cd8.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1001.2a472a60.chunk.js monitoring/static/js/1001.2a472a60.chunk.js
        monitoring/static/js/1035.4d5fe2fd.chunk.js monitoring/static/js/1035.4d5fe2fd.chunk.js
        monitoring/static/js/1072.9a47be2b.chunk.js monitoring/static/js/1072.9a47be2b.chunk.js
        monitoring/static/js/1074.8bbb31b1.chunk.js monitoring/static/js/1074.8bbb31b1.chunk.js
        monitoring/static/js/1109.739c032d.chunk.js monitoring/static/js/1109.739c032d.chunk.js
        monitoring/static/js/1109.739c032d.chunk.js.LICENSE.txt monitoring/static/js/1109.739c032d.chunk.js.LICENSE.txt
        monitoring/static/js/1139.e67ff859.chunk.js monitoring/static/js/1139.e67ff859.chunk.js
        monitoring/static/js/1222.1b644c9d.chunk.js monitoring/static/js/1222.1b644c9d.chunk.js
        monitoring/static/js/1222.1b644c9d.chunk.js.LICENSE.txt monitoring/static/js/1222.1b644c9d.chunk.js.LICENSE.txt
        monitoring/static/js/1236.3b0b47e7.chunk.js monitoring/static/js/1236.3b0b47e7.chunk.js
        monitoring/static/js/127.031dd9bd.chunk.js monitoring/static/js/127.031dd9bd.chunk.js
        monitoring/static/js/1276.f7603cad.chunk.js monitoring/static/js/1276.f7603cad.chunk.js
        monitoring/static/js/1376.41e671ac.chunk.js monitoring/static/js/1376.41e671ac.chunk.js
        monitoring/static/js/1376.41e671ac.chunk.js.LICENSE.txt monitoring/static/js/1376.41e671ac.chunk.js.LICENSE.txt
        monitoring/static/js/1388.dc200448.chunk.js monitoring/static/js/1388.dc200448.chunk.js
        monitoring/static/js/1389.4a6fe794.chunk.js monitoring/static/js/1389.4a6fe794.chunk.js
        monitoring/static/js/1410.9bec28d3.chunk.js monitoring/static/js/1410.9bec28d3.chunk.js
        monitoring/static/js/1410.9bec28d3.chunk.js.LICENSE.txt monitoring/static/js/1410.9bec28d3.chunk.js.LICENSE.txt
        monitoring/static/js/1460.53037d83.chunk.js monitoring/static/js/1460.53037d83.chunk.js
        monitoring/static/js/1460.53037d83.chunk.js.LICENSE.txt monitoring/static/js/1460.53037d83.chunk.js.LICENSE.txt
        monitoring/static/js/1487.4365b2d5.chunk.js monitoring/static/js/1487.4365b2d5.chunk.js
        monitoring/static/js/1487.4365b2d5.chunk.js.LICENSE.txt monitoring/static/js/1487.4365b2d5.chunk.js.LICENSE.txt
        monitoring/static/js/1577.e3c3298a.chunk.js monitoring/static/js/1577.e3c3298a.chunk.js
        monitoring/static/js/1640.eae19c57.chunk.js monitoring/static/js/1640.eae19c57.chunk.js
        monitoring/static/js/1640.eae19c57.chunk.js.LICENSE.txt monitoring/static/js/1640.eae19c57.chunk.js.LICENSE.txt
        monitoring/static/js/165.fe88e976.chunk.js monitoring/static/js/165.fe88e976.chunk.js
        monitoring/static/js/1670.886ee3f9.chunk.js monitoring/static/js/1670.886ee3f9.chunk.js
        monitoring/static/js/1670.886ee3f9.chunk.js.LICENSE.txt monitoring/static/js/1670.886ee3f9.chunk.js.LICENSE.txt
        monitoring/static/js/1728.782bd268.chunk.js monitoring/static/js/1728.782bd268.chunk.js
        monitoring/static/js/1758.8e77088d.chunk.js monitoring/static/js/1758.8e77088d.chunk.js
        monitoring/static/js/1758.8e77088d.chunk.js.LICENSE.txt monitoring/static/js/1758.8e77088d.chunk.js.LICENSE.txt
        monitoring/static/js/178.25784a3a.chunk.js monitoring/static/js/178.25784a3a.chunk.js
        monitoring/static/js/182.a977025f.chunk.js monitoring/static/js/182.a977025f.chunk.js
        monitoring/static/js/1854.ed793d70.chunk.js monitoring/static/js/1854.ed793d70.chunk.js
        monitoring/static/js/1854.ed793d70.chunk.js.LICENSE.txt monitoring/static/js/1854.ed793d70.chunk.js.LICENSE.txt
        monitoring/static/js/1855.17a4a673.chunk.js monitoring/static/js/1855.17a4a673.chunk.js
        monitoring/static/js/1886.e1f61c67.chunk.js monitoring/static/js/1886.e1f61c67.chunk.js
        monitoring/static/js/1923.a4cf691b.chunk.js monitoring/static/js/1923.a4cf691b.chunk.js
        monitoring/static/js/1948.47c6f139.chunk.js monitoring/static/js/1948.47c6f139.chunk.js
        monitoring/static/js/1948.47c6f139.chunk.js.LICENSE.txt monitoring/static/js/1948.47c6f139.chunk.js.LICENSE.txt
        monitoring/static/js/1957.b5fe44dd.chunk.js monitoring/static/js/1957.b5fe44dd.chunk.js
        monitoring/static/js/1957.b5fe44dd.chunk.js.LICENSE.txt monitoring/static/js/1957.b5fe44dd.chunk.js.LICENSE.txt
        monitoring/static/js/1971.a2544f07.chunk.js monitoring/static/js/1971.a2544f07.chunk.js
        monitoring/static/js/2039.674765db.chunk.js monitoring/static/js/2039.674765db.chunk.js
        monitoring/static/js/2070.62b9c9e9.chunk.js monitoring/static/js/2070.62b9c9e9.chunk.js
        monitoring/static/js/2100.06ecdc28.chunk.js monitoring/static/js/2100.06ecdc28.chunk.js
        monitoring/static/js/2216.be9ff335.chunk.js monitoring/static/js/2216.be9ff335.chunk.js
        monitoring/static/js/225.66110a65.chunk.js monitoring/static/js/225.66110a65.chunk.js
        monitoring/static/js/2262.2ed64c2e.chunk.js monitoring/static/js/2262.2ed64c2e.chunk.js
        monitoring/static/js/2262.2ed64c2e.chunk.js.LICENSE.txt monitoring/static/js/2262.2ed64c2e.chunk.js.LICENSE.txt
        monitoring/static/js/228.21aa1ffb.chunk.js monitoring/static/js/228.21aa1ffb.chunk.js
        monitoring/static/js/228.21aa1ffb.chunk.js.LICENSE.txt monitoring/static/js/228.21aa1ffb.chunk.js.LICENSE.txt
        monitoring/static/js/2293.c95dbf41.chunk.js monitoring/static/js/2293.c95dbf41.chunk.js
        monitoring/static/js/2293.c95dbf41.chunk.js.LICENSE.txt monitoring/static/js/2293.c95dbf41.chunk.js.LICENSE.txt
        monitoring/static/js/2405.6cb977b5.chunk.js monitoring/static/js/2405.6cb977b5.chunk.js
        monitoring/static/js/2421.b0e53cec.chunk.js monitoring/static/js/2421.b0e53cec.chunk.js
        monitoring/static/js/2455.36823616.chunk.js monitoring/static/js/2455.36823616.chunk.js
        monitoring/static/js/2503.96b0c070.chunk.js monitoring/static/js/2503.96b0c070.chunk.js
        monitoring/static/js/2516.44b079ed.chunk.js monitoring/static/js/2516.44b079ed.chunk.js
        monitoring/static/js/253.35728f09.chunk.js monitoring/static/js/253.35728f09.chunk.js
        monitoring/static/js/2570.8e1850a3.chunk.js monitoring/static/js/2570.8e1850a3.chunk.js
        monitoring/static/js/2570.8e1850a3.chunk.js.LICENSE.txt monitoring/static/js/2570.8e1850a3.chunk.js.LICENSE.txt
        monitoring/static/js/2598.77403ca9.chunk.js monitoring/static/js/2598.77403ca9.chunk.js
        monitoring/static/js/2651.a0376f78.chunk.js monitoring/static/js/2651.a0376f78.chunk.js
        monitoring/static/js/2670.33b83982.chunk.js monitoring/static/js/2670.33b83982.chunk.js
        monitoring/static/js/2734.2f81aa93.chunk.js monitoring/static/js/2734.2f81aa93.chunk.js
        monitoring/static/js/2769.49fdcd1a.chunk.js monitoring/static/js/2769.49fdcd1a.chunk.js
        monitoring/static/js/2769.49fdcd1a.chunk.js.LICENSE.txt monitoring/static/js/2769.49fdcd1a.chunk.js.LICENSE.txt
        monitoring/static/js/2780.3e502f16.chunk.js monitoring/static/js/2780.3e502f16.chunk.js
        monitoring/static/js/2804.6549b96e.chunk.js monitoring/static/js/2804.6549b96e.chunk.js
        monitoring/static/js/2807.2824c79c.chunk.js monitoring/static/js/2807.2824c79c.chunk.js
        monitoring/static/js/2807.2824c79c.chunk.js.LICENSE.txt monitoring/static/js/2807.2824c79c.chunk.js.LICENSE.txt
        monitoring/static/js/2845.2216d105.chunk.js monitoring/static/js/2845.2216d105.chunk.js
        monitoring/static/js/2869.3192ed9a.chunk.js monitoring/static/js/2869.3192ed9a.chunk.js
        monitoring/static/js/2869.3192ed9a.chunk.js.LICENSE.txt monitoring/static/js/2869.3192ed9a.chunk.js.LICENSE.txt
        monitoring/static/js/2871.3d11e695.chunk.js monitoring/static/js/2871.3d11e695.chunk.js
        monitoring/static/js/2967.b531e7aa.chunk.js monitoring/static/js/2967.b531e7aa.chunk.js
        monitoring/static/js/2972.f8a04d68.chunk.js monitoring/static/js/2972.f8a04d68.chunk.js
        monitoring/static/js/2974.36362aa2.chunk.js monitoring/static/js/2974.36362aa2.chunk.js
        monitoring/static/js/3010.b103dfa4.chunk.js monitoring/static/js/3010.b103dfa4.chunk.js
        monitoring/static/js/3010.b103dfa4.chunk.js.LICENSE.txt monitoring/static/js/3010.b103dfa4.chunk.js.LICENSE.txt
        monitoring/static/js/3092.117dd3ef.chunk.js monitoring/static/js/3092.117dd3ef.chunk.js
        monitoring/static/js/3107.902f71ab.chunk.js monitoring/static/js/3107.902f71ab.chunk.js
        monitoring/static/js/3107.902f71ab.chunk.js.LICENSE.txt monitoring/static/js/3107.902f71ab.chunk.js.LICENSE.txt
        monitoring/static/js/3145.cfb85bc6.chunk.js monitoring/static/js/3145.cfb85bc6.chunk.js
        monitoring/static/js/3164.b52af9a7.chunk.js monitoring/static/js/3164.b52af9a7.chunk.js
        monitoring/static/js/3164.b52af9a7.chunk.js.LICENSE.txt monitoring/static/js/3164.b52af9a7.chunk.js.LICENSE.txt
        monitoring/static/js/3166.95b28d63.chunk.js monitoring/static/js/3166.95b28d63.chunk.js
        monitoring/static/js/3166.95b28d63.chunk.js.LICENSE.txt monitoring/static/js/3166.95b28d63.chunk.js.LICENSE.txt
        monitoring/static/js/3209.c7f538f3.chunk.js monitoring/static/js/3209.c7f538f3.chunk.js
        monitoring/static/js/323.e36b8268.chunk.js monitoring/static/js/323.e36b8268.chunk.js
        monitoring/static/js/3230.ce6d3a8f.chunk.js monitoring/static/js/3230.ce6d3a8f.chunk.js
        monitoring/static/js/3230.ce6d3a8f.chunk.js.LICENSE.txt monitoring/static/js/3230.ce6d3a8f.chunk.js.LICENSE.txt
        monitoring/static/js/3235.884b5b99.chunk.js monitoring/static/js/3235.884b5b99.chunk.js
        monitoring/static/js/328.ff68c87b.chunk.js monitoring/static/js/328.ff68c87b.chunk.js
        monitoring/static/js/3283.59bb81e0.chunk.js monitoring/static/js/3283.59bb81e0.chunk.js
        monitoring/static/js/3366.7739db1f.chunk.js monitoring/static/js/3366.7739db1f.chunk.js
        monitoring/static/js/3421.1b907a2a.chunk.js monitoring/static/js/3421.1b907a2a.chunk.js
        monitoring/static/js/3421.1b907a2a.chunk.js.LICENSE.txt monitoring/static/js/3421.1b907a2a.chunk.js.LICENSE.txt
        monitoring/static/js/346.f47bbaef.chunk.js monitoring/static/js/346.f47bbaef.chunk.js
        monitoring/static/js/346.f47bbaef.chunk.js.LICENSE.txt monitoring/static/js/346.f47bbaef.chunk.js.LICENSE.txt
        monitoring/static/js/3493.b911f175.chunk.js monitoring/static/js/3493.b911f175.chunk.js
        monitoring/static/js/3495.77665cfd.chunk.js monitoring/static/js/3495.77665cfd.chunk.js
        monitoring/static/js/3520.5df2f9dc.chunk.js monitoring/static/js/3520.5df2f9dc.chunk.js
        monitoring/static/js/3607.6bedcc44.chunk.js monitoring/static/js/3607.6bedcc44.chunk.js
        monitoring/static/js/3643.bec8ad3c.chunk.js monitoring/static/js/3643.bec8ad3c.chunk.js
        monitoring/static/js/3768.988c811a.chunk.js monitoring/static/js/3768.988c811a.chunk.js
        monitoring/static/js/3812.62e6074c.chunk.js monitoring/static/js/3812.62e6074c.chunk.js
        monitoring/static/js/3828.a61513fa.chunk.js monitoring/static/js/3828.a61513fa.chunk.js
        monitoring/static/js/3830.4c0547b6.chunk.js monitoring/static/js/3830.4c0547b6.chunk.js
        monitoring/static/js/3883.9513e6a0.chunk.js monitoring/static/js/3883.9513e6a0.chunk.js
        monitoring/static/js/3950.57b0da11.chunk.js monitoring/static/js/3950.57b0da11.chunk.js
        monitoring/static/js/3972.1d8f063d.chunk.js monitoring/static/js/3972.1d8f063d.chunk.js
        monitoring/static/js/3972.1d8f063d.chunk.js.LICENSE.txt monitoring/static/js/3972.1d8f063d.chunk.js.LICENSE.txt
        monitoring/static/js/3997.030699b5.chunk.js monitoring/static/js/3997.030699b5.chunk.js
        monitoring/static/js/3998.051d103c.chunk.js monitoring/static/js/3998.051d103c.chunk.js
        monitoring/static/js/4006.183d37e4.chunk.js monitoring/static/js/4006.183d37e4.chunk.js
        monitoring/static/js/4018.76940440.chunk.js monitoring/static/js/4018.76940440.chunk.js
        monitoring/static/js/4023.1cf957ab.chunk.js monitoring/static/js/4023.1cf957ab.chunk.js
        monitoring/static/js/4040.6f8f7155.chunk.js monitoring/static/js/4040.6f8f7155.chunk.js
        monitoring/static/js/4066.de4c1e02.chunk.js monitoring/static/js/4066.de4c1e02.chunk.js
        monitoring/static/js/4066.de4c1e02.chunk.js.LICENSE.txt monitoring/static/js/4066.de4c1e02.chunk.js.LICENSE.txt
        monitoring/static/js/4087.46473bf5.chunk.js monitoring/static/js/4087.46473bf5.chunk.js
        monitoring/static/js/4087.46473bf5.chunk.js.LICENSE.txt monitoring/static/js/4087.46473bf5.chunk.js.LICENSE.txt
        monitoring/static/js/4099.0a311abf.chunk.js monitoring/static/js/4099.0a311abf.chunk.js
        monitoring/static/js/4099.0a311abf.chunk.js.LICENSE.txt monitoring/static/js/4099.0a311abf.chunk.js.LICENSE.txt
        monitoring/static/js/4175.62734866.chunk.js monitoring/static/js/4175.62734866.chunk.js
        monitoring/static/js/4222.d4ec5a9d.chunk.js monitoring/static/js/4222.d4ec5a9d.chunk.js
        monitoring/static/js/4222.d4ec5a9d.chunk.js.LICENSE.txt monitoring/static/js/4222.d4ec5a9d.chunk.js.LICENSE.txt
        monitoring/static/js/4226.9f88ef38.chunk.js monitoring/static/js/4226.9f88ef38.chunk.js
        monitoring/static/js/4231.14c1bca0.chunk.js monitoring/static/js/4231.14c1bca0.chunk.js
        monitoring/static/js/4231.14c1bca0.chunk.js.LICENSE.txt monitoring/static/js/4231.14c1bca0.chunk.js.LICENSE.txt
        monitoring/static/js/4254.9abe8ed3.chunk.js monitoring/static/js/4254.9abe8ed3.chunk.js
        monitoring/static/js/4320.f2de8175.chunk.js monitoring/static/js/4320.f2de8175.chunk.js
        monitoring/static/js/4324.8b4dfeeb.chunk.js monitoring/static/js/4324.8b4dfeeb.chunk.js
        monitoring/static/js/4408.4cb113d4.chunk.js monitoring/static/js/4408.4cb113d4.chunk.js
        monitoring/static/js/4413.424607bb.chunk.js monitoring/static/js/4413.424607bb.chunk.js
        monitoring/static/js/4442.9a979278.chunk.js monitoring/static/js/4442.9a979278.chunk.js
        monitoring/static/js/4463.e01e2d73.chunk.js monitoring/static/js/4463.e01e2d73.chunk.js
        monitoring/static/js/4465.a054353e.chunk.js monitoring/static/js/4465.a054353e.chunk.js
        monitoring/static/js/4465.a054353e.chunk.js.LICENSE.txt monitoring/static/js/4465.a054353e.chunk.js.LICENSE.txt
        monitoring/static/js/4503.fa229fbe.chunk.js monitoring/static/js/4503.fa229fbe.chunk.js
        monitoring/static/js/452.004a8d50.chunk.js monitoring/static/js/452.004a8d50.chunk.js
        monitoring/static/js/453.ea7639b5.chunk.js monitoring/static/js/453.ea7639b5.chunk.js
        monitoring/static/js/453.ea7639b5.chunk.js.LICENSE.txt monitoring/static/js/453.ea7639b5.chunk.js.LICENSE.txt
        monitoring/static/js/4535.457b09fd.chunk.js monitoring/static/js/4535.457b09fd.chunk.js
        monitoring/static/js/4535.457b09fd.chunk.js.LICENSE.txt monitoring/static/js/4535.457b09fd.chunk.js.LICENSE.txt
        monitoring/static/js/4563.c214a5dd.chunk.js monitoring/static/js/4563.c214a5dd.chunk.js
        monitoring/static/js/4609.28ef81ef.chunk.js monitoring/static/js/4609.28ef81ef.chunk.js
        monitoring/static/js/4609.28ef81ef.chunk.js.LICENSE.txt monitoring/static/js/4609.28ef81ef.chunk.js.LICENSE.txt
        monitoring/static/js/4628.388e99ac.chunk.js monitoring/static/js/4628.388e99ac.chunk.js
        monitoring/static/js/4628.388e99ac.chunk.js.LICENSE.txt monitoring/static/js/4628.388e99ac.chunk.js.LICENSE.txt
        monitoring/static/js/4639.a666f082.chunk.js monitoring/static/js/4639.a666f082.chunk.js
        monitoring/static/js/4657.793f8fff.chunk.js monitoring/static/js/4657.793f8fff.chunk.js
        monitoring/static/js/4712.68848e03.chunk.js monitoring/static/js/4712.68848e03.chunk.js
        monitoring/static/js/4723.b5ccc693.chunk.js monitoring/static/js/4723.b5ccc693.chunk.js
        monitoring/static/js/4779.f5b56e5a.chunk.js monitoring/static/js/4779.f5b56e5a.chunk.js
        monitoring/static/js/4806.cff0f21e.chunk.js monitoring/static/js/4806.cff0f21e.chunk.js
        monitoring/static/js/4808.382260ae.chunk.js monitoring/static/js/4808.382260ae.chunk.js
        monitoring/static/js/4859.5bb93a34.chunk.js monitoring/static/js/4859.5bb93a34.chunk.js
        monitoring/static/js/4859.5bb93a34.chunk.js.LICENSE.txt monitoring/static/js/4859.5bb93a34.chunk.js.LICENSE.txt
        monitoring/static/js/5014.51de99f4.chunk.js monitoring/static/js/5014.51de99f4.chunk.js
        monitoring/static/js/5066.a0bc3ca7.chunk.js monitoring/static/js/5066.a0bc3ca7.chunk.js
        monitoring/static/js/5070.58b434a0.chunk.js monitoring/static/js/5070.58b434a0.chunk.js
        monitoring/static/js/511.34323faa.chunk.js monitoring/static/js/511.34323faa.chunk.js
        monitoring/static/js/511.34323faa.chunk.js.LICENSE.txt monitoring/static/js/511.34323faa.chunk.js.LICENSE.txt
        monitoring/static/js/5124.661ee0ac.chunk.js monitoring/static/js/5124.661ee0ac.chunk.js
        monitoring/static/js/5124.661ee0ac.chunk.js.LICENSE.txt monitoring/static/js/5124.661ee0ac.chunk.js.LICENSE.txt
        monitoring/static/js/5130.12a9aaae.chunk.js monitoring/static/js/5130.12a9aaae.chunk.js
        monitoring/static/js/5154.e5b3bec6.chunk.js monitoring/static/js/5154.e5b3bec6.chunk.js
        monitoring/static/js/5154.e5b3bec6.chunk.js.LICENSE.txt monitoring/static/js/5154.e5b3bec6.chunk.js.LICENSE.txt
        monitoring/static/js/5160.17372d9c.chunk.js monitoring/static/js/5160.17372d9c.chunk.js
        monitoring/static/js/5160.17372d9c.chunk.js.LICENSE.txt monitoring/static/js/5160.17372d9c.chunk.js.LICENSE.txt
        monitoring/static/js/5203.dbaf6737.chunk.js monitoring/static/js/5203.dbaf6737.chunk.js
        monitoring/static/js/521.4d640434.chunk.js monitoring/static/js/521.4d640434.chunk.js
        monitoring/static/js/521.4d640434.chunk.js.LICENSE.txt monitoring/static/js/521.4d640434.chunk.js.LICENSE.txt
        monitoring/static/js/5257.020aa417.chunk.js monitoring/static/js/5257.020aa417.chunk.js
        monitoring/static/js/5319.acb1fb2c.chunk.js monitoring/static/js/5319.acb1fb2c.chunk.js
        monitoring/static/js/5319.acb1fb2c.chunk.js.LICENSE.txt monitoring/static/js/5319.acb1fb2c.chunk.js.LICENSE.txt
        monitoring/static/js/5426.c44b6444.chunk.js monitoring/static/js/5426.c44b6444.chunk.js
        monitoring/static/js/5467.23a85e74.chunk.js monitoring/static/js/5467.23a85e74.chunk.js
        monitoring/static/js/5467.23a85e74.chunk.js.LICENSE.txt monitoring/static/js/5467.23a85e74.chunk.js.LICENSE.txt
        monitoring/static/js/5484.c5ede353.chunk.js monitoring/static/js/5484.c5ede353.chunk.js
        monitoring/static/js/5484.c5ede353.chunk.js.LICENSE.txt monitoring/static/js/5484.c5ede353.chunk.js.LICENSE.txt
        monitoring/static/js/5503.ecc3bcca.chunk.js monitoring/static/js/5503.ecc3bcca.chunk.js
        monitoring/static/js/5573.6b78d499.chunk.js monitoring/static/js/5573.6b78d499.chunk.js
        monitoring/static/js/5605.49c4cf85.chunk.js monitoring/static/js/5605.49c4cf85.chunk.js
        monitoring/static/js/5605.49c4cf85.chunk.js.LICENSE.txt monitoring/static/js/5605.49c4cf85.chunk.js.LICENSE.txt
        monitoring/static/js/5641.b3b5ddf6.chunk.js monitoring/static/js/5641.b3b5ddf6.chunk.js
        monitoring/static/js/5682.966a8cc0.chunk.js monitoring/static/js/5682.966a8cc0.chunk.js
        monitoring/static/js/5682.966a8cc0.chunk.js.LICENSE.txt monitoring/static/js/5682.966a8cc0.chunk.js.LICENSE.txt
        monitoring/static/js/5685.d6c42c56.chunk.js monitoring/static/js/5685.d6c42c56.chunk.js
        monitoring/static/js/5685.d6c42c56.chunk.js.LICENSE.txt monitoring/static/js/5685.d6c42c56.chunk.js.LICENSE.txt
        monitoring/static/js/5748.d17b947f.chunk.js monitoring/static/js/5748.d17b947f.chunk.js
        monitoring/static/js/579.ff7e5b95.chunk.js monitoring/static/js/579.ff7e5b95.chunk.js
        monitoring/static/js/5821.269b9961.chunk.js monitoring/static/js/5821.269b9961.chunk.js
        monitoring/static/js/592.07f568c9.chunk.js monitoring/static/js/592.07f568c9.chunk.js
        monitoring/static/js/5957.22d94683.chunk.js monitoring/static/js/5957.22d94683.chunk.js
        monitoring/static/js/5982.b7dba432.chunk.js monitoring/static/js/5982.b7dba432.chunk.js
        monitoring/static/js/6010.23d4bb8d.chunk.js monitoring/static/js/6010.23d4bb8d.chunk.js
        monitoring/static/js/6010.23d4bb8d.chunk.js.LICENSE.txt monitoring/static/js/6010.23d4bb8d.chunk.js.LICENSE.txt
        monitoring/static/js/6062.6eaa8611.chunk.js monitoring/static/js/6062.6eaa8611.chunk.js
        monitoring/static/js/6062.6eaa8611.chunk.js.LICENSE.txt monitoring/static/js/6062.6eaa8611.chunk.js.LICENSE.txt
        monitoring/static/js/6070.d22f533e.chunk.js monitoring/static/js/6070.d22f533e.chunk.js
        monitoring/static/js/6079.95240888.chunk.js monitoring/static/js/6079.95240888.chunk.js
        monitoring/static/js/610.307fc3ff.chunk.js monitoring/static/js/610.307fc3ff.chunk.js
        monitoring/static/js/6118.9403a72b.chunk.js monitoring/static/js/6118.9403a72b.chunk.js
        monitoring/static/js/6118.9403a72b.chunk.js.LICENSE.txt monitoring/static/js/6118.9403a72b.chunk.js.LICENSE.txt
        monitoring/static/js/6170.f5db6881.chunk.js monitoring/static/js/6170.f5db6881.chunk.js
        monitoring/static/js/6246.acd51635.chunk.js monitoring/static/js/6246.acd51635.chunk.js
        monitoring/static/js/6246.acd51635.chunk.js.LICENSE.txt monitoring/static/js/6246.acd51635.chunk.js.LICENSE.txt
        monitoring/static/js/6289.ce9965dc.chunk.js monitoring/static/js/6289.ce9965dc.chunk.js
        monitoring/static/js/6289.ce9965dc.chunk.js.LICENSE.txt monitoring/static/js/6289.ce9965dc.chunk.js.LICENSE.txt
        monitoring/static/js/6292.fee79b86.chunk.js monitoring/static/js/6292.fee79b86.chunk.js
        monitoring/static/js/633.834b6a1c.chunk.js monitoring/static/js/633.834b6a1c.chunk.js
        monitoring/static/js/6332.982e12ce.chunk.js monitoring/static/js/6332.982e12ce.chunk.js
        monitoring/static/js/6395.82fdc368.chunk.js monitoring/static/js/6395.82fdc368.chunk.js
        monitoring/static/js/6435.15f0b61d.chunk.js monitoring/static/js/6435.15f0b61d.chunk.js
        monitoring/static/js/6435.15f0b61d.chunk.js.LICENSE.txt monitoring/static/js/6435.15f0b61d.chunk.js.LICENSE.txt
        monitoring/static/js/6595.53cb237b.chunk.js monitoring/static/js/6595.53cb237b.chunk.js
        monitoring/static/js/6625.1d36f68b.chunk.js monitoring/static/js/6625.1d36f68b.chunk.js
        monitoring/static/js/6659.4c0edc60.chunk.js monitoring/static/js/6659.4c0edc60.chunk.js
        monitoring/static/js/6659.4c0edc60.chunk.js.LICENSE.txt monitoring/static/js/6659.4c0edc60.chunk.js.LICENSE.txt
        monitoring/static/js/6660.ad1074e6.chunk.js monitoring/static/js/6660.ad1074e6.chunk.js
        monitoring/static/js/6663.26d7c75f.chunk.js monitoring/static/js/6663.26d7c75f.chunk.js
        monitoring/static/js/6679.5a32e19d.chunk.js monitoring/static/js/6679.5a32e19d.chunk.js
        monitoring/static/js/6698.7c473cb5.chunk.js monitoring/static/js/6698.7c473cb5.chunk.js
        monitoring/static/js/6698.7c473cb5.chunk.js.LICENSE.txt monitoring/static/js/6698.7c473cb5.chunk.js.LICENSE.txt
        monitoring/static/js/6731.36cc76de.chunk.js monitoring/static/js/6731.36cc76de.chunk.js
        monitoring/static/js/6789.febacb48.chunk.js monitoring/static/js/6789.febacb48.chunk.js
        monitoring/static/js/682.229cd996.chunk.js monitoring/static/js/682.229cd996.chunk.js
        monitoring/static/js/682.229cd996.chunk.js.LICENSE.txt monitoring/static/js/682.229cd996.chunk.js.LICENSE.txt
        monitoring/static/js/6845.e64bd413.chunk.js monitoring/static/js/6845.e64bd413.chunk.js
        monitoring/static/js/6866.ea949d3d.chunk.js monitoring/static/js/6866.ea949d3d.chunk.js
        monitoring/static/js/689.e9ec08fb.chunk.js monitoring/static/js/689.e9ec08fb.chunk.js
        monitoring/static/js/6898.25df1470.chunk.js monitoring/static/js/6898.25df1470.chunk.js
        monitoring/static/js/6898.25df1470.chunk.js.LICENSE.txt monitoring/static/js/6898.25df1470.chunk.js.LICENSE.txt
        monitoring/static/js/6914.3b644505.chunk.js monitoring/static/js/6914.3b644505.chunk.js
        monitoring/static/js/6914.3b644505.chunk.js.LICENSE.txt monitoring/static/js/6914.3b644505.chunk.js.LICENSE.txt
        monitoring/static/js/6927.a0faae6b.chunk.js monitoring/static/js/6927.a0faae6b.chunk.js
        monitoring/static/js/6943.a900c8d8.chunk.js monitoring/static/js/6943.a900c8d8.chunk.js
        monitoring/static/js/6953.3f8107dd.chunk.js monitoring/static/js/6953.3f8107dd.chunk.js
        monitoring/static/js/6953.3f8107dd.chunk.js.LICENSE.txt monitoring/static/js/6953.3f8107dd.chunk.js.LICENSE.txt
        monitoring/static/js/7043.e2958b70.chunk.js monitoring/static/js/7043.e2958b70.chunk.js
        monitoring/static/js/7043.e2958b70.chunk.js.LICENSE.txt monitoring/static/js/7043.e2958b70.chunk.js.LICENSE.txt
        monitoring/static/js/7054.3701f39e.chunk.js monitoring/static/js/7054.3701f39e.chunk.js
        monitoring/static/js/7076.99b06857.chunk.js monitoring/static/js/7076.99b06857.chunk.js
        monitoring/static/js/7083.04557a73.chunk.js monitoring/static/js/7083.04557a73.chunk.js
        monitoring/static/js/7083.04557a73.chunk.js.LICENSE.txt monitoring/static/js/7083.04557a73.chunk.js.LICENSE.txt
        monitoring/static/js/7132.d1397593.chunk.js monitoring/static/js/7132.d1397593.chunk.js
        monitoring/static/js/7199.fa3c4603.chunk.js monitoring/static/js/7199.fa3c4603.chunk.js
        monitoring/static/js/7245.79bffd2e.chunk.js monitoring/static/js/7245.79bffd2e.chunk.js
        monitoring/static/js/7273.1cfa7ba2.chunk.js monitoring/static/js/7273.1cfa7ba2.chunk.js
        monitoring/static/js/7275.84809a86.chunk.js monitoring/static/js/7275.84809a86.chunk.js
        monitoring/static/js/7289.188a7958.chunk.js monitoring/static/js/7289.188a7958.chunk.js
        monitoring/static/js/7289.188a7958.chunk.js.LICENSE.txt monitoring/static/js/7289.188a7958.chunk.js.LICENSE.txt
        monitoring/static/js/7367.37be1f98.chunk.js monitoring/static/js/7367.37be1f98.chunk.js
        monitoring/static/js/7439.9de88a36.chunk.js monitoring/static/js/7439.9de88a36.chunk.js
        monitoring/static/js/7441.8f4b4d65.chunk.js monitoring/static/js/7441.8f4b4d65.chunk.js
        monitoring/static/js/7441.8f4b4d65.chunk.js.LICENSE.txt monitoring/static/js/7441.8f4b4d65.chunk.js.LICENSE.txt
        monitoring/static/js/7443.85993ad1.chunk.js monitoring/static/js/7443.85993ad1.chunk.js
        monitoring/static/js/7446.c434978e.chunk.js monitoring/static/js/7446.c434978e.chunk.js
        monitoring/static/js/7512.923733a2.chunk.js monitoring/static/js/7512.923733a2.chunk.js
        monitoring/static/js/753.7b0f09c6.chunk.js monitoring/static/js/753.7b0f09c6.chunk.js
        monitoring/static/js/7628.66f1aa47.chunk.js monitoring/static/js/7628.66f1aa47.chunk.js
        monitoring/static/js/7628.66f1aa47.chunk.js.LICENSE.txt monitoring/static/js/7628.66f1aa47.chunk.js.LICENSE.txt
        monitoring/static/js/7656.527a98cb.chunk.js monitoring/static/js/7656.527a98cb.chunk.js
        monitoring/static/js/766.70c23808.chunk.js monitoring/static/js/766.70c23808.chunk.js
        monitoring/static/js/766.70c23808.chunk.js.LICENSE.txt monitoring/static/js/766.70c23808.chunk.js.LICENSE.txt
        monitoring/static/js/7690.ec833ac0.chunk.js monitoring/static/js/7690.ec833ac0.chunk.js
        monitoring/static/js/7690.ec833ac0.chunk.js.LICENSE.txt monitoring/static/js/7690.ec833ac0.chunk.js.LICENSE.txt
        monitoring/static/js/778.af57bf4a.chunk.js monitoring/static/js/778.af57bf4a.chunk.js
        monitoring/static/js/778.af57bf4a.chunk.js.LICENSE.txt monitoring/static/js/778.af57bf4a.chunk.js.LICENSE.txt
        monitoring/static/js/7828.5627775e.chunk.js monitoring/static/js/7828.5627775e.chunk.js
        monitoring/static/js/7828.5627775e.chunk.js.LICENSE.txt monitoring/static/js/7828.5627775e.chunk.js.LICENSE.txt
        monitoring/static/js/7852.6a83a8ff.chunk.js monitoring/static/js/7852.6a83a8ff.chunk.js
        monitoring/static/js/7852.6a83a8ff.chunk.js.LICENSE.txt monitoring/static/js/7852.6a83a8ff.chunk.js.LICENSE.txt
        monitoring/static/js/7862.970a7935.chunk.js monitoring/static/js/7862.970a7935.chunk.js
        monitoring/static/js/7950.868790bd.chunk.js monitoring/static/js/7950.868790bd.chunk.js
        monitoring/static/js/7981.7e989bc3.chunk.js monitoring/static/js/7981.7e989bc3.chunk.js
        monitoring/static/js/7981.7e989bc3.chunk.js.LICENSE.txt monitoring/static/js/7981.7e989bc3.chunk.js.LICENSE.txt
        monitoring/static/js/7994.5c64203a.chunk.js monitoring/static/js/7994.5c64203a.chunk.js
        monitoring/static/js/8021.341b37dd.chunk.js monitoring/static/js/8021.341b37dd.chunk.js
        monitoring/static/js/8122.fae00246.chunk.js monitoring/static/js/8122.fae00246.chunk.js
        monitoring/static/js/8122.fae00246.chunk.js.LICENSE.txt monitoring/static/js/8122.fae00246.chunk.js.LICENSE.txt
        monitoring/static/js/8169.3b607870.chunk.js monitoring/static/js/8169.3b607870.chunk.js
        monitoring/static/js/824.c56f39b3.chunk.js monitoring/static/js/824.c56f39b3.chunk.js
        monitoring/static/js/8297.67ab228e.chunk.js monitoring/static/js/8297.67ab228e.chunk.js
        monitoring/static/js/8297.67ab228e.chunk.js.LICENSE.txt monitoring/static/js/8297.67ab228e.chunk.js.LICENSE.txt
        monitoring/static/js/8329.e74d4179.chunk.js monitoring/static/js/8329.e74d4179.chunk.js
        monitoring/static/js/8329.e74d4179.chunk.js.LICENSE.txt monitoring/static/js/8329.e74d4179.chunk.js.LICENSE.txt
        monitoring/static/js/8332.c2056746.chunk.js monitoring/static/js/8332.c2056746.chunk.js
        monitoring/static/js/8337.93117b59.chunk.js monitoring/static/js/8337.93117b59.chunk.js
        monitoring/static/js/8337.93117b59.chunk.js.LICENSE.txt monitoring/static/js/8337.93117b59.chunk.js.LICENSE.txt
        monitoring/static/js/835.fcf4f3fd.chunk.js monitoring/static/js/835.fcf4f3fd.chunk.js
        monitoring/static/js/835.fcf4f3fd.chunk.js.LICENSE.txt monitoring/static/js/835.fcf4f3fd.chunk.js.LICENSE.txt
        monitoring/static/js/8427.e5a51e3c.chunk.js monitoring/static/js/8427.e5a51e3c.chunk.js
        monitoring/static/js/8427.e5a51e3c.chunk.js.LICENSE.txt monitoring/static/js/8427.e5a51e3c.chunk.js.LICENSE.txt
        monitoring/static/js/8446.e6a23d4b.chunk.js monitoring/static/js/8446.e6a23d4b.chunk.js
        monitoring/static/js/846.c37c0f60.chunk.js monitoring/static/js/846.c37c0f60.chunk.js
        monitoring/static/js/8504.b46947f0.chunk.js monitoring/static/js/8504.b46947f0.chunk.js
        monitoring/static/js/8504.b46947f0.chunk.js.LICENSE.txt monitoring/static/js/8504.b46947f0.chunk.js.LICENSE.txt
        monitoring/static/js/8505.c733306c.chunk.js monitoring/static/js/8505.c733306c.chunk.js
        monitoring/static/js/856.0c5fe91c.chunk.js monitoring/static/js/856.0c5fe91c.chunk.js
        monitoring/static/js/8606.3593957e.chunk.js monitoring/static/js/8606.3593957e.chunk.js
        monitoring/static/js/8606.3593957e.chunk.js.LICENSE.txt monitoring/static/js/8606.3593957e.chunk.js.LICENSE.txt
        monitoring/static/js/862.e5ac847a.chunk.js monitoring/static/js/862.e5ac847a.chunk.js
        monitoring/static/js/8645.08b4eef2.chunk.js monitoring/static/js/8645.08b4eef2.chunk.js
        monitoring/static/js/8726.76eea1ab.chunk.js monitoring/static/js/8726.76eea1ab.chunk.js
        monitoring/static/js/8731.72990525.chunk.js monitoring/static/js/8731.72990525.chunk.js
        monitoring/static/js/8828.d910f13b.chunk.js monitoring/static/js/8828.d910f13b.chunk.js
        monitoring/static/js/8835.e0455f9c.chunk.js monitoring/static/js/8835.e0455f9c.chunk.js
        monitoring/static/js/8835.e0455f9c.chunk.js.LICENSE.txt monitoring/static/js/8835.e0455f9c.chunk.js.LICENSE.txt
        monitoring/static/js/8841.d097b94c.chunk.js monitoring/static/js/8841.d097b94c.chunk.js
        monitoring/static/js/8864.be2503b0.chunk.js monitoring/static/js/8864.be2503b0.chunk.js
        monitoring/static/js/8908.eb7bba4b.chunk.js monitoring/static/js/8908.eb7bba4b.chunk.js
        monitoring/static/js/8908.eb7bba4b.chunk.js.LICENSE.txt monitoring/static/js/8908.eb7bba4b.chunk.js.LICENSE.txt
        monitoring/static/js/896.a21d0b2a.chunk.js monitoring/static/js/896.a21d0b2a.chunk.js
        monitoring/static/js/9008.d4b4f454.chunk.js monitoring/static/js/9008.d4b4f454.chunk.js
        monitoring/static/js/9101.c3da85f5.chunk.js monitoring/static/js/9101.c3da85f5.chunk.js
        monitoring/static/js/9139.d6e566d7.chunk.js monitoring/static/js/9139.d6e566d7.chunk.js
        monitoring/static/js/9350.692675cc.chunk.js monitoring/static/js/9350.692675cc.chunk.js
        monitoring/static/js/9352.9d3dd18e.chunk.js monitoring/static/js/9352.9d3dd18e.chunk.js
        monitoring/static/js/9368.8f2cf4ea.chunk.js monitoring/static/js/9368.8f2cf4ea.chunk.js
        monitoring/static/js/9396.e8ca3d45.chunk.js monitoring/static/js/9396.e8ca3d45.chunk.js
        monitoring/static/js/9396.e8ca3d45.chunk.js.LICENSE.txt monitoring/static/js/9396.e8ca3d45.chunk.js.LICENSE.txt
        monitoring/static/js/9433.0240620f.chunk.js monitoring/static/js/9433.0240620f.chunk.js
        monitoring/static/js/9530.ada06b35.chunk.js monitoring/static/js/9530.ada06b35.chunk.js
        monitoring/static/js/9530.ada06b35.chunk.js.LICENSE.txt monitoring/static/js/9530.ada06b35.chunk.js.LICENSE.txt
        monitoring/static/js/9617.3a063882.chunk.js monitoring/static/js/9617.3a063882.chunk.js
        monitoring/static/js/9625.c44ae47a.chunk.js monitoring/static/js/9625.c44ae47a.chunk.js
        monitoring/static/js/9685.590d0ab0.chunk.js monitoring/static/js/9685.590d0ab0.chunk.js
        monitoring/static/js/9703.c7826405.chunk.js monitoring/static/js/9703.c7826405.chunk.js
        monitoring/static/js/9711.5936b47c.chunk.js monitoring/static/js/9711.5936b47c.chunk.js
        monitoring/static/js/9757.1fa8ec24.chunk.js monitoring/static/js/9757.1fa8ec24.chunk.js
        monitoring/static/js/9765.410b7864.chunk.js monitoring/static/js/9765.410b7864.chunk.js
        monitoring/static/js/9776.c950cc09.chunk.js monitoring/static/js/9776.c950cc09.chunk.js
        monitoring/static/js/9776.c950cc09.chunk.js.LICENSE.txt monitoring/static/js/9776.c950cc09.chunk.js.LICENSE.txt
        monitoring/static/js/9811.52fb63e7.chunk.js monitoring/static/js/9811.52fb63e7.chunk.js
        monitoring/static/js/9811.52fb63e7.chunk.js.LICENSE.txt monitoring/static/js/9811.52fb63e7.chunk.js.LICENSE.txt
        monitoring/static/js/9821.c0703c78.chunk.js monitoring/static/js/9821.c0703c78.chunk.js
        monitoring/static/js/9937.2df3d582.chunk.js monitoring/static/js/9937.2df3d582.chunk.js
        monitoring/static/js/9963.fc7b507c.chunk.js monitoring/static/js/9963.fc7b507c.chunk.js
        monitoring/static/js/9974.57e4b5e1.chunk.js monitoring/static/js/9974.57e4b5e1.chunk.js
        monitoring/static/js/main.7cff0321.js monitoring/static/js/main.7cff0321.js
        monitoring/static/js/main.7cff0321.js.LICENSE.txt monitoring/static/js/main.7cff0321.js.LICENSE.txt
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.762fced46d6cddbda272.ttf monitoring/static/media/codicon.762fced46d6cddbda272.ttf
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
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types
    contrib/libs/yaml-cpp
)

YQL_LAST_ABI_VERSION()

END()
