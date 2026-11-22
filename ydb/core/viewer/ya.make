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
    viewer_bsgroupinfo.cpp
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
    viewer_nodeinfo.cpp
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
    viewer_utils.cpp
    viewer_utils.h
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
        monitoring/static/css/51463.278c95da.css monitoring/static/css/51463.278c95da.css
        monitoring/static/css/async/20505.8b277006.css monitoring/static/css/async/20505.8b277006.css
        monitoring/static/css/async/21498.08816d05.css monitoring/static/css/async/21498.08816d05.css
        monitoring/static/css/async/24007.6342ce96.css monitoring/static/css/async/24007.6342ce96.css
        monitoring/static/css/async/33029.7bbdbe47.css monitoring/static/css/async/33029.7bbdbe47.css
        monitoring/static/css/async/34004.7a77e099.css monitoring/static/css/async/34004.7a77e099.css
        monitoring/static/css/async/48248.2c4e6ced.css monitoring/static/css/async/48248.2c4e6ced.css
        monitoring/static/css/async/66542.c13a7ac7.css monitoring/static/css/async/66542.c13a7ac7.css
        monitoring/static/css/async/67100.54dbbb27.css monitoring/static/css/async/67100.54dbbb27.css
        monitoring/static/css/async/69687.5ed2e1c7.css monitoring/static/css/async/69687.5ed2e1c7.css
        monitoring/static/css/async/71066.37c6b914.css monitoring/static/css/async/71066.37c6b914.css
        monitoring/static/css/async/77099.21b9dd4c.css monitoring/static/css/async/77099.21b9dd4c.css
        monitoring/static/css/async/82670.f6fc1692.css monitoring/static/css/async/82670.f6fc1692.css
        monitoring/static/css/async/90926.7ff7222f.css monitoring/static/css/async/90926.7ff7222f.css
        monitoring/static/css/async/94054.3d394ca6.css monitoring/static/css/async/94054.3d394ca6.css
        monitoring/static/css/async/9426.7919aa47.css monitoring/static/css/async/9426.7919aa47.css
        monitoring/static/css/index.b45946e4.css monitoring/static/css/index.b45946e4.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/51463.a738a014.js monitoring/static/js/51463.a738a014.js
        monitoring/static/js/51463.a738a014.js.LICENSE.txt monitoring/static/js/51463.a738a014.js.LICENSE.txt
        monitoring/static/js/async/10051.a9733718.js monitoring/static/js/async/10051.a9733718.js
        monitoring/static/js/async/10070.88eb6fc2.js monitoring/static/js/async/10070.88eb6fc2.js
        monitoring/static/js/async/10105.15abd4ef.js monitoring/static/js/async/10105.15abd4ef.js
        monitoring/static/js/async/1018.708416d3.js monitoring/static/js/async/1018.708416d3.js
        monitoring/static/js/async/10287.fde4d226.js monitoring/static/js/async/10287.fde4d226.js
        monitoring/static/js/async/10575.926ddac1.js monitoring/static/js/async/10575.926ddac1.js
        monitoring/static/js/async/10755.9b7169cd.js monitoring/static/js/async/10755.9b7169cd.js
        monitoring/static/js/async/10834.256b45f2.js monitoring/static/js/async/10834.256b45f2.js
        monitoring/static/js/async/10834.256b45f2.js.LICENSE.txt monitoring/static/js/async/10834.256b45f2.js.LICENSE.txt
        monitoring/static/js/async/1087.90efce9c.js monitoring/static/js/async/1087.90efce9c.js
        monitoring/static/js/async/11120.0605f60e.js monitoring/static/js/async/11120.0605f60e.js
        monitoring/static/js/async/11120.0605f60e.js.LICENSE.txt monitoring/static/js/async/11120.0605f60e.js.LICENSE.txt
        monitoring/static/js/async/11230.51d73049.js monitoring/static/js/async/11230.51d73049.js
        monitoring/static/js/async/11327.e98eacf3.js monitoring/static/js/async/11327.e98eacf3.js
        monitoring/static/js/async/11360.eb669225.js monitoring/static/js/async/11360.eb669225.js
        monitoring/static/js/async/11400.a9861273.js monitoring/static/js/async/11400.a9861273.js
        monitoring/static/js/async/11400.a9861273.js.LICENSE.txt monitoring/static/js/async/11400.a9861273.js.LICENSE.txt
        monitoring/static/js/async/1146.d306d87e.js monitoring/static/js/async/1146.d306d87e.js
        monitoring/static/js/async/11908.09cec43d.js monitoring/static/js/async/11908.09cec43d.js
        monitoring/static/js/async/12120.343f2af9.js monitoring/static/js/async/12120.343f2af9.js
        monitoring/static/js/async/12120.343f2af9.js.LICENSE.txt monitoring/static/js/async/12120.343f2af9.js.LICENSE.txt
        monitoring/static/js/async/1215.b4ef9b8c.js monitoring/static/js/async/1215.b4ef9b8c.js
        monitoring/static/js/async/12569.ff92d8ef.js monitoring/static/js/async/12569.ff92d8ef.js
        monitoring/static/js/async/12593.9907f999.js monitoring/static/js/async/12593.9907f999.js
        monitoring/static/js/async/12604.66c3ec3c.js monitoring/static/js/async/12604.66c3ec3c.js
        monitoring/static/js/async/12687.addee502.js monitoring/static/js/async/12687.addee502.js
        monitoring/static/js/async/12733.87500907.js monitoring/static/js/async/12733.87500907.js
        monitoring/static/js/async/12822.989c5e40.js monitoring/static/js/async/12822.989c5e40.js
        monitoring/static/js/async/12918.2df2aee7.js monitoring/static/js/async/12918.2df2aee7.js
        monitoring/static/js/async/13118.6282156d.js monitoring/static/js/async/13118.6282156d.js
        monitoring/static/js/async/13505.b0812a71.js monitoring/static/js/async/13505.b0812a71.js
        monitoring/static/js/async/13613.3c768fe3.js monitoring/static/js/async/13613.3c768fe3.js
        monitoring/static/js/async/13887.0114f014.js monitoring/static/js/async/13887.0114f014.js
        monitoring/static/js/async/13961.b4b946ab.js monitoring/static/js/async/13961.b4b946ab.js
        monitoring/static/js/async/14076.d323d5f2.js monitoring/static/js/async/14076.d323d5f2.js
        monitoring/static/js/async/14328.e9edca2d.js monitoring/static/js/async/14328.e9edca2d.js
        monitoring/static/js/async/14328.e9edca2d.js.LICENSE.txt monitoring/static/js/async/14328.e9edca2d.js.LICENSE.txt
        monitoring/static/js/async/14409.a8358ef6.js monitoring/static/js/async/14409.a8358ef6.js
        monitoring/static/js/async/14474.2a27552d.js monitoring/static/js/async/14474.2a27552d.js
        monitoring/static/js/async/14871.dec272fa.js monitoring/static/js/async/14871.dec272fa.js
        monitoring/static/js/async/14961.dd327815.js monitoring/static/js/async/14961.dd327815.js
        monitoring/static/js/async/1512.e887918f.js monitoring/static/js/async/1512.e887918f.js
        monitoring/static/js/async/15288.5c49eccf.js monitoring/static/js/async/15288.5c49eccf.js
        monitoring/static/js/async/15288.5c49eccf.js.LICENSE.txt monitoring/static/js/async/15288.5c49eccf.js.LICENSE.txt
        monitoring/static/js/async/15313.ac879b2b.js monitoring/static/js/async/15313.ac879b2b.js
        monitoring/static/js/async/15454.91c27bbc.js monitoring/static/js/async/15454.91c27bbc.js
        monitoring/static/js/async/15454.91c27bbc.js.LICENSE.txt monitoring/static/js/async/15454.91c27bbc.js.LICENSE.txt
        monitoring/static/js/async/15785.a711a8ab.js monitoring/static/js/async/15785.a711a8ab.js
        monitoring/static/js/async/15800.df0258bd.js monitoring/static/js/async/15800.df0258bd.js
        monitoring/static/js/async/15800.df0258bd.js.LICENSE.txt monitoring/static/js/async/15800.df0258bd.js.LICENSE.txt
        monitoring/static/js/async/15833.4e87a964.js monitoring/static/js/async/15833.4e87a964.js
        monitoring/static/js/async/16047.d56151ff.js monitoring/static/js/async/16047.d56151ff.js
        monitoring/static/js/async/16110.03ea7f3a.js monitoring/static/js/async/16110.03ea7f3a.js
        monitoring/static/js/async/16164.60d351a5.js monitoring/static/js/async/16164.60d351a5.js
        monitoring/static/js/async/16352.79b46055.js monitoring/static/js/async/16352.79b46055.js
        monitoring/static/js/async/16710.613ee5a5.js monitoring/static/js/async/16710.613ee5a5.js
        monitoring/static/js/async/16732.98d2428b.js monitoring/static/js/async/16732.98d2428b.js
        monitoring/static/js/async/17058.3f3fa7e6.js monitoring/static/js/async/17058.3f3fa7e6.js
        monitoring/static/js/async/17265.5e145e34.js monitoring/static/js/async/17265.5e145e34.js
        monitoring/static/js/async/17265.5e145e34.js.LICENSE.txt monitoring/static/js/async/17265.5e145e34.js.LICENSE.txt
        monitoring/static/js/async/17432.c3cd6e2d.js monitoring/static/js/async/17432.c3cd6e2d.js
        monitoring/static/js/async/17958.6dc304f7.js monitoring/static/js/async/17958.6dc304f7.js
        monitoring/static/js/async/18072.c2b8c81e.js monitoring/static/js/async/18072.c2b8c81e.js
        monitoring/static/js/async/18072.c2b8c81e.js.LICENSE.txt monitoring/static/js/async/18072.c2b8c81e.js.LICENSE.txt
        monitoring/static/js/async/18279.6e4dc48a.js monitoring/static/js/async/18279.6e4dc48a.js
        monitoring/static/js/async/18343.6881b1cc.js monitoring/static/js/async/18343.6881b1cc.js
        monitoring/static/js/async/18480.6f8d40b1.js monitoring/static/js/async/18480.6f8d40b1.js
        monitoring/static/js/async/18576.0ea2187d.js monitoring/static/js/async/18576.0ea2187d.js
        monitoring/static/js/async/18576.0ea2187d.js.LICENSE.txt monitoring/static/js/async/18576.0ea2187d.js.LICENSE.txt
        monitoring/static/js/async/18629.13b514af.js monitoring/static/js/async/18629.13b514af.js
        monitoring/static/js/async/1866.5e275e4b.js monitoring/static/js/async/1866.5e275e4b.js
        monitoring/static/js/async/18711.4141ae73.js monitoring/static/js/async/18711.4141ae73.js
        monitoring/static/js/async/18986.f7377009.js monitoring/static/js/async/18986.f7377009.js
        monitoring/static/js/async/19083.fbc0545e.js monitoring/static/js/async/19083.fbc0545e.js
        monitoring/static/js/async/19186.912ef000.js monitoring/static/js/async/19186.912ef000.js
        monitoring/static/js/async/19329.ce09c89f.js monitoring/static/js/async/19329.ce09c89f.js
        monitoring/static/js/async/19480.dbe87d1e.js monitoring/static/js/async/19480.dbe87d1e.js
        monitoring/static/js/async/19480.dbe87d1e.js.LICENSE.txt monitoring/static/js/async/19480.dbe87d1e.js.LICENSE.txt
        monitoring/static/js/async/19497.4707ddd7.js monitoring/static/js/async/19497.4707ddd7.js
        monitoring/static/js/async/19518.696caa81.js monitoring/static/js/async/19518.696caa81.js
        monitoring/static/js/async/19567.909ef733.js monitoring/static/js/async/19567.909ef733.js
        monitoring/static/js/async/19862.d03552ee.js monitoring/static/js/async/19862.d03552ee.js
        monitoring/static/js/async/19976.420eb9a2.js monitoring/static/js/async/19976.420eb9a2.js
        monitoring/static/js/async/19996.b83e2209.js monitoring/static/js/async/19996.b83e2209.js
        monitoring/static/js/async/20056.da6d4537.js monitoring/static/js/async/20056.da6d4537.js
        monitoring/static/js/async/20056.da6d4537.js.LICENSE.txt monitoring/static/js/async/20056.da6d4537.js.LICENSE.txt
        monitoring/static/js/async/20257.537d0cae.js monitoring/static/js/async/20257.537d0cae.js
        monitoring/static/js/async/20505.eed19a8c.js monitoring/static/js/async/20505.eed19a8c.js
        monitoring/static/js/async/20571.fcc408fc.js monitoring/static/js/async/20571.fcc408fc.js
        monitoring/static/js/async/20578.f5aa7d26.js monitoring/static/js/async/20578.f5aa7d26.js
        monitoring/static/js/async/21011.ba1ff4e1.js monitoring/static/js/async/21011.ba1ff4e1.js
        monitoring/static/js/async/21098.55fc5c67.js monitoring/static/js/async/21098.55fc5c67.js
        monitoring/static/js/async/21107.3b83db6f.js monitoring/static/js/async/21107.3b83db6f.js
        monitoring/static/js/async/21159.2df2f765.js monitoring/static/js/async/21159.2df2f765.js
        monitoring/static/js/async/21234.acfe0f9f.js monitoring/static/js/async/21234.acfe0f9f.js
        monitoring/static/js/async/2126.cb8279ad.js monitoring/static/js/async/2126.cb8279ad.js
        monitoring/static/js/async/21484.6e8d34f4.js monitoring/static/js/async/21484.6e8d34f4.js
        monitoring/static/js/async/21498.583b76ed.js monitoring/static/js/async/21498.583b76ed.js
        monitoring/static/js/async/21984.24611444.js monitoring/static/js/async/21984.24611444.js
        monitoring/static/js/async/21990.f8fd5087.js monitoring/static/js/async/21990.f8fd5087.js
        monitoring/static/js/async/21993.d9c94afb.js monitoring/static/js/async/21993.d9c94afb.js
        monitoring/static/js/async/22001.7f1d8b74.js monitoring/static/js/async/22001.7f1d8b74.js
        monitoring/static/js/async/22155.38813979.js monitoring/static/js/async/22155.38813979.js
        monitoring/static/js/async/22255.edf33e2d.js monitoring/static/js/async/22255.edf33e2d.js
        monitoring/static/js/async/22276.ce866865.js monitoring/static/js/async/22276.ce866865.js
        monitoring/static/js/async/22704.1bbe60af.js monitoring/static/js/async/22704.1bbe60af.js
        monitoring/static/js/async/22784.a05897a4.js monitoring/static/js/async/22784.a05897a4.js
        monitoring/static/js/async/2288.73f58901.js monitoring/static/js/async/2288.73f58901.js
        monitoring/static/js/async/23779.2f7dfd72.js monitoring/static/js/async/23779.2f7dfd72.js
        monitoring/static/js/async/2384.9eb277d0.js monitoring/static/js/async/2384.9eb277d0.js
        monitoring/static/js/async/24007.be07fa9a.js monitoring/static/js/async/24007.be07fa9a.js
        monitoring/static/js/async/24662.37bcdfd0.js monitoring/static/js/async/24662.37bcdfd0.js
        monitoring/static/js/async/24662.37bcdfd0.js.LICENSE.txt monitoring/static/js/async/24662.37bcdfd0.js.LICENSE.txt
        monitoring/static/js/async/24708.7239dda8.js monitoring/static/js/async/24708.7239dda8.js
        monitoring/static/js/async/24778.d83c7dae.js monitoring/static/js/async/24778.d83c7dae.js
        monitoring/static/js/async/25123.0ad91930.js monitoring/static/js/async/25123.0ad91930.js
        monitoring/static/js/async/25268.e355a497.js monitoring/static/js/async/25268.e355a497.js
        monitoring/static/js/async/25375.69cff890.js monitoring/static/js/async/25375.69cff890.js
        monitoring/static/js/async/25576.f30fd223.js monitoring/static/js/async/25576.f30fd223.js
        monitoring/static/js/async/25576.f30fd223.js.LICENSE.txt monitoring/static/js/async/25576.f30fd223.js.LICENSE.txt
        monitoring/static/js/async/25802.eb11ad58.js monitoring/static/js/async/25802.eb11ad58.js
        monitoring/static/js/async/26174.84be90d9.js monitoring/static/js/async/26174.84be90d9.js
        monitoring/static/js/async/26371.b9185a87.js monitoring/static/js/async/26371.b9185a87.js
        monitoring/static/js/async/26420.83dfdd3b.js monitoring/static/js/async/26420.83dfdd3b.js
        monitoring/static/js/async/26478.67fea4c6.js monitoring/static/js/async/26478.67fea4c6.js
        monitoring/static/js/async/26478.67fea4c6.js.LICENSE.txt monitoring/static/js/async/26478.67fea4c6.js.LICENSE.txt
        monitoring/static/js/async/26521.f9f4d0a3.js monitoring/static/js/async/26521.f9f4d0a3.js
        monitoring/static/js/async/26757.d697f174.js monitoring/static/js/async/26757.d697f174.js
        monitoring/static/js/async/26912.9f639213.js monitoring/static/js/async/26912.9f639213.js
        monitoring/static/js/async/26912.9f639213.js.LICENSE.txt monitoring/static/js/async/26912.9f639213.js.LICENSE.txt
        monitoring/static/js/async/27141.37dfc702.js monitoring/static/js/async/27141.37dfc702.js
        monitoring/static/js/async/27211.ab260633.js monitoring/static/js/async/27211.ab260633.js
        monitoring/static/js/async/27259.1f5fdf27.js monitoring/static/js/async/27259.1f5fdf27.js
        monitoring/static/js/async/27295.69caaec9.js monitoring/static/js/async/27295.69caaec9.js
        monitoring/static/js/async/27327.2b62ea1f.js monitoring/static/js/async/27327.2b62ea1f.js
        monitoring/static/js/async/27426.ba572ce8.js monitoring/static/js/async/27426.ba572ce8.js
        monitoring/static/js/async/27554.da74c5d3.js monitoring/static/js/async/27554.da74c5d3.js
        monitoring/static/js/async/27951.4f3117c3.js monitoring/static/js/async/27951.4f3117c3.js
        monitoring/static/js/async/27973.bb5d9a9d.js monitoring/static/js/async/27973.bb5d9a9d.js
        monitoring/static/js/async/28136.7d6946d0.js monitoring/static/js/async/28136.7d6946d0.js
        monitoring/static/js/async/28266.ccec3d62.js monitoring/static/js/async/28266.ccec3d62.js
        monitoring/static/js/async/2856.9ff297aa.js monitoring/static/js/async/2856.9ff297aa.js
        monitoring/static/js/async/28608.5cccd057.js monitoring/static/js/async/28608.5cccd057.js
        monitoring/static/js/async/2920.8581c94d.js monitoring/static/js/async/2920.8581c94d.js
        monitoring/static/js/async/29274.3b2dcb75.js monitoring/static/js/async/29274.3b2dcb75.js
        monitoring/static/js/async/2937.088df62c.js monitoring/static/js/async/2937.088df62c.js
        monitoring/static/js/async/294.81eae1c7.js monitoring/static/js/async/294.81eae1c7.js
        monitoring/static/js/async/2952.1ae6b8f1.js monitoring/static/js/async/2952.1ae6b8f1.js
        monitoring/static/js/async/2952.1ae6b8f1.js.LICENSE.txt monitoring/static/js/async/2952.1ae6b8f1.js.LICENSE.txt
        monitoring/static/js/async/29909.b7870afd.js monitoring/static/js/async/29909.b7870afd.js
        monitoring/static/js/async/30004.79325650.js monitoring/static/js/async/30004.79325650.js
        monitoring/static/js/async/30111.182e300f.js monitoring/static/js/async/30111.182e300f.js
        monitoring/static/js/async/30435.1a1d89cb.js monitoring/static/js/async/30435.1a1d89cb.js
        monitoring/static/js/async/30465.3464a780.js monitoring/static/js/async/30465.3464a780.js
        monitoring/static/js/async/30633.113102ca.js monitoring/static/js/async/30633.113102ca.js
        monitoring/static/js/async/30699.dfba2cdc.js monitoring/static/js/async/30699.dfba2cdc.js
        monitoring/static/js/async/30933.f2ede36d.js monitoring/static/js/async/30933.f2ede36d.js
        monitoring/static/js/async/30962.2f2e04bb.js monitoring/static/js/async/30962.2f2e04bb.js
        monitoring/static/js/async/30962.2f2e04bb.js.LICENSE.txt monitoring/static/js/async/30962.2f2e04bb.js.LICENSE.txt
        monitoring/static/js/async/31227.48da3d50.js monitoring/static/js/async/31227.48da3d50.js
        monitoring/static/js/async/31380.28b03970.js monitoring/static/js/async/31380.28b03970.js
        monitoring/static/js/async/31380.28b03970.js.LICENSE.txt monitoring/static/js/async/31380.28b03970.js.LICENSE.txt
        monitoring/static/js/async/31475.e18ff6f5.js monitoring/static/js/async/31475.e18ff6f5.js
        monitoring/static/js/async/31502.42ee0465.js monitoring/static/js/async/31502.42ee0465.js
        monitoring/static/js/async/31652.d194abca.js monitoring/static/js/async/31652.d194abca.js
        monitoring/static/js/async/31676.e0b0ba4c.js monitoring/static/js/async/31676.e0b0ba4c.js
        monitoring/static/js/async/31865.43438ace.js monitoring/static/js/async/31865.43438ace.js
        monitoring/static/js/async/31915.3c57d896.js monitoring/static/js/async/31915.3c57d896.js
        monitoring/static/js/async/32103.c4242aa6.js monitoring/static/js/async/32103.c4242aa6.js
        monitoring/static/js/async/32150.5063f0b4.js monitoring/static/js/async/32150.5063f0b4.js
        monitoring/static/js/async/32637.f9b4ce45.js monitoring/static/js/async/32637.f9b4ce45.js
        monitoring/static/js/async/32742.a3e34e31.js monitoring/static/js/async/32742.a3e34e31.js
        monitoring/static/js/async/32953.00a56e32.js monitoring/static/js/async/32953.00a56e32.js
        monitoring/static/js/async/32980.46a9c6e6.js monitoring/static/js/async/32980.46a9c6e6.js
        monitoring/static/js/async/33029.b09f0cc9.js monitoring/static/js/async/33029.b09f0cc9.js
        monitoring/static/js/async/33130.f40a31b9.js monitoring/static/js/async/33130.f40a31b9.js
        monitoring/static/js/async/33278.916e23a1.js monitoring/static/js/async/33278.916e23a1.js
        monitoring/static/js/async/33330.b3f3b19d.js monitoring/static/js/async/33330.b3f3b19d.js
        monitoring/static/js/async/33375.34558e4e.js monitoring/static/js/async/33375.34558e4e.js
        monitoring/static/js/async/33922.498a0a17.js monitoring/static/js/async/33922.498a0a17.js
        monitoring/static/js/async/33922.498a0a17.js.LICENSE.txt monitoring/static/js/async/33922.498a0a17.js.LICENSE.txt
        monitoring/static/js/async/34004.afdeacec.js monitoring/static/js/async/34004.afdeacec.js
        monitoring/static/js/async/34394.78f7b069.js monitoring/static/js/async/34394.78f7b069.js
        monitoring/static/js/async/34543.e7d6e9ae.js monitoring/static/js/async/34543.e7d6e9ae.js
        monitoring/static/js/async/34589.1ee33abe.js monitoring/static/js/async/34589.1ee33abe.js
        monitoring/static/js/async/34646.0fe9152e.js monitoring/static/js/async/34646.0fe9152e.js
        monitoring/static/js/async/34646.0fe9152e.js.LICENSE.txt monitoring/static/js/async/34646.0fe9152e.js.LICENSE.txt
        monitoring/static/js/async/3474.6bd17d6e.js monitoring/static/js/async/3474.6bd17d6e.js
        monitoring/static/js/async/34980.97062b62.js monitoring/static/js/async/34980.97062b62.js
        monitoring/static/js/async/35181.242d09b3.js monitoring/static/js/async/35181.242d09b3.js
        monitoring/static/js/async/35196.4d742da8.js monitoring/static/js/async/35196.4d742da8.js
        monitoring/static/js/async/35529.99144349.js monitoring/static/js/async/35529.99144349.js
        monitoring/static/js/async/35897.5918bf1b.js monitoring/static/js/async/35897.5918bf1b.js
        monitoring/static/js/async/3599.e2f7f56a.js monitoring/static/js/async/3599.e2f7f56a.js
        monitoring/static/js/async/36214.acbe489f.js monitoring/static/js/async/36214.acbe489f.js
        monitoring/static/js/async/3629.e49ce93f.js monitoring/static/js/async/3629.e49ce93f.js
        monitoring/static/js/async/36476.ca579c96.js monitoring/static/js/async/36476.ca579c96.js
        monitoring/static/js/async/366.927812cf.js monitoring/static/js/async/366.927812cf.js
        monitoring/static/js/async/36673.74a7c2b6.js monitoring/static/js/async/36673.74a7c2b6.js
        monitoring/static/js/async/36696.df978491.js monitoring/static/js/async/36696.df978491.js
        monitoring/static/js/async/36696.df978491.js.LICENSE.txt monitoring/static/js/async/36696.df978491.js.LICENSE.txt
        monitoring/static/js/async/36706.b8b141c4.js monitoring/static/js/async/36706.b8b141c4.js
        monitoring/static/js/async/36706.b8b141c4.js.LICENSE.txt monitoring/static/js/async/36706.b8b141c4.js.LICENSE.txt
        monitoring/static/js/async/3676.7a4ed627.js monitoring/static/js/async/3676.7a4ed627.js
        monitoring/static/js/async/3676.7a4ed627.js.LICENSE.txt monitoring/static/js/async/3676.7a4ed627.js.LICENSE.txt
        monitoring/static/js/async/37016.cd28e3ff.js monitoring/static/js/async/37016.cd28e3ff.js
        monitoring/static/js/async/37016.cd28e3ff.js.LICENSE.txt monitoring/static/js/async/37016.cd28e3ff.js.LICENSE.txt
        monitoring/static/js/async/37176.4ad72fb2.js monitoring/static/js/async/37176.4ad72fb2.js
        monitoring/static/js/async/37231.2a4d779b.js monitoring/static/js/async/37231.2a4d779b.js
        monitoring/static/js/async/37291.ce158dc0.js monitoring/static/js/async/37291.ce158dc0.js
        monitoring/static/js/async/37532.3d9e3f5b.js monitoring/static/js/async/37532.3d9e3f5b.js
        monitoring/static/js/async/37608.4b9f16ca.js monitoring/static/js/async/37608.4b9f16ca.js
        monitoring/static/js/async/37608.4b9f16ca.js.LICENSE.txt monitoring/static/js/async/37608.4b9f16ca.js.LICENSE.txt
        monitoring/static/js/async/37702.1aaadb42.js monitoring/static/js/async/37702.1aaadb42.js
        monitoring/static/js/async/37864.ddeae4cd.js monitoring/static/js/async/37864.ddeae4cd.js
        monitoring/static/js/async/3817.2cc705c3.js monitoring/static/js/async/3817.2cc705c3.js
        monitoring/static/js/async/3832.a9e69229.js monitoring/static/js/async/3832.a9e69229.js
        monitoring/static/js/async/3832.a9e69229.js.LICENSE.txt monitoring/static/js/async/3832.a9e69229.js.LICENSE.txt
        monitoring/static/js/async/38984.e3179e2e.js monitoring/static/js/async/38984.e3179e2e.js
        monitoring/static/js/async/38984.e3179e2e.js.LICENSE.txt monitoring/static/js/async/38984.e3179e2e.js.LICENSE.txt
        monitoring/static/js/async/39004.496abe47.js monitoring/static/js/async/39004.496abe47.js
        monitoring/static/js/async/39004.496abe47.js.LICENSE.txt monitoring/static/js/async/39004.496abe47.js.LICENSE.txt
        monitoring/static/js/async/39075.b6551043.js monitoring/static/js/async/39075.b6551043.js
        monitoring/static/js/async/39160.2f49c8da.js monitoring/static/js/async/39160.2f49c8da.js
        monitoring/static/js/async/39160.2f49c8da.js.LICENSE.txt monitoring/static/js/async/39160.2f49c8da.js.LICENSE.txt
        monitoring/static/js/async/39320.e3dc53ee.js monitoring/static/js/async/39320.e3dc53ee.js
        monitoring/static/js/async/39320.e3dc53ee.js.LICENSE.txt monitoring/static/js/async/39320.e3dc53ee.js.LICENSE.txt
        monitoring/static/js/async/39640.98b68349.js monitoring/static/js/async/39640.98b68349.js
        monitoring/static/js/async/39640.98b68349.js.LICENSE.txt monitoring/static/js/async/39640.98b68349.js.LICENSE.txt
        monitoring/static/js/async/39836.1bf60357.js monitoring/static/js/async/39836.1bf60357.js
        monitoring/static/js/async/399.cad0c644.js monitoring/static/js/async/399.cad0c644.js
        monitoring/static/js/async/39970.77d82e3f.js monitoring/static/js/async/39970.77d82e3f.js
        monitoring/static/js/async/39978.1c61781d.js monitoring/static/js/async/39978.1c61781d.js
        monitoring/static/js/async/40019.053d52ff.js monitoring/static/js/async/40019.053d52ff.js
        monitoring/static/js/async/40132.d949a4a6.js monitoring/static/js/async/40132.d949a4a6.js
        monitoring/static/js/async/40132.d949a4a6.js.LICENSE.txt monitoring/static/js/async/40132.d949a4a6.js.LICENSE.txt
        monitoring/static/js/async/40248.a4fc0653.js monitoring/static/js/async/40248.a4fc0653.js
        monitoring/static/js/async/4045.13c32521.js monitoring/static/js/async/4045.13c32521.js
        monitoring/static/js/async/41321.a57b2e0b.js monitoring/static/js/async/41321.a57b2e0b.js
        monitoring/static/js/async/41343.25e21974.js monitoring/static/js/async/41343.25e21974.js
        monitoring/static/js/async/41914.98471894.js monitoring/static/js/async/41914.98471894.js
        monitoring/static/js/async/42072.bc165999.js monitoring/static/js/async/42072.bc165999.js
        monitoring/static/js/async/42072.bc165999.js.LICENSE.txt monitoring/static/js/async/42072.bc165999.js.LICENSE.txt
        monitoring/static/js/async/42624.31c5e0af.js monitoring/static/js/async/42624.31c5e0af.js
        monitoring/static/js/async/42672.0f9c231e.js monitoring/static/js/async/42672.0f9c231e.js
        monitoring/static/js/async/42869.8c4ea2ba.js monitoring/static/js/async/42869.8c4ea2ba.js
        monitoring/static/js/async/42934.e578d8cb.js monitoring/static/js/async/42934.e578d8cb.js
        monitoring/static/js/async/4338.78668872.js monitoring/static/js/async/4338.78668872.js
        monitoring/static/js/async/43416.843ab86e.js monitoring/static/js/async/43416.843ab86e.js
        monitoring/static/js/async/43416.843ab86e.js.LICENSE.txt monitoring/static/js/async/43416.843ab86e.js.LICENSE.txt
        monitoring/static/js/async/43888.8a404a04.js monitoring/static/js/async/43888.8a404a04.js
        monitoring/static/js/async/44045.4e72a1eb.js monitoring/static/js/async/44045.4e72a1eb.js
        monitoring/static/js/async/44183.5ba80e43.js monitoring/static/js/async/44183.5ba80e43.js
        monitoring/static/js/async/44225.ffa45dd8.js monitoring/static/js/async/44225.ffa45dd8.js
        monitoring/static/js/async/44464.8c828818.js monitoring/static/js/async/44464.8c828818.js
        monitoring/static/js/async/44464.8c828818.js.LICENSE.txt monitoring/static/js/async/44464.8c828818.js.LICENSE.txt
        monitoring/static/js/async/44590.dfe67496.js monitoring/static/js/async/44590.dfe67496.js
        monitoring/static/js/async/44608.9139f530.js monitoring/static/js/async/44608.9139f530.js
        monitoring/static/js/async/44696.a753d4d7.js monitoring/static/js/async/44696.a753d4d7.js
        monitoring/static/js/async/44696.a753d4d7.js.LICENSE.txt monitoring/static/js/async/44696.a753d4d7.js.LICENSE.txt
        monitoring/static/js/async/44986.d2fd8780.js monitoring/static/js/async/44986.d2fd8780.js
        monitoring/static/js/async/45072.ab57a04d.js monitoring/static/js/async/45072.ab57a04d.js
        monitoring/static/js/async/45072.ab57a04d.js.LICENSE.txt monitoring/static/js/async/45072.ab57a04d.js.LICENSE.txt
        monitoring/static/js/async/4547.1f61bb22.js monitoring/static/js/async/4547.1f61bb22.js
        monitoring/static/js/async/45514.e3a993bd.js monitoring/static/js/async/45514.e3a993bd.js
        monitoring/static/js/async/45576.860e688a.js monitoring/static/js/async/45576.860e688a.js
        monitoring/static/js/async/45576.860e688a.js.LICENSE.txt monitoring/static/js/async/45576.860e688a.js.LICENSE.txt
        monitoring/static/js/async/45579.282ad97e.js monitoring/static/js/async/45579.282ad97e.js
        monitoring/static/js/async/45624.46040174.js monitoring/static/js/async/45624.46040174.js
        monitoring/static/js/async/45649.584fdfbd.js monitoring/static/js/async/45649.584fdfbd.js
        monitoring/static/js/async/45672.09169cc1.js monitoring/static/js/async/45672.09169cc1.js
        monitoring/static/js/async/45710.2a16e4d8.js monitoring/static/js/async/45710.2a16e4d8.js
        monitoring/static/js/async/45870.00150ce8.js monitoring/static/js/async/45870.00150ce8.js
        monitoring/static/js/async/46355.effeb220.js monitoring/static/js/async/46355.effeb220.js
        monitoring/static/js/async/46726.0fd8c987.js monitoring/static/js/async/46726.0fd8c987.js
        monitoring/static/js/async/4733.86be3f0b.js monitoring/static/js/async/4733.86be3f0b.js
        monitoring/static/js/async/47417.33caa7ae.js monitoring/static/js/async/47417.33caa7ae.js
        monitoring/static/js/async/47417.33caa7ae.js.LICENSE.txt monitoring/static/js/async/47417.33caa7ae.js.LICENSE.txt
        monitoring/static/js/async/47642.38a8b430.js monitoring/static/js/async/47642.38a8b430.js
        monitoring/static/js/async/47680.62573458.js monitoring/static/js/async/47680.62573458.js
        monitoring/static/js/async/47770.0a98fec4.js monitoring/static/js/async/47770.0a98fec4.js
        monitoring/static/js/async/4778.2647f292.js monitoring/static/js/async/4778.2647f292.js
        monitoring/static/js/async/4778.2647f292.js.LICENSE.txt monitoring/static/js/async/4778.2647f292.js.LICENSE.txt
        monitoring/static/js/async/47959.c1dc98f1.js monitoring/static/js/async/47959.c1dc98f1.js
        monitoring/static/js/async/47964.c2c2fec2.js monitoring/static/js/async/47964.c2c2fec2.js
        monitoring/static/js/async/48134.c8d60797.js monitoring/static/js/async/48134.c8d60797.js
        monitoring/static/js/async/48392.05e3cd09.js monitoring/static/js/async/48392.05e3cd09.js
        monitoring/static/js/async/48521.19c5261c.js monitoring/static/js/async/48521.19c5261c.js
        monitoring/static/js/async/48868.da8724e3.js monitoring/static/js/async/48868.da8724e3.js
        monitoring/static/js/async/4898.7aba8e0d.js monitoring/static/js/async/4898.7aba8e0d.js
        monitoring/static/js/async/49126.4b7c0f87.js monitoring/static/js/async/49126.4b7c0f87.js
        monitoring/static/js/async/49176.3cda87ef.js monitoring/static/js/async/49176.3cda87ef.js
        monitoring/static/js/async/49176.3cda87ef.js.LICENSE.txt monitoring/static/js/async/49176.3cda87ef.js.LICENSE.txt
        monitoring/static/js/async/49360.40b332be.js monitoring/static/js/async/49360.40b332be.js
        monitoring/static/js/async/49419.a9b58875.js monitoring/static/js/async/49419.a9b58875.js
        monitoring/static/js/async/49462.9208ed20.js monitoring/static/js/async/49462.9208ed20.js
        monitoring/static/js/async/49570.da8a7604.js monitoring/static/js/async/49570.da8a7604.js
        monitoring/static/js/async/49578.68ecaf08.js monitoring/static/js/async/49578.68ecaf08.js
        monitoring/static/js/async/49640.d8d5d187.js monitoring/static/js/async/49640.d8d5d187.js
        monitoring/static/js/async/498.982cc1ac.js monitoring/static/js/async/498.982cc1ac.js
        monitoring/static/js/async/4989.09ad7378.js monitoring/static/js/async/4989.09ad7378.js
        monitoring/static/js/async/49903.98091020.js monitoring/static/js/async/49903.98091020.js
        monitoring/static/js/async/50051.9c5b908a.js monitoring/static/js/async/50051.9c5b908a.js
        monitoring/static/js/async/50079.47733904.js monitoring/static/js/async/50079.47733904.js
        monitoring/static/js/async/50554.81dd4bcf.js monitoring/static/js/async/50554.81dd4bcf.js
        monitoring/static/js/async/50562.d630d2dc.js monitoring/static/js/async/50562.d630d2dc.js
        monitoring/static/js/async/50994.61272e11.js monitoring/static/js/async/50994.61272e11.js
        monitoring/static/js/async/50994.61272e11.js.LICENSE.txt monitoring/static/js/async/50994.61272e11.js.LICENSE.txt
        monitoring/static/js/async/5100.8ee388fe.js monitoring/static/js/async/5100.8ee388fe.js
        monitoring/static/js/async/51142.edaaf295.js monitoring/static/js/async/51142.edaaf295.js
        monitoring/static/js/async/51228.a905fc5e.js monitoring/static/js/async/51228.a905fc5e.js
        monitoring/static/js/async/51228.a905fc5e.js.LICENSE.txt monitoring/static/js/async/51228.a905fc5e.js.LICENSE.txt
        monitoring/static/js/async/51305.cfa4bc98.js monitoring/static/js/async/51305.cfa4bc98.js
        monitoring/static/js/async/51393.acdc4b4f.js monitoring/static/js/async/51393.acdc4b4f.js
        monitoring/static/js/async/51412.7c5131e4.js monitoring/static/js/async/51412.7c5131e4.js
        monitoring/static/js/async/51425.82add6d9.js monitoring/static/js/async/51425.82add6d9.js
        monitoring/static/js/async/51720.7b39b95d.js monitoring/static/js/async/51720.7b39b95d.js
        monitoring/static/js/async/5207.dd595b28.js monitoring/static/js/async/5207.dd595b28.js
        monitoring/static/js/async/52150.a976d268.js monitoring/static/js/async/52150.a976d268.js
        monitoring/static/js/async/52150.a976d268.js.LICENSE.txt monitoring/static/js/async/52150.a976d268.js.LICENSE.txt
        monitoring/static/js/async/52259.c7603fbb.js monitoring/static/js/async/52259.c7603fbb.js
        monitoring/static/js/async/52280.7cbcc84c.js monitoring/static/js/async/52280.7cbcc84c.js
        monitoring/static/js/async/5244.97412088.js monitoring/static/js/async/5244.97412088.js
        monitoring/static/js/async/52505.b0886296.js monitoring/static/js/async/52505.b0886296.js
        monitoring/static/js/async/53013.3f522408.js monitoring/static/js/async/53013.3f522408.js
        monitoring/static/js/async/53236.33a011f9.js monitoring/static/js/async/53236.33a011f9.js
        monitoring/static/js/async/53336.19784abc.js monitoring/static/js/async/53336.19784abc.js
        monitoring/static/js/async/53732.db90dc21.js monitoring/static/js/async/53732.db90dc21.js
        monitoring/static/js/async/53980.1d833c6e.js monitoring/static/js/async/53980.1d833c6e.js
        monitoring/static/js/async/53985.bbc2d40b.js monitoring/static/js/async/53985.bbc2d40b.js
        monitoring/static/js/async/54353.ade54fa1.js monitoring/static/js/async/54353.ade54fa1.js
        monitoring/static/js/async/54374.e90672f2.js monitoring/static/js/async/54374.e90672f2.js
        monitoring/static/js/async/54374.e90672f2.js.LICENSE.txt monitoring/static/js/async/54374.e90672f2.js.LICENSE.txt
        monitoring/static/js/async/54732.80cb8e49.js monitoring/static/js/async/54732.80cb8e49.js
        monitoring/static/js/async/54744.10e57d54.js monitoring/static/js/async/54744.10e57d54.js
        monitoring/static/js/async/54744.10e57d54.js.LICENSE.txt monitoring/static/js/async/54744.10e57d54.js.LICENSE.txt
        monitoring/static/js/async/54820.a42bf2c8.js monitoring/static/js/async/54820.a42bf2c8.js
        monitoring/static/js/async/54833.8df389ea.js monitoring/static/js/async/54833.8df389ea.js
        monitoring/static/js/async/54896.4b8011cd.js monitoring/static/js/async/54896.4b8011cd.js
        monitoring/static/js/async/55292.87e0f161.js monitoring/static/js/async/55292.87e0f161.js
        monitoring/static/js/async/55312.081a06a9.js monitoring/static/js/async/55312.081a06a9.js
        monitoring/static/js/async/55312.081a06a9.js.LICENSE.txt monitoring/static/js/async/55312.081a06a9.js.LICENSE.txt
        monitoring/static/js/async/55940.563e7d79.js monitoring/static/js/async/55940.563e7d79.js
        monitoring/static/js/async/55940.563e7d79.js.LICENSE.txt monitoring/static/js/async/55940.563e7d79.js.LICENSE.txt
        monitoring/static/js/async/56035.393e3e13.js monitoring/static/js/async/56035.393e3e13.js
        monitoring/static/js/async/56094.c17b773d.js monitoring/static/js/async/56094.c17b773d.js
        monitoring/static/js/async/56298.9790c888.js monitoring/static/js/async/56298.9790c888.js
        monitoring/static/js/async/56803.735f8ff9.js monitoring/static/js/async/56803.735f8ff9.js
        monitoring/static/js/async/56888.e3e31e1e.js monitoring/static/js/async/56888.e3e31e1e.js
        monitoring/static/js/async/57033.835f20e3.js monitoring/static/js/async/57033.835f20e3.js
        monitoring/static/js/async/5719.1325dd8c.js monitoring/static/js/async/5719.1325dd8c.js
        monitoring/static/js/async/57327.3ac0ecf9.js monitoring/static/js/async/57327.3ac0ecf9.js
        monitoring/static/js/async/57670.d9acd894.js monitoring/static/js/async/57670.d9acd894.js
        monitoring/static/js/async/57670.d9acd894.js.LICENSE.txt monitoring/static/js/async/57670.d9acd894.js.LICENSE.txt
        monitoring/static/js/async/5771.4b3cdaa2.js monitoring/static/js/async/5771.4b3cdaa2.js
        monitoring/static/js/async/57993.16b669a7.js monitoring/static/js/async/57993.16b669a7.js
        monitoring/static/js/async/58280.6428c3b2.js monitoring/static/js/async/58280.6428c3b2.js
        monitoring/static/js/async/58280.6428c3b2.js.LICENSE.txt monitoring/static/js/async/58280.6428c3b2.js.LICENSE.txt
        monitoring/static/js/async/58331.fd38b0cc.js monitoring/static/js/async/58331.fd38b0cc.js
        monitoring/static/js/async/58365.da806e0a.js monitoring/static/js/async/58365.da806e0a.js
        monitoring/static/js/async/58460.e6769ad7.js monitoring/static/js/async/58460.e6769ad7.js
        monitoring/static/js/async/58748.f6b1b749.js monitoring/static/js/async/58748.f6b1b749.js
        monitoring/static/js/async/58748.f6b1b749.js.LICENSE.txt monitoring/static/js/async/58748.f6b1b749.js.LICENSE.txt
        monitoring/static/js/async/58865.34d5feb6.js monitoring/static/js/async/58865.34d5feb6.js
        monitoring/static/js/async/59324.bc9e9264.js monitoring/static/js/async/59324.bc9e9264.js
        monitoring/static/js/async/59324.bc9e9264.js.LICENSE.txt monitoring/static/js/async/59324.bc9e9264.js.LICENSE.txt
        monitoring/static/js/async/59712.7f089cd7.js monitoring/static/js/async/59712.7f089cd7.js
        monitoring/static/js/async/59761.6ea63e79.js monitoring/static/js/async/59761.6ea63e79.js
        monitoring/static/js/async/60292.46b201f6.js monitoring/static/js/async/60292.46b201f6.js
        monitoring/static/js/async/60606.9707b2c7.js monitoring/static/js/async/60606.9707b2c7.js
        monitoring/static/js/async/60619.1cd626b5.js monitoring/static/js/async/60619.1cd626b5.js
        monitoring/static/js/async/60889.f46cb5df.js monitoring/static/js/async/60889.f46cb5df.js
        monitoring/static/js/async/60968.7ac8163c.js monitoring/static/js/async/60968.7ac8163c.js
        monitoring/static/js/async/61089.ea885f38.js monitoring/static/js/async/61089.ea885f38.js
        monitoring/static/js/async/61186.c37df7c2.js monitoring/static/js/async/61186.c37df7c2.js
        monitoring/static/js/async/61195.5a288bf3.js monitoring/static/js/async/61195.5a288bf3.js
        monitoring/static/js/async/61327.b84c6569.js monitoring/static/js/async/61327.b84c6569.js
        monitoring/static/js/async/61593.dc613f67.js monitoring/static/js/async/61593.dc613f67.js
        monitoring/static/js/async/61890.7aeb1567.js monitoring/static/js/async/61890.7aeb1567.js
        monitoring/static/js/async/6196.76b17ea2.js monitoring/static/js/async/6196.76b17ea2.js
        monitoring/static/js/async/62247.4c2d7bda.js monitoring/static/js/async/62247.4c2d7bda.js
        monitoring/static/js/async/62529.b17345fc.js monitoring/static/js/async/62529.b17345fc.js
        monitoring/static/js/async/63047.64caef3a.js monitoring/static/js/async/63047.64caef3a.js
        monitoring/static/js/async/63157.b52ec4c7.js monitoring/static/js/async/63157.b52ec4c7.js
        monitoring/static/js/async/63193.061fb1c8.js monitoring/static/js/async/63193.061fb1c8.js
        monitoring/static/js/async/6344.43299d49.js monitoring/static/js/async/6344.43299d49.js
        monitoring/static/js/async/63763.df0142a4.js monitoring/static/js/async/63763.df0142a4.js
        monitoring/static/js/async/63879.7b15bccc.js monitoring/static/js/async/63879.7b15bccc.js
        monitoring/static/js/async/64323.eed62f7f.js monitoring/static/js/async/64323.eed62f7f.js
        monitoring/static/js/async/64486.f5f926b6.js monitoring/static/js/async/64486.f5f926b6.js
        monitoring/static/js/async/64817.f89430de.js monitoring/static/js/async/64817.f89430de.js
        monitoring/static/js/async/65038.62a2a2b6.js monitoring/static/js/async/65038.62a2a2b6.js
        monitoring/static/js/async/65433.f0394fb0.js monitoring/static/js/async/65433.f0394fb0.js
        monitoring/static/js/async/65558.9d494d63.js monitoring/static/js/async/65558.9d494d63.js
        monitoring/static/js/async/65693.23bbe3eb.js monitoring/static/js/async/65693.23bbe3eb.js
        monitoring/static/js/async/65694.356e7847.js monitoring/static/js/async/65694.356e7847.js
        monitoring/static/js/async/66316.e18d1960.js monitoring/static/js/async/66316.e18d1960.js
        monitoring/static/js/async/66542.c9138e15.js monitoring/static/js/async/66542.c9138e15.js
        monitoring/static/js/async/66542.c9138e15.js.LICENSE.txt monitoring/static/js/async/66542.c9138e15.js.LICENSE.txt
        monitoring/static/js/async/66569.6bf36b09.js monitoring/static/js/async/66569.6bf36b09.js
        monitoring/static/js/async/6662.6b2f4cfb.js monitoring/static/js/async/6662.6b2f4cfb.js
        monitoring/static/js/async/66628.a2d19a01.js monitoring/static/js/async/66628.a2d19a01.js
        monitoring/static/js/async/67100.f4e9911e.js monitoring/static/js/async/67100.f4e9911e.js
        monitoring/static/js/async/6727.3433e51f.js monitoring/static/js/async/6727.3433e51f.js
        monitoring/static/js/async/6728.03e4802a.js monitoring/static/js/async/6728.03e4802a.js
        monitoring/static/js/async/67512.57176614.js monitoring/static/js/async/67512.57176614.js
        monitoring/static/js/async/67512.57176614.js.LICENSE.txt monitoring/static/js/async/67512.57176614.js.LICENSE.txt
        monitoring/static/js/async/67648.33b455c6.js monitoring/static/js/async/67648.33b455c6.js
        monitoring/static/js/async/67810.ab42f0a0.js monitoring/static/js/async/67810.ab42f0a0.js
        monitoring/static/js/async/67878.736ba55d.js monitoring/static/js/async/67878.736ba55d.js
        monitoring/static/js/async/68514.7e8ffd2e.js monitoring/static/js/async/68514.7e8ffd2e.js
        monitoring/static/js/async/68983.1cb72f5d.js monitoring/static/js/async/68983.1cb72f5d.js
        monitoring/static/js/async/69225.c87be69c.js monitoring/static/js/async/69225.c87be69c.js
        monitoring/static/js/async/69297.1fe2e2c6.js monitoring/static/js/async/69297.1fe2e2c6.js
        monitoring/static/js/async/69687.175ecabd.js monitoring/static/js/async/69687.175ecabd.js
        monitoring/static/js/async/69687.175ecabd.js.LICENSE.txt monitoring/static/js/async/69687.175ecabd.js.LICENSE.txt
        monitoring/static/js/async/69688.d33cccca.js monitoring/static/js/async/69688.d33cccca.js
        monitoring/static/js/async/69688.d33cccca.js.LICENSE.txt monitoring/static/js/async/69688.d33cccca.js.LICENSE.txt
        monitoring/static/js/async/69720.34d28115.js monitoring/static/js/async/69720.34d28115.js
        monitoring/static/js/async/69750.567e882e.js monitoring/static/js/async/69750.567e882e.js
        monitoring/static/js/async/70020.b5833264.js monitoring/static/js/async/70020.b5833264.js
        monitoring/static/js/async/70020.b5833264.js.LICENSE.txt monitoring/static/js/async/70020.b5833264.js.LICENSE.txt
        monitoring/static/js/async/70371.77c4a868.js monitoring/static/js/async/70371.77c4a868.js
        monitoring/static/js/async/70419.a87e04c2.js monitoring/static/js/async/70419.a87e04c2.js
        monitoring/static/js/async/70510.3ee497b4.js monitoring/static/js/async/70510.3ee497b4.js
        monitoring/static/js/async/70510.3ee497b4.js.LICENSE.txt monitoring/static/js/async/70510.3ee497b4.js.LICENSE.txt
        monitoring/static/js/async/70520.a96ad60e.js monitoring/static/js/async/70520.a96ad60e.js
        monitoring/static/js/async/70520.a96ad60e.js.LICENSE.txt monitoring/static/js/async/70520.a96ad60e.js.LICENSE.txt
        monitoring/static/js/async/70751.8e4f6725.js monitoring/static/js/async/70751.8e4f6725.js
        monitoring/static/js/async/70859.71b2cc85.js monitoring/static/js/async/70859.71b2cc85.js
        monitoring/static/js/async/71066.98f51549.js monitoring/static/js/async/71066.98f51549.js
        monitoring/static/js/async/71379.baaa4198.js monitoring/static/js/async/71379.baaa4198.js
        monitoring/static/js/async/71381.737e7933.js monitoring/static/js/async/71381.737e7933.js
        monitoring/static/js/async/71387.3cf1f5e0.js monitoring/static/js/async/71387.3cf1f5e0.js
        monitoring/static/js/async/71887.eb23bad8.js monitoring/static/js/async/71887.eb23bad8.js
        monitoring/static/js/async/72019.48576ae6.js monitoring/static/js/async/72019.48576ae6.js
        monitoring/static/js/async/72068.63c7fc67.js monitoring/static/js/async/72068.63c7fc67.js
        monitoring/static/js/async/72069.0782a327.js monitoring/static/js/async/72069.0782a327.js
        monitoring/static/js/async/72285.cd20cb32.js monitoring/static/js/async/72285.cd20cb32.js
        monitoring/static/js/async/72293.c7d6d3be.js monitoring/static/js/async/72293.c7d6d3be.js
        monitoring/static/js/async/72566.0d7357cb.js monitoring/static/js/async/72566.0d7357cb.js
        monitoring/static/js/async/72603.beb6601c.js monitoring/static/js/async/72603.beb6601c.js
        monitoring/static/js/async/73037.91ae707a.js monitoring/static/js/async/73037.91ae707a.js
        monitoring/static/js/async/73041.807a8d57.js monitoring/static/js/async/73041.807a8d57.js
        monitoring/static/js/async/73365.946a7c3f.js monitoring/static/js/async/73365.946a7c3f.js
        monitoring/static/js/async/73537.e171c3d9.js monitoring/static/js/async/73537.e171c3d9.js
        monitoring/static/js/async/73670.f363be2f.js monitoring/static/js/async/73670.f363be2f.js
        monitoring/static/js/async/73802.53aa2a9c.js monitoring/static/js/async/73802.53aa2a9c.js
        monitoring/static/js/async/73992.c1f22df9.js monitoring/static/js/async/73992.c1f22df9.js
        monitoring/static/js/async/73992.c1f22df9.js.LICENSE.txt monitoring/static/js/async/73992.c1f22df9.js.LICENSE.txt
        monitoring/static/js/async/74054.276af662.js monitoring/static/js/async/74054.276af662.js
        monitoring/static/js/async/74381.4c63bf7c.js monitoring/static/js/async/74381.4c63bf7c.js
        monitoring/static/js/async/74772.79a8e6e1.js monitoring/static/js/async/74772.79a8e6e1.js
        monitoring/static/js/async/74772.79a8e6e1.js.LICENSE.txt monitoring/static/js/async/74772.79a8e6e1.js.LICENSE.txt
        monitoring/static/js/async/74794.73d7265a.js monitoring/static/js/async/74794.73d7265a.js
        monitoring/static/js/async/74856.21b0f71c.js monitoring/static/js/async/74856.21b0f71c.js
        monitoring/static/js/async/74856.21b0f71c.js.LICENSE.txt monitoring/static/js/async/74856.21b0f71c.js.LICENSE.txt
        monitoring/static/js/async/75008.e2068bca.js monitoring/static/js/async/75008.e2068bca.js
        monitoring/static/js/async/75008.e2068bca.js.LICENSE.txt monitoring/static/js/async/75008.e2068bca.js.LICENSE.txt
        monitoring/static/js/async/75024.e54cb4e1.js monitoring/static/js/async/75024.e54cb4e1.js
        monitoring/static/js/async/75031.f676df04.js monitoring/static/js/async/75031.f676df04.js
        monitoring/static/js/async/75044.f48cacdb.js monitoring/static/js/async/75044.f48cacdb.js
        monitoring/static/js/async/75044.f48cacdb.js.LICENSE.txt monitoring/static/js/async/75044.f48cacdb.js.LICENSE.txt
        monitoring/static/js/async/75752.75917d72.js monitoring/static/js/async/75752.75917d72.js
        monitoring/static/js/async/75833.034c71a9.js monitoring/static/js/async/75833.034c71a9.js
        monitoring/static/js/async/75959.f4388e3e.js monitoring/static/js/async/75959.f4388e3e.js
        monitoring/static/js/async/7624.7473eff5.js monitoring/static/js/async/7624.7473eff5.js
        monitoring/static/js/async/7624.7473eff5.js.LICENSE.txt monitoring/static/js/async/7624.7473eff5.js.LICENSE.txt
        monitoring/static/js/async/76297.2caf405e.js monitoring/static/js/async/76297.2caf405e.js
        monitoring/static/js/async/77000.18a22fc7.js monitoring/static/js/async/77000.18a22fc7.js
        monitoring/static/js/async/77000.18a22fc7.js.LICENSE.txt monitoring/static/js/async/77000.18a22fc7.js.LICENSE.txt
        monitoring/static/js/async/77099.e01c38c0.js monitoring/static/js/async/77099.e01c38c0.js
        monitoring/static/js/async/77203.6653f62f.js monitoring/static/js/async/77203.6653f62f.js
        monitoring/static/js/async/77388.6fdb4d39.js monitoring/static/js/async/77388.6fdb4d39.js
        monitoring/static/js/async/77388.6fdb4d39.js.LICENSE.txt monitoring/static/js/async/77388.6fdb4d39.js.LICENSE.txt
        monitoring/static/js/async/77526.47edc362.js monitoring/static/js/async/77526.47edc362.js
        monitoring/static/js/async/77526.47edc362.js.LICENSE.txt monitoring/static/js/async/77526.47edc362.js.LICENSE.txt
        monitoring/static/js/async/77648.dd669763.js monitoring/static/js/async/77648.dd669763.js
        monitoring/static/js/async/77791.45a16078.js monitoring/static/js/async/77791.45a16078.js
        monitoring/static/js/async/78017.1eaacca6.js monitoring/static/js/async/78017.1eaacca6.js
        monitoring/static/js/async/78085.23ea4d74.js monitoring/static/js/async/78085.23ea4d74.js
        monitoring/static/js/async/7811.e2f726a0.js monitoring/static/js/async/7811.e2f726a0.js
        monitoring/static/js/async/78407.21a9a4ab.js monitoring/static/js/async/78407.21a9a4ab.js
        monitoring/static/js/async/78458.79ad17cc.js monitoring/static/js/async/78458.79ad17cc.js
        monitoring/static/js/async/78868.81755fb7.js monitoring/static/js/async/78868.81755fb7.js
        monitoring/static/js/async/78925.c73d23fd.js monitoring/static/js/async/78925.c73d23fd.js
        monitoring/static/js/async/78978.d1582df1.js monitoring/static/js/async/78978.d1582df1.js
        monitoring/static/js/async/78978.d1582df1.js.LICENSE.txt monitoring/static/js/async/78978.d1582df1.js.LICENSE.txt
        monitoring/static/js/async/79426.5a2f5cd2.js monitoring/static/js/async/79426.5a2f5cd2.js
        monitoring/static/js/async/79584.ab12d008.js monitoring/static/js/async/79584.ab12d008.js
        monitoring/static/js/async/79942.2aebbbe6.js monitoring/static/js/async/79942.2aebbbe6.js
        monitoring/static/js/async/80030.b5746f9c.js monitoring/static/js/async/80030.b5746f9c.js
        monitoring/static/js/async/80054.8f101cfe.js monitoring/static/js/async/80054.8f101cfe.js
        monitoring/static/js/async/80070.ad65e6f9.js monitoring/static/js/async/80070.ad65e6f9.js
        monitoring/static/js/async/80172.5ddeaa24.js monitoring/static/js/async/80172.5ddeaa24.js
        monitoring/static/js/async/80328.3b5f3345.js monitoring/static/js/async/80328.3b5f3345.js
        monitoring/static/js/async/80328.3b5f3345.js.LICENSE.txt monitoring/static/js/async/80328.3b5f3345.js.LICENSE.txt
        monitoring/static/js/async/8051.8b6829fc.js monitoring/static/js/async/8051.8b6829fc.js
        monitoring/static/js/async/80799.58cfcd54.js monitoring/static/js/async/80799.58cfcd54.js
        monitoring/static/js/async/80914.52cad09c.js monitoring/static/js/async/80914.52cad09c.js
        monitoring/static/js/async/81112.04bdae44.js monitoring/static/js/async/81112.04bdae44.js
        monitoring/static/js/async/81196.9972391d.js monitoring/static/js/async/81196.9972391d.js
        monitoring/static/js/async/81685.a5708bed.js monitoring/static/js/async/81685.a5708bed.js
        monitoring/static/js/async/81779.0c5fe5e4.js monitoring/static/js/async/81779.0c5fe5e4.js
        monitoring/static/js/async/81810.180f581a.js monitoring/static/js/async/81810.180f581a.js
        monitoring/static/js/async/81894.657413cf.js monitoring/static/js/async/81894.657413cf.js
        monitoring/static/js/async/81960.12c37444.js monitoring/static/js/async/81960.12c37444.js
        monitoring/static/js/async/82424.27411083.js monitoring/static/js/async/82424.27411083.js
        monitoring/static/js/async/82424.27411083.js.LICENSE.txt monitoring/static/js/async/82424.27411083.js.LICENSE.txt
        monitoring/static/js/async/82666.5889b387.js monitoring/static/js/async/82666.5889b387.js
        monitoring/static/js/async/82670.68b81274.js monitoring/static/js/async/82670.68b81274.js
        monitoring/static/js/async/82732.9ad6904b.js monitoring/static/js/async/82732.9ad6904b.js
        monitoring/static/js/async/82750.bc23775f.js monitoring/static/js/async/82750.bc23775f.js
        monitoring/static/js/async/82817.30d86cd8.js monitoring/static/js/async/82817.30d86cd8.js
        monitoring/static/js/async/83005.724425db.js monitoring/static/js/async/83005.724425db.js
        monitoring/static/js/async/83216.8d48f70b.js monitoring/static/js/async/83216.8d48f70b.js
        monitoring/static/js/async/83314.2e86363e.js monitoring/static/js/async/83314.2e86363e.js
        monitoring/static/js/async/83333.e9ee4092.js monitoring/static/js/async/83333.e9ee4092.js
        monitoring/static/js/async/83352.af7a5447.js monitoring/static/js/async/83352.af7a5447.js
        monitoring/static/js/async/83352.af7a5447.js.LICENSE.txt monitoring/static/js/async/83352.af7a5447.js.LICENSE.txt
        monitoring/static/js/async/8346.0631af21.js monitoring/static/js/async/8346.0631af21.js
        monitoring/static/js/async/8364.a8bfe672.js monitoring/static/js/async/8364.a8bfe672.js
        monitoring/static/js/async/84168.85423c9d.js monitoring/static/js/async/84168.85423c9d.js
        monitoring/static/js/async/84168.85423c9d.js.LICENSE.txt monitoring/static/js/async/84168.85423c9d.js.LICENSE.txt
        monitoring/static/js/async/84191.a954b0d0.js monitoring/static/js/async/84191.a954b0d0.js
        monitoring/static/js/async/84755.5574cf9b.js monitoring/static/js/async/84755.5574cf9b.js
        monitoring/static/js/async/85077.c1d27aba.js monitoring/static/js/async/85077.c1d27aba.js
        monitoring/static/js/async/85251.2f0e6efa.js monitoring/static/js/async/85251.2f0e6efa.js
        monitoring/static/js/async/85298.1251d28e.js monitoring/static/js/async/85298.1251d28e.js
        monitoring/static/js/async/85314.c16ccc5d.js monitoring/static/js/async/85314.c16ccc5d.js
        monitoring/static/js/async/85410.654e63f4.js monitoring/static/js/async/85410.654e63f4.js
        monitoring/static/js/async/85468.d2e57d4e.js monitoring/static/js/async/85468.d2e57d4e.js
        monitoring/static/js/async/85468.d2e57d4e.js.LICENSE.txt monitoring/static/js/async/85468.d2e57d4e.js.LICENSE.txt
        monitoring/static/js/async/85574.d46f5463.js monitoring/static/js/async/85574.d46f5463.js
        monitoring/static/js/async/8581.70308367.js monitoring/static/js/async/8581.70308367.js
        monitoring/static/js/async/8584.cc7bb1c2.js monitoring/static/js/async/8584.cc7bb1c2.js
        monitoring/static/js/async/8584.cc7bb1c2.js.LICENSE.txt monitoring/static/js/async/8584.cc7bb1c2.js.LICENSE.txt
        monitoring/static/js/async/85919.16856ebb.js monitoring/static/js/async/85919.16856ebb.js
        monitoring/static/js/async/85932.2c0c8c68.js monitoring/static/js/async/85932.2c0c8c68.js
        monitoring/static/js/async/85980.80142ba0.js monitoring/static/js/async/85980.80142ba0.js
        monitoring/static/js/async/86050.876ae119.js monitoring/static/js/async/86050.876ae119.js
        monitoring/static/js/async/86570.77467ab8.js monitoring/static/js/async/86570.77467ab8.js
        monitoring/static/js/async/86669.40a08995.js monitoring/static/js/async/86669.40a08995.js
        monitoring/static/js/async/86722.5550f873.js monitoring/static/js/async/86722.5550f873.js
        monitoring/static/js/async/86870.fd480755.js monitoring/static/js/async/86870.fd480755.js
        monitoring/static/js/async/8741.b1cb936c.js monitoring/static/js/async/8741.b1cb936c.js
        monitoring/static/js/async/87535.ddd1632c.js monitoring/static/js/async/87535.ddd1632c.js
        monitoring/static/js/async/87537.e7d993c7.js monitoring/static/js/async/87537.e7d993c7.js
        monitoring/static/js/async/87558.290aa1a4.js monitoring/static/js/async/87558.290aa1a4.js
        monitoring/static/js/async/87863.489c631b.js monitoring/static/js/async/87863.489c631b.js
        monitoring/static/js/async/88014.0cdbc8fd.js monitoring/static/js/async/88014.0cdbc8fd.js
        monitoring/static/js/async/88441.2ec9fc28.js monitoring/static/js/async/88441.2ec9fc28.js
        monitoring/static/js/async/88458.b598514c.js monitoring/static/js/async/88458.b598514c.js
        monitoring/static/js/async/88758.f85a7176.js monitoring/static/js/async/88758.f85a7176.js
        monitoring/static/js/async/89445.65c502ec.js monitoring/static/js/async/89445.65c502ec.js
        monitoring/static/js/async/89496.ea60c2c7.js monitoring/static/js/async/89496.ea60c2c7.js
        monitoring/static/js/async/90091.f83155be.js monitoring/static/js/async/90091.f83155be.js
        monitoring/static/js/async/90112.e14c16ad.js monitoring/static/js/async/90112.e14c16ad.js
        monitoring/static/js/async/90117.ba34e343.js monitoring/static/js/async/90117.ba34e343.js
        monitoring/static/js/async/90192.de1305fe.js monitoring/static/js/async/90192.de1305fe.js
        monitoring/static/js/async/90280.ba0f1712.js monitoring/static/js/async/90280.ba0f1712.js
        monitoring/static/js/async/9035.8b023716.js monitoring/static/js/async/9035.8b023716.js
        monitoring/static/js/async/90384.bf70e1a8.js monitoring/static/js/async/90384.bf70e1a8.js
        monitoring/static/js/async/90384.bf70e1a8.js.LICENSE.txt monitoring/static/js/async/90384.bf70e1a8.js.LICENSE.txt
        monitoring/static/js/async/90483.2f9df17f.js monitoring/static/js/async/90483.2f9df17f.js
        monitoring/static/js/async/90652.2238e49d.js monitoring/static/js/async/90652.2238e49d.js
        monitoring/static/js/async/90926.b84dd59a.js monitoring/static/js/async/90926.b84dd59a.js
        monitoring/static/js/async/91714.bc52270a.js monitoring/static/js/async/91714.bc52270a.js
        monitoring/static/js/async/9176.e8e1464d.js monitoring/static/js/async/9176.e8e1464d.js
        monitoring/static/js/async/9176.e8e1464d.js.LICENSE.txt monitoring/static/js/async/9176.e8e1464d.js.LICENSE.txt
        monitoring/static/js/async/91999.336428dd.js monitoring/static/js/async/91999.336428dd.js
        monitoring/static/js/async/92473.c38af6a9.js monitoring/static/js/async/92473.c38af6a9.js
        monitoring/static/js/async/92764.ec8e66a9.js monitoring/static/js/async/92764.ec8e66a9.js
        monitoring/static/js/async/93000.80e04cb6.js monitoring/static/js/async/93000.80e04cb6.js
        monitoring/static/js/async/93084.99915ac2.js monitoring/static/js/async/93084.99915ac2.js
        monitoring/static/js/async/93084.99915ac2.js.LICENSE.txt monitoring/static/js/async/93084.99915ac2.js.LICENSE.txt
        monitoring/static/js/async/93192.082fc413.js monitoring/static/js/async/93192.082fc413.js
        monitoring/static/js/async/93278.91683f8f.js monitoring/static/js/async/93278.91683f8f.js
        monitoring/static/js/async/93278.91683f8f.js.LICENSE.txt monitoring/static/js/async/93278.91683f8f.js.LICENSE.txt
        monitoring/static/js/async/93614.c552faae.js monitoring/static/js/async/93614.c552faae.js
        monitoring/static/js/async/9364.ad9e4725.js monitoring/static/js/async/9364.ad9e4725.js
        monitoring/static/js/async/93848.b625304f.js monitoring/static/js/async/93848.b625304f.js
        monitoring/static/js/async/93848.b625304f.js.LICENSE.txt monitoring/static/js/async/93848.b625304f.js.LICENSE.txt
        monitoring/static/js/async/93951.7cb5f6f1.js monitoring/static/js/async/93951.7cb5f6f1.js
        monitoring/static/js/async/93951.7cb5f6f1.js.LICENSE.txt monitoring/static/js/async/93951.7cb5f6f1.js.LICENSE.txt
        monitoring/static/js/async/93964.8a9483c4.js monitoring/static/js/async/93964.8a9483c4.js
        monitoring/static/js/async/93964.8a9483c4.js.LICENSE.txt monitoring/static/js/async/93964.8a9483c4.js.LICENSE.txt
        monitoring/static/js/async/94052.0b79e6f6.js monitoring/static/js/async/94052.0b79e6f6.js
        monitoring/static/js/async/94052.0b79e6f6.js.LICENSE.txt monitoring/static/js/async/94052.0b79e6f6.js.LICENSE.txt
        monitoring/static/js/async/94054.aa7f22d9.js monitoring/static/js/async/94054.aa7f22d9.js
        monitoring/static/js/async/94083.aaac8a5e.js monitoring/static/js/async/94083.aaac8a5e.js
        monitoring/static/js/async/9426.9079e1ec.js monitoring/static/js/async/9426.9079e1ec.js
        monitoring/static/js/async/94288.7db44d33.js monitoring/static/js/async/94288.7db44d33.js
        monitoring/static/js/async/94367.d5c328bc.js monitoring/static/js/async/94367.d5c328bc.js
        monitoring/static/js/async/94383.d62e696c.js monitoring/static/js/async/94383.d62e696c.js
        monitoring/static/js/async/94411.a5c755f0.js monitoring/static/js/async/94411.a5c755f0.js
        monitoring/static/js/async/94820.92ff6884.js monitoring/static/js/async/94820.92ff6884.js
        monitoring/static/js/async/94821.25a24be2.js monitoring/static/js/async/94821.25a24be2.js
        monitoring/static/js/async/94918.1217ea41.js monitoring/static/js/async/94918.1217ea41.js
        monitoring/static/js/async/95172.0587619b.js monitoring/static/js/async/95172.0587619b.js
        monitoring/static/js/async/95298.042c9916.js monitoring/static/js/async/95298.042c9916.js
        monitoring/static/js/async/95298.042c9916.js.LICENSE.txt monitoring/static/js/async/95298.042c9916.js.LICENSE.txt
        monitoring/static/js/async/95472.8fe31d1b.js monitoring/static/js/async/95472.8fe31d1b.js
        monitoring/static/js/async/95620.67fcdfb1.js monitoring/static/js/async/95620.67fcdfb1.js
        monitoring/static/js/async/95720.9b6bb1de.js monitoring/static/js/async/95720.9b6bb1de.js
        monitoring/static/js/async/95739.e82d990a.js monitoring/static/js/async/95739.e82d990a.js
        monitoring/static/js/async/95909.58339081.js monitoring/static/js/async/95909.58339081.js
        monitoring/static/js/async/96047.d7936625.js monitoring/static/js/async/96047.d7936625.js
        monitoring/static/js/async/96070.66f28e21.js monitoring/static/js/async/96070.66f28e21.js
        monitoring/static/js/async/9617.52cf0875.js monitoring/static/js/async/9617.52cf0875.js
        monitoring/static/js/async/96178.4dea614d.js monitoring/static/js/async/96178.4dea614d.js
        monitoring/static/js/async/96826.f7052500.js monitoring/static/js/async/96826.f7052500.js
        monitoring/static/js/async/97097.c8135f24.js monitoring/static/js/async/97097.c8135f24.js
        monitoring/static/js/async/97127.1bbb39b0.js monitoring/static/js/async/97127.1bbb39b0.js
        monitoring/static/js/async/97229.e5a6d102.js monitoring/static/js/async/97229.e5a6d102.js
        monitoring/static/js/async/97324.9a0574fb.js monitoring/static/js/async/97324.9a0574fb.js
        monitoring/static/js/async/9736.1c72efe7.js monitoring/static/js/async/9736.1c72efe7.js
        monitoring/static/js/async/9736.1c72efe7.js.LICENSE.txt monitoring/static/js/async/9736.1c72efe7.js.LICENSE.txt
        monitoring/static/js/async/97506.ff1ede4a.js monitoring/static/js/async/97506.ff1ede4a.js
        monitoring/static/js/async/97620.875ba848.js monitoring/static/js/async/97620.875ba848.js
        monitoring/static/js/async/9820.f017550b.js monitoring/static/js/async/9820.f017550b.js
        monitoring/static/js/async/98244.0ff8be05.js monitoring/static/js/async/98244.0ff8be05.js
        monitoring/static/js/async/98260.f2f720c7.js monitoring/static/js/async/98260.f2f720c7.js
        monitoring/static/js/async/98403.8f25ce26.js monitoring/static/js/async/98403.8f25ce26.js
        monitoring/static/js/async/98542.4ade2562.js monitoring/static/js/async/98542.4ade2562.js
        monitoring/static/js/async/98658.559e0932.js monitoring/static/js/async/98658.559e0932.js
        monitoring/static/js/async/98927.049aa363.js monitoring/static/js/async/98927.049aa363.js
        monitoring/static/js/async/98943.f899c5d0.js monitoring/static/js/async/98943.f899c5d0.js
        monitoring/static/js/async/98943.f899c5d0.js.LICENSE.txt monitoring/static/js/async/98943.f899c5d0.js.LICENSE.txt
        monitoring/static/js/async/98961.0eab16c5.js monitoring/static/js/async/98961.0eab16c5.js
        monitoring/static/js/async/99009.7be3e53a.js monitoring/static/js/async/99009.7be3e53a.js
        monitoring/static/js/async/99445.a0849539.js monitoring/static/js/async/99445.a0849539.js
        monitoring/static/js/async/9957.064b722a.js monitoring/static/js/async/9957.064b722a.js
        monitoring/static/js/async/99570.ad57d758.js monitoring/static/js/async/99570.ad57d758.js
        monitoring/static/js/async/9993.932dfaef.js monitoring/static/js/async/9993.932dfaef.js
        monitoring/static/js/index.e9815842.js monitoring/static/js/index.e9815842.js
        monitoring/static/js/lib-axios.b2c29dd8.js monitoring/static/js/lib-axios.b2c29dd8.js
        monitoring/static/js/lib-react.4ade9ac9.js monitoring/static/js/lib-react.4ade9ac9.js
        monitoring/static/js/lib-react.4ade9ac9.js.LICENSE.txt monitoring/static/js/lib-react.4ade9ac9.js.LICENSE.txt
        monitoring/static/js/lib-router.2ef1769b.js monitoring/static/js/lib-router.2ef1769b.js
        monitoring/static/js/lib-router.2ef1769b.js.LICENSE.txt monitoring/static/js/lib-router.2ef1769b.js.LICENSE.txt
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
    ydb/library/actors/http
    library/cpp/archive
    library/cpp/json
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
    ydb/core/path_aliasing
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
