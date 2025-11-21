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
        monitoring/asset-manifest.json monitoring/asset-manifest.json
        monitoring/editor.worker.js monitoring/editor.worker.js
        monitoring/index.html monitoring/index.html
        monitoring/static/css/17091.6014df5f.chunk.css monitoring/static/css/17091.6014df5f.chunk.css
        monitoring/static/css/17937.2650d4b8.chunk.css monitoring/static/css/17937.2650d4b8.chunk.css
        monitoring/static/css/22798.5b41c60f.chunk.css monitoring/static/css/22798.5b41c60f.chunk.css
        monitoring/static/css/22812.6c41bf94.chunk.css monitoring/static/css/22812.6c41bf94.chunk.css
        monitoring/static/css/24456.3fc3332c.chunk.css monitoring/static/css/24456.3fc3332c.chunk.css
        monitoring/static/css/26158.929c3a81.chunk.css monitoring/static/css/26158.929c3a81.chunk.css
        monitoring/static/css/30772.6f22331d.chunk.css monitoring/static/css/30772.6f22331d.chunk.css
        monitoring/static/css/31893.8b2ae560.chunk.css monitoring/static/css/31893.8b2ae560.chunk.css
        monitoring/static/css/32212.d838e763.chunk.css monitoring/static/css/32212.d838e763.chunk.css
        monitoring/static/css/32439.0f3b83b5.chunk.css monitoring/static/css/32439.0f3b83b5.chunk.css
        monitoring/static/css/4444.bed1ebbf.chunk.css monitoring/static/css/4444.bed1ebbf.chunk.css
        monitoring/static/css/48593.7fdf5ffc.chunk.css monitoring/static/css/48593.7fdf5ffc.chunk.css
        monitoring/static/css/80698.8af0abb7.chunk.css monitoring/static/css/80698.8af0abb7.chunk.css
        monitoring/static/css/8593.a3377c59.chunk.css monitoring/static/css/8593.a3377c59.chunk.css
        monitoring/static/css/86299.7cffb936.chunk.css monitoring/static/css/86299.7cffb936.chunk.css
        monitoring/static/css/main.797d82b0.css monitoring/static/css/main.797d82b0.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/10246.ee23b775.chunk.js monitoring/static/js/10246.ee23b775.chunk.js
        monitoring/static/js/10246.ee23b775.chunk.js.LICENSE.txt monitoring/static/js/10246.ee23b775.chunk.js.LICENSE.txt
        monitoring/static/js/10310.a0a2e8d7.chunk.js monitoring/static/js/10310.a0a2e8d7.chunk.js
        monitoring/static/js/10457.91c52b24.chunk.js monitoring/static/js/10457.91c52b24.chunk.js
        monitoring/static/js/10902.cd23357e.chunk.js monitoring/static/js/10902.cd23357e.chunk.js
        monitoring/static/js/10902.cd23357e.chunk.js.LICENSE.txt monitoring/static/js/10902.cd23357e.chunk.js.LICENSE.txt
        monitoring/static/js/11278.aab5c12c.chunk.js monitoring/static/js/11278.aab5c12c.chunk.js
        monitoring/static/js/11278.aab5c12c.chunk.js.LICENSE.txt monitoring/static/js/11278.aab5c12c.chunk.js.LICENSE.txt
        monitoring/static/js/11537.6c7fdaf8.chunk.js monitoring/static/js/11537.6c7fdaf8.chunk.js
        monitoring/static/js/11784.1f5f34df.chunk.js monitoring/static/js/11784.1f5f34df.chunk.js
        monitoring/static/js/1187.c5435886.chunk.js monitoring/static/js/1187.c5435886.chunk.js
        monitoring/static/js/11891.c01892d1.chunk.js monitoring/static/js/11891.c01892d1.chunk.js
        monitoring/static/js/12003.15474e6a.chunk.js monitoring/static/js/12003.15474e6a.chunk.js
        monitoring/static/js/12257.9b4ac0dc.chunk.js monitoring/static/js/12257.9b4ac0dc.chunk.js
        monitoring/static/js/1237.328d0241.chunk.js monitoring/static/js/1237.328d0241.chunk.js
        monitoring/static/js/12518.7c3529a0.chunk.js monitoring/static/js/12518.7c3529a0.chunk.js
        monitoring/static/js/12678.a958cf5b.chunk.js monitoring/static/js/12678.a958cf5b.chunk.js
        monitoring/static/js/12776.d400f028.chunk.js monitoring/static/js/12776.d400f028.chunk.js
        monitoring/static/js/13345.92722d09.chunk.js monitoring/static/js/13345.92722d09.chunk.js
        monitoring/static/js/13528.7e59fcf9.chunk.js monitoring/static/js/13528.7e59fcf9.chunk.js
        monitoring/static/js/13638.e739b34f.chunk.js monitoring/static/js/13638.e739b34f.chunk.js
        monitoring/static/js/13638.e739b34f.chunk.js.LICENSE.txt monitoring/static/js/13638.e739b34f.chunk.js.LICENSE.txt
        monitoring/static/js/13647.4d4544b8.chunk.js monitoring/static/js/13647.4d4544b8.chunk.js
        monitoring/static/js/13782.226d8da6.chunk.js monitoring/static/js/13782.226d8da6.chunk.js
        monitoring/static/js/13821.4ffb5945.chunk.js monitoring/static/js/13821.4ffb5945.chunk.js
        monitoring/static/js/14017.79836b18.chunk.js monitoring/static/js/14017.79836b18.chunk.js
        monitoring/static/js/1408.539aa2f5.chunk.js monitoring/static/js/1408.539aa2f5.chunk.js
        monitoring/static/js/14100.c2dd405c.chunk.js monitoring/static/js/14100.c2dd405c.chunk.js
        monitoring/static/js/14542.fea67816.chunk.js monitoring/static/js/14542.fea67816.chunk.js
        monitoring/static/js/14542.fea67816.chunk.js.LICENSE.txt monitoring/static/js/14542.fea67816.chunk.js.LICENSE.txt
        monitoring/static/js/14824.f28b642a.chunk.js monitoring/static/js/14824.f28b642a.chunk.js
        monitoring/static/js/15073.05a5ae4f.chunk.js monitoring/static/js/15073.05a5ae4f.chunk.js
        monitoring/static/js/15418.978d5fff.chunk.js monitoring/static/js/15418.978d5fff.chunk.js
        monitoring/static/js/15542.e6a4dbf6.chunk.js monitoring/static/js/15542.e6a4dbf6.chunk.js
        monitoring/static/js/15542.e6a4dbf6.chunk.js.LICENSE.txt monitoring/static/js/15542.e6a4dbf6.chunk.js.LICENSE.txt
        monitoring/static/js/15586.9ea357de.chunk.js monitoring/static/js/15586.9ea357de.chunk.js
        monitoring/static/js/15754.a91357c7.chunk.js monitoring/static/js/15754.a91357c7.chunk.js
        monitoring/static/js/15945.b32039c3.chunk.js monitoring/static/js/15945.b32039c3.chunk.js
        monitoring/static/js/15992.cd06f080.chunk.js monitoring/static/js/15992.cd06f080.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js monitoring/static/js/1606.0041ed7d.chunk.js
        monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt monitoring/static/js/1606.0041ed7d.chunk.js.LICENSE.txt
        monitoring/static/js/16192.4430dbe2.chunk.js monitoring/static/js/16192.4430dbe2.chunk.js
        monitoring/static/js/16210.d89e4a60.chunk.js monitoring/static/js/16210.d89e4a60.chunk.js
        monitoring/static/js/16210.d89e4a60.chunk.js.LICENSE.txt monitoring/static/js/16210.d89e4a60.chunk.js.LICENSE.txt
        monitoring/static/js/16397.3ce44e41.chunk.js monitoring/static/js/16397.3ce44e41.chunk.js
        monitoring/static/js/16664.195e9acf.chunk.js monitoring/static/js/16664.195e9acf.chunk.js
        monitoring/static/js/168.3aff2b4f.chunk.js monitoring/static/js/168.3aff2b4f.chunk.js
        monitoring/static/js/17091.c56b5c66.chunk.js monitoring/static/js/17091.c56b5c66.chunk.js
        monitoring/static/js/17091.c56b5c66.chunk.js.LICENSE.txt monitoring/static/js/17091.c56b5c66.chunk.js.LICENSE.txt
        monitoring/static/js/17240.74653f76.chunk.js monitoring/static/js/17240.74653f76.chunk.js
        monitoring/static/js/17324.3e2fbe81.chunk.js monitoring/static/js/17324.3e2fbe81.chunk.js
        monitoring/static/js/17937.591e24c2.chunk.js monitoring/static/js/17937.591e24c2.chunk.js
        monitoring/static/js/18620.0ee4d544.chunk.js monitoring/static/js/18620.0ee4d544.chunk.js
        monitoring/static/js/19043.d8c76083.chunk.js monitoring/static/js/19043.d8c76083.chunk.js
        monitoring/static/js/19507.0512979b.chunk.js monitoring/static/js/19507.0512979b.chunk.js
        monitoring/static/js/19728.daf9b9f8.chunk.js monitoring/static/js/19728.daf9b9f8.chunk.js
        monitoring/static/js/19728.daf9b9f8.chunk.js.LICENSE.txt monitoring/static/js/19728.daf9b9f8.chunk.js.LICENSE.txt
        monitoring/static/js/2013.9399d19d.chunk.js monitoring/static/js/2013.9399d19d.chunk.js
        monitoring/static/js/20257.bc722cff.chunk.js monitoring/static/js/20257.bc722cff.chunk.js
        monitoring/static/js/20463.dcc6a94a.chunk.js monitoring/static/js/20463.dcc6a94a.chunk.js
        monitoring/static/js/20516.5cce9175.chunk.js monitoring/static/js/20516.5cce9175.chunk.js
        monitoring/static/js/20536.a953f25b.chunk.js monitoring/static/js/20536.a953f25b.chunk.js
        monitoring/static/js/20654.f715fac2.chunk.js monitoring/static/js/20654.f715fac2.chunk.js
        monitoring/static/js/20654.f715fac2.chunk.js.LICENSE.txt monitoring/static/js/20654.f715fac2.chunk.js.LICENSE.txt
        monitoring/static/js/21053.3d2e8f28.chunk.js monitoring/static/js/21053.3d2e8f28.chunk.js
        monitoring/static/js/21346.951c3e97.chunk.js monitoring/static/js/21346.951c3e97.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js monitoring/static/js/2162.cca3e026.chunk.js
        monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt monitoring/static/js/2162.cca3e026.chunk.js.LICENSE.txt
        monitoring/static/js/21657.6821035c.chunk.js monitoring/static/js/21657.6821035c.chunk.js
        monitoring/static/js/21956.3e818e6c.chunk.js monitoring/static/js/21956.3e818e6c.chunk.js
        monitoring/static/js/22372.9c5be99f.chunk.js monitoring/static/js/22372.9c5be99f.chunk.js
        monitoring/static/js/22372.9c5be99f.chunk.js.LICENSE.txt monitoring/static/js/22372.9c5be99f.chunk.js.LICENSE.txt
        monitoring/static/js/22626.2495b693.chunk.js monitoring/static/js/22626.2495b693.chunk.js
        monitoring/static/js/22635.b39f9121.chunk.js monitoring/static/js/22635.b39f9121.chunk.js
        monitoring/static/js/22798.b7dcbb04.chunk.js monitoring/static/js/22798.b7dcbb04.chunk.js
        monitoring/static/js/22812.e27859b3.chunk.js monitoring/static/js/22812.e27859b3.chunk.js
        monitoring/static/js/23158.a522a83d.chunk.js monitoring/static/js/23158.a522a83d.chunk.js
        monitoring/static/js/23158.a522a83d.chunk.js.LICENSE.txt monitoring/static/js/23158.a522a83d.chunk.js.LICENSE.txt
        monitoring/static/js/23321.4a32d0c2.chunk.js monitoring/static/js/23321.4a32d0c2.chunk.js
        monitoring/static/js/23368.bd2b1654.chunk.js monitoring/static/js/23368.bd2b1654.chunk.js
        monitoring/static/js/23702.887336e8.chunk.js monitoring/static/js/23702.887336e8.chunk.js
        monitoring/static/js/23702.887336e8.chunk.js.LICENSE.txt monitoring/static/js/23702.887336e8.chunk.js.LICENSE.txt
        monitoring/static/js/23721.5ac8de6d.chunk.js monitoring/static/js/23721.5ac8de6d.chunk.js
        monitoring/static/js/23904.5b8d756f.chunk.js monitoring/static/js/23904.5b8d756f.chunk.js
        monitoring/static/js/23941.ff6a1d14.chunk.js monitoring/static/js/23941.ff6a1d14.chunk.js
        monitoring/static/js/24046.4b8ff5ef.chunk.js monitoring/static/js/24046.4b8ff5ef.chunk.js
        monitoring/static/js/24349.ff3855f6.chunk.js monitoring/static/js/24349.ff3855f6.chunk.js
        monitoring/static/js/24456.ceb2c982.chunk.js monitoring/static/js/24456.ceb2c982.chunk.js
        monitoring/static/js/24534.3cac028e.chunk.js monitoring/static/js/24534.3cac028e.chunk.js
        monitoring/static/js/24534.3cac028e.chunk.js.LICENSE.txt monitoring/static/js/24534.3cac028e.chunk.js.LICENSE.txt
        monitoring/static/js/24615.c2422e3e.chunk.js monitoring/static/js/24615.c2422e3e.chunk.js
        monitoring/static/js/24662.5e8417ae.chunk.js monitoring/static/js/24662.5e8417ae.chunk.js
        monitoring/static/js/24662.5e8417ae.chunk.js.LICENSE.txt monitoring/static/js/24662.5e8417ae.chunk.js.LICENSE.txt
        monitoring/static/js/25201.928508cb.chunk.js monitoring/static/js/25201.928508cb.chunk.js
        monitoring/static/js/25453.c8bfcbe1.chunk.js monitoring/static/js/25453.c8bfcbe1.chunk.js
        monitoring/static/js/25541.57586950.chunk.js monitoring/static/js/25541.57586950.chunk.js
        monitoring/static/js/25636.358f92b4.chunk.js monitoring/static/js/25636.358f92b4.chunk.js
        monitoring/static/js/25636.358f92b4.chunk.js.LICENSE.txt monitoring/static/js/25636.358f92b4.chunk.js.LICENSE.txt
        monitoring/static/js/2585.82f114cf.chunk.js monitoring/static/js/2585.82f114cf.chunk.js
        monitoring/static/js/26158.f777c8d5.chunk.js monitoring/static/js/26158.f777c8d5.chunk.js
        monitoring/static/js/26214.42be0c73.chunk.js monitoring/static/js/26214.42be0c73.chunk.js
        monitoring/static/js/26214.42be0c73.chunk.js.LICENSE.txt monitoring/static/js/26214.42be0c73.chunk.js.LICENSE.txt
        monitoring/static/js/26324.10b41523.chunk.js monitoring/static/js/26324.10b41523.chunk.js
        monitoring/static/js/26358.23555994.chunk.js monitoring/static/js/26358.23555994.chunk.js
        monitoring/static/js/26625.d5154eea.chunk.js monitoring/static/js/26625.d5154eea.chunk.js
        monitoring/static/js/26833.d6c6c578.chunk.js monitoring/static/js/26833.d6c6c578.chunk.js
        monitoring/static/js/27148.5289994c.chunk.js monitoring/static/js/27148.5289994c.chunk.js
        monitoring/static/js/27148.5289994c.chunk.js.LICENSE.txt monitoring/static/js/27148.5289994c.chunk.js.LICENSE.txt
        monitoring/static/js/27221.4bd74f24.chunk.js monitoring/static/js/27221.4bd74f24.chunk.js
        monitoring/static/js/2724.ac9a7465.chunk.js monitoring/static/js/2724.ac9a7465.chunk.js
        monitoring/static/js/2726.abc3a0c2.chunk.js monitoring/static/js/2726.abc3a0c2.chunk.js
        monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt monitoring/static/js/2726.abc3a0c2.chunk.js.LICENSE.txt
        monitoring/static/js/27293.bb10db01.chunk.js monitoring/static/js/27293.bb10db01.chunk.js
        monitoring/static/js/2760.7886ebf4.chunk.js monitoring/static/js/2760.7886ebf4.chunk.js
        monitoring/static/js/2782.667eef91.chunk.js monitoring/static/js/2782.667eef91.chunk.js
        monitoring/static/js/27839.6b2cd4eb.chunk.js monitoring/static/js/27839.6b2cd4eb.chunk.js
        monitoring/static/js/27857.9d2057ef.chunk.js monitoring/static/js/27857.9d2057ef.chunk.js
        monitoring/static/js/27930.9ab14e9e.chunk.js monitoring/static/js/27930.9ab14e9e.chunk.js
        monitoring/static/js/28256.20804e43.chunk.js monitoring/static/js/28256.20804e43.chunk.js
        monitoring/static/js/28269.f186112e.chunk.js monitoring/static/js/28269.f186112e.chunk.js
        monitoring/static/js/28665.40d350bd.chunk.js monitoring/static/js/28665.40d350bd.chunk.js
        monitoring/static/js/288.eb092734.chunk.js monitoring/static/js/288.eb092734.chunk.js
        monitoring/static/js/28868.3c0ecf71.chunk.js monitoring/static/js/28868.3c0ecf71.chunk.js
        monitoring/static/js/29131.441064b7.chunk.js monitoring/static/js/29131.441064b7.chunk.js
        monitoring/static/js/29252.b5c83ec5.chunk.js monitoring/static/js/29252.b5c83ec5.chunk.js
        monitoring/static/js/29324.218f075b.chunk.js monitoring/static/js/29324.218f075b.chunk.js
        monitoring/static/js/29394.e027e9c7.chunk.js monitoring/static/js/29394.e027e9c7.chunk.js
        monitoring/static/js/29394.e027e9c7.chunk.js.LICENSE.txt monitoring/static/js/29394.e027e9c7.chunk.js.LICENSE.txt
        monitoring/static/js/29782.5bff7f1d.chunk.js monitoring/static/js/29782.5bff7f1d.chunk.js
        monitoring/static/js/29874.3f099818.chunk.js monitoring/static/js/29874.3f099818.chunk.js
        monitoring/static/js/29880.b8bec8f6.chunk.js monitoring/static/js/29880.b8bec8f6.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js monitoring/static/js/2994.6c6016a8.chunk.js
        monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt monitoring/static/js/2994.6c6016a8.chunk.js.LICENSE.txt
        monitoring/static/js/30141.09cff783.chunk.js monitoring/static/js/30141.09cff783.chunk.js
        monitoring/static/js/30269.ecc48769.chunk.js monitoring/static/js/30269.ecc48769.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js monitoring/static/js/3048.692b5966.chunk.js
        monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt monitoring/static/js/3048.692b5966.chunk.js.LICENSE.txt
        monitoring/static/js/30523.726a6c98.chunk.js monitoring/static/js/30523.726a6c98.chunk.js
        monitoring/static/js/30755.4ca31b85.chunk.js monitoring/static/js/30755.4ca31b85.chunk.js
        monitoring/static/js/30772.4d294149.chunk.js monitoring/static/js/30772.4d294149.chunk.js
        monitoring/static/js/30811.b4fed9a8.chunk.js monitoring/static/js/30811.b4fed9a8.chunk.js
        monitoring/static/js/31200.17f870c0.chunk.js monitoring/static/js/31200.17f870c0.chunk.js
        monitoring/static/js/3121.74e9e7dc.chunk.js monitoring/static/js/3121.74e9e7dc.chunk.js
        monitoring/static/js/31666.c014804a.chunk.js monitoring/static/js/31666.c014804a.chunk.js
        monitoring/static/js/31857.7af3db2c.chunk.js monitoring/static/js/31857.7af3db2c.chunk.js
        monitoring/static/js/31893.ec89b364.chunk.js monitoring/static/js/31893.ec89b364.chunk.js
        monitoring/static/js/31975.54132ada.chunk.js monitoring/static/js/31975.54132ada.chunk.js
        monitoring/static/js/32166.8e70395f.chunk.js monitoring/static/js/32166.8e70395f.chunk.js
        monitoring/static/js/32212.edfaa99e.chunk.js monitoring/static/js/32212.edfaa99e.chunk.js
        monitoring/static/js/32286.23bbbad8.chunk.js monitoring/static/js/32286.23bbbad8.chunk.js
        monitoring/static/js/32439.5f893360.chunk.js monitoring/static/js/32439.5f893360.chunk.js
        monitoring/static/js/32439.5f893360.chunk.js.LICENSE.txt monitoring/static/js/32439.5f893360.chunk.js.LICENSE.txt
        monitoring/static/js/32742.d4094814.chunk.js monitoring/static/js/32742.d4094814.chunk.js
        monitoring/static/js/32742.d4094814.chunk.js.LICENSE.txt monitoring/static/js/32742.d4094814.chunk.js.LICENSE.txt
        monitoring/static/js/32854.6c426003.chunk.js monitoring/static/js/32854.6c426003.chunk.js
        monitoring/static/js/32854.6c426003.chunk.js.LICENSE.txt monitoring/static/js/32854.6c426003.chunk.js.LICENSE.txt
        monitoring/static/js/3320.a589ce6c.chunk.js monitoring/static/js/3320.a589ce6c.chunk.js
        monitoring/static/js/33338.c39231db.chunk.js monitoring/static/js/33338.c39231db.chunk.js
        monitoring/static/js/33338.c39231db.chunk.js.LICENSE.txt monitoring/static/js/33338.c39231db.chunk.js.LICENSE.txt
        monitoring/static/js/33356.ade6b74f.chunk.js monitoring/static/js/33356.ade6b74f.chunk.js
        monitoring/static/js/33462.2f6818c6.chunk.js monitoring/static/js/33462.2f6818c6.chunk.js
        monitoring/static/js/3349.f47c900f.chunk.js monitoring/static/js/3349.f47c900f.chunk.js
        monitoring/static/js/33521.11130f2d.chunk.js monitoring/static/js/33521.11130f2d.chunk.js
        monitoring/static/js/33614.67428c2b.chunk.js monitoring/static/js/33614.67428c2b.chunk.js
        monitoring/static/js/33644.0f30a76e.chunk.js monitoring/static/js/33644.0f30a76e.chunk.js
        monitoring/static/js/33822.3b7da7cd.chunk.js monitoring/static/js/33822.3b7da7cd.chunk.js
        monitoring/static/js/33822.3b7da7cd.chunk.js.LICENSE.txt monitoring/static/js/33822.3b7da7cd.chunk.js.LICENSE.txt
        monitoring/static/js/34056.2e510f7a.chunk.js monitoring/static/js/34056.2e510f7a.chunk.js
        monitoring/static/js/3410.6391755f.chunk.js monitoring/static/js/3410.6391755f.chunk.js
        monitoring/static/js/34169.9a22281f.chunk.js monitoring/static/js/34169.9a22281f.chunk.js
        monitoring/static/js/34447.11cd7198.chunk.js monitoring/static/js/34447.11cd7198.chunk.js
        monitoring/static/js/34542.6199c708.chunk.js monitoring/static/js/34542.6199c708.chunk.js
        monitoring/static/js/34557.47eeab14.chunk.js monitoring/static/js/34557.47eeab14.chunk.js
        monitoring/static/js/34847.ec23543c.chunk.js monitoring/static/js/34847.ec23543c.chunk.js
        monitoring/static/js/34849.feb7ca9a.chunk.js monitoring/static/js/34849.feb7ca9a.chunk.js
        monitoring/static/js/35161.d7f85805.chunk.js monitoring/static/js/35161.d7f85805.chunk.js
        monitoring/static/js/35298.32769c5c.chunk.js monitoring/static/js/35298.32769c5c.chunk.js
        monitoring/static/js/35360.62cc8861.chunk.js monitoring/static/js/35360.62cc8861.chunk.js
        monitoring/static/js/35368.12335d09.chunk.js monitoring/static/js/35368.12335d09.chunk.js
        monitoring/static/js/35382.6471a2ea.chunk.js monitoring/static/js/35382.6471a2ea.chunk.js
        monitoring/static/js/35382.6471a2ea.chunk.js.LICENSE.txt monitoring/static/js/35382.6471a2ea.chunk.js.LICENSE.txt
        monitoring/static/js/35495.76254796.chunk.js monitoring/static/js/35495.76254796.chunk.js
        monitoring/static/js/35865.676081fb.chunk.js monitoring/static/js/35865.676081fb.chunk.js
        monitoring/static/js/35888.e5062b23.chunk.js monitoring/static/js/35888.e5062b23.chunk.js
        monitoring/static/js/35888.e5062b23.chunk.js.LICENSE.txt monitoring/static/js/35888.e5062b23.chunk.js.LICENSE.txt
        monitoring/static/js/35935.2ce2ab9b.chunk.js monitoring/static/js/35935.2ce2ab9b.chunk.js
        monitoring/static/js/3621.d2b97b71.chunk.js monitoring/static/js/3621.d2b97b71.chunk.js
        monitoring/static/js/36355.367a02c7.chunk.js monitoring/static/js/36355.367a02c7.chunk.js
        monitoring/static/js/36374.16f4dcdb.chunk.js monitoring/static/js/36374.16f4dcdb.chunk.js
        monitoring/static/js/36374.16f4dcdb.chunk.js.LICENSE.txt monitoring/static/js/36374.16f4dcdb.chunk.js.LICENSE.txt
        monitoring/static/js/3648.3a72999a.chunk.js monitoring/static/js/3648.3a72999a.chunk.js
        monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt monitoring/static/js/3648.3a72999a.chunk.js.LICENSE.txt
        monitoring/static/js/36786.ca3962c1.chunk.js monitoring/static/js/36786.ca3962c1.chunk.js
        monitoring/static/js/37129.87b2cbdf.chunk.js monitoring/static/js/37129.87b2cbdf.chunk.js
        monitoring/static/js/37163.743c630a.chunk.js monitoring/static/js/37163.743c630a.chunk.js
        monitoring/static/js/37380.7c50d99e.chunk.js monitoring/static/js/37380.7c50d99e.chunk.js
        monitoring/static/js/37484.a20e8339.chunk.js monitoring/static/js/37484.a20e8339.chunk.js
        monitoring/static/js/37526.cd6ea77b.chunk.js monitoring/static/js/37526.cd6ea77b.chunk.js
        monitoring/static/js/37579.aa311c74.chunk.js monitoring/static/js/37579.aa311c74.chunk.js
        monitoring/static/js/37768.d2ef9acd.chunk.js monitoring/static/js/37768.d2ef9acd.chunk.js
        monitoring/static/js/37847.40c6ac09.chunk.js monitoring/static/js/37847.40c6ac09.chunk.js
        monitoring/static/js/38103.a27aa378.chunk.js monitoring/static/js/38103.a27aa378.chunk.js
        monitoring/static/js/38241.89e1e4f3.chunk.js monitoring/static/js/38241.89e1e4f3.chunk.js
        monitoring/static/js/38490.db6da5ae.chunk.js monitoring/static/js/38490.db6da5ae.chunk.js
        monitoring/static/js/38499.95c6ad2d.chunk.js monitoring/static/js/38499.95c6ad2d.chunk.js
        monitoring/static/js/38782.4da71348.chunk.js monitoring/static/js/38782.4da71348.chunk.js
        monitoring/static/js/38889.556f7cb7.chunk.js monitoring/static/js/38889.556f7cb7.chunk.js
        monitoring/static/js/3902.973b73c6.chunk.js monitoring/static/js/3902.973b73c6.chunk.js
        monitoring/static/js/3952.ce1b4fad.chunk.js monitoring/static/js/3952.ce1b4fad.chunk.js
        monitoring/static/js/39656.9e642fb4.chunk.js monitoring/static/js/39656.9e642fb4.chunk.js
        monitoring/static/js/39704.df8ada97.chunk.js monitoring/static/js/39704.df8ada97.chunk.js
        monitoring/static/js/40047.1e272b92.chunk.js monitoring/static/js/40047.1e272b92.chunk.js
        monitoring/static/js/40051.a8bd78c8.chunk.js monitoring/static/js/40051.a8bd78c8.chunk.js
        monitoring/static/js/40060.5f9cbddd.chunk.js monitoring/static/js/40060.5f9cbddd.chunk.js
        monitoring/static/js/40132.8f54cbd2.chunk.js monitoring/static/js/40132.8f54cbd2.chunk.js
        monitoring/static/js/40132.8f54cbd2.chunk.js.LICENSE.txt monitoring/static/js/40132.8f54cbd2.chunk.js.LICENSE.txt
        monitoring/static/js/4057.d29cc9ab.chunk.js monitoring/static/js/4057.d29cc9ab.chunk.js
        monitoring/static/js/40710.a00731c6.chunk.js monitoring/static/js/40710.a00731c6.chunk.js
        monitoring/static/js/40730.5e1bc3d1.chunk.js monitoring/static/js/40730.5e1bc3d1.chunk.js
        monitoring/static/js/41144.67cd95e7.chunk.js monitoring/static/js/41144.67cd95e7.chunk.js
        monitoring/static/js/41473.6996e242.chunk.js monitoring/static/js/41473.6996e242.chunk.js
        monitoring/static/js/41488.6c8e8260.chunk.js monitoring/static/js/41488.6c8e8260.chunk.js
        monitoring/static/js/41696.f9f2ec5d.chunk.js monitoring/static/js/41696.f9f2ec5d.chunk.js
        monitoring/static/js/4180.1de6c8ba.chunk.js monitoring/static/js/4180.1de6c8ba.chunk.js
        monitoring/static/js/42182.a71d5155.chunk.js monitoring/static/js/42182.a71d5155.chunk.js
        monitoring/static/js/42182.a71d5155.chunk.js.LICENSE.txt monitoring/static/js/42182.a71d5155.chunk.js.LICENSE.txt
        monitoring/static/js/42236.f6fc2b1d.chunk.js monitoring/static/js/42236.f6fc2b1d.chunk.js
        monitoring/static/js/42326.ad839537.chunk.js monitoring/static/js/42326.ad839537.chunk.js
        monitoring/static/js/42612.7c3ffc1d.chunk.js monitoring/static/js/42612.7c3ffc1d.chunk.js
        monitoring/static/js/42861.4328c47b.chunk.js monitoring/static/js/42861.4328c47b.chunk.js
        monitoring/static/js/43041.131cf855.chunk.js monitoring/static/js/43041.131cf855.chunk.js
        monitoring/static/js/4333.fef80da5.chunk.js monitoring/static/js/4333.fef80da5.chunk.js
        monitoring/static/js/43564.9600fad2.chunk.js monitoring/static/js/43564.9600fad2.chunk.js
        monitoring/static/js/43702.745d5072.chunk.js monitoring/static/js/43702.745d5072.chunk.js
        monitoring/static/js/43702.745d5072.chunk.js.LICENSE.txt monitoring/static/js/43702.745d5072.chunk.js.LICENSE.txt
        monitoring/static/js/43761.7a876492.chunk.js monitoring/static/js/43761.7a876492.chunk.js
        monitoring/static/js/43762.ba4d3c08.chunk.js monitoring/static/js/43762.ba4d3c08.chunk.js
        monitoring/static/js/44096.4faf307e.chunk.js monitoring/static/js/44096.4faf307e.chunk.js
        monitoring/static/js/4441.2b4963cf.chunk.js monitoring/static/js/4441.2b4963cf.chunk.js
        monitoring/static/js/4444.68a722e2.chunk.js monitoring/static/js/4444.68a722e2.chunk.js
        monitoring/static/js/44601.53b5fa99.chunk.js monitoring/static/js/44601.53b5fa99.chunk.js
        monitoring/static/js/45336.2ade9ce3.chunk.js monitoring/static/js/45336.2ade9ce3.chunk.js
        monitoring/static/js/45685.47ca075a.chunk.js monitoring/static/js/45685.47ca075a.chunk.js
        monitoring/static/js/45759.cb764ce8.chunk.js monitoring/static/js/45759.cb764ce8.chunk.js
        monitoring/static/js/46012.36fc4080.chunk.js monitoring/static/js/46012.36fc4080.chunk.js
        monitoring/static/js/46012.36fc4080.chunk.js.LICENSE.txt monitoring/static/js/46012.36fc4080.chunk.js.LICENSE.txt
        monitoring/static/js/46047.240cef79.chunk.js monitoring/static/js/46047.240cef79.chunk.js
        monitoring/static/js/46304.f77ff652.chunk.js monitoring/static/js/46304.f77ff652.chunk.js
        monitoring/static/js/46514.f4f7a96b.chunk.js monitoring/static/js/46514.f4f7a96b.chunk.js
        monitoring/static/js/46541.3c0665eb.chunk.js monitoring/static/js/46541.3c0665eb.chunk.js
        monitoring/static/js/46617.a03f173c.chunk.js monitoring/static/js/46617.a03f173c.chunk.js
        monitoring/static/js/46849.f2e82da5.chunk.js monitoring/static/js/46849.f2e82da5.chunk.js
        monitoring/static/js/46986.23500d6a.chunk.js monitoring/static/js/46986.23500d6a.chunk.js
        monitoring/static/js/4730.78e66e9a.chunk.js monitoring/static/js/4730.78e66e9a.chunk.js
        monitoring/static/js/47585.0b3554ab.chunk.js monitoring/static/js/47585.0b3554ab.chunk.js
        monitoring/static/js/47614.68df3ac9.chunk.js monitoring/static/js/47614.68df3ac9.chunk.js
        monitoring/static/js/47692.c0ce8e67.chunk.js monitoring/static/js/47692.c0ce8e67.chunk.js
        monitoring/static/js/47692.c0ce8e67.chunk.js.LICENSE.txt monitoring/static/js/47692.c0ce8e67.chunk.js.LICENSE.txt
        monitoring/static/js/47878.706bd425.chunk.js monitoring/static/js/47878.706bd425.chunk.js
        monitoring/static/js/47923.b27eb7cb.chunk.js monitoring/static/js/47923.b27eb7cb.chunk.js
        monitoring/static/js/48008.f90295f8.chunk.js monitoring/static/js/48008.f90295f8.chunk.js
        monitoring/static/js/48164.729b1241.chunk.js monitoring/static/js/48164.729b1241.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js monitoring/static/js/4818.6beda30c.chunk.js
        monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt monitoring/static/js/4818.6beda30c.chunk.js.LICENSE.txt
        monitoring/static/js/48204.d23c9eb4.chunk.js monitoring/static/js/48204.d23c9eb4.chunk.js
        monitoring/static/js/48613.ba6fa511.chunk.js monitoring/static/js/48613.ba6fa511.chunk.js
        monitoring/static/js/48633.41f9d3a3.chunk.js monitoring/static/js/48633.41f9d3a3.chunk.js
        monitoring/static/js/48686.e7c8b1a7.chunk.js monitoring/static/js/48686.e7c8b1a7.chunk.js
        monitoring/static/js/48832.da75f3b8.chunk.js monitoring/static/js/48832.da75f3b8.chunk.js
        monitoring/static/js/49067.2e09b756.chunk.js monitoring/static/js/49067.2e09b756.chunk.js
        monitoring/static/js/49134.f956b8c4.chunk.js monitoring/static/js/49134.f956b8c4.chunk.js
        monitoring/static/js/49219.87ef23d4.chunk.js monitoring/static/js/49219.87ef23d4.chunk.js
        monitoring/static/js/49381.b46bef1c.chunk.js monitoring/static/js/49381.b46bef1c.chunk.js
        monitoring/static/js/49523.654b328e.chunk.js monitoring/static/js/49523.654b328e.chunk.js
        monitoring/static/js/49582.c5a749cc.chunk.js monitoring/static/js/49582.c5a749cc.chunk.js
        monitoring/static/js/49582.c5a749cc.chunk.js.LICENSE.txt monitoring/static/js/49582.c5a749cc.chunk.js.LICENSE.txt
        monitoring/static/js/49725.3529a00c.chunk.js monitoring/static/js/49725.3529a00c.chunk.js
        monitoring/static/js/49778.b9d397f4.chunk.js monitoring/static/js/49778.b9d397f4.chunk.js
        monitoring/static/js/49778.b9d397f4.chunk.js.LICENSE.txt monitoring/static/js/49778.b9d397f4.chunk.js.LICENSE.txt
        monitoring/static/js/49859.e7c2fdf7.chunk.js monitoring/static/js/49859.e7c2fdf7.chunk.js
        monitoring/static/js/50181.2a1af2ba.chunk.js monitoring/static/js/50181.2a1af2ba.chunk.js
        monitoring/static/js/50795.7ee2d6c2.chunk.js monitoring/static/js/50795.7ee2d6c2.chunk.js
        monitoring/static/js/51092.c60397da.chunk.js monitoring/static/js/51092.c60397da.chunk.js
        monitoring/static/js/51094.f421b808.chunk.js monitoring/static/js/51094.f421b808.chunk.js
        monitoring/static/js/51094.f421b808.chunk.js.LICENSE.txt monitoring/static/js/51094.f421b808.chunk.js.LICENSE.txt
        monitoring/static/js/51414.1b5a0681.chunk.js monitoring/static/js/51414.1b5a0681.chunk.js
        monitoring/static/js/51414.1b5a0681.chunk.js.LICENSE.txt monitoring/static/js/51414.1b5a0681.chunk.js.LICENSE.txt
        monitoring/static/js/51496.423aebfa.chunk.js monitoring/static/js/51496.423aebfa.chunk.js
        monitoring/static/js/51627.9762f671.chunk.js monitoring/static/js/51627.9762f671.chunk.js
        monitoring/static/js/51669.4f7fc0dc.chunk.js monitoring/static/js/51669.4f7fc0dc.chunk.js
        monitoring/static/js/52156.932b04fc.chunk.js monitoring/static/js/52156.932b04fc.chunk.js
        monitoring/static/js/52182.735ff091.chunk.js monitoring/static/js/52182.735ff091.chunk.js
        monitoring/static/js/52459.426f79bd.chunk.js monitoring/static/js/52459.426f79bd.chunk.js
        monitoring/static/js/52518.2a3ff21a.chunk.js monitoring/static/js/52518.2a3ff21a.chunk.js
        monitoring/static/js/52518.2a3ff21a.chunk.js.LICENSE.txt monitoring/static/js/52518.2a3ff21a.chunk.js.LICENSE.txt
        monitoring/static/js/52536.304b8c3d.chunk.js monitoring/static/js/52536.304b8c3d.chunk.js
        monitoring/static/js/52627.5124983e.chunk.js monitoring/static/js/52627.5124983e.chunk.js
        monitoring/static/js/52919.137ca756.chunk.js monitoring/static/js/52919.137ca756.chunk.js
        monitoring/static/js/53338.161bc4dd.chunk.js monitoring/static/js/53338.161bc4dd.chunk.js
        monitoring/static/js/53338.161bc4dd.chunk.js.LICENSE.txt monitoring/static/js/53338.161bc4dd.chunk.js.LICENSE.txt
        monitoring/static/js/53514.a837d54f.chunk.js monitoring/static/js/53514.a837d54f.chunk.js
        monitoring/static/js/53529.2313d088.chunk.js monitoring/static/js/53529.2313d088.chunk.js
        monitoring/static/js/53672.a70ebf8e.chunk.js monitoring/static/js/53672.a70ebf8e.chunk.js
        monitoring/static/js/53735.4dd0ccec.chunk.js monitoring/static/js/53735.4dd0ccec.chunk.js
        monitoring/static/js/53741.78865669.chunk.js monitoring/static/js/53741.78865669.chunk.js
        monitoring/static/js/54229.f109edb0.chunk.js monitoring/static/js/54229.f109edb0.chunk.js
        monitoring/static/js/5423.cf9d797f.chunk.js monitoring/static/js/5423.cf9d797f.chunk.js
        monitoring/static/js/54466.3976873e.chunk.js monitoring/static/js/54466.3976873e.chunk.js
        monitoring/static/js/54678.5a7b7b35.chunk.js monitoring/static/js/54678.5a7b7b35.chunk.js
        monitoring/static/js/54678.5a7b7b35.chunk.js.LICENSE.txt monitoring/static/js/54678.5a7b7b35.chunk.js.LICENSE.txt
        monitoring/static/js/5475.20ce4f75.chunk.js monitoring/static/js/5475.20ce4f75.chunk.js
        monitoring/static/js/55454.4f52583e.chunk.js monitoring/static/js/55454.4f52583e.chunk.js
        monitoring/static/js/55454.4f52583e.chunk.js.LICENSE.txt monitoring/static/js/55454.4f52583e.chunk.js.LICENSE.txt
        monitoring/static/js/5549.9ab14e9e.chunk.js monitoring/static/js/5549.9ab14e9e.chunk.js
        monitoring/static/js/55760.405af6cc.chunk.js monitoring/static/js/55760.405af6cc.chunk.js
        monitoring/static/js/55816.ceb201d4.chunk.js monitoring/static/js/55816.ceb201d4.chunk.js
        monitoring/static/js/55853.2ca378d0.chunk.js monitoring/static/js/55853.2ca378d0.chunk.js
        monitoring/static/js/5634.0a0bddae.chunk.js monitoring/static/js/5634.0a0bddae.chunk.js
        monitoring/static/js/56358.3a141569.chunk.js monitoring/static/js/56358.3a141569.chunk.js
        monitoring/static/js/56358.3a141569.chunk.js.LICENSE.txt monitoring/static/js/56358.3a141569.chunk.js.LICENSE.txt
        monitoring/static/js/56405.5fa107d3.chunk.js monitoring/static/js/56405.5fa107d3.chunk.js
        monitoring/static/js/5647.0920ef73.chunk.js monitoring/static/js/5647.0920ef73.chunk.js
        monitoring/static/js/56536.e652c270.chunk.js monitoring/static/js/56536.e652c270.chunk.js
        monitoring/static/js/56665.62589785.chunk.js monitoring/static/js/56665.62589785.chunk.js
        monitoring/static/js/5681.4a95449d.chunk.js monitoring/static/js/5681.4a95449d.chunk.js
        monitoring/static/js/57102.0691c322.chunk.js monitoring/static/js/57102.0691c322.chunk.js
        monitoring/static/js/57118.e38774e7.chunk.js monitoring/static/js/57118.e38774e7.chunk.js
        monitoring/static/js/57118.e38774e7.chunk.js.LICENSE.txt monitoring/static/js/57118.e38774e7.chunk.js.LICENSE.txt
        monitoring/static/js/57206.7544a09d.chunk.js monitoring/static/js/57206.7544a09d.chunk.js
        monitoring/static/js/57206.7544a09d.chunk.js.LICENSE.txt monitoring/static/js/57206.7544a09d.chunk.js.LICENSE.txt
        monitoring/static/js/57518.ffe4814b.chunk.js monitoring/static/js/57518.ffe4814b.chunk.js
        monitoring/static/js/57776.12fe6dc5.chunk.js monitoring/static/js/57776.12fe6dc5.chunk.js
        monitoring/static/js/57875.9073c95d.chunk.js monitoring/static/js/57875.9073c95d.chunk.js
        monitoring/static/js/57946.31f41343.chunk.js monitoring/static/js/57946.31f41343.chunk.js
        monitoring/static/js/57946.31f41343.chunk.js.LICENSE.txt monitoring/static/js/57946.31f41343.chunk.js.LICENSE.txt
        monitoring/static/js/57970.67e88902.chunk.js monitoring/static/js/57970.67e88902.chunk.js
        monitoring/static/js/58090.f05344ed.chunk.js monitoring/static/js/58090.f05344ed.chunk.js
        monitoring/static/js/58306.932129eb.chunk.js monitoring/static/js/58306.932129eb.chunk.js
        monitoring/static/js/58480.64f1d96b.chunk.js monitoring/static/js/58480.64f1d96b.chunk.js
        monitoring/static/js/58642.475e1d98.chunk.js monitoring/static/js/58642.475e1d98.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js monitoring/static/js/5866.14d27c8c.chunk.js
        monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt monitoring/static/js/5866.14d27c8c.chunk.js.LICENSE.txt
        monitoring/static/js/58787.5f99eed1.chunk.js monitoring/static/js/58787.5f99eed1.chunk.js
        monitoring/static/js/58840.529e7b9b.chunk.js monitoring/static/js/58840.529e7b9b.chunk.js
        monitoring/static/js/58878.7dcfdaa2.chunk.js monitoring/static/js/58878.7dcfdaa2.chunk.js
        monitoring/static/js/58890.cf2d0d90.chunk.js monitoring/static/js/58890.cf2d0d90.chunk.js
        monitoring/static/js/58892.09f1a711.chunk.js monitoring/static/js/58892.09f1a711.chunk.js
        monitoring/static/js/58948.3d1c9bcd.chunk.js monitoring/static/js/58948.3d1c9bcd.chunk.js
        monitoring/static/js/58986.472d72cc.chunk.js monitoring/static/js/58986.472d72cc.chunk.js
        monitoring/static/js/58986.472d72cc.chunk.js.LICENSE.txt monitoring/static/js/58986.472d72cc.chunk.js.LICENSE.txt
        monitoring/static/js/59243.5de594f4.chunk.js monitoring/static/js/59243.5de594f4.chunk.js
        monitoring/static/js/59554.878aad6f.chunk.js monitoring/static/js/59554.878aad6f.chunk.js
        monitoring/static/js/59554.878aad6f.chunk.js.LICENSE.txt monitoring/static/js/59554.878aad6f.chunk.js.LICENSE.txt
        monitoring/static/js/59645.16f90ac0.chunk.js monitoring/static/js/59645.16f90ac0.chunk.js
        monitoring/static/js/59748.4682a957.chunk.js monitoring/static/js/59748.4682a957.chunk.js
        monitoring/static/js/59748.4682a957.chunk.js.LICENSE.txt monitoring/static/js/59748.4682a957.chunk.js.LICENSE.txt
        monitoring/static/js/59882.b6def2ca.chunk.js monitoring/static/js/59882.b6def2ca.chunk.js
        monitoring/static/js/59908.4f5fa1f6.chunk.js monitoring/static/js/59908.4f5fa1f6.chunk.js
        monitoring/static/js/60110.448cdddf.chunk.js monitoring/static/js/60110.448cdddf.chunk.js
        monitoring/static/js/60110.448cdddf.chunk.js.LICENSE.txt monitoring/static/js/60110.448cdddf.chunk.js.LICENSE.txt
        monitoring/static/js/60221.8d560e16.chunk.js monitoring/static/js/60221.8d560e16.chunk.js
        monitoring/static/js/605.4e218fc1.chunk.js monitoring/static/js/605.4e218fc1.chunk.js
        monitoring/static/js/60692.e3a3825e.chunk.js monitoring/static/js/60692.e3a3825e.chunk.js
        monitoring/static/js/60949.c74a3708.chunk.js monitoring/static/js/60949.c74a3708.chunk.js
        monitoring/static/js/61088.c55195af.chunk.js monitoring/static/js/61088.c55195af.chunk.js
        monitoring/static/js/61289.41bb98a2.chunk.js monitoring/static/js/61289.41bb98a2.chunk.js
        monitoring/static/js/61319.851fb355.chunk.js monitoring/static/js/61319.851fb355.chunk.js
        monitoring/static/js/61865.21725853.chunk.js monitoring/static/js/61865.21725853.chunk.js
        monitoring/static/js/61917.92d39b4c.chunk.js monitoring/static/js/61917.92d39b4c.chunk.js
        monitoring/static/js/61926.2781d8c2.chunk.js monitoring/static/js/61926.2781d8c2.chunk.js
        monitoring/static/js/62042.e21d383b.chunk.js monitoring/static/js/62042.e21d383b.chunk.js
        monitoring/static/js/62042.e21d383b.chunk.js.LICENSE.txt monitoring/static/js/62042.e21d383b.chunk.js.LICENSE.txt
        monitoring/static/js/62104.f76bc0a7.chunk.js monitoring/static/js/62104.f76bc0a7.chunk.js
        monitoring/static/js/62308.fe05af2f.chunk.js monitoring/static/js/62308.fe05af2f.chunk.js
        monitoring/static/js/62350.07b0039d.chunk.js monitoring/static/js/62350.07b0039d.chunk.js
        monitoring/static/js/62350.07b0039d.chunk.js.LICENSE.txt monitoring/static/js/62350.07b0039d.chunk.js.LICENSE.txt
        monitoring/static/js/62423.f88570ad.chunk.js monitoring/static/js/62423.f88570ad.chunk.js
        monitoring/static/js/6261.78de43a8.chunk.js monitoring/static/js/6261.78de43a8.chunk.js
        monitoring/static/js/63008.97387142.chunk.js monitoring/static/js/63008.97387142.chunk.js
        monitoring/static/js/63104.1446c826.chunk.js monitoring/static/js/63104.1446c826.chunk.js
        monitoring/static/js/63507.7a3523bb.chunk.js monitoring/static/js/63507.7a3523bb.chunk.js
        monitoring/static/js/63597.552755a8.chunk.js monitoring/static/js/63597.552755a8.chunk.js
        monitoring/static/js/63630.b7eaa11c.chunk.js monitoring/static/js/63630.b7eaa11c.chunk.js
        monitoring/static/js/63653.c2f7dcde.chunk.js monitoring/static/js/63653.c2f7dcde.chunk.js
        monitoring/static/js/63679.05a63e19.chunk.js monitoring/static/js/63679.05a63e19.chunk.js
        monitoring/static/js/63769.731ffb68.chunk.js monitoring/static/js/63769.731ffb68.chunk.js
        monitoring/static/js/63843.134a7798.chunk.js monitoring/static/js/63843.134a7798.chunk.js
        monitoring/static/js/64293.e1dedb11.chunk.js monitoring/static/js/64293.e1dedb11.chunk.js
        monitoring/static/js/64305.058a2a06.chunk.js monitoring/static/js/64305.058a2a06.chunk.js
        monitoring/static/js/64344.249e0844.chunk.js monitoring/static/js/64344.249e0844.chunk.js
        monitoring/static/js/64726.d4b86d3b.chunk.js monitoring/static/js/64726.d4b86d3b.chunk.js
        monitoring/static/js/64758.ffb8fd71.chunk.js monitoring/static/js/64758.ffb8fd71.chunk.js
        monitoring/static/js/65198.bb223d36.chunk.js monitoring/static/js/65198.bb223d36.chunk.js
        monitoring/static/js/65252.2655458e.chunk.js monitoring/static/js/65252.2655458e.chunk.js
        monitoring/static/js/65252.2655458e.chunk.js.LICENSE.txt monitoring/static/js/65252.2655458e.chunk.js.LICENSE.txt
        monitoring/static/js/65401.e76db1a2.chunk.js monitoring/static/js/65401.e76db1a2.chunk.js
        monitoring/static/js/65430.223357d4.chunk.js monitoring/static/js/65430.223357d4.chunk.js
        monitoring/static/js/65579.1ec2325b.chunk.js monitoring/static/js/65579.1ec2325b.chunk.js
        monitoring/static/js/6591.3788faef.chunk.js monitoring/static/js/6591.3788faef.chunk.js
        monitoring/static/js/65988.11e4149b.chunk.js monitoring/static/js/65988.11e4149b.chunk.js
        monitoring/static/js/65988.11e4149b.chunk.js.LICENSE.txt monitoring/static/js/65988.11e4149b.chunk.js.LICENSE.txt
        monitoring/static/js/66262.b361ce28.chunk.js monitoring/static/js/66262.b361ce28.chunk.js
        monitoring/static/js/66262.b361ce28.chunk.js.LICENSE.txt monitoring/static/js/66262.b361ce28.chunk.js.LICENSE.txt
        monitoring/static/js/66271.749f45a0.chunk.js monitoring/static/js/66271.749f45a0.chunk.js
        monitoring/static/js/66397.ac0f2622.chunk.js monitoring/static/js/66397.ac0f2622.chunk.js
        monitoring/static/js/66447.716a34f7.chunk.js monitoring/static/js/66447.716a34f7.chunk.js
        monitoring/static/js/66447.716a34f7.chunk.js.LICENSE.txt monitoring/static/js/66447.716a34f7.chunk.js.LICENSE.txt
        monitoring/static/js/67016.db9e559e.chunk.js monitoring/static/js/67016.db9e559e.chunk.js
        monitoring/static/js/67335.1e21170a.chunk.js monitoring/static/js/67335.1e21170a.chunk.js
        monitoring/static/js/67348.c1ed85fa.chunk.js monitoring/static/js/67348.c1ed85fa.chunk.js
        monitoring/static/js/67357.dd9aa014.chunk.js monitoring/static/js/67357.dd9aa014.chunk.js
        monitoring/static/js/67392.48698f5f.chunk.js monitoring/static/js/67392.48698f5f.chunk.js
        monitoring/static/js/67416.a513ef9c.chunk.js monitoring/static/js/67416.a513ef9c.chunk.js
        monitoring/static/js/67451.fd193381.chunk.js monitoring/static/js/67451.fd193381.chunk.js
        monitoring/static/js/67474.60143b39.chunk.js monitoring/static/js/67474.60143b39.chunk.js
        monitoring/static/js/6748.76102ba6.chunk.js monitoring/static/js/6748.76102ba6.chunk.js
        monitoring/static/js/67574.31643beb.chunk.js monitoring/static/js/67574.31643beb.chunk.js
        monitoring/static/js/67574.31643beb.chunk.js.LICENSE.txt monitoring/static/js/67574.31643beb.chunk.js.LICENSE.txt
        monitoring/static/js/67605.6cd42d90.chunk.js monitoring/static/js/67605.6cd42d90.chunk.js
        monitoring/static/js/68421.1cc30685.chunk.js monitoring/static/js/68421.1cc30685.chunk.js
        monitoring/static/js/6881.7e6434c9.chunk.js monitoring/static/js/6881.7e6434c9.chunk.js
        monitoring/static/js/68821.a96b8277.chunk.js monitoring/static/js/68821.a96b8277.chunk.js
        monitoring/static/js/68821.a96b8277.chunk.js.LICENSE.txt monitoring/static/js/68821.a96b8277.chunk.js.LICENSE.txt
        monitoring/static/js/68965.f442f1f7.chunk.js monitoring/static/js/68965.f442f1f7.chunk.js
        monitoring/static/js/69094.1e6c5519.chunk.js monitoring/static/js/69094.1e6c5519.chunk.js
        monitoring/static/js/69220.f0f0b57b.chunk.js monitoring/static/js/69220.f0f0b57b.chunk.js
        monitoring/static/js/70225.f064b5ad.chunk.js monitoring/static/js/70225.f064b5ad.chunk.js
        monitoring/static/js/70225.f064b5ad.chunk.js.LICENSE.txt monitoring/static/js/70225.f064b5ad.chunk.js.LICENSE.txt
        monitoring/static/js/70684.7a40aff8.chunk.js monitoring/static/js/70684.7a40aff8.chunk.js
        monitoring/static/js/70911.370b5c15.chunk.js monitoring/static/js/70911.370b5c15.chunk.js
        monitoring/static/js/71055.540864f9.chunk.js monitoring/static/js/71055.540864f9.chunk.js
        monitoring/static/js/7125.1b17ee96.chunk.js monitoring/static/js/7125.1b17ee96.chunk.js
        monitoring/static/js/71387.c39f0391.chunk.js monitoring/static/js/71387.c39f0391.chunk.js
        monitoring/static/js/71486.3e01f058.chunk.js monitoring/static/js/71486.3e01f058.chunk.js
        monitoring/static/js/71614.6c1a6c19.chunk.js monitoring/static/js/71614.6c1a6c19.chunk.js
        monitoring/static/js/71756.324c49c8.chunk.js monitoring/static/js/71756.324c49c8.chunk.js
        monitoring/static/js/71987.9c325aa1.chunk.js monitoring/static/js/71987.9c325aa1.chunk.js
        monitoring/static/js/72047.38b2078b.chunk.js monitoring/static/js/72047.38b2078b.chunk.js
        monitoring/static/js/72277.a74dac7c.chunk.js monitoring/static/js/72277.a74dac7c.chunk.js
        monitoring/static/js/72534.6637fdcf.chunk.js monitoring/static/js/72534.6637fdcf.chunk.js
        monitoring/static/js/72555.27422471.chunk.js monitoring/static/js/72555.27422471.chunk.js
        monitoring/static/js/72568.9f8b7a4b.chunk.js monitoring/static/js/72568.9f8b7a4b.chunk.js
        monitoring/static/js/72669.5c0d9dd2.chunk.js monitoring/static/js/72669.5c0d9dd2.chunk.js
        monitoring/static/js/72669.5c0d9dd2.chunk.js.LICENSE.txt monitoring/static/js/72669.5c0d9dd2.chunk.js.LICENSE.txt
        monitoring/static/js/72768.2db45c32.chunk.js monitoring/static/js/72768.2db45c32.chunk.js
        monitoring/static/js/72775.0e6824d4.chunk.js monitoring/static/js/72775.0e6824d4.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js monitoring/static/js/734.3fe325e9.chunk.js
        monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt monitoring/static/js/734.3fe325e9.chunk.js.LICENSE.txt
        monitoring/static/js/73478.353da8fe.chunk.js monitoring/static/js/73478.353da8fe.chunk.js
        monitoring/static/js/73478.353da8fe.chunk.js.LICENSE.txt monitoring/static/js/73478.353da8fe.chunk.js.LICENSE.txt
        monitoring/static/js/73534.e63e8bd4.chunk.js monitoring/static/js/73534.e63e8bd4.chunk.js
        monitoring/static/js/73534.e63e8bd4.chunk.js.LICENSE.txt monitoring/static/js/73534.e63e8bd4.chunk.js.LICENSE.txt
        monitoring/static/js/73617.1c789e9e.chunk.js monitoring/static/js/73617.1c789e9e.chunk.js
        monitoring/static/js/73655.b9773531.chunk.js monitoring/static/js/73655.b9773531.chunk.js
        monitoring/static/js/7373.8a73c02b.chunk.js monitoring/static/js/7373.8a73c02b.chunk.js
        monitoring/static/js/73769.2db50c10.chunk.js monitoring/static/js/73769.2db50c10.chunk.js
        monitoring/static/js/73879.9dc10432.chunk.js monitoring/static/js/73879.9dc10432.chunk.js
        monitoring/static/js/73950.c2b40fc8.chunk.js monitoring/static/js/73950.c2b40fc8.chunk.js
        monitoring/static/js/74324.03761f87.chunk.js monitoring/static/js/74324.03761f87.chunk.js
        monitoring/static/js/74515.9ad6bf76.chunk.js monitoring/static/js/74515.9ad6bf76.chunk.js
        monitoring/static/js/7452.878fd3ca.chunk.js monitoring/static/js/7452.878fd3ca.chunk.js
        monitoring/static/js/74891.fa3d6f11.chunk.js monitoring/static/js/74891.fa3d6f11.chunk.js
        monitoring/static/js/74918.f078deaa.chunk.js monitoring/static/js/74918.f078deaa.chunk.js
        monitoring/static/js/75111.fdc16103.chunk.js monitoring/static/js/75111.fdc16103.chunk.js
        monitoring/static/js/75276.ae3634ed.chunk.js monitoring/static/js/75276.ae3634ed.chunk.js
        monitoring/static/js/7548.fd5d2b6c.chunk.js monitoring/static/js/7548.fd5d2b6c.chunk.js
        monitoring/static/js/75735.3dd0d5b8.chunk.js monitoring/static/js/75735.3dd0d5b8.chunk.js
        monitoring/static/js/76155.592a22a8.chunk.js monitoring/static/js/76155.592a22a8.chunk.js
        monitoring/static/js/76879.ff0dd32c.chunk.js monitoring/static/js/76879.ff0dd32c.chunk.js
        monitoring/static/js/76957.e415eb04.chunk.js monitoring/static/js/76957.e415eb04.chunk.js
        monitoring/static/js/76969.7393eb17.chunk.js monitoring/static/js/76969.7393eb17.chunk.js
        monitoring/static/js/77181.8bbcb40a.chunk.js monitoring/static/js/77181.8bbcb40a.chunk.js
        monitoring/static/js/77642.1a976f0c.chunk.js monitoring/static/js/77642.1a976f0c.chunk.js
        monitoring/static/js/77642.1a976f0c.chunk.js.LICENSE.txt monitoring/static/js/77642.1a976f0c.chunk.js.LICENSE.txt
        monitoring/static/js/7769.58cb76b5.chunk.js monitoring/static/js/7769.58cb76b5.chunk.js
        monitoring/static/js/77697.23abc7db.chunk.js monitoring/static/js/77697.23abc7db.chunk.js
        monitoring/static/js/77718.38572b28.chunk.js monitoring/static/js/77718.38572b28.chunk.js
        monitoring/static/js/77718.38572b28.chunk.js.LICENSE.txt monitoring/static/js/77718.38572b28.chunk.js.LICENSE.txt
        monitoring/static/js/78053.1b2b1602.chunk.js monitoring/static/js/78053.1b2b1602.chunk.js
        monitoring/static/js/78094.583cdac6.chunk.js monitoring/static/js/78094.583cdac6.chunk.js
        monitoring/static/js/78158.e18e2327.chunk.js monitoring/static/js/78158.e18e2327.chunk.js
        monitoring/static/js/78163.8446ee4c.chunk.js monitoring/static/js/78163.8446ee4c.chunk.js
        monitoring/static/js/78517.bd6413c4.chunk.js monitoring/static/js/78517.bd6413c4.chunk.js
        monitoring/static/js/78884.8d837123.chunk.js monitoring/static/js/78884.8d837123.chunk.js
        monitoring/static/js/78979.e31ea57f.chunk.js monitoring/static/js/78979.e31ea57f.chunk.js
        monitoring/static/js/79143.5c668c2a.chunk.js monitoring/static/js/79143.5c668c2a.chunk.js
        monitoring/static/js/79312.17a58c6f.chunk.js monitoring/static/js/79312.17a58c6f.chunk.js
        monitoring/static/js/79312.17a58c6f.chunk.js.LICENSE.txt monitoring/static/js/79312.17a58c6f.chunk.js.LICENSE.txt
        monitoring/static/js/79433.b3b128c9.chunk.js monitoring/static/js/79433.b3b128c9.chunk.js
        monitoring/static/js/79672.e6cdcd60.chunk.js monitoring/static/js/79672.e6cdcd60.chunk.js
        monitoring/static/js/79707.def8f77e.chunk.js monitoring/static/js/79707.def8f77e.chunk.js
        monitoring/static/js/79716.cc217f5d.chunk.js monitoring/static/js/79716.cc217f5d.chunk.js
        monitoring/static/js/79842.ef61156d.chunk.js monitoring/static/js/79842.ef61156d.chunk.js
        monitoring/static/js/79842.ef61156d.chunk.js.LICENSE.txt monitoring/static/js/79842.ef61156d.chunk.js.LICENSE.txt
        monitoring/static/js/79967.9037190b.chunk.js monitoring/static/js/79967.9037190b.chunk.js
        monitoring/static/js/79968.1860c345.chunk.js monitoring/static/js/79968.1860c345.chunk.js
        monitoring/static/js/80030.4efa59e1.chunk.js monitoring/static/js/80030.4efa59e1.chunk.js
        monitoring/static/js/80030.4efa59e1.chunk.js.LICENSE.txt monitoring/static/js/80030.4efa59e1.chunk.js.LICENSE.txt
        monitoring/static/js/80108.0334ef65.chunk.js monitoring/static/js/80108.0334ef65.chunk.js
        monitoring/static/js/80360.fa853857.chunk.js monitoring/static/js/80360.fa853857.chunk.js
        monitoring/static/js/80490.1604d686.chunk.js monitoring/static/js/80490.1604d686.chunk.js
        monitoring/static/js/80698.3dd5e960.chunk.js monitoring/static/js/80698.3dd5e960.chunk.js
        monitoring/static/js/80822.9828963d.chunk.js monitoring/static/js/80822.9828963d.chunk.js
        monitoring/static/js/80888.8541c119.chunk.js monitoring/static/js/80888.8541c119.chunk.js
        monitoring/static/js/80920.c48f3eb8.chunk.js monitoring/static/js/80920.c48f3eb8.chunk.js
        monitoring/static/js/80921.252f76ca.chunk.js monitoring/static/js/80921.252f76ca.chunk.js
        monitoring/static/js/80936.4df48389.chunk.js monitoring/static/js/80936.4df48389.chunk.js
        monitoring/static/js/81070.22858598.chunk.js monitoring/static/js/81070.22858598.chunk.js
        monitoring/static/js/81273.685ff65c.chunk.js monitoring/static/js/81273.685ff65c.chunk.js
        monitoring/static/js/81368.ef11d874.chunk.js monitoring/static/js/81368.ef11d874.chunk.js
        monitoring/static/js/81416.07be6b48.chunk.js monitoring/static/js/81416.07be6b48.chunk.js
        monitoring/static/js/81664.1a6de663.chunk.js monitoring/static/js/81664.1a6de663.chunk.js
        monitoring/static/js/81747.65a6a7f3.chunk.js monitoring/static/js/81747.65a6a7f3.chunk.js
        monitoring/static/js/81836.c32c7c12.chunk.js monitoring/static/js/81836.c32c7c12.chunk.js
        monitoring/static/js/82053.2a21538f.chunk.js monitoring/static/js/82053.2a21538f.chunk.js
        monitoring/static/js/82071.e53a518b.chunk.js monitoring/static/js/82071.e53a518b.chunk.js
        monitoring/static/js/82474.79a8c086.chunk.js monitoring/static/js/82474.79a8c086.chunk.js
        monitoring/static/js/82556.bd9b7316.chunk.js monitoring/static/js/82556.bd9b7316.chunk.js
        monitoring/static/js/82651.2dd89381.chunk.js monitoring/static/js/82651.2dd89381.chunk.js
        monitoring/static/js/82805.b42207ee.chunk.js monitoring/static/js/82805.b42207ee.chunk.js
        monitoring/static/js/82998.b761333e.chunk.js monitoring/static/js/82998.b761333e.chunk.js
        monitoring/static/js/8302.23532196.chunk.js monitoring/static/js/8302.23532196.chunk.js
        monitoring/static/js/83096.19cd033e.chunk.js monitoring/static/js/83096.19cd033e.chunk.js
        monitoring/static/js/83333.65b07752.chunk.js monitoring/static/js/83333.65b07752.chunk.js
        monitoring/static/js/83510.ed31c9f8.chunk.js monitoring/static/js/83510.ed31c9f8.chunk.js
        monitoring/static/js/83744.56cf8926.chunk.js monitoring/static/js/83744.56cf8926.chunk.js
        monitoring/static/js/83938.8a01d61b.chunk.js monitoring/static/js/83938.8a01d61b.chunk.js
        monitoring/static/js/84401.8a09e9ae.chunk.js monitoring/static/js/84401.8a09e9ae.chunk.js
        monitoring/static/js/84582.caa06f15.chunk.js monitoring/static/js/84582.caa06f15.chunk.js
        monitoring/static/js/84582.caa06f15.chunk.js.LICENSE.txt monitoring/static/js/84582.caa06f15.chunk.js.LICENSE.txt
        monitoring/static/js/84652.5dfa8103.chunk.js monitoring/static/js/84652.5dfa8103.chunk.js
        monitoring/static/js/84870.ab138a59.chunk.js monitoring/static/js/84870.ab138a59.chunk.js
        monitoring/static/js/84870.ab138a59.chunk.js.LICENSE.txt monitoring/static/js/84870.ab138a59.chunk.js.LICENSE.txt
        monitoring/static/js/85027.462a7fdc.chunk.js monitoring/static/js/85027.462a7fdc.chunk.js
        monitoring/static/js/85047.23b0ab8e.chunk.js monitoring/static/js/85047.23b0ab8e.chunk.js
        monitoring/static/js/85172.27bde3c6.chunk.js monitoring/static/js/85172.27bde3c6.chunk.js
        monitoring/static/js/85187.882bc505.chunk.js monitoring/static/js/85187.882bc505.chunk.js
        monitoring/static/js/8534.f7aec532.chunk.js monitoring/static/js/8534.f7aec532.chunk.js
        monitoring/static/js/85463.255934dc.chunk.js monitoring/static/js/85463.255934dc.chunk.js
        monitoring/static/js/85487.6691e221.chunk.js monitoring/static/js/85487.6691e221.chunk.js
        monitoring/static/js/85580.33179508.chunk.js monitoring/static/js/85580.33179508.chunk.js
        monitoring/static/js/85668.aac75a39.chunk.js monitoring/static/js/85668.aac75a39.chunk.js
        monitoring/static/js/8593.6e445efa.chunk.js monitoring/static/js/8593.6e445efa.chunk.js
        monitoring/static/js/85950.98e6e44a.chunk.js monitoring/static/js/85950.98e6e44a.chunk.js
        monitoring/static/js/85953.ea1ee23e.chunk.js monitoring/static/js/85953.ea1ee23e.chunk.js
        monitoring/static/js/86114.0fc0acbb.chunk.js monitoring/static/js/86114.0fc0acbb.chunk.js
        monitoring/static/js/86299.27a9b847.chunk.js monitoring/static/js/86299.27a9b847.chunk.js
        monitoring/static/js/86342.528e5efc.chunk.js monitoring/static/js/86342.528e5efc.chunk.js
        monitoring/static/js/86342.528e5efc.chunk.js.LICENSE.txt monitoring/static/js/86342.528e5efc.chunk.js.LICENSE.txt
        monitoring/static/js/86373.d429e135.chunk.js monitoring/static/js/86373.d429e135.chunk.js
        monitoring/static/js/86407.8c7a56ec.chunk.js monitoring/static/js/86407.8c7a56ec.chunk.js
        monitoring/static/js/86658.6a6caa74.chunk.js monitoring/static/js/86658.6a6caa74.chunk.js
        monitoring/static/js/86658.6a6caa74.chunk.js.LICENSE.txt monitoring/static/js/86658.6a6caa74.chunk.js.LICENSE.txt
        monitoring/static/js/86681.dad78388.chunk.js monitoring/static/js/86681.dad78388.chunk.js
        monitoring/static/js/8704.87492da1.chunk.js monitoring/static/js/8704.87492da1.chunk.js
        monitoring/static/js/87338.69291562.chunk.js monitoring/static/js/87338.69291562.chunk.js
        monitoring/static/js/87362.87249c8c.chunk.js monitoring/static/js/87362.87249c8c.chunk.js
        monitoring/static/js/87453.b747599e.chunk.js monitoring/static/js/87453.b747599e.chunk.js
        monitoring/static/js/87508.2058178d.chunk.js monitoring/static/js/87508.2058178d.chunk.js
        monitoring/static/js/87789.9f75b4bb.chunk.js monitoring/static/js/87789.9f75b4bb.chunk.js
        monitoring/static/js/87850.5e022d3e.chunk.js monitoring/static/js/87850.5e022d3e.chunk.js
        monitoring/static/js/87945.d0ceebab.chunk.js monitoring/static/js/87945.d0ceebab.chunk.js
        monitoring/static/js/87962.2094d7c1.chunk.js monitoring/static/js/87962.2094d7c1.chunk.js
        monitoring/static/js/88081.e48ec099.chunk.js monitoring/static/js/88081.e48ec099.chunk.js
        monitoring/static/js/88119.041d294e.chunk.js monitoring/static/js/88119.041d294e.chunk.js
        monitoring/static/js/88162.cecdf912.chunk.js monitoring/static/js/88162.cecdf912.chunk.js
        monitoring/static/js/88706.b895fbe4.chunk.js monitoring/static/js/88706.b895fbe4.chunk.js
        monitoring/static/js/89025.e20277a3.chunk.js monitoring/static/js/89025.e20277a3.chunk.js
        monitoring/static/js/89222.d941dfbd.chunk.js monitoring/static/js/89222.d941dfbd.chunk.js
        monitoring/static/js/89697.31847e00.chunk.js monitoring/static/js/89697.31847e00.chunk.js
        monitoring/static/js/89822.b9f37c6b.chunk.js monitoring/static/js/89822.b9f37c6b.chunk.js
        monitoring/static/js/89922.e5924c1e.chunk.js monitoring/static/js/89922.e5924c1e.chunk.js
        monitoring/static/js/90118.4fc97e01.chunk.js monitoring/static/js/90118.4fc97e01.chunk.js
        monitoring/static/js/90118.4fc97e01.chunk.js.LICENSE.txt monitoring/static/js/90118.4fc97e01.chunk.js.LICENSE.txt
        monitoring/static/js/90144.ce52dc13.chunk.js monitoring/static/js/90144.ce52dc13.chunk.js
        monitoring/static/js/90290.a7f1549c.chunk.js monitoring/static/js/90290.a7f1549c.chunk.js
        monitoring/static/js/90397.46f4f374.chunk.js monitoring/static/js/90397.46f4f374.chunk.js
        monitoring/static/js/90513.c6053ab5.chunk.js monitoring/static/js/90513.c6053ab5.chunk.js
        monitoring/static/js/90628.e6a33d41.chunk.js monitoring/static/js/90628.e6a33d41.chunk.js
        monitoring/static/js/91048.3370751c.chunk.js monitoring/static/js/91048.3370751c.chunk.js
        monitoring/static/js/91289.cb50a47b.chunk.js monitoring/static/js/91289.cb50a47b.chunk.js
        monitoring/static/js/9160.6df6d7cf.chunk.js monitoring/static/js/9160.6df6d7cf.chunk.js
        monitoring/static/js/9174.ae7682da.chunk.js monitoring/static/js/9174.ae7682da.chunk.js
        monitoring/static/js/9198.92566050.chunk.js monitoring/static/js/9198.92566050.chunk.js
        monitoring/static/js/92016.1c9c5217.chunk.js monitoring/static/js/92016.1c9c5217.chunk.js
        monitoring/static/js/92016.1c9c5217.chunk.js.LICENSE.txt monitoring/static/js/92016.1c9c5217.chunk.js.LICENSE.txt
        monitoring/static/js/92082.77e58985.chunk.js monitoring/static/js/92082.77e58985.chunk.js
        monitoring/static/js/92466.f38204fe.chunk.js monitoring/static/js/92466.f38204fe.chunk.js
        monitoring/static/js/92777.98fd20b3.chunk.js monitoring/static/js/92777.98fd20b3.chunk.js
        monitoring/static/js/92867.d5fd786f.chunk.js monitoring/static/js/92867.d5fd786f.chunk.js
        monitoring/static/js/92984.bc3d29a6.chunk.js monitoring/static/js/92984.bc3d29a6.chunk.js
        monitoring/static/js/93033.55bd21d1.chunk.js monitoring/static/js/93033.55bd21d1.chunk.js
        monitoring/static/js/93251.504ff347.chunk.js monitoring/static/js/93251.504ff347.chunk.js
        monitoring/static/js/9332.5c9a9ba8.chunk.js monitoring/static/js/9332.5c9a9ba8.chunk.js
        monitoring/static/js/93621.f4436cd9.chunk.js monitoring/static/js/93621.f4436cd9.chunk.js
        monitoring/static/js/93756.c3c309ab.chunk.js monitoring/static/js/93756.c3c309ab.chunk.js
        monitoring/static/js/94577.fd2b71dc.chunk.js monitoring/static/js/94577.fd2b71dc.chunk.js
        monitoring/static/js/94928.b560e16f.chunk.js monitoring/static/js/94928.b560e16f.chunk.js
        monitoring/static/js/95050.8837060e.chunk.js monitoring/static/js/95050.8837060e.chunk.js
        monitoring/static/js/9518.3fffdd45.chunk.js monitoring/static/js/9518.3fffdd45.chunk.js
        monitoring/static/js/95357.06fd4627.chunk.js monitoring/static/js/95357.06fd4627.chunk.js
        monitoring/static/js/95452.a9a71757.chunk.js monitoring/static/js/95452.a9a71757.chunk.js
        monitoring/static/js/95455.d752a8f0.chunk.js monitoring/static/js/95455.d752a8f0.chunk.js
        monitoring/static/js/95819.5ebcb088.chunk.js monitoring/static/js/95819.5ebcb088.chunk.js
        monitoring/static/js/95924.d3281fc7.chunk.js monitoring/static/js/95924.d3281fc7.chunk.js
        monitoring/static/js/96030.dc8f5e89.chunk.js monitoring/static/js/96030.dc8f5e89.chunk.js
        monitoring/static/js/9606.fc9247cb.chunk.js monitoring/static/js/9606.fc9247cb.chunk.js
        monitoring/static/js/96266.3f7264b8.chunk.js monitoring/static/js/96266.3f7264b8.chunk.js
        monitoring/static/js/96554.bff09e47.chunk.js monitoring/static/js/96554.bff09e47.chunk.js
        monitoring/static/js/96554.bff09e47.chunk.js.LICENSE.txt monitoring/static/js/96554.bff09e47.chunk.js.LICENSE.txt
        monitoring/static/js/96592.0952d00e.chunk.js monitoring/static/js/96592.0952d00e.chunk.js
        monitoring/static/js/96758.90ff8746.chunk.js monitoring/static/js/96758.90ff8746.chunk.js
        monitoring/static/js/96789.56ec1199.chunk.js monitoring/static/js/96789.56ec1199.chunk.js
        monitoring/static/js/96792.cbf3a9c2.chunk.js monitoring/static/js/96792.cbf3a9c2.chunk.js
        monitoring/static/js/9711.5b02f8ca.chunk.js monitoring/static/js/9711.5b02f8ca.chunk.js
        monitoring/static/js/97420.08729928.chunk.js monitoring/static/js/97420.08729928.chunk.js
        monitoring/static/js/97638.a9af06da.chunk.js monitoring/static/js/97638.a9af06da.chunk.js
        monitoring/static/js/97638.a9af06da.chunk.js.LICENSE.txt monitoring/static/js/97638.a9af06da.chunk.js.LICENSE.txt
        monitoring/static/js/97748.5df46a4a.chunk.js monitoring/static/js/97748.5df46a4a.chunk.js
        monitoring/static/js/98014.a4ad6ba5.chunk.js monitoring/static/js/98014.a4ad6ba5.chunk.js
        monitoring/static/js/98014.a4ad6ba5.chunk.js.LICENSE.txt monitoring/static/js/98014.a4ad6ba5.chunk.js.LICENSE.txt
        monitoring/static/js/98210.28fa04bd.chunk.js monitoring/static/js/98210.28fa04bd.chunk.js
        monitoring/static/js/98234.c2934def.chunk.js monitoring/static/js/98234.c2934def.chunk.js
        monitoring/static/js/98234.c2934def.chunk.js.LICENSE.txt monitoring/static/js/98234.c2934def.chunk.js.LICENSE.txt
        monitoring/static/js/9833.693907d1.chunk.js monitoring/static/js/9833.693907d1.chunk.js
        monitoring/static/js/98812.b6b9a527.chunk.js monitoring/static/js/98812.b6b9a527.chunk.js
        monitoring/static/js/98899.66bdc43c.chunk.js monitoring/static/js/98899.66bdc43c.chunk.js
        monitoring/static/js/98907.9638c31a.chunk.js monitoring/static/js/98907.9638c31a.chunk.js
        monitoring/static/js/99010.0141f0a0.chunk.js monitoring/static/js/99010.0141f0a0.chunk.js
        monitoring/static/js/99010.0141f0a0.chunk.js.LICENSE.txt monitoring/static/js/99010.0141f0a0.chunk.js.LICENSE.txt
        monitoring/static/js/99049.f76d887d.chunk.js monitoring/static/js/99049.f76d887d.chunk.js
        monitoring/static/js/99072.524bdb71.chunk.js monitoring/static/js/99072.524bdb71.chunk.js
        monitoring/static/js/99176.319e969d.chunk.js monitoring/static/js/99176.319e969d.chunk.js
        monitoring/static/js/99176.319e969d.chunk.js.LICENSE.txt monitoring/static/js/99176.319e969d.chunk.js.LICENSE.txt
        monitoring/static/js/99466.a47d41f5.chunk.js monitoring/static/js/99466.a47d41f5.chunk.js
        monitoring/static/js/9986.7a93962c.chunk.js monitoring/static/js/9986.7a93962c.chunk.js
        monitoring/static/js/99872.c308e65f.chunk.js monitoring/static/js/99872.c308e65f.chunk.js
        monitoring/static/js/99872.c308e65f.chunk.js.LICENSE.txt monitoring/static/js/99872.c308e65f.chunk.js.LICENSE.txt
        monitoring/static/js/main.98f823e0.js monitoring/static/js/main.98f823e0.js
        monitoring/static/js/main.98f823e0.js.LICENSE.txt monitoring/static/js/main.98f823e0.js.LICENSE.txt
        monitoring/static/media/codicon.f6283f7ccaed1249d9eb.ttf monitoring/static/media/codicon.f6283f7ccaed1249d9eb.ttf
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
