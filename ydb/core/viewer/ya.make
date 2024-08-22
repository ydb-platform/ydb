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
        monitoring/index.html monitoring/index.html
        monitoring/static/css/1551.d5e5efc2.chunk.css monitoring/static/css/1551.d5e5efc2.chunk.css
        monitoring/static/css/163.1284ddc4.chunk.css monitoring/static/css/163.1284ddc4.chunk.css
        monitoring/static/css/2418.3ce054a3.chunk.css monitoring/static/css/2418.3ce054a3.chunk.css
        monitoring/static/css/321.e12415cc.chunk.css monitoring/static/css/321.e12415cc.chunk.css
        monitoring/static/css/328.a726d329.chunk.css monitoring/static/css/328.a726d329.chunk.css
        monitoring/static/css/4983.5c3e5de4.chunk.css monitoring/static/css/4983.5c3e5de4.chunk.css
        monitoring/static/css/5246.49d67ade.chunk.css monitoring/static/css/5246.49d67ade.chunk.css
        monitoring/static/css/5715.07ca45c9.chunk.css monitoring/static/css/5715.07ca45c9.chunk.css
        monitoring/static/css/7045.ebbcb0f7.chunk.css monitoring/static/css/7045.ebbcb0f7.chunk.css
        monitoring/static/css/7542.9a1fbaca.chunk.css monitoring/static/css/7542.9a1fbaca.chunk.css
        monitoring/static/css/8076.5d26c70c.chunk.css monitoring/static/css/8076.5d26c70c.chunk.css
        monitoring/static/css/8424.308a04db.chunk.css monitoring/static/css/8424.308a04db.chunk.css
        monitoring/static/css/8429.ef9c97b7.chunk.css monitoring/static/css/8429.ef9c97b7.chunk.css
        monitoring/static/css/9818.3ebe673f.chunk.css monitoring/static/css/9818.3ebe673f.chunk.css
        monitoring/static/css/main.efd6a3a5.css monitoring/static/css/main.efd6a3a5.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1148.3c629236.chunk.js monitoring/static/js/1148.3c629236.chunk.js
        monitoring/static/js/115.2c4de87e.chunk.js monitoring/static/js/115.2c4de87e.chunk.js
        monitoring/static/js/1150.2b47004d.chunk.js monitoring/static/js/1150.2b47004d.chunk.js
        monitoring/static/js/1155.4fce1854.chunk.js monitoring/static/js/1155.4fce1854.chunk.js
        monitoring/static/js/1155.4fce1854.chunk.js.LICENSE.txt monitoring/static/js/1155.4fce1854.chunk.js.LICENSE.txt
        monitoring/static/js/1168.91d9e2c2.chunk.js monitoring/static/js/1168.91d9e2c2.chunk.js
        monitoring/static/js/1179.15d7ac65.chunk.js monitoring/static/js/1179.15d7ac65.chunk.js
        monitoring/static/js/1278.c0717a20.chunk.js monitoring/static/js/1278.c0717a20.chunk.js
        monitoring/static/js/1343.b5e020af.chunk.js monitoring/static/js/1343.b5e020af.chunk.js
        monitoring/static/js/1350.21b6a9ef.chunk.js monitoring/static/js/1350.21b6a9ef.chunk.js
        monitoring/static/js/1478.5044be66.chunk.js monitoring/static/js/1478.5044be66.chunk.js
        monitoring/static/js/1478.5044be66.chunk.js.LICENSE.txt monitoring/static/js/1478.5044be66.chunk.js.LICENSE.txt
        monitoring/static/js/148.b60f0e5e.chunk.js monitoring/static/js/148.b60f0e5e.chunk.js
        monitoring/static/js/1508.f0158935.chunk.js monitoring/static/js/1508.f0158935.chunk.js
        monitoring/static/js/1528.2a39d066.chunk.js monitoring/static/js/1528.2a39d066.chunk.js
        monitoring/static/js/1551.2e8e3e50.chunk.js monitoring/static/js/1551.2e8e3e50.chunk.js
        monitoring/static/js/1551.2e8e3e50.chunk.js.LICENSE.txt monitoring/static/js/1551.2e8e3e50.chunk.js.LICENSE.txt
        monitoring/static/js/1616.8a217b93.chunk.js monitoring/static/js/1616.8a217b93.chunk.js
        monitoring/static/js/163.2451f8a1.chunk.js monitoring/static/js/163.2451f8a1.chunk.js
        monitoring/static/js/1736.9f4a6b02.chunk.js monitoring/static/js/1736.9f4a6b02.chunk.js
        monitoring/static/js/1746.a8ba5c62.chunk.js monitoring/static/js/1746.a8ba5c62.chunk.js
        monitoring/static/js/1747.b4331799.chunk.js monitoring/static/js/1747.b4331799.chunk.js
        monitoring/static/js/1747.b4331799.chunk.js.LICENSE.txt monitoring/static/js/1747.b4331799.chunk.js.LICENSE.txt
        monitoring/static/js/178.e0df04cc.chunk.js monitoring/static/js/178.e0df04cc.chunk.js
        monitoring/static/js/185.7d51fcfa.chunk.js monitoring/static/js/185.7d51fcfa.chunk.js
        monitoring/static/js/185.7d51fcfa.chunk.js.LICENSE.txt monitoring/static/js/185.7d51fcfa.chunk.js.LICENSE.txt
        monitoring/static/js/1869.d6661a03.chunk.js monitoring/static/js/1869.d6661a03.chunk.js
        monitoring/static/js/1956.0205a5bb.chunk.js monitoring/static/js/1956.0205a5bb.chunk.js
        monitoring/static/js/1956.0205a5bb.chunk.js.LICENSE.txt monitoring/static/js/1956.0205a5bb.chunk.js.LICENSE.txt
        monitoring/static/js/202.52f13cd5.chunk.js monitoring/static/js/202.52f13cd5.chunk.js
        monitoring/static/js/2033.5c6dfca9.chunk.js monitoring/static/js/2033.5c6dfca9.chunk.js
        monitoring/static/js/2104.4f22ecac.chunk.js monitoring/static/js/2104.4f22ecac.chunk.js
        monitoring/static/js/2104.4f22ecac.chunk.js.LICENSE.txt monitoring/static/js/2104.4f22ecac.chunk.js.LICENSE.txt
        monitoring/static/js/2118.bc169874.chunk.js monitoring/static/js/2118.bc169874.chunk.js
        monitoring/static/js/2118.bc169874.chunk.js.LICENSE.txt monitoring/static/js/2118.bc169874.chunk.js.LICENSE.txt
        monitoring/static/js/214.99a17949.chunk.js monitoring/static/js/214.99a17949.chunk.js
        monitoring/static/js/214.99a17949.chunk.js.LICENSE.txt monitoring/static/js/214.99a17949.chunk.js.LICENSE.txt
        monitoring/static/js/2141.26c930aa.chunk.js monitoring/static/js/2141.26c930aa.chunk.js
        monitoring/static/js/2141.26c930aa.chunk.js.LICENSE.txt monitoring/static/js/2141.26c930aa.chunk.js.LICENSE.txt
        monitoring/static/js/2180.adcde51c.chunk.js monitoring/static/js/2180.adcde51c.chunk.js
        monitoring/static/js/2183.e2318c37.chunk.js monitoring/static/js/2183.e2318c37.chunk.js
        monitoring/static/js/2183.e2318c37.chunk.js.LICENSE.txt monitoring/static/js/2183.e2318c37.chunk.js.LICENSE.txt
        monitoring/static/js/2190.27f354f5.chunk.js monitoring/static/js/2190.27f354f5.chunk.js
        monitoring/static/js/2190.27f354f5.chunk.js.LICENSE.txt monitoring/static/js/2190.27f354f5.chunk.js.LICENSE.txt
        monitoring/static/js/2194.38bafdfc.chunk.js monitoring/static/js/2194.38bafdfc.chunk.js
        monitoring/static/js/2194.38bafdfc.chunk.js.LICENSE.txt monitoring/static/js/2194.38bafdfc.chunk.js.LICENSE.txt
        monitoring/static/js/2223.63ae5a05.chunk.js monitoring/static/js/2223.63ae5a05.chunk.js
        monitoring/static/js/2229.6687fc46.chunk.js monitoring/static/js/2229.6687fc46.chunk.js
        monitoring/static/js/2238.3cf88b79.chunk.js monitoring/static/js/2238.3cf88b79.chunk.js
        monitoring/static/js/2302.7e7a2fb4.chunk.js monitoring/static/js/2302.7e7a2fb4.chunk.js
        monitoring/static/js/2302.7e7a2fb4.chunk.js.LICENSE.txt monitoring/static/js/2302.7e7a2fb4.chunk.js.LICENSE.txt
        monitoring/static/js/2322.29255c22.chunk.js monitoring/static/js/2322.29255c22.chunk.js
        monitoring/static/js/2322.29255c22.chunk.js.LICENSE.txt monitoring/static/js/2322.29255c22.chunk.js.LICENSE.txt
        monitoring/static/js/2335.eb54f5e5.chunk.js monitoring/static/js/2335.eb54f5e5.chunk.js
        monitoring/static/js/2367.052e678b.chunk.js monitoring/static/js/2367.052e678b.chunk.js
        monitoring/static/js/2403.82cd0025.chunk.js monitoring/static/js/2403.82cd0025.chunk.js
        monitoring/static/js/2403.82cd0025.chunk.js.LICENSE.txt monitoring/static/js/2403.82cd0025.chunk.js.LICENSE.txt
        monitoring/static/js/2418.b7bfcc5a.chunk.js monitoring/static/js/2418.b7bfcc5a.chunk.js
        monitoring/static/js/2435.092e8d7f.chunk.js monitoring/static/js/2435.092e8d7f.chunk.js
        monitoring/static/js/2477.e6121bfd.chunk.js monitoring/static/js/2477.e6121bfd.chunk.js
        monitoring/static/js/2492.64b7d727.chunk.js monitoring/static/js/2492.64b7d727.chunk.js
        monitoring/static/js/2492.64b7d727.chunk.js.LICENSE.txt monitoring/static/js/2492.64b7d727.chunk.js.LICENSE.txt
        monitoring/static/js/2521.21bdfab9.chunk.js monitoring/static/js/2521.21bdfab9.chunk.js
        monitoring/static/js/2521.21bdfab9.chunk.js.LICENSE.txt monitoring/static/js/2521.21bdfab9.chunk.js.LICENSE.txt
        monitoring/static/js/2532.30bb087d.chunk.js monitoring/static/js/2532.30bb087d.chunk.js
        monitoring/static/js/2532.30bb087d.chunk.js.LICENSE.txt monitoring/static/js/2532.30bb087d.chunk.js.LICENSE.txt
        monitoring/static/js/2553.5faabf5a.chunk.js monitoring/static/js/2553.5faabf5a.chunk.js
        monitoring/static/js/2553.5faabf5a.chunk.js.LICENSE.txt monitoring/static/js/2553.5faabf5a.chunk.js.LICENSE.txt
        monitoring/static/js/2590.75b6626e.chunk.js monitoring/static/js/2590.75b6626e.chunk.js
        monitoring/static/js/2620.8e5c52fb.chunk.js monitoring/static/js/2620.8e5c52fb.chunk.js
        monitoring/static/js/2677.3d7ea3fc.chunk.js monitoring/static/js/2677.3d7ea3fc.chunk.js
        monitoring/static/js/2701.86912840.chunk.js monitoring/static/js/2701.86912840.chunk.js
        monitoring/static/js/2840.b69eb597.chunk.js monitoring/static/js/2840.b69eb597.chunk.js
        monitoring/static/js/2840.b69eb597.chunk.js.LICENSE.txt monitoring/static/js/2840.b69eb597.chunk.js.LICENSE.txt
        monitoring/static/js/2876.afe7e47f.chunk.js monitoring/static/js/2876.afe7e47f.chunk.js
        monitoring/static/js/2876.afe7e47f.chunk.js.LICENSE.txt monitoring/static/js/2876.afe7e47f.chunk.js.LICENSE.txt
        monitoring/static/js/2931.3ade3bc3.chunk.js monitoring/static/js/2931.3ade3bc3.chunk.js
        monitoring/static/js/2931.3ade3bc3.chunk.js.LICENSE.txt monitoring/static/js/2931.3ade3bc3.chunk.js.LICENSE.txt
        monitoring/static/js/2962.66e01691.chunk.js monitoring/static/js/2962.66e01691.chunk.js
        monitoring/static/js/2962.66e01691.chunk.js.LICENSE.txt monitoring/static/js/2962.66e01691.chunk.js.LICENSE.txt
        monitoring/static/js/2981.6d027811.chunk.js monitoring/static/js/2981.6d027811.chunk.js
        monitoring/static/js/2986.2100fcad.chunk.js monitoring/static/js/2986.2100fcad.chunk.js
        monitoring/static/js/2994.e6c77407.chunk.js monitoring/static/js/2994.e6c77407.chunk.js
        monitoring/static/js/2994.e6c77407.chunk.js.LICENSE.txt monitoring/static/js/2994.e6c77407.chunk.js.LICENSE.txt
        monitoring/static/js/30.b097cbb4.chunk.js monitoring/static/js/30.b097cbb4.chunk.js
        monitoring/static/js/3025.7e536c57.chunk.js monitoring/static/js/3025.7e536c57.chunk.js
        monitoring/static/js/3074.bbb8aaef.chunk.js monitoring/static/js/3074.bbb8aaef.chunk.js
        monitoring/static/js/3074.bbb8aaef.chunk.js.LICENSE.txt monitoring/static/js/3074.bbb8aaef.chunk.js.LICENSE.txt
        monitoring/static/js/321.9a9868e1.chunk.js monitoring/static/js/321.9a9868e1.chunk.js
        monitoring/static/js/321.9a9868e1.chunk.js.LICENSE.txt monitoring/static/js/321.9a9868e1.chunk.js.LICENSE.txt
        monitoring/static/js/3231.65396654.chunk.js monitoring/static/js/3231.65396654.chunk.js
        monitoring/static/js/3271.7b005742.chunk.js monitoring/static/js/3271.7b005742.chunk.js
        monitoring/static/js/328.4a94f418.chunk.js monitoring/static/js/328.4a94f418.chunk.js
        monitoring/static/js/3304.f5897a96.chunk.js monitoring/static/js/3304.f5897a96.chunk.js
        monitoring/static/js/3333.ceb196e6.chunk.js monitoring/static/js/3333.ceb196e6.chunk.js
        monitoring/static/js/3358.c777fe1f.chunk.js monitoring/static/js/3358.c777fe1f.chunk.js
        monitoring/static/js/3358.c777fe1f.chunk.js.LICENSE.txt monitoring/static/js/3358.c777fe1f.chunk.js.LICENSE.txt
        monitoring/static/js/337.b6fc715e.chunk.js monitoring/static/js/337.b6fc715e.chunk.js
        monitoring/static/js/337.b6fc715e.chunk.js.LICENSE.txt monitoring/static/js/337.b6fc715e.chunk.js.LICENSE.txt
        monitoring/static/js/3397.9c0005a3.chunk.js monitoring/static/js/3397.9c0005a3.chunk.js
        monitoring/static/js/3457.b193afe6.chunk.js monitoring/static/js/3457.b193afe6.chunk.js
        monitoring/static/js/3466.98f036ac.chunk.js monitoring/static/js/3466.98f036ac.chunk.js
        monitoring/static/js/3498.c7d39060.chunk.js monitoring/static/js/3498.c7d39060.chunk.js
        monitoring/static/js/3498.c7d39060.chunk.js.LICENSE.txt monitoring/static/js/3498.c7d39060.chunk.js.LICENSE.txt
        monitoring/static/js/358.d6300019.chunk.js monitoring/static/js/358.d6300019.chunk.js
        monitoring/static/js/358.d6300019.chunk.js.LICENSE.txt monitoring/static/js/358.d6300019.chunk.js.LICENSE.txt
        monitoring/static/js/3621.9b6c61ab.chunk.js monitoring/static/js/3621.9b6c61ab.chunk.js
        monitoring/static/js/3621.9b6c61ab.chunk.js.LICENSE.txt monitoring/static/js/3621.9b6c61ab.chunk.js.LICENSE.txt
        monitoring/static/js/3630.8eda2d3f.chunk.js monitoring/static/js/3630.8eda2d3f.chunk.js
        monitoring/static/js/3644.aeda46ca.chunk.js monitoring/static/js/3644.aeda46ca.chunk.js
        monitoring/static/js/3644.aeda46ca.chunk.js.LICENSE.txt monitoring/static/js/3644.aeda46ca.chunk.js.LICENSE.txt
        monitoring/static/js/3645.bdd20200.chunk.js monitoring/static/js/3645.bdd20200.chunk.js
        monitoring/static/js/3756.67bd6b00.chunk.js monitoring/static/js/3756.67bd6b00.chunk.js
        monitoring/static/js/3757.7c534899.chunk.js monitoring/static/js/3757.7c534899.chunk.js
        monitoring/static/js/3771.764124c3.chunk.js monitoring/static/js/3771.764124c3.chunk.js
        monitoring/static/js/3771.764124c3.chunk.js.LICENSE.txt monitoring/static/js/3771.764124c3.chunk.js.LICENSE.txt
        monitoring/static/js/383.4faec08b.chunk.js monitoring/static/js/383.4faec08b.chunk.js
        monitoring/static/js/3898.1fec42e6.chunk.js monitoring/static/js/3898.1fec42e6.chunk.js
        monitoring/static/js/3920.11b8c9d7.chunk.js monitoring/static/js/3920.11b8c9d7.chunk.js
        monitoring/static/js/3926.8f2c9741.chunk.js monitoring/static/js/3926.8f2c9741.chunk.js
        monitoring/static/js/3945.054c871d.chunk.js monitoring/static/js/3945.054c871d.chunk.js
        monitoring/static/js/4046.5dac72a9.chunk.js monitoring/static/js/4046.5dac72a9.chunk.js
        monitoring/static/js/4046.5dac72a9.chunk.js.LICENSE.txt monitoring/static/js/4046.5dac72a9.chunk.js.LICENSE.txt
        monitoring/static/js/4080.07be3744.chunk.js monitoring/static/js/4080.07be3744.chunk.js
        monitoring/static/js/4123.64882a16.chunk.js monitoring/static/js/4123.64882a16.chunk.js
        monitoring/static/js/4123.64882a16.chunk.js.LICENSE.txt monitoring/static/js/4123.64882a16.chunk.js.LICENSE.txt
        monitoring/static/js/4132.04be158e.chunk.js monitoring/static/js/4132.04be158e.chunk.js
        monitoring/static/js/4159.5e0cfd91.chunk.js monitoring/static/js/4159.5e0cfd91.chunk.js
        monitoring/static/js/4198.d0671061.chunk.js monitoring/static/js/4198.d0671061.chunk.js
        monitoring/static/js/425.c6dd581a.chunk.js monitoring/static/js/425.c6dd581a.chunk.js
        monitoring/static/js/425.c6dd581a.chunk.js.LICENSE.txt monitoring/static/js/425.c6dd581a.chunk.js.LICENSE.txt
        monitoring/static/js/4326.d5c34c54.chunk.js monitoring/static/js/4326.d5c34c54.chunk.js
        monitoring/static/js/4345.9238776d.chunk.js monitoring/static/js/4345.9238776d.chunk.js
        monitoring/static/js/4345.9238776d.chunk.js.LICENSE.txt monitoring/static/js/4345.9238776d.chunk.js.LICENSE.txt
        monitoring/static/js/4347.adf03999.chunk.js monitoring/static/js/4347.adf03999.chunk.js
        monitoring/static/js/436.564ff0f8.chunk.js monitoring/static/js/436.564ff0f8.chunk.js
        monitoring/static/js/4388.edb51304.chunk.js monitoring/static/js/4388.edb51304.chunk.js
        monitoring/static/js/4388.edb51304.chunk.js.LICENSE.txt monitoring/static/js/4388.edb51304.chunk.js.LICENSE.txt
        monitoring/static/js/451.3b449e79.chunk.js monitoring/static/js/451.3b449e79.chunk.js
        monitoring/static/js/451.3b449e79.chunk.js.LICENSE.txt monitoring/static/js/451.3b449e79.chunk.js.LICENSE.txt
        monitoring/static/js/4535.5d1c8322.chunk.js monitoring/static/js/4535.5d1c8322.chunk.js
        monitoring/static/js/4550.2e04d705.chunk.js monitoring/static/js/4550.2e04d705.chunk.js
        monitoring/static/js/4550.2e04d705.chunk.js.LICENSE.txt monitoring/static/js/4550.2e04d705.chunk.js.LICENSE.txt
        monitoring/static/js/4583.1682cf86.chunk.js monitoring/static/js/4583.1682cf86.chunk.js
        monitoring/static/js/4618.131d9563.chunk.js monitoring/static/js/4618.131d9563.chunk.js
        monitoring/static/js/4635.ffa9b6b7.chunk.js monitoring/static/js/4635.ffa9b6b7.chunk.js
        monitoring/static/js/4635.ffa9b6b7.chunk.js.LICENSE.txt monitoring/static/js/4635.ffa9b6b7.chunk.js.LICENSE.txt
        monitoring/static/js/4663.b893c670.chunk.js monitoring/static/js/4663.b893c670.chunk.js
        monitoring/static/js/4684.27f737c4.chunk.js monitoring/static/js/4684.27f737c4.chunk.js
        monitoring/static/js/4789.d52069de.chunk.js monitoring/static/js/4789.d52069de.chunk.js
        monitoring/static/js/4812.73af8448.chunk.js monitoring/static/js/4812.73af8448.chunk.js
        monitoring/static/js/4812.73af8448.chunk.js.LICENSE.txt monitoring/static/js/4812.73af8448.chunk.js.LICENSE.txt
        monitoring/static/js/4814.11309069.chunk.js monitoring/static/js/4814.11309069.chunk.js
        monitoring/static/js/4826.d2723706.chunk.js monitoring/static/js/4826.d2723706.chunk.js
        monitoring/static/js/4842.57182d38.chunk.js monitoring/static/js/4842.57182d38.chunk.js
        monitoring/static/js/4848.64f47dc3.chunk.js monitoring/static/js/4848.64f47dc3.chunk.js
        monitoring/static/js/4949.6bf46e71.chunk.js monitoring/static/js/4949.6bf46e71.chunk.js
        monitoring/static/js/496.5964f8aa.chunk.js monitoring/static/js/496.5964f8aa.chunk.js
        monitoring/static/js/4964.c7c75eb0.chunk.js monitoring/static/js/4964.c7c75eb0.chunk.js
        monitoring/static/js/4985.991de003.chunk.js monitoring/static/js/4985.991de003.chunk.js
        monitoring/static/js/5107.8cac6a03.chunk.js monitoring/static/js/5107.8cac6a03.chunk.js
        monitoring/static/js/5107.8cac6a03.chunk.js.LICENSE.txt monitoring/static/js/5107.8cac6a03.chunk.js.LICENSE.txt
        monitoring/static/js/5112.6189bbe0.chunk.js monitoring/static/js/5112.6189bbe0.chunk.js
        monitoring/static/js/5117.896f7ffb.chunk.js monitoring/static/js/5117.896f7ffb.chunk.js
        monitoring/static/js/515.cd9a8a90.chunk.js monitoring/static/js/515.cd9a8a90.chunk.js
        monitoring/static/js/5161.45b4f520.chunk.js monitoring/static/js/5161.45b4f520.chunk.js
        monitoring/static/js/5168.6fb23f08.chunk.js monitoring/static/js/5168.6fb23f08.chunk.js
        monitoring/static/js/5168.6fb23f08.chunk.js.LICENSE.txt monitoring/static/js/5168.6fb23f08.chunk.js.LICENSE.txt
        monitoring/static/js/5226.675d55fb.chunk.js monitoring/static/js/5226.675d55fb.chunk.js
        monitoring/static/js/5246.224ba018.chunk.js monitoring/static/js/5246.224ba018.chunk.js
        monitoring/static/js/530.582a0d34.chunk.js monitoring/static/js/530.582a0d34.chunk.js
        monitoring/static/js/5311.a500a1ea.chunk.js monitoring/static/js/5311.a500a1ea.chunk.js
        monitoring/static/js/5311.a500a1ea.chunk.js.LICENSE.txt monitoring/static/js/5311.a500a1ea.chunk.js.LICENSE.txt
        monitoring/static/js/5341.2c19c723.chunk.js monitoring/static/js/5341.2c19c723.chunk.js
        monitoring/static/js/5352.3d3187b7.chunk.js monitoring/static/js/5352.3d3187b7.chunk.js
        monitoring/static/js/5373.90c95a6e.chunk.js monitoring/static/js/5373.90c95a6e.chunk.js
        monitoring/static/js/5378.86805fba.chunk.js monitoring/static/js/5378.86805fba.chunk.js
        monitoring/static/js/5378.86805fba.chunk.js.LICENSE.txt monitoring/static/js/5378.86805fba.chunk.js.LICENSE.txt
        monitoring/static/js/5387.8af1d694.chunk.js monitoring/static/js/5387.8af1d694.chunk.js
        monitoring/static/js/5399.f9398084.chunk.js monitoring/static/js/5399.f9398084.chunk.js
        monitoring/static/js/5448.cef3c129.chunk.js monitoring/static/js/5448.cef3c129.chunk.js
        monitoring/static/js/5450.f0dcfc15.chunk.js monitoring/static/js/5450.f0dcfc15.chunk.js
        monitoring/static/js/5491.a460479e.chunk.js monitoring/static/js/5491.a460479e.chunk.js
        monitoring/static/js/556.55f00ac6.chunk.js monitoring/static/js/556.55f00ac6.chunk.js
        monitoring/static/js/5643.00957838.chunk.js monitoring/static/js/5643.00957838.chunk.js
        monitoring/static/js/5661.c83a4eb0.chunk.js monitoring/static/js/5661.c83a4eb0.chunk.js
        monitoring/static/js/5661.c83a4eb0.chunk.js.LICENSE.txt monitoring/static/js/5661.c83a4eb0.chunk.js.LICENSE.txt
        monitoring/static/js/5670.5c30cef1.chunk.js monitoring/static/js/5670.5c30cef1.chunk.js
        monitoring/static/js/5715.0941d934.chunk.js monitoring/static/js/5715.0941d934.chunk.js
        monitoring/static/js/5720.39a954f1.chunk.js monitoring/static/js/5720.39a954f1.chunk.js
        monitoring/static/js/5790.e3d88e2c.chunk.js monitoring/static/js/5790.e3d88e2c.chunk.js
        monitoring/static/js/5790.e3d88e2c.chunk.js.LICENSE.txt monitoring/static/js/5790.e3d88e2c.chunk.js.LICENSE.txt
        monitoring/static/js/5809.d78ebebb.chunk.js monitoring/static/js/5809.d78ebebb.chunk.js
        monitoring/static/js/5863.e2cd2452.chunk.js monitoring/static/js/5863.e2cd2452.chunk.js
        monitoring/static/js/5868.be04313a.chunk.js monitoring/static/js/5868.be04313a.chunk.js
        monitoring/static/js/5868.be04313a.chunk.js.LICENSE.txt monitoring/static/js/5868.be04313a.chunk.js.LICENSE.txt
        monitoring/static/js/598.243fd68d.chunk.js monitoring/static/js/598.243fd68d.chunk.js
        monitoring/static/js/599.c58caf58.chunk.js monitoring/static/js/599.c58caf58.chunk.js
        monitoring/static/js/6044.2de9962d.chunk.js monitoring/static/js/6044.2de9962d.chunk.js
        monitoring/static/js/6044.2de9962d.chunk.js.LICENSE.txt monitoring/static/js/6044.2de9962d.chunk.js.LICENSE.txt
        monitoring/static/js/6058.7f474f92.chunk.js monitoring/static/js/6058.7f474f92.chunk.js
        monitoring/static/js/6065.b08e9640.chunk.js monitoring/static/js/6065.b08e9640.chunk.js
        monitoring/static/js/6142.b2452554.chunk.js monitoring/static/js/6142.b2452554.chunk.js
        monitoring/static/js/6142.b2452554.chunk.js.LICENSE.txt monitoring/static/js/6142.b2452554.chunk.js.LICENSE.txt
        monitoring/static/js/6144.e1568f26.chunk.js monitoring/static/js/6144.e1568f26.chunk.js
        monitoring/static/js/6156.0c562627.chunk.js monitoring/static/js/6156.0c562627.chunk.js
        monitoring/static/js/619.f27ddcbd.chunk.js monitoring/static/js/619.f27ddcbd.chunk.js
        monitoring/static/js/619.f27ddcbd.chunk.js.LICENSE.txt monitoring/static/js/619.f27ddcbd.chunk.js.LICENSE.txt
        monitoring/static/js/620.7aea5425.chunk.js monitoring/static/js/620.7aea5425.chunk.js
        monitoring/static/js/6227.fc562bbf.chunk.js monitoring/static/js/6227.fc562bbf.chunk.js
        monitoring/static/js/6230.8e64216a.chunk.js monitoring/static/js/6230.8e64216a.chunk.js
        monitoring/static/js/6230.8e64216a.chunk.js.LICENSE.txt monitoring/static/js/6230.8e64216a.chunk.js.LICENSE.txt
        monitoring/static/js/6289.51f8741e.chunk.js monitoring/static/js/6289.51f8741e.chunk.js
        monitoring/static/js/6289.51f8741e.chunk.js.LICENSE.txt monitoring/static/js/6289.51f8741e.chunk.js.LICENSE.txt
        monitoring/static/js/6291.e7cdf7f2.chunk.js monitoring/static/js/6291.e7cdf7f2.chunk.js
        monitoring/static/js/6300.dca75d45.chunk.js monitoring/static/js/6300.dca75d45.chunk.js
        monitoring/static/js/6300.dca75d45.chunk.js.LICENSE.txt monitoring/static/js/6300.dca75d45.chunk.js.LICENSE.txt
        monitoring/static/js/632.b6c03857.chunk.js monitoring/static/js/632.b6c03857.chunk.js
        monitoring/static/js/6321.aa3e44de.chunk.js monitoring/static/js/6321.aa3e44de.chunk.js
        monitoring/static/js/6321.aa3e44de.chunk.js.LICENSE.txt monitoring/static/js/6321.aa3e44de.chunk.js.LICENSE.txt
        monitoring/static/js/6329.d78c1432.chunk.js monitoring/static/js/6329.d78c1432.chunk.js
        monitoring/static/js/6329.d78c1432.chunk.js.LICENSE.txt monitoring/static/js/6329.d78c1432.chunk.js.LICENSE.txt
        monitoring/static/js/6361.a9f11e7a.chunk.js monitoring/static/js/6361.a9f11e7a.chunk.js
        monitoring/static/js/6390.497d0ec8.chunk.js monitoring/static/js/6390.497d0ec8.chunk.js
        monitoring/static/js/6390.497d0ec8.chunk.js.LICENSE.txt monitoring/static/js/6390.497d0ec8.chunk.js.LICENSE.txt
        monitoring/static/js/6392.134ee5e4.chunk.js monitoring/static/js/6392.134ee5e4.chunk.js
        monitoring/static/js/6393.b0de2d9e.chunk.js monitoring/static/js/6393.b0de2d9e.chunk.js
        monitoring/static/js/6521.371403ec.chunk.js monitoring/static/js/6521.371403ec.chunk.js
        monitoring/static/js/6531.fbd78a3e.chunk.js monitoring/static/js/6531.fbd78a3e.chunk.js
        monitoring/static/js/6619.9e1de7a6.chunk.js monitoring/static/js/6619.9e1de7a6.chunk.js
        monitoring/static/js/6619.9e1de7a6.chunk.js.LICENSE.txt monitoring/static/js/6619.9e1de7a6.chunk.js.LICENSE.txt
        monitoring/static/js/6679.6e0a87d5.chunk.js monitoring/static/js/6679.6e0a87d5.chunk.js
        monitoring/static/js/6692.9322b59d.chunk.js monitoring/static/js/6692.9322b59d.chunk.js
        monitoring/static/js/6692.9322b59d.chunk.js.LICENSE.txt monitoring/static/js/6692.9322b59d.chunk.js.LICENSE.txt
        monitoring/static/js/674.e6536250.chunk.js monitoring/static/js/674.e6536250.chunk.js
        monitoring/static/js/678.b73063ff.chunk.js monitoring/static/js/678.b73063ff.chunk.js
        monitoring/static/js/6795.5ec0c96a.chunk.js monitoring/static/js/6795.5ec0c96a.chunk.js
        monitoring/static/js/6795.5ec0c96a.chunk.js.LICENSE.txt monitoring/static/js/6795.5ec0c96a.chunk.js.LICENSE.txt
        monitoring/static/js/6815.672badd5.chunk.js monitoring/static/js/6815.672badd5.chunk.js
        monitoring/static/js/6876.867b698c.chunk.js monitoring/static/js/6876.867b698c.chunk.js
        monitoring/static/js/6877.d2d51d98.chunk.js monitoring/static/js/6877.d2d51d98.chunk.js
        monitoring/static/js/6887.0855fd66.chunk.js monitoring/static/js/6887.0855fd66.chunk.js
        monitoring/static/js/6892.2c3c2bcb.chunk.js monitoring/static/js/6892.2c3c2bcb.chunk.js
        monitoring/static/js/6898.5580b941.chunk.js monitoring/static/js/6898.5580b941.chunk.js
        monitoring/static/js/6898.5580b941.chunk.js.LICENSE.txt monitoring/static/js/6898.5580b941.chunk.js.LICENSE.txt
        monitoring/static/js/6919.84ed9ccc.chunk.js monitoring/static/js/6919.84ed9ccc.chunk.js
        monitoring/static/js/6919.84ed9ccc.chunk.js.LICENSE.txt monitoring/static/js/6919.84ed9ccc.chunk.js.LICENSE.txt
        monitoring/static/js/6954.e18be130.chunk.js monitoring/static/js/6954.e18be130.chunk.js
        monitoring/static/js/6961.f4888ae1.chunk.js monitoring/static/js/6961.f4888ae1.chunk.js
        monitoring/static/js/7016.4a34a027.chunk.js monitoring/static/js/7016.4a34a027.chunk.js
        monitoring/static/js/704.45771d88.chunk.js monitoring/static/js/704.45771d88.chunk.js
        monitoring/static/js/7045.53fa1aaf.chunk.js monitoring/static/js/7045.53fa1aaf.chunk.js
        monitoring/static/js/7119.e94f8dac.chunk.js monitoring/static/js/7119.e94f8dac.chunk.js
        monitoring/static/js/7202.fefd43ee.chunk.js monitoring/static/js/7202.fefd43ee.chunk.js
        monitoring/static/js/7257.8ce0d045.chunk.js monitoring/static/js/7257.8ce0d045.chunk.js
        monitoring/static/js/7276.47f377a4.chunk.js monitoring/static/js/7276.47f377a4.chunk.js
        monitoring/static/js/7388.9f447514.chunk.js monitoring/static/js/7388.9f447514.chunk.js
        monitoring/static/js/7409.4408962b.chunk.js monitoring/static/js/7409.4408962b.chunk.js
        monitoring/static/js/7418.8548a710.chunk.js monitoring/static/js/7418.8548a710.chunk.js
        monitoring/static/js/7520.d245d6ac.chunk.js monitoring/static/js/7520.d245d6ac.chunk.js
        monitoring/static/js/7520.d245d6ac.chunk.js.LICENSE.txt monitoring/static/js/7520.d245d6ac.chunk.js.LICENSE.txt
        monitoring/static/js/7522.1a0f9c02.chunk.js monitoring/static/js/7522.1a0f9c02.chunk.js
        monitoring/static/js/7529.ddf87a9a.chunk.js monitoring/static/js/7529.ddf87a9a.chunk.js
        monitoring/static/js/7529.ddf87a9a.chunk.js.LICENSE.txt monitoring/static/js/7529.ddf87a9a.chunk.js.LICENSE.txt
        monitoring/static/js/7542.d61fc913.chunk.js monitoring/static/js/7542.d61fc913.chunk.js
        monitoring/static/js/7543.3fcfd3ba.chunk.js monitoring/static/js/7543.3fcfd3ba.chunk.js
        monitoring/static/js/7543.3fcfd3ba.chunk.js.LICENSE.txt monitoring/static/js/7543.3fcfd3ba.chunk.js.LICENSE.txt
        monitoring/static/js/7554.28f3da22.chunk.js monitoring/static/js/7554.28f3da22.chunk.js
        monitoring/static/js/7554.28f3da22.chunk.js.LICENSE.txt monitoring/static/js/7554.28f3da22.chunk.js.LICENSE.txt
        monitoring/static/js/7645.6565454c.chunk.js monitoring/static/js/7645.6565454c.chunk.js
        monitoring/static/js/7684.a3920b72.chunk.js monitoring/static/js/7684.a3920b72.chunk.js
        monitoring/static/js/7779.9d9b07ae.chunk.js monitoring/static/js/7779.9d9b07ae.chunk.js
        monitoring/static/js/7803.a56cfca6.chunk.js monitoring/static/js/7803.a56cfca6.chunk.js
        monitoring/static/js/783.95eb5b37.chunk.js monitoring/static/js/783.95eb5b37.chunk.js
        monitoring/static/js/785.d2eae69c.chunk.js monitoring/static/js/785.d2eae69c.chunk.js
        monitoring/static/js/785.d2eae69c.chunk.js.LICENSE.txt monitoring/static/js/785.d2eae69c.chunk.js.LICENSE.txt
        monitoring/static/js/7992.20690745.chunk.js monitoring/static/js/7992.20690745.chunk.js
        monitoring/static/js/7999.bdf4fe79.chunk.js monitoring/static/js/7999.bdf4fe79.chunk.js
        monitoring/static/js/8011.4fed4307.chunk.js monitoring/static/js/8011.4fed4307.chunk.js
        monitoring/static/js/8065.666ef449.chunk.js monitoring/static/js/8065.666ef449.chunk.js
        monitoring/static/js/8065.666ef449.chunk.js.LICENSE.txt monitoring/static/js/8065.666ef449.chunk.js.LICENSE.txt
        monitoring/static/js/8076.dac0f4f1.chunk.js monitoring/static/js/8076.dac0f4f1.chunk.js
        monitoring/static/js/8133.2afc4db4.chunk.js monitoring/static/js/8133.2afc4db4.chunk.js
        monitoring/static/js/8140.8d8e9309.chunk.js monitoring/static/js/8140.8d8e9309.chunk.js
        monitoring/static/js/8167.b9a90da5.chunk.js monitoring/static/js/8167.b9a90da5.chunk.js
        monitoring/static/js/8424.5b5c42b5.chunk.js monitoring/static/js/8424.5b5c42b5.chunk.js
        monitoring/static/js/8424.5b5c42b5.chunk.js.LICENSE.txt monitoring/static/js/8424.5b5c42b5.chunk.js.LICENSE.txt
        monitoring/static/js/8429.b285ce5a.chunk.js monitoring/static/js/8429.b285ce5a.chunk.js
        monitoring/static/js/8450.baf3a89d.chunk.js monitoring/static/js/8450.baf3a89d.chunk.js
        monitoring/static/js/8450.baf3a89d.chunk.js.LICENSE.txt monitoring/static/js/8450.baf3a89d.chunk.js.LICENSE.txt
        monitoring/static/js/8591.93172fe9.chunk.js monitoring/static/js/8591.93172fe9.chunk.js
        monitoring/static/js/86.ad271bdc.chunk.js monitoring/static/js/86.ad271bdc.chunk.js
        monitoring/static/js/86.ad271bdc.chunk.js.LICENSE.txt monitoring/static/js/86.ad271bdc.chunk.js.LICENSE.txt
        monitoring/static/js/8607.e8952666.chunk.js monitoring/static/js/8607.e8952666.chunk.js
        monitoring/static/js/8622.49f3054c.chunk.js monitoring/static/js/8622.49f3054c.chunk.js
        monitoring/static/js/8695.f17f8853.chunk.js monitoring/static/js/8695.f17f8853.chunk.js
        monitoring/static/js/8702.69a3e0d5.chunk.js monitoring/static/js/8702.69a3e0d5.chunk.js
        monitoring/static/js/8747.baf63d86.chunk.js monitoring/static/js/8747.baf63d86.chunk.js
        monitoring/static/js/8791.b209de42.chunk.js monitoring/static/js/8791.b209de42.chunk.js
        monitoring/static/js/8791.b209de42.chunk.js.LICENSE.txt monitoring/static/js/8791.b209de42.chunk.js.LICENSE.txt
        monitoring/static/js/8797.f8f0ce13.chunk.js monitoring/static/js/8797.f8f0ce13.chunk.js
        monitoring/static/js/8797.f8f0ce13.chunk.js.LICENSE.txt monitoring/static/js/8797.f8f0ce13.chunk.js.LICENSE.txt
        monitoring/static/js/8850.97635389.chunk.js monitoring/static/js/8850.97635389.chunk.js
        monitoring/static/js/8853.c8f9e9d6.chunk.js monitoring/static/js/8853.c8f9e9d6.chunk.js
        monitoring/static/js/8858.cd9d49a5.chunk.js monitoring/static/js/8858.cd9d49a5.chunk.js
        monitoring/static/js/8905.b8a9fd91.chunk.js monitoring/static/js/8905.b8a9fd91.chunk.js
        monitoring/static/js/8905.b8a9fd91.chunk.js.LICENSE.txt monitoring/static/js/8905.b8a9fd91.chunk.js.LICENSE.txt
        monitoring/static/js/9101.ce051539.chunk.js monitoring/static/js/9101.ce051539.chunk.js
        monitoring/static/js/9173.71d773f2.chunk.js monitoring/static/js/9173.71d773f2.chunk.js
        monitoring/static/js/9173.71d773f2.chunk.js.LICENSE.txt monitoring/static/js/9173.71d773f2.chunk.js.LICENSE.txt
        monitoring/static/js/919.53e04507.chunk.js monitoring/static/js/919.53e04507.chunk.js
        monitoring/static/js/919.53e04507.chunk.js.LICENSE.txt monitoring/static/js/919.53e04507.chunk.js.LICENSE.txt
        monitoring/static/js/9204.77418f94.chunk.js monitoring/static/js/9204.77418f94.chunk.js
        monitoring/static/js/9207.5881b206.chunk.js monitoring/static/js/9207.5881b206.chunk.js
        monitoring/static/js/9212.870f16f0.chunk.js monitoring/static/js/9212.870f16f0.chunk.js
        monitoring/static/js/9219.24a20881.chunk.js monitoring/static/js/9219.24a20881.chunk.js
        monitoring/static/js/924.382f18b1.chunk.js monitoring/static/js/924.382f18b1.chunk.js
        monitoring/static/js/924.382f18b1.chunk.js.LICENSE.txt monitoring/static/js/924.382f18b1.chunk.js.LICENSE.txt
        monitoring/static/js/9280.40cff028.chunk.js monitoring/static/js/9280.40cff028.chunk.js
        monitoring/static/js/9292.91ed23f7.chunk.js monitoring/static/js/9292.91ed23f7.chunk.js
        monitoring/static/js/9297.eadc4dba.chunk.js monitoring/static/js/9297.eadc4dba.chunk.js
        monitoring/static/js/9308.c72b8585.chunk.js monitoring/static/js/9308.c72b8585.chunk.js
        monitoring/static/js/9319.40f9e46a.chunk.js monitoring/static/js/9319.40f9e46a.chunk.js
        monitoring/static/js/9319.40f9e46a.chunk.js.LICENSE.txt monitoring/static/js/9319.40f9e46a.chunk.js.LICENSE.txt
        monitoring/static/js/9371.b42befbc.chunk.js monitoring/static/js/9371.b42befbc.chunk.js
        monitoring/static/js/9371.b42befbc.chunk.js.LICENSE.txt monitoring/static/js/9371.b42befbc.chunk.js.LICENSE.txt
        monitoring/static/js/9411.96fb3e2f.chunk.js monitoring/static/js/9411.96fb3e2f.chunk.js
        monitoring/static/js/9413.b2921c36.chunk.js monitoring/static/js/9413.b2921c36.chunk.js
        monitoring/static/js/9433.7ce648d0.chunk.js monitoring/static/js/9433.7ce648d0.chunk.js
        monitoring/static/js/9433.7ce648d0.chunk.js.LICENSE.txt monitoring/static/js/9433.7ce648d0.chunk.js.LICENSE.txt
        monitoring/static/js/9526.10bb1684.chunk.js monitoring/static/js/9526.10bb1684.chunk.js
        monitoring/static/js/9526.10bb1684.chunk.js.LICENSE.txt monitoring/static/js/9526.10bb1684.chunk.js.LICENSE.txt
        monitoring/static/js/9528.9991c023.chunk.js monitoring/static/js/9528.9991c023.chunk.js
        monitoring/static/js/9555.c9b5ee61.chunk.js monitoring/static/js/9555.c9b5ee61.chunk.js
        monitoring/static/js/9572.9f83f004.chunk.js monitoring/static/js/9572.9f83f004.chunk.js
        monitoring/static/js/96.6e1bf3f4.chunk.js monitoring/static/js/96.6e1bf3f4.chunk.js
        monitoring/static/js/9621.48073631.chunk.js monitoring/static/js/9621.48073631.chunk.js
        monitoring/static/js/9621.48073631.chunk.js.LICENSE.txt monitoring/static/js/9621.48073631.chunk.js.LICENSE.txt
        monitoring/static/js/9759.7dadb893.chunk.js monitoring/static/js/9759.7dadb893.chunk.js
        monitoring/static/js/9818.24b1ff88.chunk.js monitoring/static/js/9818.24b1ff88.chunk.js
        monitoring/static/js/9876.b336d1f5.chunk.js monitoring/static/js/9876.b336d1f5.chunk.js
        monitoring/static/js/9876.b336d1f5.chunk.js.LICENSE.txt monitoring/static/js/9876.b336d1f5.chunk.js.LICENSE.txt
        monitoring/static/js/9917.67d792e3.chunk.js monitoring/static/js/9917.67d792e3.chunk.js
        monitoring/static/js/9923.270f0a19.chunk.js monitoring/static/js/9923.270f0a19.chunk.js
        monitoring/static/js/9923.270f0a19.chunk.js.LICENSE.txt monitoring/static/js/9923.270f0a19.chunk.js.LICENSE.txt
        monitoring/static/js/main.9fede0a0.js monitoring/static/js/main.9fede0a0.js
        monitoring/static/js/main.9fede0a0.js.LICENSE.txt monitoring/static/js/main.9fede0a0.js.LICENSE.txt
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.762fced46d6cddbda272.ttf monitoring/static/media/codicon.762fced46d6cddbda272.ttf
        monitoring/static/media/error.9bbd075178a739dcc30f2a7a3e2a3249.svg monitoring/static/media/error.9bbd075178a739dcc30f2a7a3e2a3249.svg
        monitoring/static/media/error.ca9e31d5d3dc34da07e11a00f7af0842.svg monitoring/static/media/error.ca9e31d5d3dc34da07e11a00f7af0842.svg
        monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg
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
    ydb/public/api/protos
    ydb/public/lib/deprecated/kicli
    ydb/public/lib/json_value
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types
    contrib/libs/yaml-cpp
)

YQL_LAST_ABI_VERSION()

END()
