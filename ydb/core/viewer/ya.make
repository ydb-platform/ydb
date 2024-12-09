RECURSE(
    json
    yaml
)

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
        monitoring/static/css/1198.3fe0ca8d.chunk.css monitoring/static/css/1198.3fe0ca8d.chunk.css
        monitoring/static/css/1276.4de79a91.chunk.css monitoring/static/css/1276.4de79a91.chunk.css
        monitoring/static/css/2881.afcb07f6.chunk.css monitoring/static/css/2881.afcb07f6.chunk.css
        monitoring/static/css/3017.3afd70ce.chunk.css monitoring/static/css/3017.3afd70ce.chunk.css
        monitoring/static/css/328.86929762.chunk.css monitoring/static/css/328.86929762.chunk.css
        monitoring/static/css/3933.5f7354e1.chunk.css monitoring/static/css/3933.5f7354e1.chunk.css
        monitoring/static/css/4135.f4bca932.chunk.css monitoring/static/css/4135.f4bca932.chunk.css
        monitoring/static/css/5118.a86ede1f.chunk.css monitoring/static/css/5118.a86ede1f.chunk.css
        monitoring/static/css/5879.555349bb.chunk.css monitoring/static/css/5879.555349bb.chunk.css
        monitoring/static/css/6395.f2109564.chunk.css monitoring/static/css/6395.f2109564.chunk.css
        monitoring/static/css/6866.2c643863.chunk.css monitoring/static/css/6866.2c643863.chunk.css
        monitoring/static/css/6894.dec87f60.chunk.css monitoring/static/css/6894.dec87f60.chunk.css
        monitoring/static/css/7054.c5c1bf09.chunk.css monitoring/static/css/7054.c5c1bf09.chunk.css
        monitoring/static/css/main.26df18be.css monitoring/static/css/main.26df18be.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1035.3975a471.chunk.js monitoring/static/js/1035.3975a471.chunk.js
        monitoring/static/js/1072.13801473.chunk.js monitoring/static/js/1072.13801473.chunk.js
        monitoring/static/js/1074.27b17806.chunk.js monitoring/static/js/1074.27b17806.chunk.js
        monitoring/static/js/1109.834e0892.chunk.js monitoring/static/js/1109.834e0892.chunk.js
        monitoring/static/js/1109.834e0892.chunk.js.LICENSE.txt monitoring/static/js/1109.834e0892.chunk.js.LICENSE.txt
        monitoring/static/js/1198.40c55cbb.chunk.js monitoring/static/js/1198.40c55cbb.chunk.js
        monitoring/static/js/1198.40c55cbb.chunk.js.LICENSE.txt monitoring/static/js/1198.40c55cbb.chunk.js.LICENSE.txt
        monitoring/static/js/1222.5faf3e57.chunk.js monitoring/static/js/1222.5faf3e57.chunk.js
        monitoring/static/js/1222.5faf3e57.chunk.js.LICENSE.txt monitoring/static/js/1222.5faf3e57.chunk.js.LICENSE.txt
        monitoring/static/js/1236.2027a259.chunk.js monitoring/static/js/1236.2027a259.chunk.js
        monitoring/static/js/127.87732853.chunk.js monitoring/static/js/127.87732853.chunk.js
        monitoring/static/js/1276.bc76b58b.chunk.js monitoring/static/js/1276.bc76b58b.chunk.js
        monitoring/static/js/1376.ef761c31.chunk.js monitoring/static/js/1376.ef761c31.chunk.js
        monitoring/static/js/1376.ef761c31.chunk.js.LICENSE.txt monitoring/static/js/1376.ef761c31.chunk.js.LICENSE.txt
        monitoring/static/js/1388.48ada893.chunk.js monitoring/static/js/1388.48ada893.chunk.js
        monitoring/static/js/1389.b6125fec.chunk.js monitoring/static/js/1389.b6125fec.chunk.js
        monitoring/static/js/1410.5d95e270.chunk.js monitoring/static/js/1410.5d95e270.chunk.js
        monitoring/static/js/1410.5d95e270.chunk.js.LICENSE.txt monitoring/static/js/1410.5d95e270.chunk.js.LICENSE.txt
        monitoring/static/js/1455.867345a6.chunk.js monitoring/static/js/1455.867345a6.chunk.js
        monitoring/static/js/1460.b46aac4e.chunk.js monitoring/static/js/1460.b46aac4e.chunk.js
        monitoring/static/js/1460.b46aac4e.chunk.js.LICENSE.txt monitoring/static/js/1460.b46aac4e.chunk.js.LICENSE.txt
        monitoring/static/js/1487.e009a462.chunk.js monitoring/static/js/1487.e009a462.chunk.js
        monitoring/static/js/1487.e009a462.chunk.js.LICENSE.txt monitoring/static/js/1487.e009a462.chunk.js.LICENSE.txt
        monitoring/static/js/1577.17bb1420.chunk.js monitoring/static/js/1577.17bb1420.chunk.js
        monitoring/static/js/1640.5929573a.chunk.js monitoring/static/js/1640.5929573a.chunk.js
        monitoring/static/js/1640.5929573a.chunk.js.LICENSE.txt monitoring/static/js/1640.5929573a.chunk.js.LICENSE.txt
        monitoring/static/js/1670.2dbca85f.chunk.js monitoring/static/js/1670.2dbca85f.chunk.js
        monitoring/static/js/1670.2dbca85f.chunk.js.LICENSE.txt monitoring/static/js/1670.2dbca85f.chunk.js.LICENSE.txt
        monitoring/static/js/1728.9b6329a2.chunk.js monitoring/static/js/1728.9b6329a2.chunk.js
        monitoring/static/js/1758.6eae603b.chunk.js monitoring/static/js/1758.6eae603b.chunk.js
        monitoring/static/js/1758.6eae603b.chunk.js.LICENSE.txt monitoring/static/js/1758.6eae603b.chunk.js.LICENSE.txt
        monitoring/static/js/178.06d01a51.chunk.js monitoring/static/js/178.06d01a51.chunk.js
        monitoring/static/js/182.e718ea47.chunk.js monitoring/static/js/182.e718ea47.chunk.js
        monitoring/static/js/1854.d147fe93.chunk.js monitoring/static/js/1854.d147fe93.chunk.js
        monitoring/static/js/1854.d147fe93.chunk.js.LICENSE.txt monitoring/static/js/1854.d147fe93.chunk.js.LICENSE.txt
        monitoring/static/js/1855.56d941ac.chunk.js monitoring/static/js/1855.56d941ac.chunk.js
        monitoring/static/js/1886.28e0fe60.chunk.js monitoring/static/js/1886.28e0fe60.chunk.js
        monitoring/static/js/1923.38b88781.chunk.js monitoring/static/js/1923.38b88781.chunk.js
        monitoring/static/js/1948.5891b584.chunk.js monitoring/static/js/1948.5891b584.chunk.js
        monitoring/static/js/1948.5891b584.chunk.js.LICENSE.txt monitoring/static/js/1948.5891b584.chunk.js.LICENSE.txt
        monitoring/static/js/1957.6f0516a3.chunk.js monitoring/static/js/1957.6f0516a3.chunk.js
        monitoring/static/js/1957.6f0516a3.chunk.js.LICENSE.txt monitoring/static/js/1957.6f0516a3.chunk.js.LICENSE.txt
        monitoring/static/js/1971.4d7bf65b.chunk.js monitoring/static/js/1971.4d7bf65b.chunk.js
        monitoring/static/js/2039.1eea423d.chunk.js monitoring/static/js/2039.1eea423d.chunk.js
        monitoring/static/js/2070.aeb8e3ff.chunk.js monitoring/static/js/2070.aeb8e3ff.chunk.js
        monitoring/static/js/2100.853fb83b.chunk.js monitoring/static/js/2100.853fb83b.chunk.js
        monitoring/static/js/2216.b37b568b.chunk.js monitoring/static/js/2216.b37b568b.chunk.js
        monitoring/static/js/225.6ec6c3bc.chunk.js monitoring/static/js/225.6ec6c3bc.chunk.js
        monitoring/static/js/2262.df0b7687.chunk.js monitoring/static/js/2262.df0b7687.chunk.js
        monitoring/static/js/2262.df0b7687.chunk.js.LICENSE.txt monitoring/static/js/2262.df0b7687.chunk.js.LICENSE.txt
        monitoring/static/js/228.15a25302.chunk.js monitoring/static/js/228.15a25302.chunk.js
        monitoring/static/js/228.15a25302.chunk.js.LICENSE.txt monitoring/static/js/228.15a25302.chunk.js.LICENSE.txt
        monitoring/static/js/2293.bb3e0c81.chunk.js monitoring/static/js/2293.bb3e0c81.chunk.js
        monitoring/static/js/2293.bb3e0c81.chunk.js.LICENSE.txt monitoring/static/js/2293.bb3e0c81.chunk.js.LICENSE.txt
        monitoring/static/js/2405.94de1a6b.chunk.js monitoring/static/js/2405.94de1a6b.chunk.js
        monitoring/static/js/2421.996f12f5.chunk.js monitoring/static/js/2421.996f12f5.chunk.js
        monitoring/static/js/2455.e995eba9.chunk.js monitoring/static/js/2455.e995eba9.chunk.js
        monitoring/static/js/2503.69406a9c.chunk.js monitoring/static/js/2503.69406a9c.chunk.js
        monitoring/static/js/2516.ae307d9a.chunk.js monitoring/static/js/2516.ae307d9a.chunk.js
        monitoring/static/js/253.f7d84b09.chunk.js monitoring/static/js/253.f7d84b09.chunk.js
        monitoring/static/js/2570.b620520c.chunk.js monitoring/static/js/2570.b620520c.chunk.js
        monitoring/static/js/2570.b620520c.chunk.js.LICENSE.txt monitoring/static/js/2570.b620520c.chunk.js.LICENSE.txt
        monitoring/static/js/2598.ca5e9f76.chunk.js monitoring/static/js/2598.ca5e9f76.chunk.js
        monitoring/static/js/2651.3cdca23d.chunk.js monitoring/static/js/2651.3cdca23d.chunk.js
        monitoring/static/js/2670.6f0af2f6.chunk.js monitoring/static/js/2670.6f0af2f6.chunk.js
        monitoring/static/js/2734.0ddcbf8e.chunk.js monitoring/static/js/2734.0ddcbf8e.chunk.js
        monitoring/static/js/2769.5a17f94c.chunk.js monitoring/static/js/2769.5a17f94c.chunk.js
        monitoring/static/js/2769.5a17f94c.chunk.js.LICENSE.txt monitoring/static/js/2769.5a17f94c.chunk.js.LICENSE.txt
        monitoring/static/js/2780.482ff693.chunk.js monitoring/static/js/2780.482ff693.chunk.js
        monitoring/static/js/2804.16f024df.chunk.js monitoring/static/js/2804.16f024df.chunk.js
        monitoring/static/js/2807.8fbe9929.chunk.js monitoring/static/js/2807.8fbe9929.chunk.js
        monitoring/static/js/2807.8fbe9929.chunk.js.LICENSE.txt monitoring/static/js/2807.8fbe9929.chunk.js.LICENSE.txt
        monitoring/static/js/2840.9c4aae7d.chunk.js monitoring/static/js/2840.9c4aae7d.chunk.js
        monitoring/static/js/2845.5e07c0d9.chunk.js monitoring/static/js/2845.5e07c0d9.chunk.js
        monitoring/static/js/2869.a31f24d4.chunk.js monitoring/static/js/2869.a31f24d4.chunk.js
        monitoring/static/js/2869.a31f24d4.chunk.js.LICENSE.txt monitoring/static/js/2869.a31f24d4.chunk.js.LICENSE.txt
        monitoring/static/js/2871.7f64530d.chunk.js monitoring/static/js/2871.7f64530d.chunk.js
        monitoring/static/js/2881.44a91214.chunk.js monitoring/static/js/2881.44a91214.chunk.js
        monitoring/static/js/2967.f31227d9.chunk.js monitoring/static/js/2967.f31227d9.chunk.js
        monitoring/static/js/2972.e9dd6d38.chunk.js monitoring/static/js/2972.e9dd6d38.chunk.js
        monitoring/static/js/2974.2083f7ab.chunk.js monitoring/static/js/2974.2083f7ab.chunk.js
        monitoring/static/js/3010.ba0b7e6c.chunk.js monitoring/static/js/3010.ba0b7e6c.chunk.js
        monitoring/static/js/3010.ba0b7e6c.chunk.js.LICENSE.txt monitoring/static/js/3010.ba0b7e6c.chunk.js.LICENSE.txt
        monitoring/static/js/3017.cd53b447.chunk.js monitoring/static/js/3017.cd53b447.chunk.js
        monitoring/static/js/3062.5a010e35.chunk.js monitoring/static/js/3062.5a010e35.chunk.js
        monitoring/static/js/3092.b2453934.chunk.js monitoring/static/js/3092.b2453934.chunk.js
        monitoring/static/js/3107.8a298574.chunk.js monitoring/static/js/3107.8a298574.chunk.js
        monitoring/static/js/3107.8a298574.chunk.js.LICENSE.txt monitoring/static/js/3107.8a298574.chunk.js.LICENSE.txt
        monitoring/static/js/3145.6fe2a282.chunk.js monitoring/static/js/3145.6fe2a282.chunk.js
        monitoring/static/js/3164.4fedcbc1.chunk.js monitoring/static/js/3164.4fedcbc1.chunk.js
        monitoring/static/js/3164.4fedcbc1.chunk.js.LICENSE.txt monitoring/static/js/3164.4fedcbc1.chunk.js.LICENSE.txt
        monitoring/static/js/3166.cccccfe6.chunk.js monitoring/static/js/3166.cccccfe6.chunk.js
        monitoring/static/js/3166.cccccfe6.chunk.js.LICENSE.txt monitoring/static/js/3166.cccccfe6.chunk.js.LICENSE.txt
        monitoring/static/js/323.04d21c99.chunk.js monitoring/static/js/323.04d21c99.chunk.js
        monitoring/static/js/3230.9e1f2161.chunk.js monitoring/static/js/3230.9e1f2161.chunk.js
        monitoring/static/js/3230.9e1f2161.chunk.js.LICENSE.txt monitoring/static/js/3230.9e1f2161.chunk.js.LICENSE.txt
        monitoring/static/js/3235.98e63dfc.chunk.js monitoring/static/js/3235.98e63dfc.chunk.js
        monitoring/static/js/328.a089accc.chunk.js monitoring/static/js/328.a089accc.chunk.js
        monitoring/static/js/3283.d4976b24.chunk.js monitoring/static/js/3283.d4976b24.chunk.js
        monitoring/static/js/3421.67ee8518.chunk.js monitoring/static/js/3421.67ee8518.chunk.js
        monitoring/static/js/3421.67ee8518.chunk.js.LICENSE.txt monitoring/static/js/3421.67ee8518.chunk.js.LICENSE.txt
        monitoring/static/js/346.e1c8a643.chunk.js monitoring/static/js/346.e1c8a643.chunk.js
        monitoring/static/js/346.e1c8a643.chunk.js.LICENSE.txt monitoring/static/js/346.e1c8a643.chunk.js.LICENSE.txt
        monitoring/static/js/3493.f1cd881c.chunk.js monitoring/static/js/3493.f1cd881c.chunk.js
        monitoring/static/js/3495.b1e292fa.chunk.js monitoring/static/js/3495.b1e292fa.chunk.js
        monitoring/static/js/3520.038b81f7.chunk.js monitoring/static/js/3520.038b81f7.chunk.js
        monitoring/static/js/3607.c222c9e3.chunk.js monitoring/static/js/3607.c222c9e3.chunk.js
        monitoring/static/js/3643.05f9828c.chunk.js monitoring/static/js/3643.05f9828c.chunk.js
        monitoring/static/js/3768.ce22b76d.chunk.js monitoring/static/js/3768.ce22b76d.chunk.js
        monitoring/static/js/3828.d46cab67.chunk.js monitoring/static/js/3828.d46cab67.chunk.js
        monitoring/static/js/3830.f20a084f.chunk.js monitoring/static/js/3830.f20a084f.chunk.js
        monitoring/static/js/3883.fdb7b91a.chunk.js monitoring/static/js/3883.fdb7b91a.chunk.js
        monitoring/static/js/3933.1914913e.chunk.js monitoring/static/js/3933.1914913e.chunk.js
        monitoring/static/js/3950.e7782b0d.chunk.js monitoring/static/js/3950.e7782b0d.chunk.js
        monitoring/static/js/3972.a75c55f6.chunk.js monitoring/static/js/3972.a75c55f6.chunk.js
        monitoring/static/js/3972.a75c55f6.chunk.js.LICENSE.txt monitoring/static/js/3972.a75c55f6.chunk.js.LICENSE.txt
        monitoring/static/js/3997.49583a3c.chunk.js monitoring/static/js/3997.49583a3c.chunk.js
        monitoring/static/js/3998.22c04eb2.chunk.js monitoring/static/js/3998.22c04eb2.chunk.js
        monitoring/static/js/4006.14a73158.chunk.js monitoring/static/js/4006.14a73158.chunk.js
        monitoring/static/js/4018.1e215668.chunk.js monitoring/static/js/4018.1e215668.chunk.js
        monitoring/static/js/4023.da1a6eeb.chunk.js monitoring/static/js/4023.da1a6eeb.chunk.js
        monitoring/static/js/4040.5ec4aca9.chunk.js monitoring/static/js/4040.5ec4aca9.chunk.js
        monitoring/static/js/4066.21a0994b.chunk.js monitoring/static/js/4066.21a0994b.chunk.js
        monitoring/static/js/4066.21a0994b.chunk.js.LICENSE.txt monitoring/static/js/4066.21a0994b.chunk.js.LICENSE.txt
        monitoring/static/js/4087.76cc8418.chunk.js monitoring/static/js/4087.76cc8418.chunk.js
        monitoring/static/js/4087.76cc8418.chunk.js.LICENSE.txt monitoring/static/js/4087.76cc8418.chunk.js.LICENSE.txt
        monitoring/static/js/4099.049bf0b8.chunk.js monitoring/static/js/4099.049bf0b8.chunk.js
        monitoring/static/js/4099.049bf0b8.chunk.js.LICENSE.txt monitoring/static/js/4099.049bf0b8.chunk.js.LICENSE.txt
        monitoring/static/js/4135.066e893b.chunk.js monitoring/static/js/4135.066e893b.chunk.js
        monitoring/static/js/4175.84846314.chunk.js monitoring/static/js/4175.84846314.chunk.js
        monitoring/static/js/4222.a626a694.chunk.js monitoring/static/js/4222.a626a694.chunk.js
        monitoring/static/js/4222.a626a694.chunk.js.LICENSE.txt monitoring/static/js/4222.a626a694.chunk.js.LICENSE.txt
        monitoring/static/js/4226.b1a9f9a0.chunk.js monitoring/static/js/4226.b1a9f9a0.chunk.js
        monitoring/static/js/4231.75694c42.chunk.js monitoring/static/js/4231.75694c42.chunk.js
        monitoring/static/js/4231.75694c42.chunk.js.LICENSE.txt monitoring/static/js/4231.75694c42.chunk.js.LICENSE.txt
        monitoring/static/js/4254.b0fb883a.chunk.js monitoring/static/js/4254.b0fb883a.chunk.js
        monitoring/static/js/4320.2cdac293.chunk.js monitoring/static/js/4320.2cdac293.chunk.js
        monitoring/static/js/4324.9835557b.chunk.js monitoring/static/js/4324.9835557b.chunk.js
        monitoring/static/js/433.81528b91.chunk.js monitoring/static/js/433.81528b91.chunk.js
        monitoring/static/js/433.81528b91.chunk.js.LICENSE.txt monitoring/static/js/433.81528b91.chunk.js.LICENSE.txt
        monitoring/static/js/4408.a9538443.chunk.js monitoring/static/js/4408.a9538443.chunk.js
        monitoring/static/js/4413.5197a5f2.chunk.js monitoring/static/js/4413.5197a5f2.chunk.js
        monitoring/static/js/4442.d3e799fe.chunk.js monitoring/static/js/4442.d3e799fe.chunk.js
        monitoring/static/js/4463.8556c625.chunk.js monitoring/static/js/4463.8556c625.chunk.js
        monitoring/static/js/4465.383f9181.chunk.js monitoring/static/js/4465.383f9181.chunk.js
        monitoring/static/js/4465.383f9181.chunk.js.LICENSE.txt monitoring/static/js/4465.383f9181.chunk.js.LICENSE.txt
        monitoring/static/js/4503.e2e4d1e6.chunk.js monitoring/static/js/4503.e2e4d1e6.chunk.js
        monitoring/static/js/452.04a5841e.chunk.js monitoring/static/js/452.04a5841e.chunk.js
        monitoring/static/js/453.2308e7f9.chunk.js monitoring/static/js/453.2308e7f9.chunk.js
        monitoring/static/js/453.2308e7f9.chunk.js.LICENSE.txt monitoring/static/js/453.2308e7f9.chunk.js.LICENSE.txt
        monitoring/static/js/4535.d2047797.chunk.js monitoring/static/js/4535.d2047797.chunk.js
        monitoring/static/js/4535.d2047797.chunk.js.LICENSE.txt monitoring/static/js/4535.d2047797.chunk.js.LICENSE.txt
        monitoring/static/js/4563.c37ee423.chunk.js monitoring/static/js/4563.c37ee423.chunk.js
        monitoring/static/js/4609.4537e278.chunk.js monitoring/static/js/4609.4537e278.chunk.js
        monitoring/static/js/4609.4537e278.chunk.js.LICENSE.txt monitoring/static/js/4609.4537e278.chunk.js.LICENSE.txt
        monitoring/static/js/4628.694b1225.chunk.js monitoring/static/js/4628.694b1225.chunk.js
        monitoring/static/js/4628.694b1225.chunk.js.LICENSE.txt monitoring/static/js/4628.694b1225.chunk.js.LICENSE.txt
        monitoring/static/js/4639.06a52d60.chunk.js monitoring/static/js/4639.06a52d60.chunk.js
        monitoring/static/js/4657.375c590c.chunk.js monitoring/static/js/4657.375c590c.chunk.js
        monitoring/static/js/4712.0fccf2f7.chunk.js monitoring/static/js/4712.0fccf2f7.chunk.js
        monitoring/static/js/4723.61d3b0c8.chunk.js monitoring/static/js/4723.61d3b0c8.chunk.js
        monitoring/static/js/4779.5a27c84f.chunk.js monitoring/static/js/4779.5a27c84f.chunk.js
        monitoring/static/js/4806.7aabd034.chunk.js monitoring/static/js/4806.7aabd034.chunk.js
        monitoring/static/js/4859.99586a72.chunk.js monitoring/static/js/4859.99586a72.chunk.js
        monitoring/static/js/4859.99586a72.chunk.js.LICENSE.txt monitoring/static/js/4859.99586a72.chunk.js.LICENSE.txt
        monitoring/static/js/5014.b76ade01.chunk.js monitoring/static/js/5014.b76ade01.chunk.js
        monitoring/static/js/5066.91e2d7b8.chunk.js monitoring/static/js/5066.91e2d7b8.chunk.js
        monitoring/static/js/5070.7db921b1.chunk.js monitoring/static/js/5070.7db921b1.chunk.js
        monitoring/static/js/5124.e2e4a17c.chunk.js monitoring/static/js/5124.e2e4a17c.chunk.js
        monitoring/static/js/5124.e2e4a17c.chunk.js.LICENSE.txt monitoring/static/js/5124.e2e4a17c.chunk.js.LICENSE.txt
        monitoring/static/js/5130.8076069a.chunk.js monitoring/static/js/5130.8076069a.chunk.js
        monitoring/static/js/5154.74a87136.chunk.js monitoring/static/js/5154.74a87136.chunk.js
        monitoring/static/js/5154.74a87136.chunk.js.LICENSE.txt monitoring/static/js/5154.74a87136.chunk.js.LICENSE.txt
        monitoring/static/js/5160.f92a5ad6.chunk.js monitoring/static/js/5160.f92a5ad6.chunk.js
        monitoring/static/js/5160.f92a5ad6.chunk.js.LICENSE.txt monitoring/static/js/5160.f92a5ad6.chunk.js.LICENSE.txt
        monitoring/static/js/5203.922e0927.chunk.js monitoring/static/js/5203.922e0927.chunk.js
        monitoring/static/js/521.dba82744.chunk.js monitoring/static/js/521.dba82744.chunk.js
        monitoring/static/js/521.dba82744.chunk.js.LICENSE.txt monitoring/static/js/521.dba82744.chunk.js.LICENSE.txt
        monitoring/static/js/5257.5bc0f543.chunk.js monitoring/static/js/5257.5bc0f543.chunk.js
        monitoring/static/js/5319.3a9cc4fa.chunk.js monitoring/static/js/5319.3a9cc4fa.chunk.js
        monitoring/static/js/5319.3a9cc4fa.chunk.js.LICENSE.txt monitoring/static/js/5319.3a9cc4fa.chunk.js.LICENSE.txt
        monitoring/static/js/5467.289ca54e.chunk.js monitoring/static/js/5467.289ca54e.chunk.js
        monitoring/static/js/5467.289ca54e.chunk.js.LICENSE.txt monitoring/static/js/5467.289ca54e.chunk.js.LICENSE.txt
        monitoring/static/js/5484.4ee82172.chunk.js monitoring/static/js/5484.4ee82172.chunk.js
        monitoring/static/js/5484.4ee82172.chunk.js.LICENSE.txt monitoring/static/js/5484.4ee82172.chunk.js.LICENSE.txt
        monitoring/static/js/5503.3a78f1a9.chunk.js monitoring/static/js/5503.3a78f1a9.chunk.js
        monitoring/static/js/5605.6b78bfb4.chunk.js monitoring/static/js/5605.6b78bfb4.chunk.js
        monitoring/static/js/5605.6b78bfb4.chunk.js.LICENSE.txt monitoring/static/js/5605.6b78bfb4.chunk.js.LICENSE.txt
        monitoring/static/js/5641.f0e59fe6.chunk.js monitoring/static/js/5641.f0e59fe6.chunk.js
        monitoring/static/js/5682.853e9ee8.chunk.js monitoring/static/js/5682.853e9ee8.chunk.js
        monitoring/static/js/5682.853e9ee8.chunk.js.LICENSE.txt monitoring/static/js/5682.853e9ee8.chunk.js.LICENSE.txt
        monitoring/static/js/5685.11cd560a.chunk.js monitoring/static/js/5685.11cd560a.chunk.js
        monitoring/static/js/5685.11cd560a.chunk.js.LICENSE.txt monitoring/static/js/5685.11cd560a.chunk.js.LICENSE.txt
        monitoring/static/js/5748.47ca831b.chunk.js monitoring/static/js/5748.47ca831b.chunk.js
        monitoring/static/js/579.70d17f7b.chunk.js monitoring/static/js/579.70d17f7b.chunk.js
        monitoring/static/js/5821.26335bd8.chunk.js monitoring/static/js/5821.26335bd8.chunk.js
        monitoring/static/js/5879.abdba9dc.chunk.js monitoring/static/js/5879.abdba9dc.chunk.js
        monitoring/static/js/592.33a5d27a.chunk.js monitoring/static/js/592.33a5d27a.chunk.js
        monitoring/static/js/5957.1584eca6.chunk.js monitoring/static/js/5957.1584eca6.chunk.js
        monitoring/static/js/5982.cae758ee.chunk.js monitoring/static/js/5982.cae758ee.chunk.js
        monitoring/static/js/5986.f8e39c8a.chunk.js monitoring/static/js/5986.f8e39c8a.chunk.js
        monitoring/static/js/6010.c0760b69.chunk.js monitoring/static/js/6010.c0760b69.chunk.js
        monitoring/static/js/6010.c0760b69.chunk.js.LICENSE.txt monitoring/static/js/6010.c0760b69.chunk.js.LICENSE.txt
        monitoring/static/js/6062.2c4512c7.chunk.js monitoring/static/js/6062.2c4512c7.chunk.js
        monitoring/static/js/6062.2c4512c7.chunk.js.LICENSE.txt monitoring/static/js/6062.2c4512c7.chunk.js.LICENSE.txt
        monitoring/static/js/6070.b32e6682.chunk.js monitoring/static/js/6070.b32e6682.chunk.js
        monitoring/static/js/6079.340afa32.chunk.js monitoring/static/js/6079.340afa32.chunk.js
        monitoring/static/js/610.f75b0536.chunk.js monitoring/static/js/610.f75b0536.chunk.js
        monitoring/static/js/6118.062d9ccd.chunk.js monitoring/static/js/6118.062d9ccd.chunk.js
        monitoring/static/js/6118.062d9ccd.chunk.js.LICENSE.txt monitoring/static/js/6118.062d9ccd.chunk.js.LICENSE.txt
        monitoring/static/js/6170.8626a56e.chunk.js monitoring/static/js/6170.8626a56e.chunk.js
        monitoring/static/js/6246.0425b742.chunk.js monitoring/static/js/6246.0425b742.chunk.js
        monitoring/static/js/6246.0425b742.chunk.js.LICENSE.txt monitoring/static/js/6246.0425b742.chunk.js.LICENSE.txt
        monitoring/static/js/6289.d27d9a38.chunk.js monitoring/static/js/6289.d27d9a38.chunk.js
        monitoring/static/js/6289.d27d9a38.chunk.js.LICENSE.txt monitoring/static/js/6289.d27d9a38.chunk.js.LICENSE.txt
        monitoring/static/js/6292.e0b2ffee.chunk.js monitoring/static/js/6292.e0b2ffee.chunk.js
        monitoring/static/js/633.e316425c.chunk.js monitoring/static/js/633.e316425c.chunk.js
        monitoring/static/js/6332.d1a078f2.chunk.js monitoring/static/js/6332.d1a078f2.chunk.js
        monitoring/static/js/6395.c9121a28.chunk.js monitoring/static/js/6395.c9121a28.chunk.js
        monitoring/static/js/6435.70056056.chunk.js monitoring/static/js/6435.70056056.chunk.js
        monitoring/static/js/6435.70056056.chunk.js.LICENSE.txt monitoring/static/js/6435.70056056.chunk.js.LICENSE.txt
        monitoring/static/js/6595.bc7ea867.chunk.js monitoring/static/js/6595.bc7ea867.chunk.js
        monitoring/static/js/6625.f5590154.chunk.js monitoring/static/js/6625.f5590154.chunk.js
        monitoring/static/js/6659.cacbbf83.chunk.js monitoring/static/js/6659.cacbbf83.chunk.js
        monitoring/static/js/6659.cacbbf83.chunk.js.LICENSE.txt monitoring/static/js/6659.cacbbf83.chunk.js.LICENSE.txt
        monitoring/static/js/6660.fd286d4f.chunk.js monitoring/static/js/6660.fd286d4f.chunk.js
        monitoring/static/js/6663.debc2102.chunk.js monitoring/static/js/6663.debc2102.chunk.js
        monitoring/static/js/6679.fb8e5993.chunk.js monitoring/static/js/6679.fb8e5993.chunk.js
        monitoring/static/js/6698.31f9be10.chunk.js monitoring/static/js/6698.31f9be10.chunk.js
        monitoring/static/js/6698.31f9be10.chunk.js.LICENSE.txt monitoring/static/js/6698.31f9be10.chunk.js.LICENSE.txt
        monitoring/static/js/6731.3a45d4e4.chunk.js monitoring/static/js/6731.3a45d4e4.chunk.js
        monitoring/static/js/6789.2651b83a.chunk.js monitoring/static/js/6789.2651b83a.chunk.js
        monitoring/static/js/682.bf02377d.chunk.js monitoring/static/js/682.bf02377d.chunk.js
        monitoring/static/js/682.bf02377d.chunk.js.LICENSE.txt monitoring/static/js/682.bf02377d.chunk.js.LICENSE.txt
        monitoring/static/js/6845.d02f4e48.chunk.js monitoring/static/js/6845.d02f4e48.chunk.js
        monitoring/static/js/6866.de45efb9.chunk.js monitoring/static/js/6866.de45efb9.chunk.js
        monitoring/static/js/689.96ae92f1.chunk.js monitoring/static/js/689.96ae92f1.chunk.js
        monitoring/static/js/6894.46e65325.chunk.js monitoring/static/js/6894.46e65325.chunk.js
        monitoring/static/js/6898.5ae6e6cc.chunk.js monitoring/static/js/6898.5ae6e6cc.chunk.js
        monitoring/static/js/6898.5ae6e6cc.chunk.js.LICENSE.txt monitoring/static/js/6898.5ae6e6cc.chunk.js.LICENSE.txt
        monitoring/static/js/6914.3401c32a.chunk.js monitoring/static/js/6914.3401c32a.chunk.js
        monitoring/static/js/6914.3401c32a.chunk.js.LICENSE.txt monitoring/static/js/6914.3401c32a.chunk.js.LICENSE.txt
        monitoring/static/js/6927.a118705d.chunk.js monitoring/static/js/6927.a118705d.chunk.js
        monitoring/static/js/6943.27a31755.chunk.js monitoring/static/js/6943.27a31755.chunk.js
        monitoring/static/js/6953.3813672a.chunk.js monitoring/static/js/6953.3813672a.chunk.js
        monitoring/static/js/6953.3813672a.chunk.js.LICENSE.txt monitoring/static/js/6953.3813672a.chunk.js.LICENSE.txt
        monitoring/static/js/7043.adbaef91.chunk.js monitoring/static/js/7043.adbaef91.chunk.js
        monitoring/static/js/7043.adbaef91.chunk.js.LICENSE.txt monitoring/static/js/7043.adbaef91.chunk.js.LICENSE.txt
        monitoring/static/js/7054.356e5176.chunk.js monitoring/static/js/7054.356e5176.chunk.js
        monitoring/static/js/7076.11720320.chunk.js monitoring/static/js/7076.11720320.chunk.js
        monitoring/static/js/7083.b229fb1f.chunk.js monitoring/static/js/7083.b229fb1f.chunk.js
        monitoring/static/js/7083.b229fb1f.chunk.js.LICENSE.txt monitoring/static/js/7083.b229fb1f.chunk.js.LICENSE.txt
        monitoring/static/js/7132.d2e7640b.chunk.js monitoring/static/js/7132.d2e7640b.chunk.js
        monitoring/static/js/7199.5dcadd74.chunk.js monitoring/static/js/7199.5dcadd74.chunk.js
        monitoring/static/js/7245.cde86cfb.chunk.js monitoring/static/js/7245.cde86cfb.chunk.js
        monitoring/static/js/7273.b27ab405.chunk.js monitoring/static/js/7273.b27ab405.chunk.js
        monitoring/static/js/7275.ee76d1bd.chunk.js monitoring/static/js/7275.ee76d1bd.chunk.js
        monitoring/static/js/7289.d83d9214.chunk.js monitoring/static/js/7289.d83d9214.chunk.js
        monitoring/static/js/7289.d83d9214.chunk.js.LICENSE.txt monitoring/static/js/7289.d83d9214.chunk.js.LICENSE.txt
        monitoring/static/js/7324.d07154de.chunk.js monitoring/static/js/7324.d07154de.chunk.js
        monitoring/static/js/7367.090c8c59.chunk.js monitoring/static/js/7367.090c8c59.chunk.js
        monitoring/static/js/7439.1c298639.chunk.js monitoring/static/js/7439.1c298639.chunk.js
        monitoring/static/js/7441.71fcae9c.chunk.js monitoring/static/js/7441.71fcae9c.chunk.js
        monitoring/static/js/7441.71fcae9c.chunk.js.LICENSE.txt monitoring/static/js/7441.71fcae9c.chunk.js.LICENSE.txt
        monitoring/static/js/7443.79bf7ff1.chunk.js monitoring/static/js/7443.79bf7ff1.chunk.js
        monitoring/static/js/7446.a1010c10.chunk.js monitoring/static/js/7446.a1010c10.chunk.js
        monitoring/static/js/753.86744d44.chunk.js monitoring/static/js/753.86744d44.chunk.js
        monitoring/static/js/7628.839116b6.chunk.js monitoring/static/js/7628.839116b6.chunk.js
        monitoring/static/js/7628.839116b6.chunk.js.LICENSE.txt monitoring/static/js/7628.839116b6.chunk.js.LICENSE.txt
        monitoring/static/js/7656.6c9df81e.chunk.js monitoring/static/js/7656.6c9df81e.chunk.js
        monitoring/static/js/766.1355c78d.chunk.js monitoring/static/js/766.1355c78d.chunk.js
        monitoring/static/js/766.1355c78d.chunk.js.LICENSE.txt monitoring/static/js/766.1355c78d.chunk.js.LICENSE.txt
        monitoring/static/js/7690.c75c8231.chunk.js monitoring/static/js/7690.c75c8231.chunk.js
        monitoring/static/js/7690.c75c8231.chunk.js.LICENSE.txt monitoring/static/js/7690.c75c8231.chunk.js.LICENSE.txt
        monitoring/static/js/778.5edf04d9.chunk.js monitoring/static/js/778.5edf04d9.chunk.js
        monitoring/static/js/778.5edf04d9.chunk.js.LICENSE.txt monitoring/static/js/778.5edf04d9.chunk.js.LICENSE.txt
        monitoring/static/js/7812.f9c2eea9.chunk.js monitoring/static/js/7812.f9c2eea9.chunk.js
        monitoring/static/js/7828.4945daef.chunk.js monitoring/static/js/7828.4945daef.chunk.js
        monitoring/static/js/7828.4945daef.chunk.js.LICENSE.txt monitoring/static/js/7828.4945daef.chunk.js.LICENSE.txt
        monitoring/static/js/7852.96468333.chunk.js monitoring/static/js/7852.96468333.chunk.js
        monitoring/static/js/7852.96468333.chunk.js.LICENSE.txt monitoring/static/js/7852.96468333.chunk.js.LICENSE.txt
        monitoring/static/js/7862.e14664b0.chunk.js monitoring/static/js/7862.e14664b0.chunk.js
        monitoring/static/js/7950.8f716467.chunk.js monitoring/static/js/7950.8f716467.chunk.js
        monitoring/static/js/7994.b03482fd.chunk.js monitoring/static/js/7994.b03482fd.chunk.js
        monitoring/static/js/8122.e7079404.chunk.js monitoring/static/js/8122.e7079404.chunk.js
        monitoring/static/js/8122.e7079404.chunk.js.LICENSE.txt monitoring/static/js/8122.e7079404.chunk.js.LICENSE.txt
        monitoring/static/js/8169.79ab989b.chunk.js monitoring/static/js/8169.79ab989b.chunk.js
        monitoring/static/js/824.c19b1bc3.chunk.js monitoring/static/js/824.c19b1bc3.chunk.js
        monitoring/static/js/8297.dda322b6.chunk.js monitoring/static/js/8297.dda322b6.chunk.js
        monitoring/static/js/8297.dda322b6.chunk.js.LICENSE.txt monitoring/static/js/8297.dda322b6.chunk.js.LICENSE.txt
        monitoring/static/js/8329.074348a0.chunk.js monitoring/static/js/8329.074348a0.chunk.js
        monitoring/static/js/8329.074348a0.chunk.js.LICENSE.txt monitoring/static/js/8329.074348a0.chunk.js.LICENSE.txt
        monitoring/static/js/8332.d2782086.chunk.js monitoring/static/js/8332.d2782086.chunk.js
        monitoring/static/js/835.95ce5d5d.chunk.js monitoring/static/js/835.95ce5d5d.chunk.js
        monitoring/static/js/835.95ce5d5d.chunk.js.LICENSE.txt monitoring/static/js/835.95ce5d5d.chunk.js.LICENSE.txt
        monitoring/static/js/8427.caeabc21.chunk.js monitoring/static/js/8427.caeabc21.chunk.js
        monitoring/static/js/8427.caeabc21.chunk.js.LICENSE.txt monitoring/static/js/8427.caeabc21.chunk.js.LICENSE.txt
        monitoring/static/js/8446.394ec1f3.chunk.js monitoring/static/js/8446.394ec1f3.chunk.js
        monitoring/static/js/846.84557d12.chunk.js monitoring/static/js/846.84557d12.chunk.js
        monitoring/static/js/8504.a44b4097.chunk.js monitoring/static/js/8504.a44b4097.chunk.js
        monitoring/static/js/8504.a44b4097.chunk.js.LICENSE.txt monitoring/static/js/8504.a44b4097.chunk.js.LICENSE.txt
        monitoring/static/js/8505.d87e41c1.chunk.js monitoring/static/js/8505.d87e41c1.chunk.js
        monitoring/static/js/856.fd02cec2.chunk.js monitoring/static/js/856.fd02cec2.chunk.js
        monitoring/static/js/8606.75e4ff18.chunk.js monitoring/static/js/8606.75e4ff18.chunk.js
        monitoring/static/js/8606.75e4ff18.chunk.js.LICENSE.txt monitoring/static/js/8606.75e4ff18.chunk.js.LICENSE.txt
        monitoring/static/js/862.05757f0e.chunk.js monitoring/static/js/862.05757f0e.chunk.js
        monitoring/static/js/8645.597fca49.chunk.js monitoring/static/js/8645.597fca49.chunk.js
        monitoring/static/js/8726.fe404098.chunk.js monitoring/static/js/8726.fe404098.chunk.js
        monitoring/static/js/8731.50b8e539.chunk.js monitoring/static/js/8731.50b8e539.chunk.js
        monitoring/static/js/8828.9da287ba.chunk.js monitoring/static/js/8828.9da287ba.chunk.js
        monitoring/static/js/8835.8efe953e.chunk.js monitoring/static/js/8835.8efe953e.chunk.js
        monitoring/static/js/8835.8efe953e.chunk.js.LICENSE.txt monitoring/static/js/8835.8efe953e.chunk.js.LICENSE.txt
        monitoring/static/js/8841.4faa97c2.chunk.js monitoring/static/js/8841.4faa97c2.chunk.js
        monitoring/static/js/8864.ed883bcc.chunk.js monitoring/static/js/8864.ed883bcc.chunk.js
        monitoring/static/js/8908.92f2bf17.chunk.js monitoring/static/js/8908.92f2bf17.chunk.js
        monitoring/static/js/8908.92f2bf17.chunk.js.LICENSE.txt monitoring/static/js/8908.92f2bf17.chunk.js.LICENSE.txt
        monitoring/static/js/896.178c5b16.chunk.js monitoring/static/js/896.178c5b16.chunk.js
        monitoring/static/js/9008.edcbe0c3.chunk.js monitoring/static/js/9008.edcbe0c3.chunk.js
        monitoring/static/js/9101.45d32e67.chunk.js monitoring/static/js/9101.45d32e67.chunk.js
        monitoring/static/js/9139.488e8581.chunk.js monitoring/static/js/9139.488e8581.chunk.js
        monitoring/static/js/9350.ecc944e8.chunk.js monitoring/static/js/9350.ecc944e8.chunk.js
        monitoring/static/js/9352.156d0fc5.chunk.js monitoring/static/js/9352.156d0fc5.chunk.js
        monitoring/static/js/9396.bc133817.chunk.js monitoring/static/js/9396.bc133817.chunk.js
        monitoring/static/js/9396.bc133817.chunk.js.LICENSE.txt monitoring/static/js/9396.bc133817.chunk.js.LICENSE.txt
        monitoring/static/js/9433.2ff2430b.chunk.js monitoring/static/js/9433.2ff2430b.chunk.js
        monitoring/static/js/9530.f9b76944.chunk.js monitoring/static/js/9530.f9b76944.chunk.js
        monitoring/static/js/9530.f9b76944.chunk.js.LICENSE.txt monitoring/static/js/9530.f9b76944.chunk.js.LICENSE.txt
        monitoring/static/js/9617.96c8507d.chunk.js monitoring/static/js/9617.96c8507d.chunk.js
        monitoring/static/js/9625.9143bdfa.chunk.js monitoring/static/js/9625.9143bdfa.chunk.js
        monitoring/static/js/9685.6eae880e.chunk.js monitoring/static/js/9685.6eae880e.chunk.js
        monitoring/static/js/9687.f3eeaa60.chunk.js monitoring/static/js/9687.f3eeaa60.chunk.js
        monitoring/static/js/9687.f3eeaa60.chunk.js.LICENSE.txt monitoring/static/js/9687.f3eeaa60.chunk.js.LICENSE.txt
        monitoring/static/js/9703.ba37cf9a.chunk.js monitoring/static/js/9703.ba37cf9a.chunk.js
        monitoring/static/js/9711.238b6021.chunk.js monitoring/static/js/9711.238b6021.chunk.js
        monitoring/static/js/9757.3b30a50f.chunk.js monitoring/static/js/9757.3b30a50f.chunk.js
        monitoring/static/js/9765.40c53742.chunk.js monitoring/static/js/9765.40c53742.chunk.js
        monitoring/static/js/9776.4f121296.chunk.js monitoring/static/js/9776.4f121296.chunk.js
        monitoring/static/js/9776.4f121296.chunk.js.LICENSE.txt monitoring/static/js/9776.4f121296.chunk.js.LICENSE.txt
        monitoring/static/js/9811.048274db.chunk.js monitoring/static/js/9811.048274db.chunk.js
        monitoring/static/js/9811.048274db.chunk.js.LICENSE.txt monitoring/static/js/9811.048274db.chunk.js.LICENSE.txt
        monitoring/static/js/9821.35ddadcd.chunk.js monitoring/static/js/9821.35ddadcd.chunk.js
        monitoring/static/js/9937.5bd86e47.chunk.js monitoring/static/js/9937.5bd86e47.chunk.js
        monitoring/static/js/9963.a094a76b.chunk.js monitoring/static/js/9963.a094a76b.chunk.js
        monitoring/static/js/9974.cbfc7c5f.chunk.js monitoring/static/js/9974.cbfc7c5f.chunk.js
        monitoring/static/js/main.29d0e3af.js monitoring/static/js/main.29d0e3af.js
        monitoring/static/js/main.29d0e3af.js.LICENSE.txt monitoring/static/js/main.29d0e3af.js.LICENSE.txt
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
    ydb/public/lib/ydb_cli/common
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types
    contrib/libs/yaml-cpp
)

YQL_LAST_ABI_VERSION()

END()
