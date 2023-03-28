RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    browse_db.h
    browse_pq.h
    browse.h
    counters_hosts.h
    json_acl.h
    json_blobindexstat.h
    json_browse.h
    json_bscontrollerinfo.h
    json_bsgroupinfo.h
    json_cluster.h
    json_compute.h
    json_config.h
    json_content.h
    json_counters.h
    json_describe.h
    json_local_rpc.h
    json_getblob.h
    json_handlers_vdisk.cpp
    json_handlers_viewer.cpp
    json_healthcheck.h
    json_hiveinfo.h
    json_hotkeys.h
    json_labeledcounters.h
    json_metainfo.h
    json_netinfo.h
    json_nodeinfo.h
    json_nodelist.h
    json_nodes.h
    json_pdiskinfo.h
    json_query.h
    json_storage.h
    json_sysinfo.h
    json_tabletcounters.h
    json_tabletinfo.h
    json_tenants.h
    json_tenantinfo.h
    json_topicinfo.h
    json_pqconsumerinfo.h
    json_vdisk_req.h
    json_vdiskinfo.h
    json_vdiskstat.h
    json_wb_req.h
    json_whoami.h
    log.h
    viewer_request.cpp
    viewer_request.h
    viewer.cpp
    viewer.h
    wb_aggregate.cpp
    wb_aggregate.h
    wb_filter.cpp
    wb_filter.h
    wb_group.h
    wb_merge.cpp
    wb_merge.h
)

IF (NOT EXPORT_CMAKE)
    RESOURCE(
        monitoring/index.html monitoring/index.html
        monitoring/static/css/main.c44346f9.css monitoring/static/css/main.c44346f9.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1058.3df06184.chunk.js monitoring/static/js/1058.3df06184.chunk.js
        monitoring/static/js/1115.1e053b1d.chunk.js monitoring/static/js/1115.1e053b1d.chunk.js
        monitoring/static/js/1234.165715d4.chunk.js monitoring/static/js/1234.165715d4.chunk.js
        monitoring/static/js/1474.80932b06.chunk.js monitoring/static/js/1474.80932b06.chunk.js
        monitoring/static/js/1522.5645047d.chunk.js monitoring/static/js/1522.5645047d.chunk.js
        monitoring/static/js/1602.86ed3169.chunk.js monitoring/static/js/1602.86ed3169.chunk.js
        monitoring/static/js/1898.a0e4bd43.chunk.js monitoring/static/js/1898.a0e4bd43.chunk.js
        monitoring/static/js/2056.b607e590.chunk.js monitoring/static/js/2056.b607e590.chunk.js
        monitoring/static/js/2081.bd41025d.chunk.js monitoring/static/js/2081.bd41025d.chunk.js
        monitoring/static/js/2119.9f7a5c06.chunk.js monitoring/static/js/2119.9f7a5c06.chunk.js
        monitoring/static/js/2174.264d1736.chunk.js monitoring/static/js/2174.264d1736.chunk.js
        monitoring/static/js/2205.e7f0b9ab.chunk.js monitoring/static/js/2205.e7f0b9ab.chunk.js
        monitoring/static/js/2291.d410fd68.chunk.js monitoring/static/js/2291.d410fd68.chunk.js
        monitoring/static/js/2406.180cb966.chunk.js monitoring/static/js/2406.180cb966.chunk.js
        monitoring/static/js/245.6db2db52.chunk.js monitoring/static/js/245.6db2db52.chunk.js
        monitoring/static/js/248.736ab237.chunk.js monitoring/static/js/248.736ab237.chunk.js
        monitoring/static/js/2507.2d9c4b5c.chunk.js monitoring/static/js/2507.2d9c4b5c.chunk.js
        monitoring/static/js/254.a91c0bf4.chunk.js monitoring/static/js/254.a91c0bf4.chunk.js
        monitoring/static/js/2799.64ec0194.chunk.js monitoring/static/js/2799.64ec0194.chunk.js
        monitoring/static/js/2862.29a56bf7.chunk.js monitoring/static/js/2862.29a56bf7.chunk.js
        monitoring/static/js/2991.0db887ba.chunk.js monitoring/static/js/2991.0db887ba.chunk.js
        monitoring/static/js/3001.b1a75b50.chunk.js monitoring/static/js/3001.b1a75b50.chunk.js
        monitoring/static/js/3191.c6dbae35.chunk.js monitoring/static/js/3191.c6dbae35.chunk.js
        monitoring/static/js/3254.78ce4d35.chunk.js monitoring/static/js/3254.78ce4d35.chunk.js
        monitoring/static/js/3258.248867bd.chunk.js monitoring/static/js/3258.248867bd.chunk.js
        monitoring/static/js/3451.774580b7.chunk.js monitoring/static/js/3451.774580b7.chunk.js
        monitoring/static/js/371.93a1186d.chunk.js monitoring/static/js/371.93a1186d.chunk.js
        monitoring/static/js/3771.0e0bb0f3.chunk.js monitoring/static/js/3771.0e0bb0f3.chunk.js
        monitoring/static/js/3883.62a3dee4.chunk.js monitoring/static/js/3883.62a3dee4.chunk.js
        monitoring/static/js/4529.eb0068c3.chunk.js monitoring/static/js/4529.eb0068c3.chunk.js
        monitoring/static/js/4546.6820709e.chunk.js monitoring/static/js/4546.6820709e.chunk.js
        monitoring/static/js/4663.cc239299.chunk.js monitoring/static/js/4663.cc239299.chunk.js
        monitoring/static/js/4712.4e557974.chunk.js monitoring/static/js/4712.4e557974.chunk.js
        monitoring/static/js/4731.6929d6df.chunk.js monitoring/static/js/4731.6929d6df.chunk.js
        monitoring/static/js/4952.58b99bba.chunk.js monitoring/static/js/4952.58b99bba.chunk.js
        monitoring/static/js/5012.3afcd232.chunk.js monitoring/static/js/5012.3afcd232.chunk.js
        monitoring/static/js/5210.6e07cb51.chunk.js monitoring/static/js/5210.6e07cb51.chunk.js
        monitoring/static/js/5215.2d9f2122.chunk.js monitoring/static/js/5215.2d9f2122.chunk.js
        monitoring/static/js/5444.f86e47ff.chunk.js monitoring/static/js/5444.f86e47ff.chunk.js
        monitoring/static/js/5517.a1034916.chunk.js monitoring/static/js/5517.a1034916.chunk.js
        monitoring/static/js/5646.ced0e1ae.chunk.js monitoring/static/js/5646.ced0e1ae.chunk.js
        monitoring/static/js/569.abbf95fd.chunk.js monitoring/static/js/569.abbf95fd.chunk.js
        monitoring/static/js/5695.d81b70ca.chunk.js monitoring/static/js/5695.d81b70ca.chunk.js
        monitoring/static/js/5748.fa2a8e02.chunk.js monitoring/static/js/5748.fa2a8e02.chunk.js
        monitoring/static/js/5784.26f46213.chunk.js monitoring/static/js/5784.26f46213.chunk.js
        monitoring/static/js/5885.c5ee8345.chunk.js monitoring/static/js/5885.c5ee8345.chunk.js
        monitoring/static/js/6060.eb821066.chunk.js monitoring/static/js/6060.eb821066.chunk.js
        monitoring/static/js/6135.e49ec940.chunk.js monitoring/static/js/6135.e49ec940.chunk.js
        monitoring/static/js/6266.db91261c.chunk.js monitoring/static/js/6266.db91261c.chunk.js
        monitoring/static/js/6550.b5e85913.chunk.js monitoring/static/js/6550.b5e85913.chunk.js
        monitoring/static/js/6559.41bbd3a3.chunk.js monitoring/static/js/6559.41bbd3a3.chunk.js
        monitoring/static/js/6708.5cf2a45c.chunk.js monitoring/static/js/6708.5cf2a45c.chunk.js
        monitoring/static/js/6892.3db15360.chunk.js monitoring/static/js/6892.3db15360.chunk.js
        monitoring/static/js/698.746436d5.chunk.js monitoring/static/js/698.746436d5.chunk.js
        monitoring/static/js/7168.ed7798a9.chunk.js monitoring/static/js/7168.ed7798a9.chunk.js
        monitoring/static/js/7478.0bf003df.chunk.js monitoring/static/js/7478.0bf003df.chunk.js
        monitoring/static/js/7548.8ef3bbc0.chunk.js monitoring/static/js/7548.8ef3bbc0.chunk.js
        monitoring/static/js/758.2eb69c4e.chunk.js monitoring/static/js/758.2eb69c4e.chunk.js
        monitoring/static/js/7661.dd104c3c.chunk.js monitoring/static/js/7661.dd104c3c.chunk.js
        monitoring/static/js/7664.9f9f696d.chunk.js monitoring/static/js/7664.9f9f696d.chunk.js
        monitoring/static/js/7768.2adb4751.chunk.js monitoring/static/js/7768.2adb4751.chunk.js
        monitoring/static/js/7953.eb2256ce.chunk.js monitoring/static/js/7953.eb2256ce.chunk.js
        monitoring/static/js/8005.9c209154.chunk.js monitoring/static/js/8005.9c209154.chunk.js
        monitoring/static/js/805.d67f39bf.chunk.js monitoring/static/js/805.d67f39bf.chunk.js
        monitoring/static/js/8206.d2f5a912.chunk.js monitoring/static/js/8206.d2f5a912.chunk.js
        monitoring/static/js/8322.c2b160c6.chunk.js monitoring/static/js/8322.c2b160c6.chunk.js
        monitoring/static/js/8335.9ed734ba.chunk.js monitoring/static/js/8335.9ed734ba.chunk.js
        monitoring/static/js/842.bd21ee9f.chunk.js monitoring/static/js/842.bd21ee9f.chunk.js
        monitoring/static/js/8520.616efa9f.chunk.js monitoring/static/js/8520.616efa9f.chunk.js
        monitoring/static/js/8558.47b3557b.chunk.js monitoring/static/js/8558.47b3557b.chunk.js
        monitoring/static/js/8590.b8446f1d.chunk.js monitoring/static/js/8590.b8446f1d.chunk.js
        monitoring/static/js/8794.5ad5fb7d.chunk.js monitoring/static/js/8794.5ad5fb7d.chunk.js
        monitoring/static/js/9005.053ddf1a.chunk.js monitoring/static/js/9005.053ddf1a.chunk.js
        monitoring/static/js/9011.f3cf1dfe.chunk.js monitoring/static/js/9011.f3cf1dfe.chunk.js
        monitoring/static/js/9163.de992e19.chunk.js monitoring/static/js/9163.de992e19.chunk.js
        monitoring/static/js/9292.3ccb6509.chunk.js monitoring/static/js/9292.3ccb6509.chunk.js
        monitoring/static/js/939.4c5d6b68.chunk.js monitoring/static/js/939.4c5d6b68.chunk.js
        monitoring/static/js/9803.e1567af5.chunk.js monitoring/static/js/9803.e1567af5.chunk.js
        monitoring/static/js/983.18afe3d6.chunk.js monitoring/static/js/983.18afe3d6.chunk.js
        monitoring/static/js/main.45b97d89.js monitoring/static/js/main.45b97d89.js
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.80a4c25b73c1f97077ed.ttf monitoring/static/media/codicon.80a4c25b73c1f97077ed.ttf
        monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg monitoring/static/media/thumbsUp.d4a03fbaa64ce85a0045bf8ba77f8e2b.svg
    )
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
    content/api/css/print.css viewer/api/css/print.css
    content/api/css/reset.css viewer/api/css/reset.css
    content/api/css/screen.css viewer/api/css/screen.css
    content/api/css/style.css viewer/api/css/style.css
    content/api/css/typography.css viewer/api/css/typography.css
    content/api/fonts/DroidSans-Bold.ttf viewer/api/fonts/DroidSans-Bold.ttf
    content/api/fonts/DroidSans.ttf viewer/api/fonts/DroidSans.ttf
    content/api/images/collapse.gif viewer/api/images/collapse.gif
    content/api/images/expand.gif viewer/api/images/expand.gif
    content/api/images/explorer_icons.png viewer/api/images/explorer_icons.png
    content/api/images/favicon-16x16.png viewer/api/images/favicon-16x16.png
    content/api/images/favicon-32x32.png viewer/api/images/favicon-32x32.png
    content/api/images/favicon.ico viewer/api/images/favicon.ico
    content/api/images/logo_small.png viewer/api/images/logo_small.png
    content/api/images/throbber.gif viewer/api/images/throbber.gif
    content/api/index.html viewer/api/index.html
    content/api/lang/ca.js viewer/api/lang/ca.js
    content/api/lang/en.js viewer/api/lang/en.js
    content/api/lang/es.js viewer/api/lang/es.js
    content/api/lang/fr.js viewer/api/lang/fr.js
    content/api/lang/geo.js viewer/api/lang/geo.js
    content/api/lang/it.js viewer/api/lang/it.js
    content/api/lang/ja.js viewer/api/lang/ja.js
    content/api/lang/ko-kr.js viewer/api/lang/ko-kr.js
    content/api/lang/pl.js viewer/api/lang/pl.js
    content/api/lang/pt.js viewer/api/lang/pt.js
    content/api/lang/ru.js viewer/api/lang/ru.js
    content/api/lang/tr.js viewer/api/lang/tr.js
    content/api/lang/translator.js viewer/api/lang/translator.js
    content/api/lang/zh-cn.js viewer/api/lang/zh-cn.js
    content/api/lib/backbone-min.js viewer/api/lib/backbone-min.js
    content/api/lib/es5-shim.js viewer/api/lib/es5-shim.js
    content/api/lib/handlebars-4.0.5.js viewer/api/lib/handlebars-4.0.5.js
    content/api/lib/highlight.9.1.0.pack.js viewer/api/lib/highlight.9.1.0.pack.js
    content/api/lib/highlight.9.1.0.pack_extended.js viewer/api/lib/highlight.9.1.0.pack_extended.js
    content/api/lib/jquery-1.8.0.min.js viewer/api/lib/jquery-1.8.0.min.js
    content/api/lib/jquery.ba-bbq.min.js viewer/api/lib/jquery.ba-bbq.min.js
    content/api/lib/jquery.slideto.min.js viewer/api/lib/jquery.slideto.min.js
    content/api/lib/jquery.wiggle.min.js viewer/api/lib/jquery.wiggle.min.js
    content/api/lib/js-yaml.min.js viewer/api/lib/js-yaml.min.js
    content/api/lib/jsoneditor.min.js viewer/api/lib/jsoneditor.min.js
    content/api/lib/lodash.min.js viewer/api/lib/lodash.min.js
    content/api/lib/marked.js viewer/api/lib/marked.js
    content/api/lib/object-assign-pollyfill.js viewer/api/lib/object-assign-pollyfill.js
    content/api/lib/sanitize-html.min.js viewer/api/lib/sanitize-html.min.js
    content/api/lib/swagger-oauth.js viewer/api/lib/swagger-oauth.js
    content/api/swagger-ui.min.js viewer/api/swagger-ui.min.js
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/archive
    library/cpp/mime/types
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/client/server
    ydb/core/grpc_services
    ydb/core/grpc_services/local_rpc
    ydb/core/health_check
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tx/schemeshard
    ydb/core/tx/tx_proxy
    ydb/core/util
    ydb/core/viewer/json
    ydb/core/viewer/protos
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
    ydb/public/lib/deprecated/kicli
    ydb/public/lib/json_value
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_types
)

YQL_LAST_ABI_VERSION()

END()
