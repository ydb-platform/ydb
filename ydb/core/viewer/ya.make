RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

OWNER(
    xenoxeno
    g:kikimr
)

SRCS(
    browse_db.h
    browse_pq.h
    browse.h
    counters_hosts.h
    json_browse.h
    json_bscontrollerinfo.h
    json_bsgroupinfo.h
    json_cluster.h
    json_compute.h
    json_config.h
    json_content.h
    json_counters.h
    json_describe.h
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
    json_vdiskinfo.h
    json_wb_req.h
    json_whoami.h
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

RESOURCE(
    monitoring/index.html monitoring/index.html
    monitoring/resources/js/49.7d21cb89.chunk.js monitoring/resources/js/49.7d21cb89.chunk.js
    monitoring/resources/js/64.64aa9e00.chunk.js monitoring/resources/js/64.64aa9e00.chunk.js
    monitoring/resources/js/41.cfa9c722.chunk.js monitoring/resources/js/41.cfa9c722.chunk.js
    monitoring/resources/js/44.1fef9069.chunk.js monitoring/resources/js/44.1fef9069.chunk.js
    monitoring/resources/js/10.17fcf787.chunk.js monitoring/resources/js/10.17fcf787.chunk.js
    monitoring/resources/js/55.6a7e59af.chunk.js monitoring/resources/js/55.6a7e59af.chunk.js
    monitoring/resources/js/23.b7508195.chunk.js monitoring/resources/js/23.b7508195.chunk.js
    monitoring/resources/js/31.998f62cd.chunk.js monitoring/resources/js/31.998f62cd.chunk.js
    monitoring/resources/js/51.7ef56f89.chunk.js monitoring/resources/js/51.7ef56f89.chunk.js
    monitoring/resources/js/18.d360d7b2.chunk.js monitoring/resources/js/18.d360d7b2.chunk.js
    monitoring/resources/js/73.9c043171.chunk.js monitoring/resources/js/73.9c043171.chunk.js
    monitoring/resources/js/20.678aeec8.chunk.js monitoring/resources/js/20.678aeec8.chunk.js
    monitoring/resources/js/main.1880b6ce.chunk.js monitoring/resources/js/main.1880b6ce.chunk.js
    monitoring/resources/js/12.d0c7ea2e.chunk.js monitoring/resources/js/12.d0c7ea2e.chunk.js
    monitoring/resources/js/65.9c18b830.chunk.js monitoring/resources/js/65.9c18b830.chunk.js
    monitoring/resources/js/32.6889820a.chunk.js monitoring/resources/js/32.6889820a.chunk.js
    monitoring/resources/js/35.a0d4a9da.chunk.js monitoring/resources/js/35.a0d4a9da.chunk.js
    monitoring/resources/js/7.f0b47ff0.chunk.js monitoring/resources/js/7.f0b47ff0.chunk.js
    monitoring/resources/js/74.f47787b5.chunk.js monitoring/resources/js/74.f47787b5.chunk.js
    monitoring/resources/js/4.6be38077.chunk.js monitoring/resources/js/4.6be38077.chunk.js
    monitoring/resources/js/63.cbf672cd.chunk.js monitoring/resources/js/63.cbf672cd.chunk.js
    monitoring/resources/js/33.34cb2f3d.chunk.js monitoring/resources/js/33.34cb2f3d.chunk.js
    monitoring/resources/js/48.79933547.chunk.js monitoring/resources/js/48.79933547.chunk.js
    monitoring/resources/js/0.649193f3.chunk.js monitoring/resources/js/0.649193f3.chunk.js
    monitoring/resources/js/77.ccd46914.chunk.js monitoring/resources/js/77.ccd46914.chunk.js
    monitoring/resources/js/24.d87c5317.chunk.js monitoring/resources/js/24.d87c5317.chunk.js
    monitoring/resources/js/61.47f2d1b2.chunk.js monitoring/resources/js/61.47f2d1b2.chunk.js
    monitoring/resources/js/5.ef395233.chunk.js monitoring/resources/js/5.ef395233.chunk.js
    monitoring/resources/js/60.95fe2b2c.chunk.js monitoring/resources/js/60.95fe2b2c.chunk.js
    monitoring/resources/js/45.99e4f8f7.chunk.js monitoring/resources/js/45.99e4f8f7.chunk.js
    monitoring/resources/js/53.2b9407ba.chunk.js monitoring/resources/js/53.2b9407ba.chunk.js
    monitoring/resources/js/28.b363b973.chunk.js monitoring/resources/js/28.b363b973.chunk.js
    monitoring/resources/js/26.28b69374.chunk.js monitoring/resources/js/26.28b69374.chunk.js
    monitoring/resources/js/42.2b9399e9.chunk.js monitoring/resources/js/42.2b9399e9.chunk.js
    monitoring/resources/js/37.025f46cc.chunk.js monitoring/resources/js/37.025f46cc.chunk.js
    monitoring/resources/js/66.2832d8f4.chunk.js monitoring/resources/js/66.2832d8f4.chunk.js
    monitoring/resources/js/36.fb4f2b06.chunk.js monitoring/resources/js/36.fb4f2b06.chunk.js
    monitoring/resources/js/38.3e9e52b5.chunk.js monitoring/resources/js/38.3e9e52b5.chunk.js
    monitoring/resources/js/72.04a3b303.chunk.js monitoring/resources/js/72.04a3b303.chunk.js
    monitoring/resources/js/47.87ee2705.chunk.js monitoring/resources/js/47.87ee2705.chunk.js
    monitoring/resources/js/16.be18a0ef.chunk.js monitoring/resources/js/16.be18a0ef.chunk.js
    monitoring/resources/js/29.8b21ad99.chunk.js monitoring/resources/js/29.8b21ad99.chunk.js
    monitoring/resources/js/22.a8201ff0.chunk.js monitoring/resources/js/22.a8201ff0.chunk.js
    monitoring/resources/js/8.aee9c986.chunk.js monitoring/resources/js/8.aee9c986.chunk.js
    monitoring/resources/js/6.d6bed715.chunk.js monitoring/resources/js/6.d6bed715.chunk.js
    monitoring/resources/js/75.93b73743.chunk.js monitoring/resources/js/75.93b73743.chunk.js
    monitoring/resources/js/50.befb2117.chunk.js monitoring/resources/js/50.befb2117.chunk.js
    monitoring/resources/js/11.8301498b.chunk.js monitoring/resources/js/11.8301498b.chunk.js
    monitoring/resources/js/39.2ea99298.chunk.js monitoring/resources/js/39.2ea99298.chunk.js
    monitoring/resources/js/15.9534748b.chunk.js monitoring/resources/js/15.9534748b.chunk.js
    monitoring/resources/js/30.70c81ca7.chunk.js monitoring/resources/js/30.70c81ca7.chunk.js
    monitoring/resources/js/25.1f5ddf92.chunk.js monitoring/resources/js/25.1f5ddf92.chunk.js
    monitoring/resources/js/14.00361eca.chunk.js monitoring/resources/js/14.00361eca.chunk.js
    monitoring/resources/js/34.0414c914.chunk.js monitoring/resources/js/34.0414c914.chunk.js
    monitoring/resources/js/78.4d7d27bf.chunk.js monitoring/resources/js/78.4d7d27bf.chunk.js
    monitoring/resources/js/13.918d3c53.chunk.js monitoring/resources/js/13.918d3c53.chunk.js
    monitoring/resources/js/58.f6084f96.chunk.js monitoring/resources/js/58.f6084f96.chunk.js
    monitoring/resources/js/46.1899cdff.chunk.js monitoring/resources/js/46.1899cdff.chunk.js
    monitoring/resources/js/1.b519853a.chunk.js monitoring/resources/js/1.b519853a.chunk.js
    monitoring/resources/js/runtime-main.fa2e4ab6.js monitoring/resources/js/runtime-main.fa2e4ab6.js
    monitoring/resources/js/54.f25c7061.chunk.js monitoring/resources/js/54.f25c7061.chunk.js
    monitoring/resources/js/76.8b0485b1.chunk.js monitoring/resources/js/76.8b0485b1.chunk.js
    monitoring/resources/js/43.86cb6a87.chunk.js monitoring/resources/js/43.86cb6a87.chunk.js
    monitoring/resources/js/62.b84e3d27.chunk.js monitoring/resources/js/62.b84e3d27.chunk.js
    monitoring/resources/js/21.7303c916.chunk.js monitoring/resources/js/21.7303c916.chunk.js
    monitoring/resources/js/71.8877c2f0.chunk.js monitoring/resources/js/71.8877c2f0.chunk.js
    monitoring/resources/js/9.4a938a01.chunk.js monitoring/resources/js/9.4a938a01.chunk.js
    monitoring/resources/js/67.33b50857.chunk.js monitoring/resources/js/67.33b50857.chunk.js
    monitoring/resources/js/59.3782d2f8.chunk.js monitoring/resources/js/59.3782d2f8.chunk.js
    monitoring/resources/js/57.9d20157c.chunk.js monitoring/resources/js/57.9d20157c.chunk.js
    monitoring/resources/js/52.518e987d.chunk.js monitoring/resources/js/52.518e987d.chunk.js
    monitoring/resources/js/69.47e0b585.chunk.js monitoring/resources/js/69.47e0b585.chunk.js
    monitoring/resources/js/19.5264a9d5.chunk.js monitoring/resources/js/19.5264a9d5.chunk.js
    monitoring/resources/js/17.cab7e560.chunk.js monitoring/resources/js/17.cab7e560.chunk.js
    monitoring/resources/js/56.ada4b045.chunk.js monitoring/resources/js/56.ada4b045.chunk.js
    monitoring/resources/js/68.dd97aa64.chunk.js monitoring/resources/js/68.dd97aa64.chunk.js
    monitoring/resources/js/70.e957fd80.chunk.js monitoring/resources/js/70.e957fd80.chunk.js
    monitoring/resources/js/27.d9558962.chunk.js monitoring/resources/js/27.d9558962.chunk.js
    monitoring/resources/js/40.62438d41.chunk.js monitoring/resources/js/40.62438d41.chunk.js
    monitoring/resources/media/codicon.80a4c25b.ttf monitoring/resources/media/codicon.80a4c25b.ttf
    monitoring/resources/css/4.ee6272b2.chunk.css monitoring/resources/css/4.ee6272b2.chunk.css
    monitoring/resources/css/main.a9deaa9e.chunk.css monitoring/resources/css/main.a9deaa9e.chunk.css
    monitoring/resources/favicon.png monitoring/resources/favicon.png
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
    library/cpp/archive
    library/cpp/mime/types
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/client/server
    ydb/core/health_check
    ydb/core/node_whiteboard
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tx/schemeshard
    ydb/core/util
    ydb/core/viewer/json
    ydb/core/viewer/protos
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
    ydb/public/lib/deprecated/kicli
)

YQL_LAST_ABI_VERSION()

END()
