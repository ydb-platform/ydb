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
    json_acl.h
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
    monitoring/resources/js/71.09e486f9.chunk.js monitoring/resources/js/71.09e486f9.chunk.js
    monitoring/resources/js/63.ac3b2608.chunk.js monitoring/resources/js/63.ac3b2608.chunk.js
    monitoring/resources/js/61.952e7098.chunk.js monitoring/resources/js/61.952e7098.chunk.js
    monitoring/resources/js/8.30a04af0.chunk.js monitoring/resources/js/8.30a04af0.chunk.js
    monitoring/resources/js/9.be947608.chunk.js monitoring/resources/js/9.be947608.chunk.js
    monitoring/resources/js/44.788c2b34.chunk.js monitoring/resources/js/44.788c2b34.chunk.js
    monitoring/resources/js/34.0ef286a6.chunk.js monitoring/resources/js/34.0ef286a6.chunk.js
    monitoring/resources/js/66.843845f2.chunk.js monitoring/resources/js/66.843845f2.chunk.js
    monitoring/resources/js/7.f7e15990.chunk.js monitoring/resources/js/7.f7e15990.chunk.js
    monitoring/resources/js/46.116eab26.chunk.js monitoring/resources/js/46.116eab26.chunk.js
    monitoring/resources/js/60.ccbe624b.chunk.js monitoring/resources/js/60.ccbe624b.chunk.js
    monitoring/resources/js/73.58d8a1d2.chunk.js monitoring/resources/js/73.58d8a1d2.chunk.js
    monitoring/resources/js/49.4bf8a0ea.chunk.js monitoring/resources/js/49.4bf8a0ea.chunk.js
    monitoring/resources/js/11.63840653.chunk.js monitoring/resources/js/11.63840653.chunk.js
    monitoring/resources/js/5.3db46e5a.chunk.js monitoring/resources/js/5.3db46e5a.chunk.js
    monitoring/resources/js/52.6a42d8d6.chunk.js monitoring/resources/js/52.6a42d8d6.chunk.js
    monitoring/resources/js/17.11dbc303.chunk.js monitoring/resources/js/17.11dbc303.chunk.js
    monitoring/resources/js/68.85196c44.chunk.js monitoring/resources/js/68.85196c44.chunk.js
    monitoring/resources/js/22.a7d8757e.chunk.js monitoring/resources/js/22.a7d8757e.chunk.js
    monitoring/resources/js/56.2cca3aa1.chunk.js monitoring/resources/js/56.2cca3aa1.chunk.js
    monitoring/resources/js/62.0e7c7dbb.chunk.js monitoring/resources/js/62.0e7c7dbb.chunk.js
    monitoring/resources/js/1.bfcc94ca.chunk.js monitoring/resources/js/1.bfcc94ca.chunk.js
    monitoring/resources/js/74.32021fea.chunk.js monitoring/resources/js/74.32021fea.chunk.js
    monitoring/resources/js/69.50dc4733.chunk.js monitoring/resources/js/69.50dc4733.chunk.js
    monitoring/resources/js/24.24e9f60d.chunk.js monitoring/resources/js/24.24e9f60d.chunk.js
    monitoring/resources/js/14.cec5e7db.chunk.js monitoring/resources/js/14.cec5e7db.chunk.js
    monitoring/resources/js/4.b61784d1.chunk.js monitoring/resources/js/4.b61784d1.chunk.js
    monitoring/resources/js/54.d4de7c9d.chunk.js monitoring/resources/js/54.d4de7c9d.chunk.js
    monitoring/resources/js/30.df7470e6.chunk.js monitoring/resources/js/30.df7470e6.chunk.js
    monitoring/resources/js/6.8cdb2fbf.chunk.js monitoring/resources/js/6.8cdb2fbf.chunk.js
    monitoring/resources/js/25.7d7c8a2f.chunk.js monitoring/resources/js/25.7d7c8a2f.chunk.js
    monitoring/resources/js/13.f97a66a9.chunk.js monitoring/resources/js/13.f97a66a9.chunk.js
    monitoring/resources/js/10.92b9e4a1.chunk.js monitoring/resources/js/10.92b9e4a1.chunk.js
    monitoring/resources/js/57.d4bbaa27.chunk.js monitoring/resources/js/57.d4bbaa27.chunk.js
    monitoring/resources/js/67.0bc4105a.chunk.js monitoring/resources/js/67.0bc4105a.chunk.js
    monitoring/resources/js/39.b79b007e.chunk.js monitoring/resources/js/39.b79b007e.chunk.js
    monitoring/resources/js/51.339e01c0.chunk.js monitoring/resources/js/51.339e01c0.chunk.js
    monitoring/resources/js/19.05be344f.chunk.js monitoring/resources/js/19.05be344f.chunk.js
    monitoring/resources/js/53.e2452bf9.chunk.js monitoring/resources/js/53.e2452bf9.chunk.js
    monitoring/resources/js/48.324e0934.chunk.js monitoring/resources/js/48.324e0934.chunk.js
    monitoring/resources/js/33.cd3a27e7.chunk.js monitoring/resources/js/33.cd3a27e7.chunk.js
    monitoring/resources/js/65.ded5dca8.chunk.js monitoring/resources/js/65.ded5dca8.chunk.js
    monitoring/resources/js/27.23171e17.chunk.js monitoring/resources/js/27.23171e17.chunk.js
    monitoring/resources/js/59.c56e69da.chunk.js monitoring/resources/js/59.c56e69da.chunk.js
    monitoring/resources/js/38.077194b4.chunk.js monitoring/resources/js/38.077194b4.chunk.js
    monitoring/resources/js/40.5ff8ce60.chunk.js monitoring/resources/js/40.5ff8ce60.chunk.js
    monitoring/resources/js/0.cacaa682.chunk.js monitoring/resources/js/0.cacaa682.chunk.js
    monitoring/resources/js/55.3116dd9a.chunk.js monitoring/resources/js/55.3116dd9a.chunk.js
    monitoring/resources/js/42.ed63d240.chunk.js monitoring/resources/js/42.ed63d240.chunk.js
    monitoring/resources/js/77.3d69f187.chunk.js monitoring/resources/js/77.3d69f187.chunk.js
    monitoring/resources/js/58.d67d0314.chunk.js monitoring/resources/js/58.d67d0314.chunk.js
    monitoring/resources/js/16.1b3ba162.chunk.js monitoring/resources/js/16.1b3ba162.chunk.js
    monitoring/resources/js/37.24961211.chunk.js monitoring/resources/js/37.24961211.chunk.js
    monitoring/resources/js/47.0a9a3ec2.chunk.js monitoring/resources/js/47.0a9a3ec2.chunk.js
    monitoring/resources/js/70.640259e3.chunk.js monitoring/resources/js/70.640259e3.chunk.js
    monitoring/resources/js/35.82008f81.chunk.js monitoring/resources/js/35.82008f81.chunk.js
    monitoring/resources/js/76.bea8889b.chunk.js monitoring/resources/js/76.bea8889b.chunk.js
    monitoring/resources/js/75.17492dfd.chunk.js monitoring/resources/js/75.17492dfd.chunk.js
    monitoring/resources/js/64.7d08354e.chunk.js monitoring/resources/js/64.7d08354e.chunk.js
    monitoring/resources/js/26.5b986789.chunk.js monitoring/resources/js/26.5b986789.chunk.js
    monitoring/resources/js/20.2ace32ef.chunk.js monitoring/resources/js/20.2ace32ef.chunk.js
    monitoring/resources/js/runtime-main.f9849c26.js monitoring/resources/js/runtime-main.f9849c26.js
    monitoring/resources/js/32.16e2d32b.chunk.js monitoring/resources/js/32.16e2d32b.chunk.js
    monitoring/resources/js/15.41d064ac.chunk.js monitoring/resources/js/15.41d064ac.chunk.js
    monitoring/resources/js/21.6e60e40a.chunk.js monitoring/resources/js/21.6e60e40a.chunk.js
    monitoring/resources/js/12.471ea0c6.chunk.js monitoring/resources/js/12.471ea0c6.chunk.js
    monitoring/resources/js/28.d340c225.chunk.js monitoring/resources/js/28.d340c225.chunk.js
    monitoring/resources/js/31.5a5752ae.chunk.js monitoring/resources/js/31.5a5752ae.chunk.js
    monitoring/resources/js/50.38a9fb48.chunk.js monitoring/resources/js/50.38a9fb48.chunk.js
    monitoring/resources/js/45.917abc5b.chunk.js monitoring/resources/js/45.917abc5b.chunk.js
    monitoring/resources/js/29.5954f939.chunk.js monitoring/resources/js/29.5954f939.chunk.js
    monitoring/resources/js/78.2d9df1b5.chunk.js monitoring/resources/js/78.2d9df1b5.chunk.js
    monitoring/resources/js/36.adfe07e9.chunk.js monitoring/resources/js/36.adfe07e9.chunk.js
    monitoring/resources/js/41.3fd36433.chunk.js monitoring/resources/js/41.3fd36433.chunk.js
    monitoring/resources/js/43.24e2d345.chunk.js monitoring/resources/js/43.24e2d345.chunk.js
    monitoring/resources/js/72.21b0fecb.chunk.js monitoring/resources/js/72.21b0fecb.chunk.js
    monitoring/resources/js/18.71484fde.chunk.js monitoring/resources/js/18.71484fde.chunk.js
    monitoring/resources/js/23.4aafb879.chunk.js monitoring/resources/js/23.4aafb879.chunk.js
    monitoring/resources/js/main.d392c3f5.chunk.js monitoring/resources/js/main.d392c3f5.chunk.js
    monitoring/resources/media/codicon.80a4c25b.ttf monitoring/resources/media/codicon.80a4c25b.ttf
    monitoring/resources/css/4.501d9812.chunk.css monitoring/resources/css/4.501d9812.chunk.css
    monitoring/resources/css/main.947ae4dd.chunk.css monitoring/resources/css/main.947ae4dd.chunk.css
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
