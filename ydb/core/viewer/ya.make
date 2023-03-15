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
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/css/5.84c10974.chunk.css monitoring/static/css/5.84c10974.chunk.css
        monitoring/static/css/main.0f3d332c.chunk.css monitoring/static/css/main.0f3d332c.chunk.css
        monitoring/static/js/0.c5380b7b.chunk.js monitoring/static/js/0.c5380b7b.chunk.js
        monitoring/static/js/10.df79be40.chunk.js monitoring/static/js/10.df79be40.chunk.js
        monitoring/static/js/11.6e64557e.chunk.js monitoring/static/js/11.6e64557e.chunk.js
        monitoring/static/js/12.c2409fb8.chunk.js monitoring/static/js/12.c2409fb8.chunk.js
        monitoring/static/js/13.3ea6c80e.chunk.js monitoring/static/js/13.3ea6c80e.chunk.js
        monitoring/static/js/14.a4f43d3e.chunk.js monitoring/static/js/14.a4f43d3e.chunk.js
        monitoring/static/js/15.f97f1e2a.chunk.js monitoring/static/js/15.f97f1e2a.chunk.js
        monitoring/static/js/16.bc58756c.chunk.js monitoring/static/js/16.bc58756c.chunk.js
        monitoring/static/js/17.2e164440.chunk.js monitoring/static/js/17.2e164440.chunk.js
        monitoring/static/js/18.f88ba44b.chunk.js monitoring/static/js/18.f88ba44b.chunk.js
        monitoring/static/js/19.6a8294e8.chunk.js monitoring/static/js/19.6a8294e8.chunk.js
        monitoring/static/js/1.b6311cdf.chunk.js monitoring/static/js/1.b6311cdf.chunk.js
        monitoring/static/js/20.e618c35e.chunk.js monitoring/static/js/20.e618c35e.chunk.js
        monitoring/static/js/21.4968c55e.chunk.js monitoring/static/js/21.4968c55e.chunk.js
        monitoring/static/js/22.73e67630.chunk.js monitoring/static/js/22.73e67630.chunk.js
        monitoring/static/js/23.71a6b54a.chunk.js monitoring/static/js/23.71a6b54a.chunk.js
        monitoring/static/js/24.4ff6d622.chunk.js monitoring/static/js/24.4ff6d622.chunk.js
        monitoring/static/js/2.4db00aff.chunk.js monitoring/static/js/2.4db00aff.chunk.js
        monitoring/static/js/25.75bd6567.chunk.js monitoring/static/js/25.75bd6567.chunk.js
        monitoring/static/js/26.2d50cb53.chunk.js monitoring/static/js/26.2d50cb53.chunk.js
        monitoring/static/js/27.0ac7f563.chunk.js monitoring/static/js/27.0ac7f563.chunk.js
        monitoring/static/js/28.bd815bf5.chunk.js monitoring/static/js/28.bd815bf5.chunk.js
        monitoring/static/js/29.2d72b7aa.chunk.js monitoring/static/js/29.2d72b7aa.chunk.js
        monitoring/static/js/30.815dc3cf.chunk.js monitoring/static/js/30.815dc3cf.chunk.js
        monitoring/static/js/31.8c7c4d1c.chunk.js monitoring/static/js/31.8c7c4d1c.chunk.js
        monitoring/static/js/32.89529c16.chunk.js monitoring/static/js/32.89529c16.chunk.js
        monitoring/static/js/33.abbaea57.chunk.js monitoring/static/js/33.abbaea57.chunk.js
        monitoring/static/js/34.5abe15ab.chunk.js monitoring/static/js/34.5abe15ab.chunk.js
        monitoring/static/js/35.717d3992.chunk.js monitoring/static/js/35.717d3992.chunk.js
        monitoring/static/js/36.26ed4ff7.chunk.js monitoring/static/js/36.26ed4ff7.chunk.js
        monitoring/static/js/37.8315f92a.chunk.js monitoring/static/js/37.8315f92a.chunk.js
        monitoring/static/js/38.3b18ceb3.chunk.js monitoring/static/js/38.3b18ceb3.chunk.js
        monitoring/static/js/39.b11bced2.chunk.js monitoring/static/js/39.b11bced2.chunk.js
        monitoring/static/js/40.f2ce31a5.chunk.js monitoring/static/js/40.f2ce31a5.chunk.js
        monitoring/static/js/41.c64fec96.chunk.js monitoring/static/js/41.c64fec96.chunk.js
        monitoring/static/js/42.ec2b9095.chunk.js monitoring/static/js/42.ec2b9095.chunk.js
        monitoring/static/js/43.63b1eb86.chunk.js monitoring/static/js/43.63b1eb86.chunk.js
        monitoring/static/js/44.f7c5b90c.chunk.js monitoring/static/js/44.f7c5b90c.chunk.js
        monitoring/static/js/45.d92acdfc.chunk.js monitoring/static/js/45.d92acdfc.chunk.js
        monitoring/static/js/46.b09b0e20.chunk.js monitoring/static/js/46.b09b0e20.chunk.js
        monitoring/static/js/47.c1fecdc6.chunk.js monitoring/static/js/47.c1fecdc6.chunk.js
        monitoring/static/js/48.6d525893.chunk.js monitoring/static/js/48.6d525893.chunk.js
        monitoring/static/js/49.dd3ad8f1.chunk.js monitoring/static/js/49.dd3ad8f1.chunk.js
        monitoring/static/js/50.42b181e5.chunk.js monitoring/static/js/50.42b181e5.chunk.js
        monitoring/static/js/5.142c225c.chunk.js monitoring/static/js/5.142c225c.chunk.js
        monitoring/static/js/51.65bc7579.chunk.js monitoring/static/js/51.65bc7579.chunk.js
        monitoring/static/js/52.59715ad3.chunk.js monitoring/static/js/52.59715ad3.chunk.js
        monitoring/static/js/53.5a111676.chunk.js monitoring/static/js/53.5a111676.chunk.js
        monitoring/static/js/54.2814bff2.chunk.js monitoring/static/js/54.2814bff2.chunk.js
        monitoring/static/js/55.f4586e30.chunk.js monitoring/static/js/55.f4586e30.chunk.js
        monitoring/static/js/56.fedb6eb5.chunk.js monitoring/static/js/56.fedb6eb5.chunk.js
        monitoring/static/js/57.effec621.chunk.js monitoring/static/js/57.effec621.chunk.js
        monitoring/static/js/58.e56cbfd5.chunk.js monitoring/static/js/58.e56cbfd5.chunk.js
        monitoring/static/js/59.ee2824dd.chunk.js monitoring/static/js/59.ee2824dd.chunk.js
        monitoring/static/js/60.1d0e8735.chunk.js monitoring/static/js/60.1d0e8735.chunk.js
        monitoring/static/js/6.073862c3.chunk.js monitoring/static/js/6.073862c3.chunk.js
        monitoring/static/js/61.16a47cbf.chunk.js monitoring/static/js/61.16a47cbf.chunk.js
        monitoring/static/js/62.72cce1ae.chunk.js monitoring/static/js/62.72cce1ae.chunk.js
        monitoring/static/js/63.e0e2807e.chunk.js monitoring/static/js/63.e0e2807e.chunk.js
        monitoring/static/js/64.d92153bd.chunk.js monitoring/static/js/64.d92153bd.chunk.js
        monitoring/static/js/65.ede203eb.chunk.js monitoring/static/js/65.ede203eb.chunk.js
        monitoring/static/js/66.c00a065e.chunk.js monitoring/static/js/66.c00a065e.chunk.js
        monitoring/static/js/67.23176d23.chunk.js monitoring/static/js/67.23176d23.chunk.js
        monitoring/static/js/68.b64bd476.chunk.js monitoring/static/js/68.b64bd476.chunk.js
        monitoring/static/js/69.4e8c1b6c.chunk.js monitoring/static/js/69.4e8c1b6c.chunk.js
        monitoring/static/js/70.c0535ed8.chunk.js monitoring/static/js/70.c0535ed8.chunk.js
        monitoring/static/js/71.560b1c44.chunk.js monitoring/static/js/71.560b1c44.chunk.js
        monitoring/static/js/72.d0d02379.chunk.js monitoring/static/js/72.d0d02379.chunk.js
        monitoring/static/js/73.bd0cfa8e.chunk.js monitoring/static/js/73.bd0cfa8e.chunk.js
        monitoring/static/js/7.40244105.chunk.js monitoring/static/js/7.40244105.chunk.js
        monitoring/static/js/74.2927e0f8.chunk.js monitoring/static/js/74.2927e0f8.chunk.js
        monitoring/static/js/75.ed52e895.chunk.js monitoring/static/js/75.ed52e895.chunk.js
        monitoring/static/js/76.51b768b0.chunk.js monitoring/static/js/76.51b768b0.chunk.js
        monitoring/static/js/77.48429bd2.chunk.js monitoring/static/js/77.48429bd2.chunk.js
        monitoring/static/js/78.912de5f4.chunk.js monitoring/static/js/78.912de5f4.chunk.js
        monitoring/static/js/79.044441f1.chunk.js monitoring/static/js/79.044441f1.chunk.js
        monitoring/static/js/80.cf967f30.chunk.js monitoring/static/js/80.cf967f30.chunk.js
        monitoring/static/js/81.5cf3d67d.chunk.js monitoring/static/js/81.5cf3d67d.chunk.js
        monitoring/static/js/8.32edc080.chunk.js monitoring/static/js/8.32edc080.chunk.js
        monitoring/static/js/9.b62f3db1.chunk.js monitoring/static/js/9.b62f3db1.chunk.js
        monitoring/static/js/main.6e711058.chunk.js monitoring/static/js/main.6e711058.chunk.js
        monitoring/static/js/runtime-main.eca931b9.js monitoring/static/js/runtime-main.eca931b9.js
        monitoring/static/media/403.271ae19f.svg monitoring/static/media/403.271ae19f.svg
        monitoring/static/media/403.6367e52f.svg monitoring/static/media/403.6367e52f.svg
        monitoring/static/media/codicon.80a4c25b.ttf monitoring/static/media/codicon.80a4c25b.ttf
        monitoring/static/media/thumbsUp.d4a03fba.svg monitoring/static/media/thumbsUp.d4a03fba.svg

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
