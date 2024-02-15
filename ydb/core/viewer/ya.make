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
    json_graph.h
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
    json_render.h
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
    viewer_probes.cpp
    wb_aggregate.cpp
    wb_aggregate.h
    wb_filter.cpp
    wb_filter.h
    wb_group.h
    wb_merge.cpp
    wb_merge.h
)

IF (NOT EXPORT_CMAKE)
    # GENERATED MONITORING RESOURCES START
    RESOURCE(
        monitoring/asset-manifest.json monitoring/asset-manifest.json
        monitoring/index.html monitoring/index.html
        monitoring/static/css/1442.02364f2c.chunk.css monitoring/static/css/1442.02364f2c.chunk.css
        monitoring/static/css/4983.4c21ee06.chunk.css monitoring/static/css/4983.4c21ee06.chunk.css
        monitoring/static/css/main.b832b3b4.css monitoring/static/css/main.b832b3b4.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1155.023eda97.chunk.js monitoring/static/js/1155.023eda97.chunk.js
        monitoring/static/js/1442.94af268e.chunk.js monitoring/static/js/1442.94af268e.chunk.js
        monitoring/static/js/1478.9e1c2412.chunk.js monitoring/static/js/1478.9e1c2412.chunk.js
        monitoring/static/js/1747.74784ac8.chunk.js monitoring/static/js/1747.74784ac8.chunk.js
        monitoring/static/js/185.8514493f.chunk.js monitoring/static/js/185.8514493f.chunk.js
        monitoring/static/js/1956.2b162d64.chunk.js monitoring/static/js/1956.2b162d64.chunk.js
        monitoring/static/js/2104.44a7bcb9.chunk.js monitoring/static/js/2104.44a7bcb9.chunk.js
        monitoring/static/js/2141.15556eee.chunk.js monitoring/static/js/2141.15556eee.chunk.js
        monitoring/static/js/2190.12dbb16b.chunk.js monitoring/static/js/2190.12dbb16b.chunk.js
        monitoring/static/js/2194.a1d7ba20.chunk.js monitoring/static/js/2194.a1d7ba20.chunk.js
        monitoring/static/js/2322.f45fe6eb.chunk.js monitoring/static/js/2322.f45fe6eb.chunk.js
        monitoring/static/js/2403.1f5e0f6d.chunk.js monitoring/static/js/2403.1f5e0f6d.chunk.js
        monitoring/static/js/2435.cd3722c2.chunk.js monitoring/static/js/2435.cd3722c2.chunk.js
        monitoring/static/js/2492.ca03b5ec.chunk.js monitoring/static/js/2492.ca03b5ec.chunk.js
        monitoring/static/js/2521.f5d55d22.chunk.js monitoring/static/js/2521.f5d55d22.chunk.js
        monitoring/static/js/2532.568643e3.chunk.js monitoring/static/js/2532.568643e3.chunk.js
        monitoring/static/js/2553.8294b533.chunk.js monitoring/static/js/2553.8294b533.chunk.js
        monitoring/static/js/2840.23fda7bc.chunk.js monitoring/static/js/2840.23fda7bc.chunk.js
        monitoring/static/js/2876.0ba850ea.chunk.js monitoring/static/js/2876.0ba850ea.chunk.js
        monitoring/static/js/2931.5151718e.chunk.js monitoring/static/js/2931.5151718e.chunk.js
        monitoring/static/js/2931.5151718e.chunk.js.LICENSE.txt monitoring/static/js/2931.5151718e.chunk.js.LICENSE.txt
        monitoring/static/js/2962.12f3bfd7.chunk.js monitoring/static/js/2962.12f3bfd7.chunk.js
        monitoring/static/js/2994.8aabe9f0.chunk.js monitoring/static/js/2994.8aabe9f0.chunk.js
        monitoring/static/js/3074.385e02c4.chunk.js monitoring/static/js/3074.385e02c4.chunk.js
        monitoring/static/js/3358.97ecf82d.chunk.js monitoring/static/js/3358.97ecf82d.chunk.js
        monitoring/static/js/337.c451b52e.chunk.js monitoring/static/js/337.c451b52e.chunk.js
        monitoring/static/js/3457.f35ad0ba.chunk.js monitoring/static/js/3457.f35ad0ba.chunk.js
        monitoring/static/js/3498.96f28aa4.chunk.js monitoring/static/js/3498.96f28aa4.chunk.js
        monitoring/static/js/358.1d78ccd3.chunk.js monitoring/static/js/358.1d78ccd3.chunk.js
        monitoring/static/js/3621.8d20e0b0.chunk.js monitoring/static/js/3621.8d20e0b0.chunk.js
        monitoring/static/js/3644.2cfda6ba.chunk.js monitoring/static/js/3644.2cfda6ba.chunk.js
        monitoring/static/js/3771.d0a9d342.chunk.js monitoring/static/js/3771.d0a9d342.chunk.js
        monitoring/static/js/4046.70e870c5.chunk.js monitoring/static/js/4046.70e870c5.chunk.js
        monitoring/static/js/4123.ab3e7f49.chunk.js monitoring/static/js/4123.ab3e7f49.chunk.js
        monitoring/static/js/425.515132c7.chunk.js monitoring/static/js/425.515132c7.chunk.js
        monitoring/static/js/4345.e3e74839.chunk.js monitoring/static/js/4345.e3e74839.chunk.js
        monitoring/static/js/4388.2a399dba.chunk.js monitoring/static/js/4388.2a399dba.chunk.js
        monitoring/static/js/451.1ad6186c.chunk.js monitoring/static/js/451.1ad6186c.chunk.js
        monitoring/static/js/4550.9c0dad07.chunk.js monitoring/static/js/4550.9c0dad07.chunk.js
        monitoring/static/js/4635.7bd94499.chunk.js monitoring/static/js/4635.7bd94499.chunk.js
        monitoring/static/js/4650.fc02f164.chunk.js monitoring/static/js/4650.fc02f164.chunk.js
        monitoring/static/js/4812.60ae60fd.chunk.js monitoring/static/js/4812.60ae60fd.chunk.js
        monitoring/static/js/5107.d1eecebb.chunk.js monitoring/static/js/5107.d1eecebb.chunk.js
        monitoring/static/js/516.e4db5891.chunk.js monitoring/static/js/516.e4db5891.chunk.js
        monitoring/static/js/5311.6270a946.chunk.js monitoring/static/js/5311.6270a946.chunk.js
        monitoring/static/js/5378.767ed9ff.chunk.js monitoring/static/js/5378.767ed9ff.chunk.js
        monitoring/static/js/5396.9380ba3a.chunk.js monitoring/static/js/5396.9380ba3a.chunk.js
        monitoring/static/js/5661.fd50be67.chunk.js monitoring/static/js/5661.fd50be67.chunk.js
        monitoring/static/js/5790.add4d37b.chunk.js monitoring/static/js/5790.add4d37b.chunk.js
        monitoring/static/js/5868.546f9580.chunk.js monitoring/static/js/5868.546f9580.chunk.js
        monitoring/static/js/598.5986e396.chunk.js monitoring/static/js/598.5986e396.chunk.js
        monitoring/static/js/599.89488e03.chunk.js monitoring/static/js/599.89488e03.chunk.js
        monitoring/static/js/6044.b6992725.chunk.js monitoring/static/js/6044.b6992725.chunk.js
        monitoring/static/js/619.cca71494.chunk.js monitoring/static/js/619.cca71494.chunk.js
        monitoring/static/js/6230.de21d918.chunk.js monitoring/static/js/6230.de21d918.chunk.js
        monitoring/static/js/6289.15fd4d30.chunk.js monitoring/static/js/6289.15fd4d30.chunk.js
        monitoring/static/js/6321.35891541.chunk.js monitoring/static/js/6321.35891541.chunk.js
        monitoring/static/js/6329.e2c98f63.chunk.js monitoring/static/js/6329.e2c98f63.chunk.js
        monitoring/static/js/6390.6d3192da.chunk.js monitoring/static/js/6390.6d3192da.chunk.js
        monitoring/static/js/6619.5dceaaf5.chunk.js monitoring/static/js/6619.5dceaaf5.chunk.js
        monitoring/static/js/6692.42c507a0.chunk.js monitoring/static/js/6692.42c507a0.chunk.js
        monitoring/static/js/6795.6f7c26bb.chunk.js monitoring/static/js/6795.6f7c26bb.chunk.js
        monitoring/static/js/6876.4759c0c0.chunk.js monitoring/static/js/6876.4759c0c0.chunk.js
        monitoring/static/js/6898.d0927d1b.chunk.js monitoring/static/js/6898.d0927d1b.chunk.js
        monitoring/static/js/7409.94cd09aa.chunk.js monitoring/static/js/7409.94cd09aa.chunk.js
        monitoring/static/js/7529.5cfc6929.chunk.js monitoring/static/js/7529.5cfc6929.chunk.js
        monitoring/static/js/7543.90ee5df4.chunk.js monitoring/static/js/7543.90ee5df4.chunk.js
        monitoring/static/js/7554.8bf92e8f.chunk.js monitoring/static/js/7554.8bf92e8f.chunk.js
        monitoring/static/js/7645.81dafac3.chunk.js monitoring/static/js/7645.81dafac3.chunk.js
        monitoring/static/js/785.d30867eb.chunk.js monitoring/static/js/785.d30867eb.chunk.js
        monitoring/static/js/8450.4cca2512.chunk.js monitoring/static/js/8450.4cca2512.chunk.js
        monitoring/static/js/86.582266ce.chunk.js monitoring/static/js/86.582266ce.chunk.js
        monitoring/static/js/8622.3b20e09b.chunk.js monitoring/static/js/8622.3b20e09b.chunk.js
        monitoring/static/js/8791.f084d68f.chunk.js monitoring/static/js/8791.f084d68f.chunk.js
        monitoring/static/js/8905.9add87ea.chunk.js monitoring/static/js/8905.9add87ea.chunk.js
        monitoring/static/js/9173.d2a89c3f.chunk.js monitoring/static/js/9173.d2a89c3f.chunk.js
        monitoring/static/js/9205.e9e8569d.chunk.js monitoring/static/js/9205.e9e8569d.chunk.js
        monitoring/static/js/924.04ec4c42.chunk.js monitoring/static/js/924.04ec4c42.chunk.js
        monitoring/static/js/9319.2433c687.chunk.js monitoring/static/js/9319.2433c687.chunk.js
        monitoring/static/js/9371.fec63c96.chunk.js monitoring/static/js/9371.fec63c96.chunk.js
        monitoring/static/js/9433.3029e781.chunk.js monitoring/static/js/9433.3029e781.chunk.js
        monitoring/static/js/9526.f25868ff.chunk.js monitoring/static/js/9526.f25868ff.chunk.js
        monitoring/static/js/9621.45e648f5.chunk.js monitoring/static/js/9621.45e648f5.chunk.js
        monitoring/static/js/9876.256abc93.chunk.js monitoring/static/js/9876.256abc93.chunk.js
        monitoring/static/js/9923.5b43ddf2.chunk.js monitoring/static/js/9923.5b43ddf2.chunk.js
        monitoring/static/js/main.d88d9cdb.js monitoring/static/js/main.d88d9cdb.js
        monitoring/static/js/main.d88d9cdb.js.LICENSE.txt monitoring/static/js/main.d88d9cdb.js.LICENSE.txt
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.80a4c25b73c1f97077ed.ttf monitoring/static/media/codicon.80a4c25b73c1f97077ed.ttf
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
    ydb/library/actors/core
    ydb/library/actors/helpers
    library/cpp/archive
    library/cpp/mime/types
    library/cpp/protobuf/json
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/vdisk/common
    ydb/core/client/server
    ydb/core/graph/api
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
