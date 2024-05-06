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
    json_autocomplete.h
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
    query_autocomplete_helper.h
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
        monitoring/static/css/4983.caf0c2a0.chunk.css monitoring/static/css/4983.caf0c2a0.chunk.css
        monitoring/static/css/7539.157f7e9f.chunk.css monitoring/static/css/7539.157f7e9f.chunk.css
        monitoring/static/css/main.eab8584b.css monitoring/static/css/main.eab8584b.css
        monitoring/static/favicon.png monitoring/static/favicon.png
        monitoring/static/js/1155.4fce1854.chunk.js monitoring/static/js/1155.4fce1854.chunk.js
        monitoring/static/js/1155.4fce1854.chunk.js.LICENSE.txt monitoring/static/js/1155.4fce1854.chunk.js.LICENSE.txt
        monitoring/static/js/1478.7e1423b2.chunk.js monitoring/static/js/1478.7e1423b2.chunk.js
        monitoring/static/js/1478.7e1423b2.chunk.js.LICENSE.txt monitoring/static/js/1478.7e1423b2.chunk.js.LICENSE.txt
        monitoring/static/js/1747.b4331799.chunk.js monitoring/static/js/1747.b4331799.chunk.js
        monitoring/static/js/1747.b4331799.chunk.js.LICENSE.txt monitoring/static/js/1747.b4331799.chunk.js.LICENSE.txt
        monitoring/static/js/185.3f659c11.chunk.js monitoring/static/js/185.3f659c11.chunk.js
        monitoring/static/js/185.3f659c11.chunk.js.LICENSE.txt monitoring/static/js/185.3f659c11.chunk.js.LICENSE.txt
        monitoring/static/js/1956.23e162d0.chunk.js monitoring/static/js/1956.23e162d0.chunk.js
        monitoring/static/js/1956.23e162d0.chunk.js.LICENSE.txt monitoring/static/js/1956.23e162d0.chunk.js.LICENSE.txt
        monitoring/static/js/2104.4f22ecac.chunk.js monitoring/static/js/2104.4f22ecac.chunk.js
        monitoring/static/js/2104.4f22ecac.chunk.js.LICENSE.txt monitoring/static/js/2104.4f22ecac.chunk.js.LICENSE.txt
        monitoring/static/js/2118.fe3bd07f.chunk.js monitoring/static/js/2118.fe3bd07f.chunk.js
        monitoring/static/js/2118.fe3bd07f.chunk.js.LICENSE.txt monitoring/static/js/2118.fe3bd07f.chunk.js.LICENSE.txt
        monitoring/static/js/2141.26c930aa.chunk.js monitoring/static/js/2141.26c930aa.chunk.js
        monitoring/static/js/2141.26c930aa.chunk.js.LICENSE.txt monitoring/static/js/2141.26c930aa.chunk.js.LICENSE.txt
        monitoring/static/js/2190.27f354f5.chunk.js monitoring/static/js/2190.27f354f5.chunk.js
        monitoring/static/js/2190.27f354f5.chunk.js.LICENSE.txt monitoring/static/js/2190.27f354f5.chunk.js.LICENSE.txt
        monitoring/static/js/2194.38bafdfc.chunk.js monitoring/static/js/2194.38bafdfc.chunk.js
        monitoring/static/js/2194.38bafdfc.chunk.js.LICENSE.txt monitoring/static/js/2194.38bafdfc.chunk.js.LICENSE.txt
        monitoring/static/js/2322.29255c22.chunk.js monitoring/static/js/2322.29255c22.chunk.js
        monitoring/static/js/2322.29255c22.chunk.js.LICENSE.txt monitoring/static/js/2322.29255c22.chunk.js.LICENSE.txt
        monitoring/static/js/2403.d86e3324.chunk.js monitoring/static/js/2403.d86e3324.chunk.js
        monitoring/static/js/2403.d86e3324.chunk.js.LICENSE.txt monitoring/static/js/2403.d86e3324.chunk.js.LICENSE.txt
        monitoring/static/js/2435.092e8d7f.chunk.js monitoring/static/js/2435.092e8d7f.chunk.js
        monitoring/static/js/2492.64b7d727.chunk.js monitoring/static/js/2492.64b7d727.chunk.js
        monitoring/static/js/2492.64b7d727.chunk.js.LICENSE.txt monitoring/static/js/2492.64b7d727.chunk.js.LICENSE.txt
        monitoring/static/js/2521.21bdfab9.chunk.js monitoring/static/js/2521.21bdfab9.chunk.js
        monitoring/static/js/2521.21bdfab9.chunk.js.LICENSE.txt monitoring/static/js/2521.21bdfab9.chunk.js.LICENSE.txt
        monitoring/static/js/2532.30bb087d.chunk.js monitoring/static/js/2532.30bb087d.chunk.js
        monitoring/static/js/2532.30bb087d.chunk.js.LICENSE.txt monitoring/static/js/2532.30bb087d.chunk.js.LICENSE.txt
        monitoring/static/js/2553.9c696b27.chunk.js monitoring/static/js/2553.9c696b27.chunk.js
        monitoring/static/js/2553.9c696b27.chunk.js.LICENSE.txt monitoring/static/js/2553.9c696b27.chunk.js.LICENSE.txt
        monitoring/static/js/2840.b69eb597.chunk.js monitoring/static/js/2840.b69eb597.chunk.js
        monitoring/static/js/2840.b69eb597.chunk.js.LICENSE.txt monitoring/static/js/2840.b69eb597.chunk.js.LICENSE.txt
        monitoring/static/js/2876.afe7e47f.chunk.js monitoring/static/js/2876.afe7e47f.chunk.js
        monitoring/static/js/2876.afe7e47f.chunk.js.LICENSE.txt monitoring/static/js/2876.afe7e47f.chunk.js.LICENSE.txt
        monitoring/static/js/2931.e56f5c86.chunk.js monitoring/static/js/2931.e56f5c86.chunk.js
        monitoring/static/js/2931.e56f5c86.chunk.js.LICENSE.txt monitoring/static/js/2931.e56f5c86.chunk.js.LICENSE.txt
        monitoring/static/js/2962.66e01691.chunk.js monitoring/static/js/2962.66e01691.chunk.js
        monitoring/static/js/2962.66e01691.chunk.js.LICENSE.txt monitoring/static/js/2962.66e01691.chunk.js.LICENSE.txt
        monitoring/static/js/2994.e6c77407.chunk.js monitoring/static/js/2994.e6c77407.chunk.js
        monitoring/static/js/2994.e6c77407.chunk.js.LICENSE.txt monitoring/static/js/2994.e6c77407.chunk.js.LICENSE.txt
        monitoring/static/js/3074.bbb8aaef.chunk.js monitoring/static/js/3074.bbb8aaef.chunk.js
        monitoring/static/js/3074.bbb8aaef.chunk.js.LICENSE.txt monitoring/static/js/3074.bbb8aaef.chunk.js.LICENSE.txt
        monitoring/static/js/3358.c4b77137.chunk.js monitoring/static/js/3358.c4b77137.chunk.js
        monitoring/static/js/3358.c4b77137.chunk.js.LICENSE.txt monitoring/static/js/3358.c4b77137.chunk.js.LICENSE.txt
        monitoring/static/js/337.b6fc715e.chunk.js monitoring/static/js/337.b6fc715e.chunk.js
        monitoring/static/js/337.b6fc715e.chunk.js.LICENSE.txt monitoring/static/js/337.b6fc715e.chunk.js.LICENSE.txt
        monitoring/static/js/3457.b193afe6.chunk.js monitoring/static/js/3457.b193afe6.chunk.js
        monitoring/static/js/3498.c7d39060.chunk.js monitoring/static/js/3498.c7d39060.chunk.js
        monitoring/static/js/3498.c7d39060.chunk.js.LICENSE.txt monitoring/static/js/3498.c7d39060.chunk.js.LICENSE.txt
        monitoring/static/js/358.d6300019.chunk.js monitoring/static/js/358.d6300019.chunk.js
        monitoring/static/js/358.d6300019.chunk.js.LICENSE.txt monitoring/static/js/358.d6300019.chunk.js.LICENSE.txt
        monitoring/static/js/3621.9b6c61ab.chunk.js monitoring/static/js/3621.9b6c61ab.chunk.js
        monitoring/static/js/3621.9b6c61ab.chunk.js.LICENSE.txt monitoring/static/js/3621.9b6c61ab.chunk.js.LICENSE.txt
        monitoring/static/js/3644.aeda46ca.chunk.js monitoring/static/js/3644.aeda46ca.chunk.js
        monitoring/static/js/3644.aeda46ca.chunk.js.LICENSE.txt monitoring/static/js/3644.aeda46ca.chunk.js.LICENSE.txt
        monitoring/static/js/3771.764124c3.chunk.js monitoring/static/js/3771.764124c3.chunk.js
        monitoring/static/js/3771.764124c3.chunk.js.LICENSE.txt monitoring/static/js/3771.764124c3.chunk.js.LICENSE.txt
        monitoring/static/js/4046.5dac72a9.chunk.js monitoring/static/js/4046.5dac72a9.chunk.js
        monitoring/static/js/4046.5dac72a9.chunk.js.LICENSE.txt monitoring/static/js/4046.5dac72a9.chunk.js.LICENSE.txt
        monitoring/static/js/4123.64882a16.chunk.js monitoring/static/js/4123.64882a16.chunk.js
        monitoring/static/js/4123.64882a16.chunk.js.LICENSE.txt monitoring/static/js/4123.64882a16.chunk.js.LICENSE.txt
        monitoring/static/js/425.c6ae8758.chunk.js monitoring/static/js/425.c6ae8758.chunk.js
        monitoring/static/js/425.c6ae8758.chunk.js.LICENSE.txt monitoring/static/js/425.c6ae8758.chunk.js.LICENSE.txt
        monitoring/static/js/4345.b734ab51.chunk.js monitoring/static/js/4345.b734ab51.chunk.js
        monitoring/static/js/4345.b734ab51.chunk.js.LICENSE.txt monitoring/static/js/4345.b734ab51.chunk.js.LICENSE.txt
        monitoring/static/js/4388.edb51304.chunk.js monitoring/static/js/4388.edb51304.chunk.js
        monitoring/static/js/4388.edb51304.chunk.js.LICENSE.txt monitoring/static/js/4388.edb51304.chunk.js.LICENSE.txt
        monitoring/static/js/451.3b449e79.chunk.js monitoring/static/js/451.3b449e79.chunk.js
        monitoring/static/js/451.3b449e79.chunk.js.LICENSE.txt monitoring/static/js/451.3b449e79.chunk.js.LICENSE.txt
        monitoring/static/js/4550.2e04d705.chunk.js monitoring/static/js/4550.2e04d705.chunk.js
        monitoring/static/js/4550.2e04d705.chunk.js.LICENSE.txt monitoring/static/js/4550.2e04d705.chunk.js.LICENSE.txt
        monitoring/static/js/4635.ffa9b6b7.chunk.js monitoring/static/js/4635.ffa9b6b7.chunk.js
        monitoring/static/js/4635.ffa9b6b7.chunk.js.LICENSE.txt monitoring/static/js/4635.ffa9b6b7.chunk.js.LICENSE.txt
        monitoring/static/js/4789.41de1f87.chunk.js monitoring/static/js/4789.41de1f87.chunk.js
        monitoring/static/js/4789.41de1f87.chunk.js.LICENSE.txt monitoring/static/js/4789.41de1f87.chunk.js.LICENSE.txt
        monitoring/static/js/4812.73af8448.chunk.js monitoring/static/js/4812.73af8448.chunk.js
        monitoring/static/js/4812.73af8448.chunk.js.LICENSE.txt monitoring/static/js/4812.73af8448.chunk.js.LICENSE.txt
        monitoring/static/js/5107.e893787e.chunk.js monitoring/static/js/5107.e893787e.chunk.js
        monitoring/static/js/5107.e893787e.chunk.js.LICENSE.txt monitoring/static/js/5107.e893787e.chunk.js.LICENSE.txt
        monitoring/static/js/5168.6fb23f08.chunk.js monitoring/static/js/5168.6fb23f08.chunk.js
        monitoring/static/js/5168.6fb23f08.chunk.js.LICENSE.txt monitoring/static/js/5168.6fb23f08.chunk.js.LICENSE.txt
        monitoring/static/js/5311.a500a1ea.chunk.js monitoring/static/js/5311.a500a1ea.chunk.js
        monitoring/static/js/5311.a500a1ea.chunk.js.LICENSE.txt monitoring/static/js/5311.a500a1ea.chunk.js.LICENSE.txt
        monitoring/static/js/5378.86805fba.chunk.js monitoring/static/js/5378.86805fba.chunk.js
        monitoring/static/js/5378.86805fba.chunk.js.LICENSE.txt monitoring/static/js/5378.86805fba.chunk.js.LICENSE.txt
        monitoring/static/js/5661.c83a4eb0.chunk.js monitoring/static/js/5661.c83a4eb0.chunk.js
        monitoring/static/js/5661.c83a4eb0.chunk.js.LICENSE.txt monitoring/static/js/5661.c83a4eb0.chunk.js.LICENSE.txt
        monitoring/static/js/5790.eda45e33.chunk.js monitoring/static/js/5790.eda45e33.chunk.js
        monitoring/static/js/5790.eda45e33.chunk.js.LICENSE.txt monitoring/static/js/5790.eda45e33.chunk.js.LICENSE.txt
        monitoring/static/js/5868.be04313a.chunk.js monitoring/static/js/5868.be04313a.chunk.js
        monitoring/static/js/5868.be04313a.chunk.js.LICENSE.txt monitoring/static/js/5868.be04313a.chunk.js.LICENSE.txt
        monitoring/static/js/598.243fd68d.chunk.js monitoring/static/js/598.243fd68d.chunk.js
        monitoring/static/js/599.c58caf58.chunk.js monitoring/static/js/599.c58caf58.chunk.js
        monitoring/static/js/6044.2de9962d.chunk.js monitoring/static/js/6044.2de9962d.chunk.js
        monitoring/static/js/6044.2de9962d.chunk.js.LICENSE.txt monitoring/static/js/6044.2de9962d.chunk.js.LICENSE.txt
        monitoring/static/js/6142.b2452554.chunk.js monitoring/static/js/6142.b2452554.chunk.js
        monitoring/static/js/6142.b2452554.chunk.js.LICENSE.txt monitoring/static/js/6142.b2452554.chunk.js.LICENSE.txt
        monitoring/static/js/619.93f30f25.chunk.js monitoring/static/js/619.93f30f25.chunk.js
        monitoring/static/js/619.93f30f25.chunk.js.LICENSE.txt monitoring/static/js/619.93f30f25.chunk.js.LICENSE.txt
        monitoring/static/js/6230.8e64216a.chunk.js monitoring/static/js/6230.8e64216a.chunk.js
        monitoring/static/js/6230.8e64216a.chunk.js.LICENSE.txt monitoring/static/js/6230.8e64216a.chunk.js.LICENSE.txt
        monitoring/static/js/6289.51f8741e.chunk.js monitoring/static/js/6289.51f8741e.chunk.js
        monitoring/static/js/6289.51f8741e.chunk.js.LICENSE.txt monitoring/static/js/6289.51f8741e.chunk.js.LICENSE.txt
        monitoring/static/js/6300.b0cf51a4.chunk.js monitoring/static/js/6300.b0cf51a4.chunk.js
        monitoring/static/js/6300.b0cf51a4.chunk.js.LICENSE.txt monitoring/static/js/6300.b0cf51a4.chunk.js.LICENSE.txt
        monitoring/static/js/6321.44989ff0.chunk.js monitoring/static/js/6321.44989ff0.chunk.js
        monitoring/static/js/6321.44989ff0.chunk.js.LICENSE.txt monitoring/static/js/6321.44989ff0.chunk.js.LICENSE.txt
        monitoring/static/js/6329.a83ab1f8.chunk.js monitoring/static/js/6329.a83ab1f8.chunk.js
        monitoring/static/js/6329.a83ab1f8.chunk.js.LICENSE.txt monitoring/static/js/6329.a83ab1f8.chunk.js.LICENSE.txt
        monitoring/static/js/6390.32571915.chunk.js monitoring/static/js/6390.32571915.chunk.js
        monitoring/static/js/6390.32571915.chunk.js.LICENSE.txt monitoring/static/js/6390.32571915.chunk.js.LICENSE.txt
        monitoring/static/js/6619.9e1de7a6.chunk.js monitoring/static/js/6619.9e1de7a6.chunk.js
        monitoring/static/js/6619.9e1de7a6.chunk.js.LICENSE.txt monitoring/static/js/6619.9e1de7a6.chunk.js.LICENSE.txt
        monitoring/static/js/6692.9322b59d.chunk.js monitoring/static/js/6692.9322b59d.chunk.js
        monitoring/static/js/6692.9322b59d.chunk.js.LICENSE.txt monitoring/static/js/6692.9322b59d.chunk.js.LICENSE.txt
        monitoring/static/js/6795.5a0da658.chunk.js monitoring/static/js/6795.5a0da658.chunk.js
        monitoring/static/js/6795.5a0da658.chunk.js.LICENSE.txt monitoring/static/js/6795.5a0da658.chunk.js.LICENSE.txt
        monitoring/static/js/6876.867b698c.chunk.js monitoring/static/js/6876.867b698c.chunk.js
        monitoring/static/js/6898.5580b941.chunk.js monitoring/static/js/6898.5580b941.chunk.js
        monitoring/static/js/6898.5580b941.chunk.js.LICENSE.txt monitoring/static/js/6898.5580b941.chunk.js.LICENSE.txt
        monitoring/static/js/7409.4408962b.chunk.js monitoring/static/js/7409.4408962b.chunk.js
        monitoring/static/js/7520.d631f383.chunk.js monitoring/static/js/7520.d631f383.chunk.js
        monitoring/static/js/7520.d631f383.chunk.js.LICENSE.txt monitoring/static/js/7520.d631f383.chunk.js.LICENSE.txt
        monitoring/static/js/7529.cb72fb82.chunk.js monitoring/static/js/7529.cb72fb82.chunk.js
        monitoring/static/js/7529.cb72fb82.chunk.js.LICENSE.txt monitoring/static/js/7529.cb72fb82.chunk.js.LICENSE.txt
        monitoring/static/js/7539.05a90213.chunk.js monitoring/static/js/7539.05a90213.chunk.js
        monitoring/static/js/7543.9bf9dc54.chunk.js monitoring/static/js/7543.9bf9dc54.chunk.js
        monitoring/static/js/7543.9bf9dc54.chunk.js.LICENSE.txt monitoring/static/js/7543.9bf9dc54.chunk.js.LICENSE.txt
        monitoring/static/js/7554.23b49cda.chunk.js monitoring/static/js/7554.23b49cda.chunk.js
        monitoring/static/js/7554.23b49cda.chunk.js.LICENSE.txt monitoring/static/js/7554.23b49cda.chunk.js.LICENSE.txt
        monitoring/static/js/7645.ba8540ee.chunk.js monitoring/static/js/7645.ba8540ee.chunk.js
        monitoring/static/js/785.d2eae69c.chunk.js monitoring/static/js/785.d2eae69c.chunk.js
        monitoring/static/js/785.d2eae69c.chunk.js.LICENSE.txt monitoring/static/js/785.d2eae69c.chunk.js.LICENSE.txt
        monitoring/static/js/8065.31ab70e1.chunk.js monitoring/static/js/8065.31ab70e1.chunk.js
        monitoring/static/js/8065.31ab70e1.chunk.js.LICENSE.txt monitoring/static/js/8065.31ab70e1.chunk.js.LICENSE.txt
        monitoring/static/js/8450.baf3a89d.chunk.js monitoring/static/js/8450.baf3a89d.chunk.js
        monitoring/static/js/8450.baf3a89d.chunk.js.LICENSE.txt monitoring/static/js/8450.baf3a89d.chunk.js.LICENSE.txt
        monitoring/static/js/86.ad271bdc.chunk.js monitoring/static/js/86.ad271bdc.chunk.js
        monitoring/static/js/86.ad271bdc.chunk.js.LICENSE.txt monitoring/static/js/86.ad271bdc.chunk.js.LICENSE.txt
        monitoring/static/js/8622.49f3054c.chunk.js monitoring/static/js/8622.49f3054c.chunk.js
        monitoring/static/js/8791.b209de42.chunk.js monitoring/static/js/8791.b209de42.chunk.js
        monitoring/static/js/8791.b209de42.chunk.js.LICENSE.txt monitoring/static/js/8791.b209de42.chunk.js.LICENSE.txt
        monitoring/static/js/8797.f8f0ce13.chunk.js monitoring/static/js/8797.f8f0ce13.chunk.js
        monitoring/static/js/8797.f8f0ce13.chunk.js.LICENSE.txt monitoring/static/js/8797.f8f0ce13.chunk.js.LICENSE.txt
        monitoring/static/js/8905.b8a9fd91.chunk.js monitoring/static/js/8905.b8a9fd91.chunk.js
        monitoring/static/js/8905.b8a9fd91.chunk.js.LICENSE.txt monitoring/static/js/8905.b8a9fd91.chunk.js.LICENSE.txt
        monitoring/static/js/9173.dfde586d.chunk.js monitoring/static/js/9173.dfde586d.chunk.js
        monitoring/static/js/9173.dfde586d.chunk.js.LICENSE.txt monitoring/static/js/9173.dfde586d.chunk.js.LICENSE.txt
        monitoring/static/js/919.53e04507.chunk.js monitoring/static/js/919.53e04507.chunk.js
        monitoring/static/js/919.53e04507.chunk.js.LICENSE.txt monitoring/static/js/919.53e04507.chunk.js.LICENSE.txt
        monitoring/static/js/924.382f18b1.chunk.js monitoring/static/js/924.382f18b1.chunk.js
        monitoring/static/js/924.382f18b1.chunk.js.LICENSE.txt monitoring/static/js/924.382f18b1.chunk.js.LICENSE.txt
        monitoring/static/js/9319.40f9e46a.chunk.js monitoring/static/js/9319.40f9e46a.chunk.js
        monitoring/static/js/9319.40f9e46a.chunk.js.LICENSE.txt monitoring/static/js/9319.40f9e46a.chunk.js.LICENSE.txt
        monitoring/static/js/9371.0327dbd7.chunk.js monitoring/static/js/9371.0327dbd7.chunk.js
        monitoring/static/js/9371.0327dbd7.chunk.js.LICENSE.txt monitoring/static/js/9371.0327dbd7.chunk.js.LICENSE.txt
        monitoring/static/js/9433.7ce648d0.chunk.js monitoring/static/js/9433.7ce648d0.chunk.js
        monitoring/static/js/9433.7ce648d0.chunk.js.LICENSE.txt monitoring/static/js/9433.7ce648d0.chunk.js.LICENSE.txt
        monitoring/static/js/9526.10bb1684.chunk.js monitoring/static/js/9526.10bb1684.chunk.js
        monitoring/static/js/9526.10bb1684.chunk.js.LICENSE.txt monitoring/static/js/9526.10bb1684.chunk.js.LICENSE.txt
        monitoring/static/js/9621.48073631.chunk.js monitoring/static/js/9621.48073631.chunk.js
        monitoring/static/js/9621.48073631.chunk.js.LICENSE.txt monitoring/static/js/9621.48073631.chunk.js.LICENSE.txt
        monitoring/static/js/9876.49756d3b.chunk.js monitoring/static/js/9876.49756d3b.chunk.js
        monitoring/static/js/9876.49756d3b.chunk.js.LICENSE.txt monitoring/static/js/9876.49756d3b.chunk.js.LICENSE.txt
        monitoring/static/js/9923.270f0a19.chunk.js monitoring/static/js/9923.270f0a19.chunk.js
        monitoring/static/js/9923.270f0a19.chunk.js.LICENSE.txt monitoring/static/js/9923.270f0a19.chunk.js.LICENSE.txt
        monitoring/static/js/main.f3726f74.js monitoring/static/js/main.f3726f74.js
        monitoring/static/js/main.f3726f74.js.LICENSE.txt monitoring/static/js/main.f3726f74.js.LICENSE.txt
        monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg monitoring/static/media/403.271ae19f0d1101a2c67a904146bbd4d3.svg
        monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg monitoring/static/media/403.6367e52f9464706633f52a2488a41958.svg
        monitoring/static/media/codicon.56dba998166ea5a0ca7f.ttf monitoring/static/media/codicon.56dba998166ea5a0ca7f.ttf
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
    ydb/core/external_sources
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
