LIBRARY()

SRCS(
    cpp/optimizer_trace.cpp
    cpp/optimizer_trace_assets.cpp
    cpp/optimizer_trace_output.cpp
)

CFLAGS(
    -DOPTIMIZER_TRACE_ENABLE_BROTLI
)

RUN_PYTHON3(
    ${ARCADIA_ROOT}/build/scripts/cat.py
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/vendor/graphlib/graphlib.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/vendor/dagre/dagre.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/00_trace_worker_protocol.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/01_trace_data.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/02_trace_inspector.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/03_trace_state.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/04_core_services.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/05_trace_search_worker.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/06_icons.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/07_action_context.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/08_actions.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/09_info_target_controller.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/10_info_graph_model.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/11_action_events.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/12_runtime.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/13_layout_model.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/14_virtual_rows.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/15_trace_viewport_anchor.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/16_html_shell.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/17_plan_tree_rendering.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/18_rule_panels_lazy.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/19_fullscreen_overlay.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/20_display_navigation_controls.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/21_search.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/22_diff_ui.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/23_tree_diff.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/24_diff_renderer.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/25_resize.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/26_wheel_scroll.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/27_runtime_reset.js
        ${ARCADIA_ROOT}/ydb/core/kqp/opt/rbo/html_log/js/28_bootstrap.js
    IN
        js/vendor/graphlib/graphlib.js
        js/vendor/dagre/dagre.js
        js/00_trace_worker_protocol.js
        js/01_trace_data.js
        js/02_trace_inspector.js
        js/03_trace_state.js
        js/04_core_services.js
        js/05_trace_search_worker.js
        js/06_icons.js
        js/07_action_context.js
        js/08_actions.js
        js/09_info_target_controller.js
        js/10_info_graph_model.js
        js/11_action_events.js
        js/12_runtime.js
        js/13_layout_model.js
        js/14_virtual_rows.js
        js/15_trace_viewport_anchor.js
        js/16_html_shell.js
        js/17_plan_tree_rendering.js
        js/18_rule_panels_lazy.js
        js/19_fullscreen_overlay.js
        js/20_display_navigation_controls.js
        js/21_search.js
        js/22_diff_ui.js
        js/23_tree_diff.js
        js/24_diff_renderer.js
        js/25_resize.js
        js/26_wheel_scroll.js
        js/27_runtime_reset.js
        js/28_bootstrap.js
    STDOUT optimizer_trace.js
)

RESOURCE(
    ${BINDIR}/optimizer_trace.js /ydb/core/kqp/opt/rbo/optimizer_trace.js
    js/trace_view.css /ydb/core/kqp/opt/rbo/optimizer_trace.css
    js/vendor/brotli_dec_wasm/brotli_decoder.js /ydb/core/kqp/opt/rbo/brotli_decoder.js
    js/vendor/brotli_dec_wasm/brotli_dec_wasm_bg.wasm /ydb/core/kqp/opt/rbo/brotli_dec_wasm_bg.wasm
    js/vendor/brotli_dec_wasm/LICENSE-MIT /ydb/core/kqp/opt/rbo/brotli_decoder_LICENSE-MIT
)

PEERDIR(
    contrib/libs/brotli/c/enc
    library/cpp/http/misc
    library/cpp/http/server
    library/cpp/resource
)

YQL_LAST_ABI_VERSION()

END()
