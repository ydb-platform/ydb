LIBRARY()

SRCS(
    scan_diagnostics_actor.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/opentelemetry-proto
    ydb/core/base/generated
    ydb/core/control/lib/generated
    ydb/core/tx/columnshard/private_events
    ydb/library/aclib/protos
    ydb/library/actors/core
    yql/essentials/public/issue/protos
)

RESOURCE(
    scan-trace-viz.js scan-trace-viz.js
    viz-global.js viz-global.js
    plotly-2.35.2.min.js plotly-2.35.2.min.js
)

END()
