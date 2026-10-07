LIBRARY()

SRCS(resources.cpp)

RESOURCE(
    allocation.js metric-chart/allocation.js
    chart.js metric-chart/chart.js
    chartkit.js metric-chart/chartkit.js
    chartkit.css metric-chart/chartkit.css
    client.js metric-chart/client.js
    chart.css metric-chart/chart.css
)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/mon
)

END()
