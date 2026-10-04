LIBRARY()

SRCS(resources.cpp)

RESOURCE(
    chart.js metric-chart/chart.js
    client.js metric-chart/client.js
    chart.css metric-chart/chart.css
)

PEERDIR(
    library/cpp/monlib/service/pages
    ydb/core/mon
)

END()
