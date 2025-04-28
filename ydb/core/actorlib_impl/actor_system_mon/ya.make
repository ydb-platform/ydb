LIBRARY()

SRCS(
    actor_system_mon.cpp
    actor_system_mon.h
    actor_system_json.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/lwtrace/mon
    ydb/core/mon
)

RESOURCE(
    static/index.html actor_system_mon/static/index.html
    static/app.js actor_system_mon/static/app.js
    static/style.css actor_system_mon/static/style.css

    static/js/charts.js actor_system_mon/static/js/charts.js
    static/js/data.js actor_system_mon/static/js/data.js
    static/js/renderUtils.js actor_system_mon/static/js/renderUtils.js
    static/js/ui.js actor_system_mon/static/js/ui.js
    static/js/utils.js actor_system_mon/static/js/utils.js
)

YQL_LAST_ABI_VERSION()

END()
