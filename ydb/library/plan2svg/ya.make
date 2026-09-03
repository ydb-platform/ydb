LIBRARY()

SRCS(
    config.cpp
    format.cpp
    layout.cpp
    loader.cpp
    metrics.cpp
    parse.cpp
    plan2svg.cpp
    render.cpp
    svg.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/resource
)

RESOURCE(
    assets/icons.svg plan2svg/icons.svg
    assets/plan2svg.js plan2svg/plan2svg.js
)

END()

RECURSE_FOR_TESTS(
    ut
)
