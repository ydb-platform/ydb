LIBRARY()

SRCS(
    bbox.cpp
    geo.cpp
    point.cpp
    polygon.cpp
    load_save_helper.cpp
    size.cpp
    util.cpp
    window.cpp
)

END()

RECURSE_FOR_TESTS(
    ut 
    style
    )
