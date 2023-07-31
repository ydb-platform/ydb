LIBRARY()

PEERDIR(
    library/cpp/reverse_geocoder/library
    library/cpp/reverse_geocoder/proto
    library/cpp/digest/crc32c
)

SRCS(
    area_box.cpp
    bbox.cpp
    common.cpp
    edge.cpp
    reverse_geocoder.cpp
    kv.cpp
    location.cpp
    part.cpp
    point.cpp
    polygon.cpp
    region.cpp
    geo_data/debug.cpp
    geo_data/def.cpp
    geo_data/geo_data.cpp
    geo_data/map.cpp
    geo_data/proxy.cpp
)

END()
