LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/cluster
    yql/essentials/sql/v1/complete/name/service
    library/cpp/case_insensitive_string
)

END()
