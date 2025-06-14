LIBRARY()

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/sql/v1/complete/name/service
)

END()

RECURSE(
    cache
    cluster
    object
    service
)
