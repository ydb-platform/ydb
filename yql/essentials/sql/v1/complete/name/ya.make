LIBRARY()

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/sql/v1/complete/name/service
)

END()

RECURSE(
    cluster
    object
    service
)
