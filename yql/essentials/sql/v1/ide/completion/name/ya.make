LIBRARY()

PEERDIR(
    yql/essentials/sql/v1/ide/completion/core
    yql/essentials/sql/v1/ide/completion/name/service
)

END()

RECURSE(
    cache
    cluster
    object
    service
)
