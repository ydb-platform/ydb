LIBRARY()

SRCS(
    name_service.cpp
    replicating.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/analysis/global
    yql/essentials/sql/v1/ide/completion/name/object/simple/static
    yql/essentials/sql/v1/ide/completion/name/service
)

END()
