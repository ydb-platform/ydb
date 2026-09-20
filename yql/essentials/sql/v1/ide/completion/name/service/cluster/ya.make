LIBRARY()

SRCS(
    name_service.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/name/cluster
    yql/essentials/sql/v1/ide/completion/name/service
    library/cpp/case_insensitive_string
)

END()
