LIBRARY()

SRCS(
    schema.cpp
    key.cpp
)

PEERDIR(
    yql/essentials/sql/v1/ide/completion/name/object/simple
    yql/essentials/sql/v1/ide/completion/name/cache
    yql/essentials/utils/meta
)

END()
