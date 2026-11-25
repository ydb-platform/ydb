LIBRARY()

SRCS(
    yarchive.cpp
    yarchive.h
    directory_models_archive_reader.cpp
    directory_models_archive_reader.h
    models_archive_reader.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/libs/lua
)

END()

RECURSE_FOR_TESTS(
    ut
)
