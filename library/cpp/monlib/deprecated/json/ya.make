LIBRARY()

# Deprecated writer of Solomon JSON format
# https://wiki.yandex-team.ru/solomon/api/dataformat/json
#
# This writer will be deleted soon, so please consider to use
# high level library library/cpp/monlib/encode which is decoupled from the
# particular format.

SRCS(
    writer.h
    writer.cpp
)

PEERDIR(
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(ut)
