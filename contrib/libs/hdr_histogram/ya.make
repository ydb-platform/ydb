LIBRARY()

LICENSE(
    BSD-2-Clause AND
    CC0-1.0
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(0.9.5)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/libs/hdr_histogram/src
)

SRCS(
    src/hdr_encoding.c
    src/hdr_interval_recorder.c
    src/hdr_histogram.c
    src/hdr_writer_reader_phaser.c
    src/hdr_time.c
    src/hdr_thread.c
)

PEERDIR(
    contrib/libs/zlib
)

END()
