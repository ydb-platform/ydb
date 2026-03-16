LIBRARY(glog)

LICENSE(
    BSD-3-Clause AND
    CC-BY-4.0
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(0.3.3)

NO_UTIL()
NO_COMPILER_WARNINGS()

SRCS(
    sources/src/logging.cc
    sources/src/raw_logging.cc
    sources/src/vlog_is_on.cc
    sources/src/utilities.cc
    sources/src/demangle.cc
    sources/src/symbolize.cc
    sources/src/signalhandler.cc
)

PEERDIR(
    contrib/libs/gflags
)

ADDINCL(
    GLOBAL contrib/libs/glog/sources/src
)

END()
