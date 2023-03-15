LIBRARY(dragonbox)

# git repository: https://github.com/ClickHouse-Extras/dragonbox.git
# fork of: https://github.com/jk-jeon/dragonbox.git
# revision: b2751c65c0592c0239aec3becd53d0ea2fde9329

VERSION(2020-12-14-b2751c65c0592c0239aec3becd53d0ea2fde9329)

LICENSE(
    "(BSL-1.0 OR Apache-2.0)" AND
    Apache-2.0 AND
    Apache-2.0 WITH LLVM-exception AND
    BSL-1.0 AND
    MIT
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/restricted/dragonbox/include
)

SRCS(
    source/dragonbox_to_chars.cpp
)

END()
