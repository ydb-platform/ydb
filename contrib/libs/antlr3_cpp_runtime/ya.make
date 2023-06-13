LIBRARY()

# git repository: https://github.com/ibre5041/antlr3.git
# XXX fork of: https://github.com/antlr/antlr3.git
# directory: runtime/Cpp
# revision: a4d1928e03b2b3f74579e54a6211cd1d695001b9

VERSION(2016-03-31-a4d1928e03b2b3f74579e54a6211cd1d695001b9)

LICENSE(
    BSD-3-Clause AND
    Unicode-Mappings
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/antlr3_cpp_runtime/include
)

SRCS(
    antlr3.cpp
)

END()
