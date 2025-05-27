UNITTEST_FOR(ydb/core/kqp)

SIZE(MEDIUM)

SRCS(
    view_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    yql/essentials/sql
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/sql/v1/lexer/antlr3
    yql/essentials/sql/v1/lexer/antlr3_ansi
    yql/essentials/sql/v1/proto_parser/antlr3
    yql/essentials/sql/v1/proto_parser/antlr3_ansi
    yql/essentials/utils/log

    ydb/core/testlib/basics/default
)

DATA(arcadia/ydb/core/kqp/ut/view/input)

YQL_LAST_ABI_VERSION()

END()
