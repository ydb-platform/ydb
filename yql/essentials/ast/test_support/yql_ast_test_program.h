#pragma once

#include <util/generic/strbuf.h>

namespace NYql {

inline constexpr TStringBuf TEST_PROGRAM =
    "(\n"
    "#comment\n"
    "(let mr_source (DataSource 'yamr 'cedar))\n"
    "(let x (Read! world mr_source (Key '('table (KeyString 'Input))) '('key 'value) '()))\n"
    "(let world (Left! x))\n"
    "(let table1 (Right! x))\n"
    "(let tresh (Int32 '100))\n"
    "(let table1low (Filter table1 (lambda '(item) (< (member item 'key) tresh))))\n"
    "(let mr_sink (DataSink 'yamr (quote cedar)))\n"
    "(let world (Write! world mr_sink (Key '('table (KeyString 'Output))) table1low '('('mode 'append))))\n"
    "(let world (Commit! world mr_sink))\n"
    "(return world)\n"
    ")";

} // namespace NYql
