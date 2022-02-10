DATA(
    arcadia/ydb/tests/supp
)

ENV(YDB_UBSAN_OPTIONS=ydb/tests/supp/ubsan_default.txt)

DEPENDS(
    ydb/tests/supp
)

USE_RECIPE(
    ydb/tests/supp/ydb_supp
)
