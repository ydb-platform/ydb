
PY3_PROGRAM(tz_gen)

OWNER(g:yql)

SRCDIR(ydb/library/yql/public/udf/tz) 

PY_SRCS(
    TOP_LEVEL
    update.py
)

PY_MAIN(update)

END()


