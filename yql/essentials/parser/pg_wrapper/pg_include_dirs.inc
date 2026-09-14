ADDINCL(
    contrib/libs/lz4
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/bootstrap
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/parser
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/replication
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/replication/logical
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/utils/adt
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/utils/misc
    yql/essentials/parser/pg_wrapper/postgresql/src/backend/utils/sort
    yql/essentials/parser/pg_wrapper/postgresql/src/common
    yql/essentials/parser/pg_wrapper/postgresql/src/include
    yql/essentials/parser/pg_wrapper/postgresql/src/port
)

IF (OS_WINDOWS)
    ADDINCL(
        yql/essentials/parser/pg_wrapper/postgresql/src/include
        yql/essentials/parser/pg_wrapper/postgresql/src/include/port/win32
        yql/essentials/parser/pg_wrapper/postgresql/src/include/port/win32_msvc
    )
ENDIF()
