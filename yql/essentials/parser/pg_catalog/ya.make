LIBRARY()

# need to avoid generated files
ENABLE(SKIP_YQL_STYLE_CPP)

RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_operator.dat pg_operator.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_proc.dat pg_proc.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_type.dat pg_type.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_cast.dat pg_cast.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_aggregate.dat pg_aggregate.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_opfamily.dat pg_opfamily.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_opclass.dat pg_opclass.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_amproc.dat pg_amproc.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_amop.dat pg_amop.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_am.dat pg_am.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_conversion.dat pg_conversion.dat)
RESOURCE(../pg_wrapper/postgresql/src/include/catalog/pg_language.dat pg_language.dat)
RESOURCE(../pg_wrapper/postgresql/src/backend/catalog/system_functions.sql system_functions.sql)

SRCS(
    catalog.cpp
)

PEERDIR(
    library/cpp/resource
    yql/essentials/public/issue
    library/cpp/digest/md5
    yql/essentials/parser/pg_catalog/proto
    yql/essentials/utils/log
)

END()

RECURSE_FOR_TESTS(
   ut
)

