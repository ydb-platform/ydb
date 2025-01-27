LIBRARY()

RESOURCE(data/pg_operator.dat pg_operator.dat)
RESOURCE(data/pg_proc.dat pg_proc.dat)
RESOURCE(data/pg_type.dat pg_type.dat)
RESOURCE(data/pg_cast.dat pg_cast.dat)
RESOURCE(data/pg_aggregate.dat pg_aggregate.dat)
RESOURCE(data/pg_opfamily.dat pg_opfamily.dat)
RESOURCE(data/pg_opclass.dat pg_opclass.dat)
RESOURCE(data/pg_amproc.dat pg_amproc.dat)
RESOURCE(data/pg_amop.dat pg_amop.dat)
RESOURCE(data/pg_am.dat pg_am.dat)
RESOURCE(data/pg_conversion.dat pg_conversion.dat)
RESOURCE(data/pg_language.dat pg_language.dat)
RESOURCE(data/system_functions.sql system_functions.sql)

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

