LIBRARY()

OWNER(g:yql)

RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_operator.dat pg_operator.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_proc.dat pg_proc.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_type.dat pg_type.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_cast.dat pg_cast.dat)

SRCS(
    catalog.cpp
)

PEERDIR(
    library/cpp/resource
)

END()

RECURSE_FOR_TESTS(
    ut
)
