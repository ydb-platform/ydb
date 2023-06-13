LIBRARY()

RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_operator.dat pg_operator.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_proc.dat pg_proc.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_type.dat pg_type.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_cast.dat pg_cast.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_aggregate.dat pg_aggregate.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_opfamily.dat pg_opfamily.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_opclass.dat pg_opclass.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_amproc.dat pg_amproc.dat)
RESOURCE(../../../../../contrib/libs/postgresql/src/include/catalog/pg_amop.dat pg_amop.dat)

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
