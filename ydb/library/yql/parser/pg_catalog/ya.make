LIBRARY()

OWNER(g:yql)

RESOURCE(pg_operator.dat pg_operator.dat)
RESOURCE(pg_proc.dat pg_proc.dat)
RESOURCE(pg_type.dat pg_type.dat)

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
