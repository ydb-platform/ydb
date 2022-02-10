RECURSE_FOR_TESTS( 
    ut 
) 

LIBRARY()

OWNER(
    svc
    g:kikimr
)

SRCS(
    bill_record.cpp
    bill_record.h
    metering.cpp
    metering.h
    time_grid.h
)

GENERATE_ENUM_SERIALIZATION(bill_record.h) 

PEERDIR(
    library/cpp/actors/core
    library/cpp/json
    library/cpp/logger
    ydb/core/base 
)

RESOURCE(
    ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt 
)

END()
