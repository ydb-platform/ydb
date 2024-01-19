LIBRARY()

SRCS(
    change_record.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

YQL_LAST_ABI_VERSION()

END()
