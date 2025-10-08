LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    yql_user_data.cpp
)

GENERATE_ENUM_SERIALIZATION(yql_user_data.h)

END()
