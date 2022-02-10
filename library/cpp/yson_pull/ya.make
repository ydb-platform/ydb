LIBRARY(yson_pull)

OWNER(borman)

SRCS(
    consumer.cpp
    event.cpp
    exceptions.cpp
    input.cpp
    output.cpp
    read_ops.cpp
    reader.cpp
    scalar.cpp
    writer.cpp
)

GENERATE_ENUM_SERIALIZATION(event.h)
 
GENERATE_ENUM_SERIALIZATION(scalar.h)

END()
