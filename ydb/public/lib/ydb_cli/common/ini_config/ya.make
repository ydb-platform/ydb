LIBRARY(ini_config)

SRCS(
    config.cpp
    ini.cpp
    value.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
