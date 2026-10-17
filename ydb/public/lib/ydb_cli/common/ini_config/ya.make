LIBRARY(ini_config)

# gn: move into parent

SRCS(
    config.cpp
    ini.cpp
    value.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
