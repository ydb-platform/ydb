LIBRARY()

PEERDIR(
    library/cpp/json
    library/cpp/yconf
)

SRCS(
    config_patcher.cpp
    unstrict_config.cpp
)

END()
