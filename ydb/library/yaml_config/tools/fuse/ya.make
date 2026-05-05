PROGRAM(yaml-config-fuse)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/yaml_config/public
    ydb/library/fyamlcpp
    library/cpp/getopt
)

END()
