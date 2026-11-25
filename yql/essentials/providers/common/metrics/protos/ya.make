PROTO_LIBRARY()

SRCS(
    metrics_registry.proto
)

EXCLUDE_TAGS(GO_PROTO)

PEERDIR_FROM_POLICY(
    ALLOW
    market/robotics/cv/cpp_inventory_app/bin
)

END()
