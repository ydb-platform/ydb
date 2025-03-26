GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.0.0-20240822170219-fc7c04adadcd)

SRCS(
    launch_stage.pb.go
)

END()

RECURSE(
    annotations
    configchange
    distribution
    error_reason
    expr
    httpbody
    label
    metric
    monitoredres
    serviceconfig
    visibility
)
