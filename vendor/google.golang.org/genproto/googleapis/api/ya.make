GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.0.0-20260401024825-9d38bb4040a9)

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
