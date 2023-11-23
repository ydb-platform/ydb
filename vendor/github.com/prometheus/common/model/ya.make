GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    alert.go
    fingerprinting.go
    fnv.go
    labels.go
    labelset.go
    metric.go
    model.go
    signature.go
    silence.go
    time.go
    value.go
    value_float.go
    value_histogram.go
    value_type.go
)

GO_TEST_SRCS(
    alert_test.go
    fingerprinting_test.go
    labels_test.go
    labelset_test.go
    metric_test.go
    signature_test.go
    silence_test.go
    time_test.go
    value_float_test.go
    value_histogram_test.go
    value_test.go
)

END()

RECURSE(gotest)
