UNITTEST_FOR(library/cpp/monlib/encode/json)

SRCS(
    json_decoder_ut.cpp
    json_ut.cpp
)

RESOURCE(
    buffered_test.json /buffered_test.json
    buffered_ts_merge.json /buffered_ts_merge.json
    empty_series.json /empty_series.json
    expected.json /expected.json
    expected_buffered.json /expected_buffered.json
    expected_cloud.json /expected_cloud.json
    expected_cloud_buffered.json /expected_cloud_buffered.json
    merged.json /merged.json
    histogram_timeseries.json /histogram_timeseries.json
    histogram_value.json /histogram_value.json
    histogram_value_inf_before_bounds.json /histogram_value_inf_before_bounds.json
    int_gauge.json /int_gauge.json
    sensors.json /sensors.json
    metrics.json /metrics.json
    named_metrics.json /named_metrics.json
    test_decode_to_encode.json /test_decode_to_encode.json
    crash.json /crash.json
    hist_crash.json /hist_crash.json
    summary_value.json /summary_value.json
    summary_inf.json /summary_inf.json
    summary_timeseries.json /summary_timeseries.json
    log_histogram_value.json /log_histogram_value.json
    log_histogram_timeseries.json /log_histogram_timeseries.json
)

PEERDIR(
    library/cpp/json
    library/cpp/monlib/consumers
    library/cpp/monlib/encode/protobuf
    library/cpp/resource
)

END()
