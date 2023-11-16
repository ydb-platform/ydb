GO_LIBRARY()

LICENSE(MIT)

SRCS(
    buffered_write_syncer.go
    clock.go
    console_encoder.go
    core.go
    doc.go
    encoder.go
    entry.go
    error.go
    field.go
    hook.go
    increase_level.go
    json_encoder.go
    lazy_with.go
    level.go
    level_strings.go
    marshaler.go
    memory_encoder.go
    reflected_encoder.go
    sampler.go
    tee.go
    write_syncer.go
)

GO_TEST_SRCS(
    buffered_write_syncer_bench_test.go
    buffered_write_syncer_test.go
    clock_test.go
    entry_test.go
    json_encoder_impl_test.go
    leak_test.go
    level_strings_test.go
    level_test.go
    memory_encoder_test.go
    write_syncer_bench_test.go
    write_syncer_test.go
)

GO_XTEST_SRCS(
    console_encoder_bench_test.go
    console_encoder_test.go
    core_test.go
    encoder_test.go
    error_test.go
    # field_test.go
    # hook_test.go
    # increase_level_test.go
    json_encoder_bench_test.go
    # json_encoder_test.go
    sampler_bench_test.go
    # sampler_test.go
    #tee_logger_bench_test.go
    # tee_test.go
)

GO_SKIP_TESTS(
    entry_ext_test.go
    lazy_with_test.go
)

END()

RECURSE(
    gotest
)
