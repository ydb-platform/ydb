PY3TEST()

PEERDIR(
    contrib/python/pamqp
)

TEST_SRCS(
    test_command_argument_errors.py
    test_commands.py
    test_decoding.py
    test_encode_decode.py
    test_encoding.py
    test_frame_marshaling.py
    test_frame_unmarshaling.py
    test_frame_unmarshaling_errors.py
    test_tag_uri_scheme.py
)

END()
