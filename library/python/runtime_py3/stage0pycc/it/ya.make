PY3TEST()

EXPLICIT_DATA()
SIZE(MEDIUM)

TEST_SRCS(test_command_file_args.py)

DEPENDS(library/python/runtime_py3/stage0pycc)

END()
