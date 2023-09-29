PY3TEST()

TEST_SRCS(
    conftest.py
    test_arguments.py
    test_basic.py
    test_chain.py
    test_command_decorators.py
    test_commands.py
    test_compat.py
    test_context.py
    test_custom_classes.py
    test_defaults.py
    test_formatting.py
    test_imports.py
    test_info_dict.py
    test_normalization.py
    test_options.py
    test_parser.py
    test_shell_completion.py
    test_termui.py
    test_testing.py
    test_types.py
    test_utils.py
)

PEERDIR(
    contrib/python/click
)

NO_LINT()

END()
