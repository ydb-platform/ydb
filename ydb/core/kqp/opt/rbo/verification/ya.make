PY3_LIBRARY()

PY_SRCS(
    rbo_verifier/__init__.py
    rbo_verifier/__main__.py
    rbo_verifier/cli.py
    rbo_verifier/decimal.py
    rbo_verifier/ir.py
    rbo_verifier/relation.py
    rbo_verifier/scalar.py
    rbo_verifier/smt.py
    rbo_verifier/sort_network.py
    rbo_verifier/stages.py
    rbo_verifier/string_order.py
    rbo_verifier/types.py
    rbo_verifier/verify.py
)

END()

RECURSE_FOR_TESTS(
    benchmark_ut
    bisect_ut
    confirmation_ut
    cpp_ut
    inspect_ut
    integration_ut
    prefix_capture/ut
    replay_ut
    ut
)

RECURSE(
    bin
    bisect_bin
    confirm_bin
    confirmation
    inspect_bin
    inspector
    prefix_capture
    prefix_capture/bin
    replay
    replay_bin
    tools
)
