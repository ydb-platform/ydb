PY3_LIBRARY()

PY_SRCS(
    rbo_verifier/__init__.py
    rbo_verifier/__main__.py
    rbo_verifier/cli.py
    rbo_verifier/ir.py
    rbo_verifier/relation.py
    rbo_verifier/scalar.py
    rbo_verifier/smt.py
    rbo_verifier/stages.py
    rbo_verifier/types.py
    rbo_verifier/verify.py
)

END()

RECURSE_FOR_TESTS(
    benchmark_ut
    bisect_ut
    cpp_ut
    inspect_ut
    integration_ut
    replay_ut
    ut
)

RECURSE(
    bin
    bisect_bin
    inspect_bin
    inspector
    replay
    replay_bin
    tools
)
