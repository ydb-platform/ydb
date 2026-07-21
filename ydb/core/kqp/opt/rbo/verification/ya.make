PY3_LIBRARY()

PY_SRCS(
    rbo_verifier/__init__.py
    rbo_verifier/__main__.py
    rbo_verifier/cli.py
    rbo_verifier/ir.py
    rbo_verifier/relation.py
    rbo_verifier/scalar.py
    rbo_verifier/smt.py
    rbo_verifier/verify.py
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    bin
)
