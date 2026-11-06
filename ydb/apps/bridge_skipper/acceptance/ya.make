RECURSE(lib)
# Browser acceptance requires a prepared bundle and is run explicitly.
RECURSE_FOR_TESTS(
    tests
    browser/provenance
)
