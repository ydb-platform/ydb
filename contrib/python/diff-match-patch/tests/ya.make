PY3TEST()

SRCDIR(contrib/python/diff-match-patch/diff_match_patch/tests)

TEST_SRCS(
    diff_match_patch_test.py
)

NO_LINT()

PEERDIR(
    contrib/python/diff-match-patch
)

END()
