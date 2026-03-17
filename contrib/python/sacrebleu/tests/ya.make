PY3TEST()

PEERDIR(
    contrib/python/sacrebleu
)

TEST_SRCS(
    test_api.py
    test_bleu.py
    test_chrf.py
    test_ter.py
    test_tokenizer_ter.py
)

NO_LINT()

END()
