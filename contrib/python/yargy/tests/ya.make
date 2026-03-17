PY3TEST()

PEERDIR(
    contrib/python/yargy
)

FORK_SUBTESTS()

TEST_SRCS(
		test_interpretation.py
		test_morph.py
		test_person.py
		test_pipeline.py
		test_predicate.py
		test_relations.py
		test_rule.py
		test_tagger.py
		test_tokenizer.py
)

NO_LINT()

END()
