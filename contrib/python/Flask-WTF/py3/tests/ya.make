PY3TEST()

PEERDIR(
    contrib/python/Flask
    contrib/python/Flask-WTF
)

TEST_SRCS(
    conftest.py
    test_csrf_extension.py
    test_csrf_form.py
    test_file.py
    test_form.py
    test_i18n.py
    test_recaptcha.py
)

NO_LINT()

END()
