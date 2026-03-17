PY3TEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/python/pypdfium2
    contrib/python/Pillow
    contrib/python/numpy
)

DATA(
    arcadia/contrib/python/pypdfium2/tests
)

TEST_SRCS(
    conftest.py
    test_attachments.py
    test_cli.py
    test_document.py
    test_misc.py
    test_nup.py
    test_opener.py
    test_page.py
    test_pageobjects.py
    test_rendering.py
    test_saving.py
    test_textpage.py
    test_toc.py
)

NO_LINT()

END()
