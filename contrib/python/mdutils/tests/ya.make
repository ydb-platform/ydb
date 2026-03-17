PY3TEST()

PEERDIR(
    contrib/python/mdutils
)

NO_LINT()

TEST_SRCS(
    test_fileutils/test_fileutils.py
    test_mdutils.py
    test_tools/__init__.py
    test_tools/test_header.py
    test_tools/test_html.py
    test_tools/test_image.py
    test_tools/test_inline.py
    test_tools/test_mdcheckbox.py
    test_tools/test_mdlist.py
    test_tools/test_reference.py
    test_tools/test_table_of_contents.py
    test_tools/test_table.py
    test_tools/test_textutils.py
)


END()