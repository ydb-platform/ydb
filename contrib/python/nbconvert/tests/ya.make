PY3TEST()

FORK_TESTS()

PEERDIR(
    contrib/python/nbconvert
    contrib/python/ipython
    contrib/python/jupyter-client
    contrib/python/tornado
)

DATA(
    arcadia/contrib/python/nbconvert/nbconvert
)

SRCDIR(contrib/python/nbconvert)

PY_SRCS(
    TOP_LEVEL
    nbconvert/exporters/tests/cheese.py
    nbconvert/tests/__init__.py
    nbconvert/tests/base.py
    nbconvert/tests/exporter_entrypoint/eptest.py
    nbconvert/tests/fake_exporters.py
    #nbconvert/tests/files/hello.py
    #nbconvert/tests/files/jupyter_nbconvert_config.py
    #nbconvert/tests/files/override.py
    nbconvert/tests/utils.py
)

TEST_SRCS(
    nbconvert/exporters/tests/__init__.py
    nbconvert/exporters/tests/base.py
    nbconvert/exporters/tests/test_asciidoc.py
    nbconvert/exporters/tests/test_export.py
    nbconvert/exporters/tests/test_exporter.py
    nbconvert/exporters/tests/test_html.py
    nbconvert/exporters/tests/test_latex.py
    nbconvert/exporters/tests/test_markdown.py
    nbconvert/exporters/tests/test_notebook.py
    nbconvert/exporters/tests/test_pdf.py
    nbconvert/exporters/tests/test_python.py
    nbconvert/exporters/tests/test_rst.py
    nbconvert/exporters/tests/test_script.py
    nbconvert/exporters/tests/test_slides.py
    nbconvert/exporters/tests/test_templateexporter.py
    nbconvert/filters/tests/__init__.py
    nbconvert/filters/tests/test_ansi.py
    nbconvert/filters/tests/test_citation.py
    nbconvert/filters/tests/test_datatypefilter.py
    nbconvert/filters/tests/test_highlight.py
    nbconvert/filters/tests/test_latex.py
    nbconvert/filters/tests/test_markdown.py
    nbconvert/filters/tests/test_metadata.py
    nbconvert/filters/tests/test_strings.py
    nbconvert/postprocessors/tests/__init__.py
    nbconvert/postprocessors/tests/test_serve.py
    nbconvert/preprocessors/tests/__init__.py
    nbconvert/preprocessors/tests/base.py
    nbconvert/preprocessors/tests/fake_kernelmanager.py
    nbconvert/preprocessors/tests/test_clearmetadata.py
    nbconvert/preprocessors/tests/test_clearoutput.py
    nbconvert/preprocessors/tests/test_coalescestreams.py
    nbconvert/preprocessors/tests/test_csshtmlheader.py
    #nbconvert/preprocessors/tests/test_execute.py
    nbconvert/preprocessors/tests/test_extractoutput.py
    nbconvert/preprocessors/tests/test_highlightmagics.py
    nbconvert/preprocessors/tests/test_latex.py
    nbconvert/preprocessors/tests/test_regexremove.py
    nbconvert/preprocessors/tests/test_sanitize.py
    nbconvert/preprocessors/tests/test_svg2pdf.py
    nbconvert/preprocessors/tests/test_tagremove.py
    nbconvert/tests/test_nbconvertapp.py
    nbconvert/utils/tests/__init__.py
    nbconvert/utils/tests/test_io.py
    nbconvert/utils/tests/test_pandoc.py
    nbconvert/utils/tests/test_version.py
    nbconvert/writers/tests/__init__.py
    nbconvert/writers/tests/test_debug.py
    nbconvert/writers/tests/test_files.py
    nbconvert/writers/tests/test_stdout.py
)

NO_LINT()

END()
