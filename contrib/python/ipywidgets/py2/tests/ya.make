PY2TEST()

NO_LINT()

PEERDIR(
    contrib/python/ipywidgets
    contrib/python/jsonschema
    contrib/python/tornado
    contrib/python/mock
)

SRCDIR(contrib/python/ipywidgets/py2)

PY_SRCS(
    TOP_LEVEL
    ipywidgets/widgets/tests/__init__.py
)

TEST_SRCS(
    ipywidgets/tests/test_embed.py
    ipywidgets/widgets/tests/test_docutils.py
    ipywidgets/widgets/tests/test_interaction.py
    ipywidgets/widgets/tests/test_link.py
    ipywidgets/widgets/tests/test_selectioncontainer.py
    ipywidgets/widgets/tests/test_send_state.py
    ipywidgets/widgets/tests/test_set_state.py
    # it depends on some traitlets modules which are not contribbed
    # ipywidgets/widgets/tests/test_traits.py
    ipywidgets/widgets/tests/test_widget.py
    ipywidgets/widgets/tests/test_widget_box.py
    ipywidgets/widgets/tests/test_widget_float.py
    ipywidgets/widgets/tests/test_widget_image.py
    ipywidgets/widgets/tests/test_widget_output.py
    ipywidgets/widgets/tests/test_widget_selection.py
    ipywidgets/widgets/tests/test_widget_string.py
    ipywidgets/widgets/tests/test_widget_templates.py
    ipywidgets/widgets/tests/test_widget_upload.py
    ipywidgets/widgets/tests/utils.py
)

DATA(
    arcadia/contrib/python/ipywidgets/py2/ipywidgets
)

RESOURCE_FILES(
    PREFIX contrib/python/ipywidgets/py2/
    ipywidgets/widgets/tests/data/jupyter-logo-transparent.png
)

END()
