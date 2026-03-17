from mammoth import html
from ..testing import assert_equal


def test_text_nodes_with_text_are_not_stripped():
    assert_equal(
        [html.text("H")],
        html.strip_empty([html.text("H")]))


def test_empty_text_nodes_are_stripped():
    assert_equal(
        [],
        html.strip_empty([html.text("")]))


def test_elements_with_non_empty_children_are_not_stripped():
    assert_equal(
        [html.element("p", {}, [html.text("H")])],
        html.strip_empty([html.element("p", {}, [html.text("H")])]))


def test_elements_with_no_children_are_stripped():
    assert_equal(
        [],
        html.strip_empty([html.element("p")]))


def test_elements_with_only_empty_children_are_stripped():
    assert_equal(
        [],
        html.strip_empty([html.element("p", {}, [html.text("")])]))


def test_empty_children_are_removed():
    assert_equal(
        html.strip_empty([html.element("ul", {}, [
            html.element("li", {}, [html.text("")]),
            html.element("li", {}, [html.text("H")]),
        ])]),

        [html.element("ul", {}, [
            html.element("li", {}, [html.text("H")])
        ])])


def test_self_closing_elements_are_never_empty():
    assert_equal(
        [html.element("br")],
        html.strip_empty([html.element("br")]))


def test_force_writes_are_never_empty():
    assert_equal(
        [html.force_write],
        html.strip_empty([html.force_write]))
