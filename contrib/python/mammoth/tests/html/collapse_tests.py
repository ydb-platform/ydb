from mammoth import html
from ..testing import assert_equal


def test_collapsing_does_nothing_to_single_text_node():
    assert_equal(
        html.collapse([html.text("Bluebells")]),
        [html.text("Bluebells")])


def test_consecutive_fresh_elements_are_not_collapsed():
    assert_equal(
        html.collapse([html.element("p"), html.element("p")]),
        [html.element("p"), html.element("p")])


def test_consecutive_collapsible_elements_are_collapsed_if_they_have_the_same_tag_and_attributes():
    assert_equal(
        [html.collapsible_element("p", {}, [html.text("One"), html.text("Two")])],
        html.collapse([
            html.collapsible_element("p", {}, [html.text("One")]),
            html.collapsible_element("p", {}, [html.text("Two")])
        ]))


def test_elements_with_different_tag_names_are_not_collapsed():
    assert_equal(
        [
            html.collapsible_element("p", {}, [html.text("One")]),
            html.collapsible_element("div", {}, [html.text("Two")])
        ],

        html.collapse([
            html.collapsible_element("p", {}, [html.text("One")]),
            html.collapsible_element("div", {}, [html.text("Two")])
        ]))


def test_elements_with_different_attributes_are_not_collapsed():
    assert_equal(
        [
            html.collapsible_element("p", {"id": "a"}, [html.text("One")]),
            html.collapsible_element("p", {}, [html.text("Two")])
        ],

        html.collapse([
            html.collapsible_element("p", {"id": "a"}, [html.text("One")]),
            html.collapsible_element("p", {}, [html.text("Two")])
        ]))


def test_children_of_collapsed_element_can_collapse_with_children_of_previous_element():
    assert_equal(
        [
            html.collapsible_element("blockquote", {}, [
                html.collapsible_element("p", {}, [
                    html.text("One"),
                    html.text("Two")
                ])
            ]),
        ],

        html.collapse([
            html.collapsible_element("blockquote", {}, [
                html.collapsible_element("p", {}, [html.text("One")])
            ]),
            html.collapsible_element("blockquote", {}, [
                html.collapsible_element("p", {}, [html.text("Two")])
            ]),
        ]))


def test_collapsible_element_can_collapse_into_previous_fresh_element():
    assert_equal(
        [html.element("p", {}, [html.text("One"), html.text("Two")])],
        html.collapse([
            html.element("p", {}, [html.text("One")]),
            html.collapsible_element("p", {}, [html.text("Two")])
        ]))


def test_element_with_choice_of_tag_names_can_collapse_into_previous_element_if_it_has_one_of_those_tag_names_as_its_main_tag_name():
    assert_equal(
        [html.collapsible_element(["ol"])],
        html.collapse([
            html.collapsible_element("ol"),
            html.collapsible_element(["ul", "ol"])
        ]))

    assert_equal(
        [
            html.collapsible_element(["ul", "ol"]),
            html.collapsible_element("ol")
        ],
        html.collapse([
            html.collapsible_element(["ul", "ol"]),
            html.collapsible_element("ol")
        ]))


def test_when_separator_is_present_then_separator_is_prepended_to_collapsed_element():
    assert_equal(
        [
            html.element("pre", collapsible=False, children=[
                html.text("Hello"),
                html.text("\n"),
                html.text(" the"),
                html.text("re")
            ])
        ],
        html.collapse([
            html.element("pre", collapsible=False, children=[html.text("Hello")]),
            html.element("pre", collapsible=True, separator="\n", children=[html.text(" the"), html.text("re")]),
        ]),
    )
