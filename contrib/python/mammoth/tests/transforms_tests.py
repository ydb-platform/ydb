import cobble

from mammoth import documents, transforms
from mammoth.transforms import get_descendants, get_descendants_of_type, _each_element
from .testing import assert_equal


class ParagraphTests(object):
    def test_paragraph_is_transformed(self):
        paragraph = documents.paragraph(children=[])
        result = transforms.paragraph(lambda _: documents.tab())(paragraph)
        assert_equal(documents.tab(), result)

    def test_non_paragraph_elements_are_not_transformed(self):
        run = documents.run(children=[])
        result = transforms.paragraph(lambda _: documents.tab())(run)
        assert_equal(documents.run(children=[]), result)


class RunTests(object):
    def test_run_is_transformed(self):
        run = documents.run(children=[])
        result = transforms.run(lambda _: documents.tab())(run)
        assert_equal(documents.tab(), result)

    def test_non_paragraph_elements_are_not_transformed(self):
        paragraph = documents.paragraph(children=[])
        result = transforms.run(lambda _: documents.tab())(paragraph)
        assert_equal(documents.paragraph(children=[]), result)


class EachElementTests(object):
    def test_all_descendants_are_transformed(self):
        @cobble.data
        class Count(documents.HasChildren):
            count = cobble.field()

        root = Count(count=None, children=[
            Count(count=None, children=[
                Count(count=None, children=[]),
            ]),
        ])

        current_count = [0]
        def set_count(node):
            current_count[0] += 1
            return node.copy(count=current_count[0])

        result = _each_element(set_count)(root)

        assert_equal(Count(count=3, children=[
            Count(count=2, children=[
                Count(count=1, children=[]),
            ]),
        ]), result)


class GetDescendantsTests(object):
    def test_returns_nothing_if_element_type_has_no_children(self):
        assert_equal([], get_descendants(documents.tab()))

    def test_returns_nothing_if_element_has_empty_children(self):
        assert_equal([], get_descendants(documents.paragraph(children=[])))

    def test_includes_children(self):
        children = [documents.text("child 1"), documents.text("child 2")]
        element = documents.paragraph(children=children)
        assert_equal(children, get_descendants(element))

    def test_includes_indirect_descendants(self):
        grandchild = documents.text("grandchild")
        child = documents.run(children=[grandchild])
        element = documents.paragraph(children=[child])
        assert_equal([grandchild, child], get_descendants(element))


class GetDescendantsOfTypeTests(object):
    def test_filters_descendants_to_type(self):
        tab = documents.tab()
        run = documents.run(children=[])
        element = documents.paragraph(children=[tab, run])
        assert_equal([run], get_descendants_of_type(element, documents.Run))
