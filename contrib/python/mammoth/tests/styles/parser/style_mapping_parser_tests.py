from mammoth import html_paths, document_matchers, styles
from mammoth.styles.parser.style_mapping_parser import parse_style_mapping
from mammoth.styles.parser.tokeniser import tokenise
from mammoth.styles.parser.token_iterator import TokenIterator
from ...testing import assert_equal


def test_document_matcher_is_mapped_to_html_path_using_fat_arrow():
    assert_equal(
        styles.style(document_matchers.paragraph(), html_paths.path([html_paths.element(["h1"])])),
        read_style_mapping("p => h1")
    )


def read_style_mapping(string):
    return parse_style_mapping(TokenIterator(tokenise(string)))
