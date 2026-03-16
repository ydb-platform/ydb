from parsel.utils import shorten, extract_regex

from pytest import mark, raises
import six


@mark.parametrize(
    'width,expected',
    (
        (-1, ValueError),
        (0, u''),
        (1, u'.'),
        (2, u'..'),
        (3, u'...'),
        (4, u'f...'),
        (5, u'fo...'),
        (6, u'foobar'),
        (7, u'foobar'),
    )
)
def test_shorten(width, expected):
    if isinstance(expected, six.string_types):
        assert shorten(u'foobar', width) == expected
    else:
        with raises(expected):
            shorten(u'foobar', width)


@mark.parametrize('regex, text, replace_entities, expected', (
    [r'(?P<month>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)', 'October  25, 2019', True, ['October', '25', '2019']],
    [r'(?P<month>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)', 'October  25 2019', True, ['October', '25', '2019']],
    [r'(?P<extract>\w+)\s*(?P<day>\d+)\s*\,?\s*(?P<year>\d+)', 'October  25 2019', True, ['October']],
    [r'\w+\s*\d+\s*\,?\s*\d+', 'October  25 2019', True, ['October  25 2019']],
    [r'^.*$', '&quot;sometext&quot; &amp; &quot;moretext&quot;', True, ['"sometext" &amp; "moretext"']],
    [r'^.*$', '&quot;sometext&quot; &amp; &quot;moretext&quot;', False, ['&quot;sometext&quot; &amp; &quot;moretext&quot;']],
))
def test_extract_regex(regex, text, replace_entities, expected):
    assert extract_regex(regex, text, replace_entities) == expected
