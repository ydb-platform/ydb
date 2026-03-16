import pytest

import pydash as _


@pytest.mark.mypy_testing
def test_mypy_camel_case() -> None:
    reveal_type(_.camel_case('FOO BAR_bAz'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_capitalize() -> None:
    reveal_type(_.capitalize('once upon a TIME'))  # R: builtins.str
    reveal_type(_.capitalize('once upon a TIME', False))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_chars() -> None:
    reveal_type(_.chars('onetwo'))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_chop() -> None:
    reveal_type(_.chop('abcdefg', 3))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_chop_right() -> None:
    reveal_type(_.chop_right('abcdefg', 3))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_clean() -> None:
    reveal_type(_.clean('a  b   c    d'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_count_substr() -> None:
    reveal_type(_.count_substr('aabbccddaabbccdd', 'bc'))  # R: builtins.int


@pytest.mark.mypy_testing
def test_mypy_deburr() -> None:
    reveal_type(_.deburr('déjà vu'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_decapitalize() -> None:
    reveal_type(_.decapitalize('FOO BAR'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_ends_with() -> None:
    reveal_type(_.ends_with('abc def', 'def'))  # R: builtins.bool
    reveal_type(_.ends_with('abc def', 4))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_ensure_ends_with() -> None:
    reveal_type(_.ensure_ends_with('foo bar', '!'))  # R: builtins.str
    reveal_type(_.ensure_ends_with('foo bar!', '!'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_ensure_starts_with() -> None:
    reveal_type(_.ensure_starts_with('foo bar', 'Oh my! '))  # R: builtins.str
    reveal_type(_.ensure_starts_with('Oh my! foo bar', 'Oh my! '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_escape() -> None:
    reveal_type(_.escape('"1 > 2 && 3 < 4"'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_escape_reg_exp() -> None:
    reveal_type(_.escape_reg_exp('[()]'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_has_substr() -> None:
    reveal_type(_.has_substr('abcdef', 'bc'))  # R: builtins.bool
    reveal_type(_.has_substr('abcdef', 'bb'))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_human_case() -> None:
    reveal_type(_.human_case('abc-def_hij lmn'))  # R: builtins.str
    reveal_type(_.human_case('user_id'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_insert_substr() -> None:
    reveal_type(_.insert_substr('abcdef', 3, '--'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_join() -> None:
    reveal_type(_.join(['a', 'b', 'c']))  # R: builtins.str
    reveal_type(_.join([1, 2, 3, 4], '&'))  # R: builtins.str
    reveal_type(_.join('abcdef', '-'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_kebab_case() -> None:
    reveal_type(_.kebab_case('a b c_d-e!f'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_lines() -> None:
    reveal_type(_.lines('a\nb\r\nc'))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_lower_case() -> None:
    reveal_type(_.lower_case('fooBar'))  # R: builtins.str
    reveal_type(_.lower_case('--foo-Bar--'))  # R: builtins.str
    reveal_type(_.lower_case('/?*Foo10/;"B*Ar'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_lower_first() -> None:
    reveal_type(_.lower_first('FRED'))  # R: builtins.str
    reveal_type(_.lower_first('Foo Bar'))  # R: builtins.str
    reveal_type(_.lower_first('1foobar'))  # R: builtins.str
    reveal_type(_.lower_first(';foobar'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_number_format() -> None:
    reveal_type(_.number_format(1234.5678))  # R: builtins.str
    reveal_type(_.number_format(1234.5678, 2, ',', '.'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_pad() -> None:
    reveal_type(_.pad('abc', 5))  # R: builtins.str
    reveal_type(_.pad('abc', 6, 'x'))  # R: builtins.str
    reveal_type(_.pad('abc', 5, '...'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_pad_end() -> None:
    reveal_type(_.pad_end('abc', 5))  # R: builtins.str
    reveal_type(_.pad_end('abc', 5, '.'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_pad_start() -> None:
    reveal_type(_.pad_start('abc', 5))  # R: builtins.str
    reveal_type(_.pad_start('abc', 5, '.'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_pascal_case() -> None:
    reveal_type(_.pascal_case('FOO BAR_bAz'))  # R: builtins.str
    reveal_type(_.pascal_case('FOO BAR_bAz', False))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_predecessor() -> None:
    reveal_type(_.predecessor('c'))  # R: builtins.str
    reveal_type(_.predecessor('C'))  # R: builtins.str
    reveal_type(_.predecessor('3'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_prune() -> None:
    reveal_type(_.prune('Fe fi fo fum', 5))  # R: builtins.str
    reveal_type(_.prune('Fe fi fo fum', 6))  # R: builtins.str
    reveal_type(_.prune('Fe fi fo fum', 7))  # R: builtins.str
    reveal_type(_.prune('Fe fi fo fum', 8, ',,,'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_quote() -> None:
    reveal_type(_.quote('To be or not to be'))  # R: builtins.str
    reveal_type(_.quote('To be or not to be', "'"))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_reg_exp_js_match() -> None:
    reveal_type(_.reg_exp_js_match('aaBBcc', '/bb/'))  # R: builtins.list[builtins.str]
    reveal_type(_.reg_exp_js_match('aaBBcc', '/bb/i'))  # R: builtins.list[builtins.str]
    reveal_type(_.reg_exp_js_match('aaBBccbb', '/bb/i'))  # R: builtins.list[builtins.str]
    reveal_type(_.reg_exp_js_match('aaBBccbb', '/bb/gi'))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_reg_exp_js_replace() -> None:
    reveal_type(_.reg_exp_js_replace('aaBBcc', '/bb/', 'X'))  # R: builtins.str
    reveal_type(_.reg_exp_js_replace('aaBBcc', '/bb/i', 'X'))  # R: builtins.str
    reveal_type(_.reg_exp_js_replace('aaBBccbb', '/bb/i', 'X'))  # R: builtins.str
    reveal_type(_.reg_exp_js_replace('aaBBccbb', '/bb/gi', 'X'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_reg_exp_replace() -> None:
    reveal_type(_.reg_exp_replace('aabbcc', 'b', 'X'))  # R: builtins.str
    reveal_type(_.reg_exp_replace('aabbcc', 'B', 'X', ignore_case=True))  # R: builtins.str
    reveal_type(_.reg_exp_replace('aabbcc', 'b', 'X', count=1))  # R: builtins.str
    reveal_type(_.reg_exp_replace('aabbcc', '[ab]', 'X'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_repeat() -> None:
    reveal_type(_.repeat('.', 5))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_replace() -> None:
    reveal_type(_.replace('aabbcc', 'b', 'X'))  # R: builtins.str
    reveal_type(_.replace('aabbcc', 'B', 'X', ignore_case=True))  # R: builtins.str
    reveal_type(_.replace('aabbcc', 'b', 'X', count=1))  # R: builtins.str
    reveal_type(_.replace('aabbcc', '[ab]', 'X'))  # R: builtins.str
    reveal_type(_.replace('aabbcc', '[ab]', 'X', escape=False))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_replace_end() -> None:
    reveal_type(_.replace_end('aabbcc', 'b', 'X'))  # R: builtins.str
    reveal_type(_.replace_end('aabbcc', 'c', 'X'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_replace_start() -> None:
    reveal_type(_.replace_start('aabbcc', 'b', 'X'))  # R: builtins.str
    reveal_type(_.replace_start('aabbcc', 'a', 'X'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_separator_case() -> None:
    reveal_type(_.separator_case('a!!b___c.d', '-'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_series_phrase() -> None:
    reveal_type(_.series_phrase(['apples', 'bananas', 'peaches']))  # R: builtins.str
    reveal_type(_.series_phrase(['apples', 'bananas', 'peaches'], serial=True))  # R: builtins.str
    reveal_type(_.series_phrase(['apples', 'bananas', 'peaches'], '; ', ', or '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_series_phrase_serial() -> None:
    reveal_type(_.series_phrase_serial(['apples', 'bananas', 'peaches']))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_slugify() -> None:
    reveal_type(_.slugify('This is a slug.'))  # R: builtins.str
    reveal_type(_.slugify('This is a slug.', '+'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_snake_case() -> None:
    reveal_type(_.snake_case('This is Snake Case!'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_split() -> None:
    reveal_type(_.split('one potato, two potatoes, three potatoes, four!'))  # R: builtins.list[builtins.str]
    reveal_type(_.split('one potato, two potatoes, three potatoes, four!', ','))  # R: builtins.list[builtins.str]


@pytest.mark.mypy_testing
def test_mypy_start_case() -> None:
    reveal_type(_.start_case("fooBar"))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_starts_with() -> None:
    reveal_type(_.starts_with('abcdef', 'a'))  # R: builtins.bool
    reveal_type(_.starts_with('abcdef', 'b'))  # R: builtins.bool
    reveal_type(_.starts_with('abcdef', 'a', 1))  # R: builtins.bool


@pytest.mark.mypy_testing
def test_mypy_strip_tags() -> None:
    reveal_type(_.strip_tags('<a href="#">Some link</a>'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_substr_left() -> None:
    reveal_type(_.substr_left('abcdefcdg', 'cd'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_substr_left_end() -> None:
    reveal_type(_.substr_left_end('abcdefcdg', 'cd'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_substr_right() -> None:
    reveal_type(_.substr_right('abcdefcdg', 'cd'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_substr_right_end() -> None:
    reveal_type(_.substr_right_end('abcdefcdg', 'cd'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_successor() -> None:
    reveal_type(_.successor('b'))  # R: builtins.str
    reveal_type(_.successor('B'))  # R: builtins.str
    reveal_type(_.successor('2'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_surround() -> None:
    reveal_type(_.surround('abc', '"'))  # R: builtins.str
    reveal_type(_.surround('abc', '!'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_swap_case() -> None:
    reveal_type(_.swap_case('aBcDeF'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_title_case() -> None:
    reveal_type(_.title_case("bob's shop"))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_to_lower() -> None:
    reveal_type(_.to_lower('--Foo-Bar--'))  # R: builtins.str
    reveal_type(_.to_lower('fooBar'))  # R: builtins.str
    reveal_type(_.to_lower('__FOO_BAR__'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_to_upper() -> None:
    reveal_type(_.to_upper('--Foo-Bar--'))  # R: builtins.str
    reveal_type(_.to_upper('fooBar'))  # R: builtins.str
    reveal_type(_.to_upper('__FOO_BAR__'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_trim() -> None:
    reveal_type(_.trim('  abc efg\r\n '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_trim_end() -> None:
    reveal_type(_.trim_end('  abc efg\r\n '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_trim_start() -> None:
    reveal_type(_.trim_start('  abc efg\r\n '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_truncate() -> None:
    reveal_type(_.truncate('hello world', 5))  # R: builtins.str
    reveal_type(_.truncate('hello world', 5, '..'))  # R: builtins.str
    reveal_type(_.truncate('hello world', 10))  # R: builtins.str
    reveal_type(_.truncate('hello world', 10, separator=' '))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_unescape() -> None:
    reveal_type(_.unescape('&quot;1 &gt; 2 &amp;&amp; 3 &lt; 4&quot;'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_upper_case() -> None:
    reveal_type(_.upper_case('--foo-bar--'))  # R: builtins.str
    reveal_type(_.upper_case('fooBar'))  # R: builtins.str
    reveal_type(_.upper_case('/?*Foo10/;"B*Ar'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_upper_first() -> None:
    reveal_type(_.upper_first('fred'))  # R: builtins.str
    reveal_type(_.upper_first('foo bar'))  # R: builtins.str
    reveal_type(_.upper_first('1foobar'))  # R: builtins.str
    reveal_type(_.upper_first(';foobar'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_unquote() -> None:
    reveal_type(_.unquote('"abc"'))  # R: builtins.str
    reveal_type(_.unquote('"abc"', '#'))  # R: builtins.str
    reveal_type(_.unquote('#abc', '#'))  # R: builtins.str
    reveal_type(_.unquote('#abc#', '#'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_url() -> None:
    reveal_type(_.url('a', 'b', ['c', 'd'], '/', q='X', y='Z'))  # R: builtins.str


@pytest.mark.mypy_testing
def test_mypy_words() -> None:
    reveal_type(_.words('a b, c; d-e'))  # R: builtins.list[builtins.str]
    reveal_type(_.words('fred, barney, & pebbles', '/[^, ]+/g'))  # R: builtins.list[builtins.str]
