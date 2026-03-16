import re

import pytest

from linkify_it import LinkifyIt, SchemaError
from linkify_it.main import Match
from linkify_it.tlds import TLDS


def test_pretest_false():
    linkifyit = LinkifyIt()
    assert linkifyit.pretest("nolink") is False


def test_create_instance_with_schemas():
    schemas = {"my:": {"validate": r"^\/\/[a-z]+"}}
    linkifyit = LinkifyIt(schemas)

    match = linkifyit.match("google.com. my:// my://asdf!")

    assert match[0].text == "google.com"
    assert match[1].text == "my://asdf"


def test_match_class():
    linkifyit = LinkifyIt()
    match = Match(linkifyit, 0)
    assert (
        match.__repr__()
        == "linkify_it.main.Match({'schema': '', 'index': -1, 'last_index': -1, 'raw': '', 'text': '', 'url': ''})"  # noqa: E501
    )


def test_api_extend_tlds():
    linkifyit = LinkifyIt()

    assert linkifyit.test("google.myroot") is False

    linkifyit.tlds("myroot", True)

    assert linkifyit.test("google.myroot") is True
    assert linkifyit.test("google.xyz") is False

    # ref - http://data.iana.org/TLD/tlds-alpha-by-domain.txt
    linkifyit.tlds(TLDS)

    assert linkifyit.test("google.xyz") is True
    assert linkifyit.test("google.myroot") is False


def test_api_add_rule_as_regex_with_default_normalizer():
    linkifyit = LinkifyIt().add("my:", {"validate": re.compile(r"^\/\/[a-z]+")})

    match = linkifyit.match("google.com. my:// my://asdf!")

    assert match[0].text == "google.com"
    assert match[1].text == "my://asdf"


def test_api_add_rule_as_regex_with_default_normalizer_with_no_compile():
    linkifyit = LinkifyIt().add("my:", {"validate": r"^\/\/[a-z]+"})

    match = linkifyit.match("google.com. my:// my://asdf!")

    assert match[0].text == "google.com"
    assert match[1].text == "my://asdf"


def test_api_add_rule_with_normalizer():
    def func_normalize(self, m):
        m.text = re.sub(r"^my://", "", m.text).upper()
        m.url = m.url.upper()

    linkifyit = LinkifyIt().add(
        "my:", {"validate": re.compile(r"^\/\/[a-z]+"), "normalize": func_normalize}
    )

    match = linkifyit.match("google.com. my:// my://asdf!")

    assert match[1].text == "ASDF"
    assert match[1].url == "MY://ASDF"


def test_api_add_rule_with_normalizer_no_cimpile():
    def func_normalize(self, m):
        m.text = re.sub(r"^my://", "", m.text).upper()
        m.url = m.url.upper()

    linkifyit = LinkifyIt().add(
        "my:", {"validate": r"^\/\/[a-z]+", "normalize": func_normalize}
    )

    match = linkifyit.match("google.com. my:// my://asdf!")

    assert match[1].text == "ASDF"
    assert match[1].url == "MY://ASDF"


def test_api_disable_rule():
    linkifyit = LinkifyIt()

    assert linkifyit.test("http://google.com")
    assert linkifyit.test("foo@bar.com")
    linkifyit.add("http:", None)
    linkifyit.add("mailto:", None)
    assert not linkifyit.test("http://google.com")
    assert not linkifyit.test("foo@bar.com")


def test_api_add_bad_definition():
    with pytest.raises(SchemaError):
        linkifyit = LinkifyIt({"fuzzy_link": False})

    linkifyit = LinkifyIt()

    with pytest.raises(SchemaError):
        linkifyit.add("test:", [])

    linkifyit = LinkifyIt()

    with pytest.raises(SchemaError):
        linkifyit.add("test:", {"validate": []})

    linkifyit = LinkifyIt()

    with pytest.raises(SchemaError):

        def func():
            return False

        linkifyit.add("test:", {"validate": func, "normalize": "bad"})


def test_api_at_position():
    linkifyit = LinkifyIt()

    assert linkifyit.test_schema_at("http://google.com", "http:", 5)
    assert linkifyit.test_schema_at("http://google.com", "HTTP:", 5)
    assert not linkifyit.test_schema_at("http://google.com", "http:", 6)

    assert not linkifyit.test_schema_at("http://google.com", "bad_schema:", 6)


def test_api_correct_cache_value():
    linkifyit = LinkifyIt()

    match = linkifyit.match(".com. http://google.com google.com ftp://google.com")

    assert match[0].text == "http://google.com"
    assert match[1].text == "google.com"
    assert match[2].text == "ftp://google.com"


def test_api_normalize():
    linkifyit = LinkifyIt()

    match = linkifyit.match("mailto:foo@bar.com")[0]

    # assert match.text == "foo@bar.com"
    assert match.url == "mailto:foo@bar.com"

    match = linkifyit.match("foo@bar.com")[0]

    # assert match.text == "foo@bar.com"
    assert match.url == "mailto:foo@bar.com"


def test_api_twitter_rule():
    linkifyit = LinkifyIt()

    def validate(self, text, pos):
        tail = text[pos:]

        if not self.re.get("twitter"):
            self.re["twitter"] = re.compile(
                "^([a-zA-Z0-9_]){1,15}(?!_)(?=$|" + self.re["src_ZPCc"] + ")"
            )
        if self.re["twitter"].search(tail):
            if pos > 2 and tail[pos - 2] == "@":
                return False
            return len(self.re["twitter"].search(tail).group())
        return 0

    def normalize(self, m):
        m.url = "https://twitter.com/" + re.sub(r"^@", "", m.url)

    linkifyit.add("@", {"validate": validate, "normalize": normalize})

    assert linkifyit.match("hello, @gamajoba_!")[0].text == "@gamajoba_"
    assert linkifyit.match(":@givi")[0].text == "@givi"
    assert linkifyit.match(":@givi")[0].url == "https://twitter.com/givi"
    assert not linkifyit.test("@@invalid")


def test_api_twitter_rule_no_compile():
    linkifyit = LinkifyIt()

    def validate(self, text, pos):
        tail = text[pos:]

        if not self.re.get("twitter"):
            self.re["twitter"] = (
                "^([a-zA-Z0-9_]){1,15}(?!_)(?=$|" + self.re["src_ZPCc"] + ")"
            )
        if re.search(self.re["twitter"], tail):
            if pos > 2 and tail[pos - 2] == "@":
                return False
            return len(re.search(self.re["twitter"], tail).group())
        return 0

    def normalize(self, m):
        m.url = "https://twitter.com/" + re.sub(r"^@", "", m.url)

    linkifyit.add("@", {"validate": validate, "normalize": normalize})

    assert linkifyit.match("hello, @gamajoba_!")[0].text == "@gamajoba_"
    assert linkifyit.match(":@givi")[0].text == "@givi"
    assert linkifyit.match(":@givi")[0].url == "https://twitter.com/givi"
    assert not linkifyit.test("@@invalid")


def test_api_set_option_fuzzylink():
    linkifyit = LinkifyIt(options={"fuzzy_link": False})

    assert not linkifyit.test("google.com")

    linkifyit.set({"fuzzy_link": True})

    assert linkifyit.test("google.com")
    assert linkifyit.match("google.com")[0].text == "google.com"


def test_api_set_option_fuzzyemail():
    linkifyit = LinkifyIt(options={"fuzzy_email": False})

    assert not linkifyit.test("foo@bar.com")

    linkifyit.set({"fuzzy_email": True})

    assert linkifyit.test("foo@bar.com")
    assert linkifyit.match("foo@bar.com")[0].text == "foo@bar.com"


def test_api_set_option_fuzzyip():
    linkifyit = LinkifyIt()

    assert not linkifyit.test("1.1.1.1")

    linkifyit.set({"fuzzy_ip": True})

    assert linkifyit.test("1.1.1.1")
    assert linkifyit.match("1.1.1.1")[0].text == "1.1.1.1"


def test_api_shoud_not_hang_in_fuzzy_mode_with_sequence_of_astrals():
    linkifyit = LinkifyIt()

    linkifyit.set({"fuzzy_link": True})

    linkifyit.match("😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡😡 .com")


def test_api_shoud_accept_triple_minus():
    linkifyit = LinkifyIt()

    assert linkifyit.match("http://e.com/foo---bar")[0].text == "http://e.com/foo---bar"
    assert linkifyit.match("text@example.com---foo") is None

    linkifyit = LinkifyIt(None, {"---": True})

    assert linkifyit.match("http://e.com/foo---bar")[0].text == "http://e.com/foo"
    assert linkifyit.match("text@example.com---foo")[0].text == "text@example.com"


# issue #25. Schema key containing - not producing matches
@pytest.mark.parametrize(
    "escape_str",
    {".", "?", "*", "+", "^", "$", "[", "]", "\\", "(", ")", "{", "}", "|", "-"},
)
def test_api_add_alias_rule_with_excape_re_string(escape_str):
    linkifyit = LinkifyIt()

    linkifyit.add("foo{}bar:".format(escape_str), "http:")
    assert linkifyit.test("Check foo{}bar://test".format(escape_str)) is True


def test_api_blank_test_match_at_the_start():
    linkifyit = LinkifyIt()

    assert not linkifyit.match_at_start("")


def test_api_should_find_a_match_at_the_start():
    linkifyit = LinkifyIt()

    linkifyit = LinkifyIt(options={"fuzzy_link": True})
    linkifyit.set({"fuzzy_link": True})

    assert not linkifyit.test("@@invalid")
    assert not linkifyit.match_at_start("google.com 123")
    assert not linkifyit.match_at_start("  http://google.com 123")


def test_api_match_a_start_should_not_interfere_with_normal_match():
    linkifyit = LinkifyIt()

    str = "http://google.com http://google.com"
    assert linkifyit.match_at_start(str)
    assert len(linkifyit.match(str)) == 2

    str = "aaa http://google.com http://google.com"
    assert not linkifyit.match_at_start(str)
    assert len(linkifyit.match(str)) == 2


def test_api_should_not_match_incomplete_links():
    # regression test for https://github.com/markdown-it/markdown-it/issues/868
    linkifyit = LinkifyIt()

    assert not linkifyit.match_at_start("http://")
    assert not linkifyit.match_at_start("https://")
