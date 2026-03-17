import collections.abc
import typing as t
import unittest

from uritemplate import URITemplate
from uritemplate import expand
from uritemplate import partial
from uritemplate import variable
from uritemplate import variables


def merge_dicts(
    *args: t.Union[
        variable.VariableValueDict, t.Dict[str, str], t.Dict[str, t.List[str]]
    ]
) -> variable.VariableValueDict:
    d: t.Dict[str, variable.VariableValue] = {}
    for arg in args:
        d.update(arg)
    return d


ExampleVariables = variable.VariableValueDict
ExampleTemplatesAndResults = t.List[t.Tuple[str, t.Union[str, t.List[str]]]]


class ExampleWithVariables(t.TypedDict):
    expansion: ExampleVariables
    expected: str


class RFCTemplateExamples(type):
    var = {"var": "value"}
    hello = {"hello": "Hello World!"}
    path = {"path": "/foo/bar"}
    x = {"x": "1024"}
    y = {"y": "768"}
    empty = {"empty": ""}
    merged_x_y = merge_dicts(x, y)
    list_ex = {"list": ["red", "green", "blue"]}
    keys = {"keys": [("semi", ";"), ("dot", "."), ("comma", ",")]}

    # # Level 1
    # Simple string expansion
    level1_examples = {
        "{var}": {
            "expansion": var,
            "expected": "value",
        },
        "{hello}": {
            "expansion": hello,
            "expected": "Hello%20World%21",
        },
    }

    # # Level 2
    # Reserved string expansion
    level2_reserved_examples = {
        "{+var}": {
            "expansion": var,
            "expected": "value",
        },
        "{+hello}": {
            "expansion": hello,
            "expected": "Hello%20World!",
        },
        "{+path}/here": {
            "expansion": path,
            "expected": "/foo/bar/here",
        },
        "here?ref={+path}": {
            "expansion": path,
            "expected": "here?ref=/foo/bar",
        },
    }

    # Fragment expansion, crosshatch-prefixed
    level2_fragment_examples = {
        "X{#var}": {
            "expansion": var,
            "expected": "X#value",
        },
        "X{#hello}": {"expansion": hello, "expected": "X#Hello%20World!"},
    }

    # # Level 3
    # String expansion with multiple variables
    level3_multiple_variable_examples = {
        "map?{x,y}": {
            "expansion": merged_x_y,
            "expected": "map?1024,768",
        },
        "{x,hello,y}": {
            "expansion": merge_dicts(x, y, hello),
            "expected": "1024,Hello%20World%21,768",
        },
    }

    # Reserved expansion with multiple variables
    level3_reserved_examples = {
        "{+x,hello,y}": {
            "expansion": merge_dicts(x, y, hello),
            "expected": "1024,Hello%20World!,768",
        },
        "{+path,x}/here": {
            "expansion": merge_dicts(path, x),
            "expected": "/foo/bar,1024/here",
        },
    }

    # Fragment expansion with multiple variables
    level3_fragment_examples = {
        "{#x,hello,y}": {
            "expansion": merge_dicts(x, y, hello),
            "expected": "#1024,Hello%20World!,768",
        },
        "{#path,x}/here": {
            "expansion": merge_dicts(path, x),
            "expected": "#/foo/bar,1024/here",
        },
    }

    # Label expansion, dot-prefixed
    level3_label_examples = {
        "X{.var}": {
            "expansion": var,
            "expected": "X.value",
        },
        "X{.x,y}": {
            "expansion": merged_x_y,
            "expected": "X.1024.768",
        },
    }

    # Path segments, slash-prefixed
    level3_path_segment_examples = {
        "{/var}": {
            "expansion": var,
            "expected": "/value",
        },
        "{/var,x}/here": {
            "expansion": merge_dicts(var, x),
            "expected": "/value/1024/here",
        },
    }

    # Path-style parameters, semicolon-prefixed
    level3_path_semi_examples = {
        "{;x,y}": {
            "expansion": merged_x_y,
            "expected": ";x=1024;y=768",
        },
        "{;x,y,empty}": {
            "expansion": merge_dicts(x, y, empty),
            "expected": ";x=1024;y=768;empty",
        },
    }

    # Form-style query, ampersand-separated
    level3_form_amp_examples = {
        "{?x,y}": {
            "expansion": merged_x_y,
            "expected": "?x=1024&y=768",
        },
        "{?x,y,empty}": {
            "expansion": merge_dicts(x, y, empty),
            "expected": "?x=1024&y=768&empty=",
        },
    }

    # Form-style query continuation
    level3_form_cont_examples = {
        "?fixed=yes{&x}": {
            "expansion": x,
            "expected": "?fixed=yes&x=1024",
        },
        "{&x,y,empty}": {
            "expansion": merge_dicts(x, y, empty),
            "expected": "&x=1024&y=768&empty=",
        },
    }

    # # Level 4
    # String expansion with value modifiers
    level4_value_modifier_examples = {
        "{var:3}": {
            "expansion": var,
            "expected": "val",
        },
        "{var:30}": {
            "expansion": var,
            "expected": "value",
        },
        "{list}": {
            "expansion": list_ex,
            "expected": "red,green,blue",
        },
        "{list*}": {
            "expansion": list_ex,
            "expected": "red,green,blue",
        },
        "{keys}": {
            "expansion": keys,
            "expected": "semi,%3B,dot,.,comma,%2C",
        },
        "{keys*}": {
            "expansion": keys,
            "expected": "semi=%3B,dot=.,comma=%2C",
        },
    }

    # Reserved expansion with value modifiers
    level4_reserved_examples = {
        "{+path:6}/here": {
            "expansion": path,
            "expected": "/foo/b/here",
        },
        "{+list}": {
            "expansion": list_ex,
            "expected": "red,green,blue",
        },
        "{+list*}": {
            "expansion": list_ex,
            "expected": "red,green,blue",
        },
        "{+keys}": {
            "expansion": keys,
            "expected": "semi,;,dot,.,comma,,",
        },
        "{+keys*}": {
            "expansion": keys,
            "expected": "semi=;,dot=.,comma=,",
        },
    }

    # Fragment expansion with value modifiers
    level4_fragment_examples = {
        "{#path:6}/here": {
            "expansion": path,
            "expected": "#/foo/b/here",
        },
        "{#list}": {
            "expansion": list_ex,
            "expected": "#red,green,blue",
        },
        "{#list*}": {
            "expansion": list_ex,
            "expected": "#red,green,blue",
        },
        "{#keys}": {"expansion": keys, "expected": "#semi,;,dot,.,comma,,"},
        "{#keys*}": {"expansion": keys, "expected": "#semi=;,dot=.,comma=,"},
    }

    # Label expansion, dot-prefixed
    level4_label_examples = {
        "X{.var:3}": {
            "expansion": var,
            "expected": "X.val",
        },
        "X{.list}": {
            "expansion": list_ex,
            "expected": "X.red,green,blue",
        },
        "X{.list*}": {
            "expansion": list_ex,
            "expected": "X.red.green.blue",
        },
        "X{.keys}": {
            "expansion": keys,
            "expected": "X.semi,%3B,dot,.,comma,%2C",
        },
        "X{.keys*}": {
            "expansion": keys,
            "expected": "X.semi=%3B.dot=..comma=%2C",
        },
    }

    # Path segments, slash-prefixed
    level4_path_slash_examples = {
        "{/var:1,var}": {
            "expansion": var,
            "expected": "/v/value",
        },
        "{/list}": {
            "expansion": list_ex,
            "expected": "/red,green,blue",
        },
        "{/list*}": {
            "expansion": list_ex,
            "expected": "/red/green/blue",
        },
        "{/list*,path:4}": {
            "expansion": merge_dicts(list_ex, path),
            "expected": "/red/green/blue/%2Ffoo",
        },
        "{/keys}": {
            "expansion": keys,
            "expected": "/semi,%3B,dot,.,comma,%2C",
        },
        "{/keys*}": {
            "expansion": keys,
            "expected": "/semi=%3B/dot=./comma=%2C",
        },
    }

    # Path-style parameters, semicolon-prefixed
    level4_path_semi_examples = {
        "{;hello:5}": {
            "expansion": hello,
            "expected": ";hello=Hello",
        },
        "{;list}": {
            "expansion": list_ex,
            "expected": ";list=red,green,blue",
        },
        "{;list*}": {
            "expansion": list_ex,
            "expected": ";list=red;list=green;list=blue",
        },
        "{;keys}": {
            "expansion": keys,
            "expected": ";keys=semi,%3B,dot,.,comma,%2C",
        },
        "{;keys*}": {
            "expansion": keys,
            "expected": ";semi=%3B;dot=.;comma=%2C",
        },
    }

    # Form-style query, ampersand-separated
    level4_form_amp_examples = {
        "{?var:3}": {
            "expansion": var,
            "expected": "?var=val",
        },
        "{?list}": {
            "expansion": list_ex,
            "expected": "?list=red,green,blue",
        },
        "{?list*}": {
            "expansion": list_ex,
            "expected": "?list=red&list=green&list=blue",
        },
        "{?keys}": {
            "expansion": keys,
            "expected": "?keys=semi,%3B,dot,.,comma,%2C",
        },
        "{?keys*}": {
            "expansion": keys,
            "expected": "?semi=%3B&dot=.&comma=%2C",
        },
    }

    # Form-style query continuation
    level4_form_query_examples = {
        "{&var:3}": {
            "expansion": var,
            "expected": "&var=val",
        },
        "{&list}": {
            "expansion": list_ex,
            "expected": "&list=red,green,blue",
        },
        "{&list*}": {
            "expansion": list_ex,
            "expected": "&list=red&list=green&list=blue",
        },
        "{&keys}": {
            "expansion": keys,
            "expected": "&keys=semi,%3B,dot,.,comma,%2C",
        },
        "{&keys*}": {
            "expansion": keys,
            "expected": "&semi=%3B&dot=.&comma=%2C",
        },
    }

    def __new__(
        cls, name: str, bases: t.Sequence[t.Any], attrs: t.Dict[str, t.Any]
    ) -> t.Any:
        def make_test(
            d: t.Dict[str, ExampleWithVariables],
        ) -> collections.abc.Callable[["unittest.TestCase"], None]:
            def _test_(self: "unittest.TestCase") -> None:
                for k, v in d.items():
                    with self.subTest(template=k, **v):
                        t = URITemplate(k)
                        self.assertEqual(
                            t.expand(v["expansion"]), v["expected"]
                        )

            return _test_

        examples = [
            (n, getattr(RFCTemplateExamples, n))
            for n in dir(RFCTemplateExamples)
            if n.startswith("level")
        ]

        for name, value in examples:
            testname = "test_%s" % name
            attrs[testname] = make_test(value)

        return type.__new__(cls, name, bases, attrs)  # type: ignore


class TestURITemplate(unittest.TestCase, metaclass=RFCTemplateExamples):
    def test_no_variables_in_uri(self) -> None:
        """
        This test ensures that if there are no variables present, the
        template evaluates to itself.
        """
        uri = "https://api.github.com/users"
        t = URITemplate(uri)
        self.assertEqual(t.expand(), uri)
        self.assertEqual(t.expand(users="foo"), uri)

    def test_all_variables_parsed(self) -> None:
        """
        This test ensures that all variables are parsed.
        """
        uris = [
            "https://api.github.com",
            "https://api.github.com/users{/user}",
            "https://api.github.com/repos{/user}{/repo}",
            "https://api.github.com/repos{/user}{/repo}/issues{/issue}",
        ]

        for i, uri in enumerate(uris):
            t = URITemplate(uri)
            self.assertEqual(len(t.variables), i)

    def test_expand(self) -> None:
        """
        This test ensures that expansion works as expected.
        """
        # Single
        t = URITemplate("https://api.github.com/users{/user}")
        expanded = "https://api.github.com/users/sigmavirus24"
        self.assertEqual(t.expand(user="sigmavirus24"), expanded)
        v = t.variables[0]
        self.assertEqual(v.expand({"user": None}), {"/user": ""})

        # Multiple
        t = URITemplate("https://api.github.com/users{/user}{/repo}")
        expanded = "https://api.github.com/users/sigmavirus24/github3.py"
        self.assertEqual(
            t.expand({"repo": "github3.py"}, user="sigmavirus24"), expanded
        )

    def test_str_repr(self) -> None:
        uri = "https://api.github.com{/endpoint}"
        t = URITemplate(uri)
        self.assertEqual(str(t), uri)
        self.assertEqual(str(t.variables[0]), "/endpoint")
        self.assertEqual(repr(t), 'URITemplate("%s")' % uri)
        self.assertEqual(repr(t.variables[0]), "URIVariable(/endpoint)")

    def test_hash(self) -> None:
        uri = "https://api.github.com{/endpoint}"
        self.assertEqual(hash(URITemplate(uri)), hash(uri))

    def test_default_value(self) -> None:
        uri = "https://api.github.com/user{/user=sigmavirus24}"
        t = URITemplate(uri)
        self.assertEqual(
            t.expand(), "https://api.github.com/user/sigmavirus24"
        )
        self.assertEqual(
            t.expand(user="lukasa"), "https://api.github.com/user/lukasa"
        )

    def test_query_expansion(self) -> None:
        t = URITemplate("{foo}")
        self.assertEqual(
            t.variables[0]._query_expansion("foo", None, False, None), None
        )

    def test_label_path_expansion(self) -> None:
        t = URITemplate("{foo}")
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", None, False, None),
            None,
        )

    def test_label_path_expansion_explode_slash(self) -> None:
        t = URITemplate("{/foo*}")
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", [], True, None), None
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", [None], True, None),
            None,
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", [None, None], True, None
            ),
            None,
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", ["one"], True, None),
            "one",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", ["one", "two"], True, None
            ),
            "one/two",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", ["one", None, "two"], True, None
            ),
            "one/two",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", [""], True, None), ""
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", ["", ""], True, None),
            "/",
        )

        self.assertEqual(
            t.variables[0]._label_path_expansion("foo", {}, True, None), None
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", {"one": ""}, True, None
            ),
            "one=",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", {"one": "", "two": ""}, True, None
            ),
            "one=/two=",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", {"one": None}, True, None
            ),
            None,
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", {"one": None, "two": "two"}, True, None
            ),
            "two=two",
        )
        self.assertEqual(
            t.variables[0]._label_path_expansion(
                "foo", {"one": None, "two": None}, True, None
            ),
            None,
        )

    def test_semi_path_expansion(self) -> None:
        t = URITemplate("{foo}")
        v = t.variables[0]
        self.assertEqual(
            v._semi_path_expansion("foo", None, False, False), None
        )
        t.variables[0].operator = variable.Operator.form_style_query
        self.assertEqual(
            v._semi_path_expansion("foo", ["bar", "bogus"], True, False),
            "foo=bar&foo=bogus",
        )

    def test_string_expansion(self) -> None:
        t = URITemplate("{foo}")
        self.assertEqual(
            t.variables[0]._string_expansion("foo", None, False, False), None
        )

    def test_hashability(self) -> None:
        t = URITemplate("{foo}")
        u = URITemplate("{foo}")
        d = {t: 1}
        d[u] += 1
        self.assertEqual(d, {t: 2})

    def test_no_mutate(self) -> None:
        args: variable.VariableValueDict = {}
        t = URITemplate("")
        t.expand(args, key=1)
        self.assertEqual(args, {})


class TestVariableModule(unittest.TestCase):
    def test_is_list_of_tuples(self) -> None:
        a_list = [(1, 2), (3, 4)]
        self.assertEqual(variable.is_list_of_tuples(a_list), (True, a_list))

        b_list = [1, 2, 3, 4]
        self.assertEqual(variable.is_list_of_tuples(b_list), (False, None))

    def test_list_test(self) -> None:
        a_list = [1, 2, 3, 4]
        self.assertEqual(variable.list_test(a_list), True)

        b_list = str([1, 2, 3, 4])
        self.assertEqual(variable.list_test(b_list), False)

    def test_list_of_tuples_test(self) -> None:
        a_list = [(1, 2), (3, 4)]
        self.assertEqual(variable.dict_test(a_list), False)

        d = dict(a_list)
        self.assertEqual(variable.dict_test(d), True)


class TestAPI(unittest.TestCase):
    uri = "https://api.github.com{/endpoint}"

    def test_expand(self) -> None:
        self.assertEqual(
            expand(self.uri, {"endpoint": "users"}),
            "https://api.github.com/users",
        )

    def test_partial(self) -> None:
        self.assertEqual(partial(self.uri), URITemplate(self.uri))
        uri = self.uri + "/sigmavirus24{/other}"
        self.assertEqual(
            partial(uri, endpoint="users"),
            URITemplate("https://api.github.com/users/sigmavirus24{/other}"),
        )

    def test_variables(self) -> None:
        self.assertEqual(
            variables(self.uri), URITemplate(self.uri).variable_names
        )


class TestNativeTypeSupport(unittest.TestCase):
    context: variable.VariableValueDict = {
        "zero": 0,
        "one": 1,
        "digits": list(range(10)),
        "a_float": 3.1415,
    }

    def test_expand(self) -> None:
        self.assertEqual(expand("{zero}", self.context), "0")
        self.assertEqual(expand("{one}", self.context), "1")
        self.assertEqual(
            expand("{digits}", self.context), "0,1,2,3,4,5,6,7,8,9"
        )
        self.assertEqual(
            expand("{?digits,one}", self.context),
            "?digits=0,1,2,3,4,5,6,7,8,9&one=1",
        )
        self.assertEqual(
            expand("{/digits}", self.context), "/0,1,2,3,4,5,6,7,8,9"
        )
        self.assertEqual(
            expand("{/digits*}", self.context), "/0/1/2/3/4/5/6/7/8/9"
        )
        # explicit testing for issue #53
        self.assertEqual(expand("{/zero}", self.context), "/0")
        self.assertEqual(expand("{/zero,a_float}", self.context), "/0/3.1415")


if __name__ == "__main__":
    unittest.main()
