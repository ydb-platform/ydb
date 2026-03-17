import random
import string

from flask import render_template
from flask import render_template_string

from flask_caching import make_template_fragment_key


def test_jinjaext_cache(app, cache):
    somevar = "".join([random.choice(string.ascii_letters) for x in range(6)])

    testkeys = [
        make_template_fragment_key("fragment1"),
        make_template_fragment_key("fragment1", vary_on=["key1"]),
        make_template_fragment_key("fragment1", vary_on=["key1", somevar]),
    ]
    delkey = make_template_fragment_key("fragment2")

    with app.test_request_context():
        #: Test if elements are cached
        render_template("test_template.html", somevar=somevar, timeout=60)
        for k in testkeys:
            assert cache.get(k) == somevar
        assert cache.get(delkey) == somevar

        #: Test timeout=del to delete key
        render_template("test_template.html", somevar=somevar, timeout="del")
        for k in testkeys:
            assert cache.get(k) == somevar
        assert cache.get(delkey) is None

        #: Test rendering templates from strings
        output = render_template_string(
            """{% cache 60, "fragment3" %}{{somevar}}{% endcache %}""",
            somevar=somevar,
        )
        assert cache.get(make_template_fragment_key("fragment3")) == somevar
        assert output == somevar

        #: Test backwards compatibility
        output = render_template_string(
            """{% cache 30 %}{{somevar}}{% endcache %}""", somevar=somevar
        )
        assert cache.get(make_template_fragment_key("None1")) == somevar
        assert output == somevar

        output = render_template_string(
            """{% cache 30, "fragment4", "fragment5"%}{{somevar}}{% endcache %}""",
            somevar=somevar,
        )
        k = make_template_fragment_key("fragment4", vary_on=["fragment5"])
        assert cache.get(k) == somevar
        assert output == somevar
