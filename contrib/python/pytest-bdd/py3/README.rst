BDD library for the pytest runner
=================================

.. image:: http://img.shields.io/pypi/v/pytest-bdd.svg
   :target: https://pypi.python.org/pypi/pytest-bdd
.. image:: https://codecov.io/gh/pytest-dev/pytest-bdd/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/pytest-dev/pytest-bdd
.. image:: https://travis-ci.org/pytest-dev/pytest-bdd.svg?branch=master
    :target: https://travis-ci.org/pytest-dev/pytest-bdd
.. image:: https://readthedocs.org/projects/pytest-bdd/badge/?version=stable
    :target: https://readthedocs.org/projects/pytest-bdd/
    :alt: Documentation Status

pytest-bdd implements a subset of the Gherkin language to enable automating project
requirements testing and to facilitate behavioral driven development.

Unlike many other BDD tools, it does not require a separate runner and benefits from
the power and flexibility of pytest. It enables unifying unit and functional
tests, reduces the burden of continuous integration server configuration and allows the reuse of
test setups.

Pytest fixtures written for unit tests can be reused for setup and actions
mentioned in feature steps with dependency injection. This allows a true BDD
just-enough specification of the requirements without maintaining any context object
containing the side effects of Gherkin imperative declarations.

.. _behave: https://pypi.python.org/pypi/behave
.. _pytest-splinter: https://github.com/pytest-dev/pytest-splinter

Install pytest-bdd
------------------

::

    pip install pytest-bdd


The minimum required version of pytest is 4.3.


Example
-------

An example test for a blog hosting software could look like this.
Note that pytest-splinter_ is used to get the browser fixture.

publish_article.feature:

.. code-block:: gherkin

    Feature: Blog
        A site where you can publish your articles.

        Scenario: Publishing the article
            Given I'm an author user
            And I have an article

            When I go to the article page
            And I press the publish button

            Then I should not see the error message
            And the article should be published  # Note: will query the database

Note that only one feature is allowed per feature file.

test_publish_article.py:

.. code-block:: python

    from pytest_bdd import scenario, given, when, then

    @scenario('publish_article.feature', 'Publishing the article')
    def test_publish():
        pass


    @given("I'm an author user")
    def author_user(auth, author):
        auth['user'] = author.user


    @given("I have an article", target_fixture="article")
    def article(author):
        return create_test_article(author=author)


    @when("I go to the article page")
    def go_to_article(article, browser):
        browser.visit(urljoin(browser.url, '/manage/articles/{0}/'.format(article.id)))


    @when("I press the publish button")
    def publish_article(browser):
        browser.find_by_css('button[name=publish]').first.click()


    @then("I should not see the error message")
    def no_error_message(browser):
        with pytest.raises(ElementDoesNotExist):
            browser.find_by_css('.message.error').first


    @then("the article should be published")
    def article_is_published(article):
        article.refresh()  # Refresh the object in the SQLAlchemy session
        assert article.is_published


Scenario decorator
------------------

The scenario decorator can accept the following optional keyword arguments:

* ``encoding`` - decode content of feature file in specific encoding. UTF-8 is default.
* ``example_converters`` - mapping to pass functions to convert example values provided in feature files.

Functions decorated with the `scenario` decorator behave like a normal test function,
and they will be executed after all scenario steps.
You can consider it as a normal pytest test function, e.g. order fixtures there,
call other functions and make assertions:


.. code-block:: python

    from pytest_bdd import scenario, given, when, then

    @scenario('publish_article.feature', 'Publishing the article')
    def test_publish(browser):
        assert article.title in browser.html


Step aliases
------------

Sometimes, one has to declare the same fixtures or steps with
different names for better readability. In order to use the same step
function with multiple step names simply decorate it multiple times:

.. code-block:: python

    @given("I have an article")
    @given("there's an article")
    def article(author, target_fixture="article"):
        return create_test_article(author=author)

Note that the given step aliases are independent and will be executed
when mentioned.

For example if you associate your resource to some owner or not. Admin
user can’t be an author of the article, but articles should have a
default author.

.. code-block:: gherkin

    Feature: Resource owner
        Scenario: I'm the author
            Given I'm an author
            And I have an article


        Scenario: I'm the admin
            Given I'm the admin
            And there's an article


Step arguments
--------------

Often it's possible to reuse steps giving them a parameter(s).
This allows to have single implementation and multiple use, so less code.
Also opens the possibility to use same step twice in single scenario and with different arguments!
And even more, there are several types of step parameter parsers at your disposal
(idea taken from behave_ implementation):

.. _pypi_parse: http://pypi.python.org/pypi/parse
.. _pypi_parse_type: http://pypi.python.org/pypi/parse_type

**string** (the default)
    This is the default and can be considered as a `null` or `exact` parser. It parses no parameters
    and matches the step name by equality of strings.
**parse** (based on: pypi_parse_)
    Provides a simple parser that replaces regular expressions for
    step parameters with a readable syntax like ``{param:Type}``.
    The syntax is inspired by the Python builtin ``string.format()``
    function.
    Step parameters must use the named fields syntax of pypi_parse_
    in step definitions. The named fields are extracted,
    optionally type converted and then used as step function arguments.
    Supports type conversions by using type converters passed via `extra_types`
**cfparse** (extends: pypi_parse_, based on: pypi_parse_type_)
    Provides an extended parser with "Cardinality Field" (CF) support.
    Automatically creates missing type converters for related cardinality
    as long as a type converter for cardinality=1 is provided.
    Supports parse expressions like:
    * ``{values:Type+}`` (cardinality=1..N, many)
    * ``{values:Type*}`` (cardinality=0..N, many0)
    * ``{value:Type?}``  (cardinality=0..1, optional)
    Supports type conversions (as above).
**re**
    This uses full regular expressions to parse the clause text. You will
    need to use named groups "(?P<name>...)" to define the variables pulled
    from the text and passed to your ``step()`` function.
    Type conversion can only be done via `converters` step decorator argument (see example below).

The default parser is `string`, so just plain one-to-one match to the keyword definition.
Parsers except `string`, as well as their optional arguments are specified like:

for `cfparse` parser

.. code-block:: python

    from pytest_bdd import parsers

    @given(
        parsers.cfparse("there are {start:Number} cucumbers",
        extra_types=dict(Number=int)),
        target_fixture="start_cucumbers",
    )
    def start_cucumbers(start):
        return dict(start=start, eat=0)

for `re` parser

.. code-block:: python

    from pytest_bdd import parsers

    @given(
        parsers.re(r"there are (?P<start>\d+) cucumbers"),
        converters=dict(start=int),
        target_fixture="start_cucumbers",
    )
    def start_cucumbers(start):
        return dict(start=start, eat=0)


Example:

.. code-block:: gherkin

    Feature: Step arguments
        Scenario: Arguments for given, when, thens
            Given there are 5 cucumbers

            When I eat 3 cucumbers
            And I eat 2 cucumbers

            Then I should have 0 cucumbers


The code will look like:

.. code-block:: python

    import re
    from pytest_bdd import scenario, given, when, then, parsers


    @scenario("arguments.feature", "Arguments for given, when, thens")
    def test_arguments():
        pass


    @given(parsers.parse("there are {start:d} cucumbers"), target_fixture="start_cucumbers")
    def start_cucumbers(start):
        return dict(start=start, eat=0)


    @when(parsers.parse("I eat {eat:d} cucumbers"))
    def eat_cucumbers(start_cucumbers, eat):
        start_cucumbers["eat"] += eat


    @then(parsers.parse("I should have {left:d} cucumbers"))
    def should_have_left_cucumbers(start_cucumbers, start, left):
        assert start_cucumbers['start'] == start
        assert start - start_cucumbers['eat'] == left

Example code also shows possibility to pass argument converters which may be useful if you need to postprocess step
arguments after the parser.

You can implement your own step parser. It's interface is quite simple. The code can looks like:

.. code-block:: python

    import re
    from pytest_bdd import given, parsers


    class MyParser(parsers.StepParser):
        """Custom parser."""

        def __init__(self, name, **kwargs):
            """Compile regex."""
            super(re, self).__init__(name)
            self.regex = re.compile(re.sub("%(.+)%", "(?P<\1>.+)", self.name), **kwargs)

        def parse_arguments(self, name):
            """Get step arguments.

            :return: `dict` of step arguments
            """
            return self.regex.match(name).groupdict()

        def is_matching(self, name):
            """Match given name with the step name."""
            return bool(self.regex.match(name))


    @given(parsers.parse("there are %start% cucumbers"), target_fixture="start_cucumbers")
    def start_cucumbers(start):
        return dict(start=start, eat=0)


Step arguments are fixtures as well!
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Step arguments are injected into pytest `request` context as normal fixtures with the names equal to the names of the
arguments. This opens a number of possibilies:

* you can access step's argument as a fixture in other step function just by mentioning it as an argument (just like any othe pytest fixture)
* if the name of the step argument clashes with existing fixture, it will be overridden by step's argument value; this way you can set/override the value for some fixture deeply inside of the fixture tree in a ad-hoc way by just choosing the proper name for the step argument.


Override fixtures via given steps
---------------------------------

Dependency injection is not a panacea if you have complex structure of your test setup data. Sometimes there's a need
such a given step which would imperatively change the fixture only for certain test (scenario), while for other tests
it will stay untouched. To allow this, special parameter `target_fixture` exists in the `given` decorator:

.. code-block:: python

    from pytest_bdd import given

    @pytest.fixture
    def foo():
        return "foo"


    @given("I have injecting given", target_fixture="foo")
    def injecting_given():
        return "injected foo"


    @then('foo should be "injected foo"')
    def foo_is_foo(foo):
        assert foo == 'injected foo'


.. code-block:: gherkin

    Feature: Target fixture
        Scenario: Test given fixture injection
            Given I have injecting given
            Then foo should be "injected foo"


In this example existing fixture `foo` will be overridden by given step `I have injecting given` only for scenario it's
used in.

Sometimes it is also useful to let `when` and `then` steps to provide a fixture as well.
A common use case is when we have to assert the outcome of an HTTP request:

.. code-block:: python

    # test_blog.py

    from pytest_bdd import scenarios, given, when, then

    from my_app.models import Article

    scenarios("blog.feature")


    @given("there is an article", target_fixture="article")
    def there_is_an_article():
        return Article()


    @when("I request the deletion of the article", target_fixture="request_result")
    def there_should_be_a_new_article(article, http_client):
        return http_client.delete(f"/articles/{article.uid}")


    @then("the request should be successful")
    def article_is_published(request_result):
        assert request_result.status_code == 200


.. code-block:: gherkin

    # blog.feature

    Feature: Blog
        Scenario: Deleting the article
            Given there is an article

            When I request the deletion of the article

            Then the request should be successful


Multiline steps
---------------

As Gherkin, pytest-bdd supports multiline steps
(aka `PyStrings <http://behat.org/en/v3.0/user_guide/writing_scenarios.html#pystrings>`_).
But in much cleaner and powerful way:

.. code-block:: gherkin

    Feature: Multiline steps
        Scenario: Multiline step using sub indentation
            Given I have a step with:
                Some
                Extra
                Lines
            Then the text should be parsed with correct indentation

Step is considered as multiline one, if the **next** line(s) after it's first line, is indented relatively
to the first line. The step name is then simply extended by adding further lines with newlines.
In the example above, the Given step name will be:

.. code-block:: python

    'I have a step with:\nSome\nExtra\nLines'

You can of course register step using full name (including the newlines), but it seems more practical to use
step arguments and capture lines after first line (or some subset of them) into the argument:

.. code-block:: python

    import re

    from pytest_bdd import given, then, scenario


    @scenario(
        'multiline.feature',
        'Multiline step using sub indentation',
    )
    def test_multiline():
        pass


    @given(parsers.parse("I have a step with:\n{text}"), target_fixture="i_have_text")
    def i_have_text(text):
        return text


    @then("the text should be parsed with correct indentation")
    def text_should_be_correct(i_have_text, text):
        assert i_have_text == text == 'Some\nExtra\nLines'

Note that `then` step definition (`text_should_be_correct`) in this example uses `text` fixture which is provided
by a a `given` step (`i_have_text`) argument with the same name (`text`). This possibility is described in
the `Step arguments are fixtures as well!`_ section.


Scenarios shortcut
------------------

If you have relatively large set of feature files, it's boring to manually bind scenarios to the tests using the
scenario decorator. Of course with the manual approach you get all the power to be able to additionally parametrize
the test, give the test function a nice name, document it, etc, but in the majority of the cases you don't need that.
Instead you want to bind `all` scenarios found in the `feature` folder(s) recursively automatically.
For this - there's a `scenarios` helper.

.. code-block:: python

    from pytest_bdd import scenarios

    # assume 'features' subfolder is in this file's directory
    scenarios('features')

That's all you need to do to bind all scenarios found in the `features` folder!
Note that you can pass multiple paths, and those paths can be either feature files or feature folders.


.. code-block:: python

    from pytest_bdd import scenarios

    # pass multiple paths/files
    scenarios('features', 'other_features/some.feature', 'some_other_features')

But what if you need to manually bind certain scenario, leaving others to be automatically bound?
Just write your scenario in a `normal` way, but ensure you do it `BEFORE` the call of `scenarios` helper.


.. code-block:: python

    from pytest_bdd import scenario, scenarios

    @scenario('features/some.feature', 'Test something')
    def test_something():
        pass

    # assume 'features' subfolder is in this file's directory
    scenarios('features')

In the example above `test_something` scenario binding will be kept manual, other scenarios found in the `features`
folder will be bound automatically.


Scenario outlines
-----------------

Scenarios can be parametrized to cover few cases. In Gherkin the variable
templates are written using corner braces as <somevalue>.
`Gherkin scenario outlines <http://behat.org/en/v3.0/user_guide/writing_scenarios.html#scenario-outlines>`_ are supported by pytest-bdd
exactly as it's described in be behave_ docs.

Example:

.. code-block:: gherkin

    Feature: Scenario outlines
        Scenario Outline: Outlined given, when, thens
            Given there are <start> cucumbers
            When I eat <eat> cucumbers
            Then I should have <left> cucumbers

            Examples:
            | start | eat | left |
            |  12   |  5  |  7   |

pytest-bdd feature file format also supports example tables in different way:


.. code-block:: gherkin

    Feature: Scenario outlines
        Scenario Outline: Outlined given, when, thens
            Given there are <start> cucumbers
            When I eat <eat> cucumbers
            Then I should have <left> cucumbers

            Examples: Vertical
            | start | 12 | 2 |
            | eat   | 5  | 1 |
            | left  | 7  | 1 |

This form allows to have tables with lots of columns keeping the maximum text width predictable without significant
readability change.

The code will look like:

.. code-block:: python

    from pytest_bdd import given, when, then, scenario


    @scenario(
        "outline.feature",
        "Outlined given, when, thens",
        example_converters=dict(start=int, eat=float, left=str)
    )
    def test_outlined():
        pass


    @given("there are <start> cucumbers", target_fixture="start_cucumbers")
    def start_cucumbers(start):
        assert isinstance(start, int)
        return dict(start=start)


    @when("I eat <eat> cucumbers")
    def eat_cucumbers(start_cucumbers, eat):
        assert isinstance(eat, float)
        start_cucumbers["eat"] = eat


    @then("I should have <left> cucumbers")
    def should_have_left_cucumbers(start_cucumbers, start, eat, left):
        assert isinstance(left, str)
        assert start - eat == int(left)
        assert start_cucumbers["start"] == start
        assert start_cucumbers["eat"] == eat

Example code also shows possibility to pass example converters which may be useful if you need parameter types
different than strings.


Feature examples
^^^^^^^^^^^^^^^^

It's possible to declare example table once for the whole feature, and it will be shared
among all the scenarios of that feature:

.. code-block:: gherkin

    Feature: Outline

        Examples:
        | start | eat | left |
        |  12   |  5  |  7   |
        |  5    |  4  |  1   |

        Scenario Outline: Eat cucumbers
            Given there are <start> cucumbers
            When I eat <eat> cucumbers
            Then I should have <left> cucumbers

        Scenario Outline: Eat apples
            Given there are <start> apples
            When I eat <eat> apples
            Then I should have <left> apples

For some more complex case, you might want to parametrize on both levels: feature and scenario.
This is allowed as long as parameter names do not clash:


.. code-block:: gherkin

    Feature: Outline

        Examples:
        | start | eat | left |
        |  12   |  5  |  7   |
        |  5    |  4  |  1   |

        Scenario Outline: Eat fruits
            Given there are <start> <fruits>
            When I eat <eat> <fruits>
            Then I should have <left> <fruits>

            Examples:
            | fruits  |
            | oranges |
            | apples  |

        Scenario Outline: Eat vegetables
            Given there are <start> <vegetables>
            When I eat <eat> <vegetables>
            Then I should have <left> <vegetables>

            Examples:
            | vegetables |
            | carrots    |
            | tomatoes   |


Combine scenario outline and pytest parametrization
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It's also possible to parametrize the scenario on the python side.
The reason for this is that it is sometimes not needed to mention example table for every scenario.

The code will look like:

.. code-block:: python

    import pytest
    from pytest_bdd import scenario, given, when, then


    # Here we use pytest to parametrize the test with the parameters table
    @pytest.mark.parametrize(
        ["start", "eat", "left"],
        [(12, 5, 7)],
    )
    @scenario(
        "parametrized.feature",
        "Parametrized given, when, thens",
    )
    # Note that we should take the same arguments in the test function that we use
    # for the test parametrization either directly or indirectly (fixtures depend on them).
    def test_parametrized(start, eat, left):
        """We don't need to do anything here, everything will be managed by the scenario decorator."""


    @given("there are <start> cucumbers", target_fixture="start_cucumbers")
    def start_cucumbers(start):
        return dict(start=start)


    @when("I eat <eat> cucumbers")
    def eat_cucumbers(start_cucumbers, start, eat):
        start_cucumbers["eat"] = eat


    @then("I should have <left> cucumbers")
    def should_have_left_cucumbers(start_cucumbers, start, eat, left):
        assert start - eat == left
        assert start_cucumbers["start"] == start
        assert start_cucumbers["eat"] == eat


With a parametrized.feature file:

.. code-block:: gherkin

    Feature: parametrized
        Scenario: Parametrized given, when, thens
            Given there are <start> cucumbers
            When I eat <eat> cucumbers
            Then I should have <left> cucumbers


The significant downside of this approach is inability to see the test table from the feature file.


Organizing your scenarios
-------------------------

The more features and scenarios you have, the more important becomes the question about their organization.
The things you can do (and that is also a recommended way):

* organize your feature files in the folders by semantic groups:

::

    features
    │
    ├──frontend
    │  │
    │  └──auth
    │     │
    │     └──login.feature
    └──backend
       │
       └──auth
          │
          └──login.feature

This looks fine, but how do you run tests only for certain feature?
As pytest-bdd uses pytest, and bdd scenarios are actually normal tests. But test files
are separate from the feature files, the mapping is up to developers, so the test files structure can look
completely different:

::

    tests
    │
    └──functional
       │
       └──test_auth.py
          │
          └ """Authentication tests."""
            from pytest_bdd import scenario

            @scenario('frontend/auth/login.feature')
            def test_logging_in_frontend():
                pass

            @scenario('backend/auth/login.feature')
            def test_logging_in_backend():
                pass


For picking up tests to run we can use
`tests selection <http://pytest.org/latest/usage.html#specifying-tests-selecting-tests>`_ technique. The problem is that
you have to know how your tests are organized, knowing only the feature files organization is not enough.
`cucumber tags <https://github.com/cucumber/cucumber/wiki/Tags>`_ introduce standard way of categorizing your features
and scenarios, which pytest-bdd supports. For example, we could have:

.. code-block:: gherkin

    @login @backend
    Feature: Login

      @successful
      Scenario: Successful login


pytest-bdd uses `pytest markers <http://pytest.org/latest/mark.html#mark>`_ as a `storage` of the tags for the given
scenario test, so we can use standard test selection:

.. code-block:: bash

    pytest -m "backend and login and successful"

The feature and scenario markers are not different from standard pytest markers, and the `@` symbol is stripped out
automatically to allow test selector expressions. If you want to have bdd-related tags to be distinguishable from the
other test markers, use prefix like `bdd`.
Note that if you use pytest `--strict` option, all bdd tags mentioned in the feature files should be also in the
`markers` setting of the `pytest.ini` config. Also for tags please use names which are python-compartible variable
names, eg starts with a non-number, underscore alphanumberic, etc. That way you can safely use tags for tests filtering.

You can customize how tags are converted to pytest marks by implementing the
``pytest_bdd_apply_tag`` hook and returning ``True`` from it:

.. code-block:: python

   def pytest_bdd_apply_tag(tag, function):
       if tag == 'todo':
           marker = pytest.mark.skip(reason="Not implemented yet")
           marker(function)
           return True
       else:
           # Fall back to pytest-bdd's default behavior
           return None

Test setup
----------

Test setup is implemented within the Given section. Even though these steps
are executed imperatively to apply possible side-effects, pytest-bdd is trying
to benefit of the PyTest fixtures which is based on the dependency injection
and makes the setup more declarative style.

.. code-block:: python

    @given("I have a beautiful article", target_fixture="article")
    def article():
        return Article(is_beautiful=True)

The target PyTest fixture "article" gets the return value and any other step can depend on it.

.. code-block:: gherkin

    Feature: The power of PyTest
        Scenario: Symbolic name across steps
            Given I have a beautiful article
            When I publish this article

When step is referring the article to publish it.

.. code-block:: python

    @when("I publish this article")
    def publish_article(article):
        article.publish()


Many other BDD toolkits operate a global context and put the side effects there.
This makes it very difficult to implement the steps, because the dependencies
appear only as the side-effects in the run-time and not declared in the code.
The publish article step has to trust that the article is already in the context,
has to know the name of the attribute it is stored there, the type etc.

In pytest-bdd you just declare an argument of the step function that it depends on
and the PyTest will make sure to provide it.

Still side effects can be applied in the imperative style by design of the BDD.

.. code-block:: gherkin

    Feature: News website
        Scenario: Publishing an article
            Given I have a beautiful article
            And my article is published

Functional tests can reuse your fixture libraries created for the unit-tests and upgrade
them by applying the side effects.

.. code-block:: python

    @pytest.fixture
    def article():
        return Article(is_beautiful=True)


    @given("I have a beautiful article")
    def i_have_a_beautiful_article(article):
        pass


    @given("my article is published")
    def published_article(article):
        article.publish()
        return article


This way side-effects were applied to our article and PyTest makes sure that all
steps that require the "article" fixture will receive the same object. The value
of the "published_article" and the "article" fixtures is the same object.

Fixtures are evaluated only once within the PyTest scope and their values are cached.


Backgrounds
-----------

It's often the case that to cover certain feature, you'll need multiple scenarios. And it's logical that the
setup for those scenarios will have some common parts (if not equal). For this, there are `backgrounds`.
pytest-bdd implements `Gherkin backgrounds <http://behat.org/en/v3.0/user_guide/writing_scenarios.html#backgrounds>`_ for
features.

.. code-block:: gherkin

    Feature: Multiple site support

      Background:
        Given a global administrator named "Greg"
        And a blog named "Greg's anti-tax rants"
        And a customer named "Wilson"
        And a blog named "Expensive Therapy" owned by "Wilson"

      Scenario: Wilson posts to his own blog
        Given I am logged in as Wilson
        When I try to post to "Expensive Therapy"
        Then I should see "Your article was published."

      Scenario: Greg posts to a client's blog
        Given I am logged in as Greg
        When I try to post to "Expensive Therapy"
        Then I should see "Your article was published."

In this example, all steps from the background will be executed before all the scenario's own given
steps, adding possibility to prepare some common setup for multiple scenarios in a single feature.
About background best practices, please read
`here <https://github.com/cucumber/cucumber/wiki/Background#good-practices-for-using-background>`_.

.. NOTE:: There is only step "Given" should be used in "Background" section,
          steps "When" and "Then" are prohibited, because their purpose are
          related to actions and consuming outcomes, that is conflict with
          "Background" aim - prepare system for tests or "put the system
          in a known state" as "Given" does it.
          The statement above is applied for strict Gherkin mode, which is
          enabled by default.


Reusing fixtures
----------------

Sometimes scenarios define new names for the existing fixture that can be
inherited (reused). For example, if we have pytest fixture:


.. code-block:: python

    @pytest.fixture
    def article():
       """Test article."""
       return Article()


Then this fixture can be reused with other names using given():


.. code-block:: python

    @given('I have beautiful article')
    def i_have_an_article(article):
       """I have an article."""


Reusing steps
-------------

It is possible to define some common steps in the parent conftest.py and
simply expect them in the child test file.

common_steps.feature:

.. code-block:: gherkin

    Scenario: All steps are declared in the conftest
        Given I have a bar
        Then bar should have value "bar"

conftest.py:

.. code-block:: python

    from pytest_bdd import given, then


    @given("I have a bar", target_fixture="bar")
    def bar():
        return "bar"


    @then('bar should have value "bar"')
    def bar_is_bar(bar):
        assert bar == "bar"

test_common.py:

.. code-block:: python

    @scenario("common_steps.feature", "All steps are declared in the conftest")
    def test_conftest():
        pass

There are no definitions of the steps in the test file. They were
collected from the parent conftests.


Using unicode in the feature files
----------------------------------

As mentioned above, by default, utf-8 encoding is used for parsing feature files.
For steps definition, you should use unicode strings, which is the default in python 3.
If you are on python 2, make sure you use unicode strings by prefixing them with the `u` sign.


.. code-block:: python

    @given(parsers.re(u"у мене є рядок який містить '{0}'".format(u'(?P<content>.+)')))
    def there_is_a_string_with_content(content, string):
        """Create string with unicode content."""
        string["content"] = content


Default steps
-------------

Here is the list of steps that are implemented inside of the pytest-bdd:

given
    * trace - enters the `pdb` debugger via `pytest.set_trace()`
when
    * trace - enters the `pdb` debugger via `pytest.set_trace()`
then
    * trace - enters the `pdb` debugger via `pytest.set_trace()`


Feature file paths
------------------

By default, pytest-bdd will use current module's path as base path for finding feature files, but this behaviour can be changed in the pytest configuration file (i.e. `pytest.ini`, `tox.ini` or `setup.cfg`) by declaring the new base path in the `bdd_features_base_dir` key. The path is interpreted as relative to the working directory when starting pytest.
You can also override features base path on a per-scenario basis, in order to override the path for specific tests.

pytest.ini:

.. code-block:: ini

    [pytest]
    bdd_features_base_dir = features/

tests/test_publish_article.py:

.. code-block:: python

    from pytest_bdd import scenario


    @scenario("foo.feature", "Foo feature in features/foo.feature")
    def test_foo():
        pass


    @scenario(
        "foo.feature",
        "Foo feature in tests/local-features/foo.feature",
        features_base_dir="./local-features/",
    )
    def test_foo_local():
        pass


The `features_base_dir` parameter can also be passed to the `@scenario` decorator.


Avoid retyping the feature file name
------------------------------------

If you want to avoid retyping the feature file name when defining your scenarios in a test file, use functools.partial.
This will make your life much easier when defining multiple scenarios in a test file. For example:

test_publish_article.py:

.. code-block:: python

    from functools import partial

    import pytest_bdd


    scenario = partial(pytest_bdd.scenario, "/path/to/publish_article.feature")


    @scenario("Publishing the article")
    def test_publish():
        pass


    @scenario("Publishing the article as unprivileged user")
    def test_publish_unprivileged():
        pass


You can learn more about `functools.partial <http://docs.python.org/2/library/functools.html#functools.partial>`_
in the Python docs.


Hooks
-----

pytest-bdd exposes several `pytest hooks <http://pytest.org/latest/plugins.html#well-specified-hooks>`_
which might be helpful building useful reporting, visualization, etc on top of it:

* pytest_bdd_before_scenario(request, feature, scenario) - Called before scenario is executed

* pytest_bdd_after_scenario(request, feature, scenario) - Called after scenario is executed
  (even if one of steps has failed)

* pytest_bdd_before_step(request, feature, scenario, step, step_func) - Called before step function
  is executed and it's arguments evaluated

* pytest_bdd_before_step_call(request, feature, scenario, step, step_func, step_func_args) - Called before step
  function is executed with evaluated arguments

* pytest_bdd_after_step(request, feature, scenario, step, step_func, step_func_args) - Called after step function
  is successfully executed

* pytest_bdd_step_error(request, feature, scenario, step, step_func, step_func_args, exception) - Called when step
  function failed to execute

* pytest_bdd_step_func_lookup_error(request, feature, scenario, step, exception) - Called when step lookup failed


Browser testing
---------------

Tools recommended to use for browser testing:

* pytest-splinter_ - pytest `splinter <http://splinter.cobrateam.info/>`_ integration for the real browser testing


Reporting
---------

It's important to have nice reporting out of your bdd tests. Cucumber introduced some kind of standard for
`json format <https://www.relishapp.com/cucumber/cucumber/docs/json-output-formatter>`_
which can be used for `this <https://wiki.jenkins-ci.org/display/JENKINS/Cucumber+Test+Result+Plugin>`_ jenkins
plugin

To have an output in json format:

::

    pytest --cucumberjson=<path to json report>

This will output an expanded (meaning scenario outlines will be expanded to several scenarios) cucumber format.
To also fill in parameters in the step name, you have to explicitly tell pytest-bdd to use the expanded format:

::

    pytest --cucumberjson=<path to json report> --cucumberjson-expanded

To enable gherkin-formatted output on terminal, use

::

    pytest --gherkin-terminal-reporter


Terminal reporter supports expanded format as well

::

    pytest --gherkin-terminal-reporter-expanded



Test code generation helpers
----------------------------

For newcomers it's sometimes hard to write all needed test code without being frustrated.
To simplify their life, simple code generator was implemented. It allows to create fully functional
but of course empty tests and step definitions for given a feature file.
It's done as a separate console script provided by pytest-bdd package:

::

    pytest-bdd generate <feature file name> .. <feature file nameN>

It will print the generated code to the standard output so you can easily redirect it to the file:

::

    pytest-bdd generate features/some.feature > tests/functional/test_some.py


Advanced code generation
------------------------

For more experienced users, there's smart code generation/suggestion feature. It will only generate the
test code which is not yet there, checking existing tests and step definitions the same way it's done during the
test execution. The code suggestion tool is called via passing additional pytest arguments:

::

    pytest --generate-missing --feature features tests/functional

The output will be like:

::

    ============================= test session starts ==============================
    platform linux2 -- Python 2.7.6 -- py-1.4.24 -- pytest-2.6.2
    plugins: xdist, pep8, cov, cache, bdd, bdd, bdd
    collected 2 items

    Scenario is not bound to any test: "Code is generated for scenarios which are not bound to any tests" in feature "Missing code generation" in /tmp/pytest-552/testdir/test_generate_missing0/tests/generation.feature
    --------------------------------------------------------------------------------

    Step is not defined: "I have a custom bar" in scenario: "Code is generated for scenario steps which are not yet defined(implemented)" in feature "Missing code generation" in /tmp/pytest-552/testdir/test_generate_missing0/tests/generation.feature
    --------------------------------------------------------------------------------
    Please place the code above to the test file(s):

    @scenario('tests/generation.feature', 'Code is generated for scenarios which are not bound to any tests')
    def test_Code_is_generated_for_scenarios_which_are_not_bound_to_any_tests():
        """Code is generated for scenarios which are not bound to any tests."""


    @given("I have a custom bar")
    def I_have_a_custom_bar():
        """I have a custom bar."""

As as side effect, the tool will validate the files for format errors, also some of the logic bugs, for example the
ordering of the types of the steps.


.. _Migration from 3.x.x:

Migration of your tests from versions 3.x.x
-------------------------------------------


Given steps are no longer fixtures. In case it is needed to make given step setup a fixture
the target_fixture parameter should be used.


.. code-block:: python

    @given("there's an article", target_fixture="article")
    def there_is_an_article():
        return Article()


Given steps no longer have fixture parameter. In fact the step may depend on multiple fixtures.
Just normal step declaration with the dependency injection should be used.

.. code-block:: python

    @given("there's an article")
    def there_is_an_article(article):
        pass


Strict gherkin option is removed, so the ``strict_gherkin`` parameter can be removed from the scenario decorators
as well as ``bdd_strict_gherkin`` from the ini files.

Step validation handlers for the hook ``pytest_bdd_step_validation_error`` should be removed.


License
-------

This software is licensed under the `MIT license <http://en.wikipedia.org/wiki/MIT_License>`_.

© 2013-2014 Oleg Pidsadnyi, Anatoly Bubenkov and others
